#!/usr/bin/env python3
"""Focused P2 tests for the test-only Ichi-R inline-summary config."""

import base64
import copy
import hashlib
import sys
import unittest
from pathlib import Path


sys.dont_write_bytecode = True
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
for import_path in (
    PROJECT_ROOT,
    STEP_DIR / "00_tool",
    STEP_DIR / "00_tool" / "adapters" / "inline_summary",
    STEP_DIR / "00_tool" / "canonicalize",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.json_utils import read_jsonl, read_jsonl_as_list
from canonical_overlay import build_canonical_overlay
from inline_summary_adapter import InlineSummaryAdapter
from run_offline_replay import DEFAULT_INPUT, process_records
from run_selective_pipeline_test import (
    _load_existing_modules,
    _production_artifact_snapshot,
)


CONFIG_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "companies"
    / "ichi_r.config.json.example"
)
FIXTURE_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "fixtures"
    / "redacted"
    / "inline_summary"
    / "ichi_r.fixture.jsonl.example"
)


class IchiRInlineSummaryAdapterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.adapter = InlineSummaryAdapter.from_file(CONFIG_PATH)
        fixture_records = read_jsonl_as_list(str(FIXTURE_PATH))
        if len(fixture_records) != 1:
            raise AssertionError("Ichi-R fixture must contain exactly one mail")
        cls.fixture = fixture_records[0]
        payloads = {
            "スキルシート(R.A)_20260825.xlsx": b"ichi-r-redacted-A-v1",
            "RB【匿名駅】.xlsx": b"ichi-r-redacted-B-v1",
        }
        for attachment in cls.fixture["attachments"]:
            payload = payloads[attachment["filename"]]
            attachment["data"] = base64.urlsafe_b64encode(payload).decode("ascii")
            attachment["size"] = len(payload)

        cls.production_before = _production_artifact_snapshot()
        cls.production_records = [
            record
            for record in read_jsonl(str(DEFAULT_INPUT))
            if cls.adapter.matches(record)
        ]
        cls.production_artifacts, cls.production_stats = process_records(
            cls.production_records, cls.adapter
        )
        cls.selective = cls._run_01_4_and_02_1(
            cls.production_artifacts["derived_mail_master"]
        )
        cls.production_after = _production_artifact_snapshot()

    @classmethod
    def _run_01_4_and_02_1(cls, overlays):
        modules = _load_existing_modules(("cleanup", "classify"))
        classify_module = modules["classify"]
        if classify_module.USE_LLM_CLASSIFY:
            raise AssertionError("02-1 LLM feature flag must remain OFF")
        cleanup_module = modules["cleanup"]
        cleanup_rules = cleanup_module.load_cleanup_rules(
            cleanup_module.CLEANUP_RULES_PATH
        )
        keywords = classify_module.load_keywords(classify_module.KEYWORDS_PATH)
        cleaned = []
        classified = []
        for overlay in overlays:
            cleaned_body, _ = cleanup_module.cleanup_body(
                overlay["body_text"], cleanup_rules
            )
            cleaned.append({"message_id": overlay["message_id"], "body_text": cleaned_body})
            mail_type, _, _ = classify_module.rule_classify(
                overlay["subject"],
                cleaned_body,
                keywords,
                has_attachment=bool(overlay["attachments"]),
            )
            classified.append(
                {"message_id": overlay["message_id"], "mail_type": mail_type}
            )
        return {
            "cleanup": cleaned,
            "classification": classified,
            "llm_api_calls": 0,
            "external_url_calls": 0,
        }

    def _parse_fixture(self):
        return self.adapter.parse(copy.deepcopy(self.fixture))

    @staticmethod
    def _file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as file_object:
            for chunk in iter(lambda: file_object.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def test_01_normal_mail_produces_exactly_two_items(self) -> None:
        result = self._parse_fixture()
        self.assertEqual("PARSED", result.status)
        self.assertEqual(2, len(result.items))

    def test_02_fullwidth_separator_is_supported(self) -> None:
        self.assertEqual(3, self.fixture["body_text"].count("＝＝＝＝＝＝＝＝＝＝"))
        self.assertEqual("PARSED", self._parse_fixture().status)

    def test_03_name_anchor_is_supported(self) -> None:
        self.assertEqual(2, self.fixture["body_text"].count("■氏名："))
        self.assertEqual("PARSED", self._parse_fixture().status)

    def test_04_expected_count_two_fails_atomically_when_one(self) -> None:
        mail = copy.deepcopy(self.fixture)
        second_anchor = mail["body_text"].index("■氏名：R.B.")
        mail["body_text"] = mail["body_text"][:second_anchor] + "何卒、よろしくお願いいたします。"
        result = self.adapter.parse(mail)
        self.assertEqual("PARTIAL", result.status)
        self.assertEqual([], result.items)

    def test_05_filename_grammar_a_maps_exactly(self) -> None:
        items = self._parse_fixture().items
        self.assertEqual(
            "スキルシート(R.A)_20260825.xlsx",
            items[0]["attachment_mapping"]["filename"],
        )

    def test_06_filename_grammar_b_maps_exactly(self) -> None:
        items = self._parse_fixture().items
        self.assertEqual(
            "RB【匿名駅】.xlsx",
            items[1]["attachment_mapping"]["filename"],
        )

    def test_07_false_substring_filename_does_not_match(self) -> None:
        mail = copy.deepcopy(self.fixture)
        mail["attachments"][1]["filename"] = "XRB【匿名駅】.xlsx"
        result = self.adapter.parse(mail)
        self.assertEqual("HUMAN_REVIEW", result.status)
        self.assertEqual([], result.items)

    def test_08_zero_attachment_candidate_fails_closed(self) -> None:
        mail = copy.deepcopy(self.fixture)
        mail["attachments"][1]["filename"] = "RC【匿名駅】.xlsx"
        result = self.adapter.parse(mail)
        self.assertEqual("HUMAN_REVIEW", result.status)
        self.assertEqual([], result.items)

    def test_09_multiple_attachment_candidates_fail_closed(self) -> None:
        mail = copy.deepcopy(self.fixture)
        mail["attachments"][1]["filename"] = "RA【匿名駅】.xlsx"
        result = self.adapter.parse(mail)
        self.assertEqual("HUMAN_REVIEW", result.status)
        self.assertEqual([], result.items)

    def test_10_shared_url_is_not_propagated(self) -> None:
        items = self._parse_fixture().items
        self.assertTrue(self.fixture["html_links"])
        self.assertEqual([[], []], [item["html_links"] for item in items])
        overlays = [build_canonical_overlay(self.fixture, item) for item in items]
        self.assertEqual([[], []], [record["html_links"] for record in overlays])

    def test_11_logical_ids_are_distinct_and_hashed(self) -> None:
        items = self._parse_fixture().items
        logical_ids = [item["logical_item_id"] for item in items]
        self.assertEqual(2, len(set(logical_ids)))
        self.assertTrue(all(identifier.startswith("li_") for identifier in logical_ids))
        self.assertTrue(all("RA" not in identifier.upper() for identifier in logical_ids))
        self.assertTrue(all("RB" not in identifier.upper() for identifier in logical_ids))

    def test_12_derived_ids_are_deterministic(self) -> None:
        first = self._parse_fixture().items
        second = self._parse_fixture().items
        identity_keys = (
            "logical_item_id",
            "body_fingerprint",
            "attachment_fingerprint",
            "version_fingerprint",
            "derived_item_id",
            "canonical_subject",
        )
        self.assertEqual(
            [tuple(item[key] for key in identity_keys) for item in first],
            [tuple(item[key] for key in identity_keys) for item in second],
        )

    def test_13_body_change_creates_new_version(self) -> None:
        original = self._parse_fixture().items[0]
        mail = copy.deepcopy(self.fixture)
        mail["body_text"] = mail["body_text"].replace(
            "技術情報は匿名化済み", "技術情報は匿名化済み・更新", 1
        )
        changed = self.adapter.parse(mail).items[0]
        self.assertEqual(original["logical_item_id"], changed["logical_item_id"])
        self.assertNotEqual(original["body_fingerprint"], changed["body_fingerprint"])
        self.assertNotEqual(original["version_fingerprint"], changed["version_fingerprint"])
        self.assertNotEqual(original["derived_item_id"], changed["derived_item_id"])
        self.assertNotEqual(original["canonical_subject"], changed["canonical_subject"])

    def test_14_attachment_change_creates_new_version(self) -> None:
        original = self._parse_fixture().items[0]
        mail = copy.deepcopy(self.fixture)
        payload = b"ichi-r-redacted-A-v2"
        mail["attachments"][0]["data"] = base64.urlsafe_b64encode(payload).decode("ascii")
        mail["attachments"][0]["size"] = len(payload)
        changed = self.adapter.parse(mail).items[0]
        self.assertEqual(original["logical_item_id"], changed["logical_item_id"])
        self.assertEqual(original["body_fingerprint"], changed["body_fingerprint"])
        self.assertNotEqual(
            original["attachment_fingerprint"], changed["attachment_fingerprint"]
        )
        self.assertNotEqual(original["derived_item_id"], changed["derived_item_id"])
        self.assertNotEqual(original["canonical_subject"], changed["canonical_subject"])

    def test_15_canonical_subject_has_zero_collision(self) -> None:
        items = self._parse_fixture().items
        self.assertEqual(2, len({item["canonical_subject"] for item in items}))
        self.assertEqual(
            2,
            len(
                {
                    (self.fixture["from"], item["canonical_subject"])
                    for item in items
                }
            ),
        )

    def test_16_classification_context_is_natural_resource_signal(self) -> None:
        context = self.adapter.config["canonical_body_classification_context"]
        self.assertIn("弊社フリーランス", context)
        self.assertIn("サーバエンジニア", context)
        self.assertIn("2名", context)
        self.assertNotIn("resource", context.casefold())

    def test_17_item_type_is_not_injected_into_canonical_content(self) -> None:
        items = self._parse_fixture().items
        for item in items:
            self.assertNotIn("item_type", item["body_text"])
            self.assertNotIn("resource", item["body_text"].casefold())
            self.assertNotIn(self.fixture["subject"], item["body_text"])

    def test_18_offline_replay_matches_one_mail_and_two_items(self) -> None:
        stats = self.production_stats
        self.assertEqual(1, stats["input_mails"])
        self.assertEqual(2, stats["expected_item_occurrences"])
        self.assertEqual(1, stats["parsed_mails"])
        self.assertEqual(0, stats["partial_mails"])
        self.assertEqual(0, stats["human_review_mails"])
        self.assertEqual(2, stats["parsed_occurrences"])
        self.assertEqual(2, stats["logical_distinct"])
        self.assertEqual(2, stats["derived_versions"])
        self.assertEqual(2, stats["attachment_mapping_success"])
        self.assertEqual(0, stats["duplicate_derived_id_in_overlay"])
        self.assertTrue(stats["canonical_overlay_schema_ok"])

    def test_19_offline_replay_is_order_deterministic(self) -> None:
        forward = self.production_artifacts["derived_mail_master"]
        reverse_artifacts, reverse_stats = process_records(
            reversed(self.production_records), self.adapter
        )
        self.assertEqual(forward, reverse_artifacts["derived_mail_master"])
        self.assertEqual(self.production_stats, reverse_stats)

    def test_20_offline_replay_propagates_no_html_links(self) -> None:
        overlays = self.production_artifacts["derived_mail_master"]
        self.assertEqual(2, len(overlays))
        self.assertTrue(all(record["html_links"] == [] for record in overlays))

    def test_21_01_4_and_02_1_classify_both_as_resource_without_llm(self) -> None:
        self.assertEqual(2, len(self.selective["cleanup"]))
        self.assertTrue(all(record["body_text"] for record in self.selective["cleanup"]))
        self.assertEqual(
            ["resource", "resource"],
            [record["mail_type"] for record in self.selective["classification"]],
        )
        self.assertEqual(0, self.selective["llm_api_calls"])
        self.assertEqual(0, self.selective["external_url_calls"])

    def test_22_production_artifacts_are_unchanged_and_unwritten(self) -> None:
        self.assertEqual(self.production_before, self.production_after)
        source_before = self._file_sha256(DEFAULT_INPUT)
        process_records([copy.deepcopy(self.fixture)], self.adapter)
        source_after = self._file_sha256(DEFAULT_INPUT)
        self.assertEqual(source_before, source_after)

    def test_23_missing_footer_or_required_marker_is_partial(self) -> None:
        missing_footer = copy.deepcopy(self.fixture)
        missing_footer["body_text"] = missing_footer["body_text"].replace(
            "【営業中エンジニア一覧スプレッドシート】", "【共有情報】"
        )
        self.assertEqual("PARTIAL", self.adapter.parse(missing_footer).status)
        missing_marker = copy.deepcopy(self.fixture)
        missing_marker["body_text"] = missing_marker["body_text"].replace(
            "■スキル：技術情報は匿名化済み\n", "", 1
        )
        self.assertEqual("PARTIAL", self.adapter.parse(missing_marker).status)

    def test_24_selector_requires_sender_domain_and_subject(self) -> None:
        wrong_domain = copy.deepcopy(self.fixture)
        wrong_domain["from"] = "redacted@example.invalid"
        self.assertEqual("UNSUPPORTED", self.adapter.parse(wrong_domain).status)
        wrong_subject = copy.deepcopy(self.fixture)
        wrong_subject["subject"] = "別形式の要員メール"
        self.assertEqual("UNSUPPORTED", self.adapter.parse(wrong_subject).status)


if __name__ == "__main__":
    unittest.main()

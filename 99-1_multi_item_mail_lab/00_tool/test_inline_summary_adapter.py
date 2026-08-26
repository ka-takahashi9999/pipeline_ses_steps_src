#!/usr/bin/env python3
"""Focused tests for the 99-1 test-only inline-summary adapter."""

import base64
import copy
import hashlib
import sys
import unittest
from pathlib import Path


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

from common.json_utils import read_jsonl_as_list
from inline_summary_adapter import InlineSummaryAdapter
from run_offline_replay import DEFAULT_INPUT, process_records


CONFIG_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "companies"
    / "netwisdom.config.json.example"
)
FIXTURE_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "fixtures"
    / "redacted"
    / "inline_summary"
    / "netwisdom.fixture.jsonl.example"
)


class InlineSummaryAdapterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.adapter = InlineSummaryAdapter.from_file(CONFIG_PATH)
        cls.fixtures = read_jsonl_as_list(str(FIXTURE_PATH))
        payloads = {
            "skillsheet-RESOURCE-A1.xlsx": b"resource-A-v1",
            "skillsheet-RESOURCE-B2.xlsx": b"resource-B-v1",
        }
        for mail in cls.fixtures:
            for attachment in mail["attachments"]:
                payload = payloads[attachment["filename"]]
                attachment["data"] = base64.urlsafe_b64encode(payload).decode("ascii")
                attachment["size"] = len(payload)

    def test_normal_mail_produces_exactly_two_items(self) -> None:
        result = self.adapter.parse(self.fixtures[0])
        self.assertEqual("PARSED", result.status)
        self.assertEqual(2, len(result.items))

    def test_two_anchors_and_separators_are_parsed(self) -> None:
        mail = self.fixtures[0]
        self.assertEqual(2, mail["body_text"].count("技術者:"))
        self.assertGreaterEqual(mail["body_text"].count("-" * 20), 3)
        self.assertEqual("PARSED", self.adapter.parse(mail).status)

    def test_one_block_when_two_expected_is_partial_and_atomic(self) -> None:
        mail = copy.deepcopy(self.fixtures[0])
        second_anchor = mail["body_text"].index("技術者: RESOURCE-B2")
        mail["body_text"] = mail["body_text"][:second_anchor] + "以上、1名です。"
        result = self.adapter.parse(mail)
        self.assertEqual("PARTIAL", result.status)
        self.assertEqual([], result.items)

    def test_attachments_map_one_to_one(self) -> None:
        result = self.adapter.parse(self.fixtures[0])
        filenames = [
            item["attachment_mapping"]["filename"] for item in result.items
        ]
        self.assertEqual(
            ["skillsheet-RESOURCE-A1.xlsx", "skillsheet-RESOURCE-B2.xlsx"],
            filenames,
        )
        self.assertEqual(2, len(set(filenames)))

    def test_ambiguous_attachment_mapping_needs_human_review(self) -> None:
        mail = copy.deepcopy(self.fixtures[0])
        mail["attachments"][1]["filename"] = "copy-RESOURCE-A1.xlsx"
        result = self.adapter.parse(mail)
        self.assertEqual("HUMAN_REVIEW", result.status)
        self.assertEqual([], result.items)

    def test_derived_id_is_deterministic(self) -> None:
        first = self.adapter.parse(copy.deepcopy(self.fixtures[0]))
        second = self.adapter.parse(copy.deepcopy(self.fixtures[0]))
        self.assertEqual(
            [item["derived_item_id"] for item in first.items],
            [item["derived_item_id"] for item in second.items],
        )

    def test_unchanged_resend_has_same_logical_and_content_identity(self) -> None:
        first = self.adapter.parse(self.fixtures[0])
        resend = self.adapter.parse(self.fixtures[1])
        self.assertEqual(
            [
                (item["logical_item_id"], item["content_fingerprint"])
                for item in first.items
            ],
            [
                (item["logical_item_id"], item["content_fingerprint"])
                for item in resend.items
            ],
        )

    def test_content_change_preserves_logical_id_and_changes_version(self) -> None:
        original = self.adapter.parse(self.fixtures[0]).items[0]
        changed_mail = copy.deepcopy(self.fixtures[0])
        changed_mail["body_text"] = changed_mail["body_text"].replace(
            "Java / SQL", "Java / Python"
        )
        changed = self.adapter.parse(changed_mail).items[0]
        self.assertEqual(original["logical_item_id"], changed["logical_item_id"])
        self.assertNotEqual(original["content_fingerprint"], changed["content_fingerprint"])
        self.assertNotEqual(original["derived_item_id"], changed["derived_item_id"])
        self.assertNotEqual(original["canonical_subject"], changed["canonical_subject"])

    def test_different_item_does_not_collide(self) -> None:
        original = self.adapter.parse(self.fixtures[0]).items[0]
        different_mail = copy.deepcopy(self.fixtures[0])
        different_mail["body_text"] = different_mail["body_text"].replace(
            "RESOURCE-A1", "RESOURCE-C3"
        )
        different_mail["attachments"][0]["filename"] = (
            "skillsheet-RESOURCE-C3.xlsx"
        )
        different = self.adapter.parse(different_mail).items[0]
        self.assertNotEqual(original["logical_item_id"], different["logical_item_id"])
        self.assertNotEqual(original["derived_item_id"], different["derived_item_id"])

    def test_four_resends_dedupe_to_two_overlay_versions(self) -> None:
        artifacts, stats = process_records(self.fixtures, self.adapter)
        self.assertEqual(8, stats["derived_item_occurrences"])
        self.assertEqual(2, stats["logical_distinct"])
        self.assertEqual(2, stats["content_distinct"])
        self.assertEqual(6, stats["duplicate_occurrences"])
        self.assertEqual(2, len(artifacts["derived_mail_master"]))
        self.assertEqual(0, stats["duplicate_derived_id_in_overlay"])

    def test_processing_has_zero_production_writes(self) -> None:
        source_before = hashlib.sha256(DEFAULT_INPUT.read_bytes()).hexdigest()
        process_records(self.fixtures, self.adapter)
        source_after = hashlib.sha256(DEFAULT_INPUT.read_bytes()).hexdigest()
        self.assertEqual(source_before, source_after)

    def test_false_substring_attachment_is_not_mapped(self) -> None:
        mail = copy.deepcopy(self.fixtures[0])
        mail["attachments"][0]["filename"] = (
            "skillsheet-XRESOURCE-A1Y.xlsx"
        )
        result = self.adapter.parse(mail)
        self.assertEqual("HUMAN_REVIEW", result.status)
        self.assertEqual([], result.items)

    def test_config_extracted_identifier_exact_match_passes(self) -> None:
        result = self.adapter.parse(copy.deepcopy(self.fixtures[0]))
        self.assertEqual("PARSED", result.status)
        self.assertEqual(
            "skillsheet-RESOURCE-A1.xlsx",
            result.items[0]["attachment_mapping"]["filename"],
        )

    def test_zero_exact_attachment_candidates_fails_closed(self) -> None:
        mail = copy.deepcopy(self.fixtures[0])
        mail["attachments"][0]["filename"] = "skillsheet-RESOURCE-Z9.xlsx"
        result = self.adapter.parse(mail)
        self.assertEqual("HUMAN_REVIEW", result.status)
        self.assertEqual([], result.items)

    def test_two_exact_attachment_candidates_fail_closed(self) -> None:
        mail = copy.deepcopy(self.fixtures[0])
        mail["attachments"][1]["filename"] = "skillsheet-RESOURCE-A1.xlsx"
        result = self.adapter.parse(mail)
        self.assertEqual("HUMAN_REVIEW", result.status)
        self.assertEqual([], result.items)

    def test_same_body_and_attachment_preserve_all_version_identity(self) -> None:
        first = self.adapter.parse(copy.deepcopy(self.fixtures[0])).items[0]
        second = self.adapter.parse(copy.deepcopy(self.fixtures[0])).items[0]
        identity_keys = (
            "logical_item_id",
            "body_fingerprint",
            "attachment_fingerprint",
            "version_fingerprint",
            "derived_item_id",
            "canonical_subject",
        )
        self.assertEqual(
            tuple(first[key] for key in identity_keys),
            tuple(second[key] for key in identity_keys),
        )

    def test_attachment_only_change_creates_new_version(self) -> None:
        original = self.adapter.parse(copy.deepcopy(self.fixtures[0])).items[0]
        changed_mail = copy.deepcopy(self.fixtures[0])
        changed_payload = b"resource-A-v2"
        changed_mail["attachments"][0]["data"] = (
            base64.urlsafe_b64encode(changed_payload).decode("ascii")
        )
        changed_mail["attachments"][0]["size"] = len(changed_payload)
        changed = self.adapter.parse(changed_mail).items[0]
        self.assertEqual(original["logical_item_id"], changed["logical_item_id"])
        self.assertEqual(original["body_fingerprint"], changed["body_fingerprint"])
        self.assertNotEqual(
            original["attachment_fingerprint"], changed["attachment_fingerprint"]
        )
        self.assertNotEqual(original["version_fingerprint"], changed["version_fingerprint"])
        self.assertNotEqual(original["derived_item_id"], changed["derived_item_id"])
        self.assertNotEqual(original["canonical_subject"], changed["canonical_subject"])

    def test_attachment_only_new_version_is_not_deduped(self) -> None:
        original_mail = copy.deepcopy(self.fixtures[0])
        changed_mail = copy.deepcopy(self.fixtures[0])
        changed_mail["message_id"] = "fixture-attachment-version-002"
        changed_payload = b"resource-A-v2"
        changed_mail["attachments"][0]["data"] = (
            base64.urlsafe_b64encode(changed_payload).decode("ascii")
        )
        changed_mail["attachments"][0]["size"] = len(changed_payload)
        artifacts, stats = process_records(
            [original_mail, changed_mail], self.adapter
        )
        self.assertEqual(2, stats["logical_distinct"])
        self.assertEqual(3, stats["derived_versions"])
        self.assertEqual(1, stats["duplicate_occurrences"])
        self.assertEqual(3, len(artifacts["derived_mail_master"]))

    def test_body_only_change_creates_new_version(self) -> None:
        original = self.adapter.parse(copy.deepcopy(self.fixtures[0])).items[0]
        changed_mail = copy.deepcopy(self.fixtures[0])
        changed_mail["body_text"] = changed_mail["body_text"].replace(
            "Java / SQL", "Java / Go"
        )
        changed = self.adapter.parse(changed_mail).items[0]
        self.assertEqual(original["logical_item_id"], changed["logical_item_id"])
        self.assertNotEqual(original["body_fingerprint"], changed["body_fingerprint"])
        self.assertEqual(
            original["attachment_fingerprint"], changed["attachment_fingerprint"]
        )
        self.assertNotEqual(original["derived_item_id"], changed["derived_item_id"])

    def test_distinct_items_have_no_id_or_subject_collision(self) -> None:
        items = self.adapter.parse(copy.deepcopy(self.fixtures[0])).items
        self.assertEqual(2, len({item["logical_item_id"] for item in items}))
        self.assertEqual(2, len({item["derived_item_id"] for item in items}))
        self.assertEqual(2, len({item["canonical_subject"] for item in items}))

    def test_missing_attachment_payload_fails_closed(self) -> None:
        mail = copy.deepcopy(self.fixtures[0])
        del mail["attachments"][0]["data"]
        result = self.adapter.parse(mail)
        self.assertEqual("HUMAN_REVIEW", result.status)
        self.assertEqual([], result.items)


if __name__ == "__main__":
    unittest.main()

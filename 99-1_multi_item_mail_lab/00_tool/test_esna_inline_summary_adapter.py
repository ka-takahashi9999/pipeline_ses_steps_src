#!/usr/bin/env python3
"""Focused actual and synthetic validation for one ESNA variable-N config."""

import base64
import copy
import re
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
    STEP_DIR / "00_tool" / "core",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.json_utils import read_jsonl_as_list
from canonical_overlay import MAIL_MASTER_KEYS, build_canonical_overlay
from inline_summary_adapter import InlineSummaryAdapter
from run_esna_offline_replay import CONFIG_PATH, build_esna_results
from run_offline_replay import DEFAULT_INPUT
from run_selective_pipeline_test import _production_artifact_snapshot


FIXTURE_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "fixtures"
    / "redacted"
    / "inline_summary"
    / "esna.synthetic.fixture.jsonl.example"
)


def _payload_attachment(filename, payload):
    return {
        "filename": filename,
        "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "size": len(payload),
        "data": base64.urlsafe_b64encode(payload).decode("ascii"),
    }


def _build_synthetic_mail(structural_count, declared_count=None):
    declared_count = structural_count if declared_count is None else declared_count
    blocks = []
    attachments = []
    for index in range(structural_count):
        identifier = f"A.{chr(ord('A') + index)}"
        blocks.append(
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"■氏 名：{identifier}＜匿名＞\n"
            "■所 属：弊社プロパ\n"
            "■最 寄：匿名駅\n"
            "■稼 働：調整可能\n"
            "■単 価：応相談\n"
            "■備 考：匿名化済み\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        attachments.append(
            _payload_attachment(
                f"スキルシート_{identifier.replace('.', '')}.xlsx",
                f"esna-{structural_count}-{index}".encode(),
            )
        )
    return {
        "message_id": f"synthetic-esna-{structural_count}-{declared_count}",
        "from": "ESNA Redacted <redacted@esna.jp>",
        "subject": (
            "【ESNA要員情報】弊社プロパをご紹介いたします"
            f"(エンジニア{declared_count}名)"
        ),
        "body_text": (
            "ご担当者様\n弊社プロパをご紹介いたします。\n\n"
            + "\n\n".join(blocks)
            + "\n\n以上です。\n何卒よろしくお願いいたします。"
        ),
        "attachments": attachments,
        "html_links": [
            {
                "text": "会社情報",
                "href": "https://shared.example.invalid/",
                "source": "text/html",
            }
        ],
    }


class EsnaInlineSummaryAdapterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.adapter = InlineSummaryAdapter.from_file(CONFIG_PATH)
        cls.production_before = _production_artifact_snapshot()
        cls.actual_records = [
            record
            for record in read_jsonl_as_list(str(DEFAULT_INPUT))
            if cls.adapter.matches(record)
        ]
        cls.actual_results = build_esna_results(cls.actual_records)
        cls.actual_summary = cls.actual_results["summary"]
        cls.synthetic_records = read_jsonl_as_list(str(FIXTURE_PATH))
        for record in cls.synthetic_records:
            for index, attachment in enumerate(record["attachments"]):
                payload = f"fixture-{record['message_id']}-{index}".encode()
                attachment.update(
                    _payload_attachment(attachment["filename"], payload)
                )
        cls.production_after = _production_artifact_snapshot()

    def test_01_exactly_one_esna_config_without_fixed_count(self) -> None:
        configs = list(CONFIG_PATH.parent.glob("esna*.config.json.example"))
        self.assertEqual([CONFIG_PATH], configs)
        self.assertNotIn("expected_item_count", self.adapter.config)

    def test_02_actual_deliveries_are_n3_and_n5(self) -> None:
        self.assertEqual(2, len(self.actual_records))
        self.assertEqual([3, 5], self.actual_summary["delivery_cardinalities"])
        self.assertEqual(3, self.actual_summary["parsed_n3"])
        self.assertEqual(5, self.actual_summary["parsed_n5"])

    def test_03_actual_eight_items_and_attachments_map_exactly(self) -> None:
        self.assertEqual(8, self.actual_summary["parsed_occurrences"])
        self.assertEqual(8, self.actual_summary["actual_attachment_count"])
        self.assertEqual(8, self.actual_summary["attachment_mapping_success"])
        self.assertEqual(
            {"ONE_ARTIFACT_PER_ITEM_EXACT_KEY"},
            {
                audit["attachment_mapping"]["rule"]
                for audit in self.actual_results["audit_items"]
            },
        )

    def test_04_actual_completeness_has_no_partial_or_review(self) -> None:
        self.assertEqual(2, self.actual_summary["parsed_mails"])
        self.assertEqual(0, self.actual_summary["partial_mails"])
        self.assertEqual(0, self.actual_summary["human_review_mails"])
        self.assertEqual(0, self.actual_summary["system_failure_mails"])

    def test_05_synthetic_n2_n4_n10_use_same_config(self) -> None:
        observed = {}
        for record in self.synthetic_records:
            result = self.adapter.parse(copy.deepcopy(record))
            count = int(re.search(r"([0-9]+)名", record["subject"]).group(1))
            observed[count] = (result.status, len(result.items))
            self.assertEqual(count, len(record["attachments"]))
            self.assertTrue(
                all(
                    item["attachment_mapping"]["rule"]
                    == "ONE_ARTIFACT_PER_ITEM_EXACT_KEY"
                    for item in result.items
                )
            )
        self.assertEqual(
            {2: ("PARSED", 2), 4: ("PARSED", 4), 10: ("PARSED", 10)},
            observed,
        )

    def test_06_declared_structural_mismatches_emit_zero(self) -> None:
        for declared, structural in ((3, 2), (5, 4), (10, 9), (2, 3)):
            with self.subTest(declared=declared, structural=structural):
                result = self.adapter.parse(
                    _build_synthetic_mail(structural, declared)
                )
                self.assertEqual("PARTIAL", result.status)
                self.assertEqual([], result.items)

    def test_07_missing_primary_count_emits_zero(self) -> None:
        mail = _build_synthetic_mail(2)
        mail["subject"] = "【ESNA要員情報】弊社プロパをご紹介いたします"
        result = self.adapter.parse(mail)
        self.assertEqual("PARTIAL", result.status)
        self.assertEqual([], result.items)

    def test_08_ambiguous_primary_count_emits_zero(self) -> None:
        mail = _build_synthetic_mail(2)
        mail["subject"] = (
            "【ESNA要員情報】弊社プロパをご紹介いたします"
            "(エンジニア2名・ヘルプデスク4名)"
        )
        result = self.adapter.parse(mail)
        self.assertEqual("PARTIAL", result.status)
        self.assertEqual([], result.items)

    def test_09_footer_missing_emits_zero(self) -> None:
        mail = _build_synthetic_mail(2)
        mail["body_text"] = mail["body_text"].replace("以上です。", "")
        result = self.adapter.parse(mail)
        self.assertEqual("PARTIAL", result.status)
        self.assertEqual([], result.items)

    def test_10_middle_block_broken_emits_zero(self) -> None:
        mail = _build_synthetic_mail(4)
        mail["body_text"] = mail["body_text"].replace(
            "■単 価：応相談\n", "", 1
        )
        result = self.adapter.parse(mail)
        self.assertEqual("PARTIAL", result.status)
        self.assertEqual([], result.items)

    def test_11_middle_frame_start_or_end_missing_emits_zero(self) -> None:
        start_missing = _build_synthetic_mail(4)
        start_missing["body_text"] = start_missing["body_text"].replace(
            "━━━━━━━━━━━━━━━━━━━━\n■氏 名：A.B",
            "■氏 名：A.B",
            1,
        )
        start_result = self.adapter.parse(start_missing)
        self.assertEqual("PARTIAL", start_result.status)
        self.assertEqual([], start_result.items)

        end_missing = _build_synthetic_mail(4)
        end_missing["body_text"] = end_missing["body_text"].replace(
            "■備 考：匿名化済み\n━━━━━━━━━━━━━━━━━━━━\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n■氏 名：A.B",
            "■備 考：匿名化済み\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n■氏 名：A.B",
            1,
        )
        end_result = self.adapter.parse(end_missing)
        self.assertEqual("PARTIAL", end_result.status)
        self.assertEqual([], end_result.items)

    def test_12_zero_attachment_candidate_needs_review(self) -> None:
        mail = _build_synthetic_mail(2)
        mail["attachments"][0]["filename"] = "スキルシート_ZZ.xlsx"
        result = self.adapter.parse(mail)
        self.assertEqual("HUMAN_REVIEW", result.status)
        self.assertEqual([], result.items)

    def test_13_multiple_attachment_candidates_need_review(self) -> None:
        mail = _build_synthetic_mail(2)
        mail["attachments"][1]["filename"] = "スキルシート_AA.xlsx"
        result = self.adapter.parse(mail)
        self.assertEqual("HUMAN_REVIEW", result.status)
        self.assertEqual([], result.items)

    def test_14_false_substring_filename_does_not_match(self) -> None:
        mail = _build_synthetic_mail(2)
        mail["attachments"][0]["filename"] = "Xスキルシート_AA.xlsx"
        result = self.adapter.parse(mail)
        self.assertEqual("HUMAN_REVIEW", result.status)
        self.assertEqual([], result.items)
        self.assertTrue(
            any("identifier_unextractable" in reason for reason in result.reasons)
        )

    def test_15_duplicate_identifier_fails_closed(self) -> None:
        mail = _build_synthetic_mail(2)
        mail["body_text"] = mail["body_text"].replace("A.B＜", "A.A＜")
        result = self.adapter.parse(mail)
        self.assertEqual("HUMAN_REVIEW", result.status)
        self.assertEqual([], result.items)

    def test_16_same_item_version_is_stable(self) -> None:
        mail = _build_synthetic_mail(4)
        first = self.adapter.parse(copy.deepcopy(mail))
        second = self.adapter.parse(copy.deepcopy(mail))
        self.assertEqual(
            [item["derived_item_id"] for item in first.items],
            [item["derived_item_id"] for item in second.items],
        )
        self.assertEqual(
            [item["canonical_subject"] for item in first.items],
            [item["canonical_subject"] for item in second.items],
        )

    def test_17_attachment_order_does_not_change_identity(self) -> None:
        mail = _build_synthetic_mail(4)
        first = self.adapter.parse(copy.deepcopy(mail))
        reordered = copy.deepcopy(mail)
        reordered["attachments"] = list(reversed(reordered["attachments"]))
        second = self.adapter.parse(reordered)
        self.assertEqual(
            [item["derived_item_id"] for item in first.items],
            [item["derived_item_id"] for item in second.items],
        )

    def test_18_artifact_change_versions_same_logical_item(self) -> None:
        mail = _build_synthetic_mail(2)
        original = self.adapter.parse(copy.deepcopy(mail)).items[0]
        changed = copy.deepcopy(mail)
        payload = b"changed-artifact"
        changed["attachments"][0].update(
            _payload_attachment(changed["attachments"][0]["filename"], payload)
        )
        version = self.adapter.parse(changed).items[0]
        self.assertEqual(original["logical_item_id"], version["logical_item_id"])
        self.assertNotEqual(original["derived_item_id"], version["derived_item_id"])
        self.assertNotEqual(original["canonical_subject"], version["canonical_subject"])

    def test_19_body_change_versions_same_logical_item(self) -> None:
        mail = _build_synthetic_mail(2)
        original = self.adapter.parse(copy.deepcopy(mail)).items[0]
        changed = copy.deepcopy(mail)
        changed["body_text"] = changed["body_text"].replace(
            "■備 考：匿名化済み", "■備 考：匿名化済み・更新", 1
        )
        version = self.adapter.parse(changed).items[0]
        self.assertEqual(original["logical_item_id"], version["logical_item_id"])
        self.assertNotEqual(original["derived_item_id"], version["derived_item_id"])
        self.assertNotEqual(original["canonical_subject"], version["canonical_subject"])

    def test_20_identity_and_subject_collisions_are_zero(self) -> None:
        overlays = self.actual_results["derived_mail_master"]
        self.assertEqual(8, self.actual_summary["logical_distinct"])
        self.assertEqual(8, len({record["message_id"] for record in overlays}))
        self.assertEqual(8, len({record["subject"] for record in overlays}))

    def test_21_canonical_overlay_is_item_only_and_schema_compatible(self) -> None:
        source_mail = _build_synthetic_mail(2)
        result = self.adapter.parse(source_mail)
        overlays = [
            build_canonical_overlay(source_mail, item)
            for item in result.items
        ]
        self.assertTrue(all(set(record) == MAIL_MASTER_KEYS for record in overlays))
        self.assertTrue(all(len(record["attachments"]) == 1 for record in overlays))
        self.assertTrue(all(record["html_links"] == [] for record in overlays))
        self.assertTrue(all("resource" not in record["body_text"].casefold() for record in overlays))
        self.assertTrue(all(record["from"] == source_mail["from"] for record in overlays))
        self.assertNotIn("A.B", overlays[0]["body_text"])

    def test_22_classification_context_uses_dynamic_delivery_count(self) -> None:
        n2_mail = _build_synthetic_mail(2)
        n4_mail = _build_synthetic_mail(4)
        n2_body = self.adapter.parse(n2_mail).items[0]["body_text"]
        n4_body = self.adapter.parse(n4_mail).items[0]["body_text"]
        self.assertIn("ESNA要員情報", n2_body)
        self.assertIn("弊社プロパ", n2_body)
        self.assertIn("2名", n2_body)
        self.assertIn("4名", n4_body)
        self.assertNotIn(n2_mail["subject"], n2_body)
        self.assertNotIn("resource", n2_body.casefold())

    def test_23_actual_has_no_cross_item_or_shared_url_propagation(self) -> None:
        self.assertEqual(0, self.actual_summary["cross_item_contamination"])
        self.assertEqual(0, self.actual_summary["shared_url_propagated"])

    def test_24_existing_01_4_02_1_classify_all_eight_resource(self) -> None:
        self.assertEqual(8, self.actual_summary["cleanup_output"])
        self.assertEqual(8, self.actual_summary["classification_output"])
        self.assertEqual(8, self.actual_summary["resource_output"])
        self.assertEqual(0, self.actual_summary["project_output"])
        self.assertEqual(0, self.actual_summary["ambiguous_output"])
        self.assertEqual(0, self.actual_summary["unknown_output"])

    def test_25_replay_is_fresh_idempotent_and_production_read_only(self) -> None:
        self.assertTrue(self.actual_summary["idempotency_ok"])
        self.assertEqual(0, self.actual_summary["llm_api_calls"])
        self.assertEqual(0, self.actual_summary["external_url_calls"])
        self.assertEqual(0, self.actual_summary["production_write"])
        self.assertEqual(self.production_before, self.production_after)


if __name__ == "__main__":
    unittest.main()

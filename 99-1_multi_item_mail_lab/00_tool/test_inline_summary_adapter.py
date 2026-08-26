#!/usr/bin/env python3
"""Focused tests for the 99-1 test-only inline-summary adapter."""

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


if __name__ == "__main__":
    unittest.main()

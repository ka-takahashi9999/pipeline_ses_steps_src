#!/usr/bin/env python3
"""Focused tests for the 99-1 03/04/05 selective compatibility run."""

import sys
import unittest
from pathlib import Path


sys.dont_write_bytecode = True
TOOL_DIR = Path(__file__).resolve().parent
if str(TOOL_DIR) not in sys.path:
    sys.path.insert(0, str(TOOL_DIR))

from run_selective_pipeline_test import FIVE_REQUIRED_KEYS, build_selective_results


class SelectivePipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.results = build_selective_results()
        cls.report = cls.results["report"]
        cls.expected_ids = {
            record["message_id"] for record in cls.results["derived_input"]
        }

    def test_01_derived_two_remain_resource_two(self) -> None:
        self.assertEqual(2, self.report["derived_input"])
        self.assertEqual(2, self.report["cleanup_output"])
        self.assertEqual(2, self.report["resource_output"])
        self.assertEqual(0, self.report["project_classified"])
        self.assertEqual(0, self.report["ambiguous_classified"])

    def test_02_resource_bypasses_project_only_03_for_both_items(self) -> None:
        self.assertEqual([], self.results["project_route"])
        self.assertEqual(2, self.report["resource_03_bypass_output"])
        self.assertFalse(self.report["project_steps_03_executed"])
        self.assertEqual(
            self.expected_ids,
            {record["message_id"] for record in self.results["resource_03_bypass"]},
        )

    def test_03_existing_04_maps_one_correct_attachment_per_item(self) -> None:
        self.assertEqual(2, self.report["correct_attachment_mapping"])
        self.assertEqual(0, self.report["attachment_missing"])
        self.assertEqual(0, self.report["duplicate_attachment_mapping"])
        self.assertTrue(
            all(record["mapping_correct"] for record in self.results["attachment_identity"])
        )
        self.assertTrue(
            all(record["attachment_count"] == 1 for record in self.results["attachment_identity"])
        )

    def test_04_existing_04_fetches_and_normalizes_both_skillsheets(self) -> None:
        self.assertEqual(2, self.report["skillsheet_output"])
        self.assertEqual(2, self.report["normalized_skillsheet_output"])
        self.assertTrue(
            all(record["success"] is True for record in self.results["fetch_skillsheet"])
        )
        self.assertTrue(
            all(record["source"] == "attachment" for record in self.results["fetch_skillsheet"])
        )
        self.assertTrue(
            all(record["clean_char_count"] > 0 for record in self.results["normalize_skillsheet"])
        )

    def test_05_skillsheet_content_does_not_cross_items(self) -> None:
        self.assertEqual(2, self.report["skillsheet_content_mapping"])
        self.assertEqual(0, self.report["attachment_cross_contamination"])
        self.assertEqual(0, self.report["skillsheet_cross_contamination"])
        self.assertTrue(
            all(
                record["own_content_marker_found"]
                and not record["foreign_content_marker_found"]
                for record in self.results["attachment_identity"]
            )
        )

    def test_06_derived_message_ids_continue_through_04_and_05(self) -> None:
        stage_records = [
            self.results["cleanup"],
            self.results["classification"],
            self.results["fetch_skillsheet"],
            self.results["normalize_skillsheet"],
        ]
        stage_records.extend(self.results["five_results"].values())
        for records in stage_records:
            self.assertEqual(
                self.expected_ids,
                {record["message_id"] for record in records},
            )
        self.assertTrue(self.report["message_id_continuity"])

    def test_07_all_resource_05_functions_join_both_items(self) -> None:
        self.assertEqual(10, self.report["five_step_count"])
        self.assertEqual(2, self.report["five_records_per_step"])
        self.assertEqual(2, self.report["five_joined_items"])
        self.assertEqual(2, self.report["skillsheet_five_joined_items"])
        self.assertEqual(0, self.report["join_missing"])

    def test_08_resource_05_schema_is_06_compatible(self) -> None:
        self.assertEqual(0, self.report["five_schema_errors"])
        self.assertEqual(0, self.report["normalized_schema_errors"])
        self.assertEqual(0, self.report["resource_text_schema_errors"])
        self.assertTrue(self.report["schema_compatibility"])
        self.assertTrue(self.report["contract_06_ready"])
        for key, records in self.results["five_results"].items():
            self.assertTrue(
                all(FIVE_REQUIRED_KEYS[key] <= set(record) for record in records)
            )

    def test_09_original_gmail_id_is_never_a_stage_join_key(self) -> None:
        self.assertEqual(0, self.report["original_id_join_key_uses"])
        self.assertTrue(all(message_id.startswith("mi_") for message_id in self.expected_ids))

    def test_10_derived_identity_has_no_duplicate(self) -> None:
        self.assertEqual(2, len(self.expected_ids))
        self.assertEqual(0, self.report["duplicate_ids"])

    def test_11_item_bodies_and_05_outputs_do_not_cross_items(self) -> None:
        self.assertEqual(0, self.report["body_cross_contamination"])
        self.assertEqual(2, self.report["profile_marker_retained"])
        budgets = {
            record["message_id"]: record["desired_unit_price"]
            for record in self.results["five_results"]["resource_budget"]
        }
        self.assertEqual(2, len(set(budgets.values())))

    def test_12_canonical_subject_original_from_and_cache_identity_hold(self) -> None:
        self.assertEqual(0, self.report["from_subject_collision"])
        self.assertTrue(self.report["success_cache_stable"])
        self.assertTrue(self.report["success_cache_version_subject_change"])

    def test_13_production_is_unchanged_and_unwritten(self) -> None:
        self.assertEqual(0, self.report["production_changes"])
        self.assertEqual(0, self.report["production_write"])

    def test_14_no_llm_api_external_url_or_06_plus_execution(self) -> None:
        self.assertEqual(0, self.report["llm_api_calls"])
        self.assertEqual(0, self.report["external_url_calls"])
        self.assertTrue(self.report["selective_03_04_05_contract_completed"])
        self.assertFalse(self.report["steps_06_plus_executed"])


if __name__ == "__main__":
    unittest.main()

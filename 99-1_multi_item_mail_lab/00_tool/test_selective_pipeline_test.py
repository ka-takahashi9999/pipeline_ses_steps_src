#!/usr/bin/env python3
"""Focused tests for the 99-1 selective pipeline compatibility run."""

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

    def test_derived_two_cleanup_two(self) -> None:
        self.assertEqual(2, self.report["derived_input"])
        self.assertEqual(2, self.report["cleanup_output"])

    def test_derived_message_ids_continue_through_completed_stages(self) -> None:
        for key in ("cleanup", "classification"):
            self.assertEqual(
                self.expected_ids,
                {record["message_id"] for record in self.results[key]},
            )
        self.assertTrue(self.report["message_id_continuity"])

    def test_existing_02_1_exposes_resource_classification_blocker(self) -> None:
        self.assertEqual("FAIL", self.report["result"])
        self.assertEqual("02-1", self.report["blocking_stage"])
        self.assertEqual(0, self.report["resource_output"])
        self.assertEqual(1, self.report["project_classified"])
        self.assertEqual(1, self.report["ambiguous_classified"])

    def test_completed_stage_join_has_no_missing_or_duplicate(self) -> None:
        self.assertEqual(0, self.report["join_missing"])
        self.assertEqual(0, self.report["duplicate_ids"])

    def test_item_bodies_do_not_cross_contaminate(self) -> None:
        self.assertEqual(0, self.report["body_cross_contamination"])
        self.assertEqual(2, self.report["profile_marker_retained"])

    def test_derived_attachments_are_isolated_before_04(self) -> None:
        self.assertEqual(2, self.report["attachment_identity_distinct"])
        self.assertEqual(0, self.report["attachment_cross_contamination"])
        self.assertTrue(
            all(record["attachment_count"] == 1 for record in self.results["attachment_identity"])
        )

    def test_stop_condition_prevents_03_04_05_execution(self) -> None:
        self.assertFalse(self.report["steps_03_04_05_executed"])
        self.assertEqual([], self.results["fetch_skillsheet"])
        self.assertEqual([], self.results["normalize_skillsheet"])

    def test_no_05_schema_is_claimed_after_02_1_failure(self) -> None:
        self.assertEqual(0, self.report["five_step_count"])
        self.assertEqual(set(FIVE_REQUIRED_KEYS), set(self.results["five_results"]))
        self.assertTrue(all(not records for records in self.results["five_results"].values()))

    def test_success_cache_identity_contract_still_holds(self) -> None:
        self.assertEqual(0, self.report["from_subject_collision"])
        self.assertTrue(self.report["success_cache_stable"])
        self.assertTrue(self.report["success_cache_version_subject_change"])

    def test_06_contract_is_not_claimed_ready(self) -> None:
        self.assertFalse(self.report["contract_06_ready"])
        self.assertFalse(self.report["steps_06_plus_executed"])

    def test_production_is_unchanged_and_unwritten(self) -> None:
        self.assertEqual(0, self.report["production_changes"])
        self.assertEqual(0, self.report["production_write"])

    def test_no_llm_api_or_external_url_calls(self) -> None:
        self.assertEqual(0, self.report["llm_api_calls"])
        self.assertEqual(0, self.report["external_url_calls"])


if __name__ == "__main__":
    unittest.main()

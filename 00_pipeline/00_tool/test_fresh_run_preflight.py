"""Focused offline tests for Fresh Run stale-local classification."""

import copy
import sys
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOL_DIR))

import fresh_run_preflight as preflight


def terminal_evidence():
    return {
        "run_id": "sfn-stale-fixture",
        "run_date": "20260901",
        "local_nonterminal_residue": True,
        "step_functions_status": "FAILED",
        "step_functions_redrive_status": "REDRIVABLE",
        "s3_pipeline_status": "FAILED",
        "s3_batch_state": {
            "state": "SAFE_STOPPED",
            "batch_status": "failed",
            "recovery_eligible": False,
            "recovery_state": "SAFE_STOPPED",
            "recovery_batch_id": "batch-recovery-terminal",
        },
        "process_active": False,
    }


class FreshRunPreflightClassificationTest(unittest.TestCase):
    def test_01_local_running_with_remote_terminal_is_stale_and_nonblocking(self):
        evidence = terminal_evidence()
        before = copy.deepcopy(evidence)
        result = preflight.classify_run(evidence)
        self.assertEqual(preflight.CLASS_STALE, result["classification"])
        self.assertFalse(result["blocks_fresh_run"])
        self.assertEqual(before, evidence)

    def test_02_step_functions_running_blocks(self):
        evidence = terminal_evidence()
        evidence["step_functions_status"] = "RUNNING"
        result = preflight.classify_run(evidence)
        self.assertEqual(preflight.CLASS_ACTIVE, result["classification"])
        self.assertIn("step_functions:RUNNING", result["reasons"])

    def test_03_s3_pipeline_status_running_blocks(self):
        evidence = terminal_evidence()
        evidence["s3_pipeline_status"] = "RUNNING"
        result = preflight.classify_run(evidence)
        self.assertEqual(preflight.CLASS_ACTIVE, result["classification"])
        self.assertIn("s3_pipeline_status:RUNNING", result["reasons"])

    def test_04_nonterminal_batch_state_blocks(self):
        evidence = terminal_evidence()
        evidence["s3_batch_state"]["state"] = "SUBMITTED"
        evidence["s3_batch_state"]["batch_status"] = "in_progress"
        evidence["s3_batch_state"]["recovery_state"] = None
        result = preflight.classify_run(evidence)
        self.assertEqual(preflight.CLASS_ACTIVE, result["classification"])
        self.assertIn("s3_batch_state:SUBMITTED", result["reasons"])

    def test_05_safe_stopped_without_process_does_not_block(self):
        result = preflight.classify_run(terminal_evidence())
        self.assertEqual(preflight.CLASS_STALE, result["classification"])
        self.assertTrue(result["terminal_checks"]["systemd_process_absent"])

    def test_06_redrivable_terminal_execution_does_not_block(self):
        evidence = terminal_evidence()
        evidence["step_functions_redrive_status"] = "REDRIVABLE"
        result = preflight.classify_run(evidence)
        self.assertEqual(preflight.CLASS_STALE, result["classification"])
        self.assertFalse(result["blocks_fresh_run"])

    def test_07_active_recovery_state_blocks(self):
        evidence = terminal_evidence()
        evidence["s3_batch_state"]["state"] = "RECOVERY_REQUIRED"
        evidence["s3_batch_state"]["recovery_state"] = "RECOVERY_REQUIRED"
        evidence["s3_batch_state"]["recovery_eligible"] = True
        result = preflight.classify_run(evidence)
        self.assertEqual(preflight.CLASS_ACTIVE, result["classification"])
        self.assertIn("recovery_eligible:true", result["reasons"])

    def test_08_legacy_safe_stopped_derives_recovery_ineligible_with_source(self):
        evidence = terminal_evidence()
        evidence["s3_batch_state"].pop("recovery_eligible")
        evidence["s3_batch_state"].pop("recovery_state")
        evidence["s3_batch_state"].pop("recovery_batch_id")
        result = preflight.classify_run(evidence)
        self.assertEqual(preflight.CLASS_STALE, result["classification"])
        self.assertFalse(result["recovery_eligible"])
        self.assertEqual(
            "terminal_engine_state_default", result["recovery_eligible_source"]
        )

    def test_09_incomplete_terminal_evidence_fails_closed_without_false_active_label(self):
        evidence = terminal_evidence()
        evidence["s3_pipeline_status"] = None
        with self.assertRaises(preflight.PreflightError):
            preflight.classify_run(evidence)


if __name__ == "__main__":
    unittest.main(verbosity=2)

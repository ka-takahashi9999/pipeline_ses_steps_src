#!/usr/bin/env python3
"""Run only the P1-P7 stable lab baseline and save reviewable evidence."""

import sys
import unittest
from pathlib import Path


sys.dont_write_bytecode = True
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.json_utils import write_jsonl


RESULT_PATH = STEP_DIR / "01_result" / "google_sheet_acquisition_prototype" / "baseline_test_result.jsonl"
BASELINE_FILES = (
    "test_variable_item_core.py",
    "test_inline_summary_adapter.py",
    "test_ichi_r_inline_summary_adapter.py",
    "test_esna_inline_summary_adapter.py",
    "test_link_bundle_adapter.py",
    "test_attachment_list_adapter.py",
    "test_archive_adapter.py",
    "test_spreadsheet_adapter.py",
)


def main() -> None:
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for filename in BASELINE_FILES:
        suite.addTests(loader.discover(str(STEP_DIR / "00_tool"), pattern=filename))
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    failure_count = len(result.failures) + len(result.errors)
    evidence = {
        "evidence_schema_version": "GoogleSheetDigestBaselineTestEvidence.v1",
        "baseline_passed": result.testsRun - failure_count,
        "baseline_total": result.testsRun,
        "baseline_status": "PASS" if result.wasSuccessful() else "FAIL",
        "baseline_files": list(BASELINE_FILES),
        "existing_contract_regression": failure_count,
        "pipeline_04_05_runs": 0,
        "google_live_access": 0,
        "production_write": 0,
    }
    write_jsonl(str(RESULT_PATH), [evidence])
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    main()

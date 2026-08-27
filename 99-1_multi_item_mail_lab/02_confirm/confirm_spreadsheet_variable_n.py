#!/usr/bin/env python3
"""Confirm stable P7 XLSX contract and optional saved SAKYA observation."""

import sys
from pathlib import Path
from typing import Any, Dict, List


sys.dont_write_bytecode = True
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
for import_path in (
    PROJECT_ROOT,
    STEP_DIR / "00_tool",
    STEP_DIR / "00_tool" / "adapters" / "spreadsheet",
    STEP_DIR / "00_tool" / "adapters" / "attachment_list",
    STEP_DIR / "00_tool" / "canonicalize",
    STEP_DIR / "00_tool" / "core",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.json_utils import read_jsonl_as_list
from common.logger import get_logger
from canonical_overlay import MAIL_MASTER_KEYS
from run_spreadsheet_offline_replay import (
    RESULT_SUBDIR,
    build_spreadsheet_contract_results,
    build_spreadsheet_results,
)


logger = get_logger("confirm_99-1_spreadsheet_variable_n")
RESULT_DIR = STEP_DIR / "01_result" / RESULT_SUBDIR


def _check(condition: bool, message: str, failures: List[str]) -> None:
    if not condition:
        failures.append(message)
        logger.error(message)


def _read(filename: str) -> List[Dict[str, Any]]:
    path = RESULT_DIR / filename
    if not path.exists():
        raise FileNotFoundError("missing P7 result:" + str(path))
    return read_jsonl_as_list(str(path))


def main() -> None:
    failures: List[str] = []
    contract = build_spreadsheet_contract_results()
    contract_summary = contract["summary"]
    _check(contract_summary.get("contract_status") == "PASS", "stable contract failed", failures)
    _check(contract_summary.get("variable_n") == [0, 1, 2, 4, 10], "variable-N authority mismatch", failures)
    _check(contract_summary.get("observed_variable_n") == [0, 1, 2, 4, 10], "variable-N observed mismatch", failures)
    _check(contract_summary.get("finding_count") == 0, "stable findings must be zero", failures)
    _check(contract_summary.get("formula_evaluation_count") == 0, "formula evaluation must be zero", failures)
    _check(contract_summary.get("external_resolution_count") == 0, "external resolution must be zero", failures)
    _check(contract_summary.get("macro_execution_count") == 0, "macro execution must be zero", failures)

    n4 = contract["results"][3]
    _check(len(n4.record_occurrences) == 6, "derived-view occurrence count mismatch", failures)
    _check(len(n4.items) == 4, "derived-view canonical count mismatch", failures)
    _check(n4.workbook.get("sheet_role_counts", {}).get("SUPPORTING") == 1, "supporting role mismatch", failures)
    _check(n4.workbook.get("sheet_role_counts", {}).get("DERIVED_VIEW") == 1, "derived role mismatch", failures)

    actual = build_spreadsheet_results()
    summary = actual["summary"]
    _check(
        summary.get("actual_availability")
        in {"OBSERVATION", "DATA_UNAVAILABLE", "OBSERVATION_UNAVAILABLE"},
        "actual availability status mismatch",
        failures,
    )
    _check(summary.get("actual_runtime_fixed_oracle") == 0, "actual fixed oracle must be zero", failures)
    _check(summary.get("actual_eligible") == 0, "actual must not be eligible", failures)
    _check(summary.get("actual_auto_union") is False, "actual auto-union must be disabled", failures)
    _check(summary.get("production_write") == 0, "production artifacts changed", failures)
    if summary.get("actual_availability") == "OBSERVATION":
        _check(
            summary.get("actual_source_acquisition_status") == "UNVERIFIED",
            "actual source acquisition must remain UNVERIFIED",
            failures,
        )

    source_rows = _read("source_audit.jsonl")
    sheet_rows = _read("sheet_audit.jsonl")
    occurrence_rows = _read("record_occurrences.jsonl")
    technical_rows = _read("technical_items.jsonl")
    technical_overlays = _read("technical_mail_master.jsonl")
    eligible_rows = _read("eligible_items.jsonl")
    eligible_overlays = _read("eligible_mail_master.jsonl")
    summary_rows = _read("replay_summary.jsonl")
    _check(len(summary_rows) == 1, "replay summary count mismatch", failures)
    persisted = summary_rows[0] if summary_rows else {}
    _check(len(source_rows) == persisted.get("actual_observation_count"), "source count mismatch", failures)
    _check(len(sheet_rows) == persisted.get("actual_observed_sheets"), "sheet count mismatch", failures)
    _check(
        len(occurrence_rows) == persisted.get("actual_observed_record_occurrences"),
        "record occurrence count mismatch",
        failures,
    )
    _check(
        len(technical_rows) == persisted.get("actual_observed_canonical_candidates"),
        "technical candidate count mismatch",
        failures,
    )
    _check(len(technical_overlays) == len(technical_rows), "technical overlay count mismatch", failures)
    _check(len(eligible_rows) == persisted.get("actual_eligible"), "eligible count mismatch", failures)
    _check(len(eligible_overlays) == len(eligible_rows), "eligible overlay count mismatch", failures)
    _check(
        all(set(row) == MAIL_MASTER_KEYS for row in technical_overlays),
        "canonical overlay schema mismatch",
        failures,
    )

    contract_status = "FAIL" if failures else "PASS"
    logger.info(
        "CONTRACT TESTS: "
        + contract_status
        + " variable_N=0/1/2/4/10 derived_occurrences=6 canonical=4 supporting=1"
    )
    logger.info(
        "ACTUAL OBSERVATIONS: "
        + str(summary.get("actual_availability"))
        + " source_acquisition="
        + str(summary.get("actual_source_acquisition_status"))
        + " technical="
        + str(summary.get("actual_technical_workbook_status"))
        + " sheets="
        + str(summary.get("actual_observed_sheets"))
        + " occurrences="
        + str(summary.get("actual_observed_record_occurrences"))
        + " canonical="
        + str(summary.get("actual_observed_canonical_candidates"))
        + " duplicate_groups="
        + str(summary.get("actual_observed_duplicate_groups"))
        + " eligible="
        + str(summary.get("actual_eligible"))
    )
    for sheet in sheet_rows[:3]:
        logger.info(
            "representative: sheet="
            + str(sheet.get("name"))
            + " state="
            + str(sheet.get("state"))
            + " role="
            + str(sheet.get("role"))
            + " dimensions="
            + str(sheet.get("max_row"))
            + "x"
            + str(sheet.get("max_column"))
            + " occurrences="
            + str(sheet.get("record_occurrence_count"))
        )
    if failures:
        logger.error("P7 confirm NG: failures=" + str(len(failures)))
        raise SystemExit(1)
    logger.ok(
        "P7 confirm OK: contract=PASS actual="
        + str(summary.get("actual_availability"))
        + " fixed_actual_oracle=0 production_write=0"
    )


if __name__ == "__main__":
    main()

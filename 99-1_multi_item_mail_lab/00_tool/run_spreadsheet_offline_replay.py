#!/usr/bin/env python3
"""Run the stable P7 XLSX contract and optional saved SAKYA observation."""

import copy
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List


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

from common.file_utils import ensure_result_dirs
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger
from canonical_overlay import build_canonical_overlay
from run_offline_replay import DEFAULT_INPUT
from run_selective_pipeline_test import _production_artifact_snapshot
from spreadsheet_fixture_source import build_fixture_records
from spreadsheet_parser import SpreadsheetParser


logger = get_logger("99-1_spreadsheet_variable_n")
CONFIG_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "companies"
    / "sakya_spreadsheet.config.json.example"
)
FIXTURE_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "fixtures"
    / "redacted"
    / "spreadsheet"
    / "sakya.variable_n.fixture.jsonl.example"
)
RESULT_SUBDIR = "spreadsheet_variable_n"


def _config() -> Dict[str, Any]:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def build_spreadsheet_contract_results() -> Dict[str, Any]:
    config = _config()
    parser = SpreadsheetParser(config)
    fixtures = build_fixture_records(FIXTURE_PATH, config)
    first = [parser.parse(copy.deepcopy(fixture)) for fixture in fixtures]
    repeated = [parser.parse(copy.deepcopy(fixture)) for fixture in fixtures]
    observed_n = [result.eligible_item_candidate_count for result in first]
    findings: List[str] = []
    if observed_n != [0, 1, 2, 4, 10]:
        findings.append("stable_variable_n_mismatch")
    if any(result.status != "PARSED" for result in first):
        findings.append("stable_fixture_parse_mismatch")
    if first != repeated:
        findings.append("stable_fixture_idempotency_mismatch")
    if any(
        result.workbook.get("formula_evaluation_count") != 0
        or result.workbook.get("external_resolution_count") != 0
        or result.workbook.get("macro_execution_count") != 0
        for result in first
    ):
        findings.append("stable_fixture_external_or_execution_side_effect")
    n4 = first[3]
    if (
        len(n4.record_occurrences) != 6
        or len(n4.items) != 4
        or n4.workbook.get("sheet_role_counts")
        != {
            "AUTHORITATIVE": 1,
            "DERIVED_VIEW": 1,
            "SUPPORTING": 1,
            "UNKNOWN": 0,
        }
    ):
        findings.append("stable_derived_or_supporting_contract_mismatch")
    return {
        "fixtures": fixtures,
        "results": first,
        "summary": {
            "contract_status": "PASS" if not findings else "FAIL",
            "fixture_source_count": len(fixtures),
            "variable_n": [0, 1, 2, 4, 10],
            "observed_variable_n": observed_n,
            "finding_count": len(findings),
            "findings": findings,
            "formula_evaluation_count": 0,
            "external_resolution_count": 0,
            "macro_execution_count": 0,
        },
    }


def build_spreadsheet_results(
    records: Iterable[Dict[str, Any]] = None,
    input_path: Path = DEFAULT_INPUT,
) -> Dict[str, Any]:
    parser = SpreadsheetParser.from_file(CONFIG_PATH)
    observation_findings: List[str] = []
    if records is not None:
        source_records = list(records)
        input_observable = True
    else:
        try:
            source_records = read_jsonl_as_list(str(input_path))
            input_observable = True
        except Exception as error:
            source_records = []
            input_observable = False
            observation_findings.append(
                "source_read_exception:" + type(error).__name__
            )
    selected = sorted(
        (record for record in source_records if parser.matches(record)),
        key=lambda record: str(record.get("message_id", "")),
    )
    production_before = _production_artifact_snapshot()
    results = []
    for record in selected:
        result = parser.parse(record)
        results.append((record, result))
        if result.status == "SYSTEM_FAILURE":
            observation_findings.append(
                "parser_system_failure:" + str(record.get("message_id", ""))
            )
    production_after = _production_artifact_snapshot()
    production_write = int(production_before != production_after)
    if production_write:
        observation_findings.append("production_artifact_changed")

    source_audit: List[Dict[str, Any]] = []
    sheet_audit: List[Dict[str, Any]] = []
    occurrences: List[Dict[str, Any]] = []
    technical_items: List[Dict[str, Any]] = []
    eligible_items: List[Dict[str, Any]] = []
    for record, result in results:
        source_id = str(record.get("message_id", ""))
        source_audit.append(
            {
                "original_message_id": source_id,
                "parse_status": result.status,
                "source": result.source,
                "workbook": result.workbook,
                "reasons": result.reasons,
            }
        )
        sheet_audit.extend(
            {"original_message_id": source_id, **sheet} for sheet in result.sheets
        )
        occurrences.extend(
            {"original_message_id": source_id, **row}
            for row in result.record_occurrences
        )
        technical_items.extend(result.technical_items)
        eligible_items.extend(result.items)
    technical_overlays = [
        build_canonical_overlay(record, item)
        for record, result in results
        for item in result.technical_items
    ]
    eligible_overlays = [
        build_canonical_overlay(record, item)
        for record, result in results
        for item in result.items
    ]
    acquisition_states = {
        result.source.get("source_acquisition_status") for _, result in results
    }
    technical_states = {
        result.workbook.get("technical_workbook_status") for _, result in results
    }
    role_counts = Counter(
        sheet.get("role") for _, result in results for sheet in result.sheets
    )
    reconciliation_rows = [
        result.workbook.get("reconciliation", {}) for _, result in results
    ]
    summary = {
        "actual_availability": (
            "OBSERVATION_UNAVAILABLE"
            if not input_observable
            else ("OBSERVATION" if selected else "DATA_UNAVAILABLE")
        ),
        "actual_observation_count": len(selected),
        "actual_source_acquisition_status": (
            next(iter(acquisition_states)) if len(acquisition_states) == 1 else "N/A"
        ),
        "actual_technical_workbook_status": (
            next(iter(technical_states)) if len(technical_states) == 1 else "N/A"
        ),
        "actual_observed_sheets": len(sheet_audit),
        "actual_authoritative_sheets": role_counts["AUTHORITATIVE"],
        "actual_derived_view_sheets": role_counts["DERIVED_VIEW"],
        "actual_supporting_sheets": role_counts["SUPPORTING"],
        "actual_unknown_sheets": role_counts["UNKNOWN"],
        "actual_observed_record_occurrences": len(occurrences),
        "actual_observed_canonical_candidates": len(technical_items),
        "actual_observed_distinct_fingerprints": sum(
            row.get("distinct_fingerprint_count", 0) for row in reconciliation_rows
        ),
        "actual_observed_duplicate_occurrences": sum(
            row.get("duplicate_occurrence_count", 0) for row in reconciliation_rows
        ),
        "actual_observed_duplicate_groups": sum(
            row.get("duplicate_group_count", 0) for row in reconciliation_rows
        ),
        "actual_observed_ambiguous_groups": sum(
            row.get("ambiguous_group_count", 0) for row in reconciliation_rows
        ),
        "actual_eligible": len(eligible_items),
        "actual_auto_union": any(
            result.source.get("auto_union_eligible") is True for _, result in results
        ),
        "actual_runtime_fixed_oracle": 0,
        "actual_observation_finding_count": len(observation_findings),
        "actual_observation_findings": observation_findings,
        "formula_evaluation_count": 0,
        "external_resolution_count": 0,
        "macro_execution_count": 0,
        "llm_api_calls": 0,
        "production_changes": 0,
        "production_write": production_write,
    }
    return {
        "source_audit": source_audit,
        "sheet_audit": sheet_audit,
        "record_occurrences": occurrences,
        "technical_items": technical_items,
        "technical_mail_master": technical_overlays,
        "eligible_items": eligible_items,
        "eligible_mail_master": eligible_overlays,
        "summary": summary,
    }


def write_spreadsheet_results(results: Dict[str, Any]) -> None:
    result_dirs = ensure_result_dirs(str(STEP_DIR))
    result_dir = result_dirs["result"] / RESULT_SUBDIR
    result_dir.mkdir(parents=True, exist_ok=True)
    for filename, key in (
        ("source_audit.jsonl", "source_audit"),
        ("sheet_audit.jsonl", "sheet_audit"),
        ("record_occurrences.jsonl", "record_occurrences"),
        ("technical_items.jsonl", "technical_items"),
        ("technical_mail_master.jsonl", "technical_mail_master"),
        ("eligible_items.jsonl", "eligible_items"),
        ("eligible_mail_master.jsonl", "eligible_mail_master"),
    ):
        write_jsonl(str(result_dir / filename), results[key])
    write_jsonl(str(result_dir / "replay_summary.jsonl"), [results["summary"]])


def main() -> None:
    contract = build_spreadsheet_contract_results()
    if contract["summary"]["contract_status"] != "PASS":
        raise ValueError(
            "P7 stable contract failed:"
            + ";".join(contract["summary"]["findings"])
        )
    results = build_spreadsheet_results()
    results["summary"]["contract_status"] = contract["summary"]["contract_status"]
    results["summary"]["contract_variable_n"] = contract["summary"]["variable_n"]
    results["summary"]["contract_finding_count"] = contract["summary"]["finding_count"]
    write_spreadsheet_results(results)
    logger.ok(
        "P7 spreadsheet contract PASS: variable_N=0/1/2/4/10 actual="
        + results["summary"]["actual_availability"]
        + " observed_sources="
        + str(results["summary"]["actual_observation_count"])
        + " eligible="
        + str(results["summary"]["actual_eligible"])
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Confirm exact digest/schema artifacts and saved test evidence."""

import copy
import sys
from pathlib import Path
from typing import Any, Dict, List


sys.dont_write_bytecode = True
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
ACQUISITION_DIR = STEP_DIR / "00_tool" / "acquisition"
for import_path in (PROJECT_ROOT, ACQUISITION_DIR, STEP_DIR / "00_tool"):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger
from google_sheet_acquisition_contract import (
    MANIFEST_FIELDS,
    SNAPSHOT_ENTRY_FIELDS,
    calculate_entry_raw_digest,
    offline_negative_proofs,
    validate_attempt_plan,
    validate_manifest,
    validate_profile_registry,
)
from run_google_sheet_acquisition_prototype import (
    DIGEST_SCHEMA_NEGATIVE_CASE_IDS,
    RESULT_DIR,
    _strict_implementation_pass,
)


logger = get_logger("confirm_99-1_google_sheet_acquisition_prototype")
CONFIRM_DIR = STEP_DIR / "02_confirm" / "google_sheet_acquisition_prototype"


def _one(filename: str) -> Dict[str, Any]:
    rows = read_jsonl_as_list(str(RESULT_DIR / filename))
    if len(rows) != 1:
        raise ValueError(filename + " must contain exactly one JSONL record")
    return rows[0]


def _check(condition: bool, reason: str, failures: List[str]) -> None:
    if not condition:
        failures.append(reason)
        logger.error(reason)


def main() -> None:
    failures: List[str] = []
    registry = _one("profile_registry.jsonl")
    plan = _one("attempt_plan.jsonl")
    manifest = _one("acquisition_manifest.jsonl")
    summary = _one("prototype_summary.jsonl")
    focused = _one("focused_test_result.jsonl")
    baseline = _one("baseline_test_result.jsonl")
    snapshot_rows = read_jsonl_as_list(str(RESULT_DIR / "snapshot_entries.jsonl"))
    conformance_rows = read_jsonl_as_list(str(RESULT_DIR / "digest_schema_conformance.jsonl"))

    _check(not validate_profile_registry(registry), "Profile validation failed", failures)
    _check(not validate_attempt_plan(plan), "Attempt Plan validation failed", failures)
    _check(len(registry.get("profiles", [])) == 1, "Profile count mismatch", failures)
    _check(plan.get("attempt_ordinal") == 1, "Attempt count mismatch", failures)
    planned = plan.get("planned_container_set", {}).get("planned_container_entries", [])
    _check(len(planned) == 1, "planned Container count mismatch", failures)
    _check(len(manifest) == 15 and set(manifest) == set(MANIFEST_FIELDS), "Manifest exact 15-field schema mismatch", failures)
    _check(len(snapshot_rows) == 1, "Snapshot count must equal one", failures)
    if snapshot_rows:
        _check(
            len(snapshot_rows[0]) == 18 and set(snapshot_rows[0]) == set(SNAPSHOT_ENTRY_FIELDS),
            "SnapshotEntry exact 18-field schema mismatch",
            failures,
        )
    _check(manifest.get("snapshot_count") == len(snapshot_rows), "snapshot_count mismatch", failures)

    raw_entries: Dict[str, bytes] = {}
    for entry in snapshot_rows:
        path = RESULT_DIR / entry["raw_artifact_ref"]
        _check(path.exists(), "Snapshot file missing", failures)
        if path.exists():
            payload = path.read_bytes()
            raw_entries[entry["entry_id"]] = payload
            _check(
                calculate_entry_raw_digest(payload) == entry.get("entry_raw_digest"),
                "ENTRY_RAW_DIGEST mismatch",
                failures,
            )
            _check(len(payload) == entry.get("byte_length"), "Snapshot byte_length mismatch", failures)

    validation = validate_manifest(manifest, registry, plan, raw_entries)
    _check(validation.get("valid") is True, "Manifest validation failed", failures)
    _check(validation.get("exact_manifest_schema") is True, "Manifest exact schema failed", failures)
    _check(validation.get("exact_snapshot_entry_schema") is True, "SnapshotEntry exact schema failed", failures)
    _check(validation.get("digest_conformance") is True, "Digest conformance failed", failures)
    _check(validation.get("acquisition_status") == "UNVERIFIED", "Acquisition must remain UNVERIFIED", failures)
    _check(validation.get("review_status") == "HUMAN_REVIEW", "presentation review status mismatch", failures)
    _check(validation.get("eligible") == 0, "eligible must be zero", failures)

    negative_cases = offline_negative_proofs(manifest, registry, plan, raw_entries)
    expected = {
        "profile_digest_mismatch": ("UNVERIFIED", "profile_digest_mismatch"),
        "planned_scope_mismatch": ("PARTIAL", "planned_scope_mismatch"),
        "required_container_missing": ("PARTIAL", "required_container_missing"),
        "strong_version_unavailable": ("UNVERIFIED", "strong_version_unavailable"),
        "revision_drift": ("SNAPSHOT_UNSTABLE", "revision_drift"),
        "range_gap": ("PARTIAL", "range_gap"),
        "digest_mismatch": ("INCOMPLETE", "snapshot_entry_digest_mismatch"),
        "presentation_unresolved": ("UNVERIFIED", "presentation_unresolved"),
        "attempt_uncommitted": ("INCOMPLETE", "attempt_uncommitted"),
    }
    negative_pass = 0
    for case in negative_cases:
        status, reason = expected[case["name"]]
        passed = (
            case["result"]["acquisition_status"] == status
            and reason in case["result"]["reasons"]
            and case["result"]["eligible"] == 0
        )
        if case["name"] == "presentation_unresolved":
            passed = passed and case["result"]["review_status"] == "HUMAN_REVIEW"
        negative_pass += int(passed)
    _check(len(negative_cases) == 9, "original negative case count mismatch", failures)
    _check(
        {case["name"] for case in negative_cases} == set(expected),
        "original negative case IDs mismatch",
        failures,
    )
    _check(negative_pass == 9, "original negative proof count mismatch", failures)

    conformance_pass = sum(row.get("passed") is True for row in conformance_rows)
    _check(conformance_rows and conformance_pass == len(conformance_rows), "saved conformance proof failed", failures)
    _check(focused.get("focused_status") == "PASS", "focused test evidence failed", failures)
    _check(
        focused.get("focused_passed") == 14 and focused.get("focused_total") == 14,
        "focused test count mismatch",
        failures,
    )
    individual_cases = focused.get("digest_schema_negative_cases", [])
    if not isinstance(individual_cases, list):
        individual_cases = []
    individual_passed = sum(
        case.get("passed") is True for case in individual_cases
        if isinstance(case, dict)
    )
    individual_case_ids = [
        case.get("case_id") for case in individual_cases
        if isinstance(case, dict)
    ]
    _check(
        len(individual_cases) == 21,
        "digest/schema individual negative case count mismatch",
        failures,
    )
    _check(
        individual_passed == len(individual_cases),
        "digest/schema individual negative case failed",
        failures,
    )
    _check(
        set(individual_case_ids) == set(DIGEST_SCHEMA_NEGATIVE_CASE_IDS)
        and len(individual_case_ids) == len(set(individual_case_ids)),
        "digest/schema individual negative case IDs mismatch",
        failures,
    )
    _check(
        all(
            isinstance(case, dict)
            and "expected" in case
            and "actual" in case
            and "passed" in case
            for case in individual_cases
        ),
        "digest/schema individual evidence fields missing",
        failures,
    )
    _check(
        focused.get("digest_schema_negative_passed") == individual_passed
        and focused.get("digest_schema_negative_total") == len(individual_cases)
        and individual_passed == 21,
        "digest/schema negative aggregate is not derived from individual evidence",
        failures,
    )
    _check(
        focused.get("false_pass_prevention") == "PASS",
        "false PASS prevention evidence failed",
        failures,
    )
    false_pass_check = focused.get("false_pass_check", {})
    _check(
        false_pass_check.get("mutated_case_passed") is False
        and false_pass_check.get("strict_pass") is False
        and false_pass_check.get("passed") is True,
        "false PASS in-memory check details failed",
        failures,
    )
    strict_positive = _strict_implementation_pass(
        validation, conformance_rows, negative_pass, len(negative_cases), focused
    )
    failed_focused = copy.deepcopy(focused)
    if failed_focused.get("digest_schema_negative_cases"):
        failed_focused["digest_schema_negative_cases"][0]["passed"] = False
    strict_after_failed_case = _strict_implementation_pass(
        validation, conformance_rows, negative_pass, len(negative_cases), failed_focused
    )
    _check(strict_positive is True, "strict PASS positive control failed", failures)
    _check(
        strict_after_failed_case is False,
        "strict PASS accepted one failed digest/schema case",
        failures,
    )
    _check(baseline.get("baseline_status") == "PASS", "baseline evidence failed", failures)
    _check(
        baseline.get("baseline_passed") == 195 and baseline.get("baseline_total") == 195,
        "baseline count mismatch",
        failures,
    )
    _check(baseline.get("existing_contract_regression") == 0, "existing contract regression detected", failures)
    _check(baseline.get("pipeline_04_05_runs") == 0, "04/05 must not run", failures)
    _check(summary.get("implementation") == "PASS", "strict Prototype implementation failed", failures)
    _check(summary.get("prototype_pass_condition") == "STRICT", "old Prototype PASS condition remains", failures)
    _check(summary.get("google_live_access") == 0, "unexpected Google live access", failures)
    _check(summary.get("attempt_state") == "COMMITTED", "Attempt must remain COMMITTED", failures)
    _check(summary.get("eligible") == 0, "summary eligible must be zero", failures)
    _check(summary.get("candidate_emission") == 0, "candidate emission must be zero", failures)
    _check(summary.get("manifest_validation") == "PASS", "summary Manifest validation failed", failures)
    _check(summary.get("canonical_conformance") == "PASS", "summary canonical conformance failed", failures)
    _check(summary.get("digest_conformance") == "PASS", "summary digest conformance failed", failures)
    _check(summary.get("golden") == "PASS", "summary golden evidence failed", failures)
    _check(
        summary.get("original_negative_proofs") == {"passed": 9, "total": 9},
        "summary original negative gate failed",
        failures,
    )
    _check(
        summary.get("digest_schema_negative") == {"passed": 21, "total": 21},
        "summary digest/schema negative gate failed",
        failures,
    )
    _check(
        summary.get("focused") == {"passed": 14, "total": 14},
        "summary focused gate failed",
        failures,
    )
    _check(
        summary.get("false_pass_prevention") == "PASS",
        "summary false PASS prevention gate failed",
        failures,
    )
    _check(summary.get("production_write") == 0, "production write must be zero", failures)
    _check(summary.get("P8") == "NONE", "P8 must remain NONE", failures)

    report = {
        "confirm_status": "FAIL" if failures else "PASS",
        "manifest_field_count": len(manifest),
        "snapshot_entry_field_count": len(snapshot_rows[0]) if snapshot_rows else 0,
        "canonical_schema_digest_conformance_passed": conformance_pass,
        "canonical_schema_digest_conformance_total": len(conformance_rows),
        "original_negative_passed": negative_pass,
        "original_negative_total": 9,
        "digest_schema_negative_passed": focused.get("digest_schema_negative_passed", 0),
        "digest_schema_negative_total": focused.get("digest_schema_negative_total", 0),
        "digest_schema_individual_case_count": len(individual_cases),
        "digest_schema_all_cases_passed": (
            bool(individual_cases) and individual_passed == len(individual_cases)
        ),
        "false_pass_prevention": focused.get("false_pass_prevention", "FAIL"),
        "false_pass_check": false_pass_check,
        "strict_positive_control": strict_positive,
        "strict_after_failed_case": strict_after_failed_case,
        "focused_passed": focused.get("focused_passed", 0),
        "focused_total": focused.get("focused_total", 0),
        "baseline_passed": baseline.get("baseline_passed", 0),
        "baseline_total": baseline.get("baseline_total", 0),
        "pipeline_04_05_runs": 0,
        "google_live_access": 0,
        "acquisition_status": validation.get("acquisition_status"),
        "attempt_state": "COMMITTED",
        "eligible": 0,
        "candidate_emission": 0,
        "production_write": 0,
        "failure_count": len(failures),
        "failures": failures,
    }
    write_jsonl(str(CONFIRM_DIR / "confirm_report.jsonl"), [report])
    if failures:
        logger.error("Google Sheet digest conformance confirm NG")
        raise SystemExit(1)
    logger.ok(
        "Google Sheet digest conformance confirm OK: focused="
        + str(report["focused_passed"]) + "/" + str(report["focused_total"])
        + " negative=9/9 digest_schema="
        + str(report["digest_schema_negative_passed"]) + "/"
        + str(report["digest_schema_negative_total"])
        + " individual=" + str(report["digest_schema_individual_case_count"])
        + " all_cases_passed=" + str(report["digest_schema_all_cases_passed"]).lower()
        + " baseline=" + str(report["baseline_passed"]) + "/"
        + str(report["baseline_total"])
        + " live=0 eligible=0 production_write=0"
    )


if __name__ == "__main__":
    main()

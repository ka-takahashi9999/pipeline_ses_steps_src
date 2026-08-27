#!/usr/bin/env python3
"""Confirm the one-provider Google Sheet acquisition prototype artifacts."""

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
    digest_bytes,
    offline_negative_proofs,
    validate_attempt_plan,
    validate_manifest,
    validate_profile_registry,
)
from run_google_sheet_acquisition_prototype import RESULT_DIR


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
    snapshot_rows = read_jsonl_as_list(str(RESULT_DIR / "snapshot_entries.jsonl"))
    _check(len(registry.get("profiles", [])) == 1, "Profile count mismatch", failures)
    _check(plan.get("attempt_ordinal") == 1, "Attempt count mismatch", failures)
    _check(len(plan.get("planned_containers", [])) == 1, "planned Container count mismatch", failures)
    _check(len(snapshot_rows) in {0, 1}, "Snapshot count exceeds prototype cap", failures)
    _check(summary.get("candidate_emission") == 0, "candidate emission must be zero", failures)
    _check(summary.get("eligible") == 0, "eligible must be zero", failures)
    _check(summary.get("auto_union") is False, "auto-union must be false", failures)
    _check(summary.get("actual_fixed_oracle") == 0, "actual fixed oracle must be zero", failures)
    _check(summary.get("production_write") == 0, "production write must be zero", failures)
    _check(summary.get("P8") == "NONE", "P8 must remain NONE", failures)
    _check(not validate_profile_registry(registry), "Profile validation failed", failures)
    _check(not validate_attempt_plan(plan), "Attempt Plan validation failed", failures)

    raw_entries: Dict[str, bytes] = {}
    for entry in snapshot_rows:
        path = RESULT_DIR / entry["relative_path"]
        _check(path.exists(), "Snapshot file missing", failures)
        if path.exists():
            payload = path.read_bytes()
            raw_entries[entry["snapshot_entry_id"]] = payload
            _check(
                digest_bytes(payload) == entry.get("entry_raw_digest"),
                "Snapshot ENTRY_RAW_DIGEST mismatch",
                failures,
            )
            _check(len(payload) == entry.get("byte_count"), "Snapshot byte count mismatch", failures)

    validation = validate_manifest(manifest, registry, plan, raw_entries)
    _check(validation.get("valid") is True, "Manifest integrity validation failed", failures)
    _check(validation.get("eligible") == 0, "Manifest eligible must be zero", failures)
    _check(
        validation.get("acquisition_status") == manifest.get("acquisition_status"),
        "Manifest completeness status mismatch",
        failures,
    )
    access = manifest.get("access_status")
    _check(access in {"SUCCESS", "AUTH_REQUIRED"}, "access must be SUCCESS or AUTH_REQUIRED", failures)
    if access == "SUCCESS":
        _check(len(snapshot_rows) == 1, "successful access requires one Snapshot", failures)
        observation = manifest.get("observation", {})
        _check(observation.get("workbook_tab_inventory") == "AVAILABLE", "tab inventory unavailable", failures)
        _check(observation.get("range_bounds") == "AVAILABLE", "range/bounds unavailable", failures)
        _check(
            observation.get("presentation_metadata", {}).get("overall_availability")
            in {"AVAILABLE", "PARTIAL"},
            "presentation metadata unavailable",
            failures,
        )

    negative_results = offline_negative_proofs(
        manifest, registry, plan, raw_entries
    )
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
    for case in negative_results:
        name = case["name"]
        result = case["result"]
        status, reason = expected[name]
        passed = (
            result.get("acquisition_status") == status
            and reason in result.get("reasons", [])
            and result.get("eligible") == 0
        )
        if name == "presentation_unresolved":
            passed = passed and result.get("review_status") == "HUMAN_REVIEW"
        if passed:
            negative_pass += 1
        else:
            failures.append("negative proof failed:" + name)
    _check(negative_pass == 9, "negative proof count mismatch", failures)

    report = {
        "confirm_status": "FAIL" if failures else "PASS",
        "profile_count": len(registry.get("profiles", [])),
        "attempt_count": 1,
        "planned_container_count": len(plan.get("planned_containers", [])),
        "snapshot_count": len(snapshot_rows),
        "manifest_count": 1,
        "negative_proofs_pass": negative_pass,
        "negative_proofs_total": 9,
        "acquisition_status": manifest.get("acquisition_status"),
        "eligible": 0,
        "candidate_emission": 0,
        "auto_union": False,
        "production_write": 0,
        "failure_count": len(failures),
        "failures": failures,
    }
    write_jsonl(str(CONFIRM_DIR / "confirm_report.jsonl"), [report])
    if failures:
        logger.error("Google Sheet acquisition prototype confirm NG")
        raise SystemExit(1)
    logger.ok(
        "Google Sheet acquisition prototype confirm OK: negative=9/9 eligible=0 candidate=0 production_write=0"
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Confirm 99-1 derived mails pass 01-4 and 02-1 as resources."""

import sys
from pathlib import Path
from typing import Any, Dict, List


sys.dont_write_bytecode = True
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.json_utils import read_jsonl_as_list
from common.logger import get_logger


logger = get_logger("confirm_99-1_selective_pipeline_test")
RESULT_DIR = STEP_DIR / "01_result" / "selective_pipeline_test"


def _check(condition: bool, message: str, failures: List[str]) -> None:
    if not condition:
        failures.append(message)
        logger.error(message)


def _read(filename: str) -> List[Dict[str, Any]]:
    path = RESULT_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"missing selective result: {path}")
    return read_jsonl_as_list(str(path))


def main() -> None:
    failures: List[str] = []
    try:
        derived = _read("derived_input.jsonl")
        cleanup = _read("01-4_cleanup.jsonl")
        classifications = _read("02-1_classification.jsonl")
        project_route = _read("03_project_input.jsonl")
        resource_route = _read("05_resource_input.jsonl")
        fetch = _read("04-1_fetch_skillsheets_text.jsonl")
        normalized = _read("04-2_normalize_skillsheets_text.jsonl")
        attachment_identity = _read("04_attachment_identity.jsonl")
        reports = _read("contract_report.jsonl")
    except Exception as error:
        logger.error(str(error))
        raise SystemExit(1)

    _check(len(reports) == 1, "contract report must contain one record", failures)
    if not reports:
        raise SystemExit(1)
    report = reports[0]
    derived_ids = {record.get("message_id") for record in derived}
    cleanup_ids = {record.get("message_id") for record in cleanup}
    classification_ids = {record.get("message_id") for record in classifications}
    distribution = {
        mail_type: sum(1 for record in classifications if record.get("mail_type") == mail_type)
        for mail_type in ("resource", "project", "ambiguous", "unknown")
    }

    _check(len(derived) == 2, "derived input count must be 2", failures)
    _check(len(cleanup) == 2, "01-4 output count must be 2", failures)
    _check(derived_ids == cleanup_ids == classification_ids, "message_id continuity failed", failures)
    _check(
        distribution == {"resource": 2, "project": 0, "ambiguous": 0, "unknown": 0},
        "observed 02-1 distribution changed",
        failures,
    )
    _check(report.get("result") == "PASS", "selective result must be PASS", failures)
    _check(report.get("blocking_stage") == "", "blocking stage must be empty", failures)
    _check(len(project_route) == 0, "project route must contain 0 items", failures)
    _check(len(resource_route) == 2, "resource route must contain 2 items", failures)
    _check(fetch == [] and normalized == [], "04 must not execute in classification-only scope", failures)
    _check(
        len(attachment_identity) == 2
        and len({record.get("attachment_fingerprint") for record in attachment_identity}) == 2,
        "derived input attachment isolation failed",
        failures,
    )
    _check(report.get("body_cross_contamination") == 0, "body contamination detected", failures)
    _check(report.get("join_missing") == 0, "completed-stage join missing detected", failures)
    _check(report.get("from_subject_collision") == 0, "Success Cache identity collision", failures)
    _check(report.get("contract_06_ready") is False, "06 contract must not be marked ready", failures)
    _check(report.get("steps_03_04_05_executed") is False, "03/04/05 execution guard failed", failures)
    _check(report.get("steps_06_plus_executed") is False, "06+ execution guard failed", failures)
    _check(report.get("llm_api_calls") == 0, "LLM/API call count must be 0", failures)
    _check(report.get("external_url_calls") == 0, "external URL call count must be 0", failures)
    _check(report.get("production_changes") == 0, "production change detected", failures)
    _check(report.get("production_write") == 0, "production write detected", failures)

    if failures:
        logger.error(f"selective confirm NG: failures={len(failures)}")
        raise SystemExit(1)
    logger.ok(
        "selective confirm OK: derived=2 cleanup=2 resource=2 project=0 ambiguous=0"
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Confirm the 99-1 derived resource selective 03/04/05 contract."""

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

FIVE_FILES = (
    "05-1_extract_resource_budget.jsonl",
    "05-2_extract_resource_age.jsonl",
    "05-3_extract_resource_remote.jsonl",
    "05-4_extract_resource_foreign.jsonl",
    "05-5_extract_resource_freelance.jsonl",
    "05-6_extract_resource_workload.jsonl",
    "05-7_extract_resource_vendor_tiers.jsonl",
    "05-8_extract_resource_skill_category.jsonl",
    "05-9_extract_resource_phase_category.jsonl",
    "05-10_extract_resource_location.jsonl",
)


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
        resource_bypass = _read("03_resource_bypass.jsonl")
        resource_route = _read("05_resource_input.jsonl")
        fetch = _read("04-1_fetch_skillsheets_text.jsonl")
        normalized = _read("04-2_normalize_skillsheets_text.jsonl")
        attachment_identity = _read("04_attachment_identity.jsonl")
        five_outputs = {filename: _read(filename) for filename in FIVE_FILES}
        reports = _read("contract_report.jsonl")
    except Exception as error:
        logger.error(str(error))
        raise SystemExit(1)

    _check(len(reports) == 1, "contract report must contain one record", failures)
    if not reports:
        raise SystemExit(1)
    report = reports[0]
    derived_ids = {record.get("message_id") for record in derived}
    expected_count = len(derived)
    derived_attachment_counts = {
        record.get("message_id"): len(record.get("attachments", []))
        for record in derived
    }
    distribution = {
        mail_type: sum(1 for record in classifications if record.get("mail_type") == mail_type)
        for mail_type in ("resource", "project", "ambiguous", "unknown")
    }

    _check(
        len(derived_ids) == expected_count,
        "derived input cardinality must be unique",
        failures,
    )
    _check(len(cleanup) == expected_count, "01-4 output count mismatch", failures)
    _check(
        distribution
        == {"resource": expected_count, "project": 0, "ambiguous": 0, "unknown": 0},
        "observed 02-1 distribution changed",
        failures,
    )
    _check(project_route == [], "resource items must not enter project-only 03", failures)
    _check(
        len(resource_bypass) == expected_count
        and {record.get("message_id") for record in resource_bypass} == derived_ids,
        "03 resource bypass identity cardinality mismatch",
        failures,
    )
    _check(
        len(resource_route) == expected_count
        and {record.get("message_id") for record in resource_route} == derived_ids,
        "05 resource route identity cardinality mismatch",
        failures,
    )
    _check(
        len(fetch) == expected_count
        and all(record.get("success") is True for record in fetch)
        and all(record.get("source") == "attachment" for record in fetch),
        "04-1 attachment fetch cardinality mismatch",
        failures,
    )
    _check(
        len(normalized) == expected_count
        and all(record.get("clean_char_count", 0) > 0 for record in normalized),
        "04-2 normalization cardinality mismatch",
        failures,
    )
    _check(
        len(attachment_identity) == expected_count
        and all(
            record.get("attachment_count")
            == derived_attachment_counts.get(record.get("message_id"))
            for record in attachment_identity
        )
        and all(record.get("mapping_correct") is True for record in attachment_identity),
        "04 attachment mapping cardinality mismatch",
        failures,
    )
    _check(
        all(record.get("own_content_marker_found") is True for record in attachment_identity)
        and all(
            record.get("foreign_content_marker_found") is False
            for record in attachment_identity
        ),
        "skillsheet content crossed derived items",
        failures,
    )
    _check(
        all(
            len(records) == expected_count
            and {record.get("message_id") for record in records} == derived_ids
            for records in five_outputs.values()
        ),
        "05 output join cardinality mismatch for resource steps",
        failures,
    )
    _check(report.get("result") == "PASS", "selective result must be PASS", failures)
    _check(report.get("message_id_continuity") is True, "message_id continuity failed", failures)
    _check(report.get("body_cross_contamination") == 0, "body contamination detected", failures)
    _check(report.get("skillsheet_cross_contamination") == 0, "skillsheet contamination detected", failures)
    _check(report.get("attachment_cross_contamination") == 0, "attachment contamination detected", failures)
    _check(report.get("attachment_missing") == 0, "attachment missing detected", failures)
    _check(report.get("duplicate_attachment_mapping") == 0, "duplicate attachment detected", failures)
    _check(report.get("join_missing") == 0, "05/skillsheet join missing detected", failures)
    _check(report.get("duplicate_ids") == 0, "derived ID duplicate detected", failures)
    _check(report.get("original_id_join_key_uses") == 0, "original Gmail ID used as join key", failures)
    _check(report.get("schema_compatibility") is True, "05 schema incompatibility", failures)
    _check(report.get("normalized_schema_errors") == 0, "04 normalized schema incompatibility", failures)
    _check(report.get("resource_text_schema_errors") == 0, "resource text schema incompatibility", failures)
    _check(report.get("contract_06_ready") is True, "06 contract is not ready", failures)
    _check(
        report.get("selective_03_04_05_contract_completed") is True,
        "selective 03/04/05 contract is incomplete",
        failures,
    )
    _check(report.get("project_steps_03_executed") is False, "project-only 03 executed", failures)
    _check(report.get("resource_steps_04_05_executed") is True, "04/05 did not execute", failures)
    _check(report.get("steps_06_plus_executed") is False, "06+ execution guard failed", failures)
    _check(report.get("llm_api_calls") == 0, "LLM/API call count must be 0", failures)
    _check(report.get("external_url_calls") == 0, "external URL call count must be 0", failures)
    _check(report.get("production_changes") == 0, "production change detected", failures)
    _check(report.get("production_write") == 0, "production write detected", failures)

    if failures:
        logger.error(f"selective confirm NG: failures={len(failures)}")
        raise SystemExit(1)
    logger.ok(
        f"selective confirm OK: derived={expected_count} "
        f"03_bypass={len(resource_bypass)} 04={len(fetch)} "
        f"05={expected_count}x{len(five_outputs)} 06_READY=YES"
    )


if __name__ == "__main__":
    main()

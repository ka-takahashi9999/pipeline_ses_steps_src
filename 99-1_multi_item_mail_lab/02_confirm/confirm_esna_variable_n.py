#!/usr/bin/env python3
"""Confirm fresh ESNA variable-N replay and 01-4/02-1 compatibility."""

import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List


sys.dont_write_bytecode = True
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
for import_path in (
    PROJECT_ROOT,
    STEP_DIR / "00_tool",
    STEP_DIR / "00_tool" / "adapters" / "inline_summary",
    STEP_DIR / "00_tool" / "canonicalize",
    STEP_DIR / "00_tool" / "core",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.json_utils import read_jsonl_as_list
from common.logger import get_logger
from canonical_overlay import MAIL_MASTER_KEYS
from run_esna_offline_replay import RESULT_SUBDIR, build_esna_results


logger = get_logger("confirm_99-1_esna_variable_n")
RESULT_DIR = STEP_DIR / "01_result" / RESULT_SUBDIR


def _check(condition: bool, message: str, failures: List[str]) -> None:
    if not condition:
        failures.append(message)
        logger.error(message)


def _read(filename: str) -> List[Dict[str, Any]]:
    path = RESULT_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"missing ESNA result: {path}")
    return read_jsonl_as_list(str(path))


def main() -> None:
    failures: List[str] = []
    fresh = build_esna_results()
    saved = {
        "audit_items": _read("audit_items.jsonl"),
        "derived_mail_master": _read("derived_mail_master.jsonl"),
        "derived_input_ids": _read("derived_input_ids.jsonl"),
        "cleanup": _read("01-4_cleanup.jsonl"),
        "classification": _read("02-1_classification.jsonl"),
    }
    summaries = _read("replay_summary.jsonl")
    _check(len(summaries) == 1, "ESNA summary must contain one record", failures)
    if not summaries:
        raise SystemExit(1)
    saved["summary"] = summaries[0]
    _check(saved == fresh, "saved ESNA outputs are not fresh Core results", failures)

    summary = fresh["summary"]
    required = {
        "input_mails": 2,
        "parsed_mails": 2,
        "partial_mails": 0,
        "human_review_mails": 0,
        "system_failure_mails": 0,
        "actual_item_occurrences": 8,
        "actual_attachment_count": 8,
        "parsed_occurrences": 8,
        "attachment_mapping_success": 8,
        "logical_distinct": 8,
        "derived_versions": 8,
        "duplicate_derived_id_in_overlay": 0,
        "cross_item_contamination": 0,
        "shared_url_propagated": 0,
        "cleanup_output": 8,
        "classification_output": 8,
        "resource_output": 8,
        "project_output": 0,
        "ambiguous_output": 0,
        "unknown_output": 0,
        "llm_api_calls": 0,
        "external_url_calls": 0,
        "production_changes": 0,
        "production_write": 0,
    }
    for key, expected in required.items():
        _check(
            summary.get(key) == expected,
            f"{key}:{summary.get(key)}:expected:{expected}",
            failures,
        )
    _check(
        summary.get("delivery_cardinalities") == [3, 5],
        "actual ESNA cardinalities must be 3 and 5",
        failures,
    )
    _check(summary.get("parsed_n3") == 3, "actual N=3 must parse 3", failures)
    _check(summary.get("parsed_n5") == 5, "actual N=5 must parse 5", failures)
    _check(summary.get("idempotency_ok") is True, "replay must be idempotent", failures)

    audits = fresh["audit_items"]
    overlays = fresh["derived_mail_master"]
    status_distribution = Counter(record.get("parse_status") for record in audits)
    _check(
        status_distribution == {"PARSED": 8},
        "all ESNA occurrences must be PARSED",
        failures,
    )
    _check(
        all(
            [
                evidence.get("authority")
                for evidence in audit.get("cardinality_evidence", [])
            ]
            == ["DECLARED_COUNT", "STRUCTURAL_COMPLETE"]
            for audit in audits
        ),
        "ESNA cardinality evidence order or authority mismatch",
        failures,
    )
    _check(
        all(
            len({evidence.get("count") for evidence in audit["cardinality_evidence"]})
            == 1
            for audit in audits
        ),
        "ESNA Primary and cross-check counts differ",
        failures,
    )
    _check(
        all(set(record) == MAIL_MASTER_KEYS for record in overlays),
        "ESNA canonical overlay schema mismatch",
        failures,
    )
    _check(
        all(
            isinstance(record.get("attachments"), list)
            and len(record["attachments"]) == 1
            and record.get("html_links") == []
            for record in overlays
        ),
        "ESNA canonical artifacts or links mismatch",
        failures,
    )
    _check(
        all(record.get("mail_type") == "resource" for record in fresh["classification"]),
        "02-1 must classify ESNA resource 8/8",
        failures,
    )

    logger.info(
        "counts: input=2 declared=3+5 parsed=8 mapped=8 "
        "01-4=8 02-1_resource=8"
    )
    for audit in audits[:3]:
        logger.info(
            "representative: "
            f"item_index={audit['item_index']} "
            f"derived_id={audit['derived_item_id']} "
            f"mapping={audit['attachment_mapping'].get('status')}"
        )
    if failures:
        logger.error(f"ESNA confirm NG: failures={len(failures)}")
        raise SystemExit(1)
    logger.ok(
        "ESNA confirm OK: N=3 3/3 N=5 5/5 mapping=8/8 "
        "01-4=8/8 02-1_resource=8/8"
    )


if __name__ == "__main__":
    main()

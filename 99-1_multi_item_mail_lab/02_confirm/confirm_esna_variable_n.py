#!/usr/bin/env python3
"""Confirm fresh ESNA variable-N replay and 01-4/02-1 compatibility."""

import sys
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
from run_esna_offline_replay import (
    RESULT_SUBDIR,
    build_esna_contract_results,
    build_esna_results,
)


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
    contract = build_esna_contract_results()
    contract_summary = contract["summary"]
    _check(contract_summary.get("contract_status") == "PASS", "stable contract failed", failures)
    _check(contract_summary.get("variable_n") == [2, 4, 10], "variable-N mismatch", failures)
    _check(
        contract_summary.get("observed_item_counts") == [2, 4, 10],
        "stable parsed counts mismatch",
        failures,
    )
    _check(contract_summary.get("finding_count") == 0, "stable findings must be zero", failures)

    artifacts = contract["artifacts"]
    audits = artifacts["audit_items"]
    overlays = artifacts["derived_mail_master"]
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
        all(
            record.get("mail_type") == "resource"
            for record in contract["classification"]
        ),
        "02-1 must classify stable ESNA resources",
        failures,
    )

    actual = build_esna_results()
    actual_summary = actual["summary"]
    _check(
        actual_summary.get("actual_availability")
        in {"OBSERVATION", "DATA_UNAVAILABLE", "OBSERVATION_UNAVAILABLE"},
        "actual availability status mismatch",
        failures,
    )
    _check(
        actual_summary.get("actual_runtime_fixed_oracle") == 0,
        "actual fixed oracle must be zero",
        failures,
    )
    _check(
        isinstance(actual_summary.get("actual_observation_findings"), list)
        and actual_summary.get("actual_observation_finding_count")
        == len(actual_summary.get("actual_observation_findings")),
        "actual finding report mismatch",
        failures,
    )

    logger.info(
        "CONTRACT TESTS: PASS variable_N=2/4/10 parsed=2/4/10"
    )
    logger.info(
        "ACTUAL OBSERVATIONS: "
        + str(actual_summary.get("actual_availability"))
        + " observed_mails="
        + str(actual_summary.get("actual_observation_count"))
        + " observed_items="
        + str(actual_summary.get("parsed_occurrences"))
    )
    logger.info(
        "ACTUAL OBSERVATION FINDINGS: "
        + str(actual_summary.get("actual_observation_finding_count"))
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
        "ESNA confirm OK: contract=PASS variable_N=2/4/10 actual="
        + str(actual_summary.get("actual_availability"))
        + " findings="
        + str(actual_summary.get("actual_observation_finding_count"))
        + " fixed_actual_oracle=0"
    )


if __name__ == "__main__":
    main()

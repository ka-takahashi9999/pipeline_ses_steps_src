#!/usr/bin/env python3
"""Confirm count integrity and safety invariants of the 01-3 shadow replay."""

import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List


STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.json_utils import read_jsonl_as_list
from common.logger import get_logger


logger = get_logger("confirm_99-1_01-3_false_exclusion_shadow")
RESULT_DIR = STEP_DIR / "01_result" / "01_3_false_exclusion_shadow"
DECISIONS_PATH = RESULT_DIR / "decisions.jsonl"
PARSER_REGRESSION_PATH = RESULT_DIR / "parser_regression.jsonl"
SENDER_RULES_PATH = RESULT_DIR / "sender_rules.jsonl"
SUMMARY_PATH = RESULT_DIR / "shadow_summary.jsonl"
VALID_DECISIONS = {"KEEP", "EXCLUDE", "REVIEW"}
VALID_LABELS = {
    "CLEAR_EXCLUDE",
    "LIKELY_VALID_SINGLE",
    "LIKELY_VALID_MULTI",
    "UNKNOWN",
}
REPRESENTATIVE_SENDERS = {
    "dss@d-standing.co.jp",
    "partner@lfield.co.jp",
    "r-hattori@legarea.jp",
    "inoue@netwisdom.co.jp",
    "y.ogawa@1-r.co.jp",
    "ayana.yamamoto@sakya.jp",
}


def check(condition: bool, message: str, failures: List[str]) -> None:
    if not condition:
        failures.append(message)
        logger.error(message)


def main() -> None:
    failures: List[str] = []
    for path in (
        DECISIONS_PATH,
        PARSER_REGRESSION_PATH,
        SENDER_RULES_PATH,
        SUMMARY_PATH,
    ):
        check(path.exists(), "missing result: " + str(path), failures)
    if failures:
        raise SystemExit(1)

    decisions: List[Dict[str, Any]] = read_jsonl_as_list(str(DECISIONS_PATH))
    parser_rows: List[Dict[str, Any]] = read_jsonl_as_list(
        str(PARSER_REGRESSION_PATH)
    )
    sender_rules: List[Dict[str, Any]] = read_jsonl_as_list(str(SENDER_RULES_PATH))
    summaries: List[Dict[str, Any]] = read_jsonl_as_list(str(SUMMARY_PATH))
    check(len(summaries) == 1, "summary must contain one record", failures)
    if not summaries:
        raise SystemExit(1)
    summary = summaries[0]

    decision_counts = Counter(row.get("shadow_decision") for row in decisions)
    current_rows = [row for row in decisions if row.get("current_excluded")]
    rescued = [row for row in decisions if row.get("rescued")]
    clear_rows = [
        row for row in current_rows if row.get("observation_label") == "CLEAR_EXCLUDE"
    ]
    check(
        len(decisions) == summary.get("shadow_input_records"),
        "input/output decision count mismatch",
        failures,
    )
    check(
        sum(decision_counts.values()) == len(decisions),
        "decision partition count mismatch",
        failures,
    )
    check(
        set(decision_counts).issubset(VALID_DECISIONS),
        "invalid shadow decision",
        failures,
    )
    check(
        all(row.get("observation_label") in VALID_LABELS for row in decisions),
        "invalid observation label",
        failures,
    )
    check(
        len({row.get("message_id") for row in decisions}) == len(decisions),
        "duplicate decision message_id",
        failures,
    )
    check(
        len(current_rows) == summary.get("current_excluded"),
        "current excluded count mismatch",
        failures,
    )
    check(
        summary.get("current_replay_matches_stored") is True,
        "current replay differs from stored 01-3 result",
        failures,
    )
    check(
        len(rescued) == summary.get("rescued"),
        "rescued count mismatch",
        failures,
    )
    check(
        summary.get("rescued_likely_valid_single", 0)
        + summary.get("rescued_likely_valid_multi", 0)
        + summary.get("rescued_other", 0)
        == summary.get("rescued"),
        "rescued breakdown mismatch",
        failures,
    )
    check(
        all(row.get("shadow_decision") == "EXCLUDE" for row in clear_rows),
        "CLEAR_EXCLUDE was not preserved",
        failures,
    )
    check(
        summary.get("clear_exclude_preserved") == summary.get("clear_exclude_total"),
        "CLEAR_EXCLUDE preservation count mismatch",
        failures,
    )
    check(summary.get("newly_excluded") == 0, "new exclusion detected", failures)
    check(
        summary.get("new_false_exclude_suspected") == 0,
        "false exclusion suspected",
        failures,
    )
    check(
        summary.get("new_false_keep_suspected") == 0,
        "false keep suspected",
        failures,
    )
    check(
        all(
            row.get("shadow_decision") == "KEEP"
            for row in rescued
            if row.get("routes_99_1") or row.get("remote_candidate")
        ),
        "99-1 or 99-2 candidate was not kept",
        failures,
    )
    check(
        len(parser_rows) == summary.get("rescued_99_1_applicable"),
        "99-1 applicable count mismatch",
        failures,
    )
    check(
        all(row.get("safe_reach") is True for row in parser_rows),
        "rescued 99-1 candidate did not safely reach parser",
        failures,
    )
    check(summary.get("parser_crash") == 0, "parser crash detected", failures)
    check(summary.get("system_failure") == 0, "SYSTEM_FAILURE detected", failures)
    check(
        summary.get("rescued_single_classification_total")
        == summary.get("rescued_likely_valid_single"),
        "single classification count mismatch",
        failures,
    )
    check(
        summary.get("message_id_collision") == 0,
        "rescued message_id collision detected",
        failures,
    )
    check(summary.get("external_access") == 0, "external access detected", failures)
    check(
        summary.get("production_01_3_changes") == 0,
        "production 01-3 changed",
        failures,
    )
    check(summary.get("production_write") == 0, "production write detected", failures)
    observed_senders = {row.get("sender") for row in sender_rules}
    check(
        REPRESENTATIVE_SENDERS.issubset(observed_senders),
        "representative sender coverage missing",
        failures,
    )
    check(summary.get("implementation") == "PASS", "implementation is not PASS", failures)

    if failures:
        logger.error("confirm FAIL: failures=" + str(len(failures)))
        raise SystemExit(1)
    logger.ok(
        "confirm PASS: input="
        + str(len(decisions))
        + " current_excluded="
        + str(len(current_rows))
        + " rescued="
        + str(len(rescued))
        + " shadow_excluded="
        + str(decision_counts["EXCLUDE"])
        + " review="
        + str(decision_counts["REVIEW"])
    )


if __name__ == "__main__":
    main()

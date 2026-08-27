#!/usr/bin/env python3
"""Confirm stable Ichi-R contract plus optional saved-actual observation."""

import base64
import copy
import sys
from pathlib import Path
from typing import List, Tuple


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
from inline_summary_adapter import InlineSummaryAdapter
from run_offline_replay import DEFAULT_INPUT, process_records
from run_selective_pipeline_test import (
    _load_existing_modules,
    _production_artifact_snapshot,
)


logger = get_logger("confirm_99-1_ichi_r_inline_summary")
CONFIG_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "companies"
    / "ichi_r.config.json.example"
)
FIXTURE_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "fixtures"
    / "redacted"
    / "inline_summary"
    / "ichi_r.fixture.jsonl.example"
)


def _check(condition: bool, message: str, failures: List[str]) -> None:
    if not condition:
        failures.append(message)
        logger.error(message)


def _contract_status(failures: List[str]) -> str:
    return "FAIL" if failures else "PASS"


def _contract_tests_label(failures: List[str]) -> str:
    return "CONTRACT TESTS: " + _contract_status(failures)


def _read_actual_observation(
    adapter: InlineSummaryAdapter,
    input_path: Path = DEFAULT_INPUT,
) -> Tuple[List[dict], str, List[str]]:
    try:
        records = read_jsonl_as_list(str(input_path))
    except Exception as exc:
        return (
            [],
            "OBSERVATION_UNAVAILABLE",
            ["source_read_exception:" + type(exc).__name__],
        )
    selected = [record for record in records if adapter.matches(record)]
    return (
        selected,
        "OBSERVATION" if selected else "DATA_UNAVAILABLE",
        [],
    )


def main() -> None:
    failures: List[str] = []
    adapter = InlineSummaryAdapter.from_file(CONFIG_PATH)
    production_before = _production_artifact_snapshot()
    fixtures = read_jsonl_as_list(str(FIXTURE_PATH))
    _check(len(fixtures) == 1, "stable fixture count must be one", failures)
    if not fixtures:
        raise SystemExit(1)
    fixture = copy.deepcopy(fixtures[0])
    payloads = {
        "スキルシート(R.A)_20260825.xlsx": b"ichi-r-redacted-A-v1",
        "RB【匿名駅】.xlsx": b"ichi-r-redacted-B-v1",
    }
    for attachment in fixture["attachments"]:
        payload = payloads[attachment["filename"]]
        attachment["data"] = base64.urlsafe_b64encode(payload).decode("ascii")
        attachment["size"] = len(payload)
    artifacts, stats = process_records([fixture], adapter)
    overlays = artifacts["derived_mail_master"]

    _check(stats["input_mails"] == 1, "input mail count must be 1", failures)
    _check(stats["expected_item_occurrences"] == 2, "expected items must be 2", failures)
    _check(stats["parsed_mails"] == 1, "parsed mail count must be 1", failures)
    _check(stats["parsed_occurrences"] == 2, "parsed item count must be 2", failures)
    _check(stats["partial_mails"] == 0, "PARTIAL count must be 0", failures)
    _check(stats["human_review_mails"] == 0, "HUMAN_REVIEW count must be 0", failures)
    _check(stats["logical_distinct"] == 2, "logical distinct must be 2", failures)
    _check(stats["derived_versions"] == 2, "derived versions must be 2", failures)
    _check(
        stats["attachment_mapping_success"] == 2,
        "attachment mapping must be 2/2",
        failures,
    )
    _check(
        stats["duplicate_derived_id_in_overlay"] == 0,
        "duplicate derived ID count must be 0",
        failures,
    )
    _check(stats["canonical_overlay_schema_ok"], "overlay schema mismatch", failures)
    _check(
        all(record.get("html_links") == [] for record in overlays),
        "shared URL must not be propagated",
        failures,
    )
    _check(
        len({(record.get("from"), record.get("subject")) for record in overlays}) == 2,
        "From + Subject collision must be 0",
        failures,
    )

    modules = _load_existing_modules(("cleanup", "classify"))
    cleanup_module = modules["cleanup"]
    classify_module = modules["classify"]
    _check(
        classify_module.USE_LLM_CLASSIFY is False,
        "02-1 LLM feature flag must remain OFF",
        failures,
    )
    cleanup_rules = cleanup_module.load_cleanup_rules(cleanup_module.CLEANUP_RULES_PATH)
    keywords = classify_module.load_keywords(classify_module.KEYWORDS_PATH)
    classifications = []
    for overlay in overlays:
        cleaned_body, _ = cleanup_module.cleanup_body(
            overlay["body_text"], cleanup_rules
        )
        _check(bool(cleaned_body), "01-4 produced empty body", failures)
        mail_type, _, _ = classify_module.rule_classify(
            overlay["subject"],
            cleaned_body,
            keywords,
            has_attachment=bool(overlay["attachments"]),
        )
        classifications.append(mail_type)
    _check(
        classifications == ["resource", "resource"],
        "02-1 must classify resource 2/2",
        failures,
    )

    actual_records, actual_availability, observation_findings = (
        _read_actual_observation(adapter)
    )
    try:
        actual_artifacts, actual_stats = process_records(actual_records, adapter)
    except Exception as exc:  # rotating actual is observation-only
        observation_findings.append("parser_exception:" + type(exc).__name__)
        actual_artifacts, actual_stats = process_records([], adapter)
    source_ids = [str(record.get("message_id", "")) for record in actual_records]
    if len(source_ids) != len(set(source_ids)):
        observation_findings.append("duplicate_source_message_id")
    if actual_stats["system_failure_mails"]:
        observation_findings.append("parser_system_failure")
    if not actual_stats["canonical_overlay_schema_ok"]:
        observation_findings.append("canonical_overlay_schema_mismatch")
    actual_overlay_ids = [
        str(record.get("message_id", ""))
        for record in actual_artifacts["derived_mail_master"]
    ]
    if len(actual_overlay_ids) != len(set(actual_overlay_ids)):
        observation_findings.append("duplicate_derived_message_id")
    production_after = _production_artifact_snapshot()
    _check(
        production_before == production_after,
        "production artifacts changed during confirm",
        failures,
    )

    logger.info(
        _contract_tests_label(failures)
        + " counts: input=1 expected=2 parsed=2 mapped=2 "
        "logical=2 versions=2 html_links=0 subject_collision=0"
    )
    logger.info(
        "ACTUAL OBSERVATIONS: "
        + actual_availability
        + " observed_mails="
        + str(len(actual_records))
        + " observed_items="
        + str(actual_stats["parsed_occurrences"])
    )
    logger.info(
        "ACTUAL OBSERVATION FINDINGS: " + str(len(observation_findings))
    )
    for index, overlay in enumerate(overlays[:2], 1):
        logger.info(
            f"representative: item_index={index} "
            f"derived_id={overlay['message_id']} attachment_count=1"
        )
    if failures:
        logger.error(f"confirm NG: failures={len(failures)}")
        sys.exit(1)
    logger.ok(
        "confirm OK: contract=PASS actual="
        + actual_availability
        + " findings="
        + str(len(observation_findings))
        + " fixed_actual_oracle=0 01-4=2/2 02-1_resource=2/2"
    )


if __name__ == "__main__":
    main()

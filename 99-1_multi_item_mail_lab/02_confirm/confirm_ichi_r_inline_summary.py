#!/usr/bin/env python3
"""Confirm the test-only Ichi-R P2 offline replay without writing results."""

import sys
from pathlib import Path
from typing import List


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

from common.json_utils import read_jsonl
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


def _check(condition: bool, message: str, failures: List[str]) -> None:
    if not condition:
        failures.append(message)
        logger.error(message)


def main() -> None:
    failures: List[str] = []
    adapter = InlineSummaryAdapter.from_file(CONFIG_PATH)
    production_before = _production_artifact_snapshot()
    artifacts, stats = process_records(read_jsonl(str(DEFAULT_INPUT)), adapter)
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
    production_after = _production_artifact_snapshot()
    _check(
        production_before == production_after,
        "production artifacts changed during confirm",
        failures,
    )

    logger.info(
        "counts: input=1 expected=2 parsed=2 mapped=2 "
        "logical=2 versions=2 html_links=0 subject_collision=0"
    )
    for index, overlay in enumerate(overlays[:2], 1):
        logger.info(
            f"representative: item_index={index} "
            f"derived_id={overlay['message_id']} attachment_count=1"
        )
    if failures:
        logger.error(f"confirm NG: failures={len(failures)}")
        sys.exit(1)
    logger.ok("confirm OK: 01-4=2/2 02-1_resource=2/2 LLM/API=0 external_url=0")


if __name__ == "__main__":
    main()

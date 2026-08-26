#!/usr/bin/env python3
"""Fresh test-only ESNA variable-N replay through existing 01-4/02-1 logic."""

import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


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

from common.file_utils import ensure_result_dirs
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger
from inline_summary_adapter import InlineSummaryAdapter
from run_offline_replay import DEFAULT_INPUT, process_records
from run_selective_pipeline_test import (
    _load_existing_modules,
    _production_artifact_snapshot,
)


logger = get_logger("99-1_esna_variable_n")
CONFIG_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "companies"
    / "esna.config.json.example"
)
RESULT_SUBDIR = "esna_variable_n"


def _delivery_counts(audits: List[Dict[str, Any]]) -> Dict[str, int]:
    counts_by_source: Dict[str, set] = {}
    for audit in audits:
        source_id = str(audit.get("original_message_id", ""))
        primary_counts = {
            evidence.get("count")
            for evidence in audit.get("cardinality_evidence", [])
            if evidence.get("is_primary") is True
            and evidence.get("complete") is True
            and isinstance(evidence.get("count"), int)
        }
        counts_by_source.setdefault(source_id, set()).update(primary_counts)
    result: Dict[str, int] = {}
    for source_id, counts in counts_by_source.items():
        if len(counts) != 1:
            raise ValueError(f"ESNA primary count is not singular: {source_id}")
        result[source_id] = next(iter(counts))
    return result


def _run_01_4_02_1(
    overlays: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    modules = _load_existing_modules(("cleanup", "classify"))
    cleanup_module = modules["cleanup"]
    classify_module = modules["classify"]
    if classify_module.USE_LLM_CLASSIFY:
        raise ValueError("02-1 LLM feature flag must remain OFF")
    cleanup_rules = cleanup_module.load_cleanup_rules(
        cleanup_module.CLEANUP_RULES_PATH
    )
    keywords = classify_module.load_keywords(classify_module.KEYWORDS_PATH)
    cleanup_records: List[Dict[str, Any]] = []
    classification_records: List[Dict[str, Any]] = []
    for overlay in overlays:
        cleaned_body, _ = cleanup_module.cleanup_body(
            overlay["body_text"], cleanup_rules
        )
        if not cleaned_body:
            raise ValueError(f"01-4 removed ESNA item body: {overlay['message_id']}")
        cleanup_records.append(
            {"message_id": overlay["message_id"], "body_text": cleaned_body}
        )
        mail_type, _, _ = classify_module.rule_classify(
            overlay["subject"],
            cleaned_body,
            keywords,
            has_attachment=bool(overlay["attachments"]),
        )
        classification_records.append(
            {"message_id": overlay["message_id"], "mail_type": mail_type}
        )
    return cleanup_records, classification_records


def build_esna_results(
    records: Iterable[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build fresh results from the current Core; never read prior 99-1 outputs."""
    adapter = InlineSummaryAdapter.from_file(CONFIG_PATH)
    source_records = (
        list(records)
        if records is not None
        else read_jsonl_as_list(str(DEFAULT_INPUT))
    )
    selected = [record for record in source_records if adapter.matches(record)]
    production_before = _production_artifact_snapshot()
    artifacts, replay_stats = process_records(selected, adapter)
    reverse_artifacts, reverse_stats = process_records(reversed(selected), adapter)
    idempotency_ok = artifacts == reverse_artifacts and replay_stats == reverse_stats

    overlays = artifacts["derived_mail_master"]
    cleanup_records, classification_records = _run_01_4_02_1(overlays)
    distribution = Counter(
        record["mail_type"] for record in classification_records
    )
    delivery_counts = _delivery_counts(artifacts["audit_items"])
    parsed_by_count = Counter()
    for audit in artifacts["audit_items"]:
        if audit.get("parse_status") == "PARSED":
            parsed_by_count[delivery_counts[audit["original_message_id"]]] += 1

    body_anchor_counts = [
        len(list(adapter._anchor_regex.finditer(record["body_text"])))
        for record in overlays
    ]
    cross_item_contamination = sum(
        max(0, anchor_count - 1) for anchor_count in body_anchor_counts
    )
    shared_url_propagated = sum(
        len(record.get("html_links", [])) for record in overlays
    )
    production_after = _production_artifact_snapshot()
    production_write = int(production_before != production_after)

    summary = dict(replay_stats)
    summary.update(
        {
            "delivery_cardinalities": sorted(delivery_counts.values()),
            "actual_item_occurrences": sum(delivery_counts.values()),
            "actual_attachment_count": sum(
                len(record.get("attachments", [])) for record in selected
            ),
            "parsed_n3": parsed_by_count[3],
            "parsed_n5": parsed_by_count[5],
            "cross_item_contamination": cross_item_contamination,
            "shared_url_propagated": shared_url_propagated,
            "cleanup_output": len(cleanup_records),
            "classification_output": len(classification_records),
            "resource_output": distribution["resource"],
            "project_output": distribution["project"],
            "ambiguous_output": distribution["ambiguous"],
            "unknown_output": distribution["unknown"],
            "idempotency_ok": idempotency_ok,
            "llm_api_calls": 0,
            "external_url_calls": 0,
            "production_changes": 0,
            "production_write": production_write,
        }
    )
    return {
        "audit_items": artifacts["audit_items"],
        "derived_mail_master": overlays,
        "derived_input_ids": artifacts["derived_input_ids"],
        "cleanup": cleanup_records,
        "classification": classification_records,
        "summary": summary,
    }


def write_esna_results(results: Dict[str, Any]) -> None:
    result_dirs = ensure_result_dirs(str(STEP_DIR))
    result_dir = result_dirs["result"] / RESULT_SUBDIR
    result_dir.mkdir(parents=True, exist_ok=True)
    for filename, key in (
        ("audit_items.jsonl", "audit_items"),
        ("derived_mail_master.jsonl", "derived_mail_master"),
        ("derived_input_ids.jsonl", "derived_input_ids"),
        ("01-4_cleanup.jsonl", "cleanup"),
        ("02-1_classification.jsonl", "classification"),
    ):
        write_jsonl(str(result_dir / filename), results[key])
    write_jsonl(str(result_dir / "replay_summary.jsonl"), [results["summary"]])


def main() -> None:
    results = build_esna_results()
    summary = results["summary"]
    required = {
        "input_mails": 2,
        "actual_item_occurrences": 8,
        "actual_attachment_count": 8,
        "parsed_occurrences": 8,
        "attachment_mapping_success": 8,
        "logical_distinct": 8,
        "duplicate_derived_id_in_overlay": 0,
        "cross_item_contamination": 0,
        "shared_url_propagated": 0,
        "cleanup_output": 8,
        "resource_output": 8,
        "project_output": 0,
        "ambiguous_output": 0,
        "production_write": 0,
    }
    failures = [
        f"{key}:{summary.get(key)}:expected:{expected}"
        for key, expected in required.items()
        if summary.get(key) != expected
    ]
    if summary.get("delivery_cardinalities") != [3, 5]:
        failures.append(
            f"delivery_cardinalities:{summary.get('delivery_cardinalities')}:expected:[3,5]"
        )
    if not summary.get("idempotency_ok"):
        failures.append("idempotency_failed")
    if failures:
        raise ValueError("ESNA replay contract failed: " + ";".join(failures))
    write_esna_results(results)
    logger.ok(
        "ESNA variable-N replay OK: "
        f"mails={summary['input_mails']} n3={summary['parsed_n3']} "
        f"n5={summary['parsed_n5']} resource={summary['resource_output']}"
    )


if __name__ == "__main__":
    main()

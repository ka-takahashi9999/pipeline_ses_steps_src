#!/usr/bin/env python3
"""Run the stable ESNA contract and an optional saved-actual observation."""

import base64
import copy
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
FIXTURE_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "fixtures"
    / "redacted"
    / "inline_summary"
    / "esna.synthetic.fixture.jsonl.example"
)


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


def _stable_fixture_records() -> List[Dict[str, Any]]:
    records = read_jsonl_as_list(str(FIXTURE_PATH))
    for record in records:
        for index, attachment in enumerate(record.get("attachments", [])):
            payload = ("esna-contract-" + str(record.get("message_id")) + "-" + str(index)).encode()
            attachment["data"] = base64.urlsafe_b64encode(payload).decode("ascii")
            attachment["size"] = len(payload)
    return records


def build_esna_contract_results() -> Dict[str, Any]:
    """Build the deterministic N=2/4/10 contract independently of Gmail actual."""
    adapter = InlineSummaryAdapter.from_file(CONFIG_PATH)
    records = _stable_fixture_records()
    artifacts, stats = process_records(copy.deepcopy(records), adapter)
    repeated_artifacts, repeated_stats = process_records(
        reversed(copy.deepcopy(records)), adapter
    )
    cleanup, classification = _run_01_4_02_1(artifacts["derived_mail_master"])
    observed_counts = sorted(
        len(
            [
                audit
                for audit in artifacts["audit_items"]
                if audit.get("original_message_id") == record.get("message_id")
                and audit.get("parse_status") == "PARSED"
            ]
        )
        for record in records
    )
    findings = []
    if observed_counts != [2, 4, 10]:
        findings.append("variable_n_mismatch")
    if stats.get("parsed_mails") != 3 or stats.get("parsed_occurrences") != 16:
        findings.append("stable_fixture_parse_mismatch")
    if stats.get("attachment_mapping_success") != 16:
        findings.append("stable_fixture_attachment_mapping_mismatch")
    if not stats.get("canonical_overlay_schema_ok"):
        findings.append("stable_fixture_overlay_schema_mismatch")
    if any(row.get("mail_type") != "resource" for row in classification):
        findings.append("stable_fixture_classification_mismatch")
    if artifacts != repeated_artifacts or stats != repeated_stats:
        findings.append("stable_fixture_idempotency_mismatch")
    return {
        "artifacts": artifacts,
        "cleanup": cleanup,
        "classification": classification,
        "summary": {
            "contract_status": "PASS" if not findings else "FAIL",
            "fixture_source_count": len(records),
            "variable_n": [2, 4, 10],
            "observed_item_counts": observed_counts,
            "parsed_occurrences": stats.get("parsed_occurrences"),
            "attachment_mapping_success": stats.get("attachment_mapping_success"),
            "cleanup_output": len(cleanup),
            "classification_output": len(classification),
            "finding_count": len(findings),
            "findings": findings,
        },
    }


def build_esna_results(
    records: Iterable[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build fresh results from the current Core; never read prior 99-1 outputs."""
    adapter = InlineSummaryAdapter.from_file(CONFIG_PATH)
    observation_exceptions = []
    if records is not None:
        source_records = list(records)
    elif DEFAULT_INPUT.exists():
        try:
            source_records = read_jsonl_as_list(str(DEFAULT_INPUT))
        except Exception as exc:
            source_records = []
            observation_exceptions.append(
                "source_read_exception:" + type(exc).__name__
            )
    else:
        source_records = []
    selected = [record for record in source_records if adapter.matches(record)]
    production_before = _production_artifact_snapshot()
    try:
        artifacts, replay_stats = process_records(selected, adapter)
        reverse_artifacts, reverse_stats = process_records(reversed(selected), adapter)
        idempotency_ok = artifacts == reverse_artifacts and replay_stats == reverse_stats
        overlays = artifacts["derived_mail_master"]
        cleanup_records, classification_records = _run_01_4_02_1(overlays)
    except Exception as exc:  # rotating actual must not gate the stable contract
        observation_exceptions.append("parser_exception:" + type(exc).__name__)
        artifacts, replay_stats = process_records([], adapter)
        overlays = []
        cleanup_records = []
        classification_records = []
        idempotency_ok = False
    distribution = Counter(
        record["mail_type"] for record in classification_records
    )
    try:
        delivery_counts = _delivery_counts(artifacts["audit_items"])
    except ValueError:
        delivery_counts = {}
        observation_exceptions.append("primary_count_not_singular")
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

    actual_findings = list(observation_exceptions)
    source_ids = [str(record.get("message_id", "")) for record in selected]
    if len(source_ids) != len(set(source_ids)):
        actual_findings.append("duplicate_source_message_id")
    overlay_ids = [str(record.get("message_id", "")) for record in overlays]
    if len(overlay_ids) != len(set(overlay_ids)):
        actual_findings.append("duplicate_derived_message_id")
    if replay_stats.get("system_failure_mails"):
        actual_findings.append("parser_system_failure")
    if replay_stats.get("attachment_mapping_success") != replay_stats.get(
        "parsed_occurrences"
    ):
        actual_findings.append("attachment_mapping_incomplete")
    if not replay_stats.get("canonical_overlay_schema_ok"):
        actual_findings.append("canonical_overlay_schema_mismatch")
    if cross_item_contamination:
        actual_findings.append("cross_item_contamination")
    if shared_url_propagated:
        actual_findings.append("shared_url_propagated")
    if not idempotency_ok:
        actual_findings.append("idempotency_mismatch")
    if any(
        row.get("mail_type") not in {"resource", "project", "ambiguous", "unknown"}
        for row in classification_records
    ):
        actual_findings.append("classification_status_out_of_schema")

    summary = dict(replay_stats)
    summary.update(
        {
            "actual_availability": (
                "OBSERVATION_UNAVAILABLE"
                if any(
                    finding.startswith("source_read_exception:")
                    for finding in observation_exceptions
                )
                else ("OBSERVATION" if selected else "DATA_UNAVAILABLE")
            ),
            "actual_observation_count": len(selected),
            "actual_observation_finding_count": len(actual_findings),
            "actual_observation_findings": actual_findings,
            "actual_runtime_fixed_oracle": 0,
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
    contract = build_esna_contract_results()
    results = build_esna_results()
    summary = results["summary"]
    summary["contract_status"] = contract["summary"]["contract_status"]
    summary["contract_variable_n"] = contract["summary"]["variable_n"]
    summary["contract_finding_count"] = contract["summary"]["finding_count"]
    if contract["summary"]["findings"]:
        raise ValueError(
            "ESNA stable contract failed: "
            + ";".join(contract["summary"]["findings"])
        )
    write_esna_results(results)
    logger.ok(
        "ESNA contract PASS: variable_N=2/4/10 actual="
        + str(summary["actual_availability"])
        + " observed_mails="
        + str(summary["actual_observation_count"])
        + " findings="
        + str(summary["actual_observation_finding_count"])
    )


if __name__ == "__main__":
    main()

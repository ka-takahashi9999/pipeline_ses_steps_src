#!/usr/bin/env python3
"""Fresh test-only JQIT ATTACHMENT_LIST replay through 01-4 and 02-1 only."""

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
    STEP_DIR / "00_tool" / "adapters" / "attachment_list",
    STEP_DIR / "00_tool" / "canonicalize",
    STEP_DIR / "00_tool" / "core",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.file_utils import ensure_result_dirs
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger
from attachment_list_adapter import ADAPTER_ID, ADAPTER_VERSION, AttachmentListAdapter
from canonical_overlay import build_canonical_overlay
from run_offline_replay import DEFAULT_INPUT
from run_selective_pipeline_test import _load_existing_modules, _production_artifact_snapshot


logger = get_logger("99-1_attachment_list_variable_n")
CONFIG_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "companies"
    / "jqit_attachment_list.config.json.example"
)
RESULT_SUBDIR = "attachment_list_variable_n"


def _run_01_4_02_1(
    overlays: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    modules = _load_existing_modules(("cleanup", "classify"))
    cleanup_module = modules["cleanup"]
    classify_module = modules["classify"]
    if classify_module.USE_LLM_CLASSIFY:
        raise ValueError("02-1 LLM feature flag must remain OFF")
    cleanup_rules = cleanup_module.load_cleanup_rules(cleanup_module.CLEANUP_RULES_PATH)
    keywords = classify_module.load_keywords(classify_module.KEYWORDS_PATH)
    cleanup_records: List[Dict[str, Any]] = []
    classification_records: List[Dict[str, Any]] = []
    for overlay in overlays:
        cleaned_body, _ = cleanup_module.cleanup_body(overlay["body_text"], cleanup_rules)
        if not cleaned_body:
            raise ValueError("01-4 removed ATTACHMENT_LIST item body:" + overlay["message_id"])
        cleanup_records.append({"message_id": overlay["message_id"], "body_text": cleaned_body})
        mail_type, _, _ = classify_module.rule_classify(
            overlay["subject"],
            cleaned_body,
            keywords,
            has_attachment=bool(overlay["attachments"]),
        )
        classification_records.append({"message_id": overlay["message_id"], "mail_type": mail_type})
    return cleanup_records, classification_records


def _audit_item(mail, adapter, result, item):
    return {
        "original_message_id": str(mail.get("message_id", "")),
        "logical_item_id": item["logical_item_id"],
        "derived_item_id": item["derived_item_id"],
        "item_index": item["item_index"],
        "identifier": item["identifier"],
        "source_type": "attachment_list",
        "source_company": adapter.config["source_company"],
        "source_fingerprint": result.source["source_fingerprint"],
        "source_acquisition_status": result.source["source_acquisition_status"],
        "container_enumeration_status": result.source["container_enumeration_status"],
        "inline_structure_status": result.source["inline_structure_status"],
        "attachment_mapping_status": result.source["attachment_mapping_status"],
        "source_atomic_status": result.source["source_atomic_status"],
        "auto_union_eligible": result.source["auto_union_eligible"],
        "projection_kind": "TECHNICAL_PROJECTION",
        "parse_status": "TECHNICAL_PROJECTION",
        "parse_reasons": result.reasons,
        "config_id": adapter.config["config_id"],
        "config_version": adapter.config["version"],
        "adapter_id": ADAPTER_ID,
        "adapter_version": ADAPTER_VERSION,
        "original_subject": str(mail.get("subject", "")),
        "original_timestamp": str(mail.get("date", "")),
        "body_fingerprint": item["body_fingerprint"],
        "artifact_set_fingerprint": item["artifact_set_fingerprint"],
        "version_relevant_artifact_set_fingerprint": item["version_relevant_artifact_set_fingerprint"],
        "version_fingerprint": item["version_fingerprint"],
        "content_fingerprint": item["content_fingerprint"],
        "attachment_mapping": item["attachment_mapping"],
        "item_artifacts": item["item_artifacts"],
        "identity_evidence": item["identity_evidence"],
        "identity_durability": item["identity_durability"],
        "version_scope": item["version_scope"],
        "classification_context_evidence": item["classification_context_evidence"],
    }


def _contamination_count(items_by_source):
    failures = 0
    for items in items_by_source:
        identifiers = {item["identifier"] for item in items}
        for item in items:
            if len(item["attachments"]) != 1 or item["html_links"] != []:
                failures += 1
            if any(
                identifier in item["body_text"]
                for identifier in identifiers - {item["identifier"]}
            ):
                failures += 1
    return failures


def build_attachment_list_results(
    records: Iterable[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    adapter = AttachmentListAdapter.from_file(CONFIG_PATH)
    source_records = list(records) if records is not None else read_jsonl_as_list(str(DEFAULT_INPUT))
    selected = [record for record in source_records if adapter.matches(record)]
    production_before = _production_artifact_snapshot()
    source_audits = []
    enumeration = []
    technical_audits = []
    technical_overlays = []
    technical_items = []
    items_by_source = []
    deterministic = True
    statuses = Counter()
    for mail in selected:
        result = adapter.parse(mail)
        deterministic = deterministic and result == adapter.parse(mail)
        statuses[result.status] += 1
        source_audits.append(
            {
                "original_message_id": str(mail.get("message_id", "")),
                "parse_status": result.status,
                "parse_reasons": result.reasons,
                "source": result.source,
                "container_contracts": result.containers,
            }
        )
        enumeration.extend(
            {"original_message_id": str(mail.get("message_id", "")), **row}
            for row in result.attachment_enumeration
        )
        items_by_source.append(result.technical_projection_items)
        for item in result.technical_projection_items:
            technical_audits.append(_audit_item(mail, adapter, result, item))
            technical_overlays.append(build_canonical_overlay(mail, item))
            technical_items.append(item)
    cleanup_records, classification_records = _run_01_4_02_1(technical_overlays)
    distribution = Counter(row["mail_type"] for row in classification_records)
    production_after = _production_artifact_snapshot()
    observed_counts = [len(record.get("attachments", [])) for record in selected]
    profile_counts = [audit["source"]["profile_count"] for audit in source_audits]
    mapping_counts = [audit["source"]["mapping_count"] for audit in source_audits]
    summary = {
        "actual_deliveries": len(selected),
        "actual_observed_attachment_counts": observed_counts,
        "actual_profile_counts": profile_counts,
        "actual_declared_counts": [audit["source"]["declared_count"] for audit in source_audits],
        "actual_mapping_counts": mapping_counts,
        "actual_mapping_total": sum(mapping_counts),
        "actual_station_audit_matches": sum(audit["source"]["station_audit_matches"] for audit in source_audits),
        "false_substring_matches": sum(audit["source"]["false_substring_matches"] for audit in source_audits),
        "actual_acquisition_statuses": [audit["source"]["source_acquisition_status"] for audit in source_audits],
        "actual_container_enumeration_statuses": [audit["source"]["container_enumeration_status"] for audit in source_audits],
        "actual_inline_structure_statuses": [audit["source"]["inline_structure_status"] for audit in source_audits],
        "actual_attachment_mapping_statuses": [audit["source"]["attachment_mapping_status"] for audit in source_audits],
        "actual_source_atomic_statuses": [audit["source"]["source_atomic_status"] for audit in source_audits],
        "actual_auto_union_eligible": any(audit["source"]["auto_union_eligible"] for audit in source_audits),
        "technical_projection_total": len(technical_items),
        "canonical_eligible_total": 0,
        "cross_item_contamination": _contamination_count(items_by_source),
        "derived_id_deterministic": deterministic,
        "cleanup_output": len(cleanup_records),
        "cleanup_nonempty": sum(bool(row["body_text"]) for row in cleanup_records),
        "classification_output": len(classification_records),
        "resource_classified": distribution["resource"],
        "project_classified": distribution["project"],
        "ambiguous_output": distribution["ambiguous"],
        "unknown_output": distribution["unknown"],
        "llm_api_calls": 0,
        "external_url_calls": 0,
        "production_changes": 0,
        "production_write": int(production_before != production_after),
    }
    return {
        "source_audit": source_audits,
        "attachment_enumeration": enumeration,
        "canonical_eligible_audit_items": [],
        "canonical_eligible_mail_master": [],
        "canonical_eligible_input_ids": [],
        "technical_projection_audit_items": technical_audits,
        "technical_projection_mail_master": technical_overlays,
        "technical_projection_input_ids": [{"message_id": row["message_id"]} for row in technical_overlays],
        "technical_cleanup": cleanup_records,
        "technical_classification": classification_records,
        "summary": summary,
    }


def write_attachment_list_results(results):
    result_dirs = ensure_result_dirs(str(STEP_DIR))
    result_dir = result_dirs["result"] / RESULT_SUBDIR
    result_dir.mkdir(parents=True, exist_ok=True)
    for filename, key in (
        ("source_audit.jsonl", "source_audit"),
        ("attachment_enumeration.jsonl", "attachment_enumeration"),
        ("canonical_eligible_audit_items.jsonl", "canonical_eligible_audit_items"),
        ("canonical_eligible_mail_master.jsonl", "canonical_eligible_mail_master"),
        ("canonical_eligible_input_ids.jsonl", "canonical_eligible_input_ids"),
        ("technical_projection_audit_items.jsonl", "technical_projection_audit_items"),
        ("technical_projection_mail_master.jsonl", "technical_projection_mail_master"),
        ("technical_projection_input_ids.jsonl", "technical_projection_input_ids"),
        ("technical_01-4_cleanup.jsonl", "technical_cleanup"),
        ("technical_02-1_classification.jsonl", "technical_classification"),
    ):
        write_jsonl(str(result_dir / filename), results[key])
    write_jsonl(str(result_dir / "replay_summary.jsonl"), [results["summary"]])


def main():
    results = build_attachment_list_results()
    summary = results["summary"]
    required = {
        "actual_deliveries": 3,
        "actual_observed_attachment_counts": [2, 4, 2],
        "actual_profile_counts": [2, 4, 2],
        "actual_declared_counts": [2, 4, 2],
        "actual_mapping_counts": [2, 4, 2],
        "actual_mapping_total": 8,
        "actual_station_audit_matches": 8,
        "false_substring_matches": 0,
        "actual_acquisition_statuses": ["UNVERIFIED"] * 3,
        "actual_container_enumeration_statuses": ["COMPLETE"] * 3,
        "actual_inline_structure_statuses": ["PASS"] * 3,
        "actual_attachment_mapping_statuses": ["PASS"] * 3,
        "actual_source_atomic_statuses": ["PARTIAL"] * 3,
        "actual_auto_union_eligible": False,
        "technical_projection_total": 8,
        "canonical_eligible_total": 0,
        "cross_item_contamination": 0,
        "cleanup_output": 8,
        "cleanup_nonempty": 8,
        "classification_output": 8,
        "resource_classified": 8,
        "project_classified": 0,
        "ambiguous_output": 0,
        "unknown_output": 0,
        "production_write": 0,
    }
    failures = [
        key + ":" + str(summary.get(key)) + ":expected:" + str(expected)
        for key, expected in required.items()
        if summary.get(key) != expected
    ]
    if not summary["derived_id_deterministic"]:
        failures.append("derived_id_determinism_failed")
    if failures:
        raise ValueError("ATTACHMENT_LIST actual replay failed:" + ";".join(failures))
    write_attachment_list_results(results)
    logger.ok(
        "ATTACHMENT_LIST actual replay OK: deliveries=3 observed=2/4/2 "
        "acquisition=UNVERIFIED mapping=8/8 eligible=0 technical_01-4=8/8 technical_02-1=8/8"
    )


if __name__ == "__main__":
    main()

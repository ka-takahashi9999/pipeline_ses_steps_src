#!/usr/bin/env python3
"""Run the stable LINK_BUNDLE contract and optional DrivenX observation."""

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
    STEP_DIR / "00_tool" / "adapters" / "link_bundle",
    STEP_DIR / "00_tool" / "canonicalize",
    STEP_DIR / "00_tool" / "core",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.file_utils import ensure_result_dirs
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger
from canonical_overlay import MAIL_MASTER_KEYS, build_canonical_overlay
from identity import normalize_content
from link_bundle_adapter import ADAPTER_ID, ADAPTER_VERSION, LinkBundleAdapter
from link_bundle_fixture_source import build_source_owned_fixtures
from run_offline_replay import DEFAULT_INPUT
from run_selective_pipeline_test import (
    _load_existing_modules,
    _production_artifact_snapshot,
)


logger = get_logger("99-1_link_bundle_variable_n")
CONFIG_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "companies"
    / "drivenx_link_bundle.config.json.example"
)
RESULT_SUBDIR = "link_bundle_variable_n"
FIXTURE_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "fixtures"
    / "redacted"
    / "link_bundle"
    / "drivenx.variable_n.fixture.jsonl.example"
)


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
        cleaned_body, _ = cleanup_module.cleanup_body(
            overlay["body_text"], cleanup_rules
        )
        if not cleaned_body:
            raise ValueError(
                f"01-4 removed LINK_BUNDLE item body: {overlay['message_id']}"
            )
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


def build_link_bundle_contract_results() -> Dict[str, Any]:
    """Build source-owned variable-N fixtures independently of Gmail actual."""
    adapter = LinkBundleAdapter.from_file(CONFIG_PATH)
    fixtures = build_source_owned_fixtures(read_jsonl_as_list(str(FIXTURE_PATH)))
    results = [adapter.parse(copy.deepcopy(fixture)) for fixture in fixtures]
    repeated = [adapter.parse(copy.deepcopy(fixture)) for fixture in fixtures]
    observed_distribution = [
        [
            result.source.get("section_counts", {}).get("resource"),
            result.source.get("section_counts", {}).get("project"),
        ]
        for result in results
    ]
    expected_distribution = [[0, 0], [1, 1], [2, 1], [1, 2], [10, 4], [4, 10]]
    overlays = [
        build_canonical_overlay(fixture, item)
        for fixture, result in zip(fixtures, results)
        for item in result.items
    ]
    cleanup, classification = _run_01_4_02_1(overlays)
    findings = []
    if observed_distribution != expected_distribution:
        findings.append("variable_n_role_distribution_mismatch")
    if any(result.status != "PARSED" for result in results):
        findings.append("stable_fixture_parse_mismatch")
    if results != repeated:
        findings.append("stable_fixture_idempotency_mismatch")
    deletion = copy.deepcopy(fixtures[2])
    del deletion["html_links"][4]
    deletion_result = adapter.parse(deletion)
    if deletion_result.status == "PARSED" or deletion_result.items:
        findings.append("middle_deletion_not_fail_closed")
    if len(cleanup) != len(overlays) or len(classification) != len(overlays):
        findings.append("stable_fixture_projection_count_mismatch")
    return {
        "summary": {
            "contract_status": "PASS" if not findings else "FAIL",
            "fixture_source_count": len(fixtures),
            "expected_role_distribution": expected_distribution,
            "observed_role_distribution": observed_distribution,
            "projected_item_count": len(overlays),
            "middle_deletion_fail_closed": not deletion_result.items,
            "finding_count": len(findings),
            "findings": findings,
        }
    }


def _audit_item(
    mail: Dict[str, Any],
    adapter: LinkBundleAdapter,
    result: Any,
    item: Dict[str, Any],
    projection_kind: str,
) -> Dict[str, Any]:
    return {
        "original_message_id": str(mail.get("message_id", "")),
        "logical_item_id": item["logical_item_id"],
        "derived_item_id": item["derived_item_id"],
        "item_index": item["item_index"],
        "section_type": item["section_type"],
        "source_type": "link_bundle",
        "source_company": adapter.config["source_company"],
        "source_fingerprint": result.source["source_fingerprint"],
        "delivery_semantics": result.source["delivery_semantics"],
        "acquisition_status": result.source["acquisition_status"],
        "source_acquisition_status": result.source["source_acquisition_status"],
        "container_enumeration_status": result.source[
            "container_enumeration_status"
        ],
        "role_classification_status": result.source["role_classification_status"],
        "source_atomic_status": result.source["source_atomic_status"],
        "auto_union_eligible": result.source["auto_union_eligible"],
        "projection_kind": projection_kind,
        "cardinality_evidence": result.source["cardinality_evidence"],
        "completeness_result": result.source["completeness_result"],
        "link_role_counts": result.source["link_role_counts"],
        "section_counts": result.source["section_counts"],
        "source_artifacts": result.source["source_artifacts"],
        "container_references": result.source["container_references"],
        "container_contracts": result.containers,
        "config_id": adapter.config["config_id"],
        "config_version": adapter.config["version"],
        "adapter_id": ADAPTER_ID,
        "adapter_version": ADAPTER_VERSION,
        "original_subject": str(mail.get("subject", "")),
        "original_timestamp": str(mail.get("date", "")),
        "body_fingerprint": item["body_fingerprint"],
        "artifact_set_fingerprint": item["artifact_set_fingerprint"],
        "version_relevant_artifact_set_fingerprint": item[
            "version_relevant_artifact_set_fingerprint"
        ],
        "version_fingerprint": item["version_fingerprint"],
        "content_fingerprint": item["content_fingerprint"],
        "parse_status": (
            result.status
            if projection_kind == "CANONICAL_ELIGIBLE"
            else "TECHNICAL_PROJECTION"
        ),
        "parse_reasons": result.reasons,
        "item_artifacts": item["item_artifacts"],
        "identity_evidence": item["identity_evidence"],
        "identity_durability": item["identity_durability"],
        "version_scope": item["version_scope"],
        "classification_context_evidence": item[
            "classification_context_evidence"
        ],
    }


def _canonical_contamination_count(
    items: List[Dict[str, Any]], adapter: LinkBundleAdapter
) -> int:
    failures = 0
    for item in items:
        expected = (
            adapter.config["section_context"][item["section_type"]]["body_context"]
            + "\n\n"
            + normalize_content(item["html_links"][0]["text"])
        )
        if item["body_text"] != expected:
            failures += 1
        if len(item["html_links"]) != 1:
            failures += 1
    return failures


def build_link_bundle_results(
    records: Iterable[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build fresh P4 results only from saved html_links snapshots."""
    adapter = LinkBundleAdapter.from_file(CONFIG_PATH)
    observation_exceptions: List[str] = []
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
    selected = sorted(
        (record for record in source_records if adapter.matches(record)),
        key=lambda record: str(record.get("message_id", "")),
    )
    production_before = _production_artifact_snapshot()
    source_audits: List[Dict[str, Any]] = []
    eligible_audits: List[Dict[str, Any]] = []
    eligible_overlays: List[Dict[str, Any]] = []
    technical_audits: List[Dict[str, Any]] = []
    technical_overlays: List[Dict[str, Any]] = []
    link_enumeration: List[Dict[str, Any]] = []
    eligible_items: List[Dict[str, Any]] = []
    technical_items: List[Dict[str, Any]] = []
    statuses = Counter()
    deterministic = True
    for mail in selected:
        try:
            result = adapter.parse(mail)
            repeat = adapter.parse(mail)
        except Exception as exc:  # rotating actual is observation-only
            statuses["SYSTEM_FAILURE"] += 1
            observation_exceptions.append("parser_exception:" + type(exc).__name__)
            source_audits.append(
                {
                    "original_message_id": str(mail.get("message_id", "")),
                    "parse_status": "SYSTEM_FAILURE",
                    "parse_reasons": ["observation_parser_exception"],
                    "source": {},
                    "container_contracts": [],
                }
            )
            continue
        deterministic = deterministic and result == repeat
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
        link_enumeration.extend(
            {
                "original_message_id": str(mail.get("message_id", "")),
                **row,
            }
            for row in result.link_enumeration
        )
        for item in result.items:
            eligible_audits.append(
                _audit_item(mail, adapter, result, item, "CANONICAL_ELIGIBLE")
            )
            eligible_overlays.append(build_canonical_overlay(mail, item))
            eligible_items.append(item)
        for item in result.technical_projection_items:
            technical_audits.append(
                _audit_item(mail, adapter, result, item, "TECHNICAL_PROJECTION")
            )
            technical_overlays.append(build_canonical_overlay(mail, item))
            technical_items.append(item)

    eligible_overlays.sort(key=lambda row: row["message_id"])
    eligible_audits.sort(key=lambda row: row["derived_item_id"])
    technical_overlays.sort(key=lambda row: row["message_id"])
    technical_audits.sort(key=lambda row: row["derived_item_id"])
    try:
        cleanup_records, classification_records = _run_01_4_02_1(
            technical_overlays
        )
    except Exception as exc:  # projection failure is an observation finding
        observation_exceptions.append("projection_exception:" + type(exc).__name__)
        cleanup_records = []
        classification_records = []
    classification_by_id = {
        row["message_id"]: row["mail_type"] for row in classification_records
    }
    section_by_id = {
        item["derived_item_id"]: item["section_type"] for item in technical_items
    }
    distribution = Counter(row["mail_type"] for row in classification_records)
    resource_correct = sum(
        classification_by_id.get(item_id) == "resource"
        for item_id, section in section_by_id.items()
        if section == "resource"
    )
    project_correct = sum(
        classification_by_id.get(item_id) == "project"
        for item_id, section in section_by_id.items()
        if section == "project"
    )
    role_counts = Counter(row["role"] for row in link_enumeration)
    item_hrefs = [
        row["href"]
        for row in link_enumeration
        if row["role"] in {"RESOURCE_ITEM", "PROJECT_ITEM"}
    ]
    production_after = _production_artifact_snapshot()
    acquisition_states = {
        audit["source"].get("source_acquisition_status") for audit in source_audits
    }
    container_states = {
        audit["source"].get("container_enumeration_status") for audit in source_audits
    }
    role_states = {
        audit["source"].get("role_classification_status") for audit in source_audits
    }
    atomic_states = {
        audit["source"].get("source_atomic_status") for audit in source_audits
    }
    summary = {
        "actual_availability": (
            "OBSERVATION_UNAVAILABLE"
            if any(
                finding.startswith("source_read_exception:")
                for finding in observation_exceptions
            )
            else ("OBSERVATION" if selected else "DATA_UNAVAILABLE")
        ),
        "actual_observation_count": len(selected),
        "input_sources": len(selected),
        "parsed_sources": statuses["PARSED"],
        "partial_sources": statuses["PARTIAL"],
        "human_review_sources": statuses["HUMAN_REVIEW"],
        "system_failure_sources": statuses["SYSTEM_FAILURE"],
        "actual_links": len(link_enumeration),
        "links_classified": sum(row["role"] != "UNKNOWN" for row in link_enumeration),
        "resource_headers": role_counts["RESOURCE_HEADER"],
        "project_headers": role_counts["PROJECT_HEADER"],
        "resource_items": role_counts["RESOURCE_ITEM"],
        "project_items": role_counts["PROJECT_ITEM"],
        "action_links": role_counts["ACTION"],
        "shared_links": role_counts["SHARED"],
        "non_item_role_links": role_counts["NON_ITEM"],
        "non_item_links": sum(
            role_counts[role]
            for role in (
                "RESOURCE_HEADER",
                "PROJECT_HEADER",
                "ACTION",
                "SHARED",
                "NON_ITEM",
            )
        ),
        "unknown_links": role_counts["UNKNOWN"],
        "duplicate_item_locators": len(item_hrefs) - len(set(item_hrefs)),
        "actual_acquisition_status": (
            next(iter(acquisition_states)) if len(acquisition_states) == 1 else "MIXED"
        ),
        "actual_container_enumeration_status": (
            next(iter(container_states)) if len(container_states) == 1 else "MIXED"
        ),
        "actual_role_classification_status": (
            next(iter(role_states)) if len(role_states) == 1 else "MIXED"
        ),
        "actual_source_atomic_status": (
            next(iter(atomic_states)) if len(atomic_states) == 1 else "MIXED"
        ),
        "actual_auto_union_eligible": any(
            audit["source"].get("auto_union_eligible") is True
            for audit in source_audits
        ),
        "observed_canonical_candidates": len(technical_items),
        "technical_projection_resources": sum(
            item["section_type"] == "resource" for item in technical_items
        ),
        "technical_projection_projects": sum(
            item["section_type"] == "project" for item in technical_items
        ),
        "technical_projection_total": len(technical_items),
        "canonical_eligible_resources": sum(
            item["section_type"] == "resource" for item in eligible_items
        ),
        "canonical_eligible_projects": sum(
            item["section_type"] == "project" for item in eligible_items
        ),
        "canonical_eligible_total": len(eligible_items),
        "cross_item_contamination": _canonical_contamination_count(
            technical_items, adapter
        ),
        "logical_distinct": len({item["logical_item_id"] for item in technical_items}),
        "derived_distinct": len({item["derived_item_id"] for item in technical_items}),
        "derived_id_deterministic": deterministic,
        "cleanup_output": len(cleanup_records),
        "cleanup_nonempty": sum(bool(row["body_text"]) for row in cleanup_records),
        "classification_output": len(classification_records),
        "resource_classified_correct": resource_correct,
        "project_classified_correct": project_correct,
        "ambiguous_output": distribution["ambiguous"],
        "unknown_output": distribution["unknown"],
        "llm_api_calls": 0,
        "external_url_calls": 0,
        "production_changes": 0,
        "production_write": int(production_before != production_after),
    }
    actual_findings = list(observation_exceptions)
    source_ids = [str(record.get("message_id", "")) for record in selected]
    if len(source_ids) != len(set(source_ids)):
        actual_findings.append("duplicate_source_message_id")
    if summary["system_failure_sources"]:
        actual_findings.append("parser_system_failure")
    if summary["duplicate_item_locators"]:
        actual_findings.append("duplicate_item_locator")
    if summary["unknown_links"]:
        actual_findings.append("unknown_link_role")
    if summary["cross_item_contamination"]:
        actual_findings.append("cross_item_contamination")
    if not summary["derived_id_deterministic"]:
        actual_findings.append("derived_id_nondeterministic")
    technical_ids = [record.get("message_id") for record in technical_overlays]
    if len(technical_ids) != len(set(technical_ids)):
        actual_findings.append("duplicate_technical_message_id")
    if any(set(record) != MAIL_MASTER_KEYS for record in technical_overlays):
        actual_findings.append("technical_overlay_schema_mismatch")
    indexes_by_source: Dict[str, List[int]] = {}
    for row in link_enumeration:
        indexes_by_source.setdefault(
            str(row.get("original_message_id", "")), []
        ).append(row.get("index"))
    if any(
        indexes != list(range(len(indexes)))
        for indexes in indexes_by_source.values()
    ):
        actual_findings.append("ordered_enumeration_mismatch")
    if any(
        row.get("mail_type") not in {"resource", "project", "ambiguous", "unknown"}
        for row in classification_records
    ):
        actual_findings.append("classification_status_out_of_schema")
    summary.update(
        {
            "actual_observation_finding_count": len(actual_findings),
            "actual_observation_findings": actual_findings,
            "actual_runtime_fixed_oracle": 0,
        }
    )
    return {
        "source_audit": source_audits,
        "link_enumeration": link_enumeration,
        "canonical_eligible_audit_items": eligible_audits,
        "canonical_eligible_mail_master": eligible_overlays,
        "canonical_eligible_input_ids": [
            {"message_id": record["message_id"]} for record in eligible_overlays
        ],
        "technical_projection_audit_items": technical_audits,
        "technical_projection_mail_master": technical_overlays,
        "technical_projection_input_ids": [
            {"message_id": record["message_id"]} for record in technical_overlays
        ],
        "technical_cleanup": cleanup_records,
        "technical_classification": classification_records,
        "summary": summary,
    }


def write_link_bundle_results(results: Dict[str, Any]) -> None:
    result_dirs = ensure_result_dirs(str(STEP_DIR))
    result_dir = result_dirs["result"] / RESULT_SUBDIR
    result_dir.mkdir(parents=True, exist_ok=True)
    for filename, key in (
        ("source_audit.jsonl", "source_audit"),
        ("link_enumeration.jsonl", "link_enumeration"),
        ("canonical_eligible_audit_items.jsonl", "canonical_eligible_audit_items"),
        ("canonical_eligible_mail_master.jsonl", "canonical_eligible_mail_master"),
        ("canonical_eligible_input_ids.jsonl", "canonical_eligible_input_ids"),
        (
            "technical_projection_audit_items.jsonl",
            "technical_projection_audit_items",
        ),
        (
            "technical_projection_mail_master.jsonl",
            "technical_projection_mail_master",
        ),
        (
            "technical_projection_input_ids.jsonl",
            "technical_projection_input_ids",
        ),
        ("technical_01-4_cleanup.jsonl", "technical_cleanup"),
        ("technical_02-1_classification.jsonl", "technical_classification"),
    ):
        write_jsonl(str(result_dir / filename), results[key])
    for legacy_canonical_filename in (
        "audit_items.jsonl",
        "derived_mail_master.jsonl",
        "derived_input_ids.jsonl",
        "01-4_cleanup.jsonl",
        "02-1_classification.jsonl",
    ):
        write_jsonl(str(result_dir / legacy_canonical_filename), [])
    write_jsonl(str(result_dir / "replay_summary.jsonl"), [results["summary"]])


def main() -> None:
    contract = build_link_bundle_contract_results()
    results = build_link_bundle_results()
    summary = results["summary"]
    summary["contract_status"] = contract["summary"]["contract_status"]
    summary["contract_role_distribution"] = contract["summary"][
        "observed_role_distribution"
    ]
    summary["contract_finding_count"] = contract["summary"]["finding_count"]
    if contract["summary"]["findings"]:
        raise ValueError(
            "LINK_BUNDLE stable contract failed: "
            + ";".join(contract["summary"]["findings"])
        )
    write_link_bundle_results(results)
    logger.ok(
        "LINK_BUNDLE contract PASS: variable_N role distribution actual="
        + str(summary["actual_availability"])
        + " observed_sources="
        + str(summary["actual_observation_count"])
        + " observed_links="
        + str(summary["actual_links"])
        + " findings="
        + str(summary["actual_observation_finding_count"])
    )


if __name__ == "__main__":
    main()

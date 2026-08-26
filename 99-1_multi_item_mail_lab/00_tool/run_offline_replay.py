#!/usr/bin/env python3
"""Offline replay for the test-only 99-1 inline-summary adapter."""

import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
for import_path in (
    PROJECT_ROOT,
    STEP_DIR / "00_tool" / "adapters" / "inline_summary",
    STEP_DIR / "00_tool" / "canonicalize",
    STEP_DIR / "00_tool" / "core",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.file_utils import ensure_result_dirs, write_execution_time
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger
from canonical_overlay import MAIL_MASTER_KEYS, build_canonical_overlay
from inline_summary_adapter import ADAPTER_ID, ADAPTER_VERSION, InlineSummaryAdapter


logger = get_logger("99-1_multi_item_mail_lab")
DEFAULT_INPUT = (
    PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)
DEFAULT_CONFIG = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "companies"
    / "netwisdom.config.json.example"
)


def _audit_record(
    mail: Dict[str, Any],
    adapter: InlineSummaryAdapter,
    status: str,
    reasons: List[str],
    item: Dict[str, Any] = None,
    source: Dict[str, Any] = None,
    containers: List[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    item = item or {}
    source = source or {}
    return {
        "original_message_id": str(mail.get("message_id", "")),
        "logical_item_id": item.get("logical_item_id", ""),
        "derived_item_id": item.get("derived_item_id", ""),
        "item_index": item.get("item_index", -1),
        "item_type": adapter.config["item_type"],
        "source_type": "inline_summary",
        "source_company": adapter.config["source_company"],
        "source_fingerprint": source.get("source_fingerprint", ""),
        "delivery_semantics": source.get("delivery_semantics", "UNKNOWN"),
        "acquisition_status": source.get("acquisition_status", "INCOMPLETE"),
        "cardinality_evidence": source.get("cardinality_evidence", []),
        "completeness_result": source.get("completeness_result", {}),
        "container_references": source.get("container_references", []),
        "container_contracts": containers or [],
        "config_id": adapter.config["config_id"],
        "config_version": adapter.config["version"],
        "adapter_id": ADAPTER_ID,
        "adapter_version": ADAPTER_VERSION,
        "original_subject": str(mail.get("subject", "")),
        "original_timestamp": str(mail.get("date", "")),
        "body_fingerprint": item.get("body_fingerprint", ""),
        "attachment_fingerprint": item.get("attachment_fingerprint", ""),
        "artifact_set_fingerprint": item.get("artifact_set_fingerprint", ""),
        "version_relevant_artifact_set_fingerprint": item.get(
            "version_relevant_artifact_set_fingerprint", ""
        ),
        "version_fingerprint": item.get("version_fingerprint", ""),
        "content_fingerprint": item.get("content_fingerprint", ""),
        "parse_status": status,
        "parse_reasons": reasons,
        "attachment_mapping": item.get("attachment_mapping", {}),
        "item_artifacts": item.get("item_artifacts", []),
        "identity_evidence": item.get("identity_evidence", {}),
    }


def process_records(
    records: Iterable[Dict[str, Any]], adapter: InlineSummaryAdapter
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    selected = sorted(
        (record for record in records if adapter.matches(record)),
        key=lambda record: str(record.get("message_id", "")),
    )
    audits: List[Dict[str, Any]] = []
    occurrences: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    mail_statuses = Counter()
    expected_occurrences = 0

    for mail in selected:
        result = adapter.parse(mail)
        mail_statuses[result.status] += 1
        validated_count = result.source.get("completeness_result", {}).get(
            "expected_count"
        )
        if isinstance(validated_count, int) and not isinstance(validated_count, bool):
            expected_occurrences += validated_count
        if result.status == "PARSED":
            for item in result.items:
                audits.append(
                    _audit_record(
                        mail,
                        adapter,
                        result.status,
                        result.reasons,
                        item,
                        result.source,
                        result.containers,
                    )
                )
                occurrences.append((mail, item))
        else:
            audits.append(
                _audit_record(
                    mail,
                    adapter,
                    result.status,
                    result.reasons,
                    source=result.source,
                    containers=result.containers,
                )
            )

    overlay_by_id: Dict[str, Dict[str, Any]] = {}
    identity_payload_by_id: Dict[str, Tuple[str, str, str, str, str]] = {}
    for mail, item in occurrences:
        derived_id = item["derived_item_id"]
        identity_payload = (
            item["logical_item_id"],
            item["body_fingerprint"],
            item["artifact_set_fingerprint"],
            item["version_fingerprint"],
            item["body_text"],
        )
        previous_payload = identity_payload_by_id.get(derived_id)
        if previous_payload is not None and previous_payload != identity_payload:
            raise ValueError(f"derived_item_id collision: {derived_id}")
        identity_payload_by_id[derived_id] = identity_payload
        if derived_id not in overlay_by_id:
            overlay_by_id[derived_id] = build_canonical_overlay(mail, item)

    overlays = [overlay_by_id[derived_id] for derived_id in sorted(overlay_by_id)]
    input_ids = [{"message_id": record["message_id"]} for record in overlays]
    logical_ids = {item["logical_item_id"] for _, item in occurrences}
    body_fingerprints = {item["body_fingerprint"] for _, item in occurrences}
    attachment_fingerprints = {
        item["attachment_fingerprint"]
        for _, item in occurrences
        if item["attachment_fingerprint"]
    }
    artifact_set_fingerprints = {
        item["artifact_set_fingerprint"] for _, item in occurrences
    }
    version_fingerprints = {item["version_fingerprint"] for _, item in occurrences}
    logical_versions = {
        (item["logical_item_id"], item["version_fingerprint"])
        for _, item in occurrences
    }
    derived_ids = [item["derived_item_id"] for _, item in occurrences]
    artifacts = {
        "audit_items": audits,
        "derived_mail_master": overlays,
        "derived_input_ids": input_ids,
    }
    stats = {
        "input_mails": len(selected),
        "expected_item_occurrences": expected_occurrences,
        "parsed_mails": mail_statuses["PARSED"],
        "partial_mails": mail_statuses["PARTIAL"],
        "human_review_mails": mail_statuses["HUMAN_REVIEW"],
        "unsupported_mails": mail_statuses["UNSUPPORTED"],
        "system_failure_mails": mail_statuses["SYSTEM_FAILURE"],
        "parsed_occurrences": len(occurrences),
        "derived_item_occurrences": len(occurrences),
        "logical_distinct": len(logical_ids),
        "body_distinct": len(body_fingerprints),
        "attachment_distinct": len(attachment_fingerprints),
        "artifact_set_distinct": len(artifact_set_fingerprints),
        "version_distinct": len(logical_versions),
        "content_distinct": len(version_fingerprints),
        "derived_versions": len(set(derived_ids)),
        "attachment_mapping_success": sum(
            item["attachment_mapping"].get("status") == "MAPPED"
            for _, item in occurrences
        ),
        "duplicate_occurrences": len(derived_ids) - len(set(derived_ids)),
        "duplicate_derived_id_in_overlay": len(overlays)
        - len({record["message_id"] for record in overlays}),
        "missing_items": max(0, expected_occurrences - len(occurrences)),
        "canonical_overlay_schema_ok": all(
            set(record) == MAIL_MASTER_KEYS for record in overlays
        ),
    }
    return artifacts, stats


def _write_artifacts(artifacts: Dict[str, List[Dict[str, Any]]], stats: Dict[str, Any]) -> None:
    result_dirs = ensure_result_dirs(str(STEP_DIR))
    result_dir = result_dirs["result"]
    write_jsonl(str(result_dir / "audit_items.jsonl"), artifacts["audit_items"])
    write_jsonl(
        str(result_dir / "derived_mail_master.jsonl"),
        artifacts["derived_mail_master"],
    )
    write_jsonl(
        str(result_dir / "derived_input_ids.jsonl"), artifacts["derived_input_ids"]
    )
    write_jsonl(str(result_dir / "replay_summary.jsonl"), [stats])


def main() -> None:
    started = time.monotonic()
    if not DEFAULT_INPUT.exists():
        raise FileNotFoundError(f"saved production mail master not found: {DEFAULT_INPUT}")
    adapter = InlineSummaryAdapter.from_file(DEFAULT_CONFIG)
    records = read_jsonl_as_list(str(DEFAULT_INPUT))

    first_artifacts, first_stats = process_records(records, adapter)
    second_artifacts, second_stats = process_records(list(reversed(records)), adapter)
    idempotency_ok = first_artifacts == second_artifacts and first_stats == second_stats
    first_stats["idempotency_ok"] = idempotency_ok

    expected_mails = adapter.config["offline_expected_input_mails"]
    if first_stats["input_mails"] != expected_mails:
        raise ValueError(
            f"selected mail count {first_stats['input_mails']} != expected {expected_mails}"
        )
    if not idempotency_ok:
        raise ValueError("offline replay is not idempotent")

    _write_artifacts(first_artifacts, first_stats)
    elapsed = time.monotonic() - started
    write_execution_time(
        str(STEP_DIR / "99_execution_time"),
        "99-1_multi_item_mail_lab",
        elapsed,
        first_stats["derived_item_occurrences"],
    )
    logger.ok(
        "offline replay OK: "
        f"mails={first_stats['input_mails']} "
        f"occurrences={first_stats['derived_item_occurrences']} "
        f"logical_distinct={first_stats['logical_distinct']} "
        f"version_distinct={first_stats['version_distinct']}"
    )


if __name__ == "__main__":
    main()

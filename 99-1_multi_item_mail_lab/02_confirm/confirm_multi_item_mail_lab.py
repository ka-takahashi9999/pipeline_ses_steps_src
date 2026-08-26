#!/usr/bin/env python3
"""Confirm 99-1 offline replay counts, schema, mapping, and identities."""

import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List


STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
for import_path in (
    PROJECT_ROOT,
    STEP_DIR / "00_tool" / "canonicalize",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.json_utils import read_jsonl_as_list
from common.logger import get_logger
from canonical_overlay import MAIL_MASTER_KEYS
from identity import canonical_subject, derived_item_id


logger = get_logger("confirm_99-1_multi_item_mail_lab")
RESULT_DIR = STEP_DIR / "01_result"
AUDIT_PATH = RESULT_DIR / "audit_items.jsonl"
OVERLAY_PATH = RESULT_DIR / "derived_mail_master.jsonl"
INPUT_IDS_PATH = RESULT_DIR / "derived_input_ids.jsonl"
SUMMARY_PATH = RESULT_DIR / "replay_summary.jsonl"

AUDIT_KEYS = {
    "original_message_id",
    "logical_item_id",
    "derived_item_id",
    "item_index",
    "item_type",
    "source_type",
    "source_company",
    "config_id",
    "config_version",
    "adapter_id",
    "adapter_version",
    "original_subject",
    "original_timestamp",
    "body_fingerprint",
    "attachment_fingerprint",
    "version_fingerprint",
    "content_fingerprint",
    "parse_status",
    "parse_reasons",
    "attachment_mapping",
}


def _check(condition: bool, message: str, failures: List[str]) -> None:
    if not condition:
        failures.append(message)
        logger.error(message)


def main() -> None:
    failures: List[str] = []
    for path in (AUDIT_PATH, OVERLAY_PATH, INPUT_IDS_PATH, SUMMARY_PATH):
        _check(path.exists(), f"missing result: {path}", failures)
    if failures:
        sys.exit(1)

    audits: List[Dict[str, Any]] = read_jsonl_as_list(str(AUDIT_PATH))
    overlays: List[Dict[str, Any]] = read_jsonl_as_list(str(OVERLAY_PATH))
    input_ids: List[Dict[str, Any]] = read_jsonl_as_list(str(INPUT_IDS_PATH))
    summaries = read_jsonl_as_list(str(SUMMARY_PATH))
    _check(len(summaries) == 1, "summary must contain exactly one record", failures)
    if not summaries:
        sys.exit(1)
    summary = summaries[0]

    statuses = Counter(record.get("parse_status") for record in audits)
    original_ids = {record.get("original_message_id") for record in audits}
    logical_ids = {record.get("logical_item_id") for record in audits}
    body_fingerprints = {record.get("body_fingerprint") for record in audits}
    attachment_fingerprints = {
        record.get("attachment_fingerprint") for record in audits
    }
    version_fingerprints = {record.get("version_fingerprint") for record in audits}
    logical_versions = {
        (record.get("logical_item_id"), record.get("version_fingerprint"))
        for record in audits
    }
    overlay_ids = [record.get("message_id") for record in overlays]
    overlay_by_id = {record.get("message_id"): record for record in overlays}

    _check(len(original_ids) == 4, "input mail count must be 4", failures)
    _check(len(audits) == 8, "audit item occurrence count must be 8", failures)
    _check(statuses == {"PARSED": 8}, "all 8 occurrences must be PARSED", failures)
    _check(len(logical_ids) == 2, "logical distinct must be 2", failures)
    _check(len(body_fingerprints) == 2, "body distinct must be 2", failures)
    _check(
        all(
            record.get("content_fingerprint") == record.get("version_fingerprint")
            for record in audits
        ),
        "content fingerprint must mean version fingerprint",
        failures,
    )
    _check(
        all(
            isinstance(fingerprint, str) and fingerprint.startswith("sha256:")
            for fingerprint in (
                body_fingerprints | attachment_fingerprints | version_fingerprints
            )
        ),
        "all audit fingerprints must be SHA-256 values",
        failures,
    )
    _check(
        all(record.get("attachment_mapping", {}).get("status") == "MAPPED" for record in audits),
        "all 8 attachment mappings must be MAPPED",
        failures,
    )
    _check(all(set(record) == AUDIT_KEYS for record in audits), "audit schema mismatch", failures)
    _check(
        len(overlays) == len(logical_versions),
        "overlay count must equal distinct attachment-aware versions",
        failures,
    )
    _check(len(overlay_ids) == len(set(overlay_ids)), "duplicate derived ID in overlay", failures)
    _check(
        all(set(record) == MAIL_MASTER_KEYS for record in overlays),
        "mail master overlay schema mismatch",
        failures,
    )
    _check(
        all(
            isinstance(record.get("attachments"), list)
            and len(record["attachments"]) == 1
            for record in overlays
        ),
        "every overlay item must have exactly one attachment",
        failures,
    )
    _check(
        input_ids == [{"message_id": message_id} for message_id in overlay_ids],
        "derived input IDs do not match overlay order",
        failures,
    )
    _check(
        all(
            record.get("derived_item_id")
            == derived_item_id(
                record.get("logical_item_id", ""),
                record.get("version_fingerprint", ""),
            )
            for record in audits
        ),
        "derived item ID is not based on logical ID and version fingerprint",
        failures,
    )
    _check(
        all(
            record.get("derived_item_id") in overlay_by_id
            and overlay_by_id[record["derived_item_id"]].get("subject")
            == canonical_subject(
                record.get("source_company", ""),
                record.get("item_type", ""),
                record.get("logical_item_id", ""),
                record.get("version_fingerprint", ""),
            )
            for record in audits
        ),
        "canonical subject is not based on logical ID and version fingerprint",
        failures,
    )
    _check(summary.get("idempotency_ok") is True, "idempotency must pass", failures)
    _check(summary.get("parsed_mails") == 4, "parsed mail count must be 4", failures)
    _check(summary.get("partial_mails") == 0, "PARTIAL mail count must be 0", failures)
    _check(
        summary.get("human_review_mails") == 0,
        "HUMAN_REVIEW mail count must be 0",
        failures,
    )
    _check(
        summary.get("attachment_mapping_success") == 8,
        "attachment mapping success count must be 8",
        failures,
    )
    _check(
        summary.get("duplicate_occurrences") == len(audits) - len(overlays),
        "duplicate occurrence count mismatch",
        failures,
    )
    _check(
        summary.get("derived_versions") == len(logical_versions),
        "derived version count mismatch",
        failures,
    )
    _check(summary.get("missing_items") == 0, "missing item count must be 0", failures)
    _check(
        summary.get("duplicate_derived_id_in_overlay") == 0,
        "overlay duplicate derived ID count must be 0",
        failures,
    )

    logger.info(
        "counts: input=4 expected=8 parsed=8 "
        f"overlay={len(overlays)} logical=2 mapped=8"
    )
    representative_records = [
        record
        for record in audits
        if record.get("attachment_mapping", {}).get("status") == "MAPPED"
    ]
    for record in representative_records[:3]:
        logger.info(
            "representative: "
            f"item_index={record['item_index']} "
            f"derived_id={record['derived_item_id']} "
            f"mapping={record['attachment_mapping']['status']}"
        )

    if failures:
        logger.error(f"confirm NG: failures={len(failures)}")
        sys.exit(1)
    logger.ok("confirm OK")


if __name__ == "__main__":
    main()

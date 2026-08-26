#!/usr/bin/env python3
"""Source-owned ordered attachment manifest contract for the test-only lab."""

import hashlib
import json
import re
from typing import Any, Dict, List, Sequence


MANIFEST_FIELD = "attachment_acquisition_manifest"
MANIFEST_SCHEMA_VERSION = "attachment_acquisition_manifest.v1"
SHA256_DIGEST_PATTERN = re.compile(r"\Asha256:[0-9a-fA-F]{64}\Z")


def validate_attachment_entry(entry: Any, position: int) -> List[str]:
    """Validate one authoritative entry before any lossy normalization."""
    prefix = "manifest_entry:" + str(position) + ":"
    if not isinstance(entry, dict):
        return [prefix + "not_object"]
    reasons: List[str] = []
    for field_name in ("source_entry_id", "filename", "mime_type"):
        value = entry.get(field_name)
        if not isinstance(value, str) or not value.strip():
            reasons.append(prefix + field_name + "_invalid")
    declared_size = entry.get("declared_size")
    if (
        isinstance(declared_size, bool)
        or not isinstance(declared_size, int)
        or declared_size < 0
    ):
        reasons.append(prefix + "declared_size_invalid")
    content_digest = entry.get("content_digest")
    if (
        not isinstance(content_digest, str)
        or SHA256_DIGEST_PATTERN.fullmatch(content_digest) is None
    ):
        reasons.append(prefix + "content_digest_invalid")
    return reasons


def validate_authoritative_attachment_entries(entries: Any) -> List[str]:
    """Return entry-contract failures without normalizing missing values."""
    if not isinstance(entries, list):
        return ["manifest_authoritative_entries_missing"]
    return [
        reason
        for position, entry in enumerate(entries)
        for reason in validate_attachment_entry(entry, position)
    ]


def _sha256_json(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def canonical_attachment_entry(entry: Any, position: int) -> Dict[str, Any]:
    """Normalize only manifest fields while retaining sequence and duplicates."""
    value = entry if isinstance(entry, dict) else {}
    return {
        "position": position,
        "source_entry_id": value.get("source_entry_id", ""),
        "filename": value.get("filename", ""),
        "mime_type": value.get("mime_type", ""),
        "declared_size": value.get("declared_size"),
        "content_digest": value.get("content_digest", ""),
        "disposition": value.get("disposition", ""),
        "content_id": value.get("content_id", ""),
    }


def canonical_ordered_entries(entries: Sequence[Any]) -> List[Dict[str, Any]]:
    return [
        canonical_attachment_entry(entry, position)
        for position, entry in enumerate(entries)
    ]


def ordered_attachment_digest(entries: Sequence[Any]) -> str:
    """Hash an ordered list; no sorting or set conversion is permitted."""
    return _sha256_json(canonical_ordered_entries(entries))


def source_payload_digest(
    source_record: Dict[str, Any], authoritative_entries: Sequence[Any]
) -> str:
    """Digest the source envelope plus the source-owned attachment sequence."""
    payload = {
        "source_id": source_record.get("message_id", ""),
        "from": source_record.get("from", ""),
        "subject": source_record.get("subject", ""),
        "body_text": source_record.get("body_text", ""),
        "authoritative_attachment_entries": canonical_ordered_entries(
            authoritative_entries
        ),
    }
    return _sha256_json(payload)

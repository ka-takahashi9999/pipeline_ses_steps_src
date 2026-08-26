#!/usr/bin/env python3
"""Authoritative fixture source for ATTACHMENT_LIST acquisition evidence."""

import copy
from typing import Any, Dict, Iterable, List

from attachment_manifest_contract import (
    MANIFEST_FIELD,
    MANIFEST_SCHEMA_VERSION,
    canonical_ordered_entries,
    ordered_attachment_digest,
    source_payload_digest,
)
from identity import attachment_fingerprint


def _authoritative_entry(attachment: Dict[str, Any], position: int) -> Dict[str, Any]:
    if not isinstance(attachment, dict):
        raise ValueError("authoritative attachment must be an object")
    return {
        "position": position,
        "source_entry_id": attachment.get(
            "source_entry_id", "part-attachment-" + str(position)
        ),
        "filename": attachment.get("filename", ""),
        "mime_type": attachment.get("mime_type", ""),
        "declared_size": attachment.get("size"),
        "content_digest": attachment_fingerprint(attachment),
        "disposition": attachment.get("disposition", ""),
        "content_id": attachment.get("content_id", ""),
    }


def build_source_owned_fixture(source_definition: Dict[str, Any]) -> Dict[str, Any]:
    """Build manifest first from authority, then independently copy the snapshot."""
    authoritative_attachments = copy.deepcopy(
        source_definition.get("authoritative_attachments", [])
    )
    authoritative_entries = [
        _authoritative_entry(attachment, position)
        for position, attachment in enumerate(authoritative_attachments)
    ]
    canonical_entries = canonical_ordered_entries(authoritative_entries)
    manifest = {
        "source_id": source_definition.get("message_id", ""),
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "source_payload_digest": source_payload_digest(
            source_definition, canonical_entries
        ),
        "acquisition_status": "COMPLETE",
        "extractor_status": "COMPLETE",
        "expected_ordered_count": len(canonical_entries),
        "expected_ordered_digest": ordered_attachment_digest(canonical_entries),
        "authoritative_attachment_entries": canonical_entries,
        "reasons": [],
    }
    fixture = copy.deepcopy(source_definition)
    fixture.pop("authoritative_attachments", None)
    fixture[MANIFEST_FIELD] = manifest
    fixture["attachments"] = copy.deepcopy(authoritative_attachments)
    return fixture


def build_source_owned_fixtures(
    source_definitions: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    return [build_source_owned_fixture(record) for record in source_definitions]

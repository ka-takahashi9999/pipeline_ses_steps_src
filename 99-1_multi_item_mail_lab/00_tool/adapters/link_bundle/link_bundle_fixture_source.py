#!/usr/bin/env python3
"""Source-owned fixture generation for LINK_BUNDLE acquisition evidence."""

import copy
from typing import Any, Dict, Iterable, List

from link_bundle_manifest_contract import (
    MANIFEST_FIELD,
    MANIFEST_SCHEMA_VERSION,
    ordered_entry_digest,
)


def build_source_owned_fixture(source_definition: Dict[str, Any]) -> Dict[str, Any]:
    """Create the manifest from authoritative entries before making the snapshot."""
    expected_entries = copy.deepcopy(source_definition.get("html_links", []))
    source_id = source_definition.get("message_id")
    manifest = {
        "source_id": source_id,
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "acquisition_status": "COMPLETE",
        "extractor_status": "COMPLETE",
        "ordered_entry_count": len(expected_entries),
        "ordered_entry_digest": ordered_entry_digest(expected_entries),
        "reasons": [],
    }
    snapshot = copy.deepcopy(expected_entries)
    fixture = copy.deepcopy(source_definition)
    fixture[MANIFEST_FIELD] = manifest
    fixture["html_links"] = snapshot
    return fixture


def build_source_owned_fixtures(
    source_definitions: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    return [build_source_owned_fixture(record) for record in source_definitions]

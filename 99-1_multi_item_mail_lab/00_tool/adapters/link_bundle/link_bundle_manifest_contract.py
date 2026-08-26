#!/usr/bin/env python3
"""Ordered acquisition-manifest digest contract for LINK_BUNDLE sources."""

import hashlib
import json
from typing import Any, Dict, List, Sequence

from identity import normalize_content


MANIFEST_FIELD = "link_bundle_acquisition_manifest"
MANIFEST_SCHEMA_VERSION = "link_bundle_acquisition_manifest.v1"


def canonical_ordered_entries(entries: Sequence[Any]) -> List[Dict[str, Any]]:
    """Keep position and duplicates while normalizing identity-bearing fields."""
    canonical: List[Dict[str, Any]] = []
    for position, entry in enumerate(entries):
        link = entry if isinstance(entry, dict) else {}
        title = link.get("text")
        href = link.get("href")
        canonical.append(
            {
                "position": position,
                "normalized_title": normalize_content(title)
                if isinstance(title, str)
                else "",
                "normalized_locator": normalize_content(href)
                if isinstance(href, str)
                else "",
            }
        )
    return canonical


def ordered_entry_digest(entries: Sequence[Any]) -> str:
    """Hash the ordered sequence; this is intentionally not a set digest."""
    encoded = json.dumps(
        canonical_ordered_entries(entries),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()

#!/usr/bin/env python3
"""Deterministic identities for test-only multi-item mail derivation."""

import hashlib
import re
import unicodedata


def normalize_content(value: str) -> str:
    """Normalize representation-only differences while preserving item content."""
    text = unicodedata.normalize("NFKC", value or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.rstrip() for line in text.split("\n")]
    normalized = "\n".join(lines).strip()
    return re.sub(r"\n{3,}", "\n\n", normalized)


def normalize_block_identifier(value: str) -> str:
    """Normalize an anchor identifier without exposing it in generated IDs."""
    text = unicodedata.normalize("NFKC", value or "").casefold()
    return "".join(character for character in text if character.isalnum())


def content_fingerprint(body_text: str) -> str:
    normalized = normalize_content(body_text)
    return "sha256:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def logical_item_id(
    company: str,
    item_type: str,
    block_identifier: str,
) -> str:
    normalized_identifier = normalize_block_identifier(block_identifier)
    if not normalized_identifier:
        raise ValueError("block identifier is empty after normalization")
    source = "|".join((company.casefold(), item_type.casefold(), normalized_identifier))
    return "li_" + hashlib.sha256(source.encode("utf-8")).hexdigest()[:32]


def derived_item_id(logical_id: str, fingerprint: str) -> str:
    source = logical_id + "|" + fingerprint
    return "mi_" + hashlib.sha256(source.encode("utf-8")).hexdigest()[:40]


def canonical_subject(source_company: str, item_type: str, fingerprint: str) -> str:
    short_fingerprint = fingerprint.removeprefix("sha256:")[:12]
    return f"[MI {item_type} {short_fingerprint}] {source_company}"

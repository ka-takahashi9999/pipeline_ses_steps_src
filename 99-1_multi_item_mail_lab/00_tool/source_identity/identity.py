#!/usr/bin/env python3
"""Deterministic identities for test-only multi-item mail derivation."""

import base64
import binascii
import hashlib
import re
import unicodedata
from typing import Any, Dict


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


def body_fingerprint(body_text: str) -> str:
    normalized = normalize_content(body_text)
    return "sha256:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def attachment_fingerprint(attachment: Dict[str, Any]) -> str:
    """Hash decoded attachment bytes from the saved Gmail payload."""
    encoded_data = attachment.get("data")
    if not isinstance(encoded_data, str) or not encoded_data:
        raise ValueError("attachment data payload is missing")
    try:
        encoded_bytes = encoded_data.encode("ascii")
        padding = b"=" * (-len(encoded_bytes) % 4)
        payload = base64.b64decode(
            encoded_bytes + padding,
            altchars=b"-_",
            validate=True,
        )
    except (UnicodeEncodeError, binascii.Error, ValueError) as error:
        raise ValueError("attachment data payload is not valid base64") from error

    declared_size = attachment.get("size")
    if (
        isinstance(declared_size, bool)
        or not isinstance(declared_size, int)
        or declared_size < 0
    ):
        raise ValueError("attachment size is not a non-negative integer")
    if len(payload) != declared_size:
        raise ValueError(
            f"attachment payload size mismatch:{len(payload)}:declared:{declared_size}"
        )
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def version_fingerprint(body_digest: str, attachment_digest: str) -> str:
    source = body_digest + "|" + attachment_digest
    return "sha256:" + hashlib.sha256(source.encode("ascii")).hexdigest()


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


def derived_item_id(logical_id: str, version_digest: str) -> str:
    source = logical_id + "|" + version_digest
    return "mi_" + hashlib.sha256(source.encode("utf-8")).hexdigest()[:40]


def canonical_subject(
    source_company: str,
    item_type: str,
    logical_id: str,
    version_digest: str,
) -> str:
    short_logical_id = logical_id.removeprefix("li_")[:10]
    short_version = version_digest.removeprefix("sha256:")[:12]
    return f"[MI {item_type} {short_logical_id} {short_version}] {source_company}"

#!/usr/bin/env python3
"""Independent authoritative producer for stable P6 ZIP fixtures."""

import base64
import copy
import hashlib
import io
import stat
import unicodedata
import zipfile
from typing import Any, Dict, List, Sequence, Tuple

from archive_parser import MEMBER_MANIFEST_FIELD, SOURCE_ITEM_EVIDENCE_FIELD, ordered_member_digest
from attachment_fixture_source import build_source_owned_fixture


FIXED_TIMESTAMP = (2020, 1, 1, 0, 0, 0)
VALID_ROLES = {
    "ITEM_CANDIDATE",
    "SUPPORTING",
    "SHARED",
    "NESTED_ARCHIVE",
    "DIRECTORY",
    "UNKNOWN",
}


def _producer_collision_key(name: str, is_directory: bool) -> str:
    value = unicodedata.normalize("NFKC", name).replace("\\", "/")
    if is_directory and value.endswith("/"):
        value = value[:-1]
    return value.casefold()


def _producer_technical_kind(name: str, is_directory: bool) -> str:
    if is_directory:
        return "ATTACHMENT_FILE"
    lower = name.casefold()
    if lower.endswith((".xlsx", ".xls", ".csv")):
        return "SPREADSHEET"
    if lower.endswith(".pdf"):
        return "PDF"
    if lower.endswith((".zip", ".zipx")):
        return "ARCHIVE"
    return "ATTACHMENT_FILE"


def member_definition(
    name: str,
    payload: bytes = b"",
    role: str = "ITEM_CANDIDATE",
    compression_method: int = zipfile.ZIP_DEFLATED,
    member_type: str = "REGULAR_FILE",
    unix_mode: int = 0,
    extra: bytes = b"",
) -> Dict[str, Any]:
    if role not in VALID_ROLES:
        raise ValueError("invalid fixture member role:" + role)
    if member_type not in {"REGULAR_FILE", "DIRECTORY", "SYMLINK", "SPECIAL"}:
        raise ValueError("invalid fixture member type:" + member_type)
    return {
        "name": name,
        "payload": payload,
        "role": role,
        "compression_method": compression_method,
        "member_type": member_type,
        "unix_mode": unix_mode,
        "extra": extra,
    }


def build_zip_bytes(definitions: Sequence[Dict[str, Any]]) -> Tuple[bytes, List[Dict[str, Any]]]:
    """Freeze definitions first, then generate ZIP and independent expected rows."""
    authority = copy.deepcopy(list(definitions))
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", allowZip64=False) as archive:
        for definition in authority:
            name = definition["name"]
            member_type = definition["member_type"]
            info = zipfile.ZipInfo(name, FIXED_TIMESTAMP)
            info.create_system = 3
            if member_type == "DIRECTORY":
                mode = stat.S_IFDIR | 0o755
                info.external_attr = (mode << 16) | 0x10
            elif member_type == "SYMLINK":
                mode = stat.S_IFLNK | 0o777
                info.external_attr = mode << 16
            elif member_type == "SPECIAL":
                mode = definition.get("unix_mode") or (stat.S_IFIFO | 0o644)
                info.external_attr = mode << 16
            else:
                mode = stat.S_IFREG | 0o644
                info.external_attr = mode << 16
            info.compress_type = definition["compression_method"]
            info.extra = definition.get("extra", b"")
            archive.writestr(info, definition["payload"])
    payload = output.getvalue()
    expected: List[Dict[str, Any]] = []
    with zipfile.ZipFile(io.BytesIO(payload), "r") as archive:
        infos = archive.infolist()
        if len(infos) != len(authority):
            raise ValueError("fixture producer member count changed")
        for position, (definition, info) in enumerate(zip(authority, infos)):
            is_directory = definition["member_type"] == "DIRECTORY"
            expected.append(
                {
                    "position": position,
                    "original_name": definition["name"],
                    "normalized_collision_key": _producer_collision_key(definition["name"], is_directory),
                    "compressed_size": info.compress_size,
                    "uncompressed_size": info.file_size,
                    "crc32": info.CRC,
                    "compression_method": info.compress_type,
                    "flags": info.flag_bits,
                    "local_header_offset": info.header_offset,
                    "technical_kind": _producer_technical_kind(definition["name"], is_directory),
                }
            )
    return payload, expected


def _archive_attachment(filename: str, payload: bytes) -> Dict[str, Any]:
    return {
        "source_entry_id": "part:archive:0",
        "filename": filename,
        "mime_type": "application/zip",
        "size": len(payload),
        "data": base64.urlsafe_b64encode(payload).decode("ascii").rstrip("="),
    }


def build_archive_fixture(
    definitions: Sequence[Dict[str, Any]],
    item_count: int,
    message_id: str = "synthetic-archive-p6",
    archive_filename: str = "profiles.zip",
    source_from: str = "Synthetic <archive@example.invalid>",
) -> Dict[str, Any]:
    payload, expected = build_zip_bytes(definitions)
    source_item_keys = [
        definition["name"].replace("\\", "/").rsplit("/", 1)[-1].rsplit(".", 1)[0]
        for definition in definitions
        if definition["role"] == "ITEM_CANDIDATE"
    ]
    if item_count != len(source_item_keys):
        raise ValueError("fixture item count must match authoritative ITEM_CANDIDATE definitions")
    source_definition = {
        "message_id": message_id,
        "thread_id": "synthetic-archive-thread",
        "date": "Thu, 27 Aug 2026 00:00:00 +0000",
        "from": source_from,
        "to": ["test@example.invalid"],
        "cc": "",
        "reply_to": "",
        "subject": "P6 archive fixture " + str(item_count),
        "body_text": "DECLARED_ITEM_COUNT:" + str(item_count),
        "html_links": [],
        "authoritative_attachments": [_archive_attachment(archive_filename, payload)],
    }
    fixture = build_source_owned_fixture(source_definition)
    archive_digest = "sha256:" + hashlib.sha256(payload).hexdigest()
    fixture[MEMBER_MANIFEST_FIELD] = {
        "manifest_schema_version": "archive_member_manifest.v1",
        "archive_attachment_source_entry_id": "part:archive:0",
        "archive_sha256": archive_digest,
        "expected_ordered_count": len(expected),
        "expected_ordered_digest": ordered_member_digest(expected),
        "authoritative_ordered_members": expected,
        "authoritative_member_definitions": [
            {
                "position": position,
                "name": definition["name"],
                "role": definition["role"],
                "member_type": definition["member_type"],
                "payload_sha256": "sha256:" + hashlib.sha256(definition["payload"]).hexdigest(),
            }
            for position, definition in enumerate(definitions)
        ],
    }
    fixture[SOURCE_ITEM_EVIDENCE_FIELD] = {
        "authority": "DECLARED_COUNT",
        "count": item_count,
        "complete": True,
        "item_keys": source_item_keys,
        "source": "fixture_producer_before_archive_parse",
    }
    return fixture


def variable_n_definitions(item_count: int, extras: Sequence[Dict[str, Any]] = ()) -> List[Dict[str, Any]]:
    definitions = [
        member_definition(
            "item-" + str(index + 1).zfill(3) + ".xlsx",
            ("synthetic-item-" + str(index + 1)).encode("utf-8"),
            role="ITEM_CANDIDATE",
            compression_method=zipfile.ZIP_DEFLATED if index % 2 else zipfile.ZIP_STORED,
        )
        for index in range(item_count)
    ]
    definitions.extend(copy.deepcopy(list(extras)))
    return definitions

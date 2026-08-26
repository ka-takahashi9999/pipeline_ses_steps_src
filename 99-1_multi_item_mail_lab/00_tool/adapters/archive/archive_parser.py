#!/usr/bin/env python3
"""Test-only, fail-closed ZIP container enumerator for 99-1 P6."""

import base64
import binascii
import hashlib
import io
import json
import re
import stat
import struct
import unicodedata
import zipfile
import zlib
from dataclasses import dataclass, field, replace
from email.utils import parseaddr
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from attachment_manifest_contract import (
    MANIFEST_FIELD,
    MANIFEST_SCHEMA_VERSION,
    canonical_ordered_entries,
    ordered_attachment_digest,
    source_payload_digest,
    validate_authoritative_attachment_entries,
)
from variable_item_core import Container, ContainerKind, EnumerationStatus


PARSER_ID = "archive_container_enumerator"
PARSER_VERSION = "1.0.0"
MEMBER_MANIFEST_FIELD = "archive_member_manifest"
SOURCE_ITEM_EVIDENCE_FIELD = "source_item_cardinality_evidence"
EOCD_SIGNATURE = b"PK\x05\x06"
CENTRAL_SIGNATURE = b"PK\x01\x02"
LOCAL_SIGNATURE = b"PK\x03\x04"
ZIP64_EOCD_SIGNATURE = b"PK\x06\x06"
ZIP64_LOCATOR_SIGNATURE = b"PK\x06\x07"
AES_EXTRA_FIELD = 0x9901
ZIP64_EXTRA_FIELD = 0x0001
SUPPORTED_METHODS = {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}
WINDOWS_RESERVED = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}


class ArchiveInputError(ValueError):
    """A malformed or unsupported archive input, never a parser crash."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class CentralMember:
    position: int
    original_name: str
    raw_name_hex: str
    collision_key: str
    compressed_size: int
    uncompressed_size: int
    crc32: int
    compression_method: int
    flags: int
    local_header_offset: int
    external_attr: int
    version_made_by: int
    disk_start: int
    extra_fields: Tuple[int, ...]
    technical_kind: str
    member_type: str
    role: str
    role_key: str
    path_reasons: Tuple[str, ...] = ()
    type_reasons: Tuple[str, ...] = ()

    def authority_tuple(self) -> Dict[str, Any]:
        return {
            "position": self.position,
            "original_name": self.original_name,
            "normalized_collision_key": self.collision_key,
            "compressed_size": self.compressed_size,
            "uncompressed_size": self.uncompressed_size,
            "crc32": self.crc32,
            "compression_method": self.compression_method,
            "flags": self.flags,
            "local_header_offset": self.local_header_offset,
            "technical_kind": self.technical_kind,
        }

    def to_dict(self) -> Dict[str, Any]:
        result = self.authority_tuple()
        result.update(
            {
                "raw_name_hex": self.raw_name_hex,
                "external_attr": self.external_attr,
                "version_made_by": self.version_made_by,
                "disk_start": self.disk_start,
                "extra_fields": list(self.extra_fields),
                "member_type": self.member_type,
                "role": self.role,
                "role_key": self.role_key,
                "path_reasons": list(self.path_reasons),
                "type_reasons": list(self.type_reasons),
            }
        )
        return result


@dataclass(frozen=True)
class StructureResult:
    eocd: Dict[str, Any]
    members: List[CentralMember]
    central_directory_digest: str
    structure_reasons: List[str] = field(default_factory=list)
    unsupported_reasons: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class ArchiveParseResult:
    status: str
    reasons: List[str]
    eligible_item_candidate_count: int
    source: Dict[str, Any]
    archive: Dict[str, Any]
    members: List[Dict[str, Any]]
    containers: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "reasons": self.reasons,
            "eligible_item_candidate_count": self.eligible_item_candidate_count,
            "source": self.source,
            "archive": self.archive,
            "members": self.members,
            "containers": self.containers,
        }


def _digest_json(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def ordered_member_digest(rows: Sequence[Dict[str, Any]]) -> str:
    """Digest an ordered member sequence without sorting or deduplication."""
    return _digest_json(list(rows))


def _parse_extra_fields(extra: bytes) -> Tuple[Tuple[int, ...], List[str]]:
    identifiers: List[int] = []
    reasons: List[str] = []
    cursor = 0
    while cursor < len(extra):
        if cursor + 4 > len(extra):
            reasons.append("extra_field_truncated")
            break
        identifier, size = struct.unpack_from("<HH", extra, cursor)
        cursor += 4
        if cursor + size > len(extra):
            reasons.append("extra_field_payload_truncated")
            break
        identifiers.append(identifier)
        cursor += size
    return tuple(identifiers), reasons


def _strict_base64url_decode(value: Any) -> bytes:
    if not isinstance(value, str) or not value:
        raise ArchiveInputError("archive_base64url_missing")
    if re.fullmatch(r"[A-Za-z0-9_-]+={0,2}", value) is None:
        raise ArchiveInputError("archive_base64url_invalid")
    unpadded = value.rstrip("=")
    if len(unpadded) % 4 == 1:
        raise ArchiveInputError("archive_base64url_invalid_length")
    try:
        decoded = base64.b64decode(
            unpadded.encode("ascii") + b"=" * (-len(unpadded) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (UnicodeEncodeError, binascii.Error, ValueError) as error:
        raise ArchiveInputError("archive_base64url_invalid") from error
    canonical = base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=")
    if canonical != unpadded:
        raise ArchiveInputError("archive_base64url_noncanonical")
    return decoded


def _canonical_separator_path(raw_name: str) -> str:
    """Apply Unicode normalization before the final separator interpretation."""
    return unicodedata.normalize("NFKC", raw_name).replace("\\", "/")


def _path_contract(raw_name: str, is_directory: bool, limits: Dict[str, Any]) -> Tuple[str, List[str]]:
    reasons: List[str] = []
    if not raw_name:
        return "", ["path_empty"]
    if any(ord(character) < 32 or ord(character) == 127 for character in raw_name):
        reasons.append("path_control_character")
    if "\x00" in raw_name:
        reasons.append("path_nul")
    separator_name = _canonical_separator_path(raw_name)
    if any(ord(character) < 32 or ord(character) == 127 for character in separator_name):
        reasons.append("path_normalized_control_character")
    if separator_name.startswith("/"):
        reasons.append("path_absolute_or_unc")
    if separator_name.startswith("//"):
        reasons.append("path_unc")
    if re.match(r"^[A-Za-z]:", separator_name):
        reasons.append("path_windows_drive")
    path_for_segments = separator_name[:-1] if is_directory and separator_name.endswith("/") else separator_name
    normalized_segments = path_for_segments.split("/")
    if any(segment == "" for segment in normalized_segments):
        reasons.append("path_empty_segment")
    if any(segment == "." for segment in normalized_segments):
        reasons.append("path_dot_segment")
    if any(segment == ".." for segment in normalized_segments):
        reasons.append("path_parent_segment")
    if any(segment in {"", ".", ".."} for segment in normalized_segments):
        reasons.append("path_normalized_unsafe_segment")
    if any(segment.endswith((".", " ")) for segment in normalized_segments):
        reasons.append("path_segment_trailing_dot_or_space")
    if any(
        segment.split(".", 1)[0].upper() in WINDOWS_RESERVED
        for segment in normalized_segments
        if segment
    ):
        reasons.append("path_windows_reserved_name")
    if any(
        len(segment) > limits["max_filename_unicode_chars"]
        for segment in normalized_segments
    ):
        reasons.append("path_filename_length_exceeded")
    normalized_path = "/".join(normalized_segments)
    if len(normalized_path.encode("utf-8")) > limits["max_full_path_utf8_bytes"]:
        reasons.append("path_full_length_exceeded")
    if len(normalized_segments) > limits["max_path_depth_segments"]:
        reasons.append("path_depth_exceeded")
    if normalized_path.startswith("../") or "/../" in normalized_path:
        reasons.append("path_normalized_root_escape")
    collision_key = normalized_path.casefold()
    return collision_key, list(dict.fromkeys(reasons))


def _member_type(version_made_by: int, external_attr: int, name: str) -> Tuple[str, List[str]]:
    reasons: List[str] = []
    creator_system = version_made_by >> 8
    unix_mode = (external_attr >> 16) & 0xFFFF if creator_system == 3 else 0
    file_type = stat.S_IFMT(unix_mode) if unix_mode else 0
    name_directory = name.endswith(("/", "\\"))
    if file_type == stat.S_IFLNK:
        return "SYMLINK", ["member_symlink_unsupported"]
    if file_type and file_type not in {stat.S_IFREG, stat.S_IFDIR}:
        labels = {
            stat.S_IFCHR: "device",
            stat.S_IFBLK: "device",
            stat.S_IFIFO: "fifo",
            stat.S_IFSOCK: "socket",
        }
        return "SPECIAL", ["member_special_file_unsupported:" + labels.get(file_type, "unknown")]
    if file_type == stat.S_IFDIR or name_directory:
        if file_type == stat.S_IFREG and name_directory:
            reasons.append("member_file_directory_marker_mismatch")
        return "DIRECTORY", reasons
    return "REGULAR_FILE", reasons


def _technical_kind(name: str, member_type: str) -> str:
    if member_type == "DIRECTORY":
        return ContainerKind.ATTACHMENT_FILE.value
    lower = _canonical_separator_path(name).casefold()
    if lower.endswith((".xlsx", ".xls", ".csv")):
        return ContainerKind.SPREADSHEET.value
    if lower.endswith(".pdf"):
        return ContainerKind.PDF.value
    if lower.endswith((".zip", ".zipx")):
        return ContainerKind.ARCHIVE.value
    return ContainerKind.ATTACHMENT_FILE.value


def _member_rule_matches(normalized_path: str, rules: Dict[str, Any]) -> bool:
    basename = normalized_path.rsplit("/", 1)[-1]
    exact_filenames = [
        unicodedata.normalize("NFKC", value)
        for value in rules.get("exact_filenames", [])
        if isinstance(value, str)
    ]
    if basename in exact_filenames:
        return True
    if any(
        re.fullmatch(pattern, basename, re.IGNORECASE)
        for pattern in rules.get("anchored_filename_regexes", [])
    ):
        return True
    return any(
        re.fullmatch(pattern, normalized_path, re.IGNORECASE)
        for pattern in rules.get("explicit_path_regexes", [])
    )


def _classify_role(
    name: str,
    member_type: str,
    technical_kind: str,
    config: Dict[str, Any],
    member_role_config: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str]:
    normalized = _canonical_separator_path(name)
    if member_type == "DIRECTORY":
        return "DIRECTORY", ""
    if technical_kind == ContainerKind.ARCHIVE.value:
        return "NESTED_ARCHIVE", ""
    rules = config["member_roles"]
    basename = normalized.rsplit("/", 1)[-1]
    role_key = basename.rsplit(".", 1)[0].casefold()
    explicit_rules = member_role_config or {}
    if _member_rule_matches(
        normalized, explicit_rules.get("item_candidate_member_rules", {})
    ):
        return "ITEM_CANDIDATE", role_key
    if _member_rule_matches(
        normalized, explicit_rules.get("supporting_member_rules", {})
    ):
        return "SUPPORTING", ""
    if _member_rule_matches(
        normalized, explicit_rules.get("shared_member_rules", {})
    ):
        return "SHARED", ""
    if re.search(rules["item_candidate_regex"], normalized, re.IGNORECASE):
        return "ITEM_CANDIDATE", role_key
    if re.search(rules["supporting_regex"], normalized, re.IGNORECASE):
        return "SUPPORTING", ""
    if re.search(rules["shared_regex"], normalized, re.IGNORECASE):
        return "SHARED", ""
    return "UNKNOWN", ""


def metadata_limit_reasons(members: Sequence[CentralMember], archive_size: int, limits: Dict[str, Any]) -> List[str]:
    """Apply all limits available before member extraction."""
    reasons: List[str] = []
    if archive_size > limits["max_archive_compressed_bytes"]:
        reasons.append("limit_archive_compressed_bytes_exceeded")
    if len(members) > limits["max_member_count"]:
        reasons.append("limit_member_count_exceeded")
    total_uncompressed = sum(member.uncompressed_size for member in members)
    total_compressed = sum(member.compressed_size for member in members)
    if total_uncompressed > limits["max_total_uncompressed_bytes"]:
        reasons.append("limit_total_uncompressed_bytes_exceeded")
    archive_ratio = total_uncompressed / max(1, total_compressed)
    if archive_ratio > limits["max_expansion_ratio"]:
        reasons.append("limit_archive_expansion_ratio_exceeded")
    for member in members:
        if member.uncompressed_size > limits["max_single_member_uncompressed_bytes"]:
            reasons.append("limit_single_member_uncompressed_bytes_exceeded:" + str(member.position))
        ratio = member.uncompressed_size / max(1, member.compressed_size)
        if ratio > limits["max_expansion_ratio"]:
            reasons.append("limit_member_expansion_ratio_exceeded:" + str(member.position))
    return list(dict.fromkeys(reasons))


def _find_eocd(payload: bytes) -> Tuple[int, Dict[str, Any]]:
    search_start = max(0, len(payload) - (65535 + 22))
    offset = payload.rfind(EOCD_SIGNATURE, search_start)
    if offset < 0:
        raise ArchiveInputError("eocd_missing")
    if offset + 22 > len(payload):
        raise ArchiveInputError("eocd_truncated")
    fields = struct.unpack_from("<4s4H2LH", payload, offset)
    _, disk_number, central_disk, disk_entries, total_entries, central_size, central_offset, comment_size = fields
    if offset + 22 + comment_size != len(payload):
        raise ArchiveInputError("eocd_end_or_comment_mismatch")
    return offset, {
        "offset": offset,
        "disk_number": disk_number,
        "central_directory_disk": central_disk,
        "entries_on_disk": disk_entries,
        "total_entries": total_entries,
        "central_directory_size": central_size,
        "central_directory_offset": central_offset,
        "comment_size": comment_size,
    }


def _parse_structure(
    payload: bytes,
    config: Dict[str, Any],
    member_role_config: Optional[Dict[str, Any]] = None,
) -> StructureResult:
    if len(payload) < 22 or payload[:4] not in {LOCAL_SIGNATURE, EOCD_SIGNATURE}:
        raise ArchiveInputError("zip_signature_invalid")
    eocd_offset, eocd = _find_eocd(payload)
    unsupported: List[str] = []
    structure_reasons: List[str] = []
    if eocd["disk_number"] != 0 or eocd["central_directory_disk"] != 0:
        unsupported.append("multi_disk_or_spanned_zip_unsupported")
    if eocd["entries_on_disk"] != eocd["total_entries"]:
        unsupported.append("multi_disk_entry_count_unsupported")
    if (
        eocd["entries_on_disk"] == 0xFFFF
        or eocd["total_entries"] == 0xFFFF
        or eocd["central_directory_size"] == 0xFFFFFFFF
        or eocd["central_directory_offset"] == 0xFFFFFFFF
        or ZIP64_EOCD_SIGNATURE in payload[max(0, eocd_offset - 128):eocd_offset]
        or ZIP64_LOCATOR_SIGNATURE in payload[max(0, eocd_offset - 128):eocd_offset]
    ):
        raise ArchiveInputError("zip64_unsupported")
    central_start = eocd["central_directory_offset"]
    central_end = central_start + eocd["central_directory_size"]
    if central_start < 0 or central_end > eocd_offset or central_end != eocd_offset:
        raise ArchiveInputError("central_directory_bounds_invalid")
    cursor = central_start
    members: List[CentralMember] = []
    for position in range(eocd["total_entries"]):
        if cursor + 46 > central_end:
            raise ArchiveInputError("central_directory_header_truncated")
        fields = struct.unpack_from("<4s6H3L5H2L", payload, cursor)
        if fields[0] != CENTRAL_SIGNATURE:
            raise ArchiveInputError("central_directory_signature_invalid")
        (
            _, version_made_by, _version_needed, flags, method, _time, _date,
            crc32_value, compressed_size, uncompressed_size, name_size, extra_size,
            comment_size, disk_start, _internal_attr, external_attr, local_offset,
        ) = fields
        record_end = cursor + 46 + name_size + extra_size + comment_size
        if record_end > central_end:
            raise ArchiveInputError("central_directory_record_truncated")
        raw_name = payload[cursor + 46:cursor + 46 + name_size]
        extra = payload[cursor + 46 + name_size:cursor + 46 + name_size + extra_size]
        try:
            name = raw_name.decode("utf-8" if flags & 0x800 else "cp437")
        except UnicodeDecodeError as error:
            raise ArchiveInputError("member_filename_decode_failure:" + str(position)) from error
        extra_fields, extra_reasons = _parse_extra_fields(extra)
        structure_reasons.extend(reason + ":" + str(position) for reason in extra_reasons)
        if ZIP64_EXTRA_FIELD in extra_fields:
            unsupported.append("zip64_unsupported")
        member_type, type_reasons = _member_type(version_made_by, external_attr, name)
        collision_key, path_reasons = _path_contract(name, member_type == "DIRECTORY", config["limits"])
        technical_kind = _technical_kind(name, member_type)
        role, role_key = _classify_role(
            name, member_type, technical_kind, config, member_role_config
        )
        members.append(
            CentralMember(
                position, name, raw_name.hex(), collision_key, compressed_size,
                uncompressed_size, crc32_value, method, flags, local_offset,
                external_attr, version_made_by, disk_start, extra_fields,
                technical_kind, member_type, role, role_key,
                tuple(path_reasons), tuple(type_reasons),
            )
        )
        cursor = record_end
    if cursor != central_end:
        raise ArchiveInputError("central_directory_size_or_count_mismatch")
    if len(members) != eocd["total_entries"]:
        raise ArchiveInputError("central_directory_count_mismatch")
    if any(member.disk_start != 0 for member in members):
        unsupported.append("multi_disk_member_unsupported")
    central_rows = [member.authority_tuple() for member in members]
    return StructureResult(
        eocd=eocd,
        members=members,
        central_directory_digest=ordered_member_digest(central_rows),
        structure_reasons=list(dict.fromkeys(structure_reasons)),
        unsupported_reasons=list(dict.fromkeys(unsupported)),
    )


def _local_header_reasons(payload: bytes, members: Sequence[CentralMember], central_offset: int) -> List[str]:
    reasons: List[str] = []
    for member in members:
        offset = member.local_header_offset
        if offset < 0 or offset + 30 > central_offset:
            reasons.append("local_header_bounds_invalid:" + str(member.position))
            continue
        fields = struct.unpack_from("<4s5H3L2H", payload, offset)
        signature, _version, flags, method, _time, _date, crc, csize, usize, name_size, extra_size = fields
        if signature != LOCAL_SIGNATURE:
            reasons.append("local_header_signature_invalid:" + str(member.position))
            continue
        record_end = offset + 30 + name_size + extra_size + member.compressed_size
        if record_end > central_offset:
            reasons.append("local_header_data_bounds_invalid:" + str(member.position))
        raw_name = payload[offset + 30:offset + 30 + name_size]
        if raw_name.hex() != member.raw_name_hex:
            reasons.append("local_header_name_mismatch:" + str(member.position))
        if flags != member.flags:
            reasons.append("local_header_flags_mismatch:" + str(member.position))
        if method != member.compression_method:
            reasons.append("local_header_method_mismatch:" + str(member.position))
        if not (flags & 0x08) and (crc, csize, usize) != (
            member.crc32, member.compressed_size, member.uncompressed_size
        ):
            reasons.append("local_header_integrity_metadata_mismatch:" + str(member.position))
    return reasons


def _validate_source_manifest(mail: Dict[str, Any], position: int, payload: bytes) -> Dict[str, Any]:
    attachments = mail.get("attachments")
    if not isinstance(attachments, list) or position >= len(attachments):
        return {"status": "INCOMPLETE", "reasons": ["archive_attachment_missing"]}
    observed_entries: List[Dict[str, Any]] = []
    observed_integrity_reasons: List[str] = []
    for index, attachment in enumerate(attachments):
        if not isinstance(attachment, dict):
            observed_entries.append({})
            continue
        try:
            attachment_payload = _strict_base64url_decode(attachment.get("data"))
            digest = "sha256:" + hashlib.sha256(attachment_payload).hexdigest()
            declared_size = attachment.get("size")
            if (
                isinstance(declared_size, bool)
                or not isinstance(declared_size, int)
                or declared_size < 0
            ):
                observed_integrity_reasons.append("attachment_declared_size_invalid:" + str(index))
            elif declared_size != len(attachment_payload):
                observed_integrity_reasons.append("attachment_size_mismatch:" + str(index))
        except ArchiveInputError as error:
            digest = ""
            observed_integrity_reasons.append(error.reason + ":" + str(index))
        observed_entries.append(
            {
                "position": index,
                "source_entry_id": attachment.get("source_entry_id", "part-attachment-" + str(index)),
                "filename": attachment.get("filename", ""),
                "mime_type": attachment.get("mime_type", ""),
                "declared_size": attachment.get("size"),
                "content_digest": digest,
                "disposition": attachment.get("disposition", ""),
                "content_id": attachment.get("content_id", ""),
            }
        )
    manifest = mail.get(MANIFEST_FIELD)
    if not isinstance(manifest, dict):
        return {
            "status": "UNVERIFIED",
            "manifest_contract_status": "UNVERIFIED",
            "attachment_integrity_status": "PASS" if not observed_integrity_reasons else "FAIL",
            "reasons": ["source_owned_attachment_manifest_missing"] + observed_integrity_reasons,
            "observed_ordered_count": len(observed_entries),
            "observed_ordered_digest": ordered_attachment_digest(observed_entries),
        }
    reasons: List[str] = []
    authoritative = manifest.get("authoritative_attachment_entries")
    authority_rows = authoritative if isinstance(authoritative, list) else []
    entry_reasons = validate_authoritative_attachment_entries(authoritative)
    reasons.extend(entry_reasons)
    canonical_authority = canonical_ordered_entries(authority_rows)
    if manifest.get("manifest_schema_version") != MANIFEST_SCHEMA_VERSION:
        reasons.append("manifest_schema_mismatch")
    if manifest.get("source_id") != mail.get("message_id"):
        reasons.append("manifest_source_id_mismatch")
    if manifest.get("acquisition_status") != "COMPLETE" or manifest.get("extractor_status") != "COMPLETE":
        reasons.append("manifest_acquisition_incomplete")
    if manifest.get("reasons") != []:
        reasons.append("manifest_reasons_not_empty")
    if [entry.get("position") if isinstance(entry, dict) else None for entry in authority_rows] != list(range(len(canonical_authority))):
        reasons.append("manifest_positions_invalid")
    if manifest.get("expected_ordered_count") != len(canonical_authority):
        reasons.append("manifest_count_mismatch")
    if manifest.get("expected_ordered_digest") != ordered_attachment_digest(canonical_authority):
        reasons.append("manifest_digest_mismatch")
    if manifest.get("source_payload_digest") != source_payload_digest(mail, canonical_authority):
        reasons.append("source_payload_digest_mismatch")
    if canonical_ordered_entries(observed_entries) != canonical_authority:
        reasons.append("observed_attachment_entries_mismatch")
    reasons.extend(observed_integrity_reasons)
    manifest_reasons = [
        reason
        for reason in reasons
        if reason.startswith("manifest_") or reason.startswith("source_payload_digest_")
    ]
    return {
        "status": "VERIFIED_COMPLETE" if not reasons else "INCOMPLETE",
        "manifest_contract_status": "PASS" if not manifest_reasons else "FAIL",
        "attachment_integrity_status": "PASS" if not observed_integrity_reasons and canonical_ordered_entries(observed_entries) == canonical_authority else "FAIL",
        "reasons": list(dict.fromkeys(reasons)),
        "observed_ordered_count": len(observed_entries),
        "observed_ordered_digest": ordered_attachment_digest(observed_entries),
        "archive_attachment_sha256": "sha256:" + hashlib.sha256(payload).hexdigest(),
    }


def _validate_member_authority(mail: Dict[str, Any], archive_digest: str, members: Sequence[CentralMember], schema_version: str) -> Dict[str, Any]:
    manifest = mail.get(MEMBER_MANIFEST_FIELD)
    observed = [member.authority_tuple() for member in members]
    observed_digest = ordered_member_digest(observed)
    if not isinstance(manifest, dict):
        return {
            "status": "UNVERIFIED",
            "fixture_comparison_status": "UNVERIFIED",
            "expected_ordered_count": None,
            "expected_ordered_digest": "",
            "observed_ordered_count": len(observed),
            "observed_ordered_digest": observed_digest,
            "reasons": ["fixture_member_authority_missing"],
        }
    reasons: List[str] = []
    expected = manifest.get("authoritative_ordered_members")
    if not isinstance(expected, list):
        expected = []
        reasons.append("member_authority_entries_missing")
    if manifest.get("manifest_schema_version") != schema_version:
        reasons.append("member_authority_schema_mismatch")
    if manifest.get("archive_sha256") != archive_digest:
        reasons.append("member_authority_archive_digest_mismatch")
    if manifest.get("expected_ordered_count") != len(expected):
        reasons.append("member_authority_count_invalid")
    if manifest.get("expected_ordered_digest") != ordered_member_digest(expected):
        reasons.append("member_authority_digest_invalid")
    if len(expected) != len(observed):
        reasons.append("member_enumeration_count_mismatch")
    if expected != observed:
        reasons.append("member_enumeration_ordered_sequence_mismatch")
    enumeration_reasons = [
        reason
        for reason in reasons
        if reason.startswith("member_authority_count_")
        or reason.startswith("member_authority_digest_")
        or reason.startswith("member_enumeration_")
    ]
    return {
        "status": "VERIFIED_COMPLETE" if not reasons else "INCOMPLETE",
        "fixture_comparison_status": "PASS" if not enumeration_reasons else "FAIL",
        "expected_ordered_count": len(expected),
        "expected_ordered_digest": manifest.get("expected_ordered_digest", ""),
        "observed_ordered_count": len(observed),
        "observed_ordered_digest": observed_digest,
        "expected_ordered_members": expected,
        "reasons": list(dict.fromkeys(reasons)),
    }


def _validate_item_cardinality(mail: Dict[str, Any], members: Sequence[CentralMember]) -> Dict[str, Any]:
    evidence = mail.get(SOURCE_ITEM_EVIDENCE_FIELD)
    candidates = [member for member in members if member.role == "ITEM_CANDIDATE"]
    observed_keys = [member.role_key for member in candidates]
    if not isinstance(evidence, dict):
        return {
            "status": "UNKNOWN",
            "authority": "UNKNOWN",
            "source_count": None,
            "item_candidate_count": len(candidates),
            "mapping_status": "UNVERIFIED",
            "reasons": ["source_item_cardinality_evidence_missing"],
        }
    reasons: List[str] = []
    source_count = evidence.get("count")
    item_keys = evidence.get("item_keys")
    if evidence.get("authority") not in {"DECLARED_COUNT", "STRUCTURAL_COMPLETE"}:
        reasons.append("source_item_authority_unsupported")
    if evidence.get("complete") is not True:
        reasons.append("source_item_evidence_incomplete")
    if isinstance(source_count, bool) or not isinstance(source_count, int) or source_count < 0:
        reasons.append("source_item_count_invalid")
    if not isinstance(item_keys, list) or any(not isinstance(key, str) or not key for key in item_keys):
        reasons.append("source_item_keys_invalid")
        item_keys = []
    normalized_keys = [unicodedata.normalize("NFKC", key).casefold() for key in item_keys]
    if len(normalized_keys) != len(set(normalized_keys)):
        reasons.append("source_item_key_duplicate")
    if source_count != len(normalized_keys):
        reasons.append("source_item_key_count_mismatch")
    if source_count != len(candidates):
        reasons.append("source_item_candidate_count_mismatch")
    if len(observed_keys) != len(set(observed_keys)):
        reasons.append("archive_item_candidate_key_duplicate")
    if normalized_keys != observed_keys:
        reasons.append("source_archive_item_mapping_not_one_to_one")
    return {
        "status": "PASS" if not reasons else "FAIL",
        "authority": evidence.get("authority", "UNKNOWN"),
        "source_count": source_count,
        "item_candidate_count": len(candidates),
        "source_item_keys": normalized_keys,
        "observed_item_candidate_keys": observed_keys,
        "mapping_status": "MAPPED" if not reasons else "UNMAPPED",
        "reasons": list(dict.fromkeys(reasons)),
    }


def validate_archive_graph(containers: Sequence[Dict[str, Any]], source_refs: Sequence[str]) -> List[str]:
    reasons: List[str] = []
    ids = [container.get("container_id") for container in containers]
    if len(ids) != len(set(ids)):
        reasons.append("graph_duplicate_child_id")
    by_id = {container.get("container_id"): container for container in containers}
    for container in containers:
        if container.get("kind") == ContainerKind.ATTACHMENT_LIST.value:
            continue
        parent_id = container.get("parent_container_id")
        parent = by_id.get(parent_id)
        if parent is None:
            reasons.append("graph_orphan:" + str(container.get("container_id")))
        elif container.get("container_id") not in parent.get("child_container_refs", []):
            reasons.append("graph_parent_ref_missing:" + str(container.get("container_id")))
    for parent in containers:
        for child_id in parent.get("child_container_refs", []):
            child = by_id.get(child_id)
            if child is None:
                reasons.append("graph_child_missing:" + str(child_id))
            elif child.get("parent_container_id") != parent.get("container_id"):
                reasons.append("graph_child_parent_mismatch:" + str(child_id))
    required = {container.get("container_id") for container in containers if container.get("required")}
    if not required <= set(source_refs):
        reasons.append("graph_required_source_ref_missing")
    return list(dict.fromkeys(reasons))


def validate_child_container_proof(
    members: Sequence[CentralMember], containers: Sequence[Dict[str, Any]]
) -> List[str]:
    """Compare central-directory authority directly with observed child sequence."""
    archive_rows = [
        container
        for container in containers
        if container.get("kind") == ContainerKind.ARCHIVE.value
        and str(container.get("locator", "")).startswith("attachment:")
    ]
    if len(archive_rows) != 1:
        return ["child_proof_archive_container_count:" + str(len(archive_rows))]
    archive_id = archive_rows[0]["container_id"]
    observed = [
        {
            "locator": container.get("locator"),
            "kind": container.get("kind"),
            "parent_container_id": container.get("parent_container_id"),
        }
        for container in containers
        if container.get("parent_container_id") == archive_id
    ]
    expected = [
        {
            "locator": "zip-member:" + str(member.position) + ":" + member.original_name,
            "kind": member.technical_kind,
            "parent_container_id": archive_id,
        }
        for member in members
    ]
    reasons: List[str] = []
    if len(observed) != len(expected):
        reasons.append(
            "child_container_count_mismatch:"
            + str(len(observed))
            + ":expected:"
            + str(len(expected))
        )
    if observed != expected:
        reasons.append("child_container_ordered_sequence_mismatch")
    return reasons


class ArchiveParser:
    """Enumerate one depth-0 ZIP attachment into technical child Containers."""

    def __init__(
        self,
        config: Dict[str, Any],
        member_role_config: Optional[Dict[str, Any]] = None,
    ):
        self.config = config
        self.member_role_config = member_role_config or {}
        if config.get("archive_format") != "ZIP":
            raise ValueError("only ZIP config is supported")
        if config.get("expansion_depth") != 0:
            raise ValueError("P6 expansion depth must remain zero")
        if set(config.get("allowed_compression_methods", [])) != SUPPORTED_METHODS:
            raise ValueError("P6 compression methods must be STORED and DEFLATED")
        credential = config.get("credential_matching", {})
        if credential.get("credential_matching_enabled") is not False:
            raise ValueError("credential matching must remain disabled")
        if config.get("password_persistence") != "EPHEMERAL_ONLY":
            raise ValueError("password persistence contract mismatch")

    @classmethod
    def from_file(cls, path: Path) -> "ArchiveParser":
        with path.open(encoding="utf-8") as file_object:
            return cls(json.load(file_object))

    @classmethod
    def from_files(cls, security_path: Path, member_role_path: Path) -> "ArchiveParser":
        with security_path.open(encoding="utf-8") as file_object:
            security_config = json.load(file_object)
        with member_role_path.open(encoding="utf-8") as file_object:
            member_role_config = json.load(file_object)
        if not isinstance(member_role_config.get("role_config_id"), str):
            raise ValueError("member role config id is required")
        if not isinstance(member_role_config.get("role_config_version"), str):
            raise ValueError("member role config version is required")
        selectors = member_role_config.get("selectors", {})
        if not isinstance(selectors.get("sender_domain"), str) or not selectors["sender_domain"]:
            raise ValueError("member role config sender domain is required")
        return cls(security_config, member_role_config)

    def _active_member_role_config(self, mail: Dict[str, Any]) -> Dict[str, Any]:
        if not self.member_role_config:
            return {}
        sender = parseaddr(str(mail.get("from", "")))[1]
        sender_domain = sender.rsplit("@", 1)[-1].casefold() if "@" in sender else ""
        configured_domain = self.member_role_config["selectors"]["sender_domain"].casefold()
        return self.member_role_config if sender_domain == configured_domain else {}

    @staticmethod
    def _archive_position(mail: Dict[str, Any]) -> int:
        attachments = mail.get("attachments")
        if not isinstance(attachments, list):
            raise ArchiveInputError("attachments_not_list")
        positions = [
            index
            for index, attachment in enumerate(attachments)
            if isinstance(attachment, dict)
            and (
                str(attachment.get("filename", "")).casefold().endswith(".zip")
                or str(attachment.get("mime_type", "")).casefold()
                in {"application/zip", "application/x-zip-compressed"}
            )
        ]
        if len(positions) != 1:
            raise ArchiveInputError("archive_attachment_candidate_count:" + str(len(positions)))
        return positions[0]

    def parse(self, mail: Dict[str, Any]) -> ArchiveParseResult:
        try:
            return self._parse(mail)
        except ArchiveInputError as error:
            return self._failed_input(mail, error.reason)
        except Exception as error:
            return self._system_failure(mail, error)

    def _failed_input(self, mail: Dict[str, Any], reason: str) -> ArchiveParseResult:
        source_id = str(mail.get("message_id", ""))
        status = "UNSUPPORTED" if "unsupported" in reason else "PARTIAL"
        return ArchiveParseResult(
            status, [reason], 0,
            {
                "source_id": source_id,
                "source_acquisition_status": "UNVERIFIED",
                "source_atomic_status": status,
                "auto_union_eligible": False,
                "reasons": [reason],
            },
            {
                "format": "ZIP",
                "parser_id": PARSER_ID,
                "parser_version": PARSER_VERSION,
                "archive_complete": False,
                "enumeration_status": "INCOMPLETE",
                "integrity_status": "INCOMPLETE",
                "security_status": "FAIL",
                "credential_status": "NOT_ENCRYPTED",
                "processing_status": "UNSUPPORTED" if status == "UNSUPPORTED" else "FAILED",
                "reasons": [reason],
            },
            [], [],
        )

    def _system_failure(self, mail: Dict[str, Any], error: Exception) -> ArchiveParseResult:
        reason = "system_failure:" + type(error).__name__
        failed = self._failed_input(mail, reason)
        return ArchiveParseResult(
            "SYSTEM_FAILURE", failed.reasons, 0,
            dict(failed.source, source_atomic_status="SYSTEM_FAILURE"),
            dict(failed.archive, processing_status="FAILED"), [], [],
        )

    def _archive_size_failure(
        self,
        mail: Dict[str, Any],
        payload: bytes,
        source_validation: Dict[str, Any],
        member_role_config: Dict[str, Any],
    ) -> ArchiveParseResult:
        reason = "limit_archive_compressed_bytes_exceeded"
        source_id = str(mail.get("message_id", ""))
        return ArchiveParseResult(
            "UNSUPPORTED",
            list(dict.fromkeys(source_validation.get("reasons", []) + [reason])),
            0,
            {
                "source_id": source_id,
                "source_acquisition_status": source_validation["status"],
                "source_attachment_validation": source_validation,
                "source_item_cardinality": {
                    "status": "UNKNOWN",
                    "authority": "UNKNOWN",
                    "source_count": None,
                    "item_candidate_count": 0,
                    "mapping_status": "UNVERIFIED",
                    "reasons": ["archive_not_enumerated"],
                },
                "source_atomic_status": "UNSUPPORTED",
                "container_references": [],
                "auto_union_eligible": False,
                "eligible_item_candidate_count": 0,
                "reasons": [reason],
            },
            {
                "format": "ZIP",
                "archive_schema_version": self.config["archive_schema_version"],
                "parser_id": PARSER_ID,
                "parser_version": PARSER_VERSION,
                "config_id": self.config["config_id"],
                "config_version": self.config["config_version"],
                "member_role_config_id": member_role_config.get("role_config_id"),
                "member_role_config_version": member_role_config.get("role_config_version"),
                "archive_sha256": "sha256:" + hashlib.sha256(payload).hexdigest(),
                "archive_compressed_bytes": len(payload),
                "archive_complete": False,
                "enumeration_status": "INCOMPLETE",
                "integrity_status": "INCOMPLETE",
                "security_status": "FAIL",
                "credential_status": "NOT_ENCRYPTED",
                "password_persistence": "EPHEMERAL_ONLY",
                "processing_status": "UNSUPPORTED",
                "member_extraction_status": "INCOMPLETE",
                "nested_expansion_performed": False,
                "graph_status": "NOT_RUN",
                "reasons": [reason],
                "totals": {
                    "members": 0,
                    "item_candidates": 0,
                    "compressed_member_bytes": 0,
                    "uncompressed_member_bytes": 0,
                },
            },
            [],
            [],
        )

    def _extract_members(
        self, payload: bytes, members: Sequence[CentralMember]
    ) -> Tuple[List[Dict[str, Any]], List[str], List[int]]:
        rows: List[Dict[str, Any]] = []
        reasons: List[str] = []
        nested_magic_positions: List[int] = []
        total_read = 0
        limits = self.config["limits"]
        try:
            with zipfile.ZipFile(io.BytesIO(payload), "r") as archive:
                infos = archive.infolist()
                if len(infos) != len(members):
                    return [], ["zipfile_central_count_mismatch"], []
                for member, info in zip(members, infos):
                    row = member.to_dict()
                    row.update({"read_size": 0, "computed_crc32": None, "extraction_status": "NOT_READ"})
                    if member.member_type == "DIRECTORY":
                        row["extraction_status"] = "DIRECTORY"
                        rows.append(row)
                        continue
                    crc = 0
                    read_size = 0
                    signature_prefix = b""
                    try:
                        with archive.open(info, "r") as member_file:
                            while True:
                                chunk = member_file.read(65536)
                                if not chunk:
                                    break
                                read_size += len(chunk)
                                total_read += len(chunk)
                                if len(signature_prefix) < 4:
                                    signature_prefix = (signature_prefix + chunk)[:4]
                                if read_size > limits["max_single_member_uncompressed_bytes"]:
                                    raise ArchiveInputError("read_counter_single_limit_exceeded:" + str(member.position))
                                if total_read > limits["max_total_uncompressed_bytes"]:
                                    raise ArchiveInputError("read_counter_total_limit_exceeded")
                                crc = zlib.crc32(chunk, crc)
                    except (OSError, RuntimeError, zipfile.BadZipFile, zlib.error, ArchiveInputError) as error:
                        reasons.append("member_read_failure:" + str(member.position) + ":" + type(error).__name__)
                        row.update({"read_size": read_size, "computed_crc32": crc & 0xFFFFFFFF, "extraction_status": "INCOMPLETE"})
                        rows.append(row)
                        continue
                    computed_crc = crc & 0xFFFFFFFF
                    if (
                        member.technical_kind == ContainerKind.ATTACHMENT_FILE.value
                        and signature_prefix in {LOCAL_SIGNATURE, EOCD_SIGNATURE}
                    ):
                        nested_magic_positions.append(member.position)
                    if read_size != member.uncompressed_size:
                        reasons.append("member_size_mismatch:" + str(member.position))
                    if computed_crc != member.crc32:
                        reasons.append("member_crc_mismatch:" + str(member.position))
                    row.update(
                        {
                            "read_size": read_size,
                            "computed_crc32": computed_crc,
                            "extraction_status": "COMPLETE"
                            if read_size == member.uncompressed_size and computed_crc == member.crc32
                            else "INCOMPLETE",
                        }
                    )
                    rows.append(row)
        except (OSError, zipfile.BadZipFile) as error:
            return rows, ["zip_read_failure:" + type(error).__name__], nested_magic_positions
        return rows, list(dict.fromkeys(reasons)), nested_magic_positions

    def _build_containers(self, source_id: str, archive_digest: str, attachment_position: int, members: Sequence[CentralMember], archive_complete: bool) -> Tuple[List[Dict[str, Any]], List[str]]:
        list_id = "container:" + source_id + ":attachment-list"
        archive_id = "container:" + source_id + ":archive:" + str(attachment_position)
        child_ids = [
            "container:" + hashlib.sha256(
                (archive_digest + ":" + str(member.position) + ":" + member.collision_key).encode("utf-8")
            ).hexdigest()[:24]
            for member in members
        ]
        containers: List[Container] = [
            Container(
                container_id=list_id,
                parent_container_id=source_id,
                kind=ContainerKind.ATTACHMENT_LIST.value,
                locator="attachments",
                content_fingerprint=_digest_json({"source_id": source_id, "archive_position": attachment_position}),
                enumeration_status=EnumerationStatus.COMPLETE.value,
                completeness=True,
                candidate_count=1,
                child_container_refs=[archive_id],
                required=True,
            ),
            Container(
                container_id=archive_id,
                parent_container_id=list_id,
                kind=ContainerKind.ARCHIVE.value,
                locator="attachment:" + str(attachment_position),
                content_fingerprint=archive_digest,
                enumeration_status=EnumerationStatus.COMPLETE.value,
                completeness=archive_complete,
                candidate_count=sum(member.role == "ITEM_CANDIDATE" for member in members),
                child_container_refs=child_ids,
                reasons=[] if archive_complete else ["archive_not_complete"],
                required=True,
            ),
        ]
        for member, child_id in zip(members, child_ids):
            child_complete = archive_complete and member.role != "NESTED_ARCHIVE"
            containers.append(
                Container(
                    container_id=child_id,
                    parent_container_id=archive_id,
                    kind=member.technical_kind,
                    locator="zip-member:" + str(member.position) + ":" + member.original_name,
                    content_fingerprint=_digest_json(member.authority_tuple()),
                    enumeration_status=(EnumerationStatus.UNSUPPORTED.value if member.role == "NESTED_ARCHIVE" else EnumerationStatus.COMPLETE.value),
                    completeness=child_complete,
                    candidate_count=1 if member.role == "ITEM_CANDIDATE" else 0,
                    child_container_refs=[],
                    reasons=["nested_archive_detect_only"] if member.role == "NESTED_ARCHIVE" else [],
                    required=True,
                )
            )
        rows = [container.to_dict() for container in containers]
        source_refs = [container.container_id for container in containers if container.required]
        return rows, validate_archive_graph(rows, source_refs)

    def _parse(self, mail: Dict[str, Any]) -> ArchiveParseResult:
        position = self._archive_position(mail)
        attachment = mail["attachments"][position]
        payload = _strict_base64url_decode(attachment.get("data"))
        archive_digest = "sha256:" + hashlib.sha256(payload).hexdigest()
        source_validation = _validate_source_manifest(mail, position, payload)
        member_role_config = self._active_member_role_config(mail)
        if len(payload) > self.config["limits"]["max_archive_compressed_bytes"]:
            return self._archive_size_failure(
                mail, payload, source_validation, member_role_config
            )
        structure = _parse_structure(payload, self.config, member_role_config)
        members = structure.members
        central_rows = [member.authority_tuple() for member in members]
        authority = _validate_member_authority(
            mail, archive_digest, members, self.config["member_manifest_schema_version"]
        )
        item_cardinality = _validate_item_cardinality(mail, members)
        local_reasons = _local_header_reasons(
            payload, members, structure.eocd["central_directory_offset"]
        )
        path_reasons = [
            "member:" + str(member.position) + ":" + reason
            for member in members
            for reason in member.path_reasons
        ]
        type_reasons = [
            "member:" + str(member.position) + ":" + reason
            for member in members
            for reason in member.type_reasons
        ]
        collision_counts: Dict[str, int] = {}
        for member in members:
            collision_counts[member.collision_key] = collision_counts.get(member.collision_key, 0) + 1
        duplicate_reasons = [
            "duplicate_normalized_member:" + key
            for key, count in collision_counts.items()
            if count > 1
        ]
        encrypted_positions = [
            member.position
            for member in members
            if member.flags & 0x1 or AES_EXTRA_FIELD in member.extra_fields or member.compression_method == 99
        ]
        unsupported_method_positions = [
            member.position
            for member in members
            if member.compression_method not in SUPPORTED_METHODS and member.position not in encrypted_positions
        ]
        nested_positions = [member.position for member in members if member.role == "NESTED_ARCHIVE"]
        unknown_positions = [member.position for member in members if member.role == "UNKNOWN"]
        limit_reasons = metadata_limit_reasons(members, len(payload), self.config["limits"])
        security_reasons = path_reasons + type_reasons + duplicate_reasons + limit_reasons
        unsupported_reasons = list(structure.unsupported_reasons)
        if encrypted_positions:
            unsupported_reasons.append("encrypted_member_detected:" + ",".join(map(str, encrypted_positions)))
        if unsupported_method_positions:
            unsupported_reasons.append("unsupported_compression_method:" + ",".join(map(str, unsupported_method_positions)))
        if nested_positions:
            unsupported_reasons.append("nested_archive_detect_only:" + ",".join(map(str, nested_positions)))
        pre_read_reasons = structure.structure_reasons + local_reasons + security_reasons + unsupported_reasons
        if pre_read_reasons:
            member_rows = [
                dict(member.to_dict(), read_size=0, computed_crc32=None, extraction_status="NOT_READ")
                for member in members
            ]
            extraction_reasons: List[str] = []
            nested_magic_positions: List[int] = []
        else:
            member_rows, extraction_reasons, nested_magic_positions = self._extract_members(payload, members)
        if nested_magic_positions:
            members = [
                replace(
                    member,
                    technical_kind=ContainerKind.ARCHIVE.value,
                    role="NESTED_ARCHIVE",
                    role_key="",
                )
                if member.position in nested_magic_positions
                else member
                for member in members
            ]
            for row in member_rows:
                if row["position"] in nested_magic_positions:
                    row["technical_kind"] = ContainerKind.ARCHIVE.value
                    row["role"] = "NESTED_ARCHIVE"
                    row["role_key"] = ""
            nested_positions = sorted(set(nested_positions + nested_magic_positions))
            unsupported_reasons.append(
                "nested_archive_magic_detect_only:"
                + ",".join(map(str, nested_magic_positions))
            )
            unknown_positions = [member.position for member in members if member.role == "UNKNOWN"]
        enumeration_reasons = [
            reason
            for reason in authority["reasons"]
            if reason.startswith("member_authority_count_")
            or reason.startswith("member_authority_digest_")
            or reason.startswith("member_enumeration_")
        ]
        enumeration_status = "COMPLETE" if not enumeration_reasons else "INCOMPLETE"
        integrity_reasons = structure.structure_reasons + local_reasons + extraction_reasons
        integrity_status = "COMPLETE" if not integrity_reasons and not pre_read_reasons else "INCOMPLETE"
        security_status = "PASS" if not security_reasons else "FAIL"
        credential_status = "PASSWORD_REQUIRED" if encrypted_positions else "NOT_ENCRYPTED"
        member_roles_status = "HUMAN_REVIEW" if unknown_positions else "PASS"
        source_complete = source_validation["status"] == "VERIFIED_COMPLETE"
        proof_complete = authority["status"] == "VERIFIED_COMPLETE"
        item_complete = item_cardinality["status"] == "PASS"
        archive_complete = all(
            (
                enumeration_status == "COMPLETE",
                integrity_status == "COMPLETE",
                security_status == "PASS",
                not unsupported_reasons,
                member_roles_status == "PASS",
                proof_complete,
            )
        )
        containers, graph_reasons = self._build_containers(
            str(mail.get("message_id", "")), archive_digest, position, members, archive_complete
        ) if not security_reasons else ([], [])
        child_proof_reasons = (
            validate_child_container_proof(members, containers) if containers else []
        )
        graph_reasons = graph_reasons + child_proof_reasons
        if graph_reasons:
            archive_complete = False
        reasons = list(
            dict.fromkeys(
                source_validation.get("reasons", [])
                + authority["reasons"]
                + item_cardinality["reasons"]
                + structure.structure_reasons
                + local_reasons
                + security_reasons
                + unsupported_reasons
                + extraction_reasons
                + (["unknown_member_role:" + ",".join(map(str, unknown_positions))] if unknown_positions else [])
                + graph_reasons
            )
        )
        if graph_reasons:
            status = "SYSTEM_FAILURE"
        elif unknown_positions:
            status = "HUMAN_REVIEW"
        elif unsupported_reasons or security_reasons:
            status = "UNSUPPORTED"
        elif not archive_complete or not source_complete or not item_complete:
            status = "PARTIAL"
        else:
            status = "PARSED"
        eligible = (
            sum(member.role == "ITEM_CANDIDATE" for member in members)
            if status == "PARSED"
            else 0
        )
        source_refs = [container["container_id"] for container in containers if container.get("required")]
        source = {
            "source_id": str(mail.get("message_id", "")),
            "source_acquisition_status": source_validation["status"],
            "source_attachment_validation": source_validation,
            "source_item_cardinality": item_cardinality,
            "source_atomic_status": status,
            "container_references": source_refs,
            "auto_union_eligible": status == "PARSED",
            "eligible_item_candidate_count": eligible,
            "reasons": reasons,
        }
        archive = {
            "format": "ZIP",
            "archive_schema_version": self.config["archive_schema_version"],
            "parser_id": PARSER_ID,
            "parser_version": PARSER_VERSION,
            "config_id": self.config["config_id"],
            "config_version": self.config["config_version"],
            "member_role_config_id": member_role_config.get("role_config_id"),
            "member_role_config_version": member_role_config.get("role_config_version"),
            "archive_sha256": archive_digest,
            "archive_compressed_bytes": len(payload),
            "eocd": structure.eocd,
            "central_directory_offset": structure.eocd["central_directory_offset"],
            "central_directory_size": structure.eocd["central_directory_size"],
            "central_directory_count": len(members),
            "central_directory_ordered_digest": structure.central_directory_digest,
            "expected_member_proof": authority,
            "observed_child_container_count": max(0, len(containers) - 2),
            "observed_child_container_refs": [container["container_id"] for container in containers[2:]],
            "observed_child_ordered_digest": _digest_json(
                [container["container_id"] for container in containers[2:]]
            ),
            "enumeration_status": enumeration_status,
            "integrity_status": integrity_status,
            "security_status": security_status,
            "credential_status": credential_status,
            "credential_state_contract": [
                "NOT_ENCRYPTED", "PASSWORD_REQUIRED", "PASSWORD_AVAILABLE",
                "PASSWORD_AMBIGUOUS", "PASSWORD_INVALID", "DECRYPTION_FAILED",
            ],
            "password_persistence": "EPHEMERAL_ONLY",
            "member_roles_status": member_roles_status,
            "processing_status": "SUPPORTED" if not unsupported_reasons else "UNSUPPORTED",
            "member_extraction_status": integrity_status,
            "archive_complete": archive_complete,
            "nested_expansion_performed": False,
            "graph_status": "PASS" if not graph_reasons else "FAIL",
            "child_container_proof_status": "PASS" if not child_proof_reasons else "FAIL",
            "reasons": reasons,
            "totals": {
                "members": len(members),
                "item_candidates": sum(member.role == "ITEM_CANDIDATE" for member in members),
                "compressed_member_bytes": sum(member.compressed_size for member in members),
                "uncompressed_member_bytes": sum(member.uncompressed_size for member in members),
            },
        }
        return ArchiveParseResult(status, reasons, eligible, source, archive, member_rows, containers)

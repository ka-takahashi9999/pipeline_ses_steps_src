#!/usr/bin/env python3
"""Safe config-driven XLSX enumerator for the test-only 99-1 P7 lab."""

import base64
import binascii
import hashlib
import io
import json
import posixpath
import re
import unicodedata
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from email.utils import parseaddr
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from xml.etree import ElementTree as ET

from attachment_manifest_contract import (
    MANIFEST_FIELD,
    MANIFEST_SCHEMA_VERSION,
    canonical_ordered_entries,
    ordered_attachment_digest,
    source_payload_digest,
    validate_authoritative_attachment_entries,
)
from identity import (
    artifact_set_fingerprint,
    body_fingerprint,
    derived_item_id,
    logical_item_id,
    normalize_content,
    version_fingerprint,
)
from variable_item_core import (
    CardinalityAuthority,
    CardinalityEvidence,
    Container,
    ContainerKind,
    DeliverySemantics,
    EnumerationStatus,
    ItemCandidate,
    Source,
    evaluate_completeness,
)
from spreadsheet_fixture_source import (
    DECLARED_ITEM_EVIDENCE_FIELD,
    STRUCTURE_MANIFEST_FIELD,
    STRUCTURE_MANIFEST_VERSION,
)


PARSER_ID = "multi_record_spreadsheet"
PARSER_VERSION = "1.0.0"
MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
CELL_REFERENCE = re.compile(r"\A([A-Z]+)([1-9][0-9]*)\Z")
SHA256_PATTERN = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
SUPPORTED_METHODS = {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}
REQUIRED_PACKAGE_PARTS = {
    "[Content_Types].xml",
    "_rels/.rels",
    "xl/workbook.xml",
    "xl/_rels/workbook.xml.rels",
}


class SpreadsheetInputError(Exception):
    def __init__(self, reason: str, status: str = "PARTIAL"):
        super().__init__(reason)
        self.reason = reason
        self.status = status


@dataclass(frozen=True)
class SpreadsheetParseResult:
    status: str
    reasons: List[str]
    eligible_item_candidate_count: int
    source: Dict[str, Any]
    workbook: Dict[str, Any]
    sheets: List[Dict[str, Any]] = field(default_factory=list)
    record_occurrences: List[Dict[str, Any]] = field(default_factory=list)
    items: List[Dict[str, Any]] = field(default_factory=list)
    technical_items: List[Dict[str, Any]] = field(default_factory=list)
    containers: List[Dict[str, Any]] = field(default_factory=list)


def _normalize(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"\s+", " ", text).strip().casefold()


def _digest_json(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _strict_base64url_decode(value: Any) -> bytes:
    if not isinstance(value, str) or not value:
        raise SpreadsheetInputError("attachment_data_missing")
    if re.fullmatch(r"[A-Za-z0-9_-]+={0,2}", value) is None:
        raise SpreadsheetInputError("attachment_data_invalid_base64url")
    try:
        encoded = value.encode("ascii")
        return base64.b64decode(
            encoded + b"=" * (-len(encoded) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (UnicodeEncodeError, binascii.Error, ValueError) as error:
        raise SpreadsheetInputError("attachment_data_invalid_base64url") from error


def _column_index(value: str) -> int:
    result = 0
    for character in value:
        if character < "A" or character > "Z":
            raise SpreadsheetInputError("cell_column_reference_invalid")
        result = result * 26 + ord(character) - 64
    return result


def _column_name(index: int) -> str:
    value = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        value = chr(65 + remainder) + value
    return value


def _safe_xml(payload: bytes, part_name: str, max_bytes: int) -> ET.Element:
    if len(payload) > max_bytes:
        raise SpreadsheetInputError("xml_size_limit_exceeded:" + part_name, "UNSUPPORTED")
    lowered = payload.lower()
    if b"<!doctype" in lowered or b"<!entity" in lowered:
        raise SpreadsheetInputError("xml_dtd_or_entity_rejected:" + part_name, "UNSUPPORTED")
    try:
        return ET.fromstring(payload)
    except ET.ParseError as error:
        raise SpreadsheetInputError("xml_malformed:" + part_name) from error


def _normalized_package_path(name: str) -> Tuple[str, List[str]]:
    normalized = unicodedata.normalize("NFKC", name).replace("\\", "/")
    reasons: List[str] = []
    if not normalized or "\x00" in normalized:
        reasons.append("package_member_name_invalid")
    if normalized.startswith("/") or re.match(r"\A[A-Za-z]:/", normalized):
        reasons.append("package_member_absolute_path")
    segments = normalized.split("/")
    if any(segment in {"", ".", ".."} for segment in segments):
        reasons.append("package_member_path_anomaly")
    canonical = posixpath.normpath(normalized)
    if canonical == ".." or canonical.startswith("../"):
        reasons.append("package_member_path_traversal")
    return canonical.casefold(), list(dict.fromkeys(reasons))


def _rels_source_part(name: str) -> str:
    if name == "_rels/.rels":
        return ""
    directory, basename = posixpath.split(name)
    if not directory.endswith("/_rels") or not basename.endswith(".rels"):
        raise SpreadsheetInputError("relationship_part_path_invalid:" + name)
    parent = directory[: -len("/_rels")]
    return posixpath.join(parent, basename[: -len(".rels")])


def _resolve_relationship_target(source_part: str, target: str) -> str:
    normalized = unicodedata.normalize("NFKC", target).replace("\\", "/")
    if not normalized or normalized.startswith("//") or re.match(r"\A[A-Za-z]+:", normalized):
        raise SpreadsheetInputError("relationship_target_path_anomaly")
    if normalized.startswith("/"):
        resolved = posixpath.normpath(normalized[1:])
    else:
        base = posixpath.dirname(source_part)
        resolved = posixpath.normpath(posixpath.join(base, normalized))
    if resolved == ".." or resolved.startswith("../"):
        raise SpreadsheetInputError("relationship_target_path_traversal")
    return resolved


def _relationship_cycle(graph: Dict[str, Set[str]]) -> bool:
    visiting: Set[str] = set()
    visited: Set[str] = set()

    def visit(node: str) -> bool:
        if node in visiting:
            return True
        if node in visited:
            return False
        visiting.add(node)
        if any(visit(child) for child in graph.get(node, set())):
            return True
        visiting.remove(node)
        visited.add(node)
        return False

    return any(visit(node) for node in list(graph))


def _cell_value(
    cell: ET.Element, shared_strings: Sequence[str]
) -> Tuple[str, bool, str]:
    formula = cell.find("{%s}f" % MAIN_NS)
    value = cell.find("{%s}v" % MAIN_NS)
    cell_type = cell.get("t", "")
    if cell_type == "inlineStr":
        text = "".join(
            element.text or "" for element in cell.findall(".//{%s}t" % MAIN_NS)
        )
    elif cell_type == "s":
        try:
            index = int(value.text if value is not None and value.text is not None else "")
            text = shared_strings[index]
        except (ValueError, IndexError) as error:
            raise SpreadsheetInputError("shared_string_index_invalid") from error
    else:
        text = value.text if value is not None and value.text is not None else ""
    return text, formula is not None, formula.text or "" if formula is not None else ""


def _dimension_max(reference: str) -> Tuple[int, int]:
    endpoint = reference.split(":")[-1].replace("$", "")
    match = CELL_REFERENCE.fullmatch(endpoint)
    if match is None:
        raise SpreadsheetInputError("worksheet_dimension_invalid")
    return int(match.group(2)), _column_index(match.group(1))


class SpreadsheetParser:
    """Enumerate config-shaped row-field/column-record XLSX workbooks."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        if config.get("supported_format") != "XLSX":
            raise ValueError("P7 supports XLSX only")
        forbidden = {
            "expected_sheet_count",
            "expected_business_sheet_count",
            "expected_record_count",
            "expected_actual_count",
        }
        if forbidden & set(config):
            raise ValueError("fixed actual cardinality is forbidden")
        selectors = config["selectors"]
        self._subject_regex = re.compile(selectors["subject_regex"], re.IGNORECASE)
        self._attachment_regex = re.compile(
            selectors["attachment_filename_regex"], re.IGNORECASE
        )
        role_names: Dict[str, str] = {}
        for role, role_config in config["sheet_roles"].items():
            if role not in {"AUTHORITATIVE", "DERIVED_VIEW", "SUPPORTING"}:
                raise ValueError("unsupported configured sheet role:" + role)
            for name in role_config.get("exact_names", []):
                if name in role_names:
                    raise ValueError("sheet role name collision:" + name)
                role_names[name] = role
        self._role_names = role_names

    @classmethod
    def from_file(cls, path: Path) -> "SpreadsheetParser":
        with path.open(encoding="utf-8") as file_object:
            return cls(json.load(file_object))

    def matches(self, mail: Dict[str, Any]) -> bool:
        sender = parseaddr(str(mail.get("from", "")))[1]
        domain = sender.rsplit("@", 1)[-1].casefold() if "@" in sender else ""
        filenames = [
            str(attachment.get("filename", ""))
            for attachment in mail.get("attachments", [])
            if isinstance(attachment, dict)
        ]
        return (
            domain == self.config["selectors"]["sender_domain"].casefold()
            and bool(self._subject_regex.search(str(mail.get("subject", ""))))
            and any(self._attachment_regex.fullmatch(name) for name in filenames)
        )

    def parse(self, mail: Dict[str, Any]) -> SpreadsheetParseResult:
        try:
            return self._parse(mail)
        except SpreadsheetInputError as error:
            return self._failed(mail, error.reason, error.status)
        except Exception as error:
            return self._failed(
                mail, "system_failure:" + type(error).__name__, "SYSTEM_FAILURE"
            )

    def _failed(
        self, mail: Dict[str, Any], reason: str, status: str
    ) -> SpreadsheetParseResult:
        source_id = str(mail.get("message_id", ""))
        return SpreadsheetParseResult(
            status=status,
            reasons=[reason],
            eligible_item_candidate_count=0,
            source={
                "source_id": source_id,
                "source_acquisition_status": "UNVERIFIED",
                "source_atomic_status": status,
                "auto_union_eligible": False,
                "eligible_item_candidate_count": 0,
                "reasons": [reason],
            },
            workbook={
                "format": "XLSX",
                "parser_id": PARSER_ID,
                "parser_version": PARSER_VERSION,
                "technical_workbook_status": "UNAVAILABLE",
                "package_integrity_status": "FAIL",
                "external_resolution_count": 0,
                "formula_evaluation_count": 0,
                "reasons": [reason],
            },
        )

    def _attachment_position(self, mail: Dict[str, Any]) -> int:
        attachments = mail.get("attachments")
        if not isinstance(attachments, list):
            raise SpreadsheetInputError("attachments_not_list")
        unsupported = [
            str(attachment.get("filename", ""))
            for attachment in attachments
            if isinstance(attachment, dict)
            and str(attachment.get("filename", "")).casefold().endswith(
                (".xls", ".xlsm", ".ods", ".csv")
            )
        ]
        positions = [
            index
            for index, attachment in enumerate(attachments)
            if isinstance(attachment, dict)
            and str(attachment.get("filename", "")).casefold().endswith(".xlsx")
            and self._attachment_regex.fullmatch(str(attachment.get("filename", "")))
        ]
        if len(positions) != 1:
            if unsupported and not positions:
                raise SpreadsheetInputError(
                    "spreadsheet_format_unsupported:" + unsupported[0], "UNSUPPORTED"
                )
            raise SpreadsheetInputError(
                "xlsx_attachment_candidate_count:" + str(len(positions))
            )
        return positions[0]

    def _source_acquisition(
        self, mail: Dict[str, Any], target_position: int
    ) -> Dict[str, Any]:
        attachments = mail.get("attachments", [])
        observed_entries: List[Dict[str, Any]] = []
        integrity_reasons: List[str] = []
        for position, attachment in enumerate(attachments):
            if not isinstance(attachment, dict):
                observed_entries.append({})
                integrity_reasons.append("attachment_not_object:" + str(position))
                continue
            try:
                payload = _strict_base64url_decode(attachment.get("data"))
                digest = "sha256:" + hashlib.sha256(payload).hexdigest()
            except SpreadsheetInputError as error:
                payload = b""
                digest = ""
                integrity_reasons.append(error.reason + ":" + str(position))
            size = attachment.get("size")
            if isinstance(size, bool) or not isinstance(size, int) or size < 0:
                integrity_reasons.append("attachment_size_invalid:" + str(position))
            elif size != len(payload):
                integrity_reasons.append("attachment_size_mismatch:" + str(position))
            observed_entries.append(
                {
                    "position": position,
                    "source_entry_id": attachment.get(
                        "source_entry_id", "part-attachment-" + str(position)
                    ),
                    "filename": attachment.get("filename", ""),
                    "mime_type": attachment.get("mime_type", ""),
                    "declared_size": size,
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
                "attachment_integrity_status": (
                    "PASS" if not integrity_reasons else "FAIL"
                ),
                "target_position": target_position,
                "observed_ordered_count": len(observed_entries),
                "observed_ordered_digest": ordered_attachment_digest(observed_entries),
                "reasons": ["source_owned_attachment_manifest_missing"]
                + integrity_reasons,
            }
        reasons: List[str] = []
        authority = manifest.get("authoritative_attachment_entries")
        authority_rows = authority if isinstance(authority, list) else []
        reasons.extend(validate_authoritative_attachment_entries(authority))
        canonical_authority = canonical_ordered_entries(authority_rows)
        if manifest.get("manifest_schema_version") != MANIFEST_SCHEMA_VERSION:
            reasons.append("manifest_schema_mismatch")
        if manifest.get("source_id") != mail.get("message_id"):
            reasons.append("manifest_source_id_mismatch")
        if (
            manifest.get("acquisition_status") != "COMPLETE"
            or manifest.get("extractor_status") != "COMPLETE"
        ):
            reasons.append("manifest_acquisition_incomplete")
        if manifest.get("reasons") != []:
            reasons.append("manifest_reasons_not_empty")
        if manifest.get("expected_ordered_count") != len(canonical_authority):
            reasons.append("manifest_count_mismatch")
        if manifest.get("expected_ordered_digest") != ordered_attachment_digest(
            canonical_authority
        ):
            reasons.append("manifest_digest_mismatch")
        if manifest.get("source_payload_digest") != source_payload_digest(
            mail, canonical_authority
        ):
            reasons.append("source_payload_digest_mismatch")
        if canonical_ordered_entries(observed_entries) != canonical_authority:
            reasons.append("observed_attachment_entries_mismatch")
        reasons.extend(integrity_reasons)
        return {
            "status": "VERIFIED_COMPLETE" if not reasons else "INCOMPLETE",
            "manifest_contract_status": "PASS" if not reasons else "FAIL",
            "attachment_integrity_status": "PASS" if not integrity_reasons else "FAIL",
            "target_position": target_position,
            "observed_ordered_count": len(observed_entries),
            "observed_ordered_digest": ordered_attachment_digest(observed_entries),
            "reasons": list(dict.fromkeys(reasons)),
        }

    def _read_package(
        self, payload: bytes
    ) -> Tuple[Dict[str, bytes], List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
        limits = self.config["limits"]
        if len(payload) > limits["max_package_bytes"]:
            raise SpreadsheetInputError("package_size_limit_exceeded", "UNSUPPORTED")
        if len(payload) < 4 or payload[:2] != b"PK":
            raise SpreadsheetInputError("xlsx_zip_signature_invalid")
        try:
            package = zipfile.ZipFile(io.BytesIO(payload), "r")
            infos = package.infolist()
        except (zipfile.BadZipFile, OSError) as error:
            raise SpreadsheetInputError("xlsx_zip_corrupt") from error
        if len(infos) > limits["max_member_count"]:
            raise SpreadsheetInputError("package_member_count_limit_exceeded", "UNSUPPORTED")
        total_expanded = sum(info.file_size for info in infos)
        total_compressed = sum(info.compress_size for info in infos)
        if total_expanded > limits["max_total_expanded_bytes"]:
            raise SpreadsheetInputError("package_expanded_size_limit_exceeded", "UNSUPPORTED")
        if total_expanded / max(1, total_compressed) > limits["max_expansion_ratio"]:
            raise SpreadsheetInputError("package_expansion_ratio_limit_exceeded", "UNSUPPORTED")
        normalized_counts: Counter = Counter()
        member_rows: List[Dict[str, Any]] = []
        for position, info in enumerate(infos):
            collision_key, path_reasons = _normalized_package_path(info.filename)
            if path_reasons:
                raise SpreadsheetInputError(
                    "package_member:" + str(position) + ":" + path_reasons[0],
                    "UNSUPPORTED",
                )
            normalized_counts[collision_key] += 1
            if info.flag_bits & 0x1:
                raise SpreadsheetInputError("encrypted_package_member", "UNSUPPORTED")
            if info.compress_type not in SUPPORTED_METHODS:
                raise SpreadsheetInputError("unsupported_compression_method", "UNSUPPORTED")
            if info.file_size > limits["max_single_member_expanded_bytes"]:
                raise SpreadsheetInputError("package_member_size_limit_exceeded", "UNSUPPORTED")
            if info.file_size / max(1, info.compress_size) > limits["max_expansion_ratio"]:
                raise SpreadsheetInputError("package_member_ratio_limit_exceeded", "UNSUPPORTED")
            member_rows.append(
                {
                    "position": position,
                    "name": info.filename,
                    "compressed_size": info.compress_size,
                    "expanded_size": info.file_size,
                    "crc32": info.CRC,
                    "compression_method": info.compress_type,
                }
            )
        duplicates = [key for key, count in normalized_counts.items() if count > 1]
        if duplicates:
            raise SpreadsheetInputError("duplicate_package_member:" + duplicates[0], "UNSUPPORTED")
        names = {info.filename for info in infos}
        missing = sorted(REQUIRED_PACKAGE_PARTS - names)
        if missing:
            raise SpreadsheetInputError("required_package_part_missing:" + missing[0])
        forbidden = [
            name
            for name in names
            if name.casefold().startswith(
                ("xl/externalLinks/".casefold(), "xl/embeddings/", "xl/oleobjects/")
            )
            or name.casefold().endswith("vbaproject.bin")
        ]
        if forbidden:
            raise SpreadsheetInputError("executable_or_external_part_unsupported:" + forbidden[0], "UNSUPPORTED")
        try:
            bad_member = package.testzip()
            if bad_member is not None:
                raise SpreadsheetInputError("package_crc_failure:" + bad_member)
            parts = {info.filename: package.read(info) for info in infos}
        except (zipfile.BadZipFile, RuntimeError, OSError) as error:
            raise SpreadsheetInputError("package_member_read_failure") from error
        finally:
            package.close()
        for name, part_payload in parts.items():
            if name.casefold().endswith(".xml"):
                _safe_xml(part_payload, name, limits["max_xml_bytes"])
        relationship_rows: Dict[str, List[Dict[str, Any]]] = {}
        graph: Dict[str, Set[str]] = defaultdict(set)
        for name in sorted(part for part in parts if part.endswith(".rels")):
            source_part = _rels_source_part(name)
            root = _safe_xml(parts[name], name, limits["max_xml_bytes"])
            ids: Set[str] = set()
            rows: List[Dict[str, Any]] = []
            for relation in root.findall("{%s}Relationship" % PACKAGE_REL_NS):
                relationship_id = relation.get("Id", "")
                target = relation.get("Target", "")
                if not relationship_id or relationship_id in ids:
                    raise SpreadsheetInputError("relationship_id_invalid_or_duplicate")
                ids.add(relationship_id)
                if relation.get("TargetMode", "").casefold() == "external":
                    raise SpreadsheetInputError("external_relationship_unsupported", "UNSUPPORTED")
                resolved = _resolve_relationship_target(source_part, target)
                if resolved not in parts:
                    raise SpreadsheetInputError("relationship_target_missing:" + resolved)
                graph[source_part].add(resolved)
                rows.append(
                    {
                        "id": relationship_id,
                        "type": relation.get("Type", ""),
                        "target": resolved,
                    }
                )
            relationship_rows[source_part] = rows
        if _relationship_cycle(graph):
            raise SpreadsheetInputError("relationship_cycle_detected", "UNSUPPORTED")
        root_office = [
            row
            for row in relationship_rows.get("", [])
            if row["type"].endswith("/officeDocument")
        ]
        if len(root_office) != 1 or root_office[0]["target"] != "xl/workbook.xml":
            raise SpreadsheetInputError("root_workbook_relationship_invalid")
        return parts, member_rows, relationship_rows

    def _shared_strings(self, parts: Dict[str, bytes]) -> List[str]:
        if "xl/sharedStrings.xml" not in parts:
            return []
        root = _safe_xml(
            parts["xl/sharedStrings.xml"],
            "xl/sharedStrings.xml",
            self.config["limits"]["max_xml_bytes"],
        )
        return [
            "".join(element.text or "" for element in item.findall(".//{%s}t" % MAIN_NS))
            for item in root.findall("{%s}si" % MAIN_NS)
        ]

    def _ordered_sheets(
        self,
        parts: Dict[str, bytes],
        relationships: Dict[str, List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        root = _safe_xml(
            parts["xl/workbook.xml"],
            "xl/workbook.xml",
            self.config["limits"]["max_xml_bytes"],
        )
        relation_by_id = {
            row["id"]: row for row in relationships.get("xl/workbook.xml", [])
        }
        sheets: List[Dict[str, Any]] = []
        names: Set[str] = set()
        for position, sheet in enumerate(root.findall(".//{%s}sheet" % MAIN_NS)):
            name = sheet.get("name", "")
            state = sheet.get("state", "visible")
            relationship_id = sheet.get("{%s}id" % REL_NS, "")
            relation = relation_by_id.get(relationship_id)
            if not name or name in names:
                raise SpreadsheetInputError("workbook_sheet_name_invalid_or_duplicate")
            names.add(name)
            if relation is None or not relation["type"].endswith("/worksheet"):
                raise SpreadsheetInputError("worksheet_relationship_missing:" + name)
            role = self._role_names.get(name, "UNKNOWN")
            role_config = self.config["sheet_roles"].get(role, {})
            if role != "UNKNOWN" and state not in role_config.get(
                "allowed_visibility", []
            ):
                role = "UNKNOWN"
            sheets.append(
                {
                    "position": position,
                    "name": name,
                    "state": state,
                    "relationship_id": relationship_id,
                    "target": relation["target"],
                    "role": role,
                }
            )
        if len(sheets) > self.config["limits"]["max_sheets"]:
            raise SpreadsheetInputError("sheet_count_limit_exceeded", "UNSUPPORTED")
        return sheets

    def _worksheet_cells(
        self, parts: Dict[str, bytes], target: str, shared_strings: Sequence[str]
    ) -> Tuple[Dict[Tuple[int, int], Dict[str, Any]], int, int]:
        root = _safe_xml(
            parts[target], target, self.config["limits"]["max_xml_bytes"]
        )
        dimension = root.find("{%s}dimension" % MAIN_NS)
        if dimension is None or not dimension.get("ref"):
            raise SpreadsheetInputError("worksheet_dimension_missing:" + target)
        max_row, max_column = _dimension_max(dimension.get("ref", ""))
        limits = self.config["limits"]
        if max_row > limits["max_rows_per_sheet"]:
            raise SpreadsheetInputError("worksheet_row_limit_exceeded", "UNSUPPORTED")
        if max_column > limits["max_columns_per_sheet"]:
            raise SpreadsheetInputError("worksheet_column_limit_exceeded", "UNSUPPORTED")
        cells: Dict[Tuple[int, int], Dict[str, Any]] = {}
        observed_max_row = 0
        observed_max_column = 0
        for cell in root.findall(".//{%s}c" % MAIN_NS):
            reference = cell.get("r", "").replace("$", "")
            match = CELL_REFERENCE.fullmatch(reference)
            if match is None:
                raise SpreadsheetInputError("cell_reference_invalid:" + target)
            row = int(match.group(2))
            column = _column_index(match.group(1))
            if (row, column) in cells:
                raise SpreadsheetInputError("duplicate_cell_reference:" + reference)
            value, formula, formula_text = _cell_value(cell, shared_strings)
            cells[(row, column)] = {
                "value": value,
                "formula": formula,
                "formula_text": formula_text,
            }
            observed_max_row = max(observed_max_row, row)
            observed_max_column = max(observed_max_column, column)
        if observed_max_row > max_row or observed_max_column > max_column:
            raise SpreadsheetInputError("worksheet_dimension_understates_cells:" + target)
        return cells, max_row, max_column

    def _record_fingerprint(self, fields: Dict[str, str]) -> str:
        payload = [
            (name, _normalize(fields.get(name, "")))
            for name in self.config["record_layout"]["identity_fields"]
        ]
        return _digest_json(payload)

    def _anchor_fingerprint(self, fields: Dict[str, str]) -> str:
        payload = [
            (name, _normalize(fields.get(name, "")))
            for name in self.config["record_layout"]["ambiguity_anchor_fields"]
        ]
        return _digest_json(payload)

    def _enumerate_records(
        self, parts: Dict[str, bytes], sheets: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
        shared_strings = self._shared_strings(parts)
        layout = self.config["record_layout"]
        field_rows = layout["field_rows"]
        required_fields = layout["required_fields"]
        start_column = _column_index(layout["record_start_column"])
        occurrences: List[Dict[str, Any]] = []
        reasons: List[str] = []
        unsupported: List[str] = []
        for sheet in sheets:
            cells, max_row, max_column = self._worksheet_cells(
                parts, sheet["target"], shared_strings
            )
            sheet["max_row"] = max_row
            sheet["max_column"] = max_column
            sheet["record_occurrence_count"] = 0
            if sheet["role"] == "UNKNOWN":
                reasons.append("unknown_sheet_role:" + sheet["name"])
                continue
            if sheet["role"] == "SUPPORTING":
                continue
            label_failures = [
                field_name
                for field_name, row_number in field_rows.items()
                if _normalize(cells.get((row_number, 1), {}).get("value", ""))
                != _normalize(field_name)
            ]
            if label_failures:
                reasons.append(
                    "field_label_structure_mismatch:"
                    + sheet["name"]
                    + ":"
                    + ",".join(label_failures)
                )
                continue
            for column in range(start_column, max_column + 1):
                fields = {
                    field_name: str(
                        cells.get((row_number, column), {}).get("value", "")
                    ).strip()
                    for field_name, row_number in field_rows.items()
                }
                if not any(fields.values()):
                    reasons.append(
                        "blank_record_column_inside_boundary:"
                        + sheet["name"]
                        + ":"
                        + _column_name(column)
                    )
                    continue
                missing_fields = [
                    field_name for field_name in required_fields if not fields[field_name]
                ]
                formula_fields = [
                    field_name
                    for field_name, row_number in field_rows.items()
                    if cells.get((row_number, column), {}).get("formula")
                ]
                dependent_formula_fields = sorted(
                    set(formula_fields)
                    & set(required_fields + layout["identity_fields"])
                )
                if dependent_formula_fields:
                    unsupported.append(
                        "formula_dependent_field:"
                        + sheet["name"]
                        + ":"
                        + _column_name(column)
                        + ":"
                        + ",".join(dependent_formula_fields)
                    )
                fingerprint = self._record_fingerprint(fields)
                explicit_id = _normalize(fields.get(layout["explicit_id_field"], ""))
                stable_key_fields = layout.get("stable_key_fields", [])
                stable_key_values = [
                    _normalize(fields.get(field_name, ""))
                    for field_name in stable_key_fields
                ]
                if explicit_id:
                    reconciliation_key = "explicit:" + explicit_id
                    identity_strategy = "SOURCE_OWNED_EXPLICIT_ID"
                elif stable_key_fields and all(stable_key_values):
                    reconciliation_key = "stable:" + _digest_json(
                        list(zip(stable_key_fields, stable_key_values))
                    )
                    identity_strategy = "SOURCE_CONFIGURED_STABLE_KEY"
                else:
                    reconciliation_key = "fingerprint:" + fingerprint
                    identity_strategy = (
                        "SOURCE_CONFIGURED_CANONICAL_FIELD_FINGERPRINT"
                    )
                occurrence = {
                    "sheet_position": sheet["position"],
                    "sheet_name": sheet["name"],
                    "sheet_role": sheet["role"],
                    "column_index": column,
                    "column_ref": _column_name(column),
                    "fields": fields,
                    "canonical_fingerprint": fingerprint,
                    "reconciliation_key": reconciliation_key,
                    "identity_strategy": identity_strategy,
                    "ambiguity_anchor_fingerprint": self._anchor_fingerprint(fields),
                    "required_fields_complete": not missing_fields,
                    "missing_required_fields": missing_fields,
                    "formula_fields": formula_fields,
                    "identity_status": "PROVISIONAL",
                }
                occurrences.append(occurrence)
                sheet["record_occurrence_count"] += 1
                if missing_fields:
                    reasons.append(
                        "required_field_missing:"
                        + sheet["name"]
                        + ":"
                        + _column_name(column)
                        + ":"
                        + ",".join(missing_fields)
                    )
        return occurrences, reasons, unsupported

    def _structure_proof(
        self,
        mail: Dict[str, Any],
        workbook_digest: str,
        sheets: Sequence[Dict[str, Any]],
        occurrences: Sequence[Dict[str, Any]],
    ) -> Dict[str, Any]:
        manifest = mail.get(STRUCTURE_MANIFEST_FIELD)
        observed_sheets = []
        by_sheet: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        for occurrence in occurrences:
            by_sheet[occurrence["sheet_position"]].append(occurrence)
        for sheet in sheets:
            observed_sheets.append(
                {
                    "position": sheet["position"],
                    "name": sheet["name"],
                    "state": sheet["state"],
                    "relationship_id": sheet["relationship_id"],
                    "target": sheet["target"],
                    "role": sheet["role"],
                    "expected_records": [
                        {
                            "column_index": row["column_index"],
                            "column_ref": row["column_ref"],
                            "canonical_fingerprint": row["canonical_fingerprint"],
                        }
                        for row in by_sheet.get(sheet["position"], [])
                    ],
                }
            )
        if not isinstance(manifest, dict):
            return {
                "status": "UNVERIFIED",
                "fixture_comparison_status": "UNVERIFIED",
                "observed_ordered_sheet_count": len(observed_sheets),
                "observed_ordered_sheets": observed_sheets,
                "reasons": ["source_owned_workbook_structure_manifest_missing"],
            }
        reasons: List[str] = []
        expected = manifest.get("expected_ordered_sheets")
        expected_rows = expected if isinstance(expected, list) else []
        if manifest.get("manifest_schema_version") != STRUCTURE_MANIFEST_VERSION:
            reasons.append("structure_manifest_schema_mismatch")
        if manifest.get("workbook_sha256") != workbook_digest:
            reasons.append("structure_manifest_workbook_digest_mismatch")
        if manifest.get("expected_ordered_sheet_count") != len(expected_rows):
            reasons.append("structure_manifest_sheet_count_invalid")
        if len(expected_rows) != len(observed_sheets):
            reasons.append("sheet_enumeration_count_mismatch")
        if expected_rows != observed_sheets:
            reasons.append("sheet_or_record_ordered_sequence_mismatch")
        return {
            "status": "VERIFIED_COMPLETE" if not reasons else "INCOMPLETE",
            "fixture_comparison_status": "PASS" if not reasons else "FAIL",
            "expected_ordered_sheet_count": len(expected_rows),
            "observed_ordered_sheet_count": len(observed_sheets),
            "expected_ordered_sheets": expected_rows,
            "observed_ordered_sheets": observed_sheets,
            "reasons": reasons,
        }

    def _reconcile(
        self, occurrences: Sequence[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any], List[str]]:
        authoritative = [
            row for row in occurrences if row["sheet_role"] == "AUTHORITATIVE"
        ]
        derived = [row for row in occurrences if row["sheet_role"] == "DERIVED_VIEW"]
        by_reconciliation_key: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in authoritative:
            by_reconciliation_key[row["reconciliation_key"]].append(row)
        reasons: List[str] = []
        derived_orphans = [
            row
            for row in derived
            if row["reconciliation_key"] not in by_reconciliation_key
        ]
        if derived_orphans:
            reasons.append("derived_view_contains_non_authoritative_record")
        by_anchor: Dict[str, Set[str]] = defaultdict(set)
        for row in authoritative:
            by_anchor[row["ambiguity_anchor_fingerprint"]].add(
                row["canonical_fingerprint"]
            )
        ambiguous_anchors = sorted(
            anchor for anchor, fingerprints in by_anchor.items() if len(fingerprints) > 1
        )
        if ambiguous_anchors:
            reasons.append("ambiguous_duplicate_reconciliation")
        inconsistent_proven_keys = [
            key
            for key, rows in by_reconciliation_key.items()
            if len({row["canonical_fingerprint"] for row in rows}) > 1
        ]
        if inconsistent_proven_keys:
            reasons.append("proven_identity_key_content_conflict")
        canonical = [rows[0] for rows in by_reconciliation_key.values()]
        canonical.sort(key=lambda row: (row["sheet_position"], row["column_index"]))
        all_by_fingerprint: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in occurrences:
            all_by_fingerprint[row["canonical_fingerprint"]].append(row)
        duplicate_groups = [
            {
                "canonical_fingerprint": fingerprint,
                "occurrence_count": len(rows),
                "locations": [
                    row["sheet_name"] + "!" + row["column_ref"] for row in rows
                ],
            }
            for fingerprint, rows in all_by_fingerprint.items()
            if len(rows) > 1
        ]
        reconciliation = {
            "status": "HUMAN_REVIEW" if reasons else "PASS",
            "authoritative_occurrences": len(authoritative),
            "derived_view_occurrences": len(derived),
            "all_record_occurrences": len(occurrences),
            "canonical_candidate_count": len(canonical),
            "distinct_fingerprint_count": len(all_by_fingerprint),
            "duplicate_occurrence_count": sum(
                len(rows) - 1 for rows in all_by_fingerprint.values()
            ),
            "duplicate_group_count": len(duplicate_groups),
            "duplicate_groups": duplicate_groups,
            "ambiguous_group_count": len(ambiguous_anchors),
            "identity_key_conflict_count": len(inconsistent_proven_keys),
            "derived_orphan_count": len(derived_orphans),
            "identity_status": "PROVISIONAL",
            "identity_schema_version": self.config["identity"]["schema_version"],
            "reasons": reasons,
        }
        return canonical, reconciliation, reasons

    def _candidate_rows(
        self, source_id: str, canonical: Sequence[Dict[str, Any]]
    ) -> List[ItemCandidate]:
        candidates: List[ItemCandidate] = []
        for index, occurrence in enumerate(canonical, 1):
            fields = occurrence["fields"]
            fingerprint = occurrence["canonical_fingerprint"]
            reconciliation_key = occurrence["reconciliation_key"]
            logical_id = logical_item_id(
                self.config["source_company"],
                self.config["item_type"],
                self.config["identity"]["schema_version"]
                + ":"
                + reconciliation_key,
            )
            body = normalize_content(
                self.config["canonical_context"]["body_prefix"]
                + "\n"
                + "\n".join(
                    field_name + ": " + fields.get(field_name, "")
                    for field_name in self.config["record_layout"]["field_rows"]
                    if fields.get(field_name, "")
                )
            )
            candidates.append(
                ItemCandidate(
                    candidate_index=index,
                    identifier=reconciliation_key,
                    source_container_id=(
                        source_id
                        + ":spreadsheet:"
                        + occurrence["sheet_name"]
                        + ":"
                        + occurrence["column_ref"]
                    ),
                    body_text=body,
                    parse_success=occurrence["required_fields_complete"],
                    parse_reasons=[
                        "required_field_missing:" + name
                        for name in occurrence["missing_required_fields"]
                    ],
                    identity_evidence={
                        "status": "PROVISIONAL",
                        "strategy": occurrence["identity_strategy"],
                        "resolution_order": self.config["identity"][
                            "resolution_order"
                        ],
                        "identity_schema_version": self.config["identity"][
                            "schema_version"
                        ],
                        "identity_fields": self.config["record_layout"][
                            "identity_fields"
                        ],
                        "canonical_field_fingerprint": fingerprint,
                    },
                    logical_item_id=logical_id,
                )
            )
        return candidates

    def _build_items(
        self,
        source_id: str,
        workbook_digest: str,
        candidates: Sequence[ItemCandidate],
    ) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for candidate in candidates:
            candidate.item_artifacts = [
                {
                    "role": "SOURCE_EVIDENCE",
                    "artifact_kind": ContainerKind.SPREADSHEET.value,
                    "stable_locator": candidate.source_container_id,
                    "content_sha256": workbook_digest,
                    "version_relevant": False,
                    "container_id": candidate.source_container_id,
                }
            ]
            body_digest = body_fingerprint(candidate.body_text)
            artifact_digest = artifact_set_fingerprint(candidate.item_artifacts)
            relevant_digest = artifact_set_fingerprint(
                candidate.item_artifacts, version_relevant_only=True
            )
            version_digest = version_fingerprint(body_digest, relevant_digest)
            derived_id = derived_item_id(candidate.logical_item_id, version_digest)
            title = candidate.body_text.splitlines()[-1]
            subject = self.config["canonical_context"]["subject_template"].format(
                title=title,
                logical_short=candidate.logical_item_id.removeprefix("li_")[:10],
                version_short=version_digest.removeprefix("sha256:")[:12],
            )
            items.append(
                {
                    "original_message_id": source_id,
                    "logical_item_id": candidate.logical_item_id,
                    "derived_item_id": derived_id,
                    "item_index": candidate.candidate_index,
                    "item_type": self.config["item_type"],
                    "body_text": candidate.body_text,
                    "body_fingerprint": body_digest,
                    "artifact_set_fingerprint": artifact_digest,
                    "version_relevant_artifact_set_fingerprint": relevant_digest,
                    "version_fingerprint": version_digest,
                    "content_fingerprint": version_digest,
                    "canonical_subject": subject,
                    "attachments": [],
                    "html_links": [],
                    "item_artifacts": candidate.item_artifacts,
                    "identity_evidence": candidate.identity_evidence,
                }
            )
        return items

    def _parse(self, mail: Dict[str, Any]) -> SpreadsheetParseResult:
        attachment_position = self._attachment_position(mail)
        attachment = mail["attachments"][attachment_position]
        payload = _strict_base64url_decode(attachment.get("data"))
        declared_size = attachment.get("size")
        if (
            isinstance(declared_size, bool)
            or not isinstance(declared_size, int)
            or declared_size != len(payload)
        ):
            raise SpreadsheetInputError("xlsx_attachment_size_mismatch")
        workbook_digest = "sha256:" + hashlib.sha256(payload).hexdigest()
        acquisition = self._source_acquisition(mail, attachment_position)
        parts, member_rows, relationships = self._read_package(payload)
        sheets = self._ordered_sheets(parts, relationships)
        occurrences, enumeration_reasons, unsupported_reasons = (
            self._enumerate_records(parts, sheets)
        )
        proof = self._structure_proof(
            mail, workbook_digest, sheets, occurrences
        )
        canonical, reconciliation, reconciliation_reasons = self._reconcile(
            occurrences
        )
        source_id = str(mail.get("message_id", ""))
        candidates = self._candidate_rows(source_id, canonical)
        required_incomplete = any(not candidate.parse_success for candidate in candidates)
        unknown_sheet = any(sheet["role"] == "UNKNOWN" for sheet in sheets)
        proof_incomplete = proof["status"] == "INCOMPLETE"
        technical_complete = not any(
            (
                enumeration_reasons,
                unsupported_reasons,
                reconciliation_reasons,
                required_incomplete,
                unknown_sheet,
                proof_incomplete,
            )
        )
        spreadsheet_id = source_id + ":spreadsheet"
        list_id = source_id + ":attachments"
        containers = [
            Container(
                container_id=list_id,
                parent_container_id="",
                kind=ContainerKind.ATTACHMENT_LIST.value,
                locator="attachments",
                content_fingerprint=_digest_json(
                    {"source_id": source_id, "position": attachment_position}
                ),
                enumeration_status=EnumerationStatus.COMPLETE.value,
                completeness=acquisition["attachment_integrity_status"] == "PASS",
                candidate_count=1,
                child_container_refs=[spreadsheet_id],
                required=True,
            ),
            Container(
                container_id=spreadsheet_id,
                parent_container_id=list_id,
                kind=ContainerKind.SPREADSHEET.value,
                locator="attachment:" + str(attachment_position),
                content_fingerprint=workbook_digest,
                enumeration_status=(
                    EnumerationStatus.COMPLETE.value
                    if technical_complete
                    else EnumerationStatus.INCOMPLETE.value
                ),
                completeness=technical_complete,
                candidate_count=len(candidates),
                reasons=list(
                    dict.fromkeys(
                        enumeration_reasons
                        + unsupported_reasons
                        + reconciliation_reasons
                        + proof.get("reasons", [])
                    )
                ),
                required=True,
            ),
        ]
        declared = mail.get(DECLARED_ITEM_EVIDENCE_FIELD)
        cross_check_authorities = [CardinalityAuthority.STRUCTURAL_COMPLETE.value]
        evidence = [
            CardinalityEvidence(
                authority=CardinalityAuthority.CONTAINER_ENUMERATION.value,
                source="authoritative_record_column_enumeration",
                count=len(candidates),
                complete=technical_complete,
                is_primary=True,
                reasons=[] if technical_complete else ["workbook_enumeration_incomplete"],
            ),
            CardinalityEvidence(
                authority=CardinalityAuthority.STRUCTURAL_COMPLETE.value,
                source="worksheet_boundary_and_required_field_validation",
                count=len(candidates) if technical_complete else None,
                complete=technical_complete,
                reasons=[] if technical_complete else ["structural_completeness_not_proven"],
            ),
        ]
        declared_reasons: List[str] = []
        if isinstance(declared, dict):
            declared_count = declared.get("count")
            declared_complete = (
                declared.get("authority") == "DECLARED_COUNT"
                and declared.get("complete") is True
                and isinstance(declared_count, int)
                and not isinstance(declared_count, bool)
                and declared_count >= 0
            )
            if not declared_complete:
                declared_reasons.append("declared_count_evidence_invalid")
            cross_check_authorities.append(CardinalityAuthority.DECLARED_COUNT.value)
            evidence.append(
                CardinalityEvidence(
                    authority=CardinalityAuthority.DECLARED_COUNT.value,
                    source=str(declared.get("source", "source_declared_subset")),
                    count=declared_count if declared_complete else None,
                    complete=declared_complete,
                    reasons=declared_reasons,
                )
            )
        source = Source(
            source_id=source_id,
            source_type="EMAIL_ATTACHMENT",
            source_company=self.config["source_company"],
            source_fingerprint=_digest_json(
                {
                    "message_id": source_id,
                    "from": mail.get("from", ""),
                    "subject": mail.get("subject", ""),
                    "workbook_sha256": workbook_digest,
                }
            ),
            delivery_semantics=self.config.get(
                "delivery_semantics", DeliverySemantics.UNKNOWN.value
            ),
            acquisition_status=(
                "COMPLETE"
                if acquisition["status"] == "VERIFIED_COMPLETE"
                else "INCOMPLETE"
            ),
            cardinality_evidence=evidence,
            container_references=[list_id, spreadsheet_id],
            configured_primary_authority=CardinalityAuthority.CONTAINER_ENUMERATION.value,
            configured_cross_check_authorities=cross_check_authorities,
        )
        completeness = evaluate_completeness(source, containers, candidates)
        technical_items = self._build_items(
            source_id, workbook_digest, candidates
        )
        all_reasons = list(
            dict.fromkeys(
                acquisition.get("reasons", [])
                + enumeration_reasons
                + unsupported_reasons
                + reconciliation_reasons
                + proof.get("reasons", [])
                + declared_reasons
                + completeness.reasons
            )
        )
        if unsupported_reasons:
            status = "UNSUPPORTED"
        elif unknown_sheet or reconciliation_reasons:
            status = "HUMAN_REVIEW"
        elif completeness.status == "PARSED":
            status = "PARSED"
        else:
            status = "PARTIAL"
        eligible = len(technical_items) if status == "PARSED" else 0
        source_row = source.to_dict()
        source_row.update(
            {
                "source_acquisition_status": acquisition["status"],
                "source_attachment_validation": acquisition,
                "source_atomic_status": status,
                "container_enumeration_status": (
                    "COMPLETE" if technical_complete else "INCOMPLETE"
                ),
                "auto_union_eligible": status == "PARSED",
                "eligible_item_candidate_count": eligible,
                "observed_candidate_count": len(technical_items),
                "identity_status": "PROVISIONAL",
                "reasons": all_reasons,
            }
        )
        role_counts = Counter(sheet["role"] for sheet in sheets)
        workbook = {
            "format": "XLSX",
            "parser_id": PARSER_ID,
            "parser_version": PARSER_VERSION,
            "config_id": self.config["config_id"],
            "config_version": self.config["config_version"],
            "workbook_sha256": workbook_digest,
            "package_integrity_status": "PASS",
            "package_member_count": len(member_rows),
            "package_members": member_rows,
            "workbook_relationship_status": "PASS",
            "ordered_sheet_enumeration_status": "COMPLETE",
            "sheet_role_classification_status": (
                "HUMAN_REVIEW" if unknown_sheet else "PASS"
            ),
            "sheet_role_counts": {
                role: role_counts[role]
                for role in (
                    "AUTHORITATIVE",
                    "DERIVED_VIEW",
                    "SUPPORTING",
                    "UNKNOWN",
                )
            },
            "record_enumeration_status": (
                "COMPLETE" if not enumeration_reasons else "INCOMPLETE"
            ),
            "technical_workbook_status": (
                "COMPLETE" if technical_complete else "INCOMPLETE"
            ),
            "source_owned_structure_proof": proof,
            "reconciliation": reconciliation,
            "formula_evaluation_count": 0,
            "external_resolution_count": 0,
            "macro_execution_count": 0,
            "reasons": all_reasons,
        }
        return SpreadsheetParseResult(
            status=status,
            reasons=all_reasons,
            eligible_item_candidate_count=eligible,
            source=source_row,
            workbook=workbook,
            sheets=sheets,
            record_occurrences=occurrences,
            items=technical_items if status == "PARSED" else [],
            technical_items=technical_items,
            containers=[container.to_dict() for container in containers],
        )

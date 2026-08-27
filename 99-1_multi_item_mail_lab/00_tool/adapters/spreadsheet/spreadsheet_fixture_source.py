#!/usr/bin/env python3
"""Independent source-owned producer for stable P7 XLSX fixtures."""

import base64
import copy
import hashlib
import io
import json
import re
import unicodedata
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple
from xml.etree import ElementTree as ET

from attachment_fixture_source import build_source_owned_fixture
from attachment_manifest_contract import (
    canonical_ordered_entries,
    ordered_attachment_digest,
    source_payload_digest,
)


STRUCTURE_MANIFEST_FIELD = "spreadsheet_structure_manifest"
DECLARED_ITEM_EVIDENCE_FIELD = "spreadsheet_declared_item_evidence"
STRUCTURE_MANIFEST_VERSION = "spreadsheet_structure_manifest.v1"
FIXED_TIMESTAMP = (2020, 1, 1, 0, 0, 0)
MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


def _normalize(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"\s+", " ", text).strip().casefold()


def _fingerprint(fields: Dict[str, Any], identity_fields: Sequence[str]) -> str:
    payload = [(name, _normalize(fields.get(name, ""))) for name in identity_fields]
    encoded = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _column_name(index: int) -> str:
    if index < 1:
        raise ValueError("column index must be positive")
    value = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        value = chr(65 + remainder) + value
    return value


def _xml_bytes(root: ET.Element) -> bytes:
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _worksheet_bytes(
    field_rows: Dict[str, int],
    records: Sequence[Dict[str, Any]],
    formulas: Dict[Tuple[int, str], str] = None,
) -> bytes:
    formulas = formulas or {}
    worksheet = ET.Element("{%s}worksheet" % MAIN_NS)
    max_row = max(field_rows.values()) if field_rows else 1
    max_column = max(1, len(records) + 1)
    ET.SubElement(
        worksheet,
        "{%s}dimension" % MAIN_NS,
        {"ref": "A1:" + _column_name(max_column) + str(max_row)},
    )
    sheet_data = ET.SubElement(worksheet, "{%s}sheetData" % MAIN_NS)
    fields_by_row = {row: field for field, row in field_rows.items()}
    for row_number in range(1, max_row + 1):
        row = ET.SubElement(
            sheet_data, "{%s}row" % MAIN_NS, {"r": str(row_number)}
        )
        field_name = fields_by_row.get(row_number, "")
        values = [field_name] + [record.get(field_name, "") for record in records]
        for column_index, value in enumerate(values, 1):
            if value is None or str(value) == "":
                continue
            reference = _column_name(column_index) + str(row_number)
            cell = ET.SubElement(
                row, "{%s}c" % MAIN_NS, {"r": reference, "t": "inlineStr"}
            )
            formula = formulas.get((column_index, field_name))
            if formula is not None:
                ET.SubElement(cell, "{%s}f" % MAIN_NS).text = formula
                ET.SubElement(cell, "{%s}v" % MAIN_NS).text = str(value)
            else:
                inline = ET.SubElement(cell, "{%s}is" % MAIN_NS)
                ET.SubElement(inline, "{%s}t" % MAIN_NS).text = str(value)
    return _xml_bytes(worksheet)


def _supporting_worksheet_bytes() -> bytes:
    return _worksheet_bytes(
        {"氏名": 1, "メールアドレス": 2},
        [{"氏名": "担当者A", "メールアドレス": "redacted@example.invalid"}],
    )


def build_workbook_bytes(
    sheet_definitions: Sequence[Dict[str, Any]], config: Dict[str, Any]
) -> Tuple[bytes, List[Dict[str, Any]]]:
    """Create a deterministic XLSX after freezing independent sheet authority."""
    authority = copy.deepcopy(list(sheet_definitions))
    field_rows = config["record_layout"]["field_rows"]
    identity_fields = config["record_layout"]["identity_fields"]

    workbook = ET.Element("{%s}workbook" % MAIN_NS)
    sheets = ET.SubElement(workbook, "{%s}sheets" % MAIN_NS)
    workbook_rels = ET.Element("{%s}Relationships" % PACKAGE_REL_NS)
    expected_sheets: List[Dict[str, Any]] = []
    worksheet_parts: List[Tuple[str, bytes]] = []
    for position, definition in enumerate(authority, 1):
        relationship_id = "rId" + str(position)
        target = "worksheets/sheet" + str(position) + ".xml"
        state = definition.get("state", "visible")
        sheet_attributes = {
            "name": definition["name"],
            "sheetId": str(position),
            "{%s}id" % REL_NS: relationship_id,
        }
        if state != "visible":
            sheet_attributes["state"] = state
        ET.SubElement(sheets, "{%s}sheet" % MAIN_NS, sheet_attributes)
        ET.SubElement(
            workbook_rels,
            "{%s}Relationship" % PACKAGE_REL_NS,
            {
                "Id": relationship_id,
                "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet",
                "Target": target,
            },
        )
        records = definition.get("records", [])
        role = definition["role"]
        worksheet_payload = (
            _supporting_worksheet_bytes()
            if role == "SUPPORTING"
            else _worksheet_bytes(field_rows, records, definition.get("formulas"))
        )
        worksheet_parts.append(("xl/" + target, worksheet_payload))
        expected_records = [
            {
                "column_index": index + 2,
                "column_ref": _column_name(index + 2),
                "canonical_fingerprint": _fingerprint(record, identity_fields),
            }
            for index, record in enumerate(records)
        ]
        expected_sheets.append(
            {
                "position": position - 1,
                "name": definition["name"],
                "state": state,
                "relationship_id": relationship_id,
                "target": "xl/" + target,
                "role": role,
                "expected_records": expected_records,
            }
        )

    root_rels = ET.Element("{%s}Relationships" % PACKAGE_REL_NS)
    ET.SubElement(
        root_rels,
        "{%s}Relationship" % PACKAGE_REL_NS,
        {
            "Id": "rId1",
            "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument",
            "Target": "xl/workbook.xml",
        },
    )
    content_types = ET.Element(
        "{http://schemas.openxmlformats.org/package/2006/content-types}Types"
    )
    ET.SubElement(
        content_types,
        "{http://schemas.openxmlformats.org/package/2006/content-types}Default",
        {"Extension": "rels", "ContentType": "application/vnd.openxmlformats-package.relationships+xml"},
    )
    ET.SubElement(
        content_types,
        "{http://schemas.openxmlformats.org/package/2006/content-types}Default",
        {"Extension": "xml", "ContentType": "application/xml"},
    )
    ET.SubElement(
        content_types,
        "{http://schemas.openxmlformats.org/package/2006/content-types}Override",
        {
            "PartName": "/xl/workbook.xml",
            "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml",
        },
    )
    for part_name, _ in worksheet_parts:
        ET.SubElement(
            content_types,
            "{http://schemas.openxmlformats.org/package/2006/content-types}Override",
            {
                "PartName": "/" + part_name,
                "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml",
            },
        )

    package_parts = [
        ("[Content_Types].xml", _xml_bytes(content_types)),
        ("_rels/.rels", _xml_bytes(root_rels)),
        ("xl/workbook.xml", _xml_bytes(workbook)),
        ("xl/_rels/workbook.xml.rels", _xml_bytes(workbook_rels)),
    ] + worksheet_parts
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", allowZip64=False) as package:
        for name, payload in package_parts:
            info = zipfile.ZipInfo(name, FIXED_TIMESTAMP)
            info.compress_type = zipfile.ZIP_DEFLATED
            package.writestr(info, payload)
    return output.getvalue(), expected_sheets


def _attachment(filename: str, payload: bytes) -> Dict[str, Any]:
    return {
        "source_entry_id": "part:spreadsheet:0",
        "filename": filename,
        "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "size": len(payload),
        "data": base64.urlsafe_b64encode(payload).decode("ascii").rstrip("="),
    }


def build_spreadsheet_fixture(
    definition: Dict[str, Any], config: Dict[str, Any]
) -> Dict[str, Any]:
    projects = copy.deepcopy(definition.get("projects", []))
    if definition.get("count") != len(projects):
        raise ValueError("fixture count must match source-owned project definitions")
    configured_sheets = definition.get("sheet_definitions")
    if configured_sheets is not None:
        sheet_definitions = copy.deepcopy(configured_sheets)
    else:
        sheet_definitions = [
            {"name": "案件一覧", "role": "AUTHORITATIVE", "records": projects}
        ]
        derived_indexes = definition.get("derived_indexes", [])
        if derived_indexes:
            sheet_definitions.append(
                {
                    "name": "新着案件",
                    "role": "DERIVED_VIEW",
                    "records": [projects[index] for index in derived_indexes],
                }
            )
        if definition.get("supporting"):
            sheet_definitions.append(
                {"name": "連絡先", "role": "SUPPORTING", "records": []}
            )
    payload, expected_sheets = build_workbook_bytes(sheet_definitions, config)
    source_definition = {
        "message_id": definition["message_id"],
        "thread_id": "synthetic-sakya-thread",
        "date": "Thu, 27 Aug 2026 00:00:00 +0000",
        "from": "Synthetic SAKYA <lab@sakya.jp>",
        "to": ["test@example.invalid"],
        "cc": "",
        "reply_to": "",
        "subject": "サクヤ 営業中案件一覧 stable " + str(len(projects)),
        "body_text": "stable redacted spreadsheet contract",
        "html_links": [],
        "authoritative_attachments": [
            _attachment("サクヤ営業中_案件000000.xlsx", payload)
        ],
    }
    fixture = build_source_owned_fixture(source_definition)
    identity_fields = config["record_layout"]["identity_fields"]
    fixture[STRUCTURE_MANIFEST_FIELD] = {
        "manifest_schema_version": STRUCTURE_MANIFEST_VERSION,
        "workbook_sha256": "sha256:" + hashlib.sha256(payload).hexdigest(),
        "expected_ordered_sheet_count": len(expected_sheets),
        "expected_ordered_sheets": expected_sheets,
        "expected_canonical_fingerprints": [
            _fingerprint(project, identity_fields) for project in projects
        ],
    }
    fixture[DECLARED_ITEM_EVIDENCE_FIELD] = {
        "authority": "DECLARED_COUNT",
        "count": len(projects),
        "complete": True,
        "source": "fixture_source_definition",
    }
    return fixture


def build_fixture_records(path: Path, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    records = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return [build_spreadsheet_fixture(record, config) for record in records]


def workbook_payload(fixture: Dict[str, Any]) -> bytes:
    encoded = fixture["attachments"][0]["data"].encode("ascii")
    return base64.urlsafe_b64decode(encoded + b"=" * (-len(encoded) % 4))


def rewrite_package_member(payload: bytes, name: str, value: bytes) -> bytes:
    source = zipfile.ZipFile(io.BytesIO(payload), "r")
    parts = [(info.filename, source.read(info)) for info in source.infolist()]
    source.close()
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", allowZip64=False) as package:
        replaced = False
        for part_name, part_value in parts:
            info = zipfile.ZipInfo(part_name, FIXED_TIMESTAMP)
            info.compress_type = zipfile.ZIP_DEFLATED
            package.writestr(info, value if part_name == name else part_value)
            replaced = replaced or part_name == name
    if not replaced:
        raise KeyError("package member missing:" + name)
    return output.getvalue()


def replace_workbook_payload(
    fixture: Dict[str, Any], payload: bytes, update_structure_digest: bool = True
) -> Dict[str, Any]:
    result = copy.deepcopy(fixture)
    attachment = result["attachments"][0]
    digest = "sha256:" + hashlib.sha256(payload).hexdigest()
    attachment["data"] = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    attachment["size"] = len(payload)
    manifest = result["attachment_acquisition_manifest"]
    entry = manifest["authoritative_attachment_entries"][0]
    entry["declared_size"] = len(payload)
    entry["content_digest"] = digest
    canonical = canonical_ordered_entries(manifest["authoritative_attachment_entries"])
    manifest["expected_ordered_digest"] = ordered_attachment_digest(canonical)
    manifest["source_payload_digest"] = source_payload_digest(result, canonical)
    if update_structure_digest:
        result[STRUCTURE_MANIFEST_FIELD]["workbook_sha256"] = digest
    return result

#!/usr/bin/env python3
"""Acquire one public Google Sheet as one isolated XLSX snapshot."""

import io
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from xml.etree import ElementTree as ET

from openpyxl import load_workbook


sys.dont_write_bytecode = True
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
ACQUISITION_DIR = STEP_DIR / "00_tool" / "acquisition"
for import_path in (PROJECT_ROOT, ACQUISITION_DIR):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.file_utils import ensure_dir, write_execution_time
from common.json_utils import read_jsonl, write_jsonl
from common.logger import get_logger
from google_sheet_acquisition_contract import (
    ATTEMPT_PLAN_VERSION,
    MANIFEST_VERSION,
    SNAPSHOT_ENTRY_VERSION,
    bind_profile_digest,
    calculate_ordered_snapshot_set_digest,
    calculate_planned_container_set_digest,
    calculate_resolved_scope_digest,
    digest_bytes,
    digest_json,
    finalize_manifest,
    validate_manifest,
    validate_profile_registry,
)


logger = get_logger("99-1_google_sheet_acquisition_prototype")
PROFILE_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "acquisition"
    / "google_sheets_public_xlsx.v1.json.example"
)
DEFAULT_GMAIL_PATH = (
    PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)
RESULT_SUBDIR = "google_sheet_acquisition_prototype"
RESULT_DIR = STEP_DIR / "01_result" / RESULT_SUBDIR
SNAPSHOT_RELATIVE_PATH = "snapshot/google_sheet.xlsx"
MAX_DOWNLOAD_BYTES = 50 * 1024 * 1024
MAX_EXPANDED_BYTES = 250 * 1024 * 1024
MAX_PACKAGE_MEMBERS = 10000
USER_AGENT = "Mozilla/5.0 (compatible; 99-1-Google-Sheet-Acquisition-Prototype/1.0)"
GOOGLE_SHEET_PATH = re.compile(r"\A/spreadsheets/d/([A-Za-z0-9_-]+)(?:/|\Z)")
XLSX_MEDIA_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)


class AcquisitionStop(Exception):
    def __init__(self, status: str, reason: str):
        super().__init__(reason)
        self.status = status
        self.reason = reason


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_profile_registry() -> Dict[str, Any]:
    registry = json.loads(PROFILE_PATH.read_text(encoding="utf-8"))
    if registry.get("profiles", [{}])[0].get("profile_digest") == "PROFILE_DIGEST_PENDING":
        registry = bind_profile_digest(registry)
    reasons = validate_profile_registry(registry)
    if reasons:
        raise ValueError("profile registry invalid:" + ",".join(reasons))
    return registry


def _normalized_sheet_locator(href: str) -> Optional[Dict[str, str]]:
    try:
        parsed = urllib.parse.urlsplit(href)
    except ValueError:
        return None
    if parsed.scheme != "https" or parsed.hostname != "docs.google.com":
        return None
    match = GOOGLE_SHEET_PATH.match(parsed.path)
    if match is None:
        return None
    spreadsheet_key = match.group(1)
    query = urllib.parse.parse_qs(parsed.query)
    fragment = urllib.parse.parse_qs(parsed.fragment)
    gid_values = fragment.get("gid") or query.get("gid") or []
    gid = gid_values[0] if gid_values and gid_values[0].isdigit() else "UNKNOWN"
    normalized = "https://docs.google.com/spreadsheets/d/" + spreadsheet_key + "/edit"
    if gid != "UNKNOWN":
        normalized += "#gid=" + gid
    return {
        "spreadsheet_key": spreadsheet_key,
        "gid": gid,
        "normalized_locator": normalized,
    }


def select_representative_locator(
    gmail_path: Path, profile: Dict[str, Any]
) -> Dict[str, Any]:
    """Resolve exactly one common-list locator without persisting mail contents."""
    selector = profile["locator_selector"]
    sender_domain = selector["sender_domain"].casefold()
    text_marker = selector["link_text_contains"].casefold()
    evidence: Dict[str, Dict[str, Any]] = {}
    for record in read_jsonl(str(gmail_path)):
        sender = str(record.get("from", "")).casefold()
        if sender_domain not in sender:
            continue
        for link in record.get("html_links", []):
            if not isinstance(link, dict):
                continue
            if text_marker not in str(link.get("text", "")).casefold():
                continue
            locator = _normalized_sheet_locator(str(link.get("href", "")))
            if locator is None:
                continue
            normalized = locator["normalized_locator"]
            current = evidence.setdefault(
                normalized,
                {
                    **locator,
                    "evidence_message_ids": [],
                    "link_text": str(link.get("text", "")),
                },
            )
            message_id = record.get("message_id")
            if isinstance(message_id, str) and message_id:
                current["evidence_message_ids"].append(message_id)
    if len(evidence) != 1:
        raise ValueError(
            "representative locator must resolve to exactly one common-list Sheet; got "
            + str(len(evidence))
        )
    selected = next(iter(evidence.values()))
    selected["evidence_message_ids"] = sorted(set(selected["evidence_message_ids"]))
    selected["evidence_count"] = len(selected["evidence_message_ids"])
    return selected


def build_attempt_plan(
    registry: Dict[str, Any], selected: Dict[str, Any], started_at: str
) -> Dict[str, Any]:
    profile = registry["profiles"][0]
    locator_ref = digest_json(
        {
            "provider": "GOOGLE_SHEETS",
            "normalized_locator": selected["normalized_locator"],
        }
    )
    locator_binding_id = digest_json(
        {
            "locator_ref": locator_ref,
            "evidence_message_ids": selected["evidence_message_ids"],
        }
    )
    resolved_scope = {
        **profile["resolved_scope_template"],
        "spreadsheet_key": selected["spreadsheet_key"],
        "representative_gid": selected["gid"],
        "normalized_locator": selected["normalized_locator"],
    }
    profile_ref = {
        "profile_id": profile["profile_id"],
        "profile_version": profile["profile_version"],
        "profile_digest": profile["profile_digest"],
    }
    planned_container = {
        "planned_container_id": "planned-google-sheet-"
        + digest_json(
            {
                "profile_ref": profile_ref,
                "locator_binding_id": locator_binding_id,
            }
        ).split(":", 1)[1][:24],
        "container_kind": profile["planned_container_template"]["container_kind"],
        "required": True,
        "logical_role": profile["planned_container_template"]["logical_role"],
        "locator_ref": locator_ref,
        "locator_binding_id": locator_binding_id,
        **profile_ref,
    }
    planned_containers = [planned_container]
    return {
        "schema_version": ATTEMPT_PLAN_VERSION,
        "attempt_id": "google-sheet-acquisition-" + locator_binding_id.split(":", 1)[1][:24],
        "attempt_ordinal": 1,
        "attempt_started_at": started_at,
        "provider": "GOOGLE_SHEETS",
        "acquisition_method": profile["acquisition_method"],
        "profile_ref": profile_ref,
        "resolved_planned_scope": resolved_scope,
        "resolved_scope_digest": calculate_resolved_scope_digest(resolved_scope),
        "planned_containers": planned_containers,
        "planned_container_set_digest": calculate_planned_container_set_digest(
            planned_containers
        ),
        "candidate_emission": 0,
        "auto_union": False,
        "production_integration": False,
        "plan_state": "FROZEN_BEFORE_ACQUISITION",
    }


def _request(
    url: str, method: str = "GET", read_limit: Optional[int] = None
) -> Tuple[bytes, Dict[str, str], str]:
    request = urllib.request.Request(
        url,
        method=method,
        headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=30,
            context=ssl.create_default_context(),
        ) as response:
            final_host = urllib.parse.urlsplit(response.geturl()).hostname or ""
            payload = b"" if method == "HEAD" else response.read(read_limit)
            headers = {
                name.lower(): value
                for name, value in response.headers.items()
                if name.lower() in {"content-type", "etag", "last-modified", "x-goog-generation"}
            }
            return payload, headers, final_host
    except urllib.error.HTTPError as error:
        if error.code in {401, 403}:
            raise AcquisitionStop("AUTH_REQUIRED", "provider returned authentication status") from error
        raise AcquisitionStop("FAILED", "provider HTTP status:" + str(error.code)) from error
    except (urllib.error.URLError, TimeoutError, ssl.SSLError) as error:
        raise AcquisitionStop("FAILED", "provider access failed:" + type(error).__name__) from error


def _check_public_shared_url(shared_url: str) -> Dict[str, str]:
    payload, headers, final_host = _request(shared_url, read_limit=256 * 1024)
    lowered = payload.lower()
    if final_host == "accounts.google.com" or b"accounts.google.com/signin" in lowered:
        raise AcquisitionStop("AUTH_REQUIRED", "shared URL requires Google login")
    if b"request access" in lowered or b"you need access" in lowered:
        raise AcquisitionStop("AUTH_REQUIRED", "shared URL requires access grant")
    if final_host not in {"docs.google.com", "drive.google.com"}:
        raise AcquisitionStop("FAILED", "unexpected shared URL response host")
    return headers


def _version_observation(headers: Dict[str, str]) -> Dict[str, Any]:
    if headers.get("x-goog-generation"):
        return {
            "version_kind": "HTTP_X_GOOG_GENERATION",
            "version_value": headers["x-goog-generation"],
            "scope": "EXPORT_RESPONSE",
            "strength": "WEAK",
        }
    if headers.get("etag"):
        return {
            "version_kind": "HTTP_ETAG",
            "version_value": headers["etag"],
            "scope": "EXPORT_RESPONSE",
            "strength": "WEAK",
        }
    if headers.get("last-modified"):
        return {
            "version_kind": "HTTP_LAST_MODIFIED",
            "version_value": headers["last-modified"],
            "scope": "EXPORT_RESPONSE",
            "strength": "WEAK",
        }
    return {
        "version_kind": "UNAVAILABLE",
        "version_value": "UNAVAILABLE",
        "scope": "UNKNOWN",
        "strength": "UNKNOWN",
    }


def _version_authority(
    before_headers: Dict[str, str], after_headers: Dict[str, str]
) -> Dict[str, Any]:
    before = _version_observation(before_headers)
    after = _version_observation(after_headers)
    comparable = (
        before["version_kind"] != "UNAVAILABLE"
        and before["version_kind"] == after["version_kind"]
    )
    stable: Optional[bool]
    if comparable:
        stable = before["version_value"] == after["version_value"]
    else:
        stable = None
    return {
        "version_kind": before["version_kind"]
        if before["version_kind"] == after["version_kind"]
        else "UNAVAILABLE",
        "scope": before["scope"] if comparable else "UNKNOWN",
        "strength": before["strength"] if comparable else "UNKNOWN",
        "pre": before,
        "post": after,
        "pre_post_stable": stable,
        "authority_decision": "NOT_WORKBOOK_WIDE_STRONG",
    }


def _validate_xlsx_package(payload: bytes) -> None:
    if len(payload) > MAX_DOWNLOAD_BYTES:
        raise AcquisitionStop("FAILED", "XLSX download size limit exceeded")
    try:
        with zipfile.ZipFile(io.BytesIO(payload), "r") as package:
            infos = package.infolist()
            if len(infos) > MAX_PACKAGE_MEMBERS:
                raise AcquisitionStop("FAILED", "XLSX member count limit exceeded")
            expanded = sum(info.file_size for info in infos)
            if expanded > MAX_EXPANDED_BYTES:
                raise AcquisitionStop("FAILED", "XLSX expanded size limit exceeded")
            names = {info.filename for info in infos}
            if "xl/workbook.xml" not in names or "[Content_Types].xml" not in names:
                raise AcquisitionStop("FAILED", "XLSX required package parts missing")
            for info in infos:
                normalized = info.filename.replace("\\", "/")
                if normalized.startswith("/") or ".." in normalized.split("/"):
                    raise AcquisitionStop("FAILED", "XLSX unsafe package path")
    except zipfile.BadZipFile as error:
        raise AcquisitionStop("FAILED", "provider response is not XLSX") from error


def _xlsx_sheet_ids(payload: bytes) -> List[Dict[str, str]]:
    with zipfile.ZipFile(io.BytesIO(payload), "r") as package:
        xml_payload = package.read("xl/workbook.xml")
    lowered = xml_payload.lower()
    if b"<!doctype" in lowered or b"<!entity" in lowered:
        raise AcquisitionStop("FAILED", "XLSX workbook XML is unsafe")
    root = ET.fromstring(xml_payload)
    namespace = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    return [
        {
            "tab_title": element.get("name", ""),
            "tab_id": element.get("sheetId", "UNKNOWN"),
            "tab_id_kind": "XLSX_SHEET_ID",
        }
        for element in root.findall(".//{%s}sheet" % namespace)
    ]


def observe_workbook(payload: bytes) -> Dict[str, Any]:
    """Observe metadata only; formulas are never evaluated and rows emit no candidates."""
    _validate_xlsx_package(payload)
    workbook = load_workbook(
        io.BytesIO(payload), read_only=False, data_only=False, keep_links=False
    )
    values_workbook = load_workbook(
        io.BytesIO(payload), read_only=False, data_only=True, keep_links=False
    )
    sheet_ids = _xlsx_sheet_ids(payload)
    if len(sheet_ids) != len(workbook.worksheets):
        raise AcquisitionStop("FAILED", "XLSX workbook inventory mismatch")
    tabs: List[Dict[str, Any]] = []
    total_formula_count = 0
    total_cached_formula_values = 0
    for position, worksheet in enumerate(workbook.worksheets):
        values_sheet = values_workbook[worksheet.title]
        formula_count = 0
        cached_formula_values = 0
        styled_cell_count = 0
        for row in worksheet.iter_rows():
            for cell in row:
                if cell.has_style:
                    styled_cell_count += 1
                if cell.data_type == "f":
                    formula_count += 1
                    if values_sheet[cell.coordinate].value is not None:
                        cached_formula_values += 1
        total_formula_count += formula_count
        total_cached_formula_values += cached_formula_values
        hidden_rows = sum(1 for value in worksheet.row_dimensions.values() if value.hidden)
        hidden_columns = sum(
            1 for value in worksheet.column_dimensions.values() if value.hidden
        )
        dimension = worksheet.calculate_dimension()
        tabs.append(
            {
                "tab_order": position,
                **sheet_ids[position],
                "visibility": worksheet.sheet_state.upper(),
                "row_bound": worksheet.max_row,
                "column_bound": worksheet.max_column,
                "requested_range": "ENTIRE_EXPORTED_TAB_USED_BOUNDS",
                "returned_range": dimension,
                "formula_count": formula_count,
                "cached_formula_value_count": cached_formula_values,
                "styled_cell_count": styled_cell_count,
                "conditional_formatting_rule_set_count": len(
                    worksheet.conditional_formatting
                ),
                "hidden_row_count": hidden_rows,
                "hidden_column_count": hidden_columns,
            }
        )
    display_availability = (
        "AVAILABLE"
        if total_formula_count == total_cached_formula_values
        else "PARTIAL"
    )
    presentation = {
        "policy": "UNRESOLVED",
        "overall_availability": "PARTIAL",
        "effective_format": "UNAVAILABLE",
        "static_user_format": "AVAILABLE",
        "conditional_formatting": "AVAILABLE",
        "hidden_rows_columns": "AVAILABLE",
        "tab_visibility": "AVAILABLE",
        "formula_metadata": "AVAILABLE",
        "display_values": display_availability,
        "business_state_mapping": "NOT_IMPLEMENTED",
    }
    return {
        "workbook_tab_inventory": "AVAILABLE",
        "tab_id_authority": "XLSX_SHEET_ID_AVAILABLE_GOOGLE_GID_INVENTORY_UNAVAILABLE",
        "tab_count": len(tabs),
        "tabs": tabs,
        "range_bounds": "AVAILABLE",
        "formula_metadata_availability": "AVAILABLE",
        "presentation_metadata": presentation,
        "technical_record_count": sum(
            max(worksheet.max_row, 0) for worksheet in workbook.worksheets
        ),
    }


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    ensure_dir(str(path.parent))
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        with open(temporary, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(str(temporary), str(path))
    except Exception:
        if temporary.exists():
            temporary.unlink()
        raise


def _base_manifest(
    registry: Dict[str, Any],
    plan: Dict[str, Any],
    selected: Dict[str, Any],
    completed_at: str,
) -> Dict[str, Any]:
    profile = registry["profiles"][0]
    return {
        "schema_version": MANIFEST_VERSION,
        "manifest_id": "manifest:" + plan["attempt_id"],
        "provider": "GOOGLE_SHEETS",
        "representative_source": {
            "normalized_locator": selected["normalized_locator"],
            "spreadsheet_key": selected["spreadsheet_key"],
            "gid": selected["gid"],
            "list_sheet_evidence": selected["link_text"],
            "saved_gmail_evidence_count": selected["evidence_count"],
        },
        "acquisition_method": profile["acquisition_method"],
        "attempt_id": plan["attempt_id"],
        "profile_ref": plan["profile_ref"],
        "resolved_scope_digest": plan["resolved_scope_digest"],
        "planned_container_set_digest": plan["planned_container_set_digest"],
        "snapshot_entries": [],
        "ordered_snapshot_set_digest": calculate_ordered_snapshot_set_digest([]),
        "presentation_policy": profile["presentation_policy"],
        "version_authority": {
            "version_kind": "UNAVAILABLE",
            "scope": "UNKNOWN",
            "strength": "UNKNOWN",
            "pre_post_stable": None,
        },
        "completeness_evidence": {
            "resolved_scope_digest": plan["resolved_scope_digest"],
            "tab_inventory_complete": False,
            "range_complete": False,
            "required_container_count": 1,
            "captured_required_container_count": 0,
        },
        "observation": {},
        "access_status": "FAILED",
        "acquisition_status": "OTHER",
        "review_status": "NONE",
        "attempt_state": "FAILED",
        "candidate_emission": 0,
        "eligible": 0,
        "auto_union": False,
        "production_write": 0,
        "acquisition_completed_at": completed_at,
    }


def _write_outputs(
    registry: Dict[str, Any],
    plan: Dict[str, Any],
    manifest: Dict[str, Any],
    validation: Dict[str, Any],
) -> None:
    ensure_dir(str(RESULT_DIR))
    write_jsonl(str(RESULT_DIR / "profile_registry.jsonl"), [registry])
    write_jsonl(str(RESULT_DIR / "attempt_plan.jsonl"), [plan])
    write_jsonl(
        str(RESULT_DIR / "snapshot_entries.jsonl"), manifest["snapshot_entries"]
    )
    write_jsonl(str(RESULT_DIR / "acquisition_manifest.jsonl"), [manifest])
    write_jsonl(str(RESULT_DIR / "google_metadata_observation.jsonl"), [manifest["observation"]])
    summary = {
        "implementation": "PASS"
        if manifest["acquisition_status"]
        in {
            "VERIFIED_COMPLETE",
            "UNVERIFIED",
            "PARTIAL",
            "AUTH_REQUIRED",
            "SNAPSHOT_UNSTABLE",
            "INCOMPLETE",
        }
        and validation.get("eligible") == 0
        else "FAIL",
        "provider": "GOOGLE_SHEETS",
        "representative_source": manifest["representative_source"],
        "access": manifest["access_status"],
        "acquisition_method": manifest["acquisition_method"],
        "profile": "PASS",
        "attempt_plan": "PASS",
        "planned_container": "PASS",
        "snapshot": "PASS" if manifest["snapshot_entries"] else "N/A",
        "manifest": "PASS",
        "manifest_validation": "PASS" if validation.get("valid") else "FAIL",
        "acquisition_status": manifest["acquisition_status"],
        "attempt_state": manifest["attempt_state"],
        "eligible": 0,
        "auto_union": False,
        "candidate_emission": 0,
        "actual_fixed_oracle": 0,
        "production_write": 0,
        "P8": "NONE",
        "validation_reasons": validation.get("reasons", []),
    }
    write_jsonl(str(RESULT_DIR / "prototype_summary.jsonl"), [summary])


def run(gmail_path: Path = DEFAULT_GMAIL_PATH) -> Dict[str, Any]:
    started = time.monotonic()
    started_at = _utc_now()
    registry = _load_profile_registry()
    profile = registry["profiles"][0]
    selected = select_representative_locator(gmail_path, profile)
    plan = build_attempt_plan(registry, selected, started_at)
    ensure_dir(str(RESULT_DIR))
    # These are persisted before the first provider request and never rewritten.
    write_jsonl(str(RESULT_DIR / "profile_registry.jsonl"), [registry])
    write_jsonl(str(RESULT_DIR / "attempt_plan.jsonl"), [plan])
    manifest = _base_manifest(registry, plan, selected, _utc_now())
    raw_entries: Dict[str, bytes] = {}
    try:
        _check_public_shared_url(selected["normalized_locator"])
        export_url = (
            "https://docs.google.com/spreadsheets/d/"
            + selected["spreadsheet_key"]
            + "/export?format=xlsx"
        )
        _, before_headers, _ = _request(export_url, method="HEAD")
        payload, response_headers, _ = _request(
            export_url, method="GET", read_limit=MAX_DOWNLOAD_BYTES + 1
        )
        _validate_xlsx_package(payload)
        observation = observe_workbook(payload)
        _, after_headers, _ = _request(export_url, method="HEAD")
        version = _version_authority(before_headers, after_headers)
        acquired_at = _utc_now()
        entry_id = "snapshot-entry:" + plan["planned_containers"][0][
            "planned_container_id"
        ]
        entry = {
            "schema_version": SNAPSHOT_ENTRY_VERSION,
            "snapshot_entry_id": entry_id,
            "planned_container_id": plan["planned_containers"][0][
                "planned_container_id"
            ],
            "relative_path": SNAPSHOT_RELATIVE_PATH,
            "media_type": XLSX_MEDIA_TYPE,
            "byte_count": len(payload),
            "entry_raw_digest": digest_bytes(payload),
            "entry_raw_digest_kind": "ENTRY_RAW_DIGEST",
            "acquired_at": acquired_at,
        }
        snapshot_path = RESULT_DIR / SNAPSHOT_RELATIVE_PATH
        _atomic_write_bytes(snapshot_path, payload)
        raw_entries[entry_id] = payload
        manifest.update(
            {
                "snapshot_entries": [entry],
                "ordered_snapshot_set_digest": calculate_ordered_snapshot_set_digest(
                    [entry]
                ),
                "version_authority": version,
                "completeness_evidence": {
                    "resolved_scope_digest": plan["resolved_scope_digest"],
                    "tab_inventory_complete": observation[
                        "workbook_tab_inventory"
                    ]
                    == "AVAILABLE",
                    "range_complete": observation["range_bounds"] == "AVAILABLE",
                    "required_container_count": 1,
                    "captured_required_container_count": 1,
                    "snapshot_media_type_observed": response_headers.get(
                        "content-type", "UNAVAILABLE"
                    ),
                },
                "observation": {
                    "spreadsheet_key": selected["spreadsheet_key"],
                    "gid": selected["gid"],
                    "acquisition_timestamp": acquired_at,
                    **observation,
                },
                "access_status": "SUCCESS",
                "attempt_state": "COMMITTED",
                "acquisition_completed_at": acquired_at,
            }
        )
    except AcquisitionStop as error:
        manifest["access_status"] = error.status
        manifest["observation"] = {
            "spreadsheet_key": selected["spreadsheet_key"],
            "gid": selected["gid"],
            "acquisition_timestamp": _utc_now(),
            "stop_reason": error.reason,
        }

    manifest = finalize_manifest(manifest)
    validation = validate_manifest(manifest, registry, plan, raw_entries)
    manifest["acquisition_status"] = validation["acquisition_status"]
    manifest["review_status"] = validation["review_status"]
    manifest = finalize_manifest(manifest)
    validation = validate_manifest(manifest, registry, plan, raw_entries)
    _write_outputs(registry, plan, manifest, validation)
    elapsed = time.monotonic() - started
    write_execution_time(
        str(STEP_DIR / "99_execution_time"),
        "99-1_google_sheet_acquisition_prototype",
        elapsed,
        1,
    )
    logger.ok(
        "prototype="
        + ("PASS" if validation.get("eligible") == 0 else "FAIL")
        + " access="
        + manifest["access_status"]
        + " acquisition_status="
        + manifest["acquisition_status"]
        + " eligible=0 candidate_emission=0"
    )
    return {
        "registry": registry,
        "plan": plan,
        "manifest": manifest,
        "validation": validation,
        "raw_entries": raw_entries,
    }


def main() -> None:
    result = run()
    manifest = result["manifest"]
    if manifest["acquisition_status"] == "OTHER":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

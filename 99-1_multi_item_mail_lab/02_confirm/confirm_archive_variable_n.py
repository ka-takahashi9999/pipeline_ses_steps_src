#!/usr/bin/env python3
"""Confirm P6 archive replay counts, layers, graph, and fail-closed cases."""

import base64
import copy
import hashlib
import json
import struct
import sys
import zipfile
from pathlib import Path
from typing import Any, Dict, List


sys.dont_write_bytecode = True
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
for import_path in (
    PROJECT_ROOT,
    STEP_DIR / "00_tool" / "adapters" / "archive",
    STEP_DIR / "00_tool" / "adapters" / "attachment_list",
    STEP_DIR / "00_tool" / "core",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger
from archive_fixture_source import build_archive_fixture, build_zip_bytes, member_definition, variable_n_definitions
from archive_parser import ArchiveParser, MEMBER_MANIFEST_FIELD, _parse_structure
from attachment_manifest_contract import canonical_ordered_entries, ordered_attachment_digest, source_payload_digest


logger = get_logger("confirm_99-1_p6_archive")
CONFIG_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "archive"
    / "archive_security.v1.json.example"
)
RESULT_DIR = STEP_DIR / "01_result" / "archive_variable_n"
CONFIRM_PATH = STEP_DIR / "02_confirm" / "archive_variable_n" / "confirm_report.jsonl"


def _check(condition: bool, message: str, failures: List[str]) -> None:
    if not condition:
        failures.append(message)
        logger.error(message)


def _decode(fixture: Dict[str, Any]) -> bytes:
    value = fixture["attachments"][0]["data"]
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _replace_payload(fixture: Dict[str, Any], payload: bytes) -> Dict[str, Any]:
    result = copy.deepcopy(fixture)
    digest = "sha256:" + hashlib.sha256(payload).hexdigest()
    result["attachments"][0]["data"] = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    result["attachments"][0]["size"] = len(payload)
    entry = result["attachment_acquisition_manifest"]["authoritative_attachment_entries"][0]
    entry["declared_size"] = len(payload)
    entry["content_digest"] = digest
    entries = canonical_ordered_entries(result["attachment_acquisition_manifest"]["authoritative_attachment_entries"])
    result["attachment_acquisition_manifest"]["expected_ordered_digest"] = ordered_attachment_digest(entries)
    result["attachment_acquisition_manifest"]["source_payload_digest"] = source_payload_digest(result, entries)
    result[MEMBER_MANIFEST_FIELD]["archive_sha256"] = digest
    return result


def _negative_contracts(parser: ArchiveParser) -> Dict[str, bool]:
    original_definitions = variable_n_definitions(4)
    original = build_archive_fixture(original_definitions, 4, "confirm-middle-authority")
    deleted = build_archive_fixture(
        original_definitions[:2] + original_definitions[3:], 3, "confirm-middle-observed"
    )
    deleted[MEMBER_MANIFEST_FIELD] = copy.deepcopy(original[MEMBER_MANIFEST_FIELD])
    deleted[MEMBER_MANIFEST_FIELD]["archive_sha256"] = "sha256:" + hashlib.sha256(_decode(deleted)).hexdigest()
    deletion_result = parser.parse(deleted)

    traversal_result = parser.parse(
        build_archive_fixture(
            [member_definition("../item-001.xlsx", b"unsafe", "ITEM_CANDIDATE", zipfile.ZIP_STORED)],
            1,
            "confirm-traversal",
        )
    )

    nested_payload, _ = build_zip_bytes(
        [member_definition("supporting/inside.txt", b"inside", "SUPPORTING", zipfile.ZIP_STORED)]
    )
    nested_result = parser.parse(
        build_archive_fixture(
            [member_definition("nested.zip", nested_payload, "NESTED_ARCHIVE", zipfile.ZIP_DEFLATED)],
            0,
            "confirm-nested",
        )
    )

    source_unverified = build_archive_fixture(variable_n_definitions(1), 1, "confirm-source-layer")
    source_unverified.pop("attachment_acquisition_manifest")
    source_layer_result = parser.parse(source_unverified)

    member_invalid = build_archive_fixture(variable_n_definitions(1), 1, "confirm-member-layer")
    member_invalid[MEMBER_MANIFEST_FIELD]["expected_ordered_digest"] = "sha256:" + "0" * 64
    member_layer_result = parser.parse(member_invalid)

    encrypted_fixture = build_archive_fixture(
        [member_definition("supporting/file.txt", b"payload", "SUPPORTING", zipfile.ZIP_STORED)],
        0,
        "confirm-encrypted",
    )
    encrypted_payload = bytearray(_decode(encrypted_fixture))
    structure = _parse_structure(bytes(encrypted_payload), parser.config)
    central = structure.eocd["central_directory_offset"]
    member = structure.members[0]
    struct.pack_into("<H", encrypted_payload, central + 8, 1)
    struct.pack_into("<H", encrypted_payload, member.local_header_offset + 6, 1)
    encrypted_result = parser.parse(_replace_payload(encrypted_fixture, bytes(encrypted_payload)))

    return {
        "middle_deletion": (
            deletion_result.archive["enumeration_status"] == "INCOMPLETE"
            and deletion_result.eligible_item_candidate_count == 0
        ),
        "path_traversal": (
            traversal_result.archive["security_status"] == "FAIL"
            and traversal_result.eligible_item_candidate_count == 0
        ),
        "nested_detect_only": (
            nested_result.status == "UNSUPPORTED"
            and nested_result.archive["nested_expansion_performed"] is False
            and nested_result.containers[2]["kind"] == "ARCHIVE"
        ),
        "source_member_separation": (
            source_layer_result.source["source_acquisition_status"] == "UNVERIFIED"
            and source_layer_result.archive["integrity_status"] == "COMPLETE"
            and member_layer_result.source["source_acquisition_status"] == "VERIFIED_COMPLETE"
            and member_layer_result.archive["enumeration_status"] == "INCOMPLETE"
        ),
        "encrypted_detect_only": (
            encrypted_result.status == "UNSUPPORTED"
            and encrypted_result.archive["credential_status"] == "PASSWORD_REQUIRED"
            and encrypted_result.eligible_item_candidate_count == 0
        ),
    }


def main() -> None:
    failures: List[str] = []
    paths = {
        "source": RESULT_DIR / "source_audit.jsonl",
        "archive": RESULT_DIR / "archive_audit.jsonl",
        "member": RESULT_DIR / "member_audit.jsonl",
        "containers": RESULT_DIR / "child_containers.jsonl",
        "summary": RESULT_DIR / "replay_summary.jsonl",
    }
    for path in paths.values():
        _check(path.exists(), "missing P6 result:" + str(path), failures)
    if failures:
        sys.exit(1)
    records = {name: read_jsonl_as_list(str(path)) for name, path in paths.items()}
    _check(len(records["summary"]) == 1, "summary count must be one", failures)
    if not records["summary"]:
        sys.exit(1)
    summary = records["summary"][0]
    _check(len(records["source"]) == 6, "source audit count must be 5 synthetic + 1 actual", failures)
    _check(len(records["archive"]) == 6, "archive audit count must match source count", failures)
    _check(len(records["member"]) == summary.get("synthetic_member_total") + summary.get("actual_member_count"), "member audit count mismatch", failures)
    _check(
        len(records["containers"])
        == 2 * len(records["source"]) + summary.get("synthetic_child_container_total") + summary.get("actual_technical_child_count"),
        "container tree count mismatch",
        failures,
    )
    _check(summary.get("synthetic_item_candidate_counts") == [0, 1, 2, 4, 10], "variable-N sequence mismatch", failures)
    _check(summary.get("synthetic_eligible_counts") == [0, 1, 2, 4, 10], "eligible variable-N mismatch", failures)
    _check(summary.get("synthetic_statuses") == ["PARSED"] * 5, "synthetic archive status mismatch", failures)
    _check(summary.get("idempotency_ok") is True, "idempotency failed", failures)
    _check(summary.get("actual_source_acquisition") == "UNVERIFIED", "actual acquisition must remain unverified", failures)
    _check(summary.get("actual_zip_integrity") == "COMPLETE", "actual ZIP integrity mismatch", failures)
    _check(summary.get("actual_member_enumeration") == "COMPLETE", "actual enumeration mismatch", failures)
    _check(summary.get("actual_member_count") == 1, "actual observed member must be one", failures)
    _check(summary.get("actual_technical_child_kinds") == ["SPREADSHEET"], "actual technical child mismatch", failures)
    _check(summary.get("actual_eligible") == 0 and summary.get("actual_auto_union") is False, "actual must remain ineligible", failures)
    _check(
        all(
            isinstance(row.get("position"), int)
            and isinstance(row.get("compressed_size"), int)
            and isinstance(row.get("uncompressed_size"), int)
            and isinstance(row.get("crc32"), int)
            and isinstance(row.get("original_name"), str)
            for row in records["member"]
        ),
        "member audit schema/type mismatch",
        failures,
    )
    _check(
        all(
            row.get("archive_sha256", "").startswith("sha256:")
            and row.get("parser_version") == "1.0.0"
            and row.get("password_persistence") == "EPHEMERAL_ONLY"
            for row in records["archive"]
        ),
        "archive audit evidence/version mismatch",
        failures,
    )

    negative = _negative_contracts(ArchiveParser.from_file(CONFIG_PATH))
    for name, passed in negative.items():
        _check(passed, "negative contract failed:" + name, failures)

    report = {
        "status": "OK" if not failures else "NG",
        "input_source_count": len(records["source"]),
        "archive_output_count": len(records["archive"]),
        "member_output_count": len(records["member"]),
        "container_output_count": len(records["containers"]),
        "error_count": len(failures),
        "variable_n": [0, 1, 2, 4, 10],
        "negative_contracts": negative,
        "actual_source_acquisition": summary.get("actual_source_acquisition"),
        "actual_member_count": summary.get("actual_member_count"),
        "actual_eligible": summary.get("actual_eligible"),
        "failures": failures,
    }
    write_jsonl(str(CONFIRM_PATH), [report])
    for archive in records["archive"][:3]:
        logger.info(
            "representative: source_id=" + str(archive.get("source_id"))
            + " members=" + str(archive.get("totals", {}).get("members"))
            + " status=" + str(archive.get("overall_status"))
        )
    if failures:
        logger.error("P6 archive confirm NG: failures=" + str(len(failures)))
        sys.exit(1)
    logger.ok(
        "P6 archive confirm OK: sources=6 members="
        + str(len(records["member"]))
        + " variable_N=0/1/2/4/10 actual=UNVERIFIED/eligible0"
    )


if __name__ == "__main__":
    main()

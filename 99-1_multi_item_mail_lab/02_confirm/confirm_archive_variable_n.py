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
from archive_parser import (
    ArchiveParser,
    MEMBER_MANIFEST_FIELD,
    SOURCE_ITEM_EVIDENCE_FIELD,
    _parse_structure,
)
from attachment_manifest_contract import canonical_ordered_entries, ordered_attachment_digest, source_payload_digest


logger = get_logger("confirm_99-1_p6_archive")
CONFIG_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "archive"
    / "archive_security.v1.json.example"
)
ENFAST_ROLE_CONFIG_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "companies"
    / "enfast_archive.config.json.example"
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


def _negative_contracts(
    parser: ArchiveParser, enfast_parser: ArchiveParser
) -> Dict[str, bool]:
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
    fullwidth_traversal_result = parser.parse(
        build_archive_fixture(
            [
                member_definition(
                    "supporting/..＼escape.txt",
                    b"unsafe",
                    "SUPPORTING",
                    zipfile.ZIP_STORED,
                )
            ],
            0,
            "confirm-fullwidth-traversal",
        )
    )
    fullwidth_collision_result = parser.parse(
        build_archive_fixture(
            [
                member_definition(
                    "supporting/dir＼a.txt", b"a", "SUPPORTING", zipfile.ZIP_STORED
                ),
                member_definition(
                    "supporting/dir/a.txt", b"b", "SUPPORTING", zipfile.ZIP_STORED
                ),
            ],
            0,
            "confirm-fullwidth-collision",
        )
    )

    sales_items = [
        member_definition(
            "resource-" + chr(ord("A") + index) + ".xlsx",
            b"resource",
            "ITEM_CANDIDATE",
        )
        for index in range(4)
    ]
    sales_supporting = member_definition(
        "営業案内.pdf", b"sales", "SUPPORTING", zipfile.ZIP_STORED
    )
    sales_result = enfast_parser.parse(
        build_archive_fixture(
            sales_items + [sales_supporting],
            4,
            "confirm-sales-supporting",
            source_from="Enfast <common@enfast-tech.com>",
        )
    )
    sales_mismatch = build_archive_fixture(
        sales_items[:3] + [sales_supporting],
        3,
        "confirm-sales-mismatch",
        source_from="Enfast <common@enfast-tech.com>",
    )
    sales_mismatch[SOURCE_ITEM_EVIDENCE_FIELD].update(
        {"count": 4, "item_keys": ["resource-A", "resource-B", "resource-C", "resource-D"]}
    )
    sales_mismatch_result = enfast_parser.parse(sales_mismatch)
    sales_unknown_result = enfast_parser.parse(
        build_archive_fixture(
            sales_items
            + [member_definition("謎ファイル.pdf", b"unknown", "UNKNOWN")],
            4,
            "confirm-sales-unknown",
            source_from="Enfast <common@enfast-tech.com>",
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
        "fullwidth_path_traversal": (
            fullwidth_traversal_result.archive["security_status"] == "FAIL"
            and fullwidth_traversal_result.eligible_item_candidate_count == 0
        ),
        "fullwidth_normalized_collision": (
            fullwidth_collision_result.archive["security_status"] == "FAIL"
            and any(
                reason.startswith("duplicate_normalized_member:")
                for reason in fullwidth_collision_result.reasons
            )
        ),
        "sales_supporting_role": (
            sales_result.status == "PARSED"
            and sales_result.archive["totals"]["members"] == 5
            and sales_result.archive["totals"]["item_candidates"] == 4
            and sales_result.members[4]["role"] == "SUPPORTING"
            and sales_result.eligible_item_candidate_count == 4
        ),
        "sales_cardinality_mismatch": (
            sales_mismatch_result.eligible_item_candidate_count == 0
            and "source_item_candidate_count_mismatch" in sales_mismatch_result.reasons
        ),
        "sales_unknown_fail_closed": (
            sales_unknown_result.status == "HUMAN_REVIEW"
            and sales_unknown_result.eligible_item_candidate_count == 0
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
    actual_observation_count = summary.get("actual_observation_count")
    _check(actual_observation_count in {0, 1}, "actual observation count must be zero or one", failures)
    _check(len(records["source"]) == 5 + actual_observation_count, "source audit count mismatch", failures)
    _check(len(records["archive"]) == len(records["source"]), "archive audit count must match source count", failures)
    _check(
        len(records["member"])
        == summary.get("synthetic_member_total")
        + (summary.get("actual_member_count") or 0),
        "member audit count mismatch",
        failures,
    )
    _check(
        len(records["containers"])
        == summary.get("synthetic_container_total")
        + (summary.get("actual_container_tree_count") or 0),
        "container tree count mismatch",
        failures,
    )
    _check(summary.get("synthetic_item_candidate_counts") == [0, 1, 2, 4, 10], "variable-N sequence mismatch", failures)
    _check(summary.get("synthetic_eligible_counts") == [0, 1, 2, 4, 10], "eligible variable-N mismatch", failures)
    _check(summary.get("synthetic_statuses") == ["PARSED"] * 5, "synthetic archive status mismatch", failures)
    _check(summary.get("idempotency_ok") is True, "idempotency failed", failures)
    _check(
        summary.get("supporting_role_contract")
        == {
            "status": "PARSED",
            "member_count": 5,
            "item_candidate_count": 4,
            "supporting_count": 1,
            "eligible_item_count": 4,
        },
        "sales supporting role contract mismatch",
        failures,
    )
    _check(summary.get("actual_runtime_fixed_oracle") == 0, "actual fixed oracle must be zero", failures)
    if actual_observation_count == 1:
        _check(summary.get("actual_availability") == "OBSERVATION", "actual availability mismatch", failures)
        _check(summary.get("actual_source_acquisition") == "UNVERIFIED", "actual acquisition must remain unverified", failures)
        _check(summary.get("actual_eligible") == 0, "actual must remain ineligible", failures)
        _check(summary.get("actual_auto_union") is False, "actual auto-union must remain disabled", failures)
        _check(isinstance(summary.get("actual_technical_child_kinds"), list), "actual observed child kinds type mismatch", failures)
        if summary.get("actual_member_enumeration") == "COMPLETE":
            _check(isinstance(summary.get("actual_member_count"), int), "actual observed member count type mismatch", failures)
        else:
            _check(summary.get("actual_member_count") is None, "failed actual inspection must not invent member count", failures)
    else:
        _check(summary.get("actual_availability") == "DATA_UNAVAILABLE", "missing actual must be explicit", failures)
        _check(summary.get("actual_source_acquisition") == "DATA_UNAVAILABLE", "missing actual acquisition status mismatch", failures)
        _check(summary.get("actual_member_count") is None, "missing actual member count must be unknown", failures)
        _check(summary.get("actual_technical_child_kinds") is None, "missing actual child kinds must be unknown", failures)
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

    negative = _negative_contracts(
        ArchiveParser.from_file(CONFIG_PATH),
        ArchiveParser.from_files(CONFIG_PATH, ENFAST_ROLE_CONFIG_PATH),
    )
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
        "supporting_role_contract": summary.get("supporting_role_contract"),
        "actual_availability": summary.get("actual_availability"),
        "actual_source_acquisition": summary.get("actual_source_acquisition"),
        "actual_inspection_status": summary.get("actual_inspection_status"),
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
        "P6 archive confirm OK: sources="
        + str(len(records["source"]))
        + " members="
        + str(len(records["member"]))
        + " variable_N=0/1/2/4/10 supporting=PASS actual="
        + str(summary.get("actual_availability"))
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Run stable P6 synthetic archive replay plus saved Enfast observation."""

import copy
import sys
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


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
from archive_fixture_source import build_archive_fixture, member_definition, variable_n_definitions
from archive_parser import ArchiveParser


logger = get_logger("run_99-1_p6_archive_replay")
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
ACTUAL_INPUT = (
    PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)
RESULT_DIR = STEP_DIR / "01_result" / "archive_variable_n"


def _definitions(count: int) -> List[Dict[str, Any]]:
    extras: List[Dict[str, Any]] = []
    if count == 2:
        extras.append(
            member_definition(
                "supporting/readme.pdf", b"synthetic-supporting", "SUPPORTING", zipfile.ZIP_STORED
            )
        )
    if count == 4:
        extras.extend(
            [
                member_definition(
                    "supporting/readme.pdf", b"synthetic-supporting", "SUPPORTING", zipfile.ZIP_STORED
                ),
                member_definition(
                    "folder/", b"", "DIRECTORY", zipfile.ZIP_STORED, "DIRECTORY"
                ),
            ]
        )
    if count == 10:
        extras.append(
            member_definition(
                "shared/template.xlsx", b"synthetic-shared", "SHARED", zipfile.ZIP_DEFLATED
            )
        )
    return variable_n_definitions(count, extras)


def _actual_record(records: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    matches = [
        record
        for record in records
        if "@enfast-tech.com" in str(record.get("from", "")).casefold()
        and any(
            str(attachment.get("filename", "")).casefold().endswith(".zip")
            for attachment in record.get("attachments", [])
            if isinstance(attachment, dict)
        )
    ]
    return (
        sorted(matches, key=lambda row: str(row.get("date", "")), reverse=True)[0]
        if matches
        else None
    )


def _sales_supporting_contract(parser: ArchiveParser) -> Any:
    definitions = [
        member_definition(
            "resource-" + chr(ord("A") + index) + ".xlsx",
            ("resource-" + str(index)).encode("utf-8"),
            "ITEM_CANDIDATE",
        )
        for index in range(4)
    ] + [
        member_definition(
            "営業案内.pdf", b"sales-guidance", "SUPPORTING", zipfile.ZIP_STORED
        )
    ]
    return parser.parse(
        build_archive_fixture(
            definitions,
            4,
            "synthetic-enfast-sales-supporting",
            source_from="Enfast <common@enfast-tech.com>",
        )
    )


def build_results(
    actual_records: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    parser = ArchiveParser.from_file(CONFIG_PATH)
    enfast_parser = ArchiveParser.from_files(CONFIG_PATH, ENFAST_ROLE_CONFIG_PATH)
    fixtures = [
        build_archive_fixture(
            _definitions(count), count, "synthetic-archive-n-" + str(count)
        )
        for count in (0, 1, 2, 4, 10)
    ]
    synthetic_results = [parser.parse(fixture) for fixture in fixtures]
    repeated = [parser.parse(copy.deepcopy(fixture)) for fixture in fixtures]
    if [result.to_dict() for result in synthetic_results] != [result.to_dict() for result in repeated]:
        raise ValueError("archive replay is not idempotent")
    supporting_result = _sales_supporting_contract(enfast_parser)
    if supporting_result.status != "PARSED":
        raise ValueError("Enfast supporting role synthetic contract failed")
    records = (
        read_jsonl_as_list(str(ACTUAL_INPUT))
        if actual_records is None
        else list(actual_records)
    )
    actual_record = _actual_record(records)
    actual_result = enfast_parser.parse(actual_record) if actual_record else None
    results = synthetic_results + ([actual_result] if actual_result else [])
    source_audits = [result.source for result in results]
    archive_audits = [
        dict(result.archive, source_id=result.source["source_id"], overall_status=result.status)
        for result in results
    ]
    member_audits = [
        dict(member, source_id=result.source["source_id"])
        for result in results
        for member in result.members
    ]
    child_containers = [
        dict(container, source_id=result.source["source_id"])
        for result in results
        for container in result.containers
    ]
    synthetic_member_counts = [result.archive["totals"]["members"] for result in synthetic_results]
    summary = {
        "phase": "P6_ARCHIVE_VARIABLE_N",
        "implementation": "PASS",
        "supported_format": "ZIP",
        "config_version": parser.config["config_version"],
        "synthetic_source_count": len(synthetic_results),
        "synthetic_item_candidate_counts": [0, 1, 2, 4, 10],
        "synthetic_archive_member_counts": synthetic_member_counts,
        "synthetic_statuses": [result.status for result in synthetic_results],
        "synthetic_eligible_counts": [
            result.eligible_item_candidate_count for result in synthetic_results
        ],
        "synthetic_member_total": sum(synthetic_member_counts),
        "synthetic_child_container_total": sum(
            max(0, len(result.containers) - 2) for result in synthetic_results
        ),
        "synthetic_container_total": sum(
            len(result.containers) for result in synthetic_results
        ),
        "idempotency_ok": True,
        "supporting_role_contract": {
            "status": supporting_result.status,
            "member_count": supporting_result.archive["totals"]["members"],
            "item_candidate_count": supporting_result.archive["totals"]["item_candidates"],
            "supporting_count": sum(
                member["role"] == "SUPPORTING" for member in supporting_result.members
            ),
            "eligible_item_count": supporting_result.eligible_item_candidate_count,
        },
        "actual_availability": "OBSERVATION" if actual_result else "DATA_UNAVAILABLE",
        "actual_observation_count": 1 if actual_result else 0,
        "actual_source_id": actual_result.source["source_id"] if actual_result else None,
        "actual_source_acquisition": (
            actual_result.source["source_acquisition_status"] if actual_result else "DATA_UNAVAILABLE"
        ),
        "actual_inspection_status": actual_result.status if actual_result else "DATA_UNAVAILABLE",
        "actual_zip_integrity": actual_result.archive["integrity_status"] if actual_result else None,
        "actual_member_enumeration": actual_result.archive["enumeration_status"] if actual_result else None,
        "actual_member_count": (
            actual_result.archive.get("totals", {}).get("members")
            if actual_result
            else None
        ),
        "actual_technical_child_count": max(0, len(actual_result.containers) - 2) if actual_result else None,
        "actual_container_tree_count": len(actual_result.containers) if actual_result else None,
        "actual_technical_child_kinds": (
            [container["kind"] for container in actual_result.containers[2:]]
            if actual_result
            else None
        ),
        "actual_eligible": actual_result.eligible_item_candidate_count if actual_result else None,
        "actual_auto_union": actual_result.source["auto_union_eligible"] if actual_result else None,
        "actual_runtime_fixed_oracle": 0,
        "production_changes": 0,
        "production_write": 0,
        "llm_api_calls": 0,
        "external_url_calls": 0,
    }
    return {
        "source_audits": source_audits,
        "archive_audits": archive_audits,
        "member_audits": member_audits,
        "child_containers": child_containers,
        "summary": summary,
    }


def main() -> None:
    results = build_results()
    write_jsonl(str(RESULT_DIR / "source_audit.jsonl"), results["source_audits"])
    write_jsonl(str(RESULT_DIR / "archive_audit.jsonl"), results["archive_audits"])
    write_jsonl(str(RESULT_DIR / "member_audit.jsonl"), results["member_audits"])
    write_jsonl(str(RESULT_DIR / "child_containers.jsonl"), results["child_containers"])
    write_jsonl(str(RESULT_DIR / "replay_summary.jsonl"), [results["summary"]])
    logger.ok(
        "P6 archive replay OK: synthetic_N=0/1/2/4/10 supporting=PASS actual="
        + str(results["summary"]["actual_availability"])
        + " observed_members="
        + str(results["summary"]["actual_member_count"])
    )


if __name__ == "__main__":
    main()

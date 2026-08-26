#!/usr/bin/env python3
"""Confirm P5 ATTACHMENT_LIST actual replay and synthetic fail-closed cases."""

import copy
import sys
from pathlib import Path
from typing import Any, Dict, List


sys.dont_write_bytecode = True
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
for import_path in (
    PROJECT_ROOT,
    STEP_DIR / "00_tool",
    STEP_DIR / "00_tool" / "adapters" / "attachment_list",
    STEP_DIR / "00_tool" / "canonicalize",
    STEP_DIR / "00_tool" / "core",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.json_utils import read_jsonl_as_list
from common.logger import get_logger
from attachment_fixture_source import build_source_owned_fixture
from attachment_list_adapter import AttachmentListAdapter
from canonical_overlay import MAIL_MASTER_KEYS
from run_attachment_list_offline_replay import (
    CONFIG_PATH,
    RESULT_SUBDIR,
    build_attachment_list_results,
)
from test_attachment_list_adapter import (
    _attachment,
    _refresh_manifest,
    _xlsx_bytes,
    _zip_bytes,
    synthetic_source,
)


logger = get_logger("confirm_99-1_attachment_list_variable_n")
RESULT_DIR = STEP_DIR / "01_result" / RESULT_SUBDIR


def _check(condition: bool, message: str, failures: List[str]) -> None:
    if not condition:
        failures.append(message)
        logger.error(message)


def _read(filename: str) -> List[Dict[str, Any]]:
    path = RESULT_DIR / filename
    if not path.exists():
        raise FileNotFoundError("missing ATTACHMENT_LIST result:" + str(path))
    return read_jsonl_as_list(str(path))


def main() -> None:
    failures: List[str] = []
    fresh = build_attachment_list_results()
    saved = {
        "source_audit": _read("source_audit.jsonl"),
        "attachment_enumeration": _read("attachment_enumeration.jsonl"),
        "canonical_eligible_audit_items": _read("canonical_eligible_audit_items.jsonl"),
        "canonical_eligible_mail_master": _read("canonical_eligible_mail_master.jsonl"),
        "canonical_eligible_input_ids": _read("canonical_eligible_input_ids.jsonl"),
        "technical_projection_audit_items": _read("technical_projection_audit_items.jsonl"),
        "technical_projection_mail_master": _read("technical_projection_mail_master.jsonl"),
        "technical_projection_input_ids": _read("technical_projection_input_ids.jsonl"),
        "technical_cleanup": _read("technical_01-4_cleanup.jsonl"),
        "technical_classification": _read("technical_02-1_classification.jsonl"),
    }
    summaries = _read("replay_summary.jsonl")
    _check(len(summaries) == 1, "summary must contain one JSONL record", failures)
    if not summaries:
        raise SystemExit(1)
    saved["summary"] = summaries[0]
    _check(saved == fresh, "saved P5 outputs are not fresh Core/parser results", failures)

    summary = fresh["summary"]
    required = {
        "actual_deliveries": 3,
        "actual_observed_attachment_counts": [2, 4, 2],
        "actual_profile_counts": [2, 4, 2],
        "actual_declared_counts": [2, 4, 2],
        "actual_mapping_counts": [2, 4, 2],
        "actual_mapping_total": 8,
        "actual_station_audit_matches": 8,
        "false_substring_matches": 0,
        "actual_acquisition_statuses": ["UNVERIFIED"] * 3,
        "actual_container_enumeration_statuses": ["COMPLETE"] * 3,
        "actual_inline_structure_statuses": ["PASS"] * 3,
        "actual_attachment_mapping_statuses": ["PASS"] * 3,
        "actual_source_atomic_statuses": ["PARTIAL"] * 3,
        "actual_auto_union_eligible": False,
        "technical_projection_total": 8,
        "canonical_eligible_total": 0,
        "cross_item_contamination": 0,
        "cleanup_output": 8,
        "cleanup_nonempty": 8,
        "classification_output": 8,
        "resource_classified": 8,
        "project_classified": 0,
        "ambiguous_output": 0,
        "unknown_output": 0,
        "llm_api_calls": 0,
        "external_url_calls": 0,
        "production_changes": 0,
        "production_write": 0,
    }
    for key, expected in required.items():
        _check(summary.get(key) == expected, key + ":" + str(summary.get(key)) + ":expected:" + str(expected), failures)
    _check(summary.get("derived_id_deterministic") is True, "derived IDs must be deterministic", failures)

    sources = fresh["source_audit"]
    _check(
        all(
            audit["source"]["source_acquisition_status"] == "UNVERIFIED"
            and audit["source"]["manifest_contract_status"] == "UNVERIFIED"
            and audit["source"]["attachment_integrity_status"] == "PASS"
            and audit["source"]["container_enumeration_status"] == "COMPLETE"
            and audit["source"]["inline_structure_status"] == "PASS"
            and audit["source"]["attachment_mapping_status"] == "PASS"
            and audit["source"]["source_atomic_status"] == "PARTIAL"
            and audit["source"]["auto_union_eligible"] is False
            and [row["authority"] for row in audit["source"]["cardinality_evidence"]]
            == ["CONTAINER_ENUMERATION", "DECLARED_COUNT", "STRUCTURAL_COMPLETE"]
            for audit in sources
        ),
        "actual source/container/cardinality separation failed",
        failures,
    )
    _check(
        all(
            [row["kind"] for row in audit["container_contracts"][:2]]
            == ["INLINE_BODY", "ATTACHMENT_LIST"]
            for audit in sources
        ),
        "ATTACHMENT_LIST Core extension is not represented",
        failures,
    )
    enumeration = fresh["attachment_enumeration"]
    _check(
        len(enumeration) == 8
        and all(
            row["role"] == "ITEM_ATTACHMENT"
            and row["xlsx_valid"] is True
            and row["decoded_size"] == row["declared_size"]
            and row["content_digest"].startswith("sha256:")
            for row in enumeration
        ),
        "actual attachment XLSX enumeration/validation failed",
        failures,
    )
    _check(
        fresh["canonical_eligible_audit_items"] == []
        and fresh["canonical_eligible_mail_master"] == []
        and fresh["canonical_eligible_input_ids"] == [],
        "actual eligible outputs must remain empty",
        failures,
    )
    overlays = fresh["technical_projection_mail_master"]
    audits = fresh["technical_projection_audit_items"]
    _check(
        len(overlays) == 8
        and all(
            set(overlay) == MAIL_MASTER_KEYS
            and len(overlay["attachments"]) == 1
            and overlay["html_links"] == []
            and overlay["from"]
            for overlay in overlays
        ),
        "technical canonical projection schema/contamination failed",
        failures,
    )
    _check(
        all(
            audit["projection_kind"] == "TECHNICAL_PROJECTION"
            and audit["parse_status"] == "TECHNICAL_PROJECTION"
            and audit["attachment_mapping"]["strategy"] == "ONE_ARTIFACT_PER_ITEM_EXACT_KEY"
            and audit["classification_context_evidence"]["item_type_used"] is False
            for audit in audits
        ),
        "mapping, technical projection, or classification evidence failed",
        failures,
    )
    classifications = {row["message_id"]: row["mail_type"] for row in fresh["technical_classification"]}
    _check(
        all(classifications.get(audit["derived_item_id"]) == "resource" for audit in audits),
        "02-1 resource classification failed",
        failures,
    )

    adapter = AttachmentListAdapter.from_file(CONFIG_PATH)
    for count in (0, 1, 2, 4, 10):
        result = adapter.parse(build_source_owned_fixture(synthetic_source(count)))
        _check(
            result.status == "PARSED"
            and result.source["source_acquisition_status"] == "VERIFIED_COMPLETE"
            and len(result.items) == count,
            "synthetic variable-N failed:" + str(count),
            failures,
        )

    producer_mutations = {
        "source_entry_id_empty": ("source_entry_id", ""),
        "filename_empty": ("filename", ""),
        "mime_empty": ("mime_type", ""),
        "size_missing": ("size", None),
        "size_negative": ("size", -1),
        "size_string": ("size", "123"),
        "digest_invalid": ("content_digest", "sha256:short"),
    }
    for name, (field, value) in producer_mutations.items():
        source = synthetic_source(1)
        if value is None:
            source["authoritative_attachments"][0].pop(field)
        else:
            source["authoritative_attachments"][0][field] = value
        rejected = False
        try:
            build_source_owned_fixture(source)
        except ValueError:
            rejected = True
        _check(rejected, "fixture producer accepted invalid entry:" + name, failures)

    manifest_mutations = {
        "source_entry_id_missing": ("source_entry_id", None, "source_entry_id"),
        "source_entry_id_empty": ("source_entry_id", "", "source_entry_id"),
        "filename_missing": ("filename", None, "filename"),
        "filename_empty": ("filename", "", "filename"),
        "mime_missing": ("mime_type", None, "mime_type"),
        "mime_empty": ("mime_type", "", "mime_type"),
        "size_missing": ("declared_size", None, "size"),
        "size_negative": ("declared_size", -1, "size"),
        "size_string": ("declared_size", "123", "size"),
        "digest_missing": ("content_digest", None, None),
        "digest_malformed": ("content_digest", "sha256:short", None),
        "digest_non_sha256": ("content_digest", "md5:" + "0" * 32, None),
    }
    for name, (manifest_field, value, observed_field) in manifest_mutations.items():
        fixture = build_source_owned_fixture(synthetic_source(1))
        entry = fixture["attachment_acquisition_manifest"][
            "authoritative_attachment_entries"
        ][0]
        if value is None:
            entry.pop(manifest_field)
            if observed_field:
                fixture["attachments"][0].pop(observed_field)
        else:
            entry[manifest_field] = value
            if observed_field:
                fixture["attachments"][0][observed_field] = value
        _refresh_manifest(fixture)
        result = adapter.parse(fixture)
        _check(
            result.status != "PARSED"
            and result.items == []
            and result.source["source_acquisition_status"] == "INCOMPLETE"
            and result.source["manifest_contract_status"] == "FAIL"
            and result.source["container_enumeration_status"] == "COMPLETE",
            "manifest entry fail-closed failed:" + name,
            failures,
        )

    integrity_source = synthetic_source(1)
    inline = _attachment("inline-logo.png", b"image", mime_type="image/png")
    inline.update({"disposition": "inline", "content_id": "logo"})
    integrity_source["authoritative_attachments"].extend(
        [
            _attachment("shared-format.xlsx", _xlsx_bytes("shared")),
            _attachment(
                "supporting-readme.pdf", b"synthetic-pdf", mime_type="application/pdf"
            ),
            inline,
        ]
    )
    for role, position in (
        ("ITEM_ATTACHMENT", 0),
        ("SHARED", 1),
        ("SUPPORTING", 2),
        ("INLINE_ASSET", 3),
    ):
        fixture = build_source_owned_fixture(copy.deepcopy(integrity_source))
        fixture["attachments"][position].pop("data")
        result = adapter.parse(fixture)
        _check(
            result.status != "PARSED"
            and result.items == []
            and result.source["source_acquisition_status"] == "INCOMPLETE"
            and result.source["attachment_integrity_status"] == "FAIL",
            "attachment integrity atomic failed:" + role,
            failures,
        )

    blocker_source = synthetic_source(1)
    blocker_source["authoritative_attachments"].append(
        _attachment("shared-format.xlsx", _xlsx_bytes("shared"))
    )
    blocker_fixture = build_source_owned_fixture(blocker_source)
    for field in ("mime_type", "size", "data"):
        blocker_fixture["attachments"][1].pop(field)
    blocker_entry = blocker_fixture["attachment_acquisition_manifest"][
        "authoritative_attachment_entries"
    ][1]
    for field in ("mime_type", "declared_size", "content_digest"):
        blocker_entry.pop(field)
    _refresh_manifest(blocker_fixture)
    blocker_result = adapter.parse(blocker_fixture)
    _check(
        blocker_result.status != "PARSED"
        and blocker_result.items == []
        and blocker_result.source["manifest_contract_status"] == "FAIL"
        and blocker_result.source["attachment_integrity_status"] == "FAIL"
        and blocker_result.source["container_enumeration_status"] == "COMPLETE",
        "previous SHARED integrity blocker still passes",
        failures,
    )

    base = build_source_owned_fixture(synthetic_source(4))
    missing = copy.deepcopy(base)
    missing.pop("attachment_acquisition_manifest")
    deletion = copy.deepcopy(base)
    del deletion["attachments"][2]
    replacement = copy.deepcopy(base)
    replacement["attachments"][2] = _attachment(
        "R.C南林間_スキルシート.xlsx",
        _xlsx_bytes("replacement"),
        source_entry_id=base["attachments"][2]["source_entry_id"],
    )
    reordered = copy.deepcopy(base)
    reordered["attachments"][1], reordered["attachments"][2] = reordered["attachments"][2], reordered["attachments"][1]
    for name, case, expected_acquisition in (
        ("manifest_missing", missing, "UNVERIFIED"),
        ("middle_deletion", deletion, "INCOMPLETE"),
        ("replacement", replacement, "INCOMPLETE"),
        ("order", reordered, "INCOMPLETE"),
    ):
        result = adapter.parse(case)
        _check(
            result.status != "PARSED"
            and result.items == []
            and result.source["source_acquisition_status"] == expected_acquisition,
            "acquisition negative failed:" + name,
            failures,
        )
    deletion_result = adapter.parse(deletion)
    _check(
        deletion_result.source["container_enumeration_status"] == "COMPLETE"
        and deletion_result.source["source_atomic_status"] != "PARSED",
        "middle deletion source/container split failed",
        failures,
    )

    role_source = synthetic_source(4)
    role_source["authoritative_attachments"].extend(
        [
            _attachment("shared-format.xlsx", _xlsx_bytes("shared")),
            _attachment("supporting-readme.pdf", b"synthetic-pdf", mime_type="application/pdf"),
        ]
    )
    role_result = adapter.parse(build_source_owned_fixture(role_source))
    _check(
        role_result.status == "PARSED"
        and role_result.source["item_attachment_count"] == 4
        and role_result.source["attachment_role_counts"]["SHARED"] == 1
        and role_result.source["attachment_role_counts"]["SUPPORTING"] == 1,
        "shared/supporting role boundary failed",
        failures,
    )
    zip_source = synthetic_source(2)
    zip_source["authoritative_attachments"].append(
        _attachment(
            "profiles.zip",
            _zip_bytes([("inside.xlsx", _xlsx_bytes("inside"))]),
            mime_type="application/zip",
        )
    )
    zip_result = adapter.parse(build_source_owned_fixture(zip_source))
    _check(
        zip_result.status == "UNSUPPORTED"
        and zip_result.items == []
        and zip_result.source["container_enumeration_status"] == "COMPLETE"
        and zip_result.source["attachment_role_counts"]["ARCHIVE"] == 1,
        "ZIP archive boundary failed",
        failures,
    )

    logger.info("ACTUAL: deliveries=3 observed=2/4/2 profiles=2/4/2 declared=2/4/2 mapping=8/8")
    logger.info("ACTUAL STATUS: acquisition=UNVERIFIED container=COMPLETE inline=PASS mapping=PASS atomic=PARTIAL eligible=0")
    logger.info("TECHNICAL: projection=8/8 01-4=8/8 02-1_resource=8/8 project=0 ambiguous=0 unknown=0")
    logger.info("SYNTHETIC: N=0/1/2/4/10 PASS manifest_entry_negative=PASS integrity_atomic=PASS roles=PASS ZIP=PASS")
    for audit in audits[:3]:
        logger.info(
            "representative: delivery=" + audit["original_message_id"]
            + " item_index=" + str(audit["item_index"])
            + " mapping=" + audit["attachment_mapping"]["strategy"]
        )
    if failures:
        logger.error("ATTACHMENT_LIST confirm NG: failures=" + str(len(failures)))
        raise SystemExit(1)
    logger.ok(
        "ATTACHMENT_LIST confirm OK: actual=3 observed=2/4/2 mapping=8/8 "
        "acquisition=UNVERIFIED eligible=0 technical=8 synthetic=5/5"
    )


if __name__ == "__main__":
    main()

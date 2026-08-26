#!/usr/bin/env python3
"""Confirm fresh P4 LINK_BUNDLE results, cardinality, overlay, and 02-1 split."""

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
    STEP_DIR / "00_tool" / "adapters" / "link_bundle",
    STEP_DIR / "00_tool" / "canonicalize",
    STEP_DIR / "00_tool" / "core",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.json_utils import read_jsonl_as_list
from common.logger import get_logger
from canonical_overlay import MAIL_MASTER_KEYS
from link_bundle_adapter import LinkBundleAdapter
from link_bundle_fixture_source import build_source_owned_fixtures
from run_link_bundle_offline_replay import (
    CONFIG_PATH,
    RESULT_SUBDIR,
    build_link_bundle_results,
)


logger = get_logger("confirm_99-1_link_bundle_variable_n")
RESULT_DIR = STEP_DIR / "01_result" / RESULT_SUBDIR
FIXTURE_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "fixtures"
    / "redacted"
    / "link_bundle"
    / "drivenx.variable_n.fixture.jsonl.example"
)


def _check(condition: bool, message: str, failures: List[str]) -> None:
    if not condition:
        failures.append(message)
        logger.error(message)


def _read(filename: str) -> List[Dict[str, Any]]:
    path = RESULT_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"missing LINK_BUNDLE result: {path}")
    return read_jsonl_as_list(str(path))


def main() -> None:
    failures: List[str] = []
    fresh = build_link_bundle_results()
    saved = {
        "source_audit": _read("source_audit.jsonl"),
        "link_enumeration": _read("link_enumeration.jsonl"),
        "canonical_eligible_audit_items": _read(
            "canonical_eligible_audit_items.jsonl"
        ),
        "canonical_eligible_mail_master": _read(
            "canonical_eligible_mail_master.jsonl"
        ),
        "canonical_eligible_input_ids": _read("canonical_eligible_input_ids.jsonl"),
        "technical_projection_audit_items": _read(
            "technical_projection_audit_items.jsonl"
        ),
        "technical_projection_mail_master": _read(
            "technical_projection_mail_master.jsonl"
        ),
        "technical_projection_input_ids": _read(
            "technical_projection_input_ids.jsonl"
        ),
        "technical_cleanup": _read("technical_01-4_cleanup.jsonl"),
        "technical_classification": _read("technical_02-1_classification.jsonl"),
    }
    summaries = _read("replay_summary.jsonl")
    _check(len(summaries) == 1, "summary must contain one JSONL record", failures)
    if not summaries:
        raise SystemExit(1)
    saved["summary"] = summaries[0]
    _check(saved == fresh, "saved P4 outputs are not fresh Core/parser results", failures)
    for legacy_canonical_filename in (
        "audit_items.jsonl",
        "derived_mail_master.jsonl",
        "derived_input_ids.jsonl",
        "01-4_cleanup.jsonl",
        "02-1_classification.jsonl",
    ):
        _check(
            _read(legacy_canonical_filename) == [],
            f"legacy canonical path must be empty:{legacy_canonical_filename}",
            failures,
        )

    summary = fresh["summary"]
    required = {
        "input_sources": 1,
        "parsed_sources": 0,
        "partial_sources": 1,
        "human_review_sources": 0,
        "system_failure_sources": 0,
        "actual_links": 104,
        "links_classified": 104,
        "resource_headers": 1,
        "project_headers": 1,
        "resource_items": 50,
        "project_items": 50,
        "action_links": 2,
        "non_item_links": 4,
        "unknown_links": 0,
        "duplicate_item_locators": 0,
        "observed_canonical_candidates": 100,
        "technical_projection_resources": 50,
        "technical_projection_projects": 50,
        "technical_projection_total": 100,
        "canonical_eligible_resources": 0,
        "canonical_eligible_projects": 0,
        "canonical_eligible_total": 0,
        "cross_item_contamination": 0,
        "logical_distinct": 100,
        "derived_distinct": 100,
        "cleanup_output": 100,
        "cleanup_nonempty": 100,
        "classification_output": 100,
        "resource_classified_correct": 50,
        "project_classified_correct": 50,
        "ambiguous_output": 0,
        "unknown_output": 0,
        "llm_api_calls": 0,
        "external_url_calls": 0,
        "production_changes": 0,
        "production_write": 0,
    }
    for key, expected in required.items():
        _check(
            summary.get(key) == expected,
            f"{key}:{summary.get(key)}:expected:{expected}",
            failures,
        )
    _check(
        summary.get("derived_id_deterministic") is True,
        "derived IDs must be deterministic",
        failures,
    )
    for key, expected in {
        "actual_acquisition_status": "UNVERIFIED",
        "actual_container_enumeration_status": "COMPLETE",
        "actual_role_classification_status": "PASS",
        "actual_source_atomic_status": "PARTIAL",
        "actual_auto_union_eligible": False,
    }.items():
        _check(
            summary.get(key) == expected,
            f"{key}:{summary.get(key)}:expected:{expected}",
            failures,
        )

    source = fresh["source_audit"][0]["source"]
    _check(
        source.get("source_acquisition_status") == "UNVERIFIED",
        "actual acquisition must remain UNVERIFIED without a source manifest",
        failures,
    )
    _check(
        source.get("completeness_result", {}).get("status") == "PARTIAL"
        and source.get("completeness_result", {})
        .get("checks", {})
        .get("source_acquisition_complete")
        is False,
        "actual source atomic status must fail closed on acquisition evidence",
        failures,
    )
    _check(
        source.get("container_enumeration_status") == "COMPLETE",
        "actual container enumeration regression failed",
        failures,
    )
    _check(
        source.get("role_classification_status") == "PASS"
        and source.get("role_classification_count") == 104
        and source.get("role_classification_total") == 104,
        "actual role classification regression failed",
        failures,
    )
    _check(
        source.get("auto_union_eligible") is False,
        "actual source must not be auto-union eligible",
        failures,
    )
    _check(
        [row.get("authority") for row in source.get("cardinality_evidence", [])]
        == ["CONTAINER_ENUMERATION", "STRUCTURAL_COMPLETE"],
        "cardinality authority order mismatch",
        failures,
    )

    enumeration = fresh["link_enumeration"]
    _check(
        [row.get("index") for row in enumeration] == list(range(104)),
        "html_links ordered enumeration is incomplete",
        failures,
    )
    _check(
        [row.get("role") for row in enumeration[:3]]
        == ["ACTION", "ACTION", "RESOURCE_HEADER"],
        "resource boundary prefix mismatch",
        failures,
    )
    _check(
        enumeration[53].get("role") == "PROJECT_HEADER",
        "project boundary index mismatch in actual replay oracle",
        failures,
    )

    _check(
        fresh["canonical_eligible_audit_items"] == []
        and fresh["canonical_eligible_mail_master"] == []
        and fresh["canonical_eligible_input_ids"] == [],
        "actual canonical eligible outputs must be empty",
        failures,
    )
    audits = fresh["technical_projection_audit_items"]
    overlays = fresh["technical_projection_mail_master"]
    _check(
        all(set(record) == MAIL_MASTER_KEYS for record in overlays),
        "canonical mail-master schema mismatch",
        failures,
    )
    _check(
        all(
            isinstance(record.get("message_id"), str)
            and bool(record["message_id"])
            and isinstance(record.get("body_text"), str)
            and bool(record["body_text"])
            and record.get("attachments") == []
            and isinstance(record.get("html_links"), list)
            and len(record["html_links"]) == 1
            for record in overlays
        ),
        "canonical required fields, body, or item link mismatch",
        failures,
    )
    _check(
        all(
            audit.get("parse_status") == "TECHNICAL_PROJECTION"
            and audit.get("projection_kind") == "TECHNICAL_PROJECTION"
            and audit.get("auto_union_eligible") is False
            and audit.get("identity_durability") == "PROVISIONAL_DURABLE"
            and audit.get("version_scope") == "MAIL_SNAPSHOT_LIST_ITEM"
            and audit.get("item_artifacts", [{}])[0].get("role") == "PRIMARY"
            for audit in audits
        ),
        "identity, version scope, or artifact relation mismatch",
        failures,
    )

    classifications = {
        row["message_id"]: row["mail_type"]
        for row in fresh["technical_classification"]
    }
    _check(
        all(
            classifications.get(audit["derived_item_id"]) == audit["section_type"]
            for audit in audits
        ),
        "02-1 classification differs from natural section context",
        failures,
    )

    adapter = LinkBundleAdapter.from_file(CONFIG_PATH)
    fixtures = build_source_owned_fixtures(read_jsonl_as_list(str(FIXTURE_PATH)))
    expected_counts = {(0, 0), (1, 1), (2, 1), (1, 2), (10, 4), (4, 10)}
    observed_counts = set()
    for fixture in fixtures:
        result = adapter.parse(copy.deepcopy(fixture))
        counts = result.source.get("section_counts", {})
        observed_counts.add((counts.get("resource"), counts.get("project")))
        _check(
            result.status == "PARSED"
            and result.source.get("source_acquisition_status")
            == "VERIFIED_COMPLETE"
            and len(result.items) == counts.get("resource", 0) + counts.get("project", 0),
            f"synthetic variable-N failed:{fixture.get('message_id')}",
            failures,
        )
    _check(
        observed_counts == expected_counts,
        f"synthetic cardinalities mismatch:{sorted(observed_counts)}",
        failures,
    )

    duplicate = copy.deepcopy(fixtures[2])
    duplicate["html_links"].insert(4, copy.deepcopy(duplicate["html_links"][3]))
    duplicate_result = adapter.parse(duplicate)
    _check(
        duplicate_result.status != "PARSED" and duplicate_result.items == [],
        "duplicate locator must fail closed",
        failures,
    )
    unknown = copy.deepcopy(fixtures[2])
    unknown["html_links"].insert(
        4,
        {
            "text": "unknown",
            "href": "https://unknown.example.invalid/value",
            "source": "text/html",
        },
    )
    unknown_result = adapter.parse(unknown)
    _check(
        unknown_result.status != "PARSED" and unknown_result.items == [],
        "unknown link must fail closed",
        failures,
    )

    acquisition_cases = {"middle_insertion": unknown_result}
    missing = copy.deepcopy(fixtures[2])
    missing.pop("link_bundle_acquisition_manifest")
    acquisition_cases["manifest_missing"] = adapter.parse(missing)

    deletion = copy.deepcopy(fixtures[2])
    del deletion["html_links"][4]
    acquisition_cases["middle_deletion"] = adapter.parse(deletion)

    replacement = copy.deepcopy(fixtures[2])
    replacement["html_links"][4]["text"] = "replacement"
    replacement["html_links"][4]["href"] = (
        "https://cho-tatsu.com/boost/talents/replacement"
    )
    acquisition_cases["replacement"] = adapter.parse(replacement)

    reordered = copy.deepcopy(fixtures[2])
    reordered["html_links"][3], reordered["html_links"][4] = (
        reordered["html_links"][4],
        reordered["html_links"][3],
    )
    acquisition_cases["order"] = adapter.parse(reordered)

    for field, value in (
        ("source_id", "stale-source"),
        ("ordered_entry_count", 999),
        ("ordered_entry_digest", "sha256:" + "0" * 64),
    ):
        stale = copy.deepcopy(fixtures[2])
        stale["link_bundle_acquisition_manifest"][field] = value
        acquisition_cases[field] = adapter.parse(stale)

    for name, result in acquisition_cases.items():
        expected_status = "UNVERIFIED" if name == "manifest_missing" else "INCOMPLETE"
        _check(
            result.status != "PARSED"
            and result.items == []
            and result.source.get("source_acquisition_status") == expected_status,
            f"acquisition evidence case failed:{name}",
            failures,
        )
    _check(
        acquisition_cases["middle_deletion"].source.get(
            "container_enumeration_status"
        )
        == "COMPLETE",
        "middle deletion must preserve container-complete/source-incomplete split",
        failures,
    )

    logger.info(
        "ACTUAL: acquisition=UNVERIFIED container_parse=PASS observed_links=104 "
        "observed_candidates=100 auto_union_eligible=NO"
    )
    logger.info(
        "ACTUAL TECHNICAL PROJECTION: resource=50 project=50 "
        "01-4=100/100 02-1=50/50+50/50 ambiguous=0"
    )
    logger.info(
        "SYNTHETIC: acquisition_manifest=PASS middle_deletion=PASS "
        "insertion_replacement_order=PASS variable_N=PASS source_atomic=PASS"
    )
    for audit in audits[:3]:
        logger.info(
            "representative: "
            f"section={audit['section_type']} item_index={audit['item_index']} "
            f"logical_id={audit['logical_item_id']}"
        )
    if failures:
        logger.error(f"LINK_BUNDLE confirm NG: failures={len(failures)}")
        raise SystemExit(1)
    logger.ok(
        "LINK_BUNDLE confirm OK: actual_acquisition=UNVERIFIED actual_container=PASS "
        "actual_auto_union=NO technical=100 synthetic=6/6 evidence_negative=PASS"
    )


if __name__ == "__main__":
    main()

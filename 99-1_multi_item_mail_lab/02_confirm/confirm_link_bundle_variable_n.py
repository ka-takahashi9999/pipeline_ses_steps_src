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
        "audit_items": _read("audit_items.jsonl"),
        "derived_mail_master": _read("derived_mail_master.jsonl"),
        "derived_input_ids": _read("derived_input_ids.jsonl"),
        "cleanup": _read("01-4_cleanup.jsonl"),
        "classification": _read("02-1_classification.jsonl"),
    }
    summaries = _read("replay_summary.jsonl")
    _check(len(summaries) == 1, "summary must contain one JSONL record", failures)
    if not summaries:
        raise SystemExit(1)
    saved["summary"] = summaries[0]
    _check(saved == fresh, "saved P4 outputs are not fresh Core/parser results", failures)

    summary = fresh["summary"]
    required = {
        "input_sources": 1,
        "parsed_sources": 1,
        "partial_sources": 0,
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
        "canonical_resources": 50,
        "canonical_projects": 50,
        "canonical_total": 100,
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

    source = fresh["source_audit"][0]["source"]
    _check(
        source.get("completeness_result", {}).get("status") == "PARSED",
        "actual source completeness status must be PARSED",
        failures,
    )
    _check(
        all(source.get("completeness_result", {}).get("checks", {}).values()),
        "actual completeness gate has a failed check",
        failures,
    )
    _check(
        [row.get("authority") for row in source.get("cardinality_evidence", [])]
        == ["CONTAINER_ENUMERATION", "STRUCTURAL_COMPLETE", "SNAPSHOT_SET"],
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

    audits = fresh["audit_items"]
    overlays = fresh["derived_mail_master"]
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
            audit.get("parse_status") == "PARSED"
            and audit.get("identity_durability") == "PROVISIONAL_DURABLE"
            and audit.get("version_scope") == "MAIL_SNAPSHOT_LIST_ITEM"
            and audit.get("item_artifacts", [{}])[0].get("role") == "PRIMARY"
            for audit in audits
        ),
        "identity, version scope, or artifact relation mismatch",
        failures,
    )

    classifications = {
        row["message_id"]: row["mail_type"] for row in fresh["classification"]
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
    fixtures = read_jsonl_as_list(str(FIXTURE_PATH))
    expected_counts = {(0, 0), (1, 1), (2, 1), (1, 2), (10, 4), (4, 10)}
    observed_counts = set()
    for fixture in fixtures:
        result = adapter.parse(copy.deepcopy(fixture))
        counts = result.source.get("section_counts", {})
        observed_counts.add((counts.get("resource"), counts.get("project")))
        _check(
            result.status == "PARSED"
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

    logger.info(
        "counts: source=1 links=104 resource=50 project=50 non_item=4 "
        "canonical=100 01-4=100 02-1=50+50"
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
        "LINK_BUNDLE confirm OK: actual=104/104 canonical=100 "
        "resource=50/50 project=50/50 synthetic=6/6 negative=fail-closed"
    )


if __name__ == "__main__":
    main()

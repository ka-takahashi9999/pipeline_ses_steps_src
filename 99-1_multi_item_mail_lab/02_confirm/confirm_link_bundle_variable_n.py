#!/usr/bin/env python3
"""Confirm stable P4 LINK_BUNDLE contract plus optional actual observation."""

import copy
import sys
from pathlib import Path
from typing import List


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
from link_bundle_adapter import LinkBundleAdapter
from link_bundle_fixture_source import build_source_owned_fixtures
from run_link_bundle_offline_replay import (
    CONFIG_PATH,
    build_link_bundle_contract_results,
    build_link_bundle_results,
)


logger = get_logger("confirm_99-1_link_bundle_variable_n")
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


def main() -> None:
    failures: List[str] = []
    contract = build_link_bundle_contract_results()
    contract_summary = contract["summary"]
    expected_distribution = [[0, 0], [1, 1], [2, 1], [1, 2], [10, 4], [4, 10]]
    _check(
        contract_summary.get("contract_status") == "PASS",
        "stable LINK_BUNDLE contract failed",
        failures,
    )
    _check(
        contract_summary.get("observed_role_distribution") == expected_distribution,
        "stable role distribution mismatch",
        failures,
    )
    _check(
        contract_summary.get("middle_deletion_fail_closed") is True,
        "middle deletion must fail closed",
        failures,
    )
    _check(
        contract_summary.get("finding_count") == 0,
        "stable contract findings must be zero",
        failures,
    )

    adapter = LinkBundleAdapter.from_file(CONFIG_PATH)
    fixtures = build_source_owned_fixtures(read_jsonl_as_list(str(FIXTURE_PATH)))
    _check(len(fixtures) == 6, "stable fixture source count mismatch", failures)
    for fixture in fixtures:
        result = adapter.parse(copy.deepcopy(fixture))
        counts = result.source.get("section_counts", {})
        _check(
            result.status == "PARSED"
            and result.source.get("source_acquisition_status") == "VERIFIED_COMPLETE"
            and len(result.items)
            == counts.get("resource", 0) + counts.get("project", 0),
            "stable source-owned fixture failed:" + str(fixture.get("message_id")),
            failures,
        )

    deletion = copy.deepcopy(fixtures[2])
    del deletion["html_links"][4]
    deletion_result = adapter.parse(deletion)
    _check(
        deletion_result.status != "PARSED" and deletion_result.items == [],
        "middle deletion emitted canonical items",
        failures,
    )
    insertion = copy.deepcopy(fixtures[2])
    insertion["html_links"].insert(
        4,
        {
            "text": "unknown",
            "href": "https://unknown.example.invalid/value",
            "source": "text/html",
        },
    )
    insertion_result = adapter.parse(insertion)
    _check(
        insertion_result.status != "PARSED" and insertion_result.items == [],
        "middle insertion emitted canonical items",
        failures,
    )

    actual = build_link_bundle_results()
    actual_summary = actual["summary"]
    _check(
        actual_summary.get("actual_availability")
        in {"OBSERVATION", "DATA_UNAVAILABLE", "OBSERVATION_UNAVAILABLE"},
        "actual availability status mismatch",
        failures,
    )
    _check(
        actual_summary.get("actual_runtime_fixed_oracle") == 0,
        "actual fixed oracle must be zero",
        failures,
    )
    _check(
        isinstance(actual_summary.get("actual_observation_findings"), list)
        and actual_summary.get("actual_observation_finding_count")
        == len(actual_summary.get("actual_observation_findings")),
        "actual finding report mismatch",
        failures,
    )
    _check(
        actual_summary.get("production_write") == 0,
        "production artifacts changed during observation",
        failures,
    )

    logger.info(
        "CONTRACT TESTS: PASS role_distribution=0/0,1/1,2/1,1/2,10/4,4/10 "
        "middle_deletion=FAIL_CLOSED"
    )
    logger.info(
        "ACTUAL OBSERVATIONS: "
        + str(actual_summary.get("actual_availability"))
        + " observed_sources="
        + str(actual_summary.get("actual_observation_count"))
        + " observed_links="
        + str(actual_summary.get("actual_links"))
        + " observed_candidates="
        + str(actual_summary.get("technical_projection_total"))
    )
    logger.info(
        "ACTUAL OBSERVATION FINDINGS: "
        + str(actual_summary.get("actual_observation_finding_count"))
    )
    for fixture in fixtures[:3]:
        result = adapter.parse(copy.deepcopy(fixture))
        logger.info(
            "representative: source="
            + str(fixture.get("message_id"))
            + " items="
            + str(len(result.items))
            + " status="
            + str(result.status)
        )
    if failures:
        logger.error("LINK_BUNDLE confirm NG: failures=" + str(len(failures)))
        raise SystemExit(1)
    logger.ok(
        "LINK_BUNDLE confirm OK: contract=PASS actual="
        + str(actual_summary.get("actual_availability"))
        + " findings="
        + str(actual_summary.get("actual_observation_finding_count"))
        + " fixed_actual_oracle=0"
    )


if __name__ == "__main__":
    main()

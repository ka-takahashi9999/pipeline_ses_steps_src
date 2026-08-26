#!/usr/bin/env python3
"""Focused actual, variable-N, negative, identity, and overlay tests for P4."""

import copy
import sys
import unittest
from pathlib import Path


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
from canonical_overlay import MAIL_MASTER_KEYS, build_canonical_overlay
from link_bundle_adapter import LinkBundleAdapter
from run_link_bundle_offline_replay import CONFIG_PATH, build_link_bundle_results
from run_offline_replay import DEFAULT_INPUT
from run_selective_pipeline_test import _production_artifact_snapshot


FIXTURE_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "fixtures"
    / "redacted"
    / "link_bundle"
    / "drivenx.variable_n.fixture.jsonl.example"
)


class LinkBundleAdapterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.adapter = LinkBundleAdapter.from_file(CONFIG_PATH)
        cls.production_before = _production_artifact_snapshot()
        cls.actual_records = [
            record
            for record in read_jsonl_as_list(str(DEFAULT_INPUT))
            if cls.adapter.matches(record)
        ]
        cls.actual_results = build_link_bundle_results(cls.actual_records)
        cls.actual_summary = cls.actual_results["summary"]
        cls.synthetic_records = read_jsonl_as_list(str(FIXTURE_PATH))
        cls.synthetic_by_id = {
            record["message_id"]: record for record in cls.synthetic_records
        }
        cls.production_after = _production_artifact_snapshot()

    def _fixture(self, suffix: str = "r2-p1") -> dict:
        return copy.deepcopy(self.synthetic_by_id[f"fixture-link-bundle-{suffix}"])

    def _assert_fail_closed(self, mail: dict) -> None:
        result = self.adapter.parse(mail)
        self.assertIn(result.status, {"PARTIAL", "HUMAN_REVIEW"})
        self.assertEqual([], result.items)

    def test_01_config_has_no_fixed_cardinality(self) -> None:
        forbidden = {
            "expected_item_count",
            "expected_resource_count",
            "expected_project_count",
            "expected_link_count",
        }
        self.assertFalse(forbidden & set(self.adapter.config))
        self.assertEqual(
            "CONTAINER_ENUMERATION",
            self.adapter.config["cardinality"]["primary"],
        )

    def test_02_actual_ordered_enumeration_and_roles(self) -> None:
        self.assertEqual(1, len(self.actual_records))
        self.assertEqual(104, self.actual_summary["actual_links"])
        self.assertEqual(104, self.actual_summary["links_classified"])
        self.assertEqual(50, self.actual_summary["resource_items"])
        self.assertEqual(50, self.actual_summary["project_items"])
        self.assertEqual(4, self.actual_summary["non_item_links"])
        self.assertEqual(0, self.actual_summary["unknown_links"])
        roles = [row["role"] for row in self.actual_results["link_enumeration"]]
        self.assertEqual(["ACTION", "ACTION", "RESOURCE_HEADER"], roles[:3])
        self.assertEqual("PROJECT_HEADER", roles[53])

    def test_03_actual_completeness_gate_is_fully_parsed(self) -> None:
        self.assertEqual(1, self.actual_summary["parsed_sources"])
        self.assertEqual(0, self.actual_summary["partial_sources"])
        source = self.actual_results["source_audit"][0]["source"]
        self.assertEqual("PARSED", source["completeness_result"]["status"])
        self.assertTrue(all(source["completeness_result"]["checks"].values()))
        self.assertEqual(
            ["CONTAINER_ENUMERATION", "STRUCTURAL_COMPLETE", "SNAPSHOT_SET"],
            [row["authority"] for row in source["cardinality_evidence"]],
        )

    def test_04_actual_canonical_overlay_and_artifacts_are_item_specific(self) -> None:
        overlays = self.actual_results["derived_mail_master"]
        audits = self.actual_results["audit_items"]
        self.assertEqual(100, len(overlays))
        self.assertTrue(all(set(record) == MAIL_MASTER_KEYS for record in overlays))
        self.assertTrue(all(record["attachments"] == [] for record in overlays))
        self.assertTrue(all(len(record["html_links"]) == 1 for record in overlays))
        self.assertTrue(
            all(
                audit["item_artifacts"][0]["role"] == "PRIMARY"
                and audit["item_artifacts"][0]["artifact_kind"] == "WEB_PAGE"
                and audit["version_scope"] == "MAIL_SNAPSHOT_LIST_ITEM"
                for audit in audits
            )
        )
        self.assertEqual(0, self.actual_summary["cross_item_contamination"])

    def test_05_synthetic_variable_n_uses_one_config(self) -> None:
        observed = {}
        for record in self.synthetic_records:
            result = self.adapter.parse(copy.deepcopy(record))
            counts = result.source["section_counts"]
            observed[(counts["resource"], counts["project"])] = (
                result.status,
                len(result.items),
            )
        self.assertEqual(
            {
                (0, 0): ("PARSED", 0),
                (1, 1): ("PARSED", 2),
                (2, 1): ("PARSED", 3),
                (1, 2): ("PARSED", 3),
                (10, 4): ("PARSED", 14),
                (4, 10): ("PARSED", 14),
            },
            observed,
        )

    def test_06_zero_item_sections_are_structurally_proven(self) -> None:
        result = self.adapter.parse(self._fixture("r0-p0"))
        self.assertEqual("PARSED", result.status)
        self.assertEqual([], result.items)
        self.assertEqual({"resource": 0, "project": 0}, result.source["section_counts"])
        self.assertEqual(0, result.source["completeness_result"]["expected_count"])

    def test_07_missing_headers_fail_closed(self) -> None:
        for header_text in ("ブーストの人材一覧", "ブーストの案件一覧"):
            with self.subTest(header=header_text):
                mail = self._fixture()
                mail["html_links"] = [
                    link for link in mail["html_links"] if link["text"] != header_text
                ]
                self._assert_fail_closed(mail)

    def test_08_reversed_or_duplicate_headers_fail_closed(self) -> None:
        reversed_mail = self._fixture()
        links = reversed_mail["html_links"]
        resource_index = next(
            index for index, link in enumerate(links) if link["text"] == "ブーストの人材一覧"
        )
        project_index = next(
            index for index, link in enumerate(links) if link["text"] == "ブーストの案件一覧"
        )
        links[resource_index], links[project_index] = links[project_index], links[resource_index]
        self._assert_fail_closed(reversed_mail)

        for header_text in ("ブーストの人材一覧", "ブーストの案件一覧"):
            with self.subTest(duplicate=header_text):
                duplicate = self._fixture()
                header = next(
                    link for link in duplicate["html_links"] if link["text"] == header_text
                )
                duplicate["html_links"].append(copy.deepcopy(header))
                self._assert_fail_closed(duplicate)

    def test_09_duplicate_item_locator_fails_closed(self) -> None:
        mail = self._fixture()
        item = next(
            link for link in mail["html_links"] if "/boost/talents/" in link["href"]
        )
        project_header_index = next(
            index
            for index, link in enumerate(mail["html_links"])
            if link["text"] == "ブーストの案件一覧"
        )
        mail["html_links"].insert(project_header_index, copy.deepcopy(item))
        result = self.adapter.parse(mail)
        self.assertEqual([], result.items)
        self.assertTrue(any("duplicate_item_locator" in reason for reason in result.reasons))

    def test_10_unknown_and_empty_title_fail_closed(self) -> None:
        unknown = self._fixture()
        unknown["html_links"].insert(
            4,
            {
                "text": "判定不能",
                "href": "https://unknown.example.invalid/value",
                "source": "text/html",
            },
        )
        unknown_result = self.adapter.parse(unknown)
        self.assertEqual([], unknown_result.items)
        self.assertEqual(1, unknown_result.source["link_role_counts"]["UNKNOWN"])

        empty = self._fixture()
        item = next(
            link for link in empty["html_links"] if "/boost/talents/" in link["href"]
        )
        item["text"] = "  \n"
        empty_result = self.adapter.parse(empty)
        self.assertEqual([], empty_result.items)
        self.assertTrue(any("empty_item_title" in reason for reason in empty_result.reasons))

    def test_11_action_inside_section_is_classified_but_not_emitted(self) -> None:
        mail = self._fixture()
        action = copy.deepcopy(mail["html_links"][0])
        mail["html_links"].insert(4, action)
        result = self.adapter.parse(mail)
        self.assertEqual("PARSED", result.status)
        self.assertEqual(3, len(result.items))
        self.assertEqual(3, result.source["link_role_counts"]["ACTION"])
        self.assertTrue(
            all(item["html_links"][0]["href"] != action["href"] for item in result.items)
        )

    def test_12_cross_section_move_fails_closed(self) -> None:
        mail = self._fixture()
        resource_index = next(
            index
            for index, link in enumerate(mail["html_links"])
            if "/boost/talents/" in link["href"]
        )
        moved = mail["html_links"].pop(resource_index)
        mail["html_links"].append(moved)
        result = self.adapter.parse(mail)
        self.assertEqual([], result.items)
        self.assertTrue(any("section_mismatch" in reason for reason in result.reasons))

    def test_13_incomplete_snapshot_and_malformed_locator_fail_closed(self) -> None:
        incomplete = self._fixture()
        incomplete["html_links_snapshot_complete"] = False
        result = self.adapter.parse(incomplete)
        self.assertEqual("PARTIAL", result.status)
        self.assertEqual([], result.items)
        self.assertFalse(
            result.source["completeness_result"]["checks"]["source_acquisition_complete"]
        )

        malformed = self._fixture()
        item = next(
            link for link in malformed["html_links"] if "/boost/talents/" in link["href"]
        )
        item["href"] = "https://cho-tatsu.com/boost/talents/"
        malformed_result = self.adapter.parse(malformed)
        self.assertEqual([], malformed_result.items)
        self.assertEqual(1, malformed_result.source["link_role_counts"]["UNKNOWN"])

    def test_14_identity_and_version_follow_locator_contract(self) -> None:
        mail = self._fixture("r1-p1")
        original = self.adapter.parse(copy.deepcopy(mail)).items[0]
        same = self.adapter.parse(copy.deepcopy(mail)).items[0]
        self.assertEqual(original["logical_item_id"], same["logical_item_id"])
        self.assertEqual(original["derived_item_id"], same["derived_item_id"])

        title_changed = copy.deepcopy(mail)
        title_changed["html_links"][3]["text"] += "・更新"
        title_version = self.adapter.parse(title_changed).items[0]
        self.assertEqual(original["logical_item_id"], title_version["logical_item_id"])
        self.assertNotEqual(original["derived_item_id"], title_version["derived_item_id"])

        locator_changed = copy.deepcopy(mail)
        locator_changed["html_links"][3]["href"] += "-new"
        locator_version = self.adapter.parse(locator_changed).items[0]
        self.assertNotEqual(original["logical_item_id"], locator_version["logical_item_id"])

    def test_15_order_changes_container_but_not_item_identity_or_version(self) -> None:
        mail = self._fixture("r10-p4")
        original = self.adapter.parse(copy.deepcopy(mail))
        reordered = copy.deepcopy(mail)
        resource_items = reordered["html_links"][3:13]
        reordered["html_links"][3:13] = list(reversed(resource_items))
        changed = self.adapter.parse(reordered)
        original_by_href = {
            item["html_links"][0]["href"]: (
                item["logical_item_id"], item["version_fingerprint"]
            )
            for item in original.items
        }
        changed_by_href = {
            item["html_links"][0]["href"]: (
                item["logical_item_id"], item["version_fingerprint"]
            )
            for item in changed.items
        }
        self.assertEqual(original_by_href, changed_by_href)
        self.assertNotEqual(
            original.source["source_fingerprint"], changed.source["source_fingerprint"]
        )

    def test_16_actual_01_4_and_02_1_split_without_item_type_signal(self) -> None:
        self.assertEqual(100, self.actual_summary["cleanup_output"])
        self.assertEqual(100, self.actual_summary["cleanup_nonempty"])
        self.assertEqual(50, self.actual_summary["resource_classified_correct"])
        self.assertEqual(50, self.actual_summary["project_classified_correct"])
        self.assertEqual(0, self.actual_summary["ambiguous_output"])
        self.assertEqual(0, self.actual_summary["unknown_output"])
        self.assertTrue(
            all(
                "section_type" not in record and "item_type" not in record
                for record in self.actual_results["derived_mail_master"]
            )
        )

    def test_17_actual_identity_is_deterministic_and_collision_free(self) -> None:
        self.assertTrue(self.actual_summary["derived_id_deterministic"])
        self.assertEqual(100, self.actual_summary["logical_distinct"])
        self.assertEqual(100, self.actual_summary["derived_distinct"])
        self.assertEqual(0, self.actual_summary["duplicate_item_locators"])

    def test_18_replay_is_offline_and_production_read_only(self) -> None:
        self.assertEqual(0, self.actual_summary["llm_api_calls"])
        self.assertEqual(0, self.actual_summary["external_url_calls"])
        self.assertEqual(0, self.actual_summary["production_write"])
        self.assertEqual(self.production_before, self.production_after)


if __name__ == "__main__":
    unittest.main()

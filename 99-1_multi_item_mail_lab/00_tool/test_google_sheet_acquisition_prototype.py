#!/usr/bin/env python3
"""Focused offline tests for the one-Sheet acquisition prototype."""

import copy
import io
import sys
import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook
from openpyxl.formatting.rule import CellIsRule
from openpyxl.styles import PatternFill


sys.dont_write_bytecode = True
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
ACQUISITION_DIR = STEP_DIR / "00_tool" / "acquisition"
for import_path in (PROJECT_ROOT, ACQUISITION_DIR, STEP_DIR / "00_tool"):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.json_utils import write_jsonl
from google_sheet_acquisition_contract import (
    MANIFEST_VERSION,
    SNAPSHOT_ENTRY_VERSION,
    bind_profile_digest,
    calculate_ordered_snapshot_set_digest,
    digest_bytes,
    finalize_manifest,
    offline_negative_proofs,
    validate_attempt_plan,
    validate_manifest,
    validate_profile_registry,
)
from run_google_sheet_acquisition_prototype import (
    _load_profile_registry,
    build_attempt_plan,
    observe_workbook,
    select_representative_locator,
)


class GoogleSheetAcquisitionPrototypeTest(unittest.TestCase):
    def setUp(self):
        registry = _load_profile_registry()
        registry["profiles"][0]["presentation_policy"] = "NOT_USED"
        self.registry = bind_profile_digest(registry)
        self.selected = {
            "spreadsheet_key": "syntheticSheetKey",
            "gid": "123",
            "normalized_locator": "https://docs.google.com/spreadsheets/d/syntheticSheetKey/edit#gid=123",
            "evidence_message_ids": ["synthetic-message"],
            "evidence_count": 1,
            "link_text": "その他人材情報一覧",
        }
        self.plan = build_attempt_plan(
            self.registry, self.selected, "2026-01-01T00:00:00Z"
        )
        self.raw = b"offline-snapshot"
        self.entry = {
            "schema_version": SNAPSHOT_ENTRY_VERSION,
            "snapshot_entry_id": "snapshot-entry:synthetic",
            "planned_container_id": self.plan["planned_containers"][0][
                "planned_container_id"
            ],
            "relative_path": "snapshot/google_sheet.xlsx",
            "media_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "byte_count": len(self.raw),
            "entry_raw_digest": digest_bytes(self.raw),
            "entry_raw_digest_kind": "ENTRY_RAW_DIGEST",
            "acquired_at": "2026-01-01T00:00:01Z",
        }
        self.manifest = finalize_manifest(
            {
                "schema_version": MANIFEST_VERSION,
                "manifest_id": "manifest:synthetic",
                "provider": "GOOGLE_SHEETS",
                "representative_source": self.selected,
                "acquisition_method": "PUBLIC_SHARED_URL_XLSX_EXPORT",
                "attempt_id": self.plan["attempt_id"],
                "profile_ref": self.plan["profile_ref"],
                "resolved_scope_digest": self.plan["resolved_scope_digest"],
                "planned_container_set_digest": self.plan[
                    "planned_container_set_digest"
                ],
                "snapshot_entries": [self.entry],
                "ordered_snapshot_set_digest": calculate_ordered_snapshot_set_digest(
                    [self.entry]
                ),
                "presentation_policy": "NOT_USED",
                "version_authority": {
                    "version_kind": "SYNTHETIC_STRONG_REVISION",
                    "scope": "WORKBOOK_WIDE",
                    "strength": "STRONG",
                    "pre_post_stable": True,
                },
                "completeness_evidence": {
                    "resolved_scope_digest": self.plan["resolved_scope_digest"],
                    "tab_inventory_complete": True,
                    "range_complete": True,
                    "required_container_count": 1,
                    "captured_required_container_count": 1,
                },
                "observation": {},
                "access_status": "SUCCESS",
                "acquisition_status": "VERIFIED_COMPLETE",
                "review_status": "NONE",
                "attempt_state": "COMMITTED",
                "candidate_emission": 0,
                "eligible": 0,
                "auto_union": False,
                "production_write": 0,
                "acquisition_completed_at": "2026-01-01T00:00:02Z",
            }
        )
        self.raw_entries = {self.entry["snapshot_entry_id"]: self.raw}

    def test_01_profile_plan_snapshot_manifest_contract(self):
        self.assertEqual([], validate_profile_registry(self.registry))
        self.assertEqual([], validate_attempt_plan(self.plan))
        result = validate_manifest(
            self.manifest, self.registry, self.plan, self.raw_entries
        )
        self.assertTrue(result["valid"])
        self.assertEqual("VERIFIED_COMPLETE", result["acquisition_status"])
        self.assertEqual(0, result["eligible"])
        self.assertFalse(result["auto_union"])
        self.assertEqual(0, result["candidate_emission"])

    def test_02_all_nine_negative_proofs_fail_closed(self):
        results = offline_negative_proofs(
            self.manifest, self.registry, self.plan, self.raw_entries
        )
        expected = {
            "profile_digest_mismatch": ("UNVERIFIED", "profile_digest_mismatch"),
            "planned_scope_mismatch": ("PARTIAL", "planned_scope_mismatch"),
            "required_container_missing": ("PARTIAL", "required_container_missing"),
            "strong_version_unavailable": (
                "UNVERIFIED",
                "strong_version_unavailable",
            ),
            "revision_drift": ("SNAPSHOT_UNSTABLE", "revision_drift"),
            "range_gap": ("PARTIAL", "range_gap"),
            "digest_mismatch": ("INCOMPLETE", "snapshot_entry_digest_mismatch"),
            "presentation_unresolved": ("UNVERIFIED", "presentation_unresolved"),
            "attempt_uncommitted": ("INCOMPLETE", "attempt_uncommitted"),
        }
        self.assertEqual(set(expected), {case["name"] for case in results})
        for case in results:
            with self.subTest(case=case["name"]):
                status, reason = expected[case["name"]]
                self.assertEqual(status, case["result"]["acquisition_status"])
                self.assertIn(reason, case["result"]["reasons"])
                self.assertEqual(0, case["result"]["eligible"])
        presentation = next(
            case for case in results if case["name"] == "presentation_unresolved"
        )
        self.assertEqual("HUMAN_REVIEW", presentation["result"]["review_status"])

    def test_03_workbook_observation_has_inventory_bounds_and_presentation(self):
        workbook = Workbook()
        active = workbook.active
        active.title = "人材一覧"
        active["A1"] = "氏名"
        active["A2"] = "匿名A"
        active["B1"] = "計算"
        active["B2"] = "=1+1"
        active["A1"].fill = PatternFill(fill_type="solid", fgColor="FFFF00")
        active.row_dimensions[2].hidden = True
        active.column_dimensions["C"].hidden = True
        active.conditional_formatting.add(
            "A1:A2",
            CellIsRule(
                operator="equal",
                formula=['"匿名A"'],
                fill=PatternFill(fill_type="solid", fgColor="00FF00"),
            ),
        )
        hidden = workbook.create_sheet("設定")
        hidden.sheet_state = "hidden"
        hidden["A1"] = "support"
        output = io.BytesIO()
        workbook.save(output)
        observed = observe_workbook(output.getvalue())
        self.assertEqual("AVAILABLE", observed["workbook_tab_inventory"])
        self.assertEqual(2, observed["tab_count"])
        self.assertEqual([0, 1], [tab["tab_order"] for tab in observed["tabs"]])
        self.assertEqual(["VISIBLE", "HIDDEN"], [tab["visibility"] for tab in observed["tabs"]])
        self.assertEqual("AVAILABLE", observed["range_bounds"])
        presentation = observed["presentation_metadata"]
        self.assertEqual("PARTIAL", presentation["overall_availability"])
        self.assertEqual("UNAVAILABLE", presentation["effective_format"])
        self.assertEqual("AVAILABLE", presentation["static_user_format"])
        self.assertEqual("AVAILABLE", presentation["conditional_formatting"])
        self.assertEqual("AVAILABLE", presentation["hidden_rows_columns"])
        self.assertEqual("AVAILABLE", presentation["formula_metadata"])

    def test_04_saved_gmail_selection_is_one_normalized_locator(self):
        records = [
            {
                "message_id": "m1",
                "from": "Sales <sales@tanapism.co.jp>",
                "html_links": [
                    {
                        "text": "★☆その他人材情報一覧☆★",
                        "href": "https://docs.google.com/spreadsheets/d/key123/edit?pli=1&tracking=discard#gid=456",
                    }
                ],
            },
            {
                "message_id": "m2",
                "from": "Sales <sales@tanapism.co.jp>",
                "html_links": [
                    {
                        "text": "★☆その他人材情報一覧☆★",
                        "href": "https://docs.google.com/spreadsheets/d/key123/edit?usp=sharing&gid=456",
                    }
                ],
            },
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "gmail.jsonl"
            write_jsonl(str(path), records)
            selected = select_representative_locator(
                path, _load_profile_registry()["profiles"][0]
            )
        self.assertEqual("key123", selected["spreadsheet_key"])
        self.assertEqual("456", selected["gid"])
        self.assertEqual(
            "https://docs.google.com/spreadsheets/d/key123/edit#gid=456",
            selected["normalized_locator"],
        )
        self.assertEqual(2, selected["evidence_count"])


if __name__ == "__main__":
    unittest.main()

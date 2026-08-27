#!/usr/bin/env python3
"""Focused P7 tests for safe XLSX variable-record enumeration."""

import copy
import io
import json
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET


sys.dont_write_bytecode = True
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
for import_path in (
    PROJECT_ROOT,
    STEP_DIR / "00_tool",
    STEP_DIR / "00_tool" / "adapters" / "spreadsheet",
    STEP_DIR / "00_tool" / "adapters" / "attachment_list",
    STEP_DIR / "00_tool" / "canonicalize",
    STEP_DIR / "00_tool" / "core",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from canonical_overlay import MAIL_MASTER_KEYS, build_canonical_overlay
from run_spreadsheet_offline_replay import build_spreadsheet_results
from spreadsheet_fixture_source import (
    FIXED_TIMESTAMP,
    build_fixture_records,
    build_spreadsheet_fixture,
    build_workbook_bytes,
    replace_workbook_payload,
    rewrite_package_member,
    workbook_payload,
)
from spreadsheet_parser import MAIN_NS, PACKAGE_REL_NS, SpreadsheetParser


CONFIG_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "companies"
    / "sakya_spreadsheet.config.json.example"
)
FIXTURE_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "fixtures"
    / "redacted"
    / "spreadsheet"
    / "sakya.variable_n.fixture.jsonl.example"
)


def _package_parts(payload):
    with zipfile.ZipFile(io.BytesIO(payload), "r") as package:
        return [(info.filename, package.read(info)) for info in package.infolist()]


def _rebuild_package(payload, replacements=None, remove=(), additions=()):
    replacements = replacements or {}
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", allowZip64=False) as package:
        for name, value in _package_parts(payload):
            if name in remove:
                continue
            info = zipfile.ZipInfo(name, FIXED_TIMESTAMP)
            info.compress_type = zipfile.ZIP_DEFLATED
            package.writestr(info, replacements.get(name, value))
        for name, value in additions:
            info = zipfile.ZipInfo(name, FIXED_TIMESTAMP)
            info.compress_type = zipfile.ZIP_DEFLATED
            package.writestr(info, value)
    return output.getvalue()


class SpreadsheetAdapterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        cls.parser = SpreadsheetParser(cls.config)
        cls.fixtures = build_fixture_records(FIXTURE_PATH, cls.config)
        cls.by_count = {
            fixture["spreadsheet_declared_item_evidence"]["count"]: fixture
            for fixture in cls.fixtures
        }

    def assertFailClosed(self, result):
        self.assertNotEqual("PARSED", result.status)
        self.assertEqual(0, result.eligible_item_candidate_count)
        self.assertFalse(result.source["auto_union_eligible"])

    def _project(self, suffix="X"):
        return {
            "メインスキル": "Java",
            "単金": "70万",
            "作業場所": "東京・リモート",
            "面談回数": "1回",
            "担当者": "担当" + suffix,
            "新規": "新規",
            "案件概要": "匿名案件" + suffix + " Java開発",
        }

    def _custom_fixture(self, projects, sheets, message_id="synthetic-sakya-custom"):
        return build_spreadsheet_fixture(
            {
                "message_id": message_id,
                "count": len(projects),
                "projects": projects,
                "sheet_definitions": sheets,
            },
            self.config,
        )

    def test_01_variable_n_same_parser_and_config(self):
        self.assertEqual([0, 1, 2, 4, 10], sorted(self.by_count))
        for count, fixture in sorted(self.by_count.items()):
            with self.subTest(count=count):
                result = self.parser.parse(copy.deepcopy(fixture))
                self.assertEqual("PARSED", result.status)
                self.assertEqual(count, result.eligible_item_candidate_count)
                self.assertEqual(count, len(result.items))
                self.assertEqual("VERIFIED_COMPLETE", result.source["source_acquisition_status"])
                self.assertEqual("COMPLETE", result.workbook["technical_workbook_status"])

    def test_02_supporting_and_derived_view_do_not_create_items(self):
        result = self.parser.parse(copy.deepcopy(self.by_count[4]))
        self.assertEqual("PARSED", result.status)
        self.assertEqual(
            {"AUTHORITATIVE": 1, "DERIVED_VIEW": 1, "SUPPORTING": 1, "UNKNOWN": 0},
            result.workbook["sheet_role_counts"],
        )
        self.assertEqual(6, len(result.record_occurrences))
        self.assertEqual(4, len(result.items))
        self.assertEqual(2, result.workbook["reconciliation"]["duplicate_occurrence_count"])

    def test_03_safe_duplicate_across_authoritative_sheets_reconciles(self):
        project = self._project("A")
        fixture = self._custom_fixture(
            [project],
            [
                {"name": "案件一覧", "role": "AUTHORITATIVE", "records": [project]},
                {"name": "案件一覧2", "role": "AUTHORITATIVE", "records": [project]},
            ],
        )
        result = self.parser.parse(fixture)
        self.assertEqual("PARSED", result.status)
        self.assertEqual(2, len(result.record_occurrences))
        self.assertEqual(1, len(result.items))
        self.assertEqual(1, result.workbook["reconciliation"]["duplicate_group_count"])

    def test_04_ambiguous_duplicate_is_human_review(self):
        first = self._project("A")
        second = copy.deepcopy(first)
        second["単金"] = "75万"
        fixture = self._custom_fixture(
            [first, second],
            [{"name": "案件一覧", "role": "AUTHORITATIVE", "records": [first, second]}],
        )
        result = self.parser.parse(fixture)
        self.assertEqual("HUMAN_REVIEW", result.status)
        self.assertEqual(1, result.workbook["reconciliation"]["ambiguous_group_count"])
        self.assertFailClosed(result)

    def test_05_middle_deletion_insertion_and_order_mutation_fail_closed(self):
        original = copy.deepcopy(self.by_count[4])
        definition = json.loads(FIXTURE_PATH.read_text(encoding="utf-8").splitlines()[3])
        projects = definition["projects"]
        cases = {
            "middle_deletion": projects[:2] + projects[3:],
            "unexpected_insertion": projects[:2] + [self._project("INSERT")] + projects[2:],
            "order_mutation": [projects[0], projects[2], projects[1], projects[3]],
        }
        for name, records in cases.items():
            with self.subTest(name=name):
                payload, _ = build_workbook_bytes(
                    [
                        {"name": "案件一覧", "role": "AUTHORITATIVE", "records": records},
                        {"name": "新着案件", "role": "DERIVED_VIEW", "records": [projects[1], projects[3]]},
                        {"name": "連絡先", "role": "SUPPORTING", "records": []},
                    ],
                    self.config,
                )
                mutated = replace_workbook_payload(original, payload)
                result = self.parser.parse(mutated)
                self.assertIn("sheet_or_record_ordered_sequence_mismatch", result.reasons)
                self.assertFailClosed(result)

    def test_06_required_field_and_blank_column_fail_closed(self):
        incomplete = self._project("MISSING")
        incomplete["メインスキル"] = ""
        missing_result = self.parser.parse(
            self._custom_fixture(
                [incomplete],
                [{"name": "案件一覧", "role": "AUTHORITATIVE", "records": [incomplete]}],
                "synthetic-required-missing",
            )
        )
        self.assertIn("required_field_missing:案件一覧:B:メインスキル", missing_result.reasons)
        self.assertFailClosed(missing_result)

        project = self._project("A")
        blank_fixture = self._custom_fixture(
            [project],
            [{"name": "案件一覧", "role": "AUTHORITATIVE", "records": [project, {}]}],
            "synthetic-blank-column",
        )
        blank_result = self.parser.parse(blank_fixture)
        self.assertTrue(any("blank_record_column_inside_boundary" in reason for reason in blank_result.reasons))
        self.assertFailClosed(blank_result)

    def test_07_unknown_and_hidden_unknown_sheet_fail_closed(self):
        for hidden in (False, True):
            with self.subTest(hidden=hidden):
                fixture = copy.deepcopy(self.by_count[1])
                payload = workbook_payload(fixture)
                root = ET.fromstring(dict(_package_parts(payload))["xl/workbook.xml"])
                sheet = root.find(".//{%s}sheet" % MAIN_NS)
                sheet.set("name", "未知の追加sheet")
                if hidden:
                    sheet.set("state", "veryHidden")
                workbook_xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
                payload = rewrite_package_member(payload, "xl/workbook.xml", workbook_xml)
                result = self.parser.parse(replace_workbook_payload(fixture, payload))
                self.assertEqual("HUMAN_REVIEW", result.status)
                self.assertEqual(1, result.workbook["sheet_role_counts"]["UNKNOWN"])
                self.assertFailClosed(result)

    def test_08_corrupt_zip_truncated_xml_and_missing_parts_fail_closed(self):
        fixture = copy.deepcopy(self.by_count[1])
        payload = workbook_payload(fixture)
        cases = {
            "zip_corrupt": payload[:-20],
            "worksheet_truncated": rewrite_package_member(payload, "xl/worksheets/sheet1.xml", b"<worksheet"),
            "workbook_missing": _rebuild_package(payload, remove={"xl/workbook.xml"}),
            "relationship_missing": _rebuild_package(payload, remove={"xl/_rels/workbook.xml.rels"}),
        }
        for name, value in cases.items():
            with self.subTest(name=name):
                result = self.parser.parse(replace_workbook_payload(fixture, value))
                self.assertFailClosed(result)

    def test_09_xml_dtd_entity_and_malformed_xml_are_rejected(self):
        fixture = copy.deepcopy(self.by_count[1])
        payload = workbook_payload(fixture)
        cases = (
            b'<!DOCTYPE x [<!ENTITY e SYSTEM "file:///etc/passwd">]><x>&e;</x>',
            b"<worksheet><broken></worksheet>",
        )
        for xml_value in cases:
            with self.subTest(xml=xml_value[:20]):
                mutated = rewrite_package_member(payload, "xl/worksheets/sheet1.xml", xml_value)
                result = self.parser.parse(replace_workbook_payload(fixture, mutated))
                self.assertFailClosed(result)

    def test_10_path_traversal_duplicate_members_and_limits_are_rejected(self):
        fixture = copy.deepcopy(self.by_count[1])
        payload = workbook_payload(fixture)
        traversal = _rebuild_package(payload, additions=[("../escape.xml", b"<x/>")])
        duplicate = _rebuild_package(payload, additions=[("xl/workbook.xml", b"<x/>")])
        for name, value in (("traversal", traversal), ("duplicate", duplicate)):
            with self.subTest(name=name):
                result = self.parser.parse(replace_workbook_payload(fixture, value))
                self.assertEqual("UNSUPPORTED", result.status)
                self.assertFailClosed(result)

        limited = copy.deepcopy(self.config)
        limited["limits"]["max_member_count"] = 3
        result = SpreadsheetParser(limited).parse(fixture)
        self.assertEqual("UNSUPPORTED", result.status)
        self.assertFailClosed(result)

        expanded_limited = copy.deepcopy(self.config)
        expanded_limited["limits"]["max_total_expanded_bytes"] = 10
        expanded_result = SpreadsheetParser(expanded_limited).parse(fixture)
        self.assertEqual("UNSUPPORTED", expanded_result.status)
        self.assertFailClosed(expanded_result)

    def test_11_external_relationship_and_relationship_cycle_are_rejected(self):
        fixture = copy.deepcopy(self.by_count[1])
        payload = workbook_payload(fixture)
        rel_name = "xl/_rels/workbook.xml.rels"
        rel_root = ET.fromstring(dict(_package_parts(payload))[rel_name])
        ET.SubElement(
            rel_root,
            "{%s}Relationship" % PACKAGE_REL_NS,
            {
                "Id": "rIdExternal",
                "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/externalLink",
                "Target": "https://example.invalid/book.xlsx",
                "TargetMode": "External",
            },
        )
        external = rewrite_package_member(
            payload, rel_name, ET.tostring(rel_root, encoding="utf-8", xml_declaration=True)
        )
        external_result = self.parser.parse(replace_workbook_payload(fixture, external))
        self.assertEqual("UNSUPPORTED", external_result.status)
        self.assertFailClosed(external_result)

        cycle_rels = ET.Element("{%s}Relationships" % PACKAGE_REL_NS)
        ET.SubElement(
            cycle_rels,
            "{%s}Relationship" % PACKAGE_REL_NS,
            {
                "Id": "rIdCycle",
                "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument",
                "Target": "../workbook.xml",
            },
        )
        cycle = _rebuild_package(
            payload,
            additions=[
                (
                    "xl/worksheets/_rels/sheet1.xml.rels",
                    ET.tostring(cycle_rels, encoding="utf-8", xml_declaration=True),
                )
            ],
        )
        cycle_result = self.parser.parse(replace_workbook_payload(fixture, cycle))
        self.assertEqual("UNSUPPORTED", cycle_result.status)
        self.assertFailClosed(cycle_result)

    def test_12_formula_dependent_field_is_unsupported_without_evaluation(self):
        fixture = copy.deepcopy(self.by_count[1])
        payload = workbook_payload(fixture)
        sheet_name = "xl/worksheets/sheet1.xml"
        root = ET.fromstring(dict(_package_parts(payload))[sheet_name])
        cell = next(cell for cell in root.findall(".//{%s}c" % MAIN_NS) if cell.get("r") == "B7")
        ET.SubElement(cell, "{%s}f" % MAIN_NS).text = "[1]External!A1"
        mutated = rewrite_package_member(
            payload, sheet_name, ET.tostring(root, encoding="utf-8", xml_declaration=True)
        )
        result = self.parser.parse(replace_workbook_payload(fixture, mutated))
        self.assertEqual("UNSUPPORTED", result.status)
        self.assertEqual(0, result.workbook["formula_evaluation_count"])
        self.assertEqual(0, result.workbook["external_resolution_count"])
        self.assertFailClosed(result)

    def test_13_macro_parts_and_xlsm_are_unsupported_without_execution(self):
        fixture = copy.deepcopy(self.by_count[1])
        payload = workbook_payload(fixture)
        macro_payload = _rebuild_package(payload, additions=[("xl/vbaProject.bin", b"macro")])
        macro_result = self.parser.parse(replace_workbook_payload(fixture, macro_payload))
        self.assertEqual("UNSUPPORTED", macro_result.status)
        self.assertFailClosed(macro_result)

        xlsm = copy.deepcopy(fixture)
        xlsm["attachments"][0]["filename"] = "サクヤ営業中_案件000000.xlsm"
        xlsm_result = self.parser.parse(xlsm)
        self.assertEqual("UNSUPPORTED", xlsm_result.status)
        self.assertFailClosed(xlsm_result)

    def test_14_source_acquisition_and_workbook_completeness_are_independent(self):
        fixture = copy.deepcopy(self.by_count[2])
        fixture.pop("attachment_acquisition_manifest")
        result = self.parser.parse(fixture)
        self.assertEqual("UNVERIFIED", result.source["source_acquisition_status"])
        self.assertEqual("COMPLETE", result.workbook["technical_workbook_status"])
        self.assertEqual(2, len(result.technical_items))
        self.assertFailClosed(result)

    def test_15_identity_is_provisional_deterministic_and_canonical_schema_safe(self):
        first = self.parser.parse(copy.deepcopy(self.by_count[2]))
        second = self.parser.parse(copy.deepcopy(self.by_count[2]))
        self.assertEqual(first, second)
        self.assertTrue(
            all(
                item["identity_evidence"]["status"] == "PROVISIONAL"
                for item in first.items
            )
        )
        self.assertTrue(
            all(
                item["identity_evidence"]["strategy"]
                == "SOURCE_CONFIGURED_CANONICAL_FIELD_FINGERPRINT"
                for item in first.items
            )
        )
        overlays = [build_canonical_overlay(self.by_count[2], item) for item in first.items]
        self.assertTrue(all(set(overlay) == MAIL_MASTER_KEYS for overlay in overlays))
        self.assertTrue(all(overlay["attachments"] == [] for overlay in overlays))

        explicit_config = copy.deepcopy(self.config)
        explicit_config["record_layout"]["field_rows"]["案件ID"] = 8
        project = self._project("EXPLICIT")
        project["案件ID"] = "source-project-001"
        explicit_fixture = build_spreadsheet_fixture(
            {
                "message_id": "synthetic-explicit-id",
                "count": 1,
                "projects": [project],
                "sheet_definitions": [
                    {"name": "案件一覧", "role": "AUTHORITATIVE", "records": [project]},
                    {"name": "案件一覧2", "role": "AUTHORITATIVE", "records": [project]},
                ],
            },
            explicit_config,
        )
        explicit_result = SpreadsheetParser(explicit_config).parse(explicit_fixture)
        self.assertEqual("PARSED", explicit_result.status)
        self.assertEqual(
            "SOURCE_OWNED_EXPLICIT_ID",
            explicit_result.items[0]["identity_evidence"]["strategy"],
        )

    def test_16_actual_observation_three_state_boundary_has_no_fixed_oracle(self):
        matching = copy.deepcopy(self.by_count[1])
        matching.pop("attachment_acquisition_manifest")
        observed = build_spreadsheet_results([matching])["summary"]
        self.assertEqual("OBSERVATION", observed["actual_availability"])
        self.assertEqual(0, observed["actual_runtime_fixed_oracle"])
        self.assertEqual(0, observed["production_write"])

        unselected = copy.deepcopy(matching)
        unselected["from"] = "redacted@example.invalid"
        unavailable = build_spreadsheet_results([unselected])["summary"]
        self.assertEqual("DATA_UNAVAILABLE", unavailable["actual_availability"])

        with tempfile.TemporaryDirectory() as temp_dir:
            missing_path = Path(temp_dir) / "missing.jsonl"
            missing = build_spreadsheet_results(input_path=missing_path)["summary"]
            malformed_path = Path(temp_dir) / "malformed.jsonl"
            malformed_path.write_text("{invalid-jsonl}\n", encoding="utf-8")
            malformed = build_spreadsheet_results(input_path=malformed_path)["summary"]
        self.assertEqual("OBSERVATION_UNAVAILABLE", missing["actual_availability"])
        self.assertEqual("OBSERVATION_UNAVAILABLE", malformed["actual_availability"])


if __name__ == "__main__":
    unittest.main()

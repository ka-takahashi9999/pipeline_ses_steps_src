#!/usr/bin/env python3
"""Strict OOXML namespaceだけを変換するXLSX fallbackのFocused Test。"""

import hashlib
import importlib.util
import sys
import unittest
import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import openpyxl


TOOL_DIR = Path(__file__).resolve().parent
ROOT = TOOL_DIR.parents[1]
sys.path.insert(0, str(ROOT))


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "fetch_strict_ooxml_test", TOOL_DIR / "fetch_skillsheets_text.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fetch = _load_module()


def _transitional_workbook_bytes():
    workbook = openpyxl.Workbook()
    workbook.active.title = "normal"
    for column, value in enumerate(
        ("職務経歴書", "Java", "PHP", "AWS", "Python3", "Typescript", "設計", "開発"),
        start=1,
    ):
        workbook.active.cell(row=column, column=1, value=value)
    output = BytesIO()
    workbook.save(output)
    return output.getvalue()


def _strict_workbook_bytes():
    data = _transitional_workbook_bytes()
    replacements = tuple(
        (transitional, strict)
        for strict, transitional in fetch.STRICT_NAMESPACE_REPLACEMENTS
    )
    output = BytesIO()
    with zipfile.ZipFile(BytesIO(data), "r") as source:
        with zipfile.ZipFile(output, "w") as destination:
            destination.comment = source.comment
            for entry in source.infolist():
                member = source.read(entry)
                if entry.filename.endswith((".xml", ".rels")):
                    for transitional, strict in replacements:
                        member = member.replace(transitional, strict)
                destination.writestr(entry, member)
    return output.getvalue()


def _replace_zip_member(data, member_name, replacement):
    output = BytesIO()
    with zipfile.ZipFile(BytesIO(data), "r") as source:
        with zipfile.ZipFile(output, "w") as destination:
            destination.comment = source.comment
            for entry in source.infolist():
                member = replacement if entry.filename == member_name else source.read(entry)
                destination.writestr(entry, member)
    return output.getvalue()


class StrictOoxmlWorkbookFallbackTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = _strict_workbook_bytes()

    def test_real_strict_workbook_extracts_sheet_and_cells(self):
        original_sha256 = hashlib.sha256(self.data).hexdigest()
        text = fetch.extract_text_from_excel(self.data, "strict.xlsx")
        self.assertEqual(text.count("=== シート:"), 1)
        for marker in ("Java", "PHP", "AWS", "Python3", "Typescript"):
            self.assertIn(marker, text)
        self.assertIsNone(fetch.validate_skillsheet_text(text))
        self.assertEqual(hashlib.sha256(self.data).hexdigest(), original_sha256)

        converted = fetch._normalize_strict_ooxml_namespaces(self.data)
        self.assertIsNotNone(converted)
        workbook = openpyxl.load_workbook(
            BytesIO(converted), data_only=True, read_only=True
        )
        self.assertEqual(workbook.sheetnames, ["normal"])
        self.assertEqual(
            sum(
                1
                for row in workbook["normal"].iter_rows(values_only=True)
                for value in row
                if value is not None and str(value).strip()
            ),
            8,
        )

    def test_transitional_workbook_uses_existing_path_unchanged(self):
        data = _transitional_workbook_bytes()
        with patch.object(
            fetch,
            "_normalize_strict_ooxml_namespaces",
            wraps=fetch._normalize_strict_ooxml_namespaces,
        ) as strict_fallback, patch.object(
            openpyxl, "load_workbook", wraps=openpyxl.load_workbook
        ) as load_workbook:
            text = fetch.extract_text_from_excel(data, "normal.xlsx")
        strict_fallback.assert_not_called()
        self.assertEqual(load_workbook.call_count, 1)
        self.assertIn("Java", text)
        self.assertIn("設計", text)

    def test_original_archive_members_are_not_modified(self):
        before_sha256 = hashlib.sha256(self.data).hexdigest()
        converted = fetch._normalize_strict_ooxml_namespaces(self.data)
        self.assertIsNotNone(converted)
        self.assertEqual(hashlib.sha256(self.data).hexdigest(), before_sha256)
        self.assertNotEqual(converted, self.data)
        with zipfile.ZipFile(BytesIO(self.data), "r") as before, zipfile.ZipFile(
            BytesIO(converted), "r"
        ) as after:
            self.assertEqual(before.namelist(), after.namelist())

    def test_non_strict_broken_xlsx_is_not_converted(self):
        transitional = _transitional_workbook_bytes()
        broken = _replace_zip_member(
            transitional, "xl/workbook.xml", b"<broken-workbook>"
        )
        self.assertIsNone(fetch._normalize_strict_ooxml_namespaces(broken))
        with patch.object(
            fetch,
            "_normalize_strict_ooxml_namespaces",
            wraps=fetch._normalize_strict_ooxml_namespaces,
        ) as strict_fallback:
            with self.assertRaises(Exception):
                fetch.extract_text_from_excel(broken, "broken.xlsx")
        strict_fallback.assert_called_once()

    def test_strict_structure_damage_is_rejected(self):
        cases = {
            "broken_zip": b"PK\x03\x04broken",
            "missing_workbook": _replace_zip_member(
                self.data, "xl/workbook.xml", b"<workbook/>"
            ),
            "broken_worksheet": _replace_zip_member(
                self.data, "xl/worksheets/sheet1.xml", b"<worksheet>"
            ),
        }
        for label, data in cases.items():
            with self.subTest(label=label):
                self.assertIsNone(fetch._normalize_strict_ooxml_namespaces(data))


if __name__ == "__main__":
    unittest.main()

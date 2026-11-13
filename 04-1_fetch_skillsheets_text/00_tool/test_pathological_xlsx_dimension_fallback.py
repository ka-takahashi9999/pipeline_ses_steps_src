#!/usr/bin/env python3
"""異常なXLSX worksheet dimensionだけを補正するfallbackのFocused Test。"""

import hashlib
import importlib.util
import re
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
        "fetch_pathological_dimension_test", TOOL_DIR / "fetch_skillsheets_text.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fetch = _load_module()


def _workbook_bytes(builder):
    workbook = openpyxl.Workbook()
    builder(workbook)
    output = BytesIO()
    workbook.save(output)
    return output.getvalue()


def _replace_first_dimension(data, reference):
    output = BytesIO()
    changed = False
    with zipfile.ZipFile(BytesIO(data), "r") as source:
        with zipfile.ZipFile(output, "w") as destination:
            destination.comment = source.comment
            for entry in source.infolist():
                member = source.read(entry)
                if entry.filename == "xl/worksheets/sheet1.xml":
                    member, count = re.subn(
                        rb'(<dimension\b[^>]*\bref=")[^"]+("[^>]*/?>)',
                        rb"\1" + reference.encode("ascii") + rb"\2",
                        member,
                        count=1,
                    )
                    changed = count == 1
                destination.writestr(entry, member)
    if not changed:
        raise AssertionError("dimension not replaced")
    return output.getvalue()


def _add_invalid_data_validation_any(data):
    output = BytesIO()
    changed = False
    data_validation = (
        b'<dataValidations count="1"><dataValidation sqref="A1" '
        b'showDropDown="0" showInputMessage="0" showErrorMessage="0" '
        b'allowBlank="0" type="any"/></dataValidations>'
    )
    with zipfile.ZipFile(BytesIO(data), "r") as source:
        with zipfile.ZipFile(output, "w") as destination:
            destination.comment = source.comment
            for entry in source.infolist():
                member = source.read(entry)
                if entry.filename == "xl/worksheets/sheet1.xml":
                    member, count = re.subn(
                        rb"</(?:[A-Za-z_][\w.-]*:)?worksheet>\s*$",
                        data_validation + b"</worksheet>",
                        member,
                        count=1,
                    )
                    changed = count == 1
                destination.writestr(entry, member)
    if not changed:
        raise AssertionError("worksheet root not found")
    return output.getvalue()


class PathologicalXlsxDimensionFallbackTest(unittest.TestCase):
    def test_max_row_dimension_with_few_cells_uses_actual_cells(self):
        normal = _workbook_bytes(
            lambda workbook: (
                setattr(workbook.active, "title", "row-edge"),
                workbook.active.__setitem__("A1", "alpha"),
                workbook.active.__setitem__("B3", "omega"),
            )
        )
        pathological = _replace_first_dimension(normal, "A1:B1048576")
        original_sha256 = hashlib.sha256(pathological).hexdigest()
        sanitized = fetch._sanitize_pathological_worksheet_dimensions(pathological)
        self.assertIsNotNone(sanitized)
        text = fetch.extract_text_from_excel(pathological, "row-edge.xlsx")
        self.assertIn("alpha", text)
        self.assertIn("omega", text)
        self.assertEqual(hashlib.sha256(pathological).hexdigest(), original_sha256)

    def test_max_column_dimension_with_few_cells_uses_actual_cells(self):
        normal = _workbook_bytes(
            lambda workbook: (
                setattr(workbook.active, "title", "column-edge"),
                workbook.active.__setitem__("A1", "left"),
                workbook.active.__setitem__("C2", "right"),
            )
        )
        pathological = _replace_first_dimension(normal, "A1:XFD2")
        sanitized = fetch._sanitize_pathological_worksheet_dimensions(pathological)
        self.assertIsNotNone(sanitized)
        text = fetch.extract_text_from_excel(pathological, "column-edge.xlsx")
        self.assertIn("left", text)
        self.assertIn("right", text)

    def test_normal_and_genuinely_large_dimensions_are_not_sanitized(self):
        normal = _workbook_bytes(
            lambda workbook: (
                workbook.active.__setitem__("A1", "start"),
                workbook.active.__setitem__("A20000", "end"),
            )
        )
        self.assertIsNone(fetch._sanitize_pathological_worksheet_dimensions(normal))
        with patch.object(
            fetch,
            "_sanitize_pathological_worksheet_dimensions",
            wraps=fetch._sanitize_pathological_worksheet_dimensions,
        ) as sanitize:
            text = fetch.extract_text_from_excel(normal, "large-normal.xlsx")
        sanitize.assert_not_called()
        self.assertIn("start", text)
        self.assertIn("end", text)

        true_edge = _workbook_bytes(
            lambda workbook: (
                workbook.active.__setitem__("A1", "start"),
                workbook.active.__setitem__("A1048576", "real-edge"),
            )
        )
        self.assertIsNone(fetch._sanitize_pathological_worksheet_dimensions(true_edge))

    def test_merged_cells_and_multiple_sheets_are_unchanged(self):
        def build(workbook):
            first = workbook.active
            first.title = "first"
            first.merge_cells("A1:C1")
            first["A1"] = "merged-value"
            second = workbook.create_sheet("second")
            second["B2"] = "second-value"

        data = _workbook_bytes(build)
        text = fetch.extract_text_from_excel(data, "multi.xlsx")
        self.assertEqual(text.count("merged-value"), 1)
        self.assertIn("=== シート: first ===", text)
        self.assertIn("=== シート: second ===", text)
        self.assertIn("second-value", text)

    def test_data_validation_any_remains_hold(self):
        normal = _workbook_bytes(
            lambda workbook: workbook.active.__setitem__("A1", "英語(中)")
        )
        data = _add_invalid_data_validation_any(normal)
        with self.assertRaises(ValueError):
            fetch.extract_text_from_excel(data, "data-validation-any.xlsx")
        self.assertIsNone(fetch._sanitize_pathological_worksheet_dimensions(data))


if __name__ == "__main__":
    unittest.main()

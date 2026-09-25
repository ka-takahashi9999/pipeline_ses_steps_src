#!/usr/bin/env python3
"""形式不明添付のOOXML内部構造fallbackに限定したFocused Test。"""

import base64
import copy
import importlib.util
import json
import sys
import tempfile
import unittest
import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import patch


TOOL_DIR = Path(__file__).resolve().parent
STEP = TOOL_DIR.parent
ROOT = STEP.parent
sys.path.insert(0, str(ROOT))
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fetch = _load_module("fetch_skillsheets_text_ooxml", TOOL_DIR / "fetch_skillsheets_text.py")
TARGET_ID = "1a0d19284573cdc4"


def _zip_attachment(names, filename="unknown.bin", mime="application/octet-stream"):
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in names:
            archive.writestr(name, "test")
    encoded = base64.urlsafe_b64encode(buffer.getvalue()).decode("ascii").rstrip("=")
    return {"filename": filename, "mime_type": mime, "data": encoded}


class OoxmlAttachmentFallbackTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.master = read_jsonl_as_dict(str(fetch.INPUT_MAIL_MASTER))
        cls.target_mail = cls.master[TARGET_ID]
        cls.target_attachment = cls.target_mail["attachments"][0]

    def test_real_attachment_is_eligible_extracts_and_passes_quality(self):
        self.assertEqual(
            fetch.detect_ooxml_attachment_type(self.target_attachment),
            "xlsx",
        )
        self.assertTrue(fetch.is_eligible_attachment(self.target_attachment))
        text = fetch.extract_from_attachment(self.target_attachment)
        self.assertEqual(len(text), 7693)
        self.assertIsNone(fetch.validate_skillsheet_text(text))

        result = fetch.fetch_skillsheet(
            TARGET_ID,
            self.target_mail,
            {"body_text": self.target_mail.get("body_text", "")},
        )
        self.assertTrue(result["success"])
        self.assertEqual(result["source"], "attachment")
        self.assertEqual(result["skillsheet"], text)

    def test_general_and_broken_zip_are_not_eligible(self):
        cases = (
            _zip_attachment(["readme.txt", "data/example.csv"]),
            _zip_attachment(["[Content_Types].xml", "data/example.xml"]),
            {
                "filename": "broken.bin",
                "mime_type": "application/octet-stream",
                "data": base64.urlsafe_b64encode(b"PK\x03\x04broken").decode("ascii"),
            },
            {
                "filename": "invalid-base64.bin",
                "mime_type": "application/octet-stream",
                "data": "%%%not-base64%%%",
            },
        )
        for attachment in cases:
            with self.subTest(filename=attachment["filename"]):
                self.assertIsNone(fetch.detect_ooxml_attachment_type(attachment))
                self.assertFalse(fetch.is_eligible_attachment(attachment))

    def test_excel_and_word_require_unambiguous_ooxml_structure(self):
        excel = _zip_attachment(["[Content_Types].xml", "xl/workbook.xml"])
        word = _zip_attachment(["[Content_Types].xml", "word/document.xml"])
        ambiguous = _zip_attachment([
            "[Content_Types].xml", "xl/workbook.xml", "word/document.xml"
        ])
        self.assertEqual(fetch.detect_ooxml_attachment_type(excel), "xlsx")
        self.assertEqual(fetch.detect_ooxml_attachment_type(word), "docx")
        self.assertIsNone(fetch.detect_ooxml_attachment_type(ambiguous))
        self.assertTrue(fetch.is_eligible_attachment(excel))
        self.assertTrue(fetch.is_eligible_attachment(word))
        self.assertFalse(fetch.is_eligible_attachment(ambiguous))

    def test_existing_formats_and_negative_filenames_are_unchanged(self):
        normal = (
            {"filename": "resume.xlsx", "mime_type": "application/octet-stream"},
            {"filename": "resume.xls", "mime_type": "application/vnd.ms-excel"},
            {"filename": "resume.pdf", "mime_type": "application/pdf"},
            {"filename": "resume.docx", "mime_type": "application/octet-stream"},
            {"filename": "resume.doc", "mime_type": "application/msword"},
        )
        for attachment in normal:
            with self.subTest(filename=attachment["filename"]):
                self.assertTrue(fetch.is_eligible_attachment(attachment))

        excel_ooxml = _zip_attachment(["[Content_Types].xml", "xl/workbook.xml"])
        negatives = (
            {"filename": "会社案内.pdf", "mime_type": "application/pdf"},
            {"filename": "要員一覧.xlsx", "mime_type": "application/vnd.ms-excel"},
            dict(excel_ooxml, filename="会社案内.bin"),
            dict(excel_ooxml, filename="要員一覧.bin"),
        )
        for attachment in negatives:
            with self.subTest(filename=attachment["filename"]):
                self.assertFalse(fetch.is_eligible_attachment(attachment))

    def test_latest_resources_gain_only_target_attachment_and_memory_confirm(self):
        resources = read_jsonl_as_list(str(fetch.INPUT_RESOURCES))
        self.assertEqual(len(resources), 1835)
        additions = set()
        for resource in resources:
            mid = resource["message_id"]
            for index, attachment in enumerate(
                (self.master.get(mid) or {}).get("attachments") or []
            ):
                current = fetch.is_eligible_attachment(attachment)
                with patch.object(
                    fetch, "detect_ooxml_attachment_type", return_value=None
                ):
                    previous = fetch.is_eligible_attachment(attachment)
                if current != previous:
                    additions.add((mid, index, previous, current))
        self.assertEqual(additions, {(TARGET_ID, 0, False, True)})

        results = read_jsonl_as_list(str(fetch.OUTPUT_JSONL))
        no_fetch = read_jsonl_as_list(str(fetch.NO_FETCH_JSONL))
        target_result = fetch.fetch_skillsheet(
            TARGET_ID,
            self.target_mail,
            {"body_text": self.target_mail.get("body_text", "")},
        )
        updated_results = [
            target_result if row["message_id"] == TARGET_ID else copy.deepcopy(row)
            for row in results
        ]
        updated_no_fetch = [
            copy.deepcopy(row) for row in no_fetch if row["message_id"] != TARGET_ID
        ]
        self.assertEqual((len(updated_results), len(updated_no_fetch)), (1835, 82))
        changed_ids = {
            before["message_id"]
            for before, after in zip(results, updated_results)
            if before != after
        }
        self.assertEqual(changed_ids, {TARGET_ID})

        confirm = _load_module(
            "confirm_fetch_skillsheets_text_ooxml",
            STEP / "02_confirm" / "confirm_fetch_skillsheets_text.py",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            result_path = temp_path / "results.jsonl"
            no_fetch_path = temp_path / "no_fetch.jsonl"
            write_jsonl(str(result_path), updated_results)
            write_jsonl(str(no_fetch_path), updated_no_fetch)
            with patch.object(confirm, "RESULT_JSONL", result_path), patch.object(
                confirm, "NO_FETCH_JSONL", no_fetch_path
            ), patch.object(confirm, "logger") as confirm_logger:
                self.assertTrue(confirm.run_confirm())
                self.assertTrue(any(
                    "confirm 全チェック合格" in str(call)
                    for call in confirm_logger.ok.call_args_list
                ))

    def test_07_1_missing_skillsheet_impact_matches_existing_artifact(self):
        error_path = (
            ROOT / "07-1_requirement_skill_ai_matching" / "01_result"
            / "99_error_requirement_skill_ai_matching.jsonl"
        )
        errors = [
            json.loads(line) for line in error_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        missing = [
            row for row in errors
            if row.get("error_type") == "missing_resource_skillsheet"
        ]
        resource_ids = {
            (row.get("resource_info") or {}).get("message_id") for row in missing
        }
        target_errors = [
            row for row in missing
            if (row.get("resource_info") or {}).get("message_id") == TARGET_ID
        ]
        self.assertEqual((len(missing), len(resource_ids)), (360, 48))
        self.assertEqual(len(target_errors), 9)
        self.assertEqual(
            (len(missing) - len(target_errors), len(resource_ids - {TARGET_ID})),
            (351, 47),
        )


if __name__ == "__main__":
    unittest.main()

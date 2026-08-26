#!/usr/bin/env python3
"""Focused P5 tests for source-owned ATTACHMENT_LIST variable cardinality."""

import base64
import copy
import io
import json
import sys
import unittest
import zipfile
from pathlib import Path


sys.dont_write_bytecode = True
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
for import_path in (
    PROJECT_ROOT,
    STEP_DIR / "00_tool" / "adapters" / "attachment_list",
    STEP_DIR / "00_tool" / "canonicalize",
    STEP_DIR / "00_tool" / "core",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.json_utils import read_jsonl_as_list
from attachment_fixture_source import build_source_owned_fixture
from attachment_list_adapter import AttachmentListAdapter
from canonical_overlay import build_canonical_overlay


CONFIG_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "companies"
    / "jqit_attachment_list.config.json.example"
)
ACTUAL_INPUT = (
    PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)
STATIONS = [
    "港南中央",
    "綱島",
    "南林間",
    "菊川",
    "戸塚",
    "中神",
    "八幡山",
    "海老名",
    "新宿",
    "渋谷",
]


def _zip_bytes(files):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_STORED) as archive:
        for name, payload in files:
            info = zipfile.ZipInfo(name, (2020, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_STORED
            archive.writestr(info, payload)
    return output.getvalue()


def _xlsx_bytes(label):
    return _zip_bytes(
        [
            ("[Content_Types].xml", b"<Types/>") ,
            ("xl/workbook.xml", ("<workbook>" + label + "</workbook>").encode()),
        ]
    )


def _attachment(filename, payload, mime_type=None, source_entry_id=None):
    return {
        "source_entry_id": source_entry_id or "part:" + filename,
        "filename": filename,
        "mime_type": mime_type or "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "size": len(payload),
        "data": base64.urlsafe_b64encode(payload).decode("ascii").rstrip("="),
    }


def synthetic_source(profile_count, declared_count=None, attachment_count=None):
    declared_count = profile_count if declared_count is None else declared_count
    attachment_count = profile_count if attachment_count is None else attachment_count
    blocks = []
    for index in range(profile_count):
        identifier = "R." + chr(ord("A") + index)
        station = STATIONS[index]
        blocks.append(
            "***************************************************\n"
            + "【名　前】：" + identifier + "（30歳・男性）\n"
            + "【最　寄】：" + station + "駅\n"
            + "【スキル】：Python\n"
            + "【所　属】：弊社正社員\n"
        )
    attachments = []
    for index in range(attachment_count):
        identifier = "R." + chr(ord("A") + index)
        station = STATIONS[index]
        attachments.append(
            _attachment(
                identifier + station + "_スキルシート.xlsx",
                _xlsx_bytes(identifier),
            )
        )
    return {
        "message_id": "synthetic-jqit-" + str(profile_count) + "-" + str(declared_count) + "-" + str(attachment_count),
        "thread_id": "synthetic-thread",
        "date": "Tue, 25 Aug 2026 00:00:00 +0000",
        "from": "Synthetic <test@jqit.co.jp>",
        "to": ["test@example.invalid"],
        "cc": "",
        "reply_to": "",
        "subject": "【JQITプロパー情報】synthetic variable-N",
        "body_text": (
            "■スキル概要\n■synthetic（"
            + str(declared_count)
            + "名）\n\n"
            + "\n".join(blocks)
            + "\n見合う案件が御座いましたら、ご提案ください。\n"
        ),
        "html_links": [],
        "authoritative_attachments": attachments,
    }


class AttachmentListAdapterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.adapter = AttachmentListAdapter.from_file(CONFIG_PATH)

    def _parse(self, source):
        return self.adapter.parse(build_source_owned_fixture(source))

    def test_variable_n_uses_one_config(self):
        for count in (0, 1, 2, 4, 10):
            with self.subTest(count=count):
                result = self._parse(synthetic_source(count))
                self.assertEqual("PARSED", result.status)
                self.assertEqual(count, len(result.items))
                self.assertEqual("VERIFIED_COMPLETE", result.source["source_acquisition_status"])
                self.assertEqual([count, count, count], [row["count"] for row in result.source["cardinality_evidence"]])

    def test_actual_jqit_three_deliveries_are_unverified_technical_only(self):
        records = read_jsonl_as_list(str(ACTUAL_INPUT))
        selected = [record for record in records if self.adapter.matches(record)]
        self.assertEqual(3, len(selected))
        results = [self.adapter.parse(record) for record in selected]
        self.assertEqual([4, 2, 2], [len(record["attachments"]) for record in selected])
        self.assertEqual([4, 2, 2], [result.source["profile_count"] for result in results])
        self.assertTrue(all(result.status == "PARTIAL" for result in results))
        self.assertTrue(all(result.source["source_acquisition_status"] == "UNVERIFIED" for result in results))
        self.assertTrue(all(result.source["container_enumeration_status"] == "COMPLETE" for result in results))
        self.assertTrue(all(result.source["inline_structure_status"] == "PASS" for result in results))
        self.assertTrue(all(result.source["attachment_mapping_status"] == "PASS" for result in results))
        self.assertEqual(8, sum(len(result.technical_projection_items) for result in results))
        self.assertEqual(0, sum(len(result.items) for result in results))

    def test_manifest_is_source_owned_and_complete(self):
        fixture = build_source_owned_fixture(synthetic_source(2))
        manifest = fixture["attachment_acquisition_manifest"]
        self.assertEqual(2, manifest["expected_ordered_count"])
        self.assertEqual(2, len(manifest["authoritative_attachment_entries"]))
        self.assertNotIn("authoritative_attachments", fixture)
        self.assertEqual("PARSED", self.adapter.parse(fixture).status)

    def test_acquisition_negative_matrix(self):
        base = build_source_owned_fixture(synthetic_source(4))
        cases = {}
        missing = copy.deepcopy(base)
        missing.pop("attachment_acquisition_manifest")
        cases["manifest_missing"] = missing
        deletion = copy.deepcopy(base)
        del deletion["attachments"][2]
        cases["middle_deletion"] = deletion
        insertion = copy.deepcopy(base)
        insertion["attachments"].insert(2, _attachment("R.E新宿_スキルシート.xlsx", _xlsx_bytes("R.E")))
        cases["middle_insertion"] = insertion
        replacement = copy.deepcopy(base)
        replacement["attachments"][2] = _attachment("R.C南林間_スキルシート.xlsx", _xlsx_bytes("replacement"), source_entry_id=base["attachments"][2]["source_entry_id"])
        cases["replacement"] = replacement
        reordered = copy.deepcopy(base)
        reordered["attachments"][1], reordered["attachments"][2] = reordered["attachments"][2], reordered["attachments"][1]
        cases["order_change"] = reordered
        for field, value, name in (
            ("source_id", "wrong-source", "source_id"),
            ("expected_ordered_count", 99, "manifest_count"),
            ("expected_ordered_digest", "sha256:" + "0" * 64, "manifest_digest"),
            ("manifest_schema_version", "unsupported.v9", "schema"),
            ("extractor_status", "FAILED", "extractor_failed"),
            ("acquisition_status", "INCOMPLETE", "retrieval_incomplete"),
            ("acquisition_status", None, "retrieval_missing"),
        ):
            case = copy.deepcopy(base)
            if value is None:
                case["attachment_acquisition_manifest"].pop(field)
            else:
                case["attachment_acquisition_manifest"][field] = value
            cases[name] = case
        for name, case in cases.items():
            with self.subTest(name=name):
                result = self.adapter.parse(case)
                self.assertNotEqual("PARSED", result.status)
                self.assertEqual([], result.items)
                expected = "UNVERIFIED" if name == "manifest_missing" else "INCOMPLETE"
                self.assertEqual(expected, result.source["source_acquisition_status"])
        deletion_result = self.adapter.parse(cases["middle_deletion"])
        self.assertEqual("COMPLETE", deletion_result.source["container_enumeration_status"])

    def test_item_completeness_negative_matrix(self):
        cases = {
            "profile_3_attachment_4": synthetic_source(3, declared_count=4, attachment_count=4),
            "profile_4_attachment_3": synthetic_source(4, declared_count=4, attachment_count=3),
            "declared_3_profile_4": synthetic_source(4, declared_count=3, attachment_count=4),
        }
        mapping = synthetic_source(4)
        mapping["authoritative_attachments"][3]["filename"] = "R.Z新宿_スキルシート.xlsx"
        cases["mapping_3_of_4"] = mapping
        unused = synthetic_source(4)
        unused["authoritative_attachments"].append(_attachment("R.Z新宿_スキルシート.xlsx", _xlsx_bytes("R.Z")))
        cases["unused_attachment"] = unused
        duplicate = synthetic_source(4)
        duplicate_attachment = copy.deepcopy(duplicate["authoritative_attachments"][3])
        duplicate_attachment["source_entry_id"] = "part:duplicate-R.D"
        duplicate["authoritative_attachments"].append(duplicate_attachment)
        cases["duplicate_attachment_identity"] = duplicate
        unknown = synthetic_source(4)
        unknown["authoritative_attachments"].append(_attachment("unclassified.bin", b"opaque", mime_type="application/octet-stream"))
        cases["unknown_role"] = unknown
        for name, source in cases.items():
            with self.subTest(name=name):
                result = self._parse(source)
                self.assertNotEqual("PARSED", result.status)
                self.assertEqual([], result.items)

    def test_exact_key_mapping_has_no_substring_fallback(self):
        source = synthetic_source(1)
        source["authoritative_attachments"][0]["filename"] = "R.B港南中央_スキルシート.xlsx"
        result = self._parse(source)
        self.assertNotEqual("PARSED", result.status)
        self.assertEqual([], result.items)
        self.assertEqual(0, result.source["false_substring_matches"])

    def test_shared_and_supporting_do_not_change_item_cardinality(self):
        source = synthetic_source(4)
        source["authoritative_attachments"].extend(
            [
                _attachment("shared-format.xlsx", _xlsx_bytes("shared")),
                _attachment("supporting-readme.pdf", b"synthetic-pdf", mime_type="application/pdf"),
            ]
        )
        result = self._parse(source)
        self.assertEqual("PARSED", result.status)
        self.assertEqual(4, len(result.items))
        self.assertEqual(6, len(result.attachment_enumeration))
        self.assertEqual(4, result.source["item_attachment_count"])
        self.assertEqual(1, result.source["attachment_role_counts"]["SHARED"])
        self.assertEqual(1, result.source["attachment_role_counts"]["SUPPORTING"])
        self.assertTrue(all(len(item["attachments"]) == 1 for item in result.items))

    def test_zip_is_archive_boundary_and_never_expanded(self):
        source = synthetic_source(2)
        source["authoritative_attachments"].append(
            _attachment("profiles.zip", _zip_bytes([("inside.xlsx", _xlsx_bytes("inside"))]), mime_type="application/zip")
        )
        result = self._parse(source)
        self.assertEqual("UNSUPPORTED", result.status)
        self.assertEqual([], result.items)
        self.assertEqual("COMPLETE", result.source["container_enumeration_status"])
        self.assertEqual(1, result.source["attachment_role_counts"]["ARCHIVE"])
        self.assertTrue(any(row["kind"] == "ARCHIVE" for row in result.containers))

    def test_identity_and_version_contract(self):
        source = synthetic_source(4)
        first = self._parse(copy.deepcopy(source))
        repeat = self._parse(copy.deepcopy(source))
        self.assertEqual([item["derived_item_id"] for item in first.items], [item["derived_item_id"] for item in repeat.items])
        reordered_source = copy.deepcopy(source)
        reordered_source["authoritative_attachments"] = list(reversed(reordered_source["authoritative_attachments"]))
        reordered = self._parse(reordered_source)
        self.assertEqual(
            {item["logical_item_id"]: item["derived_item_id"] for item in first.items},
            {item["logical_item_id"]: item["derived_item_id"] for item in reordered.items},
        )
        artifact_changed_source = copy.deepcopy(source)
        artifact_changed_source["authoritative_attachments"][1] = _attachment("R.B綱島_スキルシート.xlsx", _xlsx_bytes("changed"))
        artifact_changed = self._parse(artifact_changed_source)
        first_by_logical = {item["logical_item_id"]: item for item in first.items}
        changed_by_logical = {item["logical_item_id"]: item for item in artifact_changed.items}
        changed_ids = [key for key in first_by_logical if first_by_logical[key]["derived_item_id"] != changed_by_logical[key]["derived_item_id"]]
        self.assertEqual(1, len(changed_ids))
        body_changed_source = copy.deepcopy(source)
        body_changed_source["body_text"] = body_changed_source["body_text"].replace("【スキル】：Python", "【スキル】：Python / AWS", 1)
        body_changed = self._parse(body_changed_source)
        body_changed_by_logical = {item["logical_item_id"]: item for item in body_changed.items}
        body_changed_ids = [key for key in first_by_logical if first_by_logical[key]["derived_item_id"] != body_changed_by_logical[key]["derived_item_id"]]
        self.assertEqual(1, len(body_changed_ids))
        self.assertEqual(set(first_by_logical), set(changed_by_logical))
        self.assertEqual(set(first_by_logical), set(body_changed_by_logical))

    def test_canonical_projection_is_one_ordinary_mail_per_item(self):
        fixture = build_source_owned_fixture(synthetic_source(4))
        result = self.adapter.parse(fixture)
        overlays = [build_canonical_overlay(fixture, item) for item in result.items]
        self.assertEqual(4, len(overlays))
        for overlay, item in zip(overlays, result.items):
            self.assertEqual(fixture["from"], overlay["from"])
            self.assertEqual(1, len(overlay["attachments"]))
            self.assertEqual([], overlay["html_links"])
            self.assertIn(item["identifier"], overlay["body_text"])
            other_identifiers = {candidate["identifier"] for candidate in result.items} - {item["identifier"]}
            self.assertTrue(all(identifier not in overlay["body_text"] for identifier in other_identifiers))


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""外部取得された複数人物一覧のquality NGに限定したfocused test。"""

import json
import sys
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
STEP_DIR = TOOL_DIR.parent
sys.path.insert(0, str(TOOL_DIR))

import normalize_skillsheets_text as target


def _read_jsonl(path):
    with Path(path).open(encoding="utf-8") as file_obj:
        return [json.loads(line) for line in file_obj if line.strip()]


class MultiPersonSkillsheetQualityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.input_records = _read_jsonl(target.INPUT_JSONL)
        cls.by_id = {record["message_id"]: record for record in cls.input_records}
        cls.previous_records = {
            record["message_id"]: record
            for record in _read_jsonl(target.OUTPUT_JSONL)
        }
        cls.previous_multi_ids = {
            message_id
            for message_id, record in cls.previous_records.items()
            if target.MULTI_PERSON_QUALITY_FLAG in (record.get("cleanup_flags") or [])
        }

    def test_explicit_external_two_person_list_is_quality_ng(self):
        text = """要員一覧
氏名: 人物A
所属: 弊社
年齢: 30歳
最寄: 東京駅
単価: 70万円
スキル: Java
氏名: 人物B
所属: 弊社
年齢: 35歳
最寄: 新宿駅
単価: 75万円
スキル: Python
"""
        record = {
            "message_id": "fixture",
            "success": True,
            "source": "google_drive",
            "urls": "https://drive.google.example/fixture",
            "skillsheet": text,
        }
        output = target.build_record(record)
        self.assertFalse(output["success"])
        self.assertEqual(output["skillsheet"], "")
        self.assertIn(target.MULTI_PERSON_QUALITY_FLAG, output["cleanup_flags"])

    def test_existing_multi_person_records_remain_quality_ng(self):
        self.assertEqual(len(self.previous_multi_ids), 5)
        for message_id in self.previous_multi_ids:
            with self.subTest(message_id=message_id):
                source = self.by_id[message_id]
                self.assertTrue(source["success"])
                self.assertTrue((source.get("skillsheet") or "").strip())
                output = target.build_record(source)
                self.assertFalse(output["success"])
                self.assertEqual(output["skillsheet"], "")
                evidence = output["cleanup_stats"][
                    target.MULTI_PERSON_QUALITY_FLAG
                ]
                self.assertGreaterEqual(evidence["complete_profile_block_count"], 2)

    def test_long_single_person_and_table_are_not_quality_ng(self):
        career_rows = "\n".join(
            f"{year} | 株式会社Example | 案件{year} | 設計・開発・テスト"
            for year in range(2000, 2030)
        )
        text = (
            "職務経歴書\n氏名: 人物A\n年齢: 40歳\n最寄: 東京駅\n"
            "スキル: Java Python SQL\n期間 | 会社名 | プロジェクト | 担当\n"
            + career_rows
        )
        record = {
            "message_id": "single-long-table",
            "success": True,
            "source": "google_drive",
            "urls": "https://drive.google.example/single",
            "skillsheet": text,
        }
        output = target.build_record(record)
        self.assertTrue(output["success"])
        self.assertNotIn(target.MULTI_PERSON_QUALITY_FLAG, output["cleanup_flags"])

    def test_horizontal_profile_columns_are_quality_ng(self):
        text = (
            "=== シート: 注力要員 ===\n"
            "サマリー | ■氏名:A.A ■年齢:30歳 ■最寄:東京駅 "
            "■所属:自社 ■単価:70万円 | "
            "■氏名:B.B ■年齢:35歳 ■最寄:新宿駅 "
            "■所属:自社 ■単価:75万円\n"
        )
        record = {
            "message_id": "horizontal-roster",
            "success": True,
            "source": "other",
            "urls": "https://example.test/roster.xlsx",
            "skillsheet": text,
        }
        output = target.build_record(record)
        self.assertFalse(output["success"])
        self.assertEqual(output["skillsheet"], "")
        evidence = output["cleanup_stats"][target.MULTI_PERSON_QUALITY_FLAG]
        self.assertEqual(evidence["list_layout"], "horizontal_profile_columns")

    def test_wide_single_person_career_table_is_not_horizontal_roster(self):
        text = (
            "職務経歴書\n氏名: A.A | 年齢: 40歳 | 最寄: 東京駅 | 所属: 自社\n"
            "期間 | 案件 | OS | DB | 言語 | FW | 要件定義 | 設計 | 製造 | テスト\n"
            + "\n".join(
                f"{year} | 案件{year} | Linux | Oracle | Java | Spring | ○ | ○ | ○ | ○"
                for year in range(2000, 2030)
            )
        )
        record = {
            "message_id": "wide-single-person",
            "success": True,
            "source": "other",
            "urls": "https://example.test/single.xlsx",
            "skillsheet": text,
        }
        output = target.build_record(record)
        self.assertTrue(output["success"])
        self.assertNotIn(target.MULTI_PERSON_QUALITY_FLAG, output["cleanup_flags"])

    def test_normal_google_drive_success_is_preserved(self):
        source = next(
            record
            for record in self.input_records
            if record.get("success") is True
            and record.get("source") == "google_drive"
            and record["message_id"] not in self.previous_multi_ids
        )
        output = target.build_record(source)
        self.assertTrue(output["success"])
        self.assertTrue(output["skillsheet"].strip())
        self.assertNotIn(target.MULTI_PERSON_QUALITY_FLAG, output["cleanup_flags"])

    def test_attachment_is_out_of_scope_even_with_list_structure(self):
        source = dict(
            next(
                record
                for record in self.input_records
                if record.get("success") is True
                and record.get("source") == "attachment"
            )
        )
        source["skillsheet"] = """要員一覧
氏名: A\n年齢: 30歳\n最寄: 東京\n単価: 70万円\nスキル: Java
氏名: B\n年齢: 35歳\n最寄: 新宿\n単価: 75万円\nスキル: Python
"""
        output = target.build_record(source)
        self.assertTrue(output["success"])
        self.assertNotIn(target.MULTI_PERSON_QUALITY_FLAG, output["cleanup_flags"])

    def test_latest_outputs_have_only_structurally_explained_new_failures(self):
        previous = self.previous_records
        new_quality_failures = {}
        unexpected_changes = []

        for source in self.input_records:
            output = target.build_record(source)
            message_id = source["message_id"]
            if target.MULTI_PERSON_QUALITY_FLAG in output["cleanup_flags"]:
                new_quality_failures[message_id] = output
            elif output != previous[message_id]:
                unexpected_changes.append(message_id)

        self.assertEqual(len(new_quality_failures), 52)
        previous_multi_person = {
            message_id
            for message_id, record in previous.items()
            if target.MULTI_PERSON_QUALITY_FLAG in (record.get("cleanup_flags") or [])
        }
        self.assertEqual(len(previous_multi_person), 5)
        self.assertEqual(len(set(new_quality_failures) - previous_multi_person), 47)
        self.assertEqual(unexpected_changes, [])


if __name__ == "__main__":
    unittest.main()

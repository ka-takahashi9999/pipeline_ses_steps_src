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


TARGET_IDS = {
    "s3_2ae222ad87d25078331bd2c5f02eaeca815029f26f92ca2446dc19accf05ef41",
    "s3_b83800b374945ee6ceec1d3e6769bddd93055318c0ff69e7b3d5fecdd6109e09",
}
NORMAL_GOOGLE_DRIVE_ID = (
    "s3_95639ba212cf9515dd4cc88dcb218de7638cda57c05ac742fdeca874e8c98e70"
)
NORMAL_ATTACHMENT_ID = (
    "s3_2f078de9f33d95302e19e222c1000632dc4713924b66f32735be495b49ddb7d5"
)


def _read_jsonl(path):
    with Path(path).open(encoding="utf-8") as file_obj:
        return [json.loads(line) for line in file_obj if line.strip()]


class MultiPersonSkillsheetQualityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.input_records = _read_jsonl(target.INPUT_JSONL)
        cls.by_id = {record["message_id"]: record for record in cls.input_records}

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

    def test_target_two_are_quality_ng_after_successful_fetch(self):
        for message_id in TARGET_IDS:
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

    def test_normal_google_drive_success_is_preserved(self):
        output = target.build_record(self.by_id[NORMAL_GOOGLE_DRIVE_ID])
        self.assertTrue(output["success"])
        self.assertTrue(output["skillsheet"].strip())
        self.assertNotIn(target.MULTI_PERSON_QUALITY_FLAG, output["cleanup_flags"])

    def test_attachment_is_out_of_scope_even_with_list_structure(self):
        source = dict(self.by_id[NORMAL_ATTACHMENT_ID])
        source["skillsheet"] = """要員一覧
氏名: A\n年齢: 30歳\n最寄: 東京\n単価: 70万円\nスキル: Java
氏名: B\n年齢: 35歳\n最寄: 新宿\n単価: 75万円\nスキル: Python
"""
        output = target.build_record(source)
        self.assertTrue(output["success"])
        self.assertNotIn(target.MULTI_PERSON_QUALITY_FLAG, output["cleanup_flags"])

    def test_latest_outputs_have_only_structurally_explained_new_failures(self):
        previous = {
            record["message_id"]: record
            for record in _read_jsonl(target.OUTPUT_JSONL)
        }
        new_quality_failures = {}
        unexpected_changes = []

        for source in self.input_records:
            output = target.build_record(source)
            message_id = source["message_id"]
            if target.MULTI_PERSON_QUALITY_FLAG in output["cleanup_flags"]:
                new_quality_failures[message_id] = output
            elif output != previous[message_id]:
                unexpected_changes.append(message_id)

        self.assertEqual(set(new_quality_failures), {
            "s3_bc9025401095edf8d1bc5ab53a9cddc3ad3f04ceef70164b9a552909fcac2aca",
            "s3_2ae222ad87d25078331bd2c5f02eaeca815029f26f92ca2446dc19accf05ef41",
            "s3_b83800b374945ee6ceec1d3e6769bddd93055318c0ff69e7b3d5fecdd6109e09",
            "s3_6333748d1e11a11bd2d49da02bbcfb76e048aee6bbe6ef3fa46679418bbbf51b",
        })
        self.assertEqual(unexpected_changes, [])


if __name__ == "__main__":
    unittest.main()

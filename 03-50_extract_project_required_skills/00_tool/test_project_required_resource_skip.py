#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""明確な案件構造を持つprojectのresource skip抑止focused test。"""

import json
import sys
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOL_DIR))

import extract_project_required_skills as target


TARGETS = {
    "s3_f0f34213a03b274d5fb26f12e79c91f06f7ebfc7fc8fb4e90928432c1876fe74": (4, 11),
    "s3_e91c08a1cfd882e5886ac4ccdea2e5aa8211a40a2aa10d33e5821ac6534651f4": (6, 5),
}


def _read_jsonl(path):
    with Path(path).open(encoding="utf-8") as file_obj:
        return [json.loads(line) for line in file_obj if line.strip()]


class ProjectRequiredResourceSkipTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cleaned = {
            record["message_id"]: record
            for record in _read_jsonl(target.INPUT_CLEANED)
        }

    def test_real_projects_are_extracted(self):
        for message_id, expected_counts in TARGETS.items():
            with self.subTest(message_id=message_id):
                body = self.cleaned[message_id]["body_text"]
                self.assertTrue(target._is_resource_introduction(body))
                self.assertTrue(target._has_explicit_required_header(body))
                self.assertTrue(target._has_clear_single_project_structure(body))
                required, optional, method = target.extract_skills(message_id, body)
                self.assertEqual(method, "rule")
                self.assertEqual((len(required), len(optional)), expected_counts)

    def test_resource_profile_is_still_skipped(self):
        body = (
            "【要員情報】\n"
            "スキルシート：添付\n並行営業：あり\n単金：70万円\n国籍：日本\n"
            "希望条件：リモート\n"
        )
        self.assertEqual(target.extract_skills("resource", body)[2], "skip")

    def test_resource_desired_project_terms_do_not_rescue(self):
        body = (
            "【必須スキル】\n・Java経験\n"
            "スキルシート：添付\n並行営業：あり\n単金：70万円\n国籍：日本\n"
            "希望案件：Web案件\n希望条件：リモート\n"
            "希望勤務地：東京\n希望作業内容：開発\n"
        )
        self.assertTrue(target._has_explicit_required_header(body))
        self.assertFalse(target._has_clear_single_project_structure(body))
        self.assertEqual(target.extract_skills("resource_desired", body)[2], "skip")

    def test_required_header_without_project_structure_does_not_rescue(self):
        body = (
            "【必須スキル】\n・Python経験\n"
            "スキルシート：添付\n並行営業：あり\n単金：80万円\n"
        )
        self.assertFalse(target._has_clear_single_project_structure(body))
        self.assertEqual(target.extract_skills("header_only", body)[2], "skip")

    def test_normal_project_extraction_is_unchanged(self):
        body = (
            "【案件内容】Webシステム開発\n"
            "【作業場所】東京\n"
            "【必須スキル】\n・Python開発経験\n"
            "【尚可スキル】\n・AWS経験\n"
        )
        required, optional, method = target.extract_skills("project", body)
        self.assertEqual(method, "rule")
        self.assertEqual([item["skill"] for item in required], ["Python開発経験"])
        self.assertEqual([item["skill"] for item in optional], ["AWS経験"])


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import unittest
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOL_DIR))
import extract_project_required_skills as extractor


class Check07GenericRequiredScopeHeaderTest(unittest.TestCase):
    def test_generic_requirements_header_with_required_scope(self):
        for heading in (
            "【要件】 以下必須項目",
            "【要件】以下必須項目",
            "【要件】 以下必須",
            "【要件】下記必須項目",
            "【要件】 下記必須",
            "【要件】 以下の必須項目",
        ):
            with self.subTest(heading=heading):
                self.assertEqual(
                    extractor._classify_line(heading),
                    ("required_header", ""),
                )

    def test_generic_requirements_header_without_required_scope_is_unchanged(self):
        for heading in (
            "【要件】",
            "【要件】 以下尚可項目",
            "【要件】 参考",
            "【要件】 ご確認ください",
            "本文中の必須という単語",
            "【人物像】 以下必須項目",
            "【備考】 以下必須項目",
            "【業務内容】 以下必須項目",
        ):
            with self.subTest(heading=heading):
                self.assertNotEqual(
                    extractor._classify_line(heading)[0],
                    "required_header",
                )

    def test_target_items_stay_required_including_parenthetical_modifier(self):
        required, optional = extractor.rule_extract_skills(
            """【要件】 以下必須項目

・Power AutomateもしくはPower Queryに明るい方（両方経験有が望ましいが、どちらかに長けている方でも可）
・単独で作業可能な方（詳細な指示がないと動けない方はNG）
・コミュニケーションスキル（不明点やアラートを積極的に発信できる方）

【単価】 68万円
"""
        )
        self.assertEqual(
            [item["skill"] for item in required],
            [
                "Power AutomateもしくはPower Queryに明るい方(両方経験有が望ましいが、どちらかに長けている方でも可)",
                "単独で作業可能な方(詳細な指示がないと動けない方はNG)",
                "コミュニケーションスキル(不明点やアラートを積極的に発信できる方)",
            ],
        )
        self.assertEqual(optional, [])

    def test_existing_headers_and_transitions_are_unchanged(self):
        required, optional = extractor.rule_extract_skills(
            """《必須スキル》※〇×記載希望
・Java開発経験
尚可スキル 以下のいずれかを満たす。
・AWS構築経験
必須スキル
・Python開発経験
尚可スキル
・Azure運用経験
"""
        )
        self.assertEqual(
            [item["skill"] for item in required],
            ["Java開発経験", "Python開発経験"],
        )
        self.assertEqual(
            [item["skill"] for item in optional],
            ["AWS構築経験", "Azure運用経験"],
        )


if __name__ == "__main__":
    unittest.main()

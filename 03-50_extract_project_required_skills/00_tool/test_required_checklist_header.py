#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""required_checklist_header_recognition のfocused test。"""

import sys
import unittest
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parent
if str(TOOL_DIR) not in sys.path:
    sys.path.insert(0, str(TOOL_DIR))
import extract_project_required_skills as target


ITEMS = [
    "FileMakerの経験年数",
    "要件定義の経験",
    "実装したときのテーブル数、API数、画面数",
    "プロジェクトマネジメントの経験・年数",
]


def _body(request: str, header: str = "★スキル確認項目") -> str:
    bullets = "\n".join(f"・{item}" for item in ITEMS)
    return f"{request}\n\n{header}\n{bullets}\n"


class RequiredChecklistHeaderTest(unittest.TestCase):
    def assert_not_required(self, body: str) -> None:
        required, optional = target.rule_extract_skills(body)
        self.assertEqual(required, [])
        self.assertEqual(optional, [])

    def test_explicit_required_response_request_extracts_multiple_bullets(self):
        required, optional = target.rule_extract_skills(
            _body("※必須スキルの●×の記載お願いいたします。")
        )

        self.assertEqual([item["skill"] for item in required], ITEMS)
        self.assertEqual(optional, [])

    def test_header_alone_is_not_required(self):
        self.assert_not_required(_body("案件のご紹介です。"))

    def test_optional_checklist_is_not_required(self):
        self.assert_not_required(_body("optional項目の回答をお願いします。"))

    def test_interview_checklist_is_not_required(self):
        self.assert_not_required(_body("面談時確認事項への回答をお願いします。"))

    def test_questionnaire_is_not_required(self):
        self.assert_not_required(_body("アンケートへの回答をお願いします。"))

    def test_voluntary_response_is_not_required(self):
        self.assert_not_required(_body("任意項目の回答をお願いします。"))

    def test_desirable_skill_checklist_is_not_required(self):
        self.assert_not_required(_body("尚可スキルの○×記載をお願いします。"))

    def test_single_bullet_is_not_required(self):
        self.assert_not_required(
            "必須スキルの○×記載をお願いします。\n"
            "★スキル確認項目\n"
            "・Java開発経験\n"
        )

    def test_distant_required_request_is_not_required(self):
        body = (
            "必須スキルの○×記載をお願いします。\n"
            + ("補足情報です。\n" * 100)
            + _body("案件のご紹介です。")
        )
        self.assert_not_required(body)


if __name__ == "__main__":
    unittest.main()

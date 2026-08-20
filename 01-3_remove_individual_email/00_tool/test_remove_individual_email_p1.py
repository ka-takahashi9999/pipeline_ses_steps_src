#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Step 01-3 P1 Subject detector focused tests."""

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent / "remove_individual_email.py"
SPEC = importlib.util.spec_from_file_location("remove_individual_email", MODULE_PATH)
detector = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(detector)


class SubjectDetectorTest(unittest.TestCase):
    def assert_excluded(self, subject: str) -> None:
        self.assertIsNotNone(detector.detect_p1_exclusion_reason(subject), subject)

    def assert_allowed(self, subject: str) -> None:
        self.assertIsNone(detector.detect_p1_exclusion_reason(subject), subject)

    def test_explicit_multiple_resource_grammars(self) -> None:
        for subject in (
            "Java要員3名のご紹介",
            "要員：3名",
            "3名の要員",
            "2名で1人月",
            "プロパー２名",
        ):
            with self.subTest(subject=subject):
                self.assert_excluded(subject)

    def test_resource_separator_is_narrow(self) -> None:
        for subject in ("エンジニア×3名", "要員・3名", "要員1名", "二名の要員"):
            with self.subTest(subject=subject):
                self.assert_allowed(subject)

    def test_recruitment_veto(self) -> None:
        for subject in (
            "Javaエンジニア募集(2名)",
            "IBM COBOL要員の募集/SE、PG各2名の募集",
            "2名募集",
            "3名募集",
            "複数名募集",
            "募集枠2名",
            "増員2名",
            "要員3名のご紹介・別案件も募集",
        ):
            with self.subTest(subject=subject):
                self.assert_allowed(subject)

    def test_management_scale_veto(self) -> None:
        for subject in (
            "要員100名規模のマネジメント実績",
            "要員3名の管理経験あり",
        ):
            with self.subTest(subject=subject):
                self.assert_allowed(subject)

    def test_list_as_primary_fullmatch(self) -> None:
        for subject in (
            "弊社営業中要員リストの送付",
            "【要員情報】弊社営業中要員リストの送付",
            "RE: [共有] 要員共有8/19",
            "Fw:【案件】案件・要員まとめ更新",
        ):
            with self.subTest(subject=subject):
                self.assert_excluded(subject)

    def test_list_word_contains_is_allowed(self) -> None:
        for subject in (
            "田中様ご紹介＋その他人材一覧",
            "Javaエンジニアのご紹介/弊社要員一覧はこちら",
            "○○様のご紹介と営業中人材一覧",
            "要員一覧を添付しましたので田中様をご紹介します",
        ):
            with self.subTest(subject=subject):
                self.assert_allowed(subject)

    def test_explicit_multiple_project_grammars(self) -> None:
        self.assert_excluded("案件3件")
        self.assert_excluded("3件の案件")
        self.assert_allowed("案件1件")
        self.assert_allowed("M365案件")
        self.assert_allowed("3案件")

    def test_body_urls_and_attachments_are_not_detector_inputs(self) -> None:
        record = {
            "subject": "単一要員のご紹介",
            "body_text": "要員3名 https://example.com/a https://example.com/b",
            "attachments": [{"filename": "A.xlsx"}, {"filename": "B.xlsx"}],
            "html_links": ["https://example.com/a", "https://example.com/b"],
        }
        self.assertIsNone(
            detector.determine_exclusion_reason(record, set(), []), record["subject"]
        )

    def test_manual_exclusion_is_evaluated_first(self) -> None:
        record = {"from": "manual@example.com", "subject": "案件3件"}
        self.assertEqual(
            detector.determine_exclusion_reason(record, {"manual@example.com"}, []),
            "manual_exclude_list",
        )


if __name__ == "__main__":
    unittest.main()

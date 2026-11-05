#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib.util
import sys
import unittest
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOL_DIR))

import extract_project_required_skills as extractor


ROOT = Path(__file__).resolve().parents[2]


def _load_required_list_module():
    path = (
        ROOT
        / "03-51_extract_project_required_skills_list"
        / "00_tool"
        / "extract_project_required_skills_list.py"
    )
    spec = importlib.util.spec_from_file_location("check04_required_list", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_required_match_module():
    path = (
        ROOT
        / "06-11_match_required_skills_list"
        / "00_tool"
        / "match_required_skills_list.py"
    )
    spec = importlib.util.spec_from_file_location("check04_required_match", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Check04RequiredOptionalTest(unittest.TestCase):
    def test_optional_header_with_trailing_scope_clause(self):
        for heading in (
            "■尚可スキル 以下のいずれかを満たす。",
            "尚可スキル：以下のいずれか",
            "WANT：以下の条件",
            "歓迎スキル（下記いずれか）",
        ):
            with self.subTest(heading=heading):
                self.assertEqual(
                    extractor._classify_line(heading),
                    ("optional_header", ""),
                )

    def test_existing_optional_headers_remain_recognized(self):
        for heading in ("尚可", "尚可スキル", "WANT", "歓迎"):
            with self.subTest(heading=heading):
                self.assertEqual(
                    extractor._classify_line(heading),
                    ("optional_header", ""),
                )

    def test_optional_scope_header_does_not_match_explanatory_text(self):
        for text in (
            "尚可ですが必須ではありません",
            "尚可経験について面談で確認",
            "以下は尚可ではなく必須です",
            "この経験はあると尚可ですが選考条件ではありません",
        ):
            with self.subTest(text=text):
                self.assertNotEqual(
                    extractor._classify_line(text)[0],
                    "optional_header",
                )

    def test_optional_scope_header_transitions_following_items(self):
        required, optional = extractor.rule_extract_skills(
            """■必須スキル
・AWS設計、構築経験

■尚可スキル 以下のいずれかを満たす。
・API Gatewayの設計、構築経験
・CloudFormationによるIaC構築経験
"""
        )
        self.assertEqual(
            [x["skill"] for x in required],
            ["AWS設計、構築経験"],
        )
        self.assertEqual(
            [x["skill"] for x in optional],
            [
                "API Gatewayの設計、構築経験",
                "CloudFormationによるIaC構築経験",
            ],
        )

    def test_parenthesized_optional_skill_header(self):
        required, optional = extractor.rule_extract_skills(
            """スキル(必須):
Java
AWS

スキル(尚可):
Python
Azure
"""
        )
        self.assertEqual([x["skill"] for x in required], ["Java", "AWS"])
        self.assertEqual([x["skill"] for x in optional], ["Python", "Azure"])

    def test_large_circle_optional_skill_header(self):
        required, optional = extractor.rule_extract_skills(
            """◎必須スキル:
Java

〇尚可スキル:
AWS
Azure
"""
        )
        self.assertEqual([x["skill"] for x in required], ["Java"])
        self.assertEqual([x["skill"] for x in optional], ["AWS", "Azure"])

    def test_existing_headers_and_held_header_are_unchanged(self):
        for heading in ("必須", "必須スキル", "必須要件"):
            self.assertEqual(extractor._classify_line(heading), ("required_header", ""))
        self.assertEqual(extractor._classify_line("尚可"), ("optional_header", ""))
        self.assertNotEqual(
            extractor._classify_line("※あると望ましいスキル")[0],
            "optional_header",
        )

    def test_standalone_negative_required_lines_are_removed(self):
        required, optional = extractor.rule_extract_skills(
            """必須スキル:
Java開発経験
Kubernetesの実務経験は不問
英語は不要
資格は問いません
利用ツール不問

尚可:
AWS
"""
        )
        self.assertEqual([x["skill"] for x in required], ["Java開発経験"])
        self.assertEqual([x["skill"] for x in optional], ["AWS"])

    def test_standalone_inexperienced_allowed_lines_are_removed(self):
        required, optional = extractor.rule_extract_skills(
            """必須スキル:
未経験可
・未経験可
※未経験可
未経験可（育成枠）
※未経験可（育成枠）

尚可:
AWS
"""
        )
        self.assertEqual(required, [])
        self.assertEqual([x["skill"] for x in optional], ["AWS"])

    def test_inexperienced_allowed_text_with_other_meaning_is_preserved(self):
        # 今回追加した限定filter自体は、別意味の表現へ広がらない。
        # 「未経験者歓迎」は既存の一般rejectルールでは抽出対象外だが、
        # _prepare_required_skill_textでは変更せず返すことを確認する。
        for value in (
            "Java未経験可",
            "業界未経験可",
            "一部未経験可",
            "未経験可だがSQL経験必須",
            "未経験者歓迎",
        ):
            with self.subTest(value=value):
                self.assertEqual(extractor._prepare_required_skill_text(value), value)

        required, _ = extractor.rule_extract_skills(
            """必須スキル:
・Java未経験可
・業界未経験可
・一部未経験可
・未経験可だがSQL経験必須
・Python開発経験
"""
        )
        self.assertEqual(
            [x["skill"] for x in required],
            [
                "Java未経験可",
                "業界未経験可",
                "一部未経験可",
                "未経験可だがSQL経験必須",
                "Python開発経験",
            ],
        )

    def test_compound_negative_keeps_only_explicit_required_clause(self):
        required, _ = extractor.rule_extract_skills(
            """必須スキル:
Kubernetes経験は不問だがAWS経験は必須
Javaは不要ではない
経験年数は不問、ただし設計経験必須
"""
        )
        required_values = [x["skill"] for x in required]
        self.assertIn("AWS経験は必須", required_values)
        self.assertIn("Javaは不要ではない", required_values)
        self.assertIn("設計経験必須", required_values)
        self.assertTrue(all("Kubernetes" not in x for x in required_values))

    def test_03_51_does_not_restore_negated_kubernetes(self):
        required, optional = extractor.rule_extract_skills(
            """必須スキル:
Docker経験
Kubernetesの実務経験は不問
"""
        )
        required_list = _load_required_list_module()
        skill_entries = required_list.load_skill_list(required_list.SKILL_DICT_PATH)
        phase_entries = required_list.load_phase_map(required_list.PHASE_DICT_PATH)
        record = required_list.build_record(
            "focused-check04",
            required,
            optional,
            skill_entries,
            phase_entries,
        )
        self.assertIn("Docker", record["required_skill_keywords"])
        self.assertNotIn("Kubernetes", record["required_skill_keywords"])

    def test_mind_subsection_is_neutral_and_technical_required_continues(self):
        required, optional = extractor.rule_extract_skills(
            """【スキル】
必須
マインド・姿勢面
・スクラムでのプロジェクト経験
・当事者意識を持ち、チームで遂行できる方
・コミュニケーション能力の高い方
・勤怠に問題のない方
・情報セキュリティを意識し行動できる方

技術面
・開発言語：JavaもしくはGo
・要件定義経験5年以上
・設計経験5年以上
・GitHub または GitLab を使用したチーム開発経験

尚可：
・プロジェクトマネジメント経験
"""
        )
        required_values = [x["skill"] for x in required]
        optional_values = [x["skill"] for x in optional]
        self.assertEqual(
            required_values,
            [
                "開発言語:JavaもしくはGo",
                "要件定義経験5年以上",
                "設計経験5年以上",
                "GitHub または GitLab を使用したチーム開発経験",
            ],
        )
        self.assertEqual(optional_values, ["プロジェクトマネジメント経験"])

        required_list = _load_required_list_module()
        skill_entries = required_list.load_skill_list(required_list.SKILL_DICT_PATH)
        phase_entries = required_list.load_phase_map(required_list.PHASE_DICT_PATH)
        record = required_list.build_record(
            "focused-mind-subsection",
            required,
            optional,
            skill_entries,
            phase_entries,
        )
        self.assertIn("Java", record["required_skill_keywords"])
        self.assertIn("Go", record["required_skill_keywords"])
        self.assertIn("要件定義", record["required_phase_keywords"])
        self.assertNotIn("スクラム", record["required_skill_keywords"])
        self.assertNotIn("情報セキュリティ", record["required_skill_keywords"])

        required_match = _load_required_match_module()
        required_keywords = required_match.build_required_skill_keyword_set(record)
        strong_phases, weak_phases = required_match.build_required_phase_keywords(record)
        mind_only = required_match.judge_match(
            required_keywords,
            strong_phases,
            weak_phases,
            "スクラム、コミュニケーション、勤怠、情報セキュリティ",
            "",
        )
        technical = required_match.judge_match(
            required_keywords,
            strong_phases,
            weak_phases,
            "Javaによる要件定義経験",
            "",
        )
        self.assertFalse(mind_only[0])
        self.assertTrue(technical[0])

    def test_confirmed_compound_and_punctuation_headers(self):
        required, optional = extractor.rule_extract_skills(
            """【求めるスキル・経験】
・Python開発経験

▼歓迎・尚可スキル:
・AWS経験

【業務内容・必須スキル】
・Java開発経験

尚可；
・OCI経験

■ 必須前提条件
・顧客折衝経験

■必須条件 / 経験
・基本設計経験

・システム移行経験（希望）
"""
        )
        self.assertEqual(
            [x["skill"] for x in required],
            ["Python開発経験", "Java開発経験", "顧客折衝経験", "基本設計経験"],
        )
        self.assertEqual(
            [x["skill"] for x in optional],
            ["AWS経験", "OCI経験", "システム移行経験"],
        )

    def test_main_clause_negative_is_removed_but_parenthetical_qualifier_remains(self):
        required, _ = extractor.rule_extract_skills(
            """必須スキル:
・製造工程はオフショア対応のため、コーディング経験は必須ではありません
・【グッズ制作の実務経験は不問】です。デザインの基礎力を重視します。
・AWS環境で動作するアプリ開発経験（インフラ構築経験は不要）
"""
        )
        self.assertEqual(
            [x["skill"] for x in required],
            ["AWS環境で動作するアプリ開発経験(インフラ構築経験は不要)"],
        )

    def test_non_bulleted_short_skill_rescue_remains_disabled(self):
        required, optional = extractor.rule_extract_skills(
            """必須
RHEL
尚可
COBOL
"""
        )
        self.assertEqual(required, [])
        self.assertEqual(optional, [])

    def test_confirmed_required_prerequisite_stops_before_footer_after_blank(self):
        required, optional = extractor.rule_extract_skills(
            """［尚可］
・金融業界での実務経験

■ 必須前提条件
・契約社員化および出向契約が可能な方

        営業管理はこれひとつ、社内で情報の共有を
株式会社サンプル
"""
        )
        self.assertEqual(
            [x["skill"] for x in required],
            ["契約社員化および出向契約が可能な方"],
        )
        self.assertEqual(
            [x["skill"] for x in optional],
            ["金融業界での実務経験"],
        )


if __name__ == "__main__":
    unittest.main()

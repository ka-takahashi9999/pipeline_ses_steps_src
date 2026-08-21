import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from common.skillsheet_ai_context import (
    build_skillsheet_ai_context,
    build_skillsheet_ai_context_result,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load_module(name: str, relative_path: str):
    path = PROJECT_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SkillsheetAIContextTest(unittest.TestCase):
    def test_safe_profile_only_lines_are_removed(self):
        text = "\n".join(
            [
                "氏名 | T・Y | 性別 | 男性",
                "年齢: 45歳",
                "最寄駅: 新宿駅",
                "test@example.com",
                "Javaによる基本設計・開発",
            ]
        )
        result = build_skillsheet_ai_context(text)
        self.assertEqual(result, "Javaによる基本設計・開発")

    def test_compound_skill_line_is_kept(self):
        text = "PM経験10年 / 年齢45歳"
        self.assertEqual(build_skillsheet_ai_context(text), text)

    def test_strict_excel_metadata_is_removed_but_url_and_serial_date_are_kept(self):
        text = "\n".join(
            [
                "=== シート: Sheet1 ===",
                "スキルシート",
                "Page 1 / 3",
                "作成日: 2026/08/20",
                "=SUM(A1:A3)",
                "45292",
                "https://example.com/java",
                "Java ○",
            ]
        )
        result = build_skillsheet_ai_context(text)
        self.assertNotIn("Sheet1", result)
        self.assertNotIn("Page 1 / 3", result)
        self.assertNotIn("作成日", result)
        self.assertNotIn("SUM", result)
        self.assertIn("45292", result)
        self.assertIn("https://example.com/java", result)
        self.assertIn("Java ○", result)

    def test_skill_matrix_duration_role_and_process_are_kept(self):
        lines = [
            "Java ○",
            "AWS ◎",
            "Python △",
            "期間 | 2021/04～2024/03",
            "役割 | PM",
            "工程 | 要件定義・基本設計",
        ]
        result = build_skillsheet_ai_context("\n".join(lines))
        for line in lines:
            self.assertIn(line, result)

    def test_sample_only_sheet_is_kept_by_full_fallback(self):
        text = """=== シート: サンプル ===
スキルシート
期間 | 業務内容 | 役割
2021/04～2024/03 | Java開発 | PM
AWS | 基本設計 | 5名
案件履歴 | ECサイト開発
"""
        result = build_skillsheet_ai_context_result(text)
        self.assertTrue(result.used_fallback)
        self.assertEqual(result.text, text)
        self.assertIn("Java開発", result.text)
        self.assertIn("AWS", result.text)

    def test_sample_is_removed_only_with_clear_real_sheet_and_example_signal(self):
        text = """=== シート: スキルシート ===
期間 | 業務内容 | 役割
2021/04～2024/03 | Python開発 | PM
AWS構築
基本設計
詳細設計
=== シート: サンプル ===
氏名 | S・A | 性別 | 男性
期間 | 業務内容 | 役割
2018/01～2020/12 | Java11開発 | SE
Springboot
詳細設計
"""
        result = build_skillsheet_ai_context_result(text)
        self.assertFalse(result.used_fallback)
        self.assertEqual(result.removed_sheet_names, ("サンプル",))
        self.assertIn("Python開発", result.text)
        self.assertNotIn("Java11", result.text)
        self.assertNotIn("Springboot", result.text)

    def test_ambiguous_sample_sheet_is_kept_by_full_fallback(self):
        text = """=== シート: スキルシート ===
期間 | 業務内容 | 役割
2021/04～2024/03 | Python開発 | PM
AWS構築
基本設計
詳細設計
=== シート: サンプル ===
期間 | 業務内容 | 役割
2020/01～2023/12 | Java開発 | PL
AWS設計
要件定義
案件履歴
"""
        result = build_skillsheet_ai_context_result(text)
        self.assertTrue(result.used_fallback)
        self.assertEqual(result.text, text)
        self.assertIn("Java開発", result.text)

    def test_cleanup_to_empty_falls_back(self):
        text = "氏名: T・Y\n年齢: 45歳\n電話番号: 03-1234-5678"
        result = build_skillsheet_ai_context_result(text)
        self.assertTrue(result.used_fallback)
        self.assertEqual(result.fallback_reason, "cleanup_empty")
        self.assertEqual(result.text, text)

    def test_malformed_sheet_heading_falls_back(self):
        text = "=== シート: サンプル\nJava\nAWS"
        result = build_skillsheet_ai_context_result(text)
        self.assertTrue(result.used_fallback)
        self.assertEqual(result.fallback_reason, "parse_or_cleanup_error")
        self.assertEqual(result.text, text)

    def test_known_sample_contamination_fixture(self):
        path = (
            PROJECT_ROOT
            / "04-2_normalize_skillsheets_text/01_result/normalize_skillsheets_text.jsonl"
        )
        record = None
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                candidate = json.loads(line)
                if candidate.get("message_id") == "1a01dd3655110b52":
                    record = candidate
                    break
        self.assertIsNotNone(record)
        result = build_skillsheet_ai_context_result(record["skillsheet"])
        self.assertEqual(result.removed_sheet_names, ("サンプル",))
        self.assertNotIn("Java11(WebAPI)", result.text)
        self.assertNotIn("Springboot", result.text)
        self.assertIn("機能追加時の技術検証・選定", result.text)


class StepIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.step07 = _load_module(
            "step07_context_test",
            "07-1_requirement_skill_ai_matching/00_tool/normalized/requirement_skill_ai_matching.py",
        )
        cls.step08 = _load_module(
            "step08_context_test",
            "08-5_high_score_required_skill_recheck/00_tool/high_score_required_skill_recheck.py",
        )

    def test_both_steps_use_04_2_normalized(self):
        expected_name = "normalize_skillsheets_text.jsonl"
        self.assertEqual(self.step07.INPUT_SKILLSHEETS.name, expected_name)
        self.assertEqual(self.step08.INPUT_SKILLSHEETS.name, expected_name)
        self.assertIn("04-2_normalize_skillsheets_text", str(self.step07.INPUT_SKILLSHEETS))
        self.assertIn("04-2_normalize_skillsheets_text", str(self.step08.INPUT_SKILLSHEETS))

    def test_07_uses_builder_without_output_schema_change(self):
        pair = {
            "project_info": {"message_id": "p1"},
            "resource_info": {"message_id": "r1"},
        }
        projects = {
            "p1": {
                "required_skills": [{"skill": "Java"}],
                "optional_skills": [],
            }
        }
        sheets = {
            "r1": {
                "success": True,
                "source": "attachment",
                "skillsheet": "年齢: 45歳\nJava開発",
            }
        }
        response = {
            "required_skills": [{"skill": "Java", "match": True, "note": "Java経験あり"}],
            "optional_skills": [],
        }
        with patch.object(self.step07, "call_llm", return_value=response) as mocked:
            result, error = self.step07.process_pair(pair, projects, sheets, unittest.mock.Mock())
        self.assertIsNone(error)
        self.assertEqual(
            set(result),
            {"project_info", "resource_info", "required_skills", "optional_skills", "evaluation_meta"},
        )
        prompt = mocked.call_args.kwargs["user_prompt"]
        self.assertIn("Java開発", prompt)
        self.assertNotIn("年齢: 45歳", prompt)

    def test_08_uses_builder_without_output_schema_change(self):
        record = {
            "project_info": {
                "message_id": "p1",
                "required_skills": [{"skill": "Java", "match": True, "note": "あり"}],
            },
            "resource_info": {"message_id": "r1"},
            "match_info": {},
        }
        sheets = {
            "r1": {
                "success": True,
                "skillsheet": "氏名: T・Y\nJava開発",
            }
        }
        response = {
            "required_skill_checks": [
                {
                    "skill": "Java",
                    "original_match": True,
                    "recheck_match": True,
                    "confidence": "confirmed",
                    "reason": "Java経験あり",
                    "evidence": "Java開発",
                }
            ],
            "category_match": "match",
            "category_note": "案件: Java / 要員: Java",
        }
        with patch.object(self.step08, "call_llm", return_value=response) as mocked:
            result, error = self.step08._process_record(record, "100percent", sheets, {})
        self.assertIsNone(error)
        prompt = mocked.call_args.kwargs["user_prompt"]
        self.assertIn("Java開発", prompt)
        self.assertNotIn("氏名: T・Y", prompt)
        self.assertEqual(
            set(result) - set(record),
            {
                "source_score_band",
                "recheck_info",
                "required_skill_checks",
                "category_match",
                "category_note",
            },
        )


if __name__ == "__main__":
    unittest.main()

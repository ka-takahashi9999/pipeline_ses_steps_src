"""
07-1 表示用note文字数正規化の focused test

- 非空の正常文字列が30文字を超えた場合のみ切り詰めて成功扱いになること
- 30文字 / 29文字は内容不変で成功すること
- note空 / null / 非string / skill文言不一致 / match非bool は
  従来どおり invalid_output_schema のままであること（schema検証を緩めない）
- 複数skillが超過した場合も全件30文字以内になり、切り詰め件数が記録されること

LLMは呼び出さず call_llm をstubに差し替える。本番成果物には触れない。

実行:
  python3 07-1_requirement_skill_ai_matching/00_tool/test_note_length_normalization.py
"""

import importlib.util
import logging
import sys
import unittest
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

TOOL_SCRIPT = (
    PROJECT_ROOT
    / "07-1_requirement_skill_ai_matching/00_tool/requirement_skill_ai_matching.py"
)


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


tool_mod = load_module("requirement_skill_ai_matching_under_test", TOOL_SCRIPT)

from common.logger import get_logger  # noqa: E402

PROJECT_MID = "P0001"
RESOURCE_MID = "R0001"
REQUIRED_SKILL = "AWS 3年以上"
OPTIONAL_SKILL = "Terraform 1年以上"

NOTE_31 = "AWS Certified AI Practitioner取得"  # 31文字（20260817 runの実例）
NOTE_30 = "A" * 30
NOTE_29 = "A" * 29


class NoteLengthNormalizationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        logging.disable(logging.CRITICAL)
        self.logger = get_logger("test_note_length_normalization")
        self.pair = {
            "project_info": {"message_id": PROJECT_MID},
            "resource_info": {"message_id": RESOURCE_MID},
        }
        self.project_skills_map = {
            PROJECT_MID: {
                "message_id": PROJECT_MID,
                "required_skills": [{"skill": REQUIRED_SKILL}],
                "optional_skills": [{"skill": OPTIONAL_SKILL}],
            }
        }
        self.skillsheet_map = {
            RESOURCE_MID: {
                "message_id": RESOURCE_MID,
                "success": True,
                "skillsheet": "AWSでの設計構築経験あり。Terraform経験あり。",
                "source": "test",
            }
        }
        self._original_call_llm = tool_mod.call_llm

    def tearDown(self) -> None:
        tool_mod.call_llm = self._original_call_llm
        logging.disable(logging.NOTSET)

    def _stub_llm(self, response: Dict[str, Any]) -> None:
        def _fake_call_llm(*args: Any, **kwargs: Any) -> Dict[str, Any]:
            return response

        tool_mod.call_llm = _fake_call_llm

    def _run(self, required_notes: List[Any], optional_notes: List[Any], **overrides):
        """LLM応答をstubしてprocess_pairを1回実行する。"""
        response = {
            "required_skills": [
                {
                    "skill": overrides.get("required_skill", REQUIRED_SKILL),
                    "match": overrides.get("required_match", True),
                    "note": note,
                }
                for note in required_notes
            ],
            "optional_skills": [
                {"skill": OPTIONAL_SKILL, "match": False, "note": note}
                for note in optional_notes
            ],
        }
        self._stub_llm(response)
        stats: Dict[str, int] = {"note_truncated_count": 0}
        result, error = tool_mod.process_pair(
            self.pair,
            self.project_skills_map,
            self.skillsheet_map,
            self.logger,
            stats,
        )
        return result, error, stats

    # ---------------------------------------------------------- note境界
    def test_note_31_chars_is_truncated_and_pair_succeeds(self):
        result, error, stats = self._run([NOTE_31], [NOTE_29])
        self.assertIsNone(error)
        self.assertIsNotNone(result)
        note = result["required_skills"][0]["note"]
        self.assertEqual(len(note), 30)
        self.assertEqual(note, NOTE_31[:30])
        self.assertEqual(stats["note_truncated_count"], 1)

    def test_note_30_chars_is_unchanged(self):
        result, error, stats = self._run([NOTE_30], [NOTE_30])
        self.assertIsNone(error)
        self.assertEqual(result["required_skills"][0]["note"], NOTE_30)
        self.assertEqual(result["optional_skills"][0]["note"], NOTE_30)
        self.assertEqual(stats["note_truncated_count"], 0)

    def test_note_29_chars_is_unchanged(self):
        result, error, stats = self._run([NOTE_29], [NOTE_29])
        self.assertIsNone(error)
        self.assertEqual(result["required_skills"][0]["note"], NOTE_29)
        self.assertEqual(stats["note_truncated_count"], 0)

    # ---------------------------------------------------------- 従来error維持
    def test_empty_note_is_still_invalid_output_schema(self):
        result, error, stats = self._run([""], [NOTE_29])
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "invalid_output_schema")
        self.assertEqual(stats["note_truncated_count"], 0)

    def test_null_note_is_still_invalid_output_schema(self):
        result, error, _ = self._run([None], [NOTE_29])
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "invalid_output_schema")

    def test_non_string_note_is_still_invalid_output_schema(self):
        result, error, _ = self._run([12345], [NOTE_29])
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "invalid_output_schema")

    def test_skill_text_mismatch_is_still_invalid_output_schema(self):
        result, error, _ = self._run(
            [NOTE_29], [NOTE_29], required_skill="AWS 5年以上（改変）"
        )
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "invalid_output_schema")

    def test_non_bool_match_is_still_invalid_output_schema(self):
        result, error, _ = self._run([NOTE_29], [NOTE_29], required_match="true")
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "invalid_output_schema")

    def test_missing_field_is_still_error(self):
        self._stub_llm(
            {
                "required_skills": [{"skill": REQUIRED_SKILL, "match": True}],
                "optional_skills": [
                    {"skill": OPTIONAL_SKILL, "match": False, "note": NOTE_29}
                ],
            }
        )
        stats: Dict[str, int] = {"note_truncated_count": 0}
        result, error = tool_mod.process_pair(
            self.pair,
            self.project_skills_map,
            self.skillsheet_map,
            self.logger,
            stats,
        )
        self.assertIsNone(result)
        self.assertIn(error["error_type"], ("invalid_output_schema", "llm_parse_error"))

    # ---------------------------------------------------------- 複数truncate
    def test_multiple_notes_truncated_and_counted(self):
        self.project_skills_map[PROJECT_MID]["required_skills"] = [
            {"skill": REQUIRED_SKILL},
            {"skill": "Python 3年以上"},
        ]
        self._stub_llm(
            {
                "required_skills": [
                    {"skill": REQUIRED_SKILL, "match": True, "note": NOTE_31},
                    {"skill": "Python 3年以上", "match": True, "note": "B" * 45},
                ],
                "optional_skills": [
                    {"skill": OPTIONAL_SKILL, "match": False, "note": "C" * 31}
                ],
            }
        )
        stats: Dict[str, int] = {"note_truncated_count": 0}
        result, error = tool_mod.process_pair(
            self.pair,
            self.project_skills_map,
            self.skillsheet_map,
            self.logger,
            stats,
        )
        self.assertIsNone(error)
        notes = [s["note"] for s in result["required_skills"]] + [
            s["note"] for s in result["optional_skills"]
        ]
        self.assertTrue(all(len(n) <= 30 for n in notes))
        self.assertEqual(stats["note_truncated_count"], 3)

    # ---------------------------------------------------------- stats未指定
    def test_stats_argument_is_optional(self):
        self._stub_llm(
            {
                "required_skills": [
                    {"skill": REQUIRED_SKILL, "match": True, "note": NOTE_31}
                ],
                "optional_skills": [
                    {"skill": OPTIONAL_SKILL, "match": False, "note": NOTE_29}
                ],
            }
        )
        result, error = tool_mod.process_pair(
            self.pair, self.project_skills_map, self.skillsheet_map, self.logger
        )
        self.assertIsNone(error)
        self.assertEqual(len(result["required_skills"][0]["note"]), 30)


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""
07-1 表示用note文字数正規化 / schema検証契約の focused test

**test対象は本番runnerが実行する active normalized版**
  07-1_requirement_skill_ai_matching/00_tool/normalized/requirement_skill_ai_matching.py

確認内容:
- note 31文字→30文字へ切り詰めて成功、30/29文字は不変で成功
- note空 / null / 非string / skill文言不一致 / match非bool(1 / 0 / "true") /
  field欠落 は invalid_output_schema のまま（schema検証を緩めない）
- JSON parse失敗のみ llm_parse_error（parse errorとschema errorの分離）
- 複数skillのnote超過が全件切り詰められ、実truncate数が記録されること
- runnerが実行するpathとtest対象pathが一致すること（wiring）

LLMは呼び出さず call_llm をstubに差し替える。本番成果物には触れない。

実行:
  python3 07-1_requirement_skill_ai_matching/00_tool/test_note_length_normalization.py
"""

import importlib.util
import json
import logging
import re
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

STEP_DIR = PROJECT_ROOT / "07-1_requirement_skill_ai_matching"
# 本番runnerが実行するactive実装
ACTIVE_SCRIPT = STEP_DIR / "00_tool/normalized/requirement_skill_ai_matching.py"
# runnerに接続されていないraw版（bool契約の乖離防止のためだけに参照する）
RAW_SCRIPT = STEP_DIR / "00_tool/requirement_skill_ai_matching.py"
RUNNERS = (
    PROJECT_ROOT / "00_pipeline/00_tool/run_full_pipeline.sh",
    PROJECT_ROOT / "00_pipeline/00_tool/run_full_pipeline_master.sh",
)


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


active_mod = load_module("active_normalized_07_1_under_test", ACTIVE_SCRIPT)
raw_mod = load_module("raw_07_1_for_parity_check", RAW_SCRIPT)

from common.logger import get_logger  # noqa: E402

PROJECT_MID = "P0001"
RESOURCE_MID = "R0001"
REQUIRED_SKILL = "AWS 3年以上"
OPTIONAL_SKILL = "Terraform 1年以上"

NOTE_31 = "AWS Certified AI Practitioner取得"  # 31文字（20260817 runの実例）
NOTE_30 = "A" * 30
NOTE_29 = "A" * 29


class ActiveWiringTestCase(unittest.TestCase):
    """runnerが実行する実装をtestしていることの確認。"""

    def test_runners_execute_normalized_implementation(self):
        expected = "07-1_requirement_skill_ai_matching/00_tool/normalized/requirement_skill_ai_matching.py"
        for runner in RUNNERS:
            text = runner.read_text(encoding="utf-8")
            active_lines = [
                line
                for line in text.splitlines()
                if re.match(r"\s*run_step\s", line)
                and "requirement_skill_ai_matching.py" in line
                and "07-1" in line
            ]
            self.assertEqual(
                len(active_lines), 1, f"{runner.name}: 07-1のactive run_stepが1行でない"
            )
            self.assertIn(expected, active_lines[0], f"{runner.name}: normalized版でない")

    def test_module_under_test_is_the_runner_target(self):
        self.assertEqual(Path(active_mod.__file__).resolve(), ACTIVE_SCRIPT.resolve())

    def test_active_implementation_uses_normalized_skillsheet_input(self):
        self.assertIn(
            "04-2_normalize_skillsheets_text", str(active_mod.INPUT_SKILLSHEETS)
        )
        self.assertEqual(active_mod.NOTE_MAX_CHARS, 30)


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
        self._original_call_llm = active_mod.call_llm

    def tearDown(self) -> None:
        active_mod.call_llm = self._original_call_llm
        logging.disable(logging.NOTSET)

    def _stub_llm_response(self, response: Dict[str, Any]) -> None:
        def _fake_call_llm(*args: Any, **kwargs: Any) -> Dict[str, Any]:
            return response

        active_mod.call_llm = _fake_call_llm

    def _stub_llm_raise(self, exc: Exception) -> None:
        def _fake_call_llm(*args: Any, **kwargs: Any) -> Dict[str, Any]:
            raise exc

        active_mod.call_llm = _fake_call_llm

    def _process(self):
        stats: Dict[str, int] = {"note_truncated_count": 0}
        result, error = active_mod.process_pair(
            self.pair,
            self.project_skills_map,
            self.skillsheet_map,
            self.logger,
            stats,
        )
        return result, error, stats

    def _run(self, required_notes: List[Any], optional_notes: List[Any], **overrides):
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
        self._stub_llm_response(response)
        return self._process()

    # ---------------------------------------------------------- 長さ境界
    def test_note_31_chars_is_truncated_and_pair_succeeds(self):
        result, error, stats = self._run([NOTE_31], [NOTE_29])
        self.assertIsNone(error)
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

    # ---------------------------------------------------------- 不正値は救済しない
    def test_empty_note_is_invalid_output_schema(self):
        result, error, stats = self._run([""], [NOTE_29])
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "invalid_output_schema")
        self.assertEqual(stats["note_truncated_count"], 0)

    def test_null_note_is_invalid_output_schema(self):
        result, error, _ = self._run([None], [NOTE_29])
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "invalid_output_schema")

    def test_non_string_note_is_invalid_output_schema(self):
        result, error, _ = self._run([12345], [NOTE_29])
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "invalid_output_schema")

    def test_skill_text_mismatch_is_invalid_output_schema(self):
        result, error, _ = self._run(
            [NOTE_29], [NOTE_29], required_skill="AWS 5年以上（改変）"
        )
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "invalid_output_schema")

    def test_match_string_true_is_invalid_output_schema(self):
        result, error, _ = self._run([NOTE_29], [NOTE_29], required_match="true")
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "invalid_output_schema")

    def test_match_int_1_is_invalid_output_schema(self):
        result, error, _ = self._run([NOTE_29], [NOTE_29], required_match=1)
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "invalid_output_schema")

    def test_match_int_0_is_invalid_output_schema(self):
        result, error, _ = self._run([NOTE_29], [NOTE_29], required_match=0)
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "invalid_output_schema")

    def test_match_null_is_invalid_output_schema(self):
        result, error, _ = self._run([NOTE_29], [NOTE_29], required_match=None)
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "invalid_output_schema")

    def test_missing_note_field_is_invalid_output_schema(self):
        self._stub_llm_response(
            {
                "required_skills": [{"skill": REQUIRED_SKILL, "match": True}],
                "optional_skills": [
                    {"skill": OPTIONAL_SKILL, "match": False, "note": NOTE_29}
                ],
            }
        )
        result, error, _ = self._process()
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "invalid_output_schema")

    def test_missing_top_level_key_is_invalid_output_schema(self):
        self._stub_llm_response(
            {
                "required_skills": [
                    {"skill": REQUIRED_SKILL, "match": True, "note": NOTE_29}
                ]
            }
        )
        result, error, _ = self._process()
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "invalid_output_schema")

    # ---------------------------------------------------------- parse error との区別
    def test_malformed_json_is_llm_parse_error(self):
        self._stub_llm_raise(
            ValueError("OpenAI APIレスポンスJSON不正: Expecting value: line 1 column 1")
        )
        result, error, _ = self._process()
        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "llm_parse_error")

    def test_schema_error_and_parse_error_types_are_distinct(self):
        self._stub_llm_response(
            {
                "required_skills": [{"skill": REQUIRED_SKILL, "match": True}],
                "optional_skills": [],
            }
        )
        _, schema_error, _ = self._process()
        self._stub_llm_raise(ValueError("OpenAI APIレスポンスJSON不正"))
        _, parse_error, _ = self._process()
        self.assertEqual(schema_error["error_type"], "invalid_output_schema")
        self.assertEqual(parse_error["error_type"], "llm_parse_error")
        self.assertNotEqual(schema_error["error_type"], parse_error["error_type"])

    # ---------------------------------------------------------- 複数truncate
    def test_multiple_notes_truncated_and_counted(self):
        self.project_skills_map[PROJECT_MID]["required_skills"] = [
            {"skill": REQUIRED_SKILL},
            {"skill": "Python 3年以上"},
        ]
        self._stub_llm_response(
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
        result, error, stats = self._process()
        self.assertIsNone(error)
        notes = [s["note"] for s in result["required_skills"]] + [
            s["note"] for s in result["optional_skills"]
        ]
        self.assertTrue(all(len(n) <= 30 for n in notes))
        self.assertEqual(stats["note_truncated_count"], 3)

    def test_stats_argument_is_optional(self):
        self._stub_llm_response(
            {
                "required_skills": [
                    {"skill": REQUIRED_SKILL, "match": True, "note": NOTE_31}
                ],
                "optional_skills": [
                    {"skill": OPTIONAL_SKILL, "match": False, "note": NOTE_29}
                ],
            }
        )
        result, error = active_mod.process_pair(
            self.pair, self.project_skills_map, self.skillsheet_map, self.logger
        )
        self.assertIsNone(error)
        self.assertEqual(len(result["required_skills"][0]["note"]), 30)


class RunMetadataNoteTruncatedCountTestCase(unittest.TestCase):
    """active normalized版のmain()がrun_metadataへnote_truncated_countを出すこと。

    本番runnerが実行するのと同じmain()を、一時ディレクトリ上のfixtureと
    stub化したcall_llmで走らせる（LLM呼び出し・本番成果物への書き込みなし）。
    """

    def setUp(self) -> None:
        logging.disable(logging.CRITICAL)
        self.tmp = Path(tempfile.mkdtemp(prefix="normalized_07_1_main_test_"))
        self._saved = {
            name: getattr(active_mod, name)
            for name in (
                "STEP_DIR",
                "INPUT_PAIRS",
                "INPUT_PROJECT_SKILLS",
                "INPUT_SKILLSHEETS",
                "OUTPUT_RESULT",
                "OUTPUT_ERROR",
                "OUTPUT_RUN_METADATA",
                "call_llm",
            )
        }
        self._saved_argv = sys.argv

        active_mod.STEP_DIR = self.tmp
        active_mod.INPUT_PAIRS = self.tmp / "duplicate_proposal_check.jsonl"
        active_mod.INPUT_PROJECT_SKILLS = self.tmp / "project_skills.jsonl"
        active_mod.INPUT_SKILLSHEETS = self.tmp / "normalize_skillsheets_text.jsonl"
        active_mod.OUTPUT_RESULT = self.tmp / "01_result/requirement_skill_ai_matching.jsonl"
        active_mod.OUTPUT_ERROR = (
            self.tmp / "01_result/99_error_requirement_skill_ai_matching.jsonl"
        )
        active_mod.OUTPUT_RUN_METADATA = self.tmp / "01_result/run_metadata.json"
        sys.argv = ["requirement_skill_ai_matching.py"]

        self._write_jsonl(
            active_mod.INPUT_PAIRS,
            [
                {
                    "project_info": {"message_id": PROJECT_MID},
                    "resource_info": {"message_id": RESOURCE_MID},
                }
            ],
        )
        self._write_jsonl(
            active_mod.INPUT_PROJECT_SKILLS,
            [
                {
                    "message_id": PROJECT_MID,
                    "required_skills": [{"skill": REQUIRED_SKILL}],
                    "optional_skills": [{"skill": OPTIONAL_SKILL}],
                }
            ],
        )
        self._write_jsonl(
            active_mod.INPUT_SKILLSHEETS,
            [
                {
                    "message_id": RESOURCE_MID,
                    "success": True,
                    "skillsheet": "AWS設計構築経験あり。Terraform経験あり。",
                    "source": "normalized",
                }
            ],
        )

    def tearDown(self) -> None:
        for name, value in self._saved.items():
            setattr(active_mod, name, value)
        sys.argv = self._saved_argv
        logging.disable(logging.NOTSET)
        shutil.rmtree(self.tmp, ignore_errors=True)

    @staticmethod
    def _write_jsonl(path: Path, records: List[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _run_main_with_notes(self, required_note: str, optional_note: str) -> dict:
        def _fake_call_llm(*args: Any, **kwargs: Any) -> Dict[str, Any]:
            return {
                "required_skills": [
                    {"skill": REQUIRED_SKILL, "match": True, "note": required_note}
                ],
                "optional_skills": [
                    {"skill": OPTIONAL_SKILL, "match": False, "note": optional_note}
                ],
            }

        active_mod.call_llm = _fake_call_llm
        active_mod.main()
        return json.loads(
            active_mod.OUTPUT_RUN_METADATA.read_text(encoding="utf-8")
        )

    def test_run_metadata_records_truncated_notes(self):
        metadata = self._run_main_with_notes(NOTE_31, "D" * 40)
        self.assertEqual(metadata["note_truncated_count"], 2)
        self.assertEqual(metadata["input_count"], 1)
        self.assertEqual(metadata["processed_count"], 1)
        results = [
            json.loads(line)
            for line in active_mod.OUTPUT_RESULT.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0]["required_skills"][0]["note"]), 30)

    def test_run_metadata_records_zero_when_no_truncation(self):
        metadata = self._run_main_with_notes(NOTE_29, NOTE_30)
        self.assertEqual(metadata["note_truncated_count"], 0)


class ValidatorParityTestCase(unittest.TestCase):
    """active normalized版とraw版で validator 契約が乖離していないこと。"""

    ORIGINAL = [{"skill": REQUIRED_SKILL}]

    def _validate(self, mod, match: Any, note: Any):
        return mod._validate_skills(
            self.ORIGINAL,
            [{"skill": REQUIRED_SKILL, "match": match, "note": note}],
            "required_skills",
        )

    def test_both_reject_int_match(self):
        for mod in (active_mod, raw_mod):
            self.assertIsNotNone(self._validate(mod, 1, NOTE_29))
            self.assertIsNotNone(self._validate(mod, 0, NOTE_29))

    def test_both_accept_bool_match(self):
        for mod in (active_mod, raw_mod):
            self.assertIsNone(self._validate(mod, True, NOTE_29))
            self.assertIsNone(self._validate(mod, False, NOTE_29))

    def test_both_truncate_only_long_non_empty_notes(self):
        for mod in (active_mod, raw_mod):
            skills = [
                {"skill": REQUIRED_SKILL, "match": True, "note": NOTE_31},
                {"skill": OPTIONAL_SKILL, "match": True, "note": ""},
                {"skill": "Java", "match": True, "note": None},
            ]
            truncated = mod._normalize_note_lengths(skills)
            self.assertEqual(truncated, 1)
            self.assertEqual(len(skills[0]["note"]), 30)
            self.assertEqual(skills[1]["note"], "")
            self.assertIsNone(skills[2]["note"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

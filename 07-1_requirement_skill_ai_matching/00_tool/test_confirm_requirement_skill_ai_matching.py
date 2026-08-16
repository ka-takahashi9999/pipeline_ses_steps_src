"""
07-1 confirm の失敗系error_type受理範囲の focused test

- P0で正式導入済みの project_skill_count_exceeded / llm_output_truncated を
  「仕様どおり分類・記録されたerror」としてconfirmが受理すること
- 未知のerror_type は従来どおりNGになること
- 既存の missing_resource_skillsheet 等の判定を壊していないこと

本番成果物には触れず、一時ディレクトリ上のfixtureに対してconfirmのmain()を実行する。
LLM呼び出し・full Pipeline実行は行わない。

実行:
  python3 07-1_requirement_skill_ai_matching/00_tool/test_confirm_requirement_skill_ai_matching.py
"""

import importlib.util
import json
import logging
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from typing import List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

CONFIRM_SCRIPT = (
    PROJECT_ROOT
    / "07-1_requirement_skill_ai_matching/02_confirm/confirm_requirement_skill_ai_matching.py"
)


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


confirm_mod = load_module("confirm_07_1_under_test", CONFIRM_SCRIPT)


def write_jsonl(path: Path, records: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


PROJECT_MID = "P0001"
RESOURCE_MID = "R0001"
SKILL_TEXT = "Java 3年以上"


def error_record(error_type: str, index: int) -> dict:
    return {
        "project_info": {"message_id": f"P{index:04d}"},
        "resource_info": {"message_id": f"R{index:04d}"},
        "error_type": error_type,
        "error_message": f"{error_type} が発生",
    }


class Confirm07_1ErrorTypeTestCase(unittest.TestCase):
    def setUp(self) -> None:
        logging.disable(logging.CRITICAL)
        self.tmp = Path(tempfile.mkdtemp(prefix="confirm_07_1_test_"))

        self.pairs_file = self.tmp / "duplicate_proposal_check.jsonl"
        self.project_skills_file = self.tmp / "extract_project_required_skills.jsonl"
        self.result_file = self.tmp / "requirement_skill_ai_matching.jsonl"
        self.error_file = self.tmp / "99_error_requirement_skill_ai_matching.jsonl"
        self.run_metadata_file = self.tmp / "run_metadata.json"
        self.confirm_result_file = self.tmp / "confirm_result.txt"

        confirm_mod.INPUT_PAIRS = self.pairs_file
        confirm_mod.INPUT_PROJECT_SKILLS = self.project_skills_file
        confirm_mod.OUTPUT_RESULT = self.result_file
        confirm_mod.OUTPUT_ERROR = self.error_file
        confirm_mod.RUN_METADATA = self.run_metadata_file
        confirm_mod.CONFIRM_RESULT = self.confirm_result_file

        write_jsonl(
            self.project_skills_file,
            [
                {
                    "message_id": PROJECT_MID,
                    "required_skills": [{"skill": SKILL_TEXT}],
                    "optional_skills": [],
                }
            ],
        )
        write_jsonl(
            self.result_file,
            [
                {
                    "project_info": {"message_id": PROJECT_MID},
                    "resource_info": {"message_id": RESOURCE_MID},
                    "required_skills": [
                        {"skill": SKILL_TEXT, "match": True, "note": "経験あり"}
                    ],
                    "optional_skills": [],
                    "evaluation_meta": {"llm_model": "test-model"},
                }
            ],
        )

    def tearDown(self) -> None:
        logging.disable(logging.NOTSET)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def setup_errors(self, error_types: List[str]) -> None:
        errors = [error_record(etype, i + 2) for i, etype in enumerate(error_types)]
        write_jsonl(self.error_file, errors)
        # 入力ペア数 = 正常系 + 失敗系
        pairs = [
            {
                "project_info": {"message_id": PROJECT_MID},
                "resource_info": {"message_id": RESOURCE_MID},
            }
        ] + [
            {
                "project_info": {"message_id": e["project_info"]["message_id"]},
                "resource_info": {"message_id": e["resource_info"]["message_id"]},
            }
            for e in errors
        ]
        write_jsonl(self.pairs_file, pairs)
        self.run_metadata_file.write_text(
            json.dumps(
                {
                    "input_count": len(pairs),
                    "processed_count": len(pairs),
                    "limit": None,
                    "is_limited_run": False,
                }
            ),
            encoding="utf-8",
        )

    def run_confirm(self) -> int:
        try:
            confirm_mod.main()
        except SystemExit as e:
            return int(e.code or 0)
        return 0

    def result_text(self) -> str:
        return self.confirm_result_file.read_text(encoding="utf-8")

    def test_project_skill_count_exceeded_is_accepted(self):
        """project_skill_count_exceeded → confirm上、未知error扱いにならない"""
        self.setup_errors(["project_skill_count_exceeded"])
        self.assertEqual(self.run_confirm(), 0)
        text = self.result_text()
        self.assertIn("project_skill_count_exceeded: 1件", text)
        self.assertNotIn("許可外error_type", text)
        self.assertIn("【結果】OK", text)

    def test_llm_output_truncated_is_accepted(self):
        """llm_output_truncated → confirm上、未知error扱いにならない"""
        self.setup_errors(["llm_output_truncated"])
        self.assertEqual(self.run_confirm(), 0)
        text = self.result_text()
        self.assertIn("llm_output_truncated: 1件", text)
        self.assertNotIn("許可外error_type", text)
        self.assertIn("【結果】OK", text)

    def test_existing_error_types_still_accepted(self):
        """既存の失敗系（missing_resource_skillsheet 等）の判定を壊していない"""
        self.setup_errors(
            [
                "missing_resource_skillsheet",
                "missing_project_required_skills",
                "llm_call_error",
                "llm_parse_error",
                "invalid_output_schema",
            ]
        )
        self.assertEqual(self.run_confirm(), 0)
        self.assertNotIn("許可外error_type", self.result_text())

    def test_unknown_error_type_is_ng(self):
        """未知のerror_type → 従来どおりNG"""
        self.setup_errors(["some_unknown_error_type"])
        self.assertEqual(self.run_confirm(), 1)
        text = self.result_text()
        self.assertIn("許可外error_type='some_unknown_error_type'", text)
        self.assertIn("【結果】NG", text)

    def test_p0_error_types_are_in_allowed_set(self):
        """P0の正規error typeがALLOWED_ERROR_TYPESに含まれる"""
        self.assertIn("project_skill_count_exceeded", confirm_mod.ALLOWED_ERROR_TYPES)
        self.assertIn("llm_output_truncated", confirm_mod.ALLOWED_ERROR_TYPES)
        self.assertNotIn("some_unknown_error_type", confirm_mod.ALLOWED_ERROR_TYPES)


if __name__ == "__main__":
    unittest.main(verbosity=2)

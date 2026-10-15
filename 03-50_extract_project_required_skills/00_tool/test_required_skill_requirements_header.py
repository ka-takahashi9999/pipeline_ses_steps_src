#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""「必須スキル・要件」構造見出しに限定したfocused test。"""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


TOOL_DIR = Path(__file__).resolve().parent
STEP_DIR = TOOL_DIR.parent
sys.path.insert(0, str(TOOL_DIR))

import extract_project_required_skills as target


TARGET_ID = "s3_dd28ed08a7b11094175a3344eb65bf304b05a44b7530b24fca5a8cdde10fdf24"
SYNTHETIC_REQUIRED = ["必須項目A", "必須項目B", "必須項目C", "必須項目D", "必須項目E"]
SYNTHETIC_OPTIONAL = ["尚可項目A"]


def _read_jsonl(path):
    with Path(path).open(encoding="utf-8") as file_obj:
        return [json.loads(line) for line in file_obj if line.strip()]


class RequiredSkillRequirementsHeaderTest(unittest.TestCase):
    def test_exact_structural_header_and_optional_boundary(self):
        body = (
            "  ■必須スキル・要件■  \n"
            + "\n".join("・" + skill for skill in SYNTHETIC_REQUIRED)
            + "\n■尚可スキル■\n・"
            + SYNTHETIC_OPTIONAL[0]
        )
        required, optional = target.rule_extract_skills(body)
        self.assertEqual([item["skill"] for item in required], SYNTHETIC_REQUIRED)
        self.assertEqual([item["skill"] for item in optional], SYNTHETIC_OPTIONAL)

    def test_similar_text_is_not_new_header(self):
        for line in (
            "要件",
            "必須スキル要件",
            "必須スキル・要件を満たす方",
            "■必須スキル・要件詳細■",
        ):
            with self.subTest(line=line):
                self.assertNotEqual(target._classify_line(line)[0], "required_header")

    def test_existing_required_headers_remain_unchanged(self):
        # これらは新規ruleの対象ではなく、従来からの見出し。
        for line in ("必須要件", "スキル要件", "必須スキル・経験"):
            with self.subTest(line=line):
                self.assertEqual(target._classify_line(line), ("required_header", ""))

    def test_real_target_extracts_five_required_and_one_optional(self):
        cleaned = {
            record["message_id"]: record
            for record in _read_jsonl(target.INPUT_CLEANED)
        }
        body = cleaned[TARGET_ID]["body_text"]
        required, optional, method = target.extract_skills(TARGET_ID, body)

        self.assertEqual(method, "rule")
        self.assertEqual(len(required), 5)
        self.assertEqual(len(optional), 1)

    def test_latest_projects_change_only_the_target(self):
        project_ids = [
            record["message_id"] for record in _read_jsonl(target.INPUT_PROJECTS)
        ]
        cleaned = {
            record["message_id"]: record
            for record in _read_jsonl(target.INPUT_CLEANED)
        }
        previous = {}
        for output_name in (target.OUTPUT_EXTRACTED, target.OUTPUT_NULL):
            for record in _read_jsonl(STEP_DIR / "01_result" / output_name):
                previous[record["message_id"]] = record

        changes = {}
        with patch.object(target, "logger"):
            for message_id in project_ids:
                body = (cleaned.get(message_id) or {}).get("body_text", "")
                required, optional, method = target.extract_skills(message_id, body)
                current = target.build_record(
                    message_id, required, optional, method=method
                )
                if current != previous[message_id]:
                    changes[message_id] = current

        self.assertEqual(len(project_ids), 1174)
        self.assertEqual(set(changes), {TARGET_ID})
        self.assertEqual(len(changes[TARGET_ID]["required_skills"]), 5)
        self.assertEqual(len(changes[TARGET_ID]["optional_skills"]), 1)
        self.assertEqual(
            changes[TARGET_ID]["optional_skills"],
            previous[TARGET_ID]["optional_skills"],
        )


if __name__ == "__main__":
    unittest.main()

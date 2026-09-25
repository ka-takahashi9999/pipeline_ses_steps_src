#!/usr/bin/env python3
"""EBA系の括弧付きラベル単独行に限定したFocused Test。"""

import copy
import importlib.util
import json
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


STEP = Path(__file__).resolve().parent.parent
ROOT = STEP.parent
sys.path.insert(0, str(STEP / "00_tool"))
import extract_project_required_skills as target
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl


TARGET_IDS = {"1a0d24c16ec48eec", "1a0d1b423966b8f2"}


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class EbaParenthesizedSectionHeadersTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cleaned = read_jsonl_as_dict(target.INPUT_CLEANED)

    def test_label_only_lines_start_sections_before_one_shot(self):
        self.assertEqual(
            target._classify_line(
                "（必須）：", allow_parenthesized_label_sections=True
            ),
            ("required_header", ""),
        )
        self.assertEqual(
            target._classify_line(
                "（尚可）：", allow_parenthesized_label_sections=True
            ),
            ("optional_header", ""),
        )
        self.assertEqual(
            target._classify_line("（必須）Java"),
            ("required_skill", "Java"),
        )
        self.assertEqual(
            target._classify_line("（尚可）AWS"),
            ("optional_skill", "AWS"),
        )

    def test_real_two_extract_seven_required_and_eight_optional(self):
        for mid in TARGET_IDS:
            with self.subTest(message_id=mid):
                req, opt = target.rule_extract_skills(self.cleaned[mid]["body_text"])
                self.assertEqual((len(req), len(opt)), (7, 8))
                self.assertNotIn(":", [item["skill"] for item in req + opt])
                self.assertFalse(any(
                    text in item["skill"]
                    for item in req + opt
                    for text in ("責任感の強い方", "主体的に課題解決できる方")
                ))

    def test_person_profile_stops_section(self):
        req, opt = target.rule_extract_skills(
            "■スキル\n（必須）：\n・Java開発経験\n（尚可）：\n・AWS構築経験\n"
            "■人物像：\n・Python開発経験"
        )
        self.assertEqual(req, [target._make_skill("Java開発経験")])
        self.assertEqual(opt, [target._make_skill("AWS構築経験")])

    def test_latest_811_memory_replay_has_only_two_changes_and_confirms(self):
        result_dir = STEP / "01_result"
        old_extracted = read_jsonl_as_list(str(result_dir / target.OUTPUT_EXTRACTED))
        old_null = read_jsonl_as_list(str(result_dir / target.OUTPUT_NULL))
        old_rule_empty = read_jsonl_as_list(str(result_dir / target.OUTPUT_RULE_EMPTY))
        self.assertEqual(
            (len(old_extracted), len(old_null), len(old_rule_empty)),
            (796, 15, 14),
        )

        captured = {}
        with patch.object(
            target,
            "ensure_result_dirs",
            return_value={"result": result_dir, "execution_time": Path("/tmp")},
        ), patch.object(
            target,
            "write_jsonl",
            side_effect=lambda path, rows: captured.update(
                {Path(path).name: copy.deepcopy(rows)}
            ),
        ), patch.object(target, "write_execution_time"), patch.object(target, "logger"):
            target.main()

        extracted = captured[target.OUTPUT_EXTRACTED]
        null_records = captured[target.OUTPUT_NULL]
        rule_empty = captured[target.OUTPUT_RULE_EMPTY]
        self.assertEqual((len(extracted), len(null_records), len(rule_empty)), (798, 13, 12))

        new_by_id = {
            row["message_id"]: row for row in extracted + null_records
        }
        project_ids = [
            row["message_id"] for row in read_jsonl_as_list(target.INPUT_PROJECTS)
        ]
        never_match = re.compile(r"(?!)")
        baseline_by_id = {}
        with patch.object(
            target, "_PAREN_LABEL_ONLY_REQ_RE", never_match
        ), patch.object(
            target, "_PAREN_LABEL_ONLY_OPT_RE", never_match
        ), patch.object(target, "logger"):
            for mid in project_ids:
                body = (self.cleaned.get(mid) or {}).get("body_text", "")
                req, opt, method = target.extract_skills(mid, body)
                baseline_by_id[mid] = target.build_record(mid, req, opt, method)

        self.assertEqual(set(baseline_by_id), set(new_by_id))
        changed_ids = {
            mid for mid in baseline_by_id
            if baseline_by_id[mid] != new_by_id[mid]
        }
        self.assertEqual(changed_ids, TARGET_IDS)
        for mid in TARGET_IDS:
            self.assertEqual(
                (
                    len(new_by_id[mid]["required_skills"]),
                    len(new_by_id[mid]["optional_skills"]),
                ),
                (7, 8),
            )

        confirm = _load_module(
            "confirm_03_50_eba",
            STEP / "02_confirm" / "confirm_extract_project_required_skills.py",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_result = Path(temp_dir)
            write_jsonl(str(temp_result / target.OUTPUT_EXTRACTED), extracted)
            write_jsonl(str(temp_result / target.OUTPUT_NULL), null_records)
            write_jsonl(str(temp_result / target.OUTPUT_RULE_EMPTY), rule_empty)
            with patch.object(
                confirm,
                "get_result_path",
                side_effect=lambda _step, name: str(temp_result / name),
            ), patch.object(confirm, "logger") as confirm_logger:
                confirm.main()
                self.assertTrue(any(
                    "confirm OK" in str(call)
                    for call in confirm_logger.ok.call_args_list
                ))

    def test_03_51_existing_classifier_handles_both(self):
        step_03_51 = _load_module(
            "extract_project_required_skills_list_eba",
            ROOT / "03-51_extract_project_required_skills_list" / "00_tool"
            / "extract_project_required_skills_list.py",
        )
        skill_entries = step_03_51.load_skill_list(step_03_51.SKILL_DICT_PATH)
        phase_entries = step_03_51.load_phase_map(step_03_51.PHASE_DICT_PATH)
        for mid in TARGET_IDS:
            with self.subTest(message_id=mid):
                req, opt = target.rule_extract_skills(self.cleaned[mid]["body_text"])
                classified = step_03_51.build_record(
                    mid, req, opt, skill_entries, phase_entries
                )
                self.assertEqual(
                    classified["required_skill_keywords"],
                    ["TypeScript", "React", "Next.js", "SQL", "Node.js"],
                )
                self.assertEqual(
                    classified["required_phase_keywords"],
                    ["開発", "製造", "基本設計", "詳細設計"],
                )
                self.assertEqual(
                    classified["optional_skill_keywords"],
                    ["NestJS", "AWS", "PostgreSQL", "Redis"],
                )
                self.assertEqual(
                    classified["optional_phase_keywords"],
                    ["開発", "移行"],
                )

    def test_targets_do_not_reach_06_11(self):
        paths = (
            ROOT / "06-10_match_location" / "01_result" / "matched_pairs_location.jsonl",
            ROOT / "06-11_match_required_skills_list" / "01_result"
            / "matched_pairs_required_skills_list.jsonl",
            ROOT / "06-11_match_required_skills_list" / "01_result"
            / "99_not_matched_pairs_required_skills_list.jsonl",
        )
        for path in paths:
            with self.subTest(path=path.name):
                with path.open(encoding="utf-8") as stream:
                    count = 0
                    for line in stream:
                        if not line.strip():
                            continue
                        row = json.loads(line)
                        project_id = (row.get("project_info") or {}).get("message_id")
                        if project_id in TARGET_IDS:
                            count += 1
                self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()

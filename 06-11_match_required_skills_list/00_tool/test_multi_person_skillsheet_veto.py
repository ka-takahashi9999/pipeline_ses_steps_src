#!/usr/bin/env python3
"""CHECK-05: 04-2 multi-person判定の06-11/06-12 veto Focused Test。"""

import importlib.util
import json
import unittest
from collections import Counter
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
ROOT = TOOL_DIR.parents[1]


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


match = _load_module("match_required_skills_list_veto", TOOL_DIR / "match_required_skills_list.py")
noise = _load_module(
    "filter_required_skills_noise_veto",
    ROOT / "06-12_filter_required_skills_noise" / "00_tool" / "filter_required_skills_noise.py",
)


def _read(path):
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


class MultiPersonSkillsheetVetoTest(unittest.TestCase):
    def test_normalized_record_is_canonical_for_both_steps(self):
        raw = {"skillsheet": "raw-only marker\nCisco\n設計・構築\nリリース"}
        flagged = {
            "success": False,
            "skillsheet": "",
            "cleanup_flags": ["normalized_whitespace", "multi_person_skillsheet"],
        }
        normal = {
            "success": True,
            "skillsheet": "normalized-only marker\nCisco",
            "cleanup_flags": ["normalized_whitespace"],
        }
        failed = {"success": False, "skillsheet": "must-not-be-used"}
        for module in (match, noise):
            with self.subTest(module=module.STEP_NAME):
                self.assertEqual(module.skillsheet_text_for_matching(raw, flagged), "")
                self.assertEqual(
                    module.skillsheet_text_for_matching(raw, normal),
                    normal["skillsheet"],
                )
                self.assertEqual(module.skillsheet_text_for_matching(raw, failed), "")
                self.assertEqual(module.skillsheet_text_for_matching(raw, {}), "")

    def test_email_body_is_still_used_when_skillsheet_is_suppressed(self):
        result = match.judge_match(
            ["Java"], [], [], "", "本人プロフィール: Java / Spring Boot"
        )
        self.assertTrue(result[0])
        self.assertEqual(result[3], ["email_body"])

    def test_current_multi_person_flags_are_all_suppressed(self):
        normalized = {
            row["message_id"]: row
            for row in _read(match.INPUT_NORMALIZED_SKILLSHEET)
        }
        raw = {row["message_id"]: row for row in _read(match.INPUT_SKILLSHEET)}
        body = {row["message_id"]: row for row in _read(match.INPUT_EMAIL_BODY)}
        project = {row["message_id"]: row for row in _read(match.INPUT_PROJECT_KWS)}
        flagged_ids = {
            message_id
            for message_id, row in normalized.items()
            if "multi_person_skillsheet" in (row.get("cleanup_flags") or [])
        }
        self.assertEqual(len(flagged_ids), 5)
        self.assertTrue(all(match.skillsheet_text_for_matching(raw[mid], normalized[mid]) == "" for mid in flagged_ids))
        self.assertTrue(all(noise.skillsheet_text_for_matching(raw[mid], normalized[mid]) == "" for mid in flagged_ids))


if __name__ == "__main__":
    unittest.main()

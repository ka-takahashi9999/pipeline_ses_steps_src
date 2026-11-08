#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""09-3営業向けphase表示のfocused test。"""

import importlib.util
import itertools
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("prepare_sales_mail_context.py")
SPEC = importlib.util.spec_from_file_location("prepare_sales_mail_context", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class PhaseDisplayOrderTest(unittest.TestCase):
    def test_same_phase_set_has_same_display_for_all_permutations(self):
        phases = ["PMO", "テスト", "要件定義", "運用保守"]
        expected = ["要件定義", "テスト", "運用保守", "PMO"]
        for permutation in itertools.permutations(phases):
            with self.subTest(permutation=permutation):
                self.assertEqual(
                    MODULE.build_phase_display_view(list(permutation)), expected
                )

    def test_all_categories_are_displayed_in_fixed_order(self):
        reversed_input = list(reversed(MODULE.PHASE_DISPLAY_ORDER))
        view = MODULE.build_phase_display_view(reversed_input)
        self.assertEqual(view, list(MODULE.PHASE_DISPLAY_ORDER))
        summary, source = MODULE.summarize_resource_skills(
            {"skills": ["Python", "AWS"]}, {"phases": reversed_input}
        )
        self.assertEqual(source, "05-8_extract_resource_skill_category+05-9_extract_resource_phase_category")
        for phase in MODULE.PHASE_DISPLAY_ORDER:
            self.assertIn(phase, summary)
        self.assertLessEqual(len(summary), MODULE.MAX_SUMMARY_LENGTH)
        self.assertFalse(summary.endswith("..."))

    def test_more_than_four_categories_are_not_dropped(self):
        phases = ["PMO", "運用保守", "テスト", "開発", "要件定義"]
        summary, _source = MODULE.summarize_resource_skills(None, {"phases": phases})
        self.assertEqual(
            summary,
            "工程: 要件定義, 開発, テスト, 運用保守, PMO",
        )

    def test_duplicate_and_unknown_categories_are_stable(self):
        phases = ["未知B", "PMO", "未知A", "PMO", "未知B", "テスト"]
        original = list(phases)
        self.assertEqual(
            MODULE.build_phase_display_view(phases),
            ["テスト", "PMO", "未知A", "未知B"],
        )
        self.assertEqual(phases, original)

    def test_null_and_empty_keep_existing_behavior(self):
        self.assertEqual(MODULE.build_phase_display_view(None), [])
        self.assertEqual(MODULE.build_phase_display_view([]), [])
        self.assertEqual(
            MODULE.summarize_resource_skills(None, None), (None, "unavailable")
        )
        self.assertEqual(
            MODULE.summarize_resource_skills({"skills": ["Python"]}, None),
            ("スキル: Python", "05-8_extract_resource_skill_category+05-9_extract_resource_phase_category"),
        )

    def test_06_9_matching_is_order_independent(self):
        phase_tool_dir = (
            MODULE_PATH.parents[2] / "06-9_match_phase_category" / "00_tool"
        )
        sys.path.insert(0, str(phase_tool_dir))
        try:
            spec = importlib.util.spec_from_file_location(
                "match_phase_category_for_display_test",
                phase_tool_dir / "match_phase_category.py",
            )
            match_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(match_module)
        finally:
            sys.path.pop(0)

        self.assertTrue(
            match_module.judge_phase_category_match(
                ["開発", "PMO"], ["PMO", "開発"]
            )
        )
        self.assertTrue(
            match_module.judge_phase_category_match(
                ["PMO", "開発"], ["開発", "PMO"]
            )
        )


if __name__ == "__main__":
    unittest.main()

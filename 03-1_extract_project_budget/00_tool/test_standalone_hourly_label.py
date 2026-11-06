#!/usr/bin/env python3
"""standalone時給labelのunit-aware segment化に対するfocused test。"""

import sys
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
ROOT = TOOL_DIR.parents[1]
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(ROOT))

import extract_project_budget as target


class StandaloneHourlyLabelTest(unittest.TestCase):
    def test_valid_standalone_hourly_fields_are_converted(self):
        cases = (
            "時給：2,100円",
            "■時給：2,100円",
            "時給:2100円",
            "■時給：２，１００円",
            "【時給】2,100円",
        )
        for body in cases:
            with self.subTest(body=body):
                record = target.build_record("fixture", body)
                self.assertEqual(record["unit_price"], 336000)
                self.assertEqual(
                    record["unit_price_sub_infor"]["reason"], "hourly-yen"
                )
                self.assertEqual(
                    record["unit_price_sub_infor"]["kind"], "monthly"
                )

    def test_unit_conflicts_and_non_amounts_are_rejected(self):
        cases = (
            "時給：65万円",
            "時給：65万",
            "時給：160時間",
            "単価：62",
        )
        for body in cases:
            with self.subTest(body=body):
                self.assertIsNone(target.build_record("fixture", body)["unit_price"])

    def test_unrelated_numbers_are_not_price_segments(self):
        cases = (
            "年齢：45歳",
            "経験年数：10年",
            "稼働率：80%",
            "精算幅：140-180時間",
            "勤務時間：9:00-18:00",
        )
        for body in cases:
            with self.subTest(body=body):
                self.assertIsNone(target.build_record("fixture", body)["unit_price"])

    def test_existing_monthly_and_range_forms_are_preserved(self):
        cases = {
            "単価：70万円": (700000, None),
            "単価：60〜70万円": (
                650000,
                {"min": 600000, "max": 700000},
            ),
            "単価：〜80万円": (800000, {"min": None, "max": 800000}),
            "単価：70万円〜": (700000, {"min": 700000, "max": None}),
        }
        for body, (price, price_range) in cases.items():
            with self.subTest(body=body):
                record = target.build_record("fixture", body)
                self.assertEqual(record["unit_price"], price)
                self.assertEqual(
                    record["unit_price_sub_infor"]["range"], price_range
                )

    def test_existing_open_high_hourly_is_preserved(self):
        record = target.build_record(
            "fixture", "金額：2,500円まで ※時給制・スキル見合い"
        )
        self.assertEqual(record["unit_price"], 400000)
        self.assertEqual(
            record["unit_price_sub_infor"]["range"],
            {"min": None, "max": 400000},
        )
        self.assertEqual(
            record["unit_price_sub_infor"]["reason"],
            "hourly-yen-open-high",
        )


if __name__ == "__main__":
    unittest.main()

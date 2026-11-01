#!/usr/bin/env python3
"""CHECK-03: project price表記揺れのfocused tests。"""

import unittest

import extract_project_budget as target


class Check03ProjectPriceFormatsTest(unittest.TestCase):
    def test_explicit_price_precedes_skill_based_fallback(self):
        explicit = target.build_record(
            "fixture",
            "単価についてはスキルによって上振れ可能です。\n"
            "【単価】スキル見合い（80万前後目安）",
        )
        fallback = target.build_record("fixture", "単価：スキル見合い")
        self.assertEqual(explicit["unit_price"], 800000)
        self.assertEqual(
            explicit["unit_price_sub_infor"]["reason"], "man-zenpo"
        )
        self.assertEqual(fallback["unit_price"], target.SKILL_BASED_PRICE)
        self.assertEqual(
            fallback["unit_price_sub_infor"]["reason"], "skill-based"
        )

    def test_limited_new_monthly_formats(self):
        cases = {
            "【予算】：55万〜MAX65万円": (
                600000,
                {"min": 550000, "max": 650000},
            ),
            "単価：70万円から80万円": (
                750000,
                {"min": 700000, "max": 800000},
            ),
            r"月額：\620,000-": (620000, None),
            "月額：¥620,000": (620000, None),
            "月額：￥620,000": (620000, None),
        }
        for body, (expected, expected_range) in cases.items():
            with self.subTest(body=body):
                record = target.build_record("fixture", body)
                self.assertEqual(record["unit_price"], expected)
                self.assertEqual(
                    record["unit_price_sub_infor"]["range"], expected_range
                )

    def test_new_rules_do_not_expand_outside_price_context_or_metadata_scope(self):
        path = target.build_record("fixture", r"保存先 C:\620,000-folder")
        standalone_max = target.build_record("fixture", "単価：MAX65万円")
        above = target.build_record("fixture", "単価：70万円以上")
        self.assertIsNone(path["unit_price"])
        self.assertEqual(standalone_max["unit_price"], 650000)
        self.assertIsNone(standalone_max["unit_price_sub_infor"]["range"])
        self.assertEqual(above["unit_price"], 700000)
        self.assertIsNone(above["unit_price_sub_infor"]["range"])

    def test_comma_yen_range_uses_midpoint(self):
        record = target.build_record("fixture", "【単 価】650,000~700,000円/月")
        self.assertEqual(record["unit_price"], 675000)
        self.assertEqual(
            record["unit_price_sub_infor"]["range"],
            {"min": 650000, "max": 700000},
        )
        self.assertEqual(
            record["unit_price_sub_infor"]["reason"],
            "range-closed-comma-yen",
        )

    def test_comma_yen_single_is_not_a_range(self):
        record = target.build_record("fixture", "単価:700,000円/月")
        self.assertEqual(record["unit_price"], 700000)
        self.assertIsNone(record["unit_price_sub_infor"]["range"])

    def test_explicit_daily_yen_uses_existing_conversion(self):
        cases = {
            "単価:日給11,000円": 220000,
            "■単価■\n10,000/日": 200000,
            "■単価■\n一律11,000円/日(交通費込み)": 220000,
        }
        for body, expected in cases.items():
            with self.subTest(body=body):
                self.assertEqual(target.rule_extract(body)[0], expected)

    def test_existing_hourly_single_and_range_are_preserved(self):
        cases = {
            "単価:時給 4,000円": (640000, None),
            "単価:3,500円〜6,000円/時": (
                760000,
                {"min": 560000, "max": 960000},
            ),
            "金額:2,500円まで ※時給制・スキル見合い": (
                400000,
                {"min": None, "max": 400000},
            ),
        }
        for body, (expected, expected_range) in cases.items():
            with self.subTest(body=body):
                record = target.build_record("fixture", body)
                self.assertEqual(record["unit_price"], expected)
                self.assertEqual(
                    record["unit_price_sub_infor"]["range"], expected_range
                )

    def test_monthly_amount_precedes_hourly_equivalent(self):
        record = target.build_record(
            "fixture", "単価:184万円(時給11000円相当)"
        )
        self.assertEqual(record["unit_price"], 1840000)
        self.assertEqual(
            record["unit_price_sub_infor"]["reason"],
            "monthly-with-hourly-equivalent",
        )

    def test_held_ambiguous_values_keep_existing_behavior(self):
        contradictory = target.build_record(
            "fixture", "時給：65万円※応相談"
        )
        multiple = target.build_record(
            "fixture", "単価75万\n金額59万"
        )
        self.assertEqual(contradictory["unit_price"], 650000)
        self.assertEqual(
            contradictory["unit_price_sub_infor"]["reason"], "man"
        )
        self.assertEqual(multiple["unit_price"], 750000)
        self.assertEqual(multiple["unit_price_sub_infor"]["reason"], "man")


if __name__ == "__main__":
    unittest.main()

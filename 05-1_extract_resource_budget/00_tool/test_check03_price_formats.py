#!/usr/bin/env python3
"""CHECK-03: resource price表記揺れのfocused tests。"""

import unittest

import extract_resource_budget as target


class Check03ResourcePriceFormatsTest(unittest.TestCase):
    def test_limited_new_monthly_formats(self):
        cases = {
            r"月額：\620,000-": (620000, None, None),
            "月額：¥620,000": (620000, None, None),
            "月額：￥620,000": (620000, None, None),
            "単価：55万〜MAX65万円": (600000, 550000, 650000),
            "月額：70万円から80万円": (750000, 700000, 800000),
        }
        for body, expected in cases.items():
            with self.subTest(body=body):
                self.assertEqual(target.rule_extract(body)[:3], expected)

    def test_new_rules_do_not_expand_outside_price_context_or_metadata_scope(self):
        path = target.build_record("fixture", r"保存先 C:\620,000-folder")
        standalone_max = target.build_record("fixture", "単価：MAX65万円")
        above = target.build_record("fixture", "単価：70万円以上")
        self.assertIsNone(path["desired_unit_price"])
        self.assertEqual(standalone_max["desired_unit_price"], 650000)
        self.assertIsNone(
            standalone_max["desired_unit_price_sub_infor"]["desired_unit_price_max"]
        )
        self.assertEqual(above["desired_unit_price"], 700000)
        self.assertIsNone(
            above["desired_unit_price_sub_infor"]["desired_unit_price_min"]
        )

    def test_comma_yen_ranges_and_open_range(self):
        cases = {
            "単金:650,000~700,000円/月": (675000, 650000, 700000),
            "■単金■\n730,000 ~ 830,000": (780000, 730000, 830000),
            "■単金■\n930,000 〜 980,000": (955000, 930000, 980000),
            "単金:720,000円〜": (720000, 720000, None),
        }
        for body, expected in cases.items():
            with self.subTest(body=body):
                price, minimum, maximum, _, _ = target.rule_extract(body)
                self.assertEqual((price, minimum, maximum), expected)

    def test_comma_yen_single_is_not_a_range(self):
        price, minimum, maximum, _, _ = target.rule_extract(
            "単金:700,000円/月"
        )
        self.assertEqual(price, 700000)
        self.assertIsNone(minimum)
        self.assertIsNone(maximum)

    def test_explicit_hourly_range_and_open_use_existing_conversion(self):
        cases = {
            "単金:時給4000円〜時給4500円": (680000, 640000, 720000),
            "単金:時給:4,500円〜": (720000, 720000, None),
            "単金:時給3000円": (480000, None, None),
        }
        for body, expected in cases.items():
            with self.subTest(body=body):
                self.assertEqual(target.rule_extract(body)[:3], expected)

    def test_explicit_daily_yen_uses_existing_conversion(self):
        self.assertEqual(target.rule_extract("単金:日給11,000円")[0], 220000)

    def test_monthly_amount_precedes_hourly_equivalent(self):
        result = target.rule_extract("■単金■\n184万円(時給11000円相当)")
        self.assertEqual(result[0], 1840000)
        self.assertEqual(result[3], "monthly-with-hourly-equivalent")

    def test_existing_thousand_yen_is_preserved(self):
        result = target.rule_extract("■単金■\n660千円")
        self.assertEqual(result[:4], (660000, None, None, "thousand-yen"))

    def test_held_ambiguous_values_keep_existing_behavior(self):
        unit_unknown = target.rule_extract("単金:6800円〜")
        parenthetical = target.rule_extract("単金:82万(76万)")
        self.assertIsNone(unit_unknown[0])
        self.assertEqual(parenthetical[0], 820000)
        self.assertEqual(parenthetical[3], "man")


if __name__ == "__main__":
    unittest.main()

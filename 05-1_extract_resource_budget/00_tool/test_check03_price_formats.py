#!/usr/bin/env python3
"""CHECK-03: resource price表記揺れのfocused tests。"""

import unittest

import extract_resource_budget as target


class Check03ResourcePriceFormatsTest(unittest.TestCase):
    def test_standalone_taxed_man_range_in_long_profile(self):
        for tax_label in ("税別", "税込"):
            with self.subTest(tax_label=tax_label):
                body = (
                    "ERP導入コンサルタントの経歴概要です。\n" * 20
                    + "135万円〜150万円 {}\n".format(tax_label)
                    + "要件定義から移行まで対応可能です。\n" * 20
                )
                self.assertEqual(
                    target.rule_extract(body),
                    (
                        1425000,
                        1350000,
                        1500000,
                        "standalone-taxed-man-range",
                        "135万円〜150万円 {}".format(tax_label),
                    ),
                )

    def test_standalone_taxed_range_rejects_non_price_ranges(self):
        cases = (
            "140-180h",
            "30〜40歳",
            "1〜2名",
            "3〜5年",
            "6o万円",
            "135〜150 税別",
        )
        for value in cases:
            with self.subTest(value=value):
                body = "要員の経歴概要です。\n" * 20 + value
                self.assertIsNone(target.rule_extract(body)[0])

    def test_existing_price_precedes_standalone_taxed_range_fallback(self):
        body = "単価:80万円\n" + "経歴詳細です。\n" * 20 + "135万円〜150万円 税別"
        self.assertEqual(target.rule_extract(body)[:4], (800000, None, None, "man"))

    def test_labeled_bare_comma_monthly_with_trailing_hyphen(self):
        cases = (
            "月 額:750,000-",
            "月額：750,000-",
            "単価:750,000-",
            "単 価：７５０，０００－",
            "■単金■\n750,000-",
        )
        for body in cases:
            with self.subTest(body=body):
                result = target.rule_extract(body)
                self.assertEqual(result[:3], (750000, None, None))
                self.assertEqual(
                    result[3], "labeled-comma-yen-trailing-hyphen"
                )
                self.assertEqual(result[4], "750,000-")

    def test_new_scalar_rule_rejects_ambiguous_or_non_monthly_values(self):
        cases = (
            "750,000-",
            "時給:750,000-",
            "日給:750,000-",
            "単価:750,000-850,000",
            "単価:750,000～850,000",
            "単価:750,000- / 予算:850,000-",
            "単価:62",
            "時給:65万円",
            "年齢:45歳",
            "精算幅:140-180h",
            "稼働率:80%",
            "人数:3名",
            "勤務時間:9:00-18:00",
            "電話番号:03-7500-0000",
        )
        for body in cases:
            with self.subTest(body=body):
                self.assertIsNone(
                    target._bare_comma_monthly_labeled(target._n(body))
                )

        # 既存のclosed rangeは新ruleでscalar化せず、従来どおり保持する。
        self.assertEqual(
            target.rule_extract("単価:750,000-850,000")[:3],
            (800000, 750000, 850000),
        )

        for body in (
            "時給単価:750,000-",
            "時給 単価:750,000-",
            "日給単価:750,000-",
            "日給 単価:750,000-",
        ):
            with self.subTest(body=body):
                self.assertIsNone(target.rule_extract(body)[0])

    def test_existing_scalar_range_and_billing_units_are_preserved(self):
        cases = {
            "単価:750,000円": (750000, None, None),
            "単価:¥750,000": (750000, None, None),
            "単価:650,000-850,000円/月": (750000, 650000, 850000),
            "単価:〜75万円": (750000, None, 750000),
            "単価:75万円〜": (750000, 750000, None),
            "単価:時給4,000円": (640000, None, None),
            "単価:日給11,000円": (220000, None, None),
            "単価:750,000円（税別）": (750000, None, None),
        }
        for body, expected in cases.items():
            with self.subTest(body=body):
                self.assertEqual(target.rule_extract(body)[:3], expected)

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

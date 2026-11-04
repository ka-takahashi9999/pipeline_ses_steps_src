#!/usr/bin/env python3
"""project settlementの日本語時間range fallbackに対するfocused test。"""

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("prepare_sales_mail_context.py")
SPEC = importlib.util.spec_from_file_location("prepare_sales_mail_context", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class ProjectSettlementJapaneseHourRangeTest(unittest.TestCase):
    def test_explicit_settlement_hour_ranges_are_extracted(self):
        cases = {
            "精算：140～180時間": "140~180時間",
            "精算幅：140H～200H": "140H~200H",
            "精算 140h～180H": "140h~180H",
            "精算：140時間～180時間": "140時間~180時間",
        }
        for body, expected in cases.items():
            with self.subTest(body=body):
                self.assertEqual(
                    (expected, "01-4_cleanup_email_text"),
                    MODULE.format_settlement(None, body),
                )

    def test_unlabeled_or_non_settlement_ranges_are_not_extracted(self):
        for body in (
            "140～180",
            "年齢：30～45",
            "単価：60～80万円",
            "稼働：80～100%",
            "勤務時間：9～18時",
        ):
            with self.subTest(body=body):
                self.assertEqual(
                    (None, "unavailable"),
                    MODULE.format_settlement(None, body),
                )

    def test_bare_range_under_settlement_label_is_not_extracted(self):
        self.assertEqual(
            (None, "unavailable"),
            MODULE.format_settlement(None, "精算幅：140～180"),
        )

    def test_existing_parser_value_is_preserved(self):
        self.assertEqual(
            ("140h~180h", "existing_source"),
            MODULE.format_settlement(
                {
                    "workload_raw": "140h~180h",
                    "workload_max_source": "existing_source",
                },
                "精算幅：140H～200H",
            ),
        )
        self.assertEqual(
            ("上下割", "01-4_cleanup_email_text"),
            MODULE.format_settlement(None, "精算幅：140-180H (上下割)"),
        )


if __name__ == "__main__":
    unittest.main()

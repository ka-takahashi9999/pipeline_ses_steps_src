#!/usr/bin/env python3
"""カンマ付き円/時rangeと数値途中への部分一致を確認するFocused Test。"""

import importlib.util
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


STEP = Path(__file__).resolve().parent.parent
ROOT = STEP.parent
sys.path.insert(0, str(STEP / "00_tool"))

import extract_project_budget as target
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list


TARGET_ID = "s3_480bfaf9bc455253a777a1d5558c5c4ef969503a54a9c87e14cd2748b467b59d"


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HourlyYenRangeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.projects = read_jsonl_as_list(target.INPUT_PROJECTS)
        cls.cleaned = read_jsonl_as_dict(target.INPUT_CLEANED)
        cls.master = read_jsonl_as_dict(target.INPUT_MASTER)
        old_rows = read_jsonl_as_list(str(
            STEP / "01_result" / target.OUTPUT_EXTRACTED
        )) + read_jsonl_as_list(str(
            STEP / "01_result" / target.OUTPUT_NULL
        ))
        cls.old = {row["message_id"]: row for row in old_rows}
        cls.new = {}
        for project in cls.projects:
            mid = project["message_id"]
            cls.new[mid] = target.build_record(
                mid,
                (cls.cleaned.get(mid) or {}).get("body_text", ""),
                (cls.master.get(mid) or {}).get("subject", ""),
            )

    def test_explicit_hourly_ranges(self):
        cases = {
            "3,500円〜6,000円/時": (
                560000, 960000, 760000, "3,500円〜6,000円/時"
            ),
            "3,500円～6,000円 / 時": (
                560000, 960000, 760000, "3,500円~6,000円 / 時"
            ),
            "2,800円〜4,200円／時": (
                448000, 672000, 560000, "2,800円〜4,200円/時"
            ),
            "4,000円 - 5,500円 / 時": (
                640000, 880000, 760000, "4,000円 - 5,500円 / 時"
            ),
        }
        for text, (minimum, maximum, representative, raw) in cases.items():
            with self.subTest(text=text):
                record = target.build_record("fixture", "単価：" + text)
                sub = record["unit_price_sub_infor"]
                self.assertEqual(record["unit_price"], representative)
                self.assertEqual(sub["range"], {"min": minimum, "max": maximum})
                self.assertEqual(sub["reason"], "hourly-yen-range")
                self.assertEqual(sub["kind"], "monthly")
                self.assertEqual(sub["unit_price_raw"], raw)

    def test_existing_single_hourly_forms(self):
        cases = {
            "単価：3500円/時": (560000, "3500円/時"),
            "単価：3,500円/時": (560000, "3,500円/時"),
            "単価：時給 4,000円": (640000, "時給 4,000円"),
            "単価：2000円/h": (320000, "2000円/h"),
        }
        for text, (price, raw) in cases.items():
            with self.subTest(text=text):
                record = target.build_record("fixture", text)
                self.assertEqual(record["unit_price"], price)
                self.assertEqual(
                    record["unit_price_sub_infor"]["unit_price_raw"], raw
                )

    def test_single_hourly_never_starts_inside_comma_number(self):
        cases = {
            "単価：6,000円/時": (960000, "6,000円/時"),
            "単価：12,000円/時": (1920000, "12,000円/時"),
            "単価：123,500円/時": (19760000, "123,500円/時"),
        }
        for text, (price, raw) in cases.items():
            with self.subTest(text=text):
                record = target.build_record("fixture", text)
                self.assertEqual(record["unit_price"], price)
                self.assertEqual(
                    record["unit_price_sub_infor"]["unit_price_raw"], raw
                )
                self.assertNotIn(
                    record["unit_price_sub_infor"]["unit_price_raw"],
                    {"000円/時", "500円/時"},
                )

    def test_non_price_ranges_are_not_claimed_by_hourly_range(self):
        cases = (
            "単価：精算幅 140〜180時間",
            "稼働 0.5〜1.0人月",
            "年齢 30〜45歳",
            "期間 4〜6月",
            "交通費 3,000〜5,000円",
            "月額 60〜80万円",
            "単価：3,500〜6,000",
        )
        for text in cases:
            with self.subTest(text=text):
                result = target.rule_extract(text)
                self.assertNotEqual(result[2], "hourly-yen-range")

    def test_latest_projects_change_only_confirmed_target(self):
        self.assertEqual(len(self.projects), 1250)
        self.assertEqual(set(self.old), set(self.new))
        changes = {
            mid for mid in self.new if self.old[mid] != self.new[mid]
        }
        self.assertEqual(changes, {TARGET_ID})

        old = self.old[TARGET_ID]
        self.assertEqual(old["unit_price"], 0)
        self.assertEqual(
            old["unit_price_sub_infor"]["unit_price_raw"], "000円 / 時"
        )
        actual = self.new[TARGET_ID]
        self.assertEqual(actual["unit_price"], 760000)
        self.assertEqual(
            actual["unit_price_sub_infor"],
            {
                "range": {"min": 560000, "max": 960000},
                "currency": "JPY",
                "method": "rule",
                "reason": "hourly-yen-range",
                "tax_included": "unknown",
                "confidence": 0.85,
                "kind": "monthly",
                "unit_price_raw": "3,500円〜6,000円 / 時",
            },
        )

    def test_in_memory_confirm(self):
        extracted = [
            self.new[row["message_id"]] for row in self.projects
            if self.new[row["message_id"]]["unit_price"] is not None
        ]
        null_rows = [
            self.new[row["message_id"]] for row in self.projects
            if self.new[row["message_id"]]["unit_price"] is None
        ]
        self.assertEqual((len(extracted), len(null_rows)), (1232, 18))

        confirm = _load_module(
            "focused_confirm_extract_project_budget_hourly_range",
            STEP / "02_confirm" / "confirm_extract_project_budget.py",
        )
        rows = {
            confirm.INPUT_PROJECTS: self.projects,
            str(STEP / "01_result" / target.OUTPUT_EXTRACTED): extracted,
            str(STEP / "01_result" / target.OUTPUT_NULL): null_rows,
        }
        with patch.object(
            confirm,
            "read_jsonl_as_list",
            side_effect=lambda source: rows[str(source)],
        ), patch.object(confirm, "logger") as logger:
            confirm.main()
            self.assertTrue(any(
                "confirm OK" in str(call)
                for call in logger.ok.call_args_list
            ))

    def test_existing_budget_logic_for_target(self):
        budget = _load_module(
            "focused_match_budget_hourly_range",
            ROOT / "06-1_match_budget" / "00_tool" / "match_budget.py",
        )
        resources = read_jsonl_as_dict(str(
            ROOT / "05-1_extract_resource_budget" / "01_result"
            / "extract_resource_budget.jsonl"
        ))
        null_resources = read_jsonl_as_dict(str(
            ROOT / "05-1_extract_resource_budget" / "01_result"
            / "99_price_null_extract_resource_budget.jsonl"
        ))
        resources.update(null_resources)

        passed = 0
        excluded = 0
        pair_count = 0
        pair_path = (
            ROOT / "06-0_match_all_message_id" / "01_result"
            / "matched_pairs_all.jsonl"
        )
        with pair_path.open(encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                pair = json.loads(line)
                if pair["project_info"]["message_id"] != TARGET_ID:
                    continue
                resource_id = pair["resource_info"]["message_id"]
                desired_price = (
                    resources.get(resource_id) or {}
                ).get("desired_unit_price")
                is_match = budget.judge_budget_match(760000, desired_price)
                pair_count += 1
                if is_match:
                    passed += 1
                else:
                    excluded += 1

        self.assertEqual(pair_count, 2165)
        self.assertEqual((passed, excluded), (615, 1550))


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""05-1 千円表記のfocused tests。"""

import json
import sys
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
STEP_DIR = TOOL_DIR.parent
PROJECT_ROOT = STEP_DIR.parent
sys.path.insert(0, str(TOOL_DIR))

import extract_resource_budget as target


TARGET_PRICES = {
    "s3_b5828e74712a62ff7239412e421b67a3cfb9aef01fd155a640550a0a6bfb2c05": 660_000,
    "s3_007da5203dc93f858d4670bfb2e2851e5b71eb856631ffdf3982e868ed53c965": 550_000,
}


def _read_jsonl(path):
    with Path(path).open(encoding="utf-8") as file_obj:
        return [json.loads(line) for line in file_obj if line.strip()]


def _read_subjects():
    subjects = {}
    with Path(target.INPUT_MASTER).open(encoding="utf-8") as file_obj:
        for line in file_obj:
            if not line.strip():
                continue
            record = json.loads(line)
            subjects[record["message_id"]] = record.get("subject") or ""
    return subjects


class ThousandYenBudgetTest(unittest.TestCase):
    def test_positive_thousand_yen_fields(self):
        cases = {
            "■単金■\n660千円": (660_000, "660千円"),
            "■単金■\n550千円/月": (550_000, "550千円/月"),
            "希望単価: 550 千円": (550_000, "550 千円"),
            "【単価】550千円／月": (550_000, "550千円/月"),
        }
        for body, (expected_price, expected_raw) in cases.items():
            with self.subTest(body=body):
                price, p_min, p_max, reason, raw = target.rule_extract(body)
                self.assertEqual(price, expected_price)
                self.assertIsNone(p_min)
                self.assertIsNone(p_max)
                self.assertEqual(reason, "thousand-yen")
                self.assertEqual(raw, expected_raw)

    def test_unlabeled_and_non_price_amounts_are_not_extracted(self):
        for body in (
            "交通費 10千円",
            "手当 20千円",
            "インセンティブ 30千円",
            "経費 40千円",
            "本文中の一般金額は50千円です",
            "参考値 550千円",
        ):
            with self.subTest(body=body):
                self.assertIsNone(target.rule_extract(body)[0])

    def test_multiple_thousand_yen_price_fields_are_not_selected(self):
        for body in (
            "単価: 550千円 600千円",
            "単価: 550千円\n希望単価: 600千円",
        ):
            with self.subTest(body=body):
                self.assertIsNone(target.rule_extract(body)[0])

    def test_price_bounds_are_preserved(self):
        self.assertIsNone(target.rule_extract("単価: 50千円")[0])
        self.assertIsNone(target.rule_extract("単価: 6000千円")[0])

    def test_latest_resources_change_only_the_two_targets(self):
        resource_ids = [
            record["message_id"] for record in _read_jsonl(target.INPUT_RESOURCES)
        ]
        cleaned = {
            record["message_id"]: record
            for record in _read_jsonl(target.INPUT_CLEANED)
        }
        subjects = _read_subjects()
        previous = {}
        for output_name in (target.OUTPUT_EXTRACTED, target.OUTPUT_NULL):
            for record in _read_jsonl(STEP_DIR / "01_result" / output_name):
                previous[record["message_id"]] = record

        changes = {}
        for message_id in resource_ids:
            current = target.build_record(
                message_id,
                (cleaned.get(message_id) or {}).get("body_text", ""),
                subjects.get(message_id, ""),
            )
            if current != previous[message_id]:
                changes[message_id] = (
                    previous[message_id]["desired_unit_price"],
                    current["desired_unit_price"],
                    current["desired_unit_price_sub_infor"][
                        "desired_unit_price_raw"
                    ],
                )

        self.assertEqual(len(resource_ids), 2225)
        self.assertEqual(
            changes,
            {
                message_id: (
                    None,
                    price,
                    "660千円" if price == 660_000 else "550千円/月",
                )
                for message_id, price in TARGET_PRICES.items()
            },
        )


if __name__ == "__main__":
    unittest.main()

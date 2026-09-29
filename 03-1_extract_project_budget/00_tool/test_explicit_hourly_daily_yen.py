#!/usr/bin/env python3
"""2026-09-29監査の価格欄「完全時給」2案件のFocused Test。"""

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


EXPECTED = {
    "1a0e5dcf18a36204": (699200, "4,370円程度(完全時給)"),
    "1a0e6a2d3c01765e": (640000, "~4,000円程度(完全時給)"),
}
REPRESENTATIVE_PROJECT_ID = "1a0e5dcf18a36204"
REPRESENTATIVE_RESOURCE_ID = "1a0e5ec10ddeaeda"


def _load_budget_module():
    path = ROOT / "06-1_match_budget" / "00_tool" / "match_budget.py"
    spec = importlib.util.spec_from_file_location("focused_match_budget", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ExplicitHourlyDailyYenTest(unittest.TestCase):
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

    def test_two_real_projects_use_existing_hourly_conversion(self):
        for mid, (price, raw) in EXPECTED.items():
            with self.subTest(message_id=mid):
                self.assertIsNone(self.old[mid]["unit_price"])
                self.assertEqual(
                    self.old[mid]["unit_price_sub_infor"]["reason"],
                    "no-match",
                )
                actual = self.new[mid]
                self.assertEqual(actual["unit_price"], price)
                self.assertEqual(
                    actual["unit_price_sub_infor"],
                    {
                        "range": None,
                        "currency": "JPY",
                        "method": "rule",
                        "reason": "hourly-yen",
                        "tax_included": "unknown",
                        "confidence": 0.82,
                        "kind": "monthly",
                        "unit_price_raw": raw,
                    },
                )

        self.assertEqual(
            self.new["1a0e5dcf18a36204"]["unit_price"],
            4370 * target.HOURLY_TO_MONTHLY,
        )
        self.assertEqual(
            self.new["1a0e6a2d3c01765e"]["unit_price"],
            4000 * target.HOURLY_TO_MONTHLY,
        )

    def test_complete_hourly_and_existing_explicit_billing_variants(self):
        cases = {
            "complete_hourly_fullwidth": (
                "■単価：4,370円程度（完全時給）※上振れ検討可能",
                699200,
                "hourly-yen",
            ),
            "complete_hourly_ascii": (
                "■単価■\n~4,000円程度(完全時給)",
                640000,
                "hourly-yen",
            ),
            "hourly_settlement": (
                "単価：〜5,300円程度(時給精算)",
                848000,
                "hourly-yen",
            ),
            "explicit_hourly": (
                "単価：時給1,745円",
                279200,
                "hourly-yen",
            ),
            "explicit_daily": (
                "単価：¥13,000/日",
                260000,
                "daily-yen-slash",
            ),
        }

        for name, (text, price, reason) in cases.items():
            with self.subTest(case=name):
                actual = target.build_record("fixture", text)
                self.assertEqual(actual["unit_price"], price)
                self.assertEqual(
                    actual["unit_price_sub_infor"]["reason"], reason
                )
                self.assertEqual(
                    actual["unit_price_sub_infor"]["kind"], "monthly"
                )

    def test_price_field_and_explicit_billing_unit_are_both_required(self):
        negatives = {
            "settlement_range": "単価：精算幅 140-180h",
            "work_time": "単価：勤務時間 9:00-18:00",
            "transportation": "交通費：4,370円",
            "incentive": "インセンティブ：4,370円",
            "allowance": "手当：時給1,745円",
            "expense": "経費：¥13,000/日",
            "general_amount": "研修参加費は4,370円です",
            "complete_hourly_description": (
                "契約は完全時給で、4,370円程度です"
            ),
            "yen_without_billing_unit": "単価：4,370円",
            "symbol_without_billing_unit": "【単価】¥13,000",
            "complete_hourly_without_price_field": (
                "4,370円程度(完全時給)"
            ),
        }
        for name, text in negatives.items():
            with self.subTest(case=name):
                self.assertIsNone(target.rule_extract(text)[0])

    def test_latest_779_projects_change_only_the_two_targets(self):
        self.assertEqual(len(self.projects), 779)
        self.assertEqual(set(self.old), set(self.new))
        full_record_changes = {
            mid for mid in self.new if self.old[mid] != self.new[mid]
        }
        unit_price_changes = {
            mid for mid in self.new
            if self.old[mid]["unit_price"] != self.new[mid]["unit_price"]
        }
        self.assertEqual(full_record_changes, set(EXPECTED))
        self.assertEqual(unit_price_changes, set(EXPECTED))
        self.assertEqual(
            sum(row["unit_price"] is None for row in self.old.values()), 11
        )
        self.assertEqual(
            sum(row["unit_price"] is None for row in self.new.values()), 9
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
        self.assertEqual((len(extracted), len(null_rows)), (770, 9))

        path = STEP / "02_confirm" / "confirm_extract_project_budget.py"
        spec = importlib.util.spec_from_file_location(
            "focused_confirm_extract_project_budget", path
        )
        confirm = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(confirm)
        output_rows = {
            confirm.INPUT_PROJECTS: self.projects,
            str(STEP / "01_result" / target.OUTPUT_EXTRACTED): extracted,
            str(STEP / "01_result" / target.OUTPUT_NULL): null_rows,
        }
        with patch.object(
            confirm,
            "read_jsonl_as_list",
            side_effect=lambda source: output_rows[str(source)],
        ), patch.object(confirm, "logger") as confirm_logger:
            confirm.main()
            self.assertTrue(any(
                "confirm OK" in str(call)
                for call in confirm_logger.ok.call_args_list
            ))

    def test_existing_budget_logic_on_3786_pairs(self):
        budget = _load_budget_module()
        resources = read_jsonl_as_dict(str(
            ROOT / "05-1_extract_resource_budget" / "01_result"
            / "extract_resource_budget.jsonl"
        ))
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
                project_id = pair["project_info"]["message_id"]
                if project_id not in EXPECTED:
                    continue
                resource_id = pair["resource_info"]["message_id"]
                project_price = self.new[project_id]["unit_price"]
                desired_price = (
                    resources.get(resource_id) or {}
                ).get("desired_unit_price")
                is_match = budget.judge_budget_match(
                    project_price, desired_price
                )
                pair_count += 1
                if is_match:
                    passed += 1
                else:
                    excluded += 1

        self.assertEqual(pair_count, 3786)
        self.assertEqual((passed, excluded), (500, 3286))

    def test_current_nine_sales_candidates_are_all_budget_excluded(self):
        budget = _load_budget_module()
        resources = read_jsonl_as_dict(str(
            ROOT / "05-1_extract_resource_budget" / "01_result"
            / "extract_resource_budget.jsonl"
        ))
        candidate_path = (
            ROOT / "08-5_high_score_required_skill_recheck" / "01_result"
            / "high_score_required_skill_recheck_all.jsonl"
        )
        results = []
        representative = None
        with candidate_path.open(encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                row = json.loads(line)
                project_id = (row.get("project_info") or {}).get("message_id")
                if project_id != REPRESENTATIVE_PROJECT_ID:
                    continue
                resource_id = (row.get("resource_info") or {}).get("message_id")
                desired_price = (
                    resources.get(resource_id) or {}
                ).get("desired_unit_price")
                is_match = budget.judge_budget_match(699200, desired_price)
                results.append(is_match)
                if resource_id == REPRESENTATIVE_RESOURCE_ID:
                    representative = (
                        699200,
                        desired_price,
                        budget.MIN_MARGIN,
                        is_match,
                    )

        self.assertEqual(len(results), 9)
        self.assertEqual(results, [False] * 9)
        self.assertEqual(representative, (699200, 1150000, 120000, False))


if __name__ == "__main__":
    unittest.main()

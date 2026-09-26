#!/usr/bin/env python3
"""2026-09-25監査の明示的な円建て時給・日給4案件のFocused Test。"""

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
    "1a0d6bf937d649a9": (848000, "hourly-yen", "〜5,300円程度(時給精算)"),
    "1a0d6bdf7e2d5317": (848000, "hourly-yen", "〜5,300円(時給精算)"),
    "1a0d61ed14f62d42": (260000, "daily-yen-slash", "¥13,000/日"),
    "1a0d5e717c29bd72": (279200, "hourly-yen", "時給1,745 円"),
}
REPRESENTATIVE_PROJECT_ID = "1a0d6bf937d649a9"
REPRESENTATIVE_RESOURCE_ID = "1a0d61ead275b248"


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

    def test_four_real_projects_use_existing_monthly_conversions(self):
        for mid, (price, reason, raw) in EXPECTED.items():
            with self.subTest(message_id=mid):
                self.assertIsNone(self.old[mid]["unit_price"])
                actual = self.new[mid]
                self.assertEqual(actual["unit_price"], price)
                self.assertEqual(
                    actual["unit_price_sub_infor"],
                    {
                        "range": None,
                        "currency": "JPY",
                        "method": "rule",
                        "reason": reason,
                        "tax_included": "unknown",
                        "confidence": 0.82 if reason == "hourly-yen" else 0.8,
                        "kind": "monthly",
                        "unit_price_raw": raw,
                    },
                )
        self.assertEqual(
            self.new["1a0d6bf937d649a9"]["unit_price"],
            5300 * target.HOURLY_TO_MONTHLY,
        )
        self.assertEqual(
            self.new["1a0d6bdf7e2d5317"]["unit_price"],
            5300 * target.HOURLY_TO_MONTHLY,
        )
        self.assertEqual(
            self.new["1a0d5e717c29bd72"]["unit_price"],
            1745 * target.HOURLY_TO_MONTHLY,
        )
        self.assertEqual(
            self.new["1a0d61ed14f62d42"]["unit_price"],
            13000 * target.DAILY_TO_MONTHLY,
        )

    def test_price_field_and_explicit_billing_unit_are_both_required(self):
        negatives = {
            "settlement_range": "単価：精算幅 140-180h",
            "work_time": "単価：勤務時間 9:00-18:00",
            "overtime": "単価：残業時間 20時間",
            "daily_incentive": "日額インセンティブ：¥13,000/日",
            "transportation": "交通費：¥13,000/日",
            "allowance": "手当：時給1,745円",
            "expense": "経費：¥13,000/日",
            "hours_only": "単価：160時間",
            "general_amount": "研修参加費は¥13,000/日です",
            "yen_without_billing_unit": "単価：5,300円",
            "symbol_without_billing_unit": "【単価】¥13,000",
            "hourly_without_price_field": "時給1,745円",
            "daily_without_price_field": "¥13,000/日",
        }
        for name, text in negatives.items():
            with self.subTest(case=name):
                self.assertIsNone(target.rule_extract(text)[0])

    def test_latest_761_projects_change_only_the_four_targets(self):
        self.assertEqual(len(self.projects), 761)
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
            sum(row["unit_price"] is None for row in self.old.values()), 12
        )
        self.assertEqual(
            sum(row["unit_price"] is None for row in self.new.values()), 8
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
        self.assertEqual((len(extracted), len(null_rows)), (753, 8))

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

    def test_existing_budget_logic_on_7456_pairs(self):
        budget = _load_budget_module()
        resources = read_jsonl_as_dict(str(
            ROOT / "05-1_extract_resource_budget" / "01_result"
            / "extract_resource_budget.jsonl"
        ))
        passed = 0
        excluded = 0
        pair_count = 0
        per_project = {mid: {"passed": 0, "excluded": 0} for mid in EXPECTED}
        representative = None
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
                key = "passed" if is_match else "excluded"
                per_project[project_id][key] += 1
                if is_match:
                    passed += 1
                else:
                    excluded += 1
                if (
                    project_id == REPRESENTATIVE_PROJECT_ID
                    and resource_id == REPRESENTATIVE_RESOURCE_ID
                ):
                    representative = (
                        project_price,
                        desired_price,
                        budget.MIN_MARGIN,
                        is_match,
                    )

        self.assertEqual(pair_count, 7456)
        self.assertEqual((passed, excluded), (1778, 5678))
        self.assertEqual(
            per_project,
            {
                "1a0d6bf937d649a9": {"passed": 876, "excluded": 988},
                "1a0d6bdf7e2d5317": {"passed": 876, "excluded": 988},
                "1a0d61ed14f62d42": {"passed": 13, "excluded": 1851},
                "1a0d5e717c29bd72": {"passed": 13, "excluded": 1851},
            },
        )
        self.assertEqual(representative, (848000, 850000, 120000, False))

        recheck_path = (
            ROOT / "08-5_high_score_required_skill_recheck" / "01_result"
            / "high_score_required_skill_recheck_all.jsonl"
        )
        representative_08_5_count = 0
        with recheck_path.open(encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                row = json.loads(line)
                if (
                    (row.get("project_info") or {}).get("message_id")
                    == REPRESENTATIVE_PROJECT_ID
                    and (row.get("resource_info") or {}).get("message_id")
                    == REPRESENTATIVE_RESOURCE_ID
                ):
                    representative_08_5_count += 1
        self.assertEqual(representative_08_5_count, 1)


if __name__ == "__main__":
    unittest.main()

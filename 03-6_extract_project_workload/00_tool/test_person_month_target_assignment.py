#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""複数人月の明示募集対象への帰属選択に限定したfocused test。"""

import json
import sys
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
STEP_DIR = TOOL_DIR.parent
sys.path.insert(0, str(TOOL_DIR))

import extract_project_workload as target


TARGET_IDS = {
    "s3_dceec66959ed4ce80cbf20d4a76ed042fb87ede6f9555ab61966970a87d0a77d",
    "s3_c20e11622c0f6b616f39ced9a3e8539e6d54f1b119dd8498ebd40579cb372304",
    "s3_0bcba405e17eb51b8da9ac9d19e13a333badd8fa17a98e2528f6117f1fb34235",
}


def _read_jsonl(path):
    with Path(path).open(encoding="utf-8") as file_obj:
        return [json.loads(line) for line in file_obj if line.strip()]


class PersonMonthTargetAssignmentTest(unittest.TestCase):
    def test_external_prime_contractor_manager_is_not_target_workload(self):
        body = (
            "今回の募集では新規開発の担当として参画します。\n"
            "元請企業のプロパが0.1人月で管理として入ります。"
        )
        self.assertEqual(target.rule_extract_workload(body), (100, 100, "default", None))

    def test_explicitly_recruited_member_wins_over_leader(self):
        body = (
            "・インフラリーダー:1名(0.6人月)\n"
            "・インフラメンバー:1名(1.0人月)\n"
            "※今回はメンバー枠の募集です"
        )
        self.assertEqual(
            target.rule_extract_workload(body),
            (100, 100, "extracted", "1.0人月"),
        )

    def test_recruited_leader_is_not_excluded_by_role_name(self):
        body = (
            "・リーダー:0.6人月\n"
            "・メンバー:1.0人月\n"
            "今回募集: リーダーポジション"
        )
        self.assertEqual(
            target.rule_extract_workload(body),
            (60, 60, "extracted", "0.6人月"),
        )

    def test_single_and_existing_workload_patterns_are_unchanged(self):
        cases = {
            "稼働 0.8人月": (80, 80, "extracted", "0.8人月"),
            "1人月": (100, 100, "extracted", "1人月"),
            "稼働: 週3日〜週5日稼働": (60, 100, "extracted", "週3日〜週5日稼働"),
        }
        for body, expected in cases.items():
            with self.subTest(body=body):
                self.assertEqual(target.rule_extract_workload(body), expected)

    def test_ambiguous_multiple_person_months_keep_existing_order(self):
        body = "リーダー 0.6人月\nメンバー 1.0人月"
        self.assertEqual(
            target.rule_extract_workload(body),
            (60, 60, "extracted", "0.6人月"),
        )

    def test_latest_projects_change_only_the_three_targets(self):
        projects = _read_jsonl(target.INPUT_PROJECTS)
        cleaned = {
            record["message_id"]: record
            for record in _read_jsonl(target.INPUT_CLEANED)
        }
        previous = {
            record["message_id"]: record
            for record in _read_jsonl(STEP_DIR / "01_result" / target.OUTPUT_EXTRACTED)
        }
        changes = {}
        for project in projects:
            message_id = project["message_id"]
            current = target.build_record(
                message_id,
                (cleaned.get(message_id) or {}).get("body_text", ""),
            )
            if current != previous[message_id]:
                changes[message_id] = current

        self.assertEqual(len(projects), 1174)
        self.assertEqual(set(changes), TARGET_IDS)
        self.assertEqual(
            changes[
                "s3_dceec66959ed4ce80cbf20d4a76ed042fb87ede6f9555ab61966970a87d0a77d"
            ],
            {
                "message_id": "s3_dceec66959ed4ce80cbf20d4a76ed042fb87ede6f9555ab61966970a87d0a77d",
                "workload_min": 100,
                "workload_max": 100,
                "workload_max_source": "default",
                "workload_raw": None,
            },
        )
        for message_id in TARGET_IDS - {
            "s3_dceec66959ed4ce80cbf20d4a76ed042fb87ede6f9555ab61966970a87d0a77d"
        }:
            self.assertEqual(changes[message_id]["workload_min"], 100)
            self.assertEqual(changes[message_id]["workload_max"], 100)
            self.assertEqual(changes[message_id]["workload_raw"], "1.0人月")


if __name__ == "__main__":
    unittest.main()

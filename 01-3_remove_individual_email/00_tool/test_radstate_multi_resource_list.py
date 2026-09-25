#!/usr/bin/env python3
"""ラッドステイト社の複数要員一覧テンプレートFocused Test。"""

import copy
import importlib.util
import inspect
import json
import re
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


STEP = Path(__file__).resolve().parent.parent
ROOT = STEP.parent
sys.path.insert(0, str(STEP / "00_tool"))
import remove_individual_email as target
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list


TARGET_IDS = {
    "1a0d272e7765710e",
    "1a0d272c067692c8",
    "1a0d272b9faf053e",
}
OTHER_EXPECTED_NEW_IDS = {
    "1a0d100d5eaf636f",
    "1a0d0d5eab75f453",
    "1a0d15e5f06de61a",
    "1a0d14c378c2ff81",
    "1a0d12c92e945be9",
    "1a0d1454442c23cf",
    "1a0d143ae017c9d5",
    "1a0d126231435729",
    "1a0d0f67fdd66805",
    "1a0d0d40178796c0",
    "1a0d0f14e9ad61c8",
    "1a0d2a71e8f16c5f",
}
OTHER_EXPECTED_PROJECT_IDS = {"1a0d1dc366b84837"}
DECISION = ("multi_item_mail", "radstate_multi_resource_list_v1")


class RadstateMultiResourceListTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.master = read_jsonl_as_dict(target.INPUT_MASTER)
        cls.prev = read_jsonl_as_list(target.INPUT_PREV)
        cls.rules = target.load_exclude_list(target.EXCLUDE_LIST_PATH)

    def test_real_three_are_excluded_and_preserved(self):
        for mid in TARGET_IDS:
            with self.subTest(message_id=mid):
                original = self.master[mid]
                self.assertEqual(target.detect_template_exclusion(original), DECISION)
                self.assertEqual(
                    target.determine_exclusion_reason(original, *self.rules),
                    "multi_item_mail",
                )
                detail = target.build_detail_record(original, *DECISION)
                self.assertEqual(
                    {key: value for key, value in detail.items()
                     if key not in ("reason", "rule_id")},
                    original,
                )

    def test_message_id_and_skill_sheet_urls_are_variable(self):
        for mid in TARGET_IDS:
            changed = copy.deepcopy(self.master[mid])
            changed["message_id"] = "future-{}".format(mid[-4:])
            sequence = iter(range(1, 100))
            changed["body_text"] = re.sub(
                r"https://bit\.ly/[A-Za-z0-9]+",
                lambda _: "https://skills.example.invalid/document/{}".format(next(sequence)),
                changed["body_text"],
            )
            with self.subTest(message_id=mid):
                self.assertEqual(target.detect_template_exclusion(changed), DECISION)

        implementation = inspect.getsource(target.detect_template_exclusion)
        self.assertFalse(any(mid in implementation for mid in TARGET_IDS))

    def test_same_sender_single_resource_is_kept(self):
        prev_ids = {row["message_id"] for row in self.prev}
        same_sender_ids = {
            mid for mid, record in self.master.items()
            if mid in prev_ids
            and target.extract_email(record.get("from") or "") == "atmaeda@radstate.co.jp"
        }
        self.assertEqual(same_sender_ids, TARGET_IDS)

        fixture = copy.deepcopy(self.master[sorted(TARGET_IDS)[0]])
        fixture["message_id"] = "single-resource-fixture"
        fixture["body_text"] = """①
●スキルシート：
https://skills.example.invalid/document/one
氏名：AA（30歳/男性）
最寄駅：新宿駅
単価：60万円/月
"""
        self.assertIsNone(target.detect_template_exclusion(fixture))
        self.assertIsNone(target.determine_exclusion_reason(fixture, set(), []))

    def test_other_sender_with_same_structure_is_kept(self):
        fixture = copy.deepcopy(self.master[sorted(TARGET_IDS)[0]])
        fixture["message_id"] = "other-sender-fixture"
        fixture["from"] = "Other Company <resource@example.com>"
        self.assertIsNone(target.detect_template_exclusion(fixture))

    def test_latest_input_memory_replay_and_confirm(self):
        result_dir = STEP / "01_result"
        old_filtered = read_jsonl_as_list(str(result_dir / target.OUTPUT_FILTERED))
        old_removed = read_jsonl_as_list(str(result_dir / target.OUTPUT_REMOVED))
        old_details = read_jsonl_as_list(str(result_dir / target.OUTPUT_DETAIL))
        self.assertEqual((len(self.prev), len(old_filtered), len(old_removed)), (2750, 2649, 101))
        self.assertEqual(len(old_details), 4)

        captured = {}
        with patch.object(
            target,
            "ensure_result_dirs",
            return_value={"result": result_dir, "execution_time": Path("/tmp")},
        ), patch.object(
            target,
            "write_jsonl",
            side_effect=lambda path, rows: captured.update(
                {Path(path).name: copy.deepcopy(rows)}
            ),
        ), patch.object(target, "write_execution_time"), patch.object(target, "logger"):
            target.main()

        filtered = captured[target.OUTPUT_FILTERED]
        removed = captured[target.OUTPUT_REMOVED]
        details = captured[target.OUTPUT_DETAIL]
        old_removed_ids = {row["message_id"] for row in old_removed}
        new_removed_ids = {row["message_id"] for row in removed}

        expected_new_ids = (
            TARGET_IDS | OTHER_EXPECTED_NEW_IDS | OTHER_EXPECTED_PROJECT_IDS
        )
        self.assertEqual(filtered, [
            row for row in old_filtered if row["message_id"] not in expected_new_ids
        ])
        self.assertEqual(new_removed_ids - old_removed_ids, expected_new_ids)
        self.assertEqual(new_removed_ids - expected_new_ids, old_removed_ids)
        self.assertEqual((len(filtered), len(removed), len(details)), (2633, 117, 20))
        self.assertTrue(all(set(row) == {"message_id"} for row in filtered + removed))

        target_details = {
            row["message_id"]: row for row in details if row["message_id"] in TARGET_IDS
        }
        self.assertEqual(set(target_details), TARGET_IDS)
        for mid, detail in target_details.items():
            self.assertEqual(detail, target.build_detail_record(self.master[mid], *DECISION))

        spec = importlib.util.spec_from_file_location(
            "confirm_radstate",
            STEP / "02_confirm" / "confirm_remove_individual_email.py",
        )
        confirm = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(confirm)
        self.assertTrue(confirm.check_counts(len(self.prev), len(filtered), len(removed)))
        self.assertTrue(confirm.check_detail_records(
            self.prev, filtered, removed, details, self.master
        ))
        output_rows = {
            str(result_dir / target.OUTPUT_FILTERED): filtered,
            str(result_dir / target.OUTPUT_REMOVED): removed,
            str(result_dir / target.OUTPUT_DETAIL): details,
            confirm.INPUT_PREV: self.prev,
        }
        with patch.object(
            confirm,
            "read_jsonl_as_dict",
            return_value=self.master,
        ), patch.object(
            confirm,
            "read_jsonl_as_list",
            side_effect=lambda path: output_rows[str(path)],
        ), patch.object(confirm, "logger") as confirm_logger:
            confirm.main()
            self.assertTrue(any(
                "confirm OK" in str(call)
                for call in confirm_logger.ok.call_args_list
            ))

        resources = read_jsonl_as_list(str(
            ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "resources.jsonl"
        ))
        resource_ids = {row["message_id"] for row in resources}
        self.assertEqual(len(resources), 1835)
        self.assertTrue(TARGET_IDS <= resource_ids)
        self.assertEqual(len(resource_ids - TARGET_IDS), 1832)

    def test_downstream_existing_artifact_id_reconciliation(self):
        paths = {
            "all_pairs": (
                ROOT / "06-0_match_all_message_id" / "01_result" / "matched_pairs_all.jsonl",
                2433,
                lambda row: (row.get("resource_info") or {}).get("message_id"),
            ),
            "06-12": (
                ROOT / "06-12_filter_required_skills_noise" / "01_result"
                / "matched_pairs_required_skills_noise_filtered.jsonl",
                62,
                lambda row: (row.get("resource_info") or {}).get("message_id"),
            ),
            "08-5": (
                ROOT / "08-5_high_score_required_skill_recheck" / "01_result"
                / "high_score_required_skill_recheck_all.jsonl",
                3,
                lambda row: (row.get("resource_info") or {}).get("message_id"),
            ),
            "sales_candidates": (
                ROOT / "09-4_remove_category_mismatch_sales_candidates" / "01_result"
                / "sales_proposal_candidates_20260924.jsonl",
                3,
                lambda row: row.get("resource_message_id"),
            ),
        }
        for name, (path, expected, get_resource_id) in paths.items():
            with self.subTest(artifact=name):
                with path.open(encoding="utf-8") as stream:
                    actual = sum(
                        get_resource_id(json.loads(line)) in TARGET_IDS
                        for line in stream if line.strip()
                    )
                self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()

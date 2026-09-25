#!/usr/bin/env python3
"""Route Zero情報共有依頼テンプレートのFocused Test。"""

import copy
import importlib.util
import inspect
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


STEP = Path(__file__).resolve().parent.parent
ROOT = STEP.parent
sys.path.insert(0, str(STEP / "00_tool"))
import remove_individual_email as target
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list


TARGET_ID = "1a0d2a71e8f16c5f"
SAKYA_PROJECT_ID = "1a0d1dc366b84837"
PREVIOUS_RESOURCE_IDS = {
    "1a0d272e7765710e",
    "1a0d272c067692c8",
    "1a0d272b9faf053e",
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
}
DECISION = (
    "service_notification",
    "route_zero_information_share_notice_v1",
)


class RouteZeroInformationShareNoticeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.master = read_jsonl_as_dict(target.INPUT_MASTER)
        cls.prev = read_jsonl_as_list(target.INPUT_PREV)
        cls.rules = target.load_exclude_list(target.EXCLUDE_LIST_PATH)
        cls.original = cls.master[TARGET_ID]

    def test_real_mail_is_excluded_and_preserved(self):
        self.assertEqual(target.detect_template_exclusion(self.original), DECISION)
        self.assertEqual(
            target.determine_exclusion_reason(self.original, *self.rules),
            "service_notification",
        )
        detail = target.build_detail_record(self.original, *DECISION)
        self.assertEqual(
            {key: value for key, value in detail.items()
             if key not in ("reason", "rule_id")},
            self.original,
        )

    def test_message_id_is_variable(self):
        changed = copy.deepcopy(self.original)
        changed["message_id"] = "future-route-zero-information-share"
        self.assertEqual(target.detect_template_exclusion(changed), DECISION)
        self.assertNotIn(TARGET_ID, inspect.getsource(target.detect_template_exclusion))

    def test_same_sender_normal_mails_are_kept(self):
        same_sender_ids = {
            mid for mid, record in self.master.items()
            if target.extract_email(record.get("from") or "")
            == "takahashi@route-zero.com"
        }
        normal_ids = same_sender_ids - {TARGET_ID}
        self.assertEqual((len(same_sender_ids), len(normal_ids)), (21, 20))
        for mid in normal_ids:
            with self.subTest(message_id=mid):
                self.assertIsNone(target.detect_template_exclusion(self.master[mid]))

    def test_all_route_zero_normal_mails_are_kept(self):
        route_zero_ids = {
            mid for mid, record in self.master.items()
            if target.extract_email(record.get("from") or "").endswith("@route-zero.com")
        }
        self.assertEqual(len(route_zero_ids), 215)
        detected = {
            mid: target.detect_template_exclusion(self.master[mid])
            for mid in route_zero_ids
            if target.detect_template_exclusion(self.master[mid]) is not None
        }
        self.assertEqual(detected, {TARGET_ID: DECISION})

    def test_missing_conditions_are_kept(self):
        replacements = (
            (
                "share_request",
                "この度は注力情報の共有をお願いしたく、ご連絡させていただきました。",
                "この度はご連絡させていただきました。",
            ),
            (
                "reply_cta",
                "もしよろしければ本メールに返信で弊社の一社先でも可能な案件や貴社要員様などご紹介いただけますと幸いです。",
                "もしよろしければご紹介いただけますと幸いです。",
            ),
            (
                "information_offer",
                "また、弊社の注力情報（案件or要員ご指定下さい）もご入用でしたらお送りいたします。",
                "また、弊社からも情報をお送りいたします。",
            ),
        )
        for name, old, new in replacements:
            with self.subTest(condition=name):
                changed = copy.deepcopy(self.original)
                changed["body_text"] = changed["body_text"].replace(old, new)
                self.assertIsNone(target.detect_template_exclusion(changed))

        changed = copy.deepcopy(self.original)
        changed["subject"] = "情報交換のお願い"
        self.assertIsNone(target.detect_template_exclusion(changed))

        changed = copy.deepcopy(self.original)
        changed["attachments"] = [{"filename": "individual.xlsx"}]
        self.assertIsNone(target.detect_template_exclusion(changed))

        changed = copy.deepcopy(self.original)
        changed["body_text"] += "\n【氏名】個別要員\n【単価】70万円"
        self.assertIsNone(target.detect_template_exclusion(changed))

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
        expected_new_ids = PREVIOUS_RESOURCE_IDS | {TARGET_ID, SAKYA_PROJECT_ID}

        self.assertEqual(filtered, [
            row for row in old_filtered if row["message_id"] not in expected_new_ids
        ])
        self.assertEqual(new_removed_ids - old_removed_ids, expected_new_ids)
        self.assertEqual(new_removed_ids - expected_new_ids, old_removed_ids)
        self.assertEqual((len(filtered), len(removed), len(details)), (2633, 117, 20))
        self.assertTrue(all(set(row) == {"message_id"} for row in filtered + removed))
        self.assertEqual(
            [row for row in details if row["message_id"] == TARGET_ID],
            [target.build_detail_record(self.original, *DECISION)],
        )

        spec = importlib.util.spec_from_file_location(
            "confirm_route_zero_information_share",
            STEP / "02_confirm" / "confirm_remove_individual_email.py",
        )
        confirm = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(confirm)
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

        projects = read_jsonl_as_list(str(
            ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl"
        ))
        resources = read_jsonl_as_list(str(
            ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "resources.jsonl"
        ))
        project_ids = {row["message_id"] for row in projects}
        resource_ids = {row["message_id"] for row in resources}
        self.assertEqual((len(projects), len(resources)), (811, 1835))
        self.assertIn(TARGET_ID, resource_ids)
        self.assertNotIn(TARGET_ID, project_ids)
        self.assertEqual(len(resource_ids - {TARGET_ID}), 1834)
        self.assertEqual(len(project_ids - {SAKYA_PROJECT_ID}), 810)
        self.assertEqual(
            len(resource_ids - PREVIOUS_RESOURCE_IDS - {TARGET_ID}),
            1820,
        )

    def test_downstream_existing_artifact_id_reconciliation(self):
        paths = {
            "all_pairs": (
                ROOT / "06-0_match_all_message_id" / "01_result" / "matched_pairs_all.jsonl",
                811,
            ),
            "06-7": (
                ROOT / "06-7_match_vendor_tiers" / "01_result"
                / "matched_pairs_vendor_tiers.jsonl",
                224,
            ),
            "07": (
                ROOT / "07-1_requirement_skill_ai_matching" / "01_result"
                / "requirement_skill_ai_matching.jsonl",
                0,
            ),
            "07_error": (
                ROOT / "07-1_requirement_skill_ai_matching" / "01_result"
                / "99_error_requirement_skill_ai_matching.jsonl",
                0,
            ),
        }
        for name, (path, expected) in paths.items():
            with self.subTest(artifact=name):
                with path.open(encoding="utf-8") as stream:
                    actual = sum(
                        (json.loads(line).get("resource_info") or {}).get("message_id")
                        == TARGET_ID
                        for line in stream if line.strip()
                    )
                self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()

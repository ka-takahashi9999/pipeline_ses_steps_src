#!/usr/bin/env python3
"""Sakya調達代行サービス案内テンプレートのFocused Test。"""

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


TARGET_ID = "1a0d1dc366b84837"
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
    "1a0d2a71e8f16c5f",
}
DECISION = (
    "service_notification",
    "sakya_procurement_service_notice_v1",
)


class SakyaProcurementServiceNoticeTest(unittest.TestCase):
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
        changed["message_id"] = "future-procurement-service-notice"
        self.assertEqual(target.detect_template_exclusion(changed), DECISION)
        self.assertNotIn(
            TARGET_ID,
            inspect.getsource(target.detect_template_exclusion),
        )

    def test_same_sender_normal_resource_mails_are_kept(self):
        prev_ids = {row["message_id"] for row in self.prev}
        same_sender_ids = {
            mid for mid, record in self.master.items()
            if mid in prev_ids
            and target.extract_email(record.get("from") or "")
            == "kensuke.kiyota@sakya.jp"
        }
        normal_ids = same_sender_ids - {TARGET_ID}
        self.assertEqual(len(normal_ids), 3)
        for mid in normal_ids:
            with self.subTest(message_id=mid):
                self.assertIsNone(target.detect_template_exclusion(self.master[mid]))

    def test_all_saved_sakya_normal_mails_are_kept(self):
        sakya_ids = {
            mid for mid, record in self.master.items()
            if target.extract_email(record.get("from") or "").endswith("@sakya.jp")
        }
        self.assertEqual(len(sakya_ids), 255)
        detected_ids = {
            mid for mid in sakya_ids
            if target.detect_template_exclusion(self.master[mid]) == DECISION
        }
        self.assertEqual(detected_ids, {TARGET_ID})

    def test_missing_conditions_are_kept(self):
        changes = (
            (
                "service_name",
                "body_text",
                self.original["body_text"].replace("サクヤ調達部", "調達サービス"),
            ),
            (
                "scale",
                "body_text",
                self.original["body_text"].replace("協力会社5,000社", "協力会社多数"),
            ),
            (
                "cta",
                "body_text",
                self.original["body_text"].replace(
                    "本メールにご返信のうえ、案件概要（スキル・単価帯・時期）をお送りください。",
                    "ご興味があればご返信ください。",
                ),
            ),
            ("subject", "subject", "エンジニア調達サービスのご案内"),
            (
                "attachment",
                "attachments",
                [{"filename": "service.pdf", "mime_type": "application/pdf"}],
            ),
        )
        for name, key, value in changes:
            with self.subTest(condition=name):
                changed = copy.deepcopy(self.original)
                changed[key] = value
                self.assertIsNone(target.detect_template_exclusion(changed))

        individual = copy.deepcopy(self.original)
        individual["body_text"] += "\n【案件名】：個別開発案件"
        self.assertIsNone(target.detect_template_exclusion(individual))

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
        expected_new_ids = PREVIOUS_RESOURCE_IDS | {TARGET_ID}

        self.assertEqual(filtered, [
            row for row in old_filtered if row["message_id"] not in expected_new_ids
        ])
        self.assertEqual(new_removed_ids - old_removed_ids, expected_new_ids)
        self.assertEqual(new_removed_ids - expected_new_ids, old_removed_ids)
        self.assertEqual((len(filtered), len(removed), len(details)), (2633, 117, 20))
        self.assertTrue(all(set(row) == {"message_id"} for row in filtered + removed))

        target_details = [row for row in details if row["message_id"] == TARGET_ID]
        self.assertEqual(
            target_details,
            [target.build_detail_record(self.original, *DECISION)],
        )
        detected_ids = {
            row["message_id"] for row in self.prev
            if target.detect_template_exclusion(self.master[row["message_id"]]) == DECISION
        }
        self.assertEqual(detected_ids, {TARGET_ID})

        spec = importlib.util.spec_from_file_location(
            "confirm_sakya_procurement",
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
        self.assertIn(TARGET_ID, project_ids)
        self.assertNotIn(TARGET_ID, resource_ids)
        self.assertEqual(len(project_ids - {TARGET_ID}), 810)
        self.assertEqual(len(resource_ids - PREVIOUS_RESOURCE_IDS), 1820)

    def test_downstream_existing_artifact_id_reconciliation(self):
        paths = {
            "all_pairs": (
                ROOT / "06-0_match_all_message_id" / "01_result" / "matched_pairs_all.jsonl",
                1835,
                lambda row: (row.get("project_info") or {}).get("message_id"),
            ),
            "06-7": (
                ROOT / "06-7_match_vendor_tiers" / "01_result"
                / "matched_pairs_vendor_tiers.jsonl",
                1144,
                lambda row: (row.get("project_info") or {}).get("message_id"),
            ),
            "07": (
                ROOT / "07-1_requirement_skill_ai_matching" / "01_result"
                / "requirement_skill_ai_matching.jsonl",
                0,
                lambda row: (row.get("project_info") or {}).get("message_id"),
            ),
            "07_error": (
                ROOT / "07-1_requirement_skill_ai_matching" / "01_result"
                / "99_error_requirement_skill_ai_matching.jsonl",
                0,
                lambda row: (row.get("project_info") or {}).get("message_id"),
            ),
        }
        for name, (path, expected, get_project_id) in paths.items():
            with self.subTest(artifact=name):
                with path.open(encoding="utf-8") as stream:
                    actual = sum(
                        get_project_id(json.loads(line)) == TARGET_ID
                        for line in stream if line.strip()
                    )
                self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()

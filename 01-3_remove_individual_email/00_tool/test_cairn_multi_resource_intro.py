#!/usr/bin/env python3
"""CAIRN複数要員紹介テンプレートのFocused Test。"""

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


TARGET_ID = "1a0d0d40178796c0"
PREVIOUS_IMPLEMENTED_IDS = {
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
    "1a0d0f14e9ad61c8",
    "1a0d2a71e8f16c5f",
}
OTHER_PROJECT_IDS = {"1a0d1dc366b84837"}
DECISION = (
    "multi_item_mail",
    "cairn_multi_resource_intro_v1",
)


class CairnMultiResourceIntroTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.master = read_jsonl_as_dict(target.INPUT_MASTER)
        cls.prev = read_jsonl_as_list(target.INPUT_PREV)
        cls.rules = target.load_exclude_list(target.EXCLUDE_LIST_PATH)
        cls.original = cls.master[TARGET_ID]

    def _fixture(self, profile_count, url_count=None):
        if url_count is None:
            url_count = profile_count
        fixture = copy.deepcopy(self.original)
        fixture["message_id"] = "future-{}-resources".format(profile_count)
        fixture["subject"] = (
            "11月~【弊社社員/サーバエンジニア×{}名】SV設計構築/Linux".format(
                profile_count
            )
        )
        intro = """株式会社テクノヴァース
ご担当者 様

お世話になっております。
株式会社CAIRNの営業部です。

11月より稼働可能な、弊社正社員の注力要員をご紹介させていただきます。
見合う案件がございましたら、ぜひご紹介いただけますと幸いです。
"""
        blocks = []
        for number in range(1, profile_count + 1):
            skill_sheet = ""
            if number <= url_count:
                skill_sheet = (
                    "【スキルシート】：\n"
                    "  https://skills.example.invalid/person/{}/variable-token\n".format(
                        number
                    )
                )
            blocks.append("""【名前】：要員{number}（30歳・男性）
【最寄】：新宿駅
【稼動】：11月～
【所属】：弊社正社員
【単価】：60万円
【工程】：設計、構築、テスト
【スキル】：Linux、AWS
【希望】：サーバ設計構築案件
{skill_sheet}""".format(number=number, skill_sheet=skill_sheet))
        fixture["body_text"] = "{}\n{}".format(intro, "\n".join(blocks))
        fixture["attachments"] = []
        return fixture

    def test_real_mail_is_excluded_and_preserved(self):
        self.assertEqual(target.detect_template_exclusion(self.original), DECISION)
        self.assertEqual(
            target.determine_exclusion_reason(self.original, *self.rules),
            "multi_item_mail",
        )
        detail = target.build_detail_record(self.original, *DECISION)
        self.assertEqual(
            {key: value for key, value in detail.items()
             if key not in ("reason", "rule_id")},
            self.original,
        )

    def test_message_id_url_and_profile_count_are_variable(self):
        for profile_count in (2, 3, 4):
            fixture = self._fixture(profile_count)
            with self.subTest(profile_count=profile_count):
                self.assertEqual(target.detect_template_exclusion(fixture), DECISION)

        implementation = inspect.getsource(target.detect_template_exclusion)
        self.assertNotIn(TARGET_ID, implementation)

    def test_same_sender_single_resources_are_kept(self):
        prev_ids = {row["message_id"] for row in self.prev}
        same_sender_ids = {
            mid for mid, record in self.master.items()
            if mid in prev_ids
            and target.extract_email(record.get("from") or "") == "sales@cair-n.co.jp"
        }
        single_ids = same_sender_ids - {TARGET_ID}
        self.assertEqual(len(single_ids), 16)
        for mid in single_ids:
            with self.subTest(message_id=mid):
                self.assertIsNone(target.detect_template_exclusion(self.master[mid]))

        self.assertIsNone(target.detect_template_exclusion(self._fixture(1)))

    def test_url_count_mismatch_and_other_sender_are_kept(self):
        mismatched = self._fixture(3, url_count=2)
        self.assertIsNone(target.detect_template_exclusion(mismatched))

        other_sender = copy.deepcopy(self.original)
        other_sender["from"] = "Other Company <sales@example.com>"
        self.assertIsNone(target.detect_template_exclusion(other_sender))

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
        expected_resource_ids = PREVIOUS_IMPLEMENTED_IDS | {TARGET_ID}
        expected_new_ids = expected_resource_ids | OTHER_PROJECT_IDS

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
            "confirm_cairn",
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

        resources = read_jsonl_as_list(str(
            ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "resources.jsonl"
        ))
        resource_ids = {row["message_id"] for row in resources}
        self.assertEqual(len(resources), 1835)
        self.assertTrue(expected_resource_ids <= resource_ids)
        self.assertEqual(len(resource_ids - {TARGET_ID}), 1834)
        self.assertEqual(len(resource_ids - expected_resource_ids), 1820)

    def test_downstream_existing_artifact_id_reconciliation(self):
        paths = {
            "all_pairs": (
                ROOT / "06-0_match_all_message_id" / "01_result" / "matched_pairs_all.jsonl",
                811,
                lambda row: (row.get("resource_info") or {}).get("message_id"),
            ),
            "06-12": (
                ROOT / "06-12_filter_required_skills_noise" / "01_result"
                / "matched_pairs_required_skills_noise_filtered.jsonl",
                38,
                lambda row: (row.get("resource_info") or {}).get("message_id"),
            ),
            "07_error": (
                ROOT / "07-1_requirement_skill_ai_matching" / "01_result"
                / "99_error_requirement_skill_ai_matching.jsonl",
                36,
                lambda row: (row.get("resource_info") or {}).get("message_id"),
            ),
        }
        for name, (path, expected, get_resource_id) in paths.items():
            with self.subTest(artifact=name):
                with path.open(encoding="utf-8") as stream:
                    actual = sum(
                        get_resource_id(json.loads(line)) == TARGET_ID
                        for line in stream if line.strip()
                    )
                self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()

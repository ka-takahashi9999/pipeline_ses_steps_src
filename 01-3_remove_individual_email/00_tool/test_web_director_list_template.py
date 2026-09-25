#!/usr/bin/env python3
"""アイデンティティーWeb人材一覧ポータルのFocused Test。"""

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


DESIGNER_ID = "1a0d100d5eaf636f"
DIRECTOR_ID = "1a0cbd3700ed3089"
SINGLE_DIRECTOR_ID = "1a0cbce0b08ac2b2"
OUTSIDE_ALLOWLIST_ID = "1a03ba5745e6d364"
RADSTATE_IDS = {
    "1a0d272e7765710e",
    "1a0d272c067692c8",
    "1a0d272b9faf053e",
}
WEBBOLT_IDS = {"1a0d0d5eab75f453"}
TECHNICATION_IDS = {
    "1a0d15e5f06de61a",
    "1a0d14c378c2ff81",
    "1a0d12c92e945be9",
}
SIGNPOST_IDS = {"1a0d1454442c23cf", "1a0d143ae017c9d5"}
ALIPLAZA_IDS = {"1a0d126231435729"}
T_E_SYSTEM_IDS = {"1a0d0f67fdd66805"}
CAIRN_IDS = {"1a0d0d40178796c0"}
SAKYA_RESOURCE_IDS = {"1a0d0f14e9ad61c8"}
ROUTE_ZERO_RESOURCE_IDS = {"1a0d2a71e8f16c5f"}
SAKYA_PROJECT_IDS = {"1a0d1dc366b84837"}
DESIGNER_DECISION = (
    "list_or_portal_notice",
    "identity_web_designer_list_portal_v1",
)
DIRECTOR_DECISION = (
    "list_or_portal_notice",
    "identity_web_director_list_portal_v1",
)


class IdentityWebListTemplateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.master = read_jsonl_as_dict(target.INPUT_MASTER)
        cls.prev = read_jsonl_as_list(target.INPUT_PREV)
        cls.rules = target.load_exclude_list(target.EXCLUDE_LIST_PATH)
        cls.designer = cls.master[DESIGNER_ID]

    def _role_fixture(self, role: str, message_id: str):
        fixture = copy.deepcopy(self.designer)
        fixture["message_id"] = message_id
        fixture["subject"] = fixture["subject"].replace("Webデザイナー", role)
        fixture["body_text"] = fixture["body_text"].replace("Webデザイナー", role)
        return fixture

    def test_web_designer_real_mail_is_excluded_and_preserved(self):
        self.assertEqual(target.detect_template_exclusion(self.designer), DESIGNER_DECISION)
        self.assertEqual(
            target.determine_exclusion_reason(self.designer, *self.rules),
            "list_or_portal_notice",
        )
        detail = target.build_detail_record(self.designer, *DESIGNER_DECISION)
        self.assertEqual(
            {key: value for key, value in detail.items()
             if key not in ("reason", "rule_id")},
            self.designer,
        )

    def test_web_director_rule_id_is_preserved(self):
        director = self._role_fixture("Webディレクター", DIRECTOR_ID)
        self.assertEqual(target.detect_template_exclusion(director), DIRECTOR_DECISION)

    def test_single_web_director_profile_is_kept(self):
        single = self._role_fixture("Webディレクター", SINGLE_DIRECTOR_ID)
        single["body_text"] = single["body_text"].replace(
            "■人材一覧リスト",
            "氏名：A.T\n年齢：35歳\n最寄駅：新宿駅\n単価：70万円\n■人材一覧リスト",
        )
        self.assertIsNone(target.detect_template_exclusion(single))
        self.assertIsNone(target.determine_exclusion_reason(single, set(), []))

    def test_message_id_and_portal_url_are_variable(self):
        changed = copy.deepcopy(self.designer)
        changed["message_id"] = "future-message-id"
        old_url = next(
            link["href"] for link in changed["html_links"]
            if "-edit-gid-" in link.get("href", "")
        )
        new_url = (
            "https://info.techcareer.jp/e/998201/"
            "newSheet-edit-gid-555-range-A1/newDelivery/123456/h/newToken"
        )
        changed["body_text"] = changed["body_text"].replace(old_url, new_url)
        for link in changed["html_links"]:
            if link.get("href") == old_url:
                link["href"] = new_url
                link["text"] = new_url
        self.assertEqual(target.detect_template_exclusion(changed), DESIGNER_DECISION)

        implementation = inspect.getsource(target.detect_template_exclusion)
        for message_id in (DESIGNER_ID, DIRECTOR_ID, SINGLE_DIRECTOR_ID):
            self.assertNotIn(message_id, implementation)

    def test_allowlist_and_structure_guards(self):
        self.assertEqual(
            set(target._IDENTITY_WEB_LIST_RULE_IDS),
            {"Webディレクター", "Webデザイナー"},
        )
        for role in ("JavaScript", "PM", "エンジニア"):
            subject_only = copy.deepcopy(self.designer)
            subject_only["subject"] = "ご提案可能な営業中{}のご紹介".format(role)
            both_changed = self._role_fixture(role, "outside-{}".format(role))
            with self.subTest(role=role):
                self.assertIsNone(target.detect_template_exclusion(subject_only))
                self.assertIsNone(target.detect_template_exclusion(both_changed))

        outside = copy.deepcopy(self.designer)
        outside["message_id"] = OUTSIDE_ALLOWLIST_ID
        outside["subject"] = "ご提案可能な営業中JavaScriptのご紹介"
        self.assertIsNone(target.detect_template_exclusion(outside))

        guard_changes = (
            ("from", "Other Company <bp@example.com>"),
            ("subject", "案件一覧のご紹介"),
            ("attachments", [{"filename": "skill.xlsx"}]),
            ("html_links", []),
            ("body_text", "案件一覧です"),
        )
        for key, value in guard_changes:
            with self.subTest(key=key):
                self.assertIsNone(target.detect_template_exclusion(
                    dict(self.designer, **{key: value})
                ))

        mismatched = copy.deepcopy(self.designer)
        mismatched["subject"] = mismatched["subject"].replace(
            "Webデザイナー", "Webディレクター"
        )
        self.assertIsNone(target.detect_template_exclusion(mismatched))

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
            RADSTATE_IDS | WEBBOLT_IDS | TECHNICATION_IDS | SIGNPOST_IDS
            | ALIPLAZA_IDS
            | T_E_SYSTEM_IDS
            | CAIRN_IDS
            | SAKYA_RESOURCE_IDS
            | ROUTE_ZERO_RESOURCE_IDS
            | SAKYA_PROJECT_IDS
            | {DESIGNER_ID}
        )

        self.assertEqual(filtered, [
            row for row in old_filtered if row["message_id"] not in expected_new_ids
        ])
        self.assertEqual(new_removed_ids - old_removed_ids, expected_new_ids)
        self.assertEqual(new_removed_ids - expected_new_ids, old_removed_ids)
        self.assertEqual((len(filtered), len(removed), len(details)), (2633, 117, 20))
        self.assertTrue(all(set(row) == {"message_id"} for row in filtered + removed))

        designer_details = [row for row in details if row["message_id"] == DESIGNER_ID]
        self.assertEqual(
            designer_details,
            [target.build_detail_record(self.designer, *DESIGNER_DECISION)],
        )
        designer_ids = {
            row["message_id"] for row in self.prev
            if target.detect_template_exclusion(self.master[row["message_id"]])
            == DESIGNER_DECISION
        }
        self.assertEqual(designer_ids, {DESIGNER_ID})

        spec = importlib.util.spec_from_file_location(
            "confirm_identity_web_list",
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
        self.assertTrue(
            RADSTATE_IDS | WEBBOLT_IDS | TECHNICATION_IDS | SIGNPOST_IDS
            | ALIPLAZA_IDS
                | T_E_SYSTEM_IDS
                | CAIRN_IDS
                | SAKYA_RESOURCE_IDS
                | ROUTE_ZERO_RESOURCE_IDS
                | {DESIGNER_ID}
            <= resource_ids
        )
        self.assertEqual(len(resource_ids - {DESIGNER_ID}), 1834)
        self.assertEqual(
            len(
                resource_ids
                - RADSTATE_IDS
                - WEBBOLT_IDS
                - TECHNICATION_IDS
                - SIGNPOST_IDS
                - ALIPLAZA_IDS
                - T_E_SYSTEM_IDS
                - CAIRN_IDS
                - SAKYA_RESOURCE_IDS
                - ROUTE_ZERO_RESOURCE_IDS
                - {DESIGNER_ID}
            ),
            1820,
        )

    def test_downstream_existing_artifact_id_reconciliation(self):
        paths = {
            "all_pairs": (
                ROOT / "06-0_match_all_message_id" / "01_result" / "matched_pairs_all.jsonl",
                811,
                lambda row: (row.get("resource_info") or {}).get("message_id"),
            ),
            "07": (
                ROOT / "07-1_requirement_skill_ai_matching" / "01_result"
                / "requirement_skill_ai_matching.jsonl",
                0,
                lambda row: (row.get("resource_info") or {}).get("message_id"),
            ),
            "08-5": (
                ROOT / "08-5_high_score_required_skill_recheck" / "01_result"
                / "high_score_required_skill_recheck_all.jsonl",
                0,
                lambda row: (row.get("resource_info") or {}).get("message_id"),
            ),
            "sales_candidates": (
                ROOT / "09-4_remove_category_mismatch_sales_candidates" / "01_result"
                / "sales_proposal_candidates_20260924.jsonl",
                0,
                lambda row: row.get("resource_message_id"),
            ),
            "drafts": (
                ROOT / "09-5_generate_sales_reply_draft" / "01_result"
                / "generate_sales_reply_draft_20260924.jsonl",
                0,
                lambda row: row.get("resource_message_id"),
            ),
        }
        for name, (path, expected, get_resource_id) in paths.items():
            with self.subTest(artifact=name):
                with path.open(encoding="utf-8") as stream:
                    actual = sum(
                        get_resource_id(json.loads(line)) == DESIGNER_ID
                        for line in stream if line.strip()
                    )
                self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()

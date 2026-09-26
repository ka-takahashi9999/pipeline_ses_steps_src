#!/usr/bin/env python3
"""2026-09-25監査の2種サービス通知に限定したFocused Test。"""

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


CLARIFY_ID = "1a0d7b474bf60fb3"
ESMC_ID = "1a0d74b834c8ec69"
TARGET_IDS = {CLARIFY_ID, ESMC_ID}
CLARIFY_DECISION = (
    "service_notification",
    "mail_distribution_registration_notice_v1",
)
ESMC_DECISION = (
    "service_notification",
    "esmc_networking_event_notice_v1",
)
DECISIONS = {CLARIFY_DECISION, ESMC_DECISION}


class TwoServiceNotificationTemplatesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.master = read_jsonl_as_dict(target.INPUT_MASTER)
        cls.prev = read_jsonl_as_list(target.INPUT_PREV)
        cls.rules = target.load_exclude_list(target.EXCLUDE_LIST_PATH)
        cls.clarify = cls.master[CLARIFY_ID]
        cls.esmc = cls.master[ESMC_ID]

    def test_real_mails_are_excluded_with_full_original_detail(self):
        for original, decision in (
            (self.clarify, CLARIFY_DECISION),
            (self.esmc, ESMC_DECISION),
        ):
            with self.subTest(message_id=original["message_id"]):
                self.assertEqual(target.detect_template_exclusion(original), decision)
                self.assertEqual(
                    target.determine_exclusion_reason(original, *self.rules),
                    "service_notification",
                )
                detail = target.build_detail_record(original, *decision)
                self.assertEqual(detail["reason"], "service_notification")
                self.assertEqual(detail["rule_id"], decision[1])
                self.assertEqual(
                    {key: value for key, value in detail.items()
                     if key not in ("reason", "rule_id")},
                    original,
                )

    def test_message_id_is_not_a_condition(self):
        for original, decision, changed_id in (
            (self.clarify, CLARIFY_DECISION, "future-clarify-registration"),
            (self.esmc, ESMC_DECISION, "future-esmc-networking-event"),
        ):
            changed = copy.deepcopy(original)
            changed["message_id"] = changed_id
            self.assertEqual(target.detect_template_exclusion(changed), decision)
        detector_source = inspect.getsource(target.detect_template_exclusion)
        self.assertNotIn(CLARIFY_ID, detector_source)
        self.assertNotIn(ESMC_ID, detector_source)

    def test_latest_3000_and_saved_normal_mails_have_no_false_exclusion(self):
        self.assertEqual(len(self.master), 3000)
        saved_ids = {
            row["message_id"] for row in read_jsonl_as_list(
                str(STEP / "01_result" / target.OUTPUT_FILTERED)
            )
        }
        populations = {
            "clarify_sender": {
                mid for mid, record in self.master.items()
                if target.extract_email(record.get("from") or "")
                == "sales@clarify.co.jp"
            },
            "clarify_domain": {
                mid for mid, record in self.master.items()
                if target.extract_email(record.get("from") or "").endswith(
                    "@clarify.co.jp"
                )
            },
            "esmc_sender": {
                mid for mid, record in self.master.items()
                if target.extract_email(record.get("from") or "")
                == "esmc@ses.cre-co.jp"
            },
            "cre_co_domain": {
                mid for mid, record in self.master.items()
                if target.extract_email(record.get("from") or "").endswith(
                    "@ses.cre-co.jp"
                )
            },
        }
        self.assertEqual(
            {name: len(ids) for name, ids in populations.items()},
            {
                "clarify_sender": 46,
                "clarify_domain": 47,
                "esmc_sender": 1,
                "cre_co_domain": 8,
            },
        )
        expected_targets = {
            "clarify_sender": {CLARIFY_ID},
            "clarify_domain": {CLARIFY_ID},
            "esmc_sender": {ESMC_ID},
            "cre_co_domain": {ESMC_ID},
        }
        for name, ids in populations.items():
            with self.subTest(population=name):
                detected = {
                    mid for mid in ids
                    if target.detect_template_exclusion(self.master[mid]) in DECISIONS
                }
                self.assertEqual(detected, expected_targets[name])
                saved_normal_ids = (ids & saved_ids) - TARGET_IDS
                self.assertFalse({
                    mid for mid in saved_normal_ids
                    if target.detect_template_exclusion(self.master[mid]) in DECISIONS
                })

    def test_clarify_missing_conditions_are_kept(self):
        changes = (
            ("sender", "from", "Other <other@clarify.co.jp>"),
            ("subject", "subject", "メールアドレス登録のお願い"),
            (
                "registration_context",
                "body_text",
                self.clarify["body_text"].replace(
                    "貴社より案件情報を配信されているメールアドレスの登録先に、",
                    "貴社のご担当者様に、",
                ),
            ),
            (
                "fixed_request",
                "body_text",
                self.clarify["body_text"].replace(
                    "弊社の下記アドレスを追加いただくことは可能でしょうか。",
                    "弊社までご連絡いただけますでしょうか。",
                ),
            ),
            (
                "distribution_address",
                "body_text",
                self.clarify["body_text"].replace(
                    "【配信先アドレス】", "【連絡先】"
                ),
            ),
            (
                "attachment",
                "attachments",
                [{"filename": "resource.xlsx"}],
            ),
            (
                "html_link",
                "html_links",
                [{"text": "登録", "href": "https://example.com"}],
            ),
            (
                "individual_profile",
                "body_text",
                self.clarify["body_text"] + "\n【氏名】A.T\n【単価】70万円",
            ),
        )
        for name, key, value in changes:
            with self.subTest(condition=name):
                changed = copy.deepcopy(self.clarify)
                changed[key] = value
                self.assertIsNone(target.detect_template_exclusion(changed))

    def test_esmc_missing_conditions_are_kept(self):
        application_links = [
            link for link in self.esmc["html_links"]
            if link.get("text") != "参加申込はコチラから！"
        ]
        changes = (
            ("sender", "from", "Other <other@ses.cre-co.jp>"),
            ("subject", "subject", "ESMC交流会のご案内"),
            (
                "organizer",
                "body_text",
                self.esmc["body_text"].replace(
                    "ESMC運営事務局で御座います。", "運営事務局です。"
                ),
            ),
            (
                "announcement",
                "body_text",
                self.esmc["body_text"].replace(
                    "下記日時で、交流会を開催します。", "イベントをご案内します。"
                ),
            ),
            (
                "application_text",
                "body_text",
                self.esmc["body_text"].replace(
                    "＞＞参加申込はコチラから！", "＞＞詳細はコチラから！"
                ),
            ),
            ("application_html_link", "html_links", application_links),
            ("attachment", "attachments", [{"filename": "profile.xlsx"}]),
            (
                "individual_project",
                "body_text",
                self.esmc["body_text"] + "\n案件名: 個別開発案件\n必須スキル: Java",
            ),
        )
        for name, key, value in changes:
            with self.subTest(condition=name):
                changed = copy.deepcopy(self.esmc)
                changed[key] = value
                self.assertIsNone(target.detect_template_exclusion(changed))

    def test_latest_run_memory_replay_and_confirm(self):
        result_dir = STEP / "01_result"
        old_filtered = read_jsonl_as_list(
            str(result_dir / target.OUTPUT_FILTERED)
        )
        old_removed = read_jsonl_as_list(
            str(result_dir / target.OUTPUT_REMOVED)
        )
        old_details = read_jsonl_as_list(
            str(result_dir / target.OUTPUT_DETAIL)
        )
        self.assertEqual(
            (len(self.prev), len(old_filtered), len(old_removed), len(old_details)),
            (2749, 2628, 121, 11),
        )

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
        ), patch.object(target, "write_execution_time"), patch.object(
            target, "logger"
        ):
            target.main()

        filtered = captured[target.OUTPUT_FILTERED]
        removed = captured[target.OUTPUT_REMOVED]
        details = captured[target.OUTPUT_DETAIL]
        old_filtered_ids = {row["message_id"] for row in old_filtered}
        old_removed_ids = {row["message_id"] for row in old_removed}
        new_filtered_ids = {row["message_id"] for row in filtered}
        new_removed_ids = {row["message_id"] for row in removed}

        self.assertEqual(old_filtered_ids - new_filtered_ids, TARGET_IDS)
        self.assertEqual(new_removed_ids - old_removed_ids, TARGET_IDS)
        self.assertEqual(new_removed_ids - TARGET_IDS, old_removed_ids)
        self.assertEqual((len(filtered), len(removed), len(details)), (2626, 123, 13))
        self.assertTrue(all(set(row) == {"message_id"} for row in filtered + removed))
        expected_target_details = {
            CLARIFY_ID: target.build_detail_record(
                self.clarify, *CLARIFY_DECISION
            ),
            ESMC_ID: target.build_detail_record(self.esmc, *ESMC_DECISION),
        }
        self.assertEqual(
            {row["message_id"]: row for row in details if row["message_id"] in TARGET_IDS},
            expected_target_details,
        )

        spec = importlib.util.spec_from_file_location(
            "confirm_two_service_notifications",
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
            confirm, "read_jsonl_as_dict", return_value=self.master
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
            ROOT / "02-2_classify_output_file_project_resource" / "01_result"
            / "projects.jsonl"
        ))
        resources = read_jsonl_as_list(str(
            ROOT / "02-2_classify_output_file_project_resource" / "01_result"
            / "resources.jsonl"
        ))
        project_ids = {row["message_id"] for row in projects}
        resource_ids = {row["message_id"] for row in resources}
        self.assertEqual((len(projects), len(resources)), (761, 1864))
        self.assertFalse(project_ids & TARGET_IDS)
        self.assertTrue(TARGET_IDS <= resource_ids)
        self.assertEqual(len(resource_ids - TARGET_IDS), 1862)

    def test_pair_and_07_onward_impact(self):
        pair_paths = {
            "06-0": (
                ROOT / "06-0_match_all_message_id" / "01_result"
                / "matched_pairs_all.jsonl",
                1418504,
            ),
            "06-1": (
                ROOT / "06-1_match_budget" / "01_result"
                / "matched_pairs_budget.jsonl",
                510064,
            ),
        }
        for name, (path, expected_total) in pair_paths.items():
            counts = {mid: 0 for mid in TARGET_IDS}
            total = 0
            with path.open(encoding="utf-8") as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    total += 1
                    resource_id = (
                        json.loads(line).get("resource_info") or {}
                    ).get("message_id")
                    if resource_id in counts:
                        counts[resource_id] += 1
            with self.subTest(artifact=name):
                self.assertEqual(total, expected_total)
                self.assertEqual(counts, {CLARIFY_ID: 761, ESMC_ID: 761})
                self.assertEqual(total - sum(counts.values()), expected_total - 1522)

        downstream_paths = [
            ROOT / "07-1_requirement_skill_ai_matching" / "01_result"
            / "requirement_skill_ai_matching.jsonl",
            ROOT / "07-1_requirement_skill_ai_matching" / "01_result"
            / "99_error_requirement_skill_ai_matching.jsonl",
            ROOT / "08-1_restore_and_merge_requirement_skill_ai_matching" / "01_result"
            / "merged_requirement_skill_ai_matching.jsonl",
            ROOT / "08-1_restore_and_merge_requirement_skill_ai_matching" / "01_result"
            / "99_error_restore_requirement_skill_ai_matching.jsonl",
            ROOT / "08-5_high_score_required_skill_recheck" / "01_result"
            / "high_score_required_skill_recheck_all.jsonl",
            ROOT / "08-5_high_score_required_skill_recheck" / "01_result"
            / "99_error_high_score_required_skill_recheck.jsonl",
        ]
        downstream_paths.extend(sorted(ROOT.glob("09-*/01_result/*20260925*.jsonl")))
        self.assertGreaterEqual(len(downstream_paths), 13)
        for path in downstream_paths:
            with self.subTest(artifact=str(path.relative_to(ROOT))):
                with path.open(encoding="utf-8") as stream:
                    self.assertFalse(any(
                        CLARIFY_ID in line or ESMC_ID in line for line in stream
                    ))


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Focused tests for the 01-3 false-exclusion shadow decision model."""

import importlib.util
import sys
import unittest
from pathlib import Path
from typing import Any, Dict, List


TOOL_DIR = Path(__file__).resolve().parent
MODULE_PATH = TOOL_DIR / "run_01_3_false_exclusion_shadow.py"
spec = importlib.util.spec_from_file_location("shadow_01_3", MODULE_PATH)
if spec is None or spec.loader is None:
    raise ImportError("cannot load shadow module")
target = importlib.util.module_from_spec(spec)
spec.loader.exec_module(target)


def record(
    sender: str,
    subject: str,
    body: str = "",
    attachments: List[Dict[str, Any]] = None,
    links: List[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "message_id": "synthetic-id",
        "from": sender,
        "subject": subject,
        "body_text": body,
        "attachments": attachments or [],
        "html_links": links or [],
    }


class ShadowDecisionTest(unittest.TestCase):
    def test_source_specific_notification_is_excluded(self) -> None:
        mail = record(
            "noreply-apps-scripts-notifications@google.com",
            "Summary of failures for Google Apps Script: Cleanup",
            "Your script has recently failed to finish successfully. "
            "Sincerely, Google Apps Script. Please do not reply.",
        )
        result = target.shadow_decision(mail, "FROM_SUBJECT", [])
        self.assertEqual("EXCLUDE", result["decision"])
        self.assertEqual("CLEAR_EXCLUDE", target.observation_label(mail, []))

    def test_sender_alone_does_not_exclude_single_resource(self) -> None:
        mail = record(
            "sales@example.com",
            "【要員情報】Javaエンジニアのご紹介",
            "弊社社員のスキルと経歴です。単価70万円、10月参画可能です。",
        )
        result = target.shadow_decision(mail, "FROM_ONLY", [])
        self.assertEqual("KEEP", result["decision"])
        self.assertEqual("LIKELY_VALID_SINGLE", target.observation_label(mail, []))

    def test_single_project_needs_no_resource_marker(self) -> None:
        mail = record(
            "sales@example.com",
            "【案件募集】Java開発、10月開始",
            "案件概要と必須スキル、単価、面談条件をご確認ください。",
        )
        result = target.shadow_decision(mail, "FROM_ONLY", [])
        self.assertEqual("KEEP", result["decision"])
        self.assertIn("project_sales_structure", result["decision_evidence"])

    def test_end_direct_project_subject_is_positive_evidence(self) -> None:
        mail = record(
            "sales@example.com",
            "【エンド直】100万円 / Python / リモート / 開発支援",
        )
        result = target.shadow_decision(mail, "FROM_ONLY", [])
        self.assertEqual("KEEP", result["decision"])
        self.assertIn("project_subject_format", result["decision_evidence"])

    def test_from_subject_multi_item_route_is_kept(self) -> None:
        mail = record(
            "inoue@netwisdom.co.jp",
            "10月入場/要員2名ご提案/NetWisdom株式会社",
            "技術者 : AA\n面接予定\n--------------------\n"
            "技術者 : BB\n結果待ち\n以上",
        )
        result = target.shadow_decision(mail, "FROM_SUBJECT", ["INLINE"])
        self.assertEqual("KEEP", result["decision"])
        self.assertEqual(
            "LIKELY_VALID_MULTI", target.observation_label(mail, ["INLINE"])
        )

    def test_attachment_candidate_is_kept(self) -> None:
        mail = record(
            "partner@example.com",
            "弊社社員のご紹介",
            "要員のスキルシートです。",
            attachments=[{"filename": "skill.xlsx"}],
        )
        result = target.shadow_decision(mail, "FROM_ONLY", [])
        self.assertEqual("KEEP", result["decision"])
        self.assertIn("processable_attachment", result["decision_evidence"])

    def test_google_sheet_is_remote_candidate_keep_without_access(self) -> None:
        mail = record(
            "sales@example.com",
            "要員情報",
            links=[
                {
                    "text": "営業中要員",
                    "href": "https://docs.google.com/spreadsheets/d/example/edit",
                }
            ],
        )
        result = target.shadow_decision(mail, "FROM_ONLY", [])
        self.assertEqual("KEEP", result["decision"])
        self.assertEqual(
            ["google_spreadsheet_url"], target.remote_candidate_evidence(mail)
        )

    def test_remote_skillsheet_link_is_kept_for_later_processing(self) -> None:
        mail = record(
            "sales@example.com",
            "【必見】Java即戦力",
            "メールがうまく表示されない方はこちらをご覧ください。",
            links=[
                {
                    "text": "スキルシートはこちら",
                    "href": "https://delivery.example.com/candidate/abc",
                }
            ],
        )
        result = target.shadow_decision(mail, "FROM_ONLY", [])
        self.assertEqual("KEEP", result["decision"])
        self.assertEqual(
            ["remote_skillsheet_link"], target.remote_candidate_evidence(mail)
        )

    def test_unknown_current_hit_is_review_not_exclude(self) -> None:
        mail = record("unknown@example.com", "ご連絡", "詳細は後ほど送ります。")
        result = target.shadow_decision(mail, "FROM_ONLY", [])
        self.assertEqual("REVIEW", result["decision"])

    def test_current_survivor_is_preserved(self) -> None:
        mail = record("unknown@example.com", "ご連絡")
        result = target.shadow_decision(mail, "NONE", [])
        self.assertEqual("KEEP", result["decision"])
        self.assertEqual("current_survivor_preserved", result["decision_reason"])

    def test_unanchored_notification_subject_is_not_excluded(self) -> None:
        mail = record(
            "noreply-apps-scripts-notifications@google.com",
            "Re: Summary of failures for Google Apps Script: Cleanup",
            "Your script has recently failed to finish successfully. "
            "Google Apps Script. Please do not reply.",
        )
        self.assertEqual([], target.strong_exclusion_evidence(mail))


if __name__ == "__main__":
    unittest.main()

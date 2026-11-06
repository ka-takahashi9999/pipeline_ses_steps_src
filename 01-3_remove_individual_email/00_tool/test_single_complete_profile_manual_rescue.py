#!/usr/bin/env python3
"""manual除外より単一complete profileを優先する一般化条件のfocused test。"""

import sys
import unittest
from pathlib import Path
from unittest import mock


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parents[1]
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

import remove_individual_email as target


MANUAL_FROM = {"manual@example.com"}
NO_SUBJECT_RULES = []


def _profile(name="A.B"):
    return (
        "氏名: {}\n"
        "所属: 弊社社員\n"
        "最寄駅: 新宿駅\n"
        "希望単価: 70万円\n"
        "稼働開始: 即日\n"
        "スキル: Java, SQL\n"
    ).format(name)


def _record(subject="人材のご紹介", body=None, attachments=None, html_links=None):
    return {
        "message_id": "fixture",
        "from": "Manual Sender <manual@example.com>",
        "subject": subject,
        "body_text": _profile() if body is None else body,
        "attachments": [] if attachments is None else attachments,
        "html_links": [] if html_links is None else html_links,
    }


class SingleCompleteProfileManualRescueTest(unittest.TestCase):
    def assert_rescued(self, record, from_only_set=None, subject_rules=None):
        self.assertIsNone(
            target.determine_exclusion_reason(
                record,
                MANUAL_FROM if from_only_set is None else from_only_set,
                NO_SUBJECT_RULES if subject_rules is None else subject_rules,
            )
        )

    def assert_manual(self, record, from_only_set=None, subject_rules=None):
        self.assertEqual(
            target.determine_exclusion_reason(
                record,
                MANUAL_FROM if from_only_set is None else from_only_set,
                NO_SUBJECT_RULES if subject_rules is None else subject_rules,
            ),
            "manual_exclude_list",
        )

    def test_broad_manual_sender_with_single_complete_profile_is_rescued(self):
        self.assert_rescued(_record())

    def test_manual_subject_with_single_complete_profile_is_rescued(self):
        record = _record(subject="manual profile")
        self.assert_rescued(
            record,
            from_only_set=set(),
            subject_rules=[("manual@example.com", "manual profile")],
        )

    def test_attachment_single_complete_profile_is_rescued(self):
        self.assert_rescued(
            _record(attachments=[{"filename": "skill-sheet.xlsx"}])
        )

    def test_ordinary_url_single_complete_profile_is_rescued(self):
        body = _profile() + "スキルシート: https://files.example.com/profile\n"
        self.assert_rescued(
            _record(
                body=body,
                html_links=[{
                    "text": "スキルシート",
                    "href": "https://files.example.com/profile",
                    "source": "text/plain",
                }],
            )
        )

    def test_body_only_single_complete_profile_is_rescued(self):
        self.assert_rescued(_record(body=_profile()))

    def test_multiple_people_are_not_rescued(self):
        self.assert_manual(_record(body=_profile("A.B") + "\n" + _profile("C.D")))

    def test_external_list_portal_is_not_rescued(self):
        body = _profile() + "\n人材一覧\nhttps://portal.example.com/people\n"
        self.assert_manual(_record(body=body))

    def test_service_notification_is_not_rescued(self):
        with mock.patch.object(
            target,
            "detect_template_exclusion",
            return_value=("service_notification", "generic_service_notice_v1"),
        ):
            self.assert_manual(_record())

    def test_project_structure_is_not_rescued(self):
        project = (
            "\n【案件】基幹システム開発\n"
            "【内容】API開発\n"
            "【要件】Java経験\n"
            "【募集】1名\n"
        )
        self.assert_manual(_record(body=_profile() + project))

    def test_incomplete_profile_is_not_rescued(self):
        self.assert_manual(_record(body="氏名: A.B\n所属: 弊社社員\n最寄駅: 新宿駅\n"))

    def test_manual_exclusion_without_profile_is_unchanged(self):
        self.assert_manual(_record(body="今月の営業情報です。\n"))


if __name__ == "__main__":
    unittest.main()

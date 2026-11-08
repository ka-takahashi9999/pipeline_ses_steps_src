import sys
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parents[1]
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

import remove_individual_email as target


def _record(subject, body, from_field="sender@example.com", attachments=None, html_links=None):
    return {
        "message_id": "fixture",
        "from": from_field,
        "subject": subject,
        "body_text": body,
        "attachments": [] if attachments is None else attachments,
        "html_links": [] if html_links is None else html_links,
    }


def _boxed_project_block(index, complete=True):
    lines = [
        "【案件名】",
        "案件{}".format(index),
        "【案件概要】",
        "業務システム開発{}".format(index),
        "【必須スキル】",
        "Skill{}".format(index),
        "【期間】",
        "12月から",
        "【勤務場所】",
        "東京都内",
    ]
    if complete:
        lines.extend(("【単価】", "80万円"))
    return "\n".join(lines)


def _boxed_projects(count, incomplete_index=None):
    return "\n\n".join(
        _boxed_project_block(index, complete=index != incomplete_index)
        for index in range(1, count + 1)
    )


def _resource_profile(name):
    return "\n".join((
        "氏名：{}".format(name),
        "所属：弊社社員",
        "最寄駅：東京駅",
        "単価：70万円",
    ))


class CountedBoxedProjectBlocksTest(unittest.TestCase):
    def test_matching_six_project_count_and_complete_blocks_are_excluded(self):
        for subject in ("6案件／最大150万円", "6案件 PMO・開発支援"):
            with self.subTest(subject=subject):
                self.assertEqual(
                    target.detect_template_exclusion(
                        _record(subject, _boxed_projects(6))
                    ),
                    ("multi_item_mail", "counted_two_project_blocks_v1"),
                )

    def test_counted_subject_with_only_one_complete_block_is_kept(self):
        self.assertIsNone(
            target.detect_template_exclusion(
                _record("3案件／ご案内", _boxed_projects(1))
            )
        )

    def test_incomplete_block_is_kept(self):
        self.assertIsNone(
            target.detect_template_exclusion(
                _record("3案件／ご案内", _boxed_projects(3, incomplete_index=3))
            )
        )

    def test_repeated_boxed_project_names_without_count_are_kept(self):
        self.assertIsNone(
            target.detect_template_exclusion(
                _record("案件情報", _boxed_projects(3))
            )
        )

    def test_single_project_description_repetition_is_kept(self):
        body = _boxed_project_block(1) + "\n案件名を資料と見積書へ記載してください。"
        self.assertIsNone(
            target.detect_template_exclusion(_record("単一案件", body))
        )

    def test_mismatched_count_and_complete_blocks_are_kept(self):
        self.assertIsNone(
            target.detect_template_exclusion(
                _record("3案件／ご案内", _boxed_projects(2))
            )
        )

    def test_conflicting_subject_counts_are_kept(self):
        self.assertIsNone(
            target.detect_template_exclusion(
                _record("3案件／別紙では6案件", _boxed_projects(3))
            )
        )

    def test_subject_n_items_without_n_projects_is_kept(self):
        self.assertIsNone(
            target.detect_template_exclusion(
                _record("3件／ご案内", _boxed_projects(3))
            )
        )

    def test_body_only_n_projects_is_kept(self):
        self.assertIsNone(
            target.detect_template_exclusion(
                _record(
                    "案件のご案内",
                    "3案件あります\n" + _boxed_projects(3),
                )
            )
        )

    def test_duplicate_project_names_are_kept(self):
        duplicate_names = "\n\n".join((
            _boxed_project_block(1),
            _boxed_project_block(2).replace("案件2", "案件1"),
            _boxed_project_block(3),
        ))
        self.assertIsNone(
            target.detect_template_exclusion(
                _record("3案件／ご案内", duplicate_names)
            )
        )

    def test_list_portal_only_is_not_treated_as_counted_blocks(self):
        record = _record(
            "3案件／一覧",
            "案件一覧\nhttps://portal.example.com/projects",
        )
        self.assertEqual(
            target.detect_template_exclusion(record),
            ("list_or_portal_notice", "external_list_portal_without_individual_detail_v1"),
        )

    def test_normal_single_project_is_kept(self):
        self.assertIsNone(
            target.detect_template_exclusion(
                _record("Java開発案件", _boxed_projects(1))
            )
        )

    def test_existing_multi_project_and_resource_rules_are_unchanged(self):
        existing_projects = "\n".join(
            "【案件】案件{0}\n【内容】開発{0}\n【要件】Skill{0}\n【募集】1名".format(index)
            for index in range(1, 6)
        )
        self.assertEqual(
            target.detect_template_exclusion(_record("複数案件", existing_projects)),
            ("multi_item_mail", "repeated_complete_project_blocks_v1"),
        )

        resources = _resource_profile("A.B") + "\n" + _resource_profile("C.D")
        self.assertEqual(
            target.detect_template_exclusion(_record("要員2名のご紹介", resources)),
            ("multi_item_mail", "repeated_resource_profiles_v1"),
        )

    def test_single_complete_profile_manual_rescue_is_unchanged(self):
        record = _record("要員のご紹介", _resource_profile("A.B"))
        self.assertIsNone(
            target.determine_exclusion_reason(
                record, {"sender@example.com"}, []
            )
        )

    def test_existing_service_notification_rule_is_unchanged(self):
        record = _record(
            "【チョータツブースト】 新着案件・人材(11月06日)",
            "チョータツブーストの新着案件・人材情報",
            from_field="noreply@driven-x.com",
            html_links=[
                {"href": "https://cho-tatsu.com/boost/talents"},
                {"href": "https://cho-tatsu.com/boost/projects"},
            ],
        )
        self.assertEqual(
            target.detect_template_exclusion(record),
            ("service_notification", "chotatsu_boost_digest_v1"),
        )


if __name__ == "__main__":
    unittest.main()

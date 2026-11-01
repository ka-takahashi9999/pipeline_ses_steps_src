#!/usr/bin/env python3
"""CHECK-05: 本文人物とskillsheet人物の明確な不一致だけを拒否するtest。"""

import unittest
from unittest.mock import patch

import fetch_skillsheets_text as target


MATCHING_BODY = """【氏名】IK 24歳 男性
【最寄】千葉寺駅
【所属】弊社社員
"""

MISMATCHED_SHEET = """職務経歴書
RY | 男性・39歳 | Ruby on Rails開発 約7年 | 最寄駅:溝の口駅
Ruby / Rails / RSpec / Docker / AWS
"""

MATCHING_SHEET = """職務経歴書
氏名: IK | 年齢:24歳 | 最寄駅:千葉寺駅
Java / SQL / T-SQL / Windows
"""


class IdentityMismatchVetoTest(unittest.TestCase):
    def test_multiple_identity_conflicts_are_rejected(self):
        evidence = target.detect_clear_identity_mismatch(
            MATCHING_BODY, MISMATCHED_SHEET
        )
        self.assertEqual(
            evidence["mismatched_attributes"],
            ["age", "initial", "station"],
        )

    def test_matching_identity_is_preserved(self):
        self.assertEqual(
            target.detect_clear_identity_mismatch(MATCHING_BODY, MATCHING_SHEET),
            {},
        )

    def test_age_only_or_small_age_difference_is_not_rejected(self):
        age_only = """職務経歴書
氏名: IK | 年齢:35歳 | 最寄駅:千葉寺駅
Java / SQL / T-SQL / Windows
"""
        small_difference = """職務経歴書
氏名: XY | 年齢:25歳 | 最寄駅:溝の口駅
Java / SQL / T-SQL / Windows
"""
        self.assertEqual(
            target.detect_clear_identity_mismatch(MATCHING_BODY, age_only),
            {},
        )
        self.assertEqual(
            target.detect_clear_identity_mismatch(MATCHING_BODY, small_difference),
            {},
        )

    def test_fetch_continues_to_matching_candidate_after_clear_mismatch(self):
        attachments = [
            {"filename": "first.xlsx"},
            {"filename": "second.xlsx"},
        ]
        mail_record = {"body_text": MATCHING_BODY, "attachments": attachments}
        cleanup_record = {"body_text": MATCHING_BODY}
        with patch.object(target, "is_eligible_attachment", return_value=True), patch.object(
            target,
            "extract_from_attachment",
            side_effect=[MISMATCHED_SHEET, MATCHING_SHEET],
        ), patch.object(target, "build_url_candidates", return_value=([], [], [])):
            output = target.fetch_skillsheet(
                "fixture", mail_record, cleanup_record
            )
        self.assertTrue(output["success"])
        self.assertEqual(output["source"], "attachment")
        self.assertEqual(output["skillsheet"], MATCHING_SHEET)

    def test_only_mismatched_candidate_fails_closed(self):
        mail_record = {
            "body_text": MATCHING_BODY,
            "attachments": [{"filename": "wrong.xlsx"}],
        }
        cleanup_record = {"body_text": MATCHING_BODY}
        with patch.object(target, "is_eligible_attachment", return_value=True), patch.object(
            target, "extract_from_attachment", return_value=MISMATCHED_SHEET
        ), patch.object(target, "build_url_candidates", return_value=([], [], [])):
            output = target.fetch_skillsheet(
                "fixture", mail_record, cleanup_record
            )
        self.assertFalse(output["success"])
        self.assertIn("[identity_mismatch]", output["failure_reason"])

    def test_url_flow_continues_to_identity_matching_candidate(self):
        first_url = "https://example.test/wrong.xlsx"
        second_url = "https://example.test/correct.xlsx"
        mail_record = {"body_text": MATCHING_BODY, "attachments": []}
        cleanup_record = {"body_text": MATCHING_BODY}
        download_results = [
            target.DownloadResult(b"wrong", first_url, "application/octet-stream", "wrong.xlsx"),
            target.DownloadResult(b"correct", second_url, "application/octet-stream", "correct.xlsx"),
        ]
        with patch.object(
            target,
            "build_url_candidates",
            return_value=(
                [(first_url, "extension_file"), (second_url, "extension_file")],
                [],
                [first_url, second_url],
            ),
        ), patch.object(
            target, "download_other_url", side_effect=download_results
        ), patch.object(
            target,
            "extract_text_from_bytes_with_timeout",
            side_effect=[MISMATCHED_SHEET, MATCHING_SHEET],
        ):
            output = target.fetch_skillsheet(
                "fixture-url", mail_record, cleanup_record
            )
        self.assertTrue(output["success"])
        self.assertEqual(output["urls"], second_url)
        self.assertEqual(output["skillsheet"], MATCHING_SHEET)


if __name__ == "__main__":
    unittest.main()

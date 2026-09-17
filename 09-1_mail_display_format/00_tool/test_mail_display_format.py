"""09-1 previous candidate営業表示のfocused test。"""

import json
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import mail_display_format as target
from mail_display_format import build_output_filename, format_pair


def pair(previous=False, previous_date="", cache_hit=False):
    return {
        "project_info": {"message_id": "project-1", "required_skills": [], "optional_skills": []},
        "resource_info": {"message_id": "resource-1"},
        "duplicate_proposal_check": cache_hit,
        "previous_candidate": previous,
        "previous_candidate_date": previous_date,
    }


MAIL_MASTER = {
    "project-1": {"date": "date", "subject": "project", "from": "p@example.com", "body_text": "body"},
    "resource-1": {"date": "date", "subject": "resource", "from": "r@example.com", "body_text": "body"},
}


class MailDisplayFormatPreviousCandidateTest(unittest.TestCase):
    def test_cache_marker_alone_does_not_show_previous_candidate_badge(self):
        record = pair(cache_hit=True)
        text = format_pair(record, MAIL_MASTER)
        self.assertNotIn("前回も候補", text)
        self.assertNotIn("前回提案済", text)
        self.assertNotIn("前回出力済", build_output_filename("100percent", 1, record))

    def test_previous_candidate_shows_comparison_date(self):
        text = format_pair(pair(previous=True, previous_date="20260819"), MAIL_MASTER)
        self.assertIn("[前回も候補: 20260819]", text)
        self.assertNotIn("前回提案済", text)

    def test_empty_subject_target_pair_and_following_pair_are_output(self):
        project_id = "1a0a819d256a4e65"
        resource_id = "1a0a94f7d0064463"
        later_id = "resource-after"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_dir = root / "input"
            previous_dir = root / "previous"
            input_dir.mkdir()
            previous_dir.mkdir()
            pairs = [
                {
                    "project_info": {"message_id": project_id},
                    "resource_info": {"message_id": resource_id},
                },
                {
                    "project_info": {"message_id": project_id},
                    "resource_info": {"message_id": later_id},
                },
            ]
            mails = [
                {"message_id": project_id, "from": "p@example.com", "subject": "案件"},
                {"message_id": resource_id, "from": "r@example.com", "subject": ""},
                {"message_id": later_id, "from": "later@example.com", "subject": "要員"},
            ]
            previous = {
                "project_message_id": project_id,
                "resource_message_id": resource_id,
                "project_from": "p@example.com",
                "project_subject": "案件",
                "resource_from": "r@example.com",
                "resource_subject": "",
            }
            for path, rows in (
                (input_dir / "pairs.jsonl", pairs),
                (root / "mail_master.jsonl", mails),
                (previous_dir / "sales_proposal_candidates_20260915.jsonl", [previous]),
            ):
                path.write_text(
                    "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
                    encoding="utf-8",
                )

            class CaptureLogger:
                def __init__(self):
                    self.warnings = []
                    self.infos = []
                def info(self, message):
                    self.infos.append(message)
                def warn(self, message):
                    self.warnings.append(message)
                def ok(self, _message):
                    pass
                def error(self, _message):
                    pass

            logger = CaptureLogger()
            with patch.object(target, "STEP_DIR", root), patch.object(target, "INPUT_DIR", input_dir), \
                 patch.object(target, "INPUT_MAIL_MASTER", root / "mail_master.jsonl"), \
                 patch.object(target, "FINAL_CANDIDATE_DIR", previous_dir), \
                 patch.object(target, "INPUT_FILES", [("pairs.jsonl", "60to79percent")]), \
                 patch.object(target, "get_logger", return_value=logger), \
                 patch.object(sys, "argv", ["mail_display_format.py", "--target-date", "20260916"]):
                self.assertIsNone(target.main())

            output_dir = root / "01_result/mail_display_format_20260916"
            first = (output_dir / "mail_display_format_60to79percent_pair_0001.txt").read_text(encoding="utf-8")
            second = (output_dir / "mail_display_format_60to79percent_pair_0002.txt").read_text(encoding="utf-8")
            self.assertIn("[前回も候補: 20260915]", first)
            self.assertIn("■■■要員メール\n受信日付:\nメールタイトル:\nFrom:r@example.com", first)
            self.assertIn("メールタイトル:要員", second)
            self.assertEqual(mails[1]["subject"], "")
            self.assertEqual(previous["resource_subject"], "")
            self.assertEqual(len(logger.warnings), 2)
            self.assertTrue(all("side=resource" in warning and resource_id in warning for warning in logger.warnings))
            self.assertTrue(any("fallback件数=2 project=0 resource=2" in message for message in logger.infos))

            confirm_path = Path(__file__).resolve().parents[1] / "02_confirm/confirm_mail_display_format.py"
            spec = importlib.util.spec_from_file_location("isolated_09_1_confirm", confirm_path)
            confirm = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(confirm)
            result_path = root / "confirm_result.txt"
            with patch.object(confirm, "INPUT_DIR", input_dir), \
                 patch.object(confirm, "OUTPUT_RESULT_DIR", root / "01_result"), \
                 patch.object(confirm, "INPUT_FILES", ["pairs.jsonl"]), \
                 patch.object(confirm, "CONFIRM_RESULT", result_path), \
                 patch.object(confirm, "get_logger", return_value=logger), \
                 patch.object(sys, "argv", ["confirm_mail_display_format.py", "--target-date", "20260916"]):
                self.assertIsNone(confirm.main())
            self.assertIn("【結果】OK", result_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()

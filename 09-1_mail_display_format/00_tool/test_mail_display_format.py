"""09-1 previous candidate営業表示のfocused test。"""

import unittest

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


if __name__ == "__main__":
    unittest.main()

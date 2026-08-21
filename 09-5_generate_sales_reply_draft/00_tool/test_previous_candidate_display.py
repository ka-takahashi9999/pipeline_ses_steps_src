"""09-5営業向けpreview filenameのlegacy suffix非表示test。"""

import unittest

from generate_sales_reply_draft import build_preview_file_name


class PreviousCandidateDisplayTest(unittest.TestCase):
    def test_success_cache_marker_does_not_add_legacy_suffix(self):
        record = {
            "pair_file_name": "mail_display_format_100percent_pair_0001_前回出力済.txt",
            "duplicate_proposal_check": True,
        }
        self.assertEqual(
            "mail_display_format_100percent_pair_0001_reply_to_project.txt",
            build_preview_file_name(record, "reply_to_project"),
        )


if __name__ == "__main__":
    unittest.main()

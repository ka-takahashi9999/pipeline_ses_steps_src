#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cleanupで除去されたraw-only skillsheet URL救済のfocused test。"""

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


TOOL_DIR = Path(__file__).resolve().parent
MODULE_PATH = TOOL_DIR / "fetch_skillsheets_text.py"
SPEC = importlib.util.spec_from_file_location(
    "fetch_skillsheets_text_raw_only_under_test", MODULE_PATH
)
assert SPEC is not None and SPEC.loader is not None
fetch = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = fetch
SPEC.loader.exec_module(fetch)

NORMALIZE_PATH = (
    TOOL_DIR.parents[1]
    / "04-2_normalize_skillsheets_text"
    / "00_tool"
    / "normalize_skillsheets_text.py"
)
NORMALIZE_SPEC = importlib.util.spec_from_file_location(
    "normalize_skillsheets_text_raw_only_under_test", NORMALIZE_PATH
)
assert NORMALIZE_SPEC is not None and NORMALIZE_SPEC.loader is not None
normalize = importlib.util.module_from_spec(NORMALIZE_SPEC)
sys.modules[NORMALIZE_SPEC.name] = normalize
NORMALIZE_SPEC.loader.exec_module(normalize)

PERSONAL_URL = "https://docs.google.com/document/d/personal-resume/edit"
LIST_URL = "https://docs.google.com/spreadsheets/d/resource-list/edit"
COMPANY_URL = "https://example.co.jp/company.pdf"
LOW_CONFIDENCE_URL = "https://example.co.jp/profile"
UNSUBSCRIBE_URL = "https://example.co.jp/unsubscribe/resume.pdf"
VALID_TEXT = "職務経歴書\n氏名: NK\n年齢: 28歳\n最寄駅: 上新庄駅\nTypeScriptとAWSの開発経験"


def _candidates(raw_body: str, cleaned_body: str = ""):
    return fetch.build_url_candidates(
        {"body_text": raw_body, "attachments": [], "html_links": []},
        {"body_text": cleaned_body},
    )[0]


class RawOnlySkillsheetUrlRoutingTest(unittest.TestCase):
    def test_rescues_high_confidence_raw_only_url_with_explicit_context(self):
        candidates = _candidates(f"【スキルシートURL】\nURL:{PERSONAL_URL}")
        self.assertEqual(candidates, [(PERSONAL_URL, "google_drive")])

    def test_resume_context_is_supported(self):
        candidates = _candidates(f"Resume\n{PERSONAL_URL}")
        self.assertEqual(candidates, [(PERSONAL_URL, "google_drive")])

    def test_does_not_rescue_negative_or_low_confidence_urls(self):
        cases = (
            f"会社案内 スキルシート\n{COMPANY_URL}",
            f"要員一覧 スキルシート\n{LIST_URL}",
            f"案件一覧 スキルシート\n{LIST_URL}",
            f"人材一覧 スキルシート\n{LIST_URL}",
            f"広告 スキルシート\n{LIST_URL}",
            f"portal スキルシート\n{LIST_URL}",
            f"スキルシート 配信停止\n{LIST_URL}",
            f"スキルシート\n{UNSUBSCRIBE_URL}",
            PERSONAL_URL,
            f"スキルシート\n{LOW_CONFIDENCE_URL}",
        )
        for raw_body in cases:
            with self.subTest(raw_body=raw_body):
                self.assertEqual(_candidates(raw_body), [])

    def test_cleanup_visible_url_keeps_existing_route(self):
        body = f"スキルシート\n{PERSONAL_URL}"
        self.assertEqual(
            _candidates(body, body),
            [(PERSONAL_URL, "google_drive")],
        )

    def test_rescued_high_confidence_url_prevents_contextual_fallback(self):
        tracking_url = "https://tracking.example.co.jp/resume"
        mail = {
            "body_text": f"スキルシート\n{PERSONAL_URL}",
            "attachments": [],
            "html_links": [
                {"text": "個人経歴", "href": tracking_url, "source": "text/html"}
            ],
        }
        cleanup = {"body_text": "スキルシート:個人経歴"}
        candidates = fetch.build_url_candidates(mail, cleanup)[0]
        self.assertEqual(candidates, [(PERSONAL_URL, "google_drive")])

    def test_identity_mismatch_still_rejects_rescued_candidate(self):
        result = fetch.DownloadResult(
            content=b"document",
            final_url=PERSONAL_URL,
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            filename="resume.docx",
        )
        mail = {
            "body_text": f"スキルシートURL\n{PERSONAL_URL}",
            "attachments": [],
            "html_links": [],
        }
        with (
            patch.object(fetch, "download_google_drive", return_value=result),
            patch.object(fetch, "extract_text_from_bytes_with_timeout", return_value=VALID_TEXT),
            patch.object(
                fetch,
                "detect_clear_identity_mismatch",
                return_value={"mismatched_attributes": ["age", "initial"]},
            ),
        ):
            record = fetch.fetch_skillsheet("identity-mismatch", mail, {"body_text": ""})
        self.assertFalse(record["success"])
        self.assertIn("[identity_mismatch]", record["failure_reason"])

    def test_quality_ng_still_rejects_rescued_candidate(self):
        result = fetch.DownloadResult(
            content=b"document",
            final_url=PERSONAL_URL,
            content_type="application/pdf",
            filename="resume.pdf",
        )
        mail = {
            "body_text": f"職務経歴書\n{PERSONAL_URL}",
            "attachments": [],
            "html_links": [],
        }
        with (
            patch.object(fetch, "download_google_drive", return_value=result),
            patch.object(fetch, "extract_text_from_bytes_with_timeout", return_value="表題だけ"),
            patch.object(fetch, "validate_skillsheet_text", return_value="表題だけの本文のため不採用"),
        ):
            record = fetch.fetch_skillsheet("quality-ng", mail, {"body_text": ""})
        self.assertFalse(record["success"])
        self.assertIn("[extract_unsupported_file]", record["failure_reason"])

    def test_multi_person_veto_remains_after_rescue(self):
        fetched = {
            "message_id": "multi-person",
            "success": True,
            "source": "google_drive",
            "urls": PERSONAL_URL,
            "skillsheet": (
                "営業中技術者\n"
                "氏名:AA\n年齢:30\n最寄駅:A駅\n単価:70万\n"
                "氏名:BB\n年齢:31\n最寄駅:B駅\n単価:75万\n"
            ),
        }
        normalized = normalize.build_record(fetched)
        self.assertFalse(normalized["success"])
        self.assertIn("multi_person_skillsheet", normalized["cleanup_flags"])


if __name__ == "__main__":
    unittest.main()

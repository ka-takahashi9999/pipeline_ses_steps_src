#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""html_anchor_adjacent_skillsheet_context_not_routed のfocused test。"""

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name("fetch_skillsheets_text.py")
SPEC = importlib.util.spec_from_file_location(
    "fetch_skillsheets_text_adjacent_anchor_under_test", MODULE_PATH
)
assert SPEC is not None and SPEC.loader is not None
fetch = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fetch)

TRACKING_URL = "https://a16.hm-f.jp/cc.php?t=resume"
GOOGLE_SHEET = "https://docs.google.com/spreadsheets/d/existing-sheet/edit"
LINE_URL = "https://line.me/ti/p/example"
COMPANY_URL = "https://example.co.jp/"
VALID_TEXT = "職務経歴書\nMK 29歳 相模原駅\nPythonとAWSによるシステム開発経験があります。"


def _mail(anchor_text="MK【29歳・男性】", href=TRACKING_URL, attachments=None):
    return {
        "body_text": "",
        "attachments": attachments or [],
        "html_links": [
            {"text": anchor_text, "href": href, "source": "text/html"}
        ],
    }


def _cleanup(anchor_text="MK【29歳・男性】", label="スキルシート"):
    return {"body_text": f"{label}：{anchor_text}\n名前：MK【29歳・男性】"}


def test_adjacent_explicit_context_routes_non_generic_anchor():
    candidates, html_urls, _body_urls = fetch.build_url_candidates(
        _mail(), _cleanup()
    )

    assert candidates[0] == (TRACKING_URL, "direct_adopt_ng")
    assert html_urls == [TRACKING_URL]


def test_contextual_candidate_uses_existing_download_and_quality_flow():
    result = fetch.DownloadResult(
        content=b"valid-xlsx",
        final_url=GOOGLE_SHEET,
        content_type=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
        filename="MK29.xlsx",
    )
    with (
        patch.object(fetch, "download_other_url", return_value=result),
        patch.object(fetch, "extract_text_from_bytes_with_timeout", return_value=VALID_TEXT),
        patch.object(fetch, "detect_clear_identity_mismatch", return_value={}),
    ):
        record = fetch.fetch_skillsheet("target", _mail(), _cleanup())

    assert record["success"] is True
    assert record["source"] == "other"
    assert record["urls"] == TRACKING_URL
    assert record["skillsheet"] == VALID_TEXT


def test_line_url_is_not_added_even_with_explicit_context():
    candidates, _html_urls, _body_urls = fetch.build_url_candidates(
        _mail(href=LINE_URL), _cleanup()
    )
    assert candidates == []


def test_generic_company_anchor_is_not_added():
    candidates, _html_urls, _body_urls = fetch.build_url_candidates(
        _mail(anchor_text="会社サイト", href=COMPANY_URL),
        _cleanup(anchor_text="会社サイト"),
    )
    assert candidates == []


def test_signature_contact_anchor_is_not_added():
    candidates, _html_urls, _body_urls = fetch.build_url_candidates(
        _mail(anchor_text="お問い合わせ", href="https://example.co.jp/contact"),
        _cleanup(anchor_text="お問い合わせ"),
    )
    assert candidates == []


def test_tracking_url_without_explicit_context_is_not_added():
    candidates, _html_urls, _body_urls = fetch.build_url_candidates(
        _mail(), _cleanup(label="プロフィール")
    )
    assert candidates == []


def test_generic_anchor_is_not_added_with_explicit_context():
    candidates, _html_urls, _body_urls = fetch.build_url_candidates(
        _mail(anchor_text="こちら"), _cleanup(anchor_text="こちら")
    )
    assert candidates == []


def test_high_confidence_attachment_prevents_fallback_candidate():
    attachment = {
        "filename": "MK_職務経歴書.xlsx",
        "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "data": "AA==",
    }
    candidates, html_urls, _body_urls = fetch.build_url_candidates(
        _mail(attachments=[attachment]), _cleanup()
    )
    assert TRACKING_URL not in [url for url, _category in candidates]
    assert html_urls == []


def test_high_confidence_url_prevents_fallback_and_keeps_priority():
    mail = _mail()
    mail["body_text"] = GOOGLE_SHEET
    cleanup = _cleanup()
    cleanup["body_text"] += f"\n{GOOGLE_SHEET}"
    candidates, html_urls, _body_urls = fetch.build_url_candidates(mail, cleanup)

    assert candidates == [(GOOGLE_SHEET, "google_drive")]
    assert html_urls == []


class AdjacentSkillsheetAnchorRoutingTest(unittest.TestCase):
    def test_true_positive(self):
        test_adjacent_explicit_context_routes_non_generic_anchor()

    def test_existing_validation_flow(self):
        test_contextual_candidate_uses_existing_download_and_quality_flow()

    def test_line_negative(self):
        test_line_url_is_not_added_even_with_explicit_context()

    def test_company_negative(self):
        test_generic_company_anchor_is_not_added()

    def test_signature_negative(self):
        test_signature_contact_anchor_is_not_added()

    def test_tracking_without_context_negative(self):
        test_tracking_url_without_explicit_context_is_not_added()

    def test_generic_anchor_negative(self):
        test_generic_anchor_is_not_added_with_explicit_context()

    def test_existing_attachment_priority(self):
        test_high_confidence_attachment_prevents_fallback_candidate()

    def test_existing_url_priority(self):
        test_high_confidence_url_prevents_fallback_and_keeps_priority()


if __name__ == "__main__":
    unittest.main()

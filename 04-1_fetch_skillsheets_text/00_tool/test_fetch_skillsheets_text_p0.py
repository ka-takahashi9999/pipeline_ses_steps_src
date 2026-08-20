#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib.util
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name("fetch_skillsheets_text.py")
SPEC = importlib.util.spec_from_file_location("fetch_skillsheets_text_p0_under_test", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
fetch = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fetch)

PERSONAL_DRIVE = "https://drive.google.com/file/d/personal12345678901234567890/view"
PERSONAL_SHEET = "https://docs.google.com/spreadsheets/d/personal123456789012345/edit"
AUX_SHEET = "https://docs.google.com/spreadsheets/d/auxiliary123456789012345/edit"
OTHER_FILE = "https://example.com/second-resume.pdf"
VALID_TEXT = "職務経歴書\nWebディレクターとして要件定義、設計、進行管理を担当しました。"


def download_result(url, filename="resume.pdf"):
    return fetch.DownloadResult(
        content=b"valid-file",
        final_url=url,
        content_type="application/pdf",
        filename=filename,
    )


def test_personal_drive_failure_does_not_fallback_to_removed_auxiliary_url():
    mail = {
        "body_text": f"スキルシート\n{PERSONAL_DRIVE}\n弊社注力人材\n{AUX_SHEET}",
        "attachments": [],
        "html_links": [],
    }
    cleaned = {"body_text": f"スキルシート\n{PERSONAL_DRIVE}"}
    with patch.object(fetch, "download_google_drive", side_effect=ValueError("抽出テキストが空")):
        result = fetch.fetch_skillsheet("m1", mail, cleaned)

    assert result["success"] is False
    assert result["skillsheet"] is None
    assert result["source"] is None
    assert result["tried_urls"] == [PERSONAL_DRIVE]
    assert AUX_SHEET not in result["tried_urls"]


def test_personal_google_spreadsheet_is_eligible_and_succeeds():
    mail = {"body_text": PERSONAL_SHEET, "attachments": [], "html_links": []}
    with (
        patch.object(fetch, "download_google_drive", return_value=download_result(PERSONAL_SHEET, "resume.xlsx")),
        patch.object(fetch, "extract_text_from_bytes_with_timeout", return_value=VALID_TEXT),
    ):
        result = fetch.fetch_skillsheet("m2", mail, {"body_text": PERSONAL_SHEET})
    assert result["success"] is True
    assert result["urls"] == PERSONAL_SHEET


def test_positive_html_anchor_is_eligible():
    mail = {"body_text": "", "html_links": [{"text": "スキルシートはこちら", "href": PERSONAL_DRIVE, "source": "text/html"}]}
    candidates, html_urls, body_urls = fetch.build_url_candidates(mail, {"body_text": ""})
    assert [url for url, _category in candidates] == [PERSONAL_DRIVE]
    assert html_urls == [PERSONAL_DRIVE]
    assert body_urls == []


def test_auxiliary_html_anchor_is_not_eligible():
    mail = {"body_text": "", "html_links": [{"text": "弊社注力人材はこちら", "href": AUX_SHEET, "source": "text/html"}]}
    candidates, _html_urls, _body_urls = fetch.build_url_candidates(mail, {"body_text": ""})
    assert candidates == []


def test_ambiguous_html_anchor_is_not_eligible():
    mail = {"body_text": "", "html_links": [{"text": "こちら", "href": PERSONAL_DRIVE, "source": "text/html"}]}
    candidates, _html_urls, _body_urls = fetch.build_url_candidates(mail, {"body_text": ""})
    assert candidates == []


def test_negative_wins_when_html_anchor_is_positive_and_negative():
    mail = {"body_text": "", "html_links": [{"text": "注力人材のスキルシートはこちら", "href": AUX_SHEET, "source": "text/html"}]}
    candidates, _html_urls, _body_urls = fetch.build_url_candidates(mail, {"body_text": ""})
    assert candidates == []


def test_removed_body_href_is_not_revived_by_positive_html_anchor():
    mail = {
        "body_text": f"弊社注力人材\n{AUX_SHEET}",
        "html_links": [{"text": "スキルシートはこちら", "href": AUX_SHEET, "source": "text/html"}],
    }
    candidates, _html_urls, _body_urls = fetch.build_url_candidates(mail, {"body_text": ""})
    assert candidates == []


def test_company_brochure_attachment_does_not_block_personal_url():
    mail = {
        "body_text": PERSONAL_DRIVE,
        "attachments": [{"filename": "会社案内.pdf", "mime_type": "application/pdf", "data": "AA=="}],
        "html_links": [],
    }
    with (
        patch.object(fetch, "extract_from_attachment") as attachment_extract,
        patch.object(fetch, "download_google_drive", return_value=download_result(PERSONAL_DRIVE)),
        patch.object(fetch, "extract_text_from_bytes_with_timeout", return_value=VALID_TEXT),
    ):
        result = fetch.fetch_skillsheet("m3", mail, {"body_text": PERSONAL_DRIVE})
    attachment_extract.assert_not_called()
    assert result["success"] is True
    assert result["urls"] == PERSONAL_DRIVE


def test_valid_attachment_wins_and_auxiliary_url_is_not_adopted():
    mail = {
        "body_text": f"弊社注力人材\n{AUX_SHEET}",
        "attachments": [{"filename": "IH_スキルシート.pdf", "mime_type": "application/pdf", "data": "AA=="}],
        "html_links": [],
    }
    with (
        patch.object(fetch, "extract_from_attachment", return_value=VALID_TEXT),
        patch.object(fetch, "download_google_drive") as downloader,
    ):
        result = fetch.fetch_skillsheet("m4", mail, {"body_text": ""})
    downloader.assert_not_called()
    assert result["success"] is True
    assert result["source"] == "attachment"
    assert result["urls"] is False


def test_google_spreadsheet_provider_alone_is_not_negative():
    candidates, _html_urls, _body_urls = fetch.build_url_candidates(
        {"body_text": PERSONAL_SHEET, "html_links": []},
        {"body_text": PERSONAL_SHEET},
    )
    assert [url for url, _category in candidates] == [PERSONAL_SHEET]


def test_second_url_is_not_excluded_just_for_position():
    body = f"{PERSONAL_DRIVE}\n{OTHER_FILE}"
    candidates, _html_urls, _body_urls = fetch.build_url_candidates(
        {"body_text": body, "html_links": []},
        {"body_text": body},
    )
    assert {url for url, _category in candidates} == {PERSONAL_DRIVE, OTHER_FILE}


def test_body_and_html_duplicate_is_tried_once():
    mail = {
        "body_text": PERSONAL_DRIVE,
        "attachments": [],
        "html_links": [{"text": "Skill Sheet", "href": PERSONAL_DRIVE, "source": "text/html"}],
    }
    with patch.object(fetch, "download_google_drive", side_effect=ValueError("取得失敗")) as downloader:
        result = fetch.fetch_skillsheet("m5", mail, {"body_text": PERSONAL_DRIVE})
    assert downloader.call_count == 1
    assert result["tried_urls"] == [PERSONAL_DRIVE]


def test_all_eligible_urls_failure_uses_existing_fail_closed_schema():
    body = f"{PERSONAL_DRIVE}\n{OTHER_FILE}"
    mail = {"body_text": body, "attachments": [], "html_links": []}
    with (
        patch.object(fetch, "download_google_drive", side_effect=ValueError("失敗1")),
        patch.object(fetch, "download_other_url", side_effect=ValueError("失敗2")),
    ):
        result = fetch.fetch_skillsheet("m6", mail, {"body_text": body})
    assert result["success"] is False
    assert result["skillsheet"] is None
    assert result["source"] is None
    assert set(result["tried_urls"]) == {PERSONAL_DRIVE, OTHER_FILE}


def test_filtered_auxiliary_url_is_not_in_tried_urls():
    mail = {
        "body_text": f"弊社注力人材\n{AUX_SHEET}",
        "attachments": [],
        "html_links": [{"text": "人材一覧", "href": AUX_SHEET, "source": "text/html"}],
    }
    result = fetch.fetch_skillsheet("m7", mail, {"body_text": ""})
    assert result["success"] is False
    assert "tried_urls" not in result
    assert result["urls"] is False


def test_no_sender_or_message_id_is_used_for_eligibility():
    for sender, message_id in (("a@example.com", "one"), ("b@another.example", "two")):
        mail = {"from": sender, "message_id": message_id, "body_text": PERSONAL_DRIVE, "html_links": []}
        candidates, _html_urls, _body_urls = fetch.build_url_candidates(mail, {"body_text": PERSONAL_DRIVE})
        assert [url for url, _category in candidates] == [PERSONAL_DRIVE]


if __name__ == "__main__":
    tests = sorted(
        (name, value)
        for name, value in globals().items()
        if name.startswith("test_") and callable(value)
    )
    for name, test in tests:
        test()
        print(f"PASS {name}")
    print(f"PASS total={len(tests)}")

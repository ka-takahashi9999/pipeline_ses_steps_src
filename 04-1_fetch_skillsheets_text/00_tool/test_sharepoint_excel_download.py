#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SharePoint Excel共有リンクのdownload変換に限定したfocused test。"""

import importlib.util
import json
import unittest
from pathlib import Path
from urllib.parse import parse_qsl, urlparse
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name("fetch_skillsheets_text.py")
SPEC = importlib.util.spec_from_file_location(
    "fetch_skillsheets_text_sharepoint_under_test", MODULE_PATH
)
fetch = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fetch)


class _Response:
    def __init__(self, status_code, content=b"", url="", content_type=""):
        self.status_code = status_code
        self.content = content
        self.url = url
        self.headers = {"Content-Type": content_type}


class _Session:
    def __init__(self):
        self.urls = []

    def get(self, url, **_kwargs):
        self.urls.append(url)
        if "api.onedrive.com" in url:
            return _Response(404, url=url)
        return _Response(
            200,
            content=b"PK\x03\x04xlsx",
            url=url,
            content_type=(
                "application/vnd.openxmlformats-officedocument."
                "spreadsheetml.sheet"
            ),
        )


class SharePointExcelDownloadUrlTest(unittest.TestCase):
    def test_preserves_existing_query_and_adds_download(self):
        source = "https://tenant.sharepoint.com/:x:/s/example/xxx?e=ABC"
        converted = fetch.build_sharepoint_excel_download_url(source)
        query = parse_qsl(urlparse(converted).query, keep_blank_values=True)
        self.assertEqual(query, [("e", "ABC"), ("download", "1")])

    def test_replaces_existing_download_without_duplicates(self):
        source = (
            "https://sharepoint.com/:x:/s/example/xxx"
            "?e=ABC&download=0&keep=&download=2"
        )
        converted = fetch.build_sharepoint_excel_download_url(source)
        query = parse_qsl(urlparse(converted).query, keep_blank_values=True)
        self.assertEqual(query, [("e", "ABC"), ("keep", ""), ("download", "1")])

    def test_non_target_urls_are_unchanged(self):
        urls = (
            "https://onedrive.live.com/:x:/s/example/xxx?e=ABC",
            "https://example.com/:x:/s/example/xxx?e=ABC",
            "https://tenant.sharepoint.com/sites/example/page.aspx?e=ABC",
            "https://tenant.sharepoint.com/:f:/s/example/folder?e=ABC",
            "https://tenant.sharepoint.com/:w:/s/example/word?e=ABC",
            "https://tenant.sharepoint.com/:p:/s/example/powerpoint?e=ABC",
            "https://tenant.sharepoint.com/s/example/file.xlsx?e=ABC",
            "http://tenant.sharepoint.com/:x:/s/example/xxx?e=ABC",
            "https://notsharepoint.com/:x:/s/example/xxx?e=ABC",
        )
        for url in urls:
            with self.subTest(url=url):
                self.assertEqual(fetch.build_sharepoint_excel_download_url(url), url)

    def test_onedrive_fallback_requests_converted_url(self):
        source = "https://tenant.sharepoint.com/:x:/s/example/xxx?e=ABC"
        session = _Session()
        with patch.object(fetch.requests, "Session", return_value=session):
            result = fetch.download_onedrive(source)

        final_query = parse_qsl(
            urlparse(session.urls[-1]).query, keep_blank_values=True
        )
        self.assertEqual(final_query, [("e", "ABC"), ("download", "1")])
        self.assertEqual(result.final_url, session.urls[-1])

    def test_html_download_is_still_rejected_by_fetch_flow(self):
        source = "https://tenant.sharepoint.com/:x:/s/example/xxx?e=ABC"
        html_result = fetch.DownloadResult(
            content=b"<!doctype html><html><body>share page</body></html>",
            final_url=source + "&download=1",
            content_type="text/html",
        )
        mail = {"body_text": source, "attachments": [], "html_links": []}
        with patch.object(fetch, "download_onedrive", return_value=html_result):
            result = fetch.fetch_skillsheet("synthetic", mail, {"body_text": source})

        self.assertFalse(result["success"])
        self.assertIsNone(result["skillsheet"])

    def test_latest_failures_limit_change_to_seven_sharepoint_excel_urls(self):
        with fetch.NO_FETCH_JSONL.open(encoding="utf-8") as file_obj:
            failures = [json.loads(line) for line in file_obj if line.strip()]

        changed = []
        dropbox_urls = []
        for record in failures:
            url = record.get("urls")
            if not isinstance(url, str):
                continue
            converted = fetch.build_sharepoint_excel_download_url(url)
            if converted != url:
                changed.append(record["message_id"])
            host = (urlparse(url).hostname or "").lower()
            if fetch.host_matches(host, "dropbox.com"):
                dropbox_urls.append((url, converted))

        self.assertEqual(len(failures), 123)
        self.assertEqual(len(changed), 7)
        self.assertEqual(len(set(changed)), 7)
        self.assertEqual(len(dropbox_urls), 4)
        self.assertTrue(all(before == after for before, after in dropbox_urls))


if __name__ == "__main__":
    unittest.main()

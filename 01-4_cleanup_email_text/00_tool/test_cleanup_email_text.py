#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("cleanup_email_text.py")
SPEC = importlib.util.spec_from_file_location("cleanup_email_text_under_test", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
cleanup = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cleanup)

RULES = cleanup.load_cleanup_rules(cleanup.CLEANUP_RULES_PATH)
PERSONAL_URL = "https://drive.google.com/file/d/personal12345678901234567890/view"
AUX_URL = "https://docs.google.com/spreadsheets/d/auxiliary123456789012345/edit"


def test_personal_profile_kept_and_auxiliary_section_removed():
    body = "\n".join(
        [
            "IH☆武蔵中原（弊社個人事業主）",
            "年齢：51歳",
            "【スキル概要】Webディレクター / PM",
            "【スキルシートURL】",
            PERSONAL_URL,
            "併せて、下記の弊社注力人材もご確認ください。",
            AUX_URL,
            "末尾の連絡事項",
        ]
    )

    cleaned, _removed = cleanup.cleanup_body(body, RULES)

    assert "IH☆武蔵中原" in cleaned
    assert "年齢：51歳" in cleaned
    assert PERSONAL_URL in cleaned
    assert "弊社注力人材" not in cleaned
    assert AUX_URL not in cleaned
    assert "末尾の連絡事項" in cleaned


def test_list_word_alone_does_not_cleanup():
    body = f"一覧\n{AUX_URL}"
    cleaned, _removed = cleanup.cleanup_body(body, RULES)
    assert cleaned == body


def test_url_on_fourth_physical_line_is_outside_bound():
    body = f"弊社注力人材\n\n\n\n{AUX_URL}"
    cleaned, _removed = cleanup.cleanup_body(body, RULES)
    assert "弊社注力人材" in cleaned
    assert AUX_URL in cleaned


def test_unregistered_nonempty_line_blocks_cleanup():
    body = f"弊社注力人材\n独自のご案内文です\n{AUX_URL}"
    cleaned, _removed = cleanup.cleanup_body(body, RULES)
    assert "弊社注力人材" in cleaned
    assert "独自のご案内文です" in cleaned
    assert AUX_URL in cleaned


def test_primary_profile_field_blocks_cleanup():
    body = f"弊社注力人材\n年齢：35歳\n{AUX_URL}"
    cleaned, _removed = cleanup.cleanup_body(body, RULES)
    assert "弊社注力人材" in cleaned
    assert "年齢：35歳" in cleaned
    assert AUX_URL in cleaned


def test_primary_skillsheet_field_on_url_line_blocks_cleanup():
    body = f"弊社注力人材\n【スキルシートURL】 {PERSONAL_URL}"
    cleaned, _removed = cleanup.cleanup_body(body, RULES)
    assert "弊社注力人材" in cleaned
    assert PERSONAL_URL in cleaned


def test_registered_bridge_line_is_removed_only_through_url():
    body = f"弊社注力人材\nこちらも併せてご確認ください\n{AUX_URL}\n後続本文"
    cleaned, _removed = cleanup.cleanup_body(body, RULES)
    assert "弊社注力人材" not in cleaned
    assert "こちらも併せてご確認ください" not in cleaned
    assert AUX_URL not in cleaned
    assert cleaned == "後続本文"


def test_heading_without_url_is_not_removed():
    body = "弊社注力人材\n後続本文"
    cleaned, _removed = cleanup.cleanup_body(body, RULES)
    assert "弊社注力人材" in cleaned


def test_heading_and_url_on_same_line_remain_supported():
    body = f"営業中人材一覧はこちら {AUX_URL}\n後続本文"
    cleaned, _removed = cleanup.cleanup_body(body, RULES)
    assert AUX_URL not in cleaned
    assert cleaned == "後続本文"


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

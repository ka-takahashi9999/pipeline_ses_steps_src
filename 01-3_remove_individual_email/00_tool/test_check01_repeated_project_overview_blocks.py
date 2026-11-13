#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CHECK-01: 案件紹介文配下の完全な`■概要`block反復test。"""

import importlib.util
import json
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent / "remove_individual_email.py"
SPEC = importlib.util.spec_from_file_location(
    "remove_individual_email_check01_overview_blocks", MODULE_PATH
)
target = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(target)

DECISION = ("multi_item_mail", "repeated_complete_project_blocks_v1")
TARGET_IDS = {
    "s3_c64b14486e0bb8fe80129524e909d1ded0c5999caee15d74040b5614d14eddcb",
    "s3_120264fda61ba3503a94dcf3fca9f43aa0333cc1e7cfd0d02554714d2c50678b",
}
INTRO = "以下、案件をご紹介させていただきます。\n"


def _record(subject, body):
    return {
        "from": "sender@example.com",
        "subject": subject,
        "body_text": body,
        "attachments": [],
        "html_links": [],
    }


def _block(title, marker="〇", rate=True, attributes=("期間", "場所")):
    lines = ["{}{}".format(marker, title), "■概要", "案件概要"]
    if rate:
        lines.append("■単価：80万円")
    values = {
        "期間": "12月〜",
        "場所": "東京",
        "募集": "1名",
        "商流": "貴社まで",
        "契約": "準委任",
        "精算": "140-180h",
        "フェーズ": "要件定義",
        "スキル": "Java",
        "国籍": "日本国籍",
        "稼働": "100%",
    }
    lines.extend("■{}：{}".format(name, values[name]) for name in attributes)
    return "\n".join(lines) + "\n"


def _numbered_block(number, title, nested_positions=False):
    separator = "━" * 14
    body = (
        "{separator}\n{number} {title}\n{separator}\n"
        "■概要\n案件概要\n■単価：80万円\n■期間：12月〜\n■場所：東京\n"
    ).format(separator=separator, number=number, title=title)
    if nested_positions:
        body += "【募集ポジション】\n" + "\n".join(
            "{} ポジション{}".format(label, index)
            for index, label in enumerate("①②③④⑤⑥⑦", 1)
        ) + "\n"
    return body


class RepeatedProjectOverviewBlocksTest(unittest.TestCase):
    def test_circle_headings_with_complete_blocks_are_excluded(self):
        record = _record(
            "複数案件",
            INTRO + _block("案件A") + _block("案件B", marker="○"),
        )
        self.assertEqual(target.detect_template_exclusion(record), DECISION)

    def test_numbered_headings_and_nested_positions_are_excluded_as_two_blocks(self):
        record = _record(
            "複数案件",
            INTRO
            + _numbered_block("①", "案件A", nested_positions=True)
            + _numbered_block("②", "案件B"),
        )
        self.assertEqual(target.detect_template_exclusion(record), DECISION)
        self.assertEqual(
            len(target._complete_distinct_project_overview_block_titles(
                record["body_text"]
            )),
            2,
        )

    def test_incomplete_or_non_repeated_overviews_are_not_excluded(self):
        cases = {
            "single_overview": INTRO + _block("案件A"),
            "missing_rate": (
                INTRO
                + _block("案件A", rate=False)
                + _block("案件B", rate=False)
            ),
            "insufficient_attributes": (
                INTRO
                + _block("案件A", attributes=("期間",))
                + _block("案件B", attributes=("期間",))
            ),
            "same_title": INTRO + _block("同一案件") + _block("同一案件"),
            "missing_intro": _block("案件A") + _block("案件B"),
        }
        for name, body in cases.items():
            with self.subTest(name=name):
                self.assertIsNone(
                    target.detect_template_exclusion(_record("案件情報", body))
                )

    def test_nested_positions_alone_do_not_create_project_blocks(self):
        record = _record(
            "単一案件",
            INTRO + _numbered_block("①", "単一案件", nested_positions=True),
        )
        self.assertIsNone(target.detect_template_exclusion(record))

    def test_normal_single_project_and_resource_are_preserved(self):
        single_project = _record("単一案件", INTRO + _block("単一案件"))
        self.assertIsNone(target.detect_template_exclusion(single_project))

        resource = _record(
            "単一要員のご紹介",
            "氏名：A.B\n所属：弊社社員\n最寄駅：東京駅\n"
            "稼働：12月\n単価：70万円\nスキル：Java\n",
        )
        self.assertIsNone(target.detect_template_exclusion(resource))
        self.assertTrue(
            target._can_rescue_single_complete_resource_profile(
                resource, None, None
            )
        )

    def test_latest_run_has_exactly_two_new_overview_block_hits(self):
        with Path(target.INPUT_MASTER).open(encoding="utf-8") as file_obj:
            master = {
                record["message_id"]: record
                for record in (
                    json.loads(line) for line in file_obj if line.strip()
                )
            }
        with Path(target.INPUT_PREV).open(encoding="utf-8") as file_obj:
            input_ids = {
                json.loads(line)["message_id"]
                for line in file_obj if line.strip()
            }

        hits = {
            message_id
            for message_id in input_ids
            if target._complete_distinct_project_overview_block_titles(
                master[message_id]["body_text"]
            )
        }
        self.assertEqual(hits, TARGET_IDS)
        for message_id in TARGET_IDS:
            self.assertEqual(
                target.detect_template_exclusion(master[message_id]), DECISION
            )


if __name__ == "__main__":
    unittest.main()

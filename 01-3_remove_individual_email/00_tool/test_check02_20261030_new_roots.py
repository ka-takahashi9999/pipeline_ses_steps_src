#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CHECK-02 2026-10-30: counted project / complete resource block tests."""

import importlib.util
import json
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent / "remove_individual_email.py"
SPEC = importlib.util.spec_from_file_location("remove_individual_email_check02_new", MODULE_PATH)
detector = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(detector)

COUNTED_PROJECT_RULE = "counted_two_project_blocks_v1"
COMPLETE_RESOURCE_RULE = (
    "repeated_complete_resource_blocks_without_accepted_count_v1"
)
EXPECTED_NEW_HITS = {
    "s3_76536d55a6affadbc5382090b8196e93732d65f41e8622f6363024f33d370814": COUNTED_PROJECT_RULE,
    "s3_3b9e3ce529dd32a35977e0dff7b8ab702ce79a3283bf5bc5f9fb83d5937e14a5": COMPLETE_RESOURCE_RULE,
    "s3_ef64199a25b5a174627da9dec927cd2a38d729e5c6cacc78a1d7f02487ad38bb": COMPLETE_RESOURCE_RULE,
    "s3_0bad056a1d887ee2df6616ab716a839ca9987d80324a423d41e80df8c3fae5b5": COMPLETE_RESOURCE_RULE,
    "s3_ed0a8d1e69749ac1afb68be05b7c714f01ad7fa339c343a17ed269742431f0e7": COMPLETE_RESOURCE_RULE,
}


def _record(subject, body):
    return {
        "from": "sender@example.com",
        "subject": subject,
        "body_text": body,
        "attachments": [],
        "html_links": [],
    }


def _counted_project_block(number):
    return (
        "{0}\n"
        "■案件名：案件{0}\n"
        "■概要：概要{0}\n"
        "■募集人数：1名\n"
        "■単価：{0}0万円\n"
    ).format(number)


def _resource_block(number, name_label="氏名"):
    return (
        "{0}【{1}】P{0}\n"
        "【年齢】{2}\n"
        "【所属】会社{0}\n"
        "【最寄駅】駅{0}\n"
        "【稼働】11月\n"
        "【単価】{0}0万円\n"
        "【スキル】Skill{0}\n"
    ).format(number, name_label, 20 + number)


class Check02NewRootTest(unittest.TestCase):
    def test_counted_two_project_blocks_positive(self):
        record = _record(
            "関西SAP案件(基本リモート)2件です!",
            _counted_project_block(1) + _counted_project_block(2),
        )
        self.assertEqual(
            detector.detect_template_exclusion(record),
            ("multi_item_mail", COUNTED_PROJECT_RULE),
        )

    def test_repeated_complete_resource_blocks_positive(self):
        record = _record(
            "弊社営業中要員のご紹介",
            _resource_block(1) + _resource_block(2, "名前"),
        )
        self.assertEqual(
            detector.detect_template_exclusion(record),
            ("multi_item_mail", COMPLETE_RESOURCE_RULE),
        )

    def test_four_numbered_complete_resource_blocks_positive(self):
        record = _record(
            "4名の経験豊富な要員様のご紹介です",
            "".join(_resource_block(number) for number in range(1, 5)),
        )
        self.assertEqual(
            detector.detect_template_exclusion(record),
            ("multi_item_mail", COMPLETE_RESOURCE_RULE),
        )

    def test_recruiting_and_participating_counts_do_not_trigger(self):
        single_project = (
            "■案件名：単一案件\n■概要：開発\n"
            "■募集人数：3名\n■単価：70万円"
        )
        for subject in ("Java案件 3名募集", "上位プロパー2名参画中"):
            with self.subTest(subject=subject):
                self.assertIsNone(
                    detector.determine_exclusion_reason(
                        _record(subject, single_project), set(), []
                    )
                )

    def test_sales_signature_is_not_a_complete_resource_block(self):
        body = (
            _resource_block(1)
            + "\n氏名：営業 太郎\n所属：営業部\n"
            + "電話：03-0000-0000\nEmail：sales@example.com"
        )
        self.assertIsNone(
            detector.detect_template_exclusion(_record("単一要員のご紹介", body))
        )

    def test_single_resource_does_not_trigger(self):
        self.assertIsNone(
            detector.detect_template_exclusion(
                _record("単一要員のご紹介", _resource_block(1))
            )
        )

    def test_single_project_with_multiple_headings_does_not_trigger(self):
        body = (
            "■案件名：単一案件\n■概要：開発\n■概要：追加説明\n"
            "■募集人数：2名\n■単価：70万円\n■単価：応相談"
        )
        self.assertIsNone(
            detector.detect_template_exclusion(_record("単一案件 2件の要件", body))
        )

    def test_fixed_run_has_exactly_five_new_hits(self):
        with Path(detector.INPUT_MASTER).open(encoding="utf-8") as file_obj:
            master = {
                record["message_id"]: record
                for record in (json.loads(line) for line in file_obj if line.strip())
            }
        with Path(detector.INPUT_PREV).open(encoding="utf-8") as file_obj:
            input_ids = [
                json.loads(line)["message_id"] for line in file_obj if line.strip()
            ]

        hits = {}
        for message_id in input_ids:
            result = detector.detect_template_exclusion(master[message_id])
            if result and result[1] in (COUNTED_PROJECT_RULE, COMPLETE_RESOURCE_RULE):
                hits[message_id] = result[1]

        self.assertEqual(hits, EXPECTED_NEW_HITS)


if __name__ == "__main__":
    unittest.main()

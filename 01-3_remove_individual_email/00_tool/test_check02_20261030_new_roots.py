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
REPEATED_PROJECT_RULE = "repeated_complete_project_blocks_v1"
COMPLETE_RESOURCE_RULE = (
    "repeated_complete_resource_blocks_without_accepted_count_v1"
)
EXPECTED_NEW_HITS = {
    "s3_3d4e2e30afe1b339e9e03397f652e4168be8c1566655d8ec9691bcd6afae711e": COMPLETE_RESOURCE_RULE,
    "s3_855b42e642bf71e27cd19499b639c22a16349ba7d64ff932f7bbc5ccc7535227": COMPLETE_RESOURCE_RULE,
    "s3_6248a5b19d317e54d438dfa4fb88878714c58c91c33468d04552797739e5b2f0": COMPLETE_RESOURCE_RULE,
    "s3_c5ea5fb90740db103d9a098604bc725fc80adbe50bf265d5849d0172ca2b18b9": COUNTED_PROJECT_RULE,
}
EXPECTED_FORMAT_VARIANT_HITS = {
    "s3_35adcdb7f1370c39c932fff857497b566ae6caa690949af4c80f31ca09c7b0e8": REPEATED_PROJECT_RULE,
    "s3_c5ea5fb90740db103d9a098604bc725fc80adbe50bf265d5849d0172ca2b18b9": COUNTED_PROJECT_RULE,
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


def _boxed_project_block(number, header="■案件名■", title=None):
    return (
        "{header}\n"
        "{title}\n"
        "■案件内容■\n開発{number}\n"
        "■単価■\n{number}0万円\n"
        "■場所■\n東京{number}\n"
        "■時間■\n9:00-18:00\n"
    ).format(
        header=header,
        title=title or "案件{}".format(number),
        number=number,
    )


def _plain_project_block(number):
    return (
        "案 件：案件{0}\n"
        "内 容：開発{0}\n"
        "スキル：Python\n"
        "場 所：東京{0}\n"
        "期 間：12月〜\n"
        "単 価：{0}0万円\n"
        "面 談：1回\n"
        "募 集：1名\n"
    ).format(number)


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

    def test_boxed_project_name_variants_positive(self):
        for header in ("■案件名", "■案件名：", "■案件名■"):
            with self.subTest(header=header):
                record = _record(
                    "複数案件のご紹介",
                    _boxed_project_block(1, header)
                    + _boxed_project_block(2, header),
                )
                self.assertEqual(
                    detector.detect_template_exclusion(record),
                    ("multi_item_mail", REPEATED_PROJECT_RULE),
                )

    def test_explicit_project_count_variants_positive(self):
        body = _plain_project_block(1) + _plain_project_block(2)
        for count_text in (
            "2案件動いています",
            "2案件あります",
            "2案件ございます",
            "2案件ご紹介します",
        ):
            with self.subTest(count_text=count_text):
                record = _record(count_text, body)
                self.assertEqual(
                    detector.detect_template_exclusion(record),
                    ("multi_item_mail", COUNTED_PROJECT_RULE),
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

    def test_project_count_without_multiple_complete_blocks_does_not_trigger(self):
        for subject in (
            "2名募集",
            "2名参画中",
            "2チーム体制",
            "2案件動いています",
        ):
            with self.subTest(subject=subject):
                self.assertIsNone(
                    detector.detect_template_exclusion(
                        _record(subject, _plain_project_block(1))
                    )
                )

    def test_same_project_repeated_by_quote_does_not_trigger(self):
        body = (
            _boxed_project_block(1, title="同一案件")
            + "\n--- quoted message ---\n"
            + _boxed_project_block(2, title="同一案件")
        )
        self.assertIsNone(
            detector.detect_template_exclusion(_record("同一案件の引用", body))
        )

    def test_single_project_positions_and_numbered_sections_do_not_trigger(self):
        body = (
            "案 件：単一案件\n"
            "内 容：1. 概要\n2. 業務内容\n"
            "ポジション1：リーダー\nポジション2：メンバー\n"
            "場 所：東京\n単 価：70万円\n"
        )
        self.assertIsNone(
            detector.detect_template_exclusion(_record("単一案件", body))
        )

    def test_profile_signature_and_career_team_size_do_not_trigger(self):
        profile = (
            _resource_block(1)
            + "\n氏名：営業 太郎\n所属：営業部\n"
            + "経歴：20名規模のチームをマネジメント\n"
        )
        self.assertIsNone(
            detector.detect_template_exclusion(_record("単一要員のご紹介", profile))
        )

    def test_fixed_run_has_current_counted_and_complete_resource_hits(self):
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

    def test_fixed_run_has_exactly_two_format_variant_hits(self):
        with Path(detector.INPUT_MASTER).open(encoding="utf-8") as file_obj:
            master = {
                record["message_id"]: record
                for record in (json.loads(line) for line in file_obj if line.strip())
            }
        filtered_path = Path(detector._STEP_DIR) / "01_result" / detector.OUTPUT_FILTERED
        with filtered_path.open(encoding="utf-8") as file_obj:
            filtered_ids = [
                json.loads(line)["message_id"] for line in file_obj if line.strip()
            ]

        hits = {}
        for message_id in filtered_ids:
            result = detector.detect_template_exclusion(master[message_id])
            if result:
                hits[message_id] = result[1]

        self.assertEqual(hits, EXPECTED_FORMAT_VARIANT_HITS)


if __name__ == "__main__":
    unittest.main()

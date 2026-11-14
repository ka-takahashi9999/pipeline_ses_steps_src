#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

import extract_project_required_skills as extractor


def _skills(items):
    return [item["skill"] for item in items]


class Check04VerifiedHeadersTest(unittest.TestCase):
    def test_verified_required_headers(self):
        headings = (
            "【要件スキル】",
            "■必 須：※必須／尚可スキルへ〇×コメントをお願い致します",
            "■必 須：以下のいずれかの経験",
            "【応募条件（業務SE／PG）】",
            "■必要経験：",
        )
        for heading in headings:
            with self.subTest(heading=heading):
                self.assertEqual(
                    extractor._classify_line(heading),
                    ("required_header", ""),
                )

    def test_verified_required_headers_extract_following_items(self):
        cases = (
            ("【要件スキル】", "AWSの基本設計経験"),
            (
                "■必 須：※必須／尚可スキルへ〇×コメントをお願い致します",
                "PythonによるAPI開発経験",
            ),
            ("■必 須：以下のいずれかの経験", "C++の開発経験"),
            ("【応募条件（業務SE／PG）】", "PL/SQLを用いた開発経験"),
            ("■必要経験：", "業務システムのPM/PMO経験必須"),
        )
        for heading, skill in cases:
            with self.subTest(heading=heading):
                required, optional = extractor.rule_extract_skills(
                    "{}\n・{}\n【単価】80万円\n".format(heading, skill)
                )
                self.assertEqual(_skills(required), [skill])
                self.assertEqual(optional, [])

    def test_verified_optional_header(self):
        heading = "▼歓迎スキル・ご経験（必須ではありません）"
        self.assertEqual(
            extractor._classify_line(heading),
            ("optional_header", ""),
        )
        required, optional = extractor.rule_extract_skills(
            "{}\n・電話対応やコールセンター業務のご経験\n".format(heading)
        )
        self.assertEqual(required, [])
        self.assertEqual(
            _skills(optional),
            ["電話対応やコールセンター業務のご経験"],
        )

    def test_required_section_hard_stops_at_targeting_points(self):
        self.assertEqual(
            extractor._classify_line("■狙い目ポイント"),
            ("stop", ""),
        )
        required, optional = extractor.rule_extract_skills(
            """必須:
・Webexに関する知識・利用経験
■狙い目ポイント
・PMポジションですが技術力重視です
MUST（PM経験）に○△×を添えてお出しください
・営業補足のPM経験
"""
        )
        self.assertEqual(_skills(required), ["Webexに関する知識・利用経験"])
        self.assertEqual(optional, [])

    def test_optional_section_hard_stops_at_targeting_points(self):
        required, optional = extractor.rule_extract_skills(
            """尚可:
・金融業務の知識
■狙い目ポイント
・営業補足のPM経験
必須:
・後続のMUST風営業説明
"""
        )
        self.assertEqual(required, [])
        self.assertEqual(_skills(optional), ["金融業務の知識"])

    def test_targeting_points_hard_stop_is_full_line_only(self):
        self.assertFalse(
            extractor._is_check04_verified_section_hard_stop(
                "狙い目ポイントの補足説明"
            )
        )
        self.assertEqual(
            extractor._classify_line("狙い目ポイントの補足説明"),
            ("content", ""),
        )

    def test_unapproved_headers_remain_unrecognized(self):
        headings = (
            "【必要なスキル】",
            "【要件】",
            "《必須業務スキル》",
            "《必須スキル：メンバークラス》",
            "必要スキル：ご提案時に〇×をお願いいたします。",
            "【必要}スキル】",
            "必須：その他の経験",
            "応募条件：Java経験",
        )
        for heading in headings:
            with self.subTest(heading=heading):
                self.assertNotIn(
                    extractor._classify_line(heading)[0],
                    {"required_header", "optional_header"},
                )


if __name__ == "__main__":
    unittest.main()

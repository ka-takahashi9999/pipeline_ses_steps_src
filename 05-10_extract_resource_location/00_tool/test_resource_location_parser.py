#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""05-10: 公式駅名のlabel完全一致と従来正規化の回帰テスト。"""

import unittest
from pathlib import Path

from extract_resource_location import load_location_dictionary
from resource_location_parser import parse_location


class ResourceLocationParserTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dictionary_path = Path(__file__).resolve().parent / "location_dictionary.txt"
        cls.entries = load_location_dictionary(str(dictionary_path))

    def test_official_station_names_survive_strong_normalization(self):
        cases = {
            "JR小倉": "近畿地方",
            "京急大津": "関東地方",
            "中央大学・明星大学": "関東地方",
            "龍谷富山高校前（永楽町）": "中部地方",
        }
        for station, expected in cases.items():
            with self.subTest(station=station):
                actual, _raw, source = parse_location(
                    f"最寄駅: {station}駅", self.entries
                )
                self.assertEqual(actual, expected)
                self.assertEqual(source, "label")

    def test_existing_route_prefix_normalization_still_works(self):
        actual, _raw, source = parse_location(
            "最寄駅: 京急本線 北品川駅", self.entries
        )
        self.assertEqual(actual, "関東地方")
        self.assertEqual(source, "label")

    def test_unregistered_misspelling_is_not_added(self):
        actual, _raw, source = parse_location("最寄駅: 薬円台駅", self.entries)
        self.assertEqual(actual, "unknown")
        self.assertEqual(source, "")


if __name__ == "__main__":
    unittest.main()

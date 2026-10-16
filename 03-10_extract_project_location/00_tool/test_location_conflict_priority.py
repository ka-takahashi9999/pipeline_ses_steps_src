#!/usr/bin/env python3
"""地方候補競合時の明示地域優先に対するfocused test。"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import extract_project_location as target
from location_parser import parse_location


class LocationConflictPriorityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.entries = target.load_location_dictionary(target.DICT_PATH)

    def assert_location(self, location_text, expected):
        body = "勤務地：" + location_text
        actual, raw, source = parse_location(body, self.entries)
        self.assertEqual(actual, expected)
        self.assertEqual(raw, location_text)
        self.assertEqual(source, "label")

    def test_target_tokyo_context_overrides_ambiguous_kyobashi(self):
        self.assert_location(
            "基本テレワーク、京橋、都内顧客オフィス(日本橋など)",
            "関東地方",
        )

    def test_osaka_kyobashi_stays_kinki(self):
        self.assert_location("大阪府大阪市 京橋", "近畿地方")
        self.assert_location("京橋、大阪市内", "近畿地方")

    def test_bare_kyobashi_keeps_existing_result(self):
        self.assert_location("京橋", "近畿地方")

    def test_explicit_tokyo_context_wins_only_on_conflict(self):
        self.assert_location("京橋、東京都内顧客先", "関東地方")
        self.assert_location("参画当初は京橋(東京)を想定", "関東地方")

    def test_explicit_osaka_context_wins_only_on_conflict(self):
        self.assert_location("日本橋(大阪)", "近畿地方")

    def test_competing_explicit_regions_keep_existing_result(self):
        self.assert_location("東京都、大阪府", "関東地方")

    def test_real_multiple_locations_keep_existing_result(self):
        self.assert_location("西葛西(リモート併用)※大阪への出張あり", "関東地方")
        self.assert_location("品川or大阪(堂島) ※選べます", "関東地方")

    def test_non_conflicting_locations_keep_existing_result(self):
        self.assert_location("横浜駅周辺", "関東地方")
        self.assert_location("大阪市内", "近畿地方")

    def test_weak_same_project_delivery_is_not_forced_to_kanto(self):
        self.assert_location(
            "基本リモート(出社の際は京橋や日本橋など)",
            "近畿地方",
        )


if __name__ == "__main__":
    unittest.main()

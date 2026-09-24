"""06-10: 非地理値remoteの扱いと既存地域比較の回帰テスト。"""

import unittest
from itertools import product

from match_location import judge_location_match


class LocationMatchTest(unittest.TestCase):
    def test_existing_comparisons_are_unchanged(self):
        locations = [
            "北海道地方", "東北地方", "関東地方", "中部地方", "近畿地方",
            "中国地方", "四国地方", "九州地方", "沖縄地方",
            "unknown", "overseas", "", None,
        ]
        for remote_type, project_loc, resource_loc in product(
            ["hybrid", "fullremote", "onsite", "unknown", None],
            locations,
            locations + ["remote"],
        ):
            with self.subTest(remote_type=remote_type, project=project_loc, resource=resource_loc):
                expected = (
                    remote_type == "fullremote"
                    or not project_loc
                    or not resource_loc
                    or project_loc == resource_loc
                )
                self.assertEqual(judge_location_match(remote_type, project_loc, resource_loc), expected)

    def test_project_remote_uses_unresolved_location_path(self):
        for remote_type, resource_loc in product(
            ["hybrid", "fullremote", "onsite", "unknown", None],
            ["関東地方", "近畿地方", "unknown", "overseas", "", None],
        ):
            with self.subTest(remote_type=remote_type, resource=resource_loc):
                self.assertEqual(
                    judge_location_match(remote_type, "remote", resource_loc),
                    judge_location_match(remote_type, None, resource_loc),
                )
                self.assertTrue(judge_location_match(remote_type, "remote", resource_loc))


if __name__ == "__main__":
    unittest.main()

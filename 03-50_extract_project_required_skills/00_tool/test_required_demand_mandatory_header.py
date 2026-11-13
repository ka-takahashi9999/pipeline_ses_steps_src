"""「要求：必須」の完全一致required headerに限定したfocused test。"""

import sys
import unittest
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOL_DIR))

import extract_project_required_skills as target


class RequiredDemandMandatoryHeaderTest(unittest.TestCase):
    def test_normalized_exact_headers(self):
        for header in ("要求：必須", "要求:必須", "要求 ： 必須"):
            with self.subTest(header=header):
                self.assertEqual(target._normalize_hdr(header), "要求:必須")
                self.assertEqual(
                    target._classify_line(header),
                    ("required_header", ""),
                )

    def test_does_not_broaden_to_nearby_or_explanatory_text(self):
        for line in (
            "要求事項：必須",
            "要求 必須",
            "顧客要求を満たすことが必須です",
            "要求スキルは必須です",
        ):
            with self.subTest(line=line):
                self.assertNotEqual(
                    target._classify_line(line)[0],
                    "required_header",
                )

    def test_target_section_stops_before_recruitment(self):
        required, optional = target.rule_extract_skills(
            """要求：必須
・Cisco8000シリーズの設計、構築経験者
・Cisco SD-WANの構築経験者
・勤怠、コミュニケーション良好な方
募集：1名
・Python開発経験
"""
        )

        self.assertEqual(
            [item["skill"] for item in required],
            [
                "Cisco8000シリーズの設計、構築経験者",
                "Cisco SD-WANの構築経験者",
            ],
        )
        self.assertEqual(optional, [])

    def test_existing_required_and_optional_headers_are_unchanged(self):
        required, optional = target.rule_extract_skills(
            """必須スキル
・Java開発経験
尚可スキル
・AWS構築経験
単価：70万円
・Python開発経験
"""
        )

        self.assertEqual([item["skill"] for item in required], ["Java開発経験"])
        self.assertEqual([item["skill"] for item in optional], ["AWS構築経験"])


if __name__ == "__main__":
    unittest.main()

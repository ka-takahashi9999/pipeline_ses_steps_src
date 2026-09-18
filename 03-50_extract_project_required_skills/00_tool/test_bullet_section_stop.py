"""箇条書き条件行のコロン前キーに限定したセクション終了テスト。"""
import unittest

import extract_project_required_skills as extractor


class BulletSectionStopTest(unittest.TestCase):
    def test_existing_stop_keys_with_bullet_colon_and_empty_value(self):
        conditions = ("単 価 ：45万円", "単 価 ：", "商流：元請", "外国籍：不可",
                      "勤務地：東京", "精算：140-180h", "清算幅：140-180h",
                      "面談：1回", "年齢：～45歳")
        for condition in conditions:
            for bullet in ("・ ", "- ", "■ ", "1. ", "① "):
                with self.subTest(condition=condition, bullet=bullet):
                    self.assertTrue(extractor._is_section_stop(bullet + condition))

    def test_optional_stops_before_business_owner_conditions(self):
        for header in ("☆ 尚可スキル ☆", "▽ 歓迎スキル ▽", "Wantスキル", "歓迎要件"):
            for value in ("可", "不可", "応相談"):
                with self.subTest(header=header, value=value):
                    req, opt = extractor.rule_extract_skills(
                        "必須スキル\n・Java開発経験\n" + header + "\n・AWS構築経験\n"
                        "・ 単 価 ：45万円\n・ 事業主 ：" + value
                    )
                    self.assertEqual(req, [extractor._make_skill("Java開発経験")])
                    self.assertEqual(opt, [extractor._make_skill("AWS構築経験")])

    def test_empty_price_stops_and_later_required_can_restart(self):
        req, opt = extractor.rule_extract_skills(
            "Wantスキル\n・AWS構築経験\n・ 単 価 ：\n・事業主：応相談\n"
            "Mustスキル\n・Java開発経験"
        )
        self.assertEqual(req, [extractor._make_skill("Java開発経験")])
        self.assertEqual(opt, [extractor._make_skill("AWS構築経験")])

    def test_required_stops_without_optional_mixing(self):
        req, opt = extractor.rule_extract_skills(
            "Mustスキル\n・Java開発経験\n・ 商流：元請\n・事業主：可\n"
            "Wantスキル\n・AWS構築経験"
        )
        self.assertEqual(req, [extractor._make_skill("Java開発経験")])
        self.assertEqual(opt, [extractor._make_skill("AWS構築経験")])

    def test_existing_plain_stop_lines(self):
        for line in ("単価：70万", "商流", "外国籍", "勤務地", "精算", "清算幅", "面談", "年齢"):
            with self.subTest(line=line):
                req, opt = extractor.rule_extract_skills(
                    "必須スキル\n・Java開発経験\n尚可スキル\n・AWS構築経験\n"
                    + line + "\n・事業主：可"
                )
                self.assertEqual(req, [extractor._make_skill("Java開発経験")])
                self.assertEqual(opt, [extractor._make_skill("AWS構築経験")])

    def test_only_exact_existing_stop_keys_with_colon(self):
        for line in ("・単価計算システム：Java開発経験", "・精算システム：SQL開発経験",
                     "・Java：開発経験", "・事業主：不可", "・単 価", "・単 価 45万円"):
            with self.subTest(line=line):
                self.assertFalse(extractor._is_section_stop(line))
        req, opt = extractor.rule_extract_skills(
            "尚可スキル\n・Java：開発経験\n・単価計算システム：Java開発経験\n・AWS構築経験"
        )
        self.assertEqual(req, [])
        self.assertIn(extractor._make_skill("AWS構築経験"), opt)


if __name__ == "__main__":
    unittest.main()

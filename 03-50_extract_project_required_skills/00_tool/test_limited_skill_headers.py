"""装飾付き見出しとMust/Wantスキルに限定した回帰テスト。"""
import unittest

import extract_project_required_skills as extractor


class LimitedSkillHeadersTest(unittest.TestCase):
    def assert_sections(self, required_header, optional_header):
        req, opt = extractor.rule_extract_skills(
            required_header + "\n・Java開発経験\n" + optional_header
            + "\n・AWS構築経験\n単価：70万円\n・Python開発経験"
        )
        self.assertEqual(req, [extractor._make_skill("Java開発経験")])
        self.assertEqual(opt, [extractor._make_skill("AWS構築経験")])

    def test_decorated_headings(self):
        for left, right in (("☆ ", " ☆"), ("▽ ", " ▽"), ("▽", ""), ("", "☆")):
            with self.subTest(left=left, right=right):
                self.assert_sections(left + "必須スキル" + right, left + "尚可スキル" + right)

    def test_must_want_skill_headings(self):
        for req, opt in (("Mustスキル", "Wantスキル"), ("◆Mustスキル", "◆Wantスキル"),
                         ("【MUSTスキル】", "【WANTスキル】"), ("mustスキル：", "wantスキル：")):
            with self.subTest(req=req):
                self.assert_sections(req, opt)
        req, opt = extractor.rule_extract_skills("Wantスキル\n・AWS構築経験\nMustスキル\n・Java開発経験")
        self.assertEqual(req, [extractor._make_skill("Java開発経験")])
        self.assertEqual(opt, [extractor._make_skill("AWS構築経験")])

    def test_existing_headings(self):
        for req, opt in (("必須", "尚可"), ("【必須スキル】", "【尚可スキル】"),
                         ("Must", "Want"), ("MUST", "WANT"), ("Required", "Preferred"),
                         ("必須要件", "歓迎要件")):
            with self.subTest(req=req):
                self.assert_sections(req, opt)

    def test_normal_skill_text_is_preserved(self):
        skills = ["Java ☆ システム開発経験 ☆", "Wantスキルについて説明した経験", "▽を表示する画面の開発経験"]
        req, opt = extractor.rule_extract_skills("必須\n" + "\n".join("・" + s for s in skills))
        self.assertEqual(req, [extractor._make_skill(s) for s in skills])
        self.assertEqual(opt, [])

    def test_deferred_headings_are_not_added(self):
        for heading in ("共通必須", "役割別必須", "【要件】", "必要な技術知識", "必須・重要スキル",
                        "募集／尚良スキル", "必要なスキル", "必須／尚可経験", "必須／尚可スキル要件"):
            with self.subTest(heading=heading):
                self.assertNotIn(extractor._classify_line(heading)[0], ("required_header", "optional_header"))


if __name__ == "__main__":
    unittest.main()

"""必須の選択親行＋○子行に限定した回帰テスト。"""
import unittest

import extract_project_required_skills as extractor


class RequiredCircleAlternativesTest(unittest.TestCase):
    def test_alternatives_keep_parent_context_and_inner_and(self):
        req, opt = extractor.rule_extract_skills(
            "【必須スキル】\n・以下のいずれかの設計・構築経験\n"
            "　○vSphereおよびNSX\n　○VMware Cloud Foundation（VCF）\n"
            "・顧客とのフロント対応、技術的なQA対応の経験\n"
            "【歓迎要件】\n・AWS構築経験"
        )
        self.assertEqual(req, [extractor._make_skill(s) for s in (
            "(vSphereおよびNSX、またはVMware Cloud Foundation(VCF))の設計・構築経験",
            "顧客とのフロント対応、技術的なQA対応の経験",
        )])
        self.assertEqual(opt, [extractor._make_skill("AWS構築経験")])

    def test_existing_bullets(self):
        for bullet in ("・", "-", "●", "◆", "■", "□", "◎", "※", "▶", "➤", "*", "1. "):
            with self.subTest(bullet=bullet):
                req, opt = extractor.rule_extract_skills(
                    "必須スキル\n" + bullet + "Java開発経験\n歓迎要件\n" + bullet + "AWS構築経験"
                )
                self.assertEqual(req, [extractor._make_skill("Java開発経験")])
                self.assertEqual(opt, [extractor._make_skill("AWS構築経験")])

    def test_no_parent_no_circle_promotion(self):
        self.assertEqual(extractor.rule_extract_skills("必須スキル\n○vSphereおよびNSX"), ([], []))

    def test_parent_does_not_cross_section_or_sibling(self):
        for boundary in ("歓迎要件", "単価：70万", "・Java開発経験"):
            with self.subTest(boundary=boundary):
                req, _ = extractor.rule_extract_skills(
                    "必須スキル\n・以下のいずれかの設計・構築経験\n○vSphere\n"
                    + boundary + "\n○NSX"
                )
                self.assertEqual(req[0]["skill"], "(vSphere)の設計・構築経験")
                self.assertFalse(any("NSX" in s["skill"] for s in req))

    def test_plain_sentence(self):
        self.assertEqual(
            extractor.rule_extract_skills("必須スキル\nJavaの設計・構築経験"),
            ([extractor._make_skill("Javaの設計・構築経験")], []),
        )


if __name__ == "__main__":
    unittest.main()

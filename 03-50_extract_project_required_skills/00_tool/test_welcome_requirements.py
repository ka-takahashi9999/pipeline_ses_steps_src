"""歓迎要件のセクション切替に限定した回帰テスト（外部API・成果物書込なし）。"""

import json
import unittest
from unittest.mock import patch

import extract_project_required_skills as extractor


class WelcomeRequirementsTest(unittest.TestCase):
    def assert_skills(self, body, required, optional):
        req, opt = extractor.rule_extract_skills(body)
        self.assertEqual(req, [extractor._make_skill(s) for s in required])
        self.assertEqual(opt, [extractor._make_skill(s) for s in optional])

    def test_welcome_heading_switches_section_and_is_not_a_skill(self):
        for heading in (
            "歓迎要件", "【歓迎要件】", "■歓迎要件", "歓迎要件：",
            "◆ 歓迎要件", "歓 迎 要 件", "歓迎要件（あれば）",
        ):
            with self.subTest(heading=heading):
                self.assert_skills(
                    "必須要件\n・Java開発経験\n" + heading
                    + "\n・AWS構築経験\n・SQL開発経験",
                    ["Java開発経験"], ["AWS構築経験", "SQL開発経験"],
                )

    def test_seven_required_items_stay_seven(self):
        required = ["Java開発経験{}".format(i) for i in range(7)]
        optional = ["AWS構築経験{}".format(i) for i in range(6)]
        body = "必須要件\n" + "\n".join("・" + s for s in required)
        body += "\n■歓迎要件\n" + "\n".join("・" + s for s in optional)
        self.assert_skills(body, required, optional)

    def test_required_only_is_unchanged(self):
        self.assert_skills(
            "必須スキル\n・Java開発経験\n・SQL開発経験\n単価：70万円",
            ["Java開発経験", "SQL開発経験"], [],
        )

    def test_existing_optional_headings_are_unchanged(self):
        for heading in (
            "尚可", "尚可スキル", "尚可条件", "尚可要件", "尚可スキル・経験",
            "歓迎", "歓迎スキル", "歓迎スキル/経験", "歓迎条件", "歓迎する経験",
            "優遇スキル", "優遇条件", "あれば尚可", "あると望ましい",
            "あると尚可", "あると嬉しいスキル", "以下、あると良いスキル",
            "望ましい経験", "プラス要素", "希望要件", "Preferred", "WANT",
        ):
            with self.subTest(heading=heading):
                self.assert_skills(
                    "必須要件\n・Java開発経験\n【" + heading + "】\n・AWS構築経験",
                    ["Java開発経験"], ["AWS構築経験"],
                )

    def test_welcome_bracket_inline_and_required_restart(self):
        self.assert_skills(
            "必須要件\n・Java開発経験\n【歓迎要件】AWS構築経験\n"
            "・SQL開発経験\n必須要件\n・Python開発経験",
            ["Java開発経験", "Python開発経験"], ["AWS構築経験", "SQL開発経験"],
        )

    def test_empty_heading_and_section_stop(self):
        self.assert_skills("歓迎要件", [], [])
        self.assert_skills(
            "必須要件\n・Java開発経験\n歓迎要件\n・AWS構築経験\n"
            "単価：70万円\n・Python開発経験",
            ["Java開発経験"], ["AWS構築経験"],
        )

    def test_welcome_word_inside_skill_does_not_switch_section(self):
        self.assert_skills(
            "必須要件\n・歓迎要件の管理システム開発経験\n・Java開発経験",
            ["歓迎要件の管理システム開発経験", "Java開発経験"], [],
        )

    def test_jsonl_schema_and_public_extraction(self):
        body = "必須要件\n・Java開発経験\n歓迎要件\n・AWS構築経験"
        with patch.object(extractor, "llm_extract_skills", side_effect=AssertionError("LLM prohibited")):
            req, opt, method = extractor.extract_skills("test-message", body)
        self.assertEqual(method, "rule")
        self.assertEqual(req, [extractor._make_skill("Java開発経験")])
        self.assertEqual(opt, [extractor._make_skill("AWS構築経験")])
        record = extractor.build_record("test-message", req, opt, method)
        self.assertEqual(set(record), {"message_id", "required_skills", "optional_skills"})
        self.assertEqual(record["message_id"], "test-message")
        for item in req + opt:
            self.assertEqual(set(item), {"skill", "match", "note"})
            self.assertIsInstance(item["skill"], str)
            self.assertIsNone(item["match"])
            self.assertIsNone(item["note"])
        line = json.dumps(record, ensure_ascii=False) + "\n"
        self.assertEqual(len(line.splitlines()), 1)
        self.assertEqual(json.loads(line.encode("utf-8")), record)


if __name__ == "__main__":
    unittest.main()

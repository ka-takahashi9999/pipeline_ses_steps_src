import sys
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parents[1]
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

import classify_type_project_resource as target


class ResourceProfileLabelDecorationTest(unittest.TestCase):
    def setUp(self):
        self.empty_keywords = target.KeywordDict(resource={}, project={})

    def _classify(self, subject, body):
        return target.rule_classify(subject, body, self.empty_keywords)[0]

    def test_tortoise_shell_bracket_profile_with_four_labels_is_resource(self):
        body = "\n".join((
            "〔氏名〕A.B",
            "〔年齢〕30歳",
            "〔所属〕弊社社員",
            "〔最寄駅〕東京駅",
        ))
        self.assertEqual(self._classify("", body), "resource")

    def test_reported_single_person_profile_shapes_are_resources(self):
        profiles = (
            "\n".join((
                "〔氏名〕A.B", "〔年齢〕30歳", "〔最寄駅〕東京駅",
                "〔所属〕弊社社員", "〔稼働〕即日", "〔単価〕80万円",
            )),
            "\n".join((
                "〔氏名〕C.D", "〔年齢〕34歳", "〔性別〕男性",
                "〔最寄駅〕恵比寿駅", "〔所属〕弊社個人事業主",
                "〔開始日〕11月", "〔単価〕95万円",
            )),
        )
        for body in profiles:
            with self.subTest(body=body):
                self.assertEqual(self._classify("", body), "resource")

    def test_existing_profile_labels_accept_tortoise_shell_brackets(self):
        labels_and_pattern_indexes = (
            ("氏名", 1), ("年齢", 3), ("所属", 5),
            ("最寄駅", 7), ("最寄り駅", 7), ("入場日", 9),
            ("単金", 11), ("単価", 13), ("名前", 15),
            ("稼働", 18), ("稼動", 19),
        )
        for label, pattern_index in labels_and_pattern_indexes:
            with self.subTest(label=label):
                body_cjk = target._remove_cjk_inner_spaces(
                    target._normalize(f"〔{label}〕値")
                )
                pattern = target._RESOURCE_SINGLE_LABEL_RES[pattern_index]
                self.assertIsNotNone(pattern.search(body_cjk))

    def test_existing_auxiliary_profile_labels_accept_same_decoration(self):
        labels_and_pattern_indexes = (
            ("場所", 1), ("所在地", 3), ("時期", 5), ("稼働開始", 7),
        )
        for label, pattern_index in labels_and_pattern_indexes:
            with self.subTest(label=label):
                pattern = target._RESOURCE_AUX_PROFILE_LABEL_RES[pattern_index]
                self.assertIsNotNone(pattern.search(f"〔{label}〕値"))

    def test_one_to_three_decorated_labels_do_not_force_resource(self):
        labels = ("氏名", "年齢", "所属")
        for count in range(1, 4):
            with self.subTest(count=count):
                body = "\n".join(f"〔{label}〕値" for label in labels[:count])
                self.assertNotEqual(self._classify("", body), "resource")

    def test_repeated_name_does_not_satisfy_distinct_label_threshold(self):
        body = "\n".join(("〔氏名〕A.B", "〔氏名〕C.D", "〔氏名〕E.F"))
        self.assertNotEqual(self._classify("", body), "resource")

    def test_project_with_incidental_decorated_label_stays_project(self):
        body = "\n".join((
            "【案件】：Java基幹システム開発",
            "【概要】：追加機能の設計・開発",
            "【場所】：東京都内",
            "〔氏名〕提案時に記載",
        ))
        self.assertEqual(self._classify("Java開発案件", body), "project")

    def test_list_portal_and_service_notification_are_not_promoted(self):
        cases = (
            ("要員一覧", "〔氏名〕A.B\n〔氏名〕C.D\n一覧ページをご確認ください"),
            ("ポータル更新", "〔氏名〕登録者\n〔所属〕登録企業\nログインして確認してください"),
            ("サービス通知", "〔氏名〕利用者\nアカウント設定が更新されました"),
        )
        for subject, body in cases:
            with self.subTest(subject=subject):
                self.assertNotEqual(self._classify(subject, body), "resource")

    def test_existing_colon_and_black_lenticular_bracket_grammar_is_unchanged(self):
        colon_profile = "氏名：A.B\n年齢：30\n所属：弊社社員\n最寄駅：東京駅"
        bracket_profile = "【氏名】A.B\n【年齢】30\n【所属】弊社社員\n【最寄駅】東京駅"
        self.assertEqual(self._classify("", colon_profile), "resource")
        self.assertEqual(self._classify("", bracket_profile), "resource")

    def test_new_label_meanings_are_not_added(self):
        body = "〔開始日〕即日\n〔性別〕男性\n〔希望〕Web案件\n〔備考〕面談可能"
        self.assertNotEqual(self._classify("", body), "resource")


if __name__ == "__main__":
    unittest.main()

"""明示的な出社上限のみの抽出と、既存remote判定の回帰確認。"""
import unittest

import extract_resource_remote as target


class OnsiteLimitTest(unittest.TestCase):
    def test_explicit_upper_limits(self):
        for text in ['週2まで出社可能', '週2日まで出社可能', '出社は週2まで', '出社週2まで',
                     '週3日まで出社可能', '出社は週1まで', '週4まで出社可能']:
            with self.subTest(text=text):
                self.assertEqual(target.rule_extract_remote(text), ('hybrid', 'extracted', text))
                self.assertEqual(target.rule_extract_remote('出社条件：' + text + '（通勤120分以内）'),
                                 ('hybrid', 'extracted', text))
        self.assertEqual(target.rule_extract_remote('出社条件：週２日まで出社可能 '),
                         ('hybrid', 'extracted', '週2日まで出社可能'))

    def test_long_body_without_remote_keyword(self):
        body = '経歴の説明。' * 100 + '\n出社条件：週2まで出社可能\n' + '経歴の説明。' * 100
        self.assertEqual(target.rule_extract_remote(body), ('hybrid', 'extracted', '週2まで出社可能'))

    def test_existing_cases(self):
        for text in ['常駐可能', 'フル出社', '週数回出社', '勤務条件記載なし', '']:
            with self.subTest(text=text):
                self.assertEqual(target.rule_extract_remote(text), ('onsite', 'default', None))
        for text in ['ハイブリッド', '週2日リモート', 'リモート希望', '一部リモート']:
            self.assertEqual(target.rule_extract_remote(text), ('hybrid', 'extracted', text))
        self.assertEqual(target.rule_extract_remote('フルリモート'), ('fullremote', 'extracted', 'フルリモート'))
        self.assertEqual(target.rule_extract_remote('フルリモート希望（週2まで出社可能）'),
                         ('fullremote', 'extracted', 'フルリモート'))
        self.assertEqual(target.rule_extract_remote('リモート希望（週2まで出社可能）'),
                         ('hybrid', 'extracted', 'リモート希望'))

    def test_do_not_expand_to_other_attendance_expressions(self):
        for text in ['週2日出社可能', '出社可能', '週5日まで出社可能', '出社は週5まで',
                     '週0まで出社可能', '週12まで出社可能', '週2日まで出社検討可',
                     '週2まで出社可能ではない', '出社は週2までではなく毎日可能',
                     '出社は週2までなら相談可能', '週2まで\n出社可能']:
            with self.subTest(text=text):
                self.assertEqual(target.rule_extract_remote(text), ('onsite', 'default', None))


if __name__ == '__main__':
    unittest.main()

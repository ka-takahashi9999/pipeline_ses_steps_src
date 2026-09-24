"""必須/尚可経験と人材スキル要件内の明示切替に限定したテスト。"""
import unittest

import extract_project_required_skills as extractor


class ExperiencePersonHeadersTest(unittest.TestCase):
    def assert_skills(self, body, required, optional):
        req, opt = extractor.rule_extract_skills(body)
        self.assertEqual([s['skill'] for s in req], required)
        self.assertEqual([s['skill'] for s in opt], optional)

    def test_experience_headings_and_work_details_boundary(self):
        for left, right in [('■', '：'), ('【', '】'), ('◆ ', ''), ('', '')]:
            with self.subTest(left=left):
                self.assert_skills(
                    left + '必須経験' + right + '\n・社内SE経験\n・英語スキル\n・資料作成能力\n'
                    + left + '尚可経験' + right + '\n・海外チーム連携経験\n・ERP保守経験\n・ネットワーク経験\n'
                    '■稼働詳細：\n・稼働単価：\n※スキル、経験年数等によります。',
                    ['社内SE経験', '英語スキル', '資料作成能力'],
                    ['海外チーム連携経験', 'ERP保守経験', 'ネットワーク経験'],
                )

    def test_person_required_to_bonus_and_stop(self):
        for required in ['以下すべて必須', '以下すべて必須となります。']:
            for bonus in ['加点', '■加点要素：', '【加点項目】', '以下はあれば加点対象です']:
                for stop in ['【発注条件】', '・契約形態：準委任', '・単価：170万円', '勤務地：東京']:
                    with self.subTest(required=required, bonus=bonus, stop=stop):
                        self.assert_skills(
                            '【人材スキル要件】\n' + required + '\n・新規事業の構想策定経験\n・高いコミュニケーション力\n'
                            + bonus + '\n・MVP開発経験\n' + stop + '\n・契約更新の運用経験\n',
                            ['新規事業の構想策定経験', '高いコミュニケーション力'], ['MVP開発経験'],
                        )

    def test_context_is_required_and_does_not_leak_after_stop(self):
        for prefix in ['', '【人材スキル要件】\n【発注条件】\n']:
            self.assert_skills(prefix + '以下すべて必須\n・Java開発経験\n加点\n・AWS構築経験', [], [])
        self.assert_skills('【人材スキル要件】\n・Java開発経験\n加点\n・AWS構築経験', [], [])


if __name__ == '__main__':
    unittest.main()

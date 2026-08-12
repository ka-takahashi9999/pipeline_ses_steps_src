import sys
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parents[1]
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

import classify_type_project_resource as target


class RuleClassifyTest(unittest.TestCase):
    def setUp(self):
        self.keywords = target.KeywordDict(
            resource={},
            project={
                target._normalize("要件定義"): 5.0,
                target._normalize("基本設計"): 5.0,
                target._normalize("詳細設計"): 5.0,
                target._normalize("求人"): 5.0,
                target._normalize("募集"): 1.3,
            },
        )

    def test_direct_individual_profile_wins_over_career_keywords(self):
        subject = (
            "【SPONTO直個人】即日〜 SE/テックリード / PHP・Laravel / "
            "58歳 / 日本 / 男性 / 深江橋駅（大阪府）"
        )
        body = """エンジニアのご紹介です。
名前：A.B
所属：個人事業主
単価：80万円
要件定義 基本設計 詳細設計 求人
要件定義 基本設計 詳細設計 求人
案件がございましたらご紹介ください。
"""

        mail_type, _, _ = target.rule_classify(subject, body, self.keywords)

        self.assertEqual(mail_type, "resource")

    def test_direct_individual_allowed_variants_follow_project_score(self):
        body = "募集：Javaエンジニア1名"

        for expression in (
            "直個人可",
            "直個人 可",
            "直個人も可",
            "直個人相談可",
            "直個人様可",
        ):
            with self.subTest(expression=expression):
                subject = f"Java開発／{expression}／50歳まで"
                mail_type, _, _ = target.rule_classify(subject, body, self.keywords)
                res_score, proj_score, *_ = target.score_text(subject, body, self.keywords)

                self.assertEqual(res_score, 0.0)
                self.assertGreater(proj_score, res_score)
                self.assertEqual(mail_type, "project")

    def test_project_with_direct_individual_allowed_is_not_forced_to_resource(self):
        subject = "【案件】Java開発／直個人可／50歳まで"
        body = """【案件：】基幹システム開発
【概要：】要件定義から詳細設計
【場所：】東京
技術者を募集しております。
"""

        mail_type, _, _ = target.rule_classify(subject, body, self.keywords)

        self.assertEqual(mail_type, "project")

    def test_clear_project_mail_is_not_classified_as_resource(self):
        subject = "Java基幹システム開発"
        body = """募集：Javaエンジニア1名
業務内容：基幹システム開発
必須スキル：Java開発経験
勤務地：東京
単価：80万円
"""

        mail_type, _, _ = target.rule_classify(subject, body, self.keywords)

        self.assertEqual(mail_type, "project")


if __name__ == "__main__":
    unittest.main()

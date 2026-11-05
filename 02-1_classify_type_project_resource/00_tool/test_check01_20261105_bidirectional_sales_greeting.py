import json
import sys
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parents[1]
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

import classify_type_project_resource as target


MISCLASSIFIED_PROJECT_ID = (
    "s3_20c32dd9f4f87fe0dc3ce30528a67d964563a892154e0619d0284898b7eaba33"
)
SAME_GREETING_EXPECTED_TYPES = {
    MISCLASSIFIED_PROJECT_ID: "project",
    "s3_a46d227cc4a520960ff7d019bd62484b03c68b33d401d999f79f26acaaabf0c1": "project",
    "s3_449b7cac773b1123f9922cae554e577d2e0c2cc093bb53981d9b7774f3e68d54": "resource",
    "s3_e14b4ad967d5711ecfc551a3fe8838b9257d6ab7d9a237b9d17424d173f01cdb": "resource",
}


def _read_jsonl(path):
    with Path(path).open(encoding="utf-8") as file_obj:
        return [json.loads(line) for line in file_obj if line.strip()]


class BidirectionalSalesGreetingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.master = {
            record["message_id"]: record
            for record in _read_jsonl(target.INPUT_MASTER)
        }
        cls.cleaned = {
            record["message_id"]: record
            for record in _read_jsonl(target.INPUT_CLEANED)
        }
        cls.keywords = target.load_keywords(target.KEYWORDS_PATH)

    def _classify_actual(self, message_id):
        mail = self.master[message_id]
        body = (
            self.cleaned[message_id].get("body_text")
            or mail.get("body_text")
            or ""
        )
        return target.rule_classify(
            mail.get("subject") or "",
            body,
            self.keywords,
            has_attachment=bool(mail.get("attachments") or []),
        )[0]

    def test_supported_bidirectional_greetings_do_not_score_either_side(self):
        greeting_keywords = target.KeywordDict(
            resource={
                target._normalize("見合う案件"): 2.0,
                target._normalize("要員情報"): 2.0,
                target._normalize("人材"): 2.0,
            },
            project={target._normalize("案件"): 2.0},
        )
        greetings = (
            "見合う案件・要員情報ございましたらご紹介ください",
            "見合う案件、要員情報がございましたらご紹介ください",
            "案件・人材情報ございましたらご連絡ください",
            "案件／要員情報ございましたらお願いします",
            "見合う案件 ・ 要員情報ございましたらご紹介いただけますと幸いです。",
        )

        for greeting in greetings:
            with self.subTest(greeting=greeting):
                res_score, proj_score, _, _, res_ctx, proj_ctx = target.score_text(
                    "", greeting, greeting_keywords
                )
                self.assertEqual(res_score, 0.0)
                self.assertEqual(proj_score, 0.0)
                self.assertEqual(res_ctx, 0.0)
                self.assertEqual(proj_ctx, 0.0)

    def test_one_way_resource_grammar_keeps_its_resource_evidence(self):
        phrases = (
            "見合う案件がございましたら",
            "案件を探しております",
            "要員をご紹介します",
            "要員情報をお送りします",
        )

        for phrase in phrases:
            with self.subTest(phrase=phrase):
                res_score, _, _, _, res_ctx, _ = target.score_text(
                    "", phrase, self.keywords
                )
                self.assertGreater(res_score, 0.0)
                self.assertGreater(res_ctx, 0.0)

    def test_actual_records_with_the_same_greeting_keep_expected_types(self):
        for message_id, expected_type in SAME_GREETING_EXPECTED_TYPES.items():
            with self.subTest(message_id=message_id):
                self.assertEqual(self._classify_actual(message_id), expected_type)

    def test_profile_structure_remains_resource_with_bidirectional_greeting(self):
        body = (
            "見合う案件・要員情報ございましたらご紹介ください。\n"
            "イニシャル：KT\n年齢：40歳\n最寄駅：東京駅\n"
            "所属：弊社社員\n希望条件：クラウド設計案件"
        )
        mail_type, _, _ = target.rule_classify("弊社社員のご紹介", body, self.keywords)
        self.assertEqual(mail_type, "resource")


if __name__ == "__main__":
    unittest.main()

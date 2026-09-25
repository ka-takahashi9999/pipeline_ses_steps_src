import json
import sys
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parents[1]
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

import classify_type_project_resource as target


TARGET_MESSAGE_ID = "1a0d0d60a8c482b7"


def _read_jsonl(path):
    with path.open(encoding="utf-8") as file_obj:
        return [json.loads(line) for line in file_obj if line.strip()]


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

    def test_resource_profile_labels_allow_horizontal_space_before_colon(self):
        labels_and_patterns = (
            ("氏名", 0),
            ("年齢", 2),
            ("所属", 4),
            ("最寄駅", 6),
            ("入場日", 8),
            ("単金", 10),
            ("単価", 12),
            ("名前", 14),
            ("稼働", 16),
            ("稼動", 17),
        )

        for label, pattern_index in labels_and_patterns:
            pattern = target._RESOURCE_SINGLE_LABEL_RES[pattern_index]
            for separator in ("", " ", "\t", "\u3000"):
                with self.subTest(label=label, separator=repr(separator)):
                    body = target._remove_cjk_inner_spaces(
                        target._normalize(f"{label}{separator}：値")
                    )
                    self.assertIsNotNone(pattern.search(body))

    def test_resource_profile_labels_do_not_allow_newline_before_colon(self):
        labels_and_patterns = (
            ("氏名", 0),
            ("年齢", 2),
            ("所属", 4),
            ("最寄駅", 6),
            ("入場日", 8),
            ("単金", 10),
            ("単価", 12),
            ("名前", 14),
            ("稼働", 16),
            ("稼動", 17),
        )

        for label, pattern_index in labels_and_patterns:
            with self.subTest(label=label):
                pattern = target._RESOURCE_SINGLE_LABEL_RES[pattern_index]
                body = target._remove_cjk_inner_spaces(
                    target._normalize(f"{label}\n：値")
                )
                self.assertIsNone(pattern.search(body))

    def test_resource_profile_threshold_remains_four_distinct_labels(self):
        empty_keywords = target.KeywordDict(resource={}, project={})
        three_labels = "氏名 ：A\n年齢\t：30\n所属　：弊社社員"
        four_labels = three_labels + "\n最寄駅 ：東京駅"

        three_type, _, _ = target.rule_classify("", three_labels, empty_keywords)
        four_type, _, _ = target.rule_classify("", four_labels, empty_keywords)

        self.assertNotEqual(three_type, "resource")
        self.assertEqual(four_type, "resource")

    def test_target_mail_is_classified_as_resource(self):
        master = {
            record["message_id"]: record
            for record in _read_jsonl(Path(target.INPUT_MASTER))
        }
        cleaned = {
            record["message_id"]: record
            for record in _read_jsonl(Path(target.INPUT_CLEANED))
        }
        mail = master[TARGET_MESSAGE_ID]
        body = cleaned[TARGET_MESSAGE_ID].get("body_text") or mail.get("body_text") or ""
        keywords = target.load_keywords(target.KEYWORDS_PATH)

        mail_type, _, _ = target.rule_classify(
            mail.get("subject") or "",
            body,
            keywords,
            has_attachment=bool(mail.get("attachments") or []),
        )

        self.assertEqual(mail_type, "resource")

    def test_latest_input_has_only_the_expected_classification_change(self):
        previous = {}
        for output_name in (target.OUTPUT_CLASSIFIED, target.OUTPUT_UNKNOWN):
            output_path = target._STEP_DIR / "01_result" / output_name
            for record in _read_jsonl(output_path):
                previous[record["message_id"]] = record["mail_type"]

        master = {
            record["message_id"]: record
            for record in _read_jsonl(Path(target.INPUT_MASTER))
        }
        cleaned = {
            record["message_id"]: record
            for record in _read_jsonl(Path(target.INPUT_CLEANED))
        }
        input_records = _read_jsonl(Path(target.INPUT_PREV))
        keywords = target.load_keywords(target.KEYWORDS_PATH)
        changes = {}

        for record in input_records:
            message_id = record["message_id"]
            mail = master.get(message_id, {})
            clean_record = cleaned.get(message_id, {})
            body = clean_record.get("body_text") or mail.get("body_text") or ""
            mail_type, _, _ = target.rule_classify(
                mail.get("subject") or "",
                body,
                keywords,
                has_attachment=bool(mail.get("attachments") or []),
            )
            if mail_type != previous[message_id]:
                changes[message_id] = (previous[message_id], mail_type)

        self.assertEqual(len(input_records), 2649)
        self.assertEqual(
            changes,
            {TARGET_MESSAGE_ID: ("project", "resource")},
        )


if __name__ == "__main__":
    unittest.main()

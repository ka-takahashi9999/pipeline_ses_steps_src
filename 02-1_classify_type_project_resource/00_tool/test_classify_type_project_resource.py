import json
import sys
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parents[1]
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

import classify_type_project_resource as target


TARGET_MESSAGE_ID = "1a0e5c142f948c56"
PROJECT_REQUEST_MESSAGE_IDS = (
    "s3_528bff3a9ed1623f8a83bec459f16a9b3ae389b438a94bc7d8bea0fd54043e3d",
    "s3_fec2114d4c2d6e8b630b133685bd08d1894b932a41241930cd6085cd492752d5",
    "s3_d083fb5caca3fde9454209b51f51b76d6b608c693b11f1efc26645c2b4fffe8f",
)
ACTUAL_RESOURCE_MESSAGE_IDS = (
    "s3_5b87003fa08ad697a86720411baa226b4746a0e7320ecde6b70fa747dd27ccdf",
    "s3_c49952aad6f9fd7c43016fc5f451ffc18a36cf57835638fba7a612005588ac9d",
)
HELD_AMBIGUOUS_MESSAGE_ID = (
    "s3_83daacd01fe012599d1ef41c7a1676b561dc6625f780c06d8e39f3d5046ded56"
)


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

    def test_initial_label_allows_only_expected_horizontal_space_and_colon_variants(self):
        pattern = target._RESOURCE_SINGLE_LABEL_RES[-1]

        for text in (
            "イニシャル: NM",
            "イニシャル：NM",
            "イニシャル : NM",
            "イニシャル　：NM",
            "イニシャル\t: NM",
        ):
            with self.subTest(text=text):
                body = target._remove_cjk_inner_spaces(target._normalize(text))
                self.assertIsNotNone(pattern.search(body))

        body = target._remove_cjk_inner_spaces(
            target._normalize("イニシャル\n: NM")
        )
        self.assertIsNone(pattern.search(body))

    def test_initial_label_alone_or_below_threshold_does_not_force_resource(self):
        empty_keywords = target.KeywordDict(resource={}, project={})

        initial_only_type, _, _ = target.rule_classify(
            "", "イニシャル: NM", empty_keywords
        )
        three_labels_type, _, _ = target.rule_classify(
            "", "イニシャル: NM\n年齢: 29\n単価: 58万円", empty_keywords
        )

        self.assertNotEqual(initial_only_type, "resource")
        self.assertNotEqual(three_labels_type, "resource")

    def test_resource_profile_threshold_remains_four_distinct_labels(self):
        empty_keywords = target.KeywordDict(resource={}, project={})
        three_labels = "氏名 ：A\n年齢\t：30\n所属　：弊社社員"
        four_labels = three_labels + "\n最寄駅 ：東京駅"

        three_type, _, _ = target.rule_classify("", three_labels, empty_keywords)
        four_type, _, _ = target.rule_classify("", four_labels, empty_keywords)

        self.assertNotEqual(three_type, "resource")
        self.assertEqual(four_type, "resource")

    def test_project_requests_for_resource_introductions_are_projects(self):
        master = {
            record["message_id"]: record
            for record in _read_jsonl(Path(target.INPUT_MASTER))
        }
        cleaned = {
            record["message_id"]: record
            for record in _read_jsonl(Path(target.INPUT_CLEANED))
        }
        previous = {
            record["message_id"]: record["mail_type"]
            for record in _read_jsonl(
                target._STEP_DIR / "01_result" / target.OUTPUT_CLASSIFIED
            )
        }
        keywords = target.load_keywords(target.KEYWORDS_PATH)

        for message_id in PROJECT_REQUEST_MESSAGE_IDS:
            with self.subTest(message_id=message_id):
                mail = master[message_id]
                body = (
                    cleaned[message_id].get("body_text")
                    or mail.get("body_text")
                    or ""
                )
                mail_type, _, _ = target.rule_classify(
                    mail.get("subject") or "",
                    body,
                    keywords,
                    has_attachment=bool(mail.get("attachments") or []),
                )
                _, _, _, _, resource_context_score, _ = target.score_text(
                    mail.get("subject") or "",
                    body,
                    keywords,
                    has_attachment=bool(mail.get("attachments") or []),
                )

                self.assertEqual(previous[message_id], "resource")
                self.assertEqual(resource_context_score, 0.0)
                self.assertEqual(mail_type, "project")

    def test_actual_resource_introductions_remain_resources(self):
        master = {
            record["message_id"]: record
            for record in _read_jsonl(Path(target.INPUT_MASTER))
        }
        cleaned = {
            record["message_id"]: record
            for record in _read_jsonl(Path(target.INPUT_CLEANED))
        }
        keywords = target.load_keywords(target.KEYWORDS_PATH)

        for message_id in ACTUAL_RESOURCE_MESSAGE_IDS:
            with self.subTest(message_id=message_id):
                mail = master[message_id]
                body = (
                    cleaned[message_id].get("body_text")
                    or mail.get("body_text")
                    or ""
                )
                mail_type, _, _ = target.rule_classify(
                    mail.get("subject") or "",
                    body,
                    keywords,
                    has_attachment=bool(mail.get("attachments") or []),
                )

                self.assertIn("要員をご紹介いたします", body)
                self.assertEqual(mail_type, "resource")

        for introduction_phrase in (
            "要員をご紹介します",
            "弊社要員をご紹介いたします",
        ):
            with self.subTest(introduction_phrase=introduction_phrase):
                mail_type, _, _ = target.rule_classify(
                    "", introduction_phrase, keywords
                )
                _, _, _, _, resource_context_score, _ = target.score_text(
                    "", introduction_phrase, keywords
                )

                self.assertGreater(resource_context_score, 0.0)
                self.assertEqual(mail_type, "resource")

    def test_resource_request_phrases_do_not_create_resource_context(self):
        keywords = target.load_keywords(target.KEYWORDS_PATH)
        project_structure = "【案件】開発案件\n【概要】新規開発\n【場所】東京\n"

        for request_phrase in (
            "要員をご紹介お願いいたします",
            "要員がおりましたらご紹介をお願いいたします",
            "見合う要員がいましたらご紹介ください",
            "要員をご提案お願いします",
        ):
            with self.subTest(request_phrase=request_phrase):
                body = project_structure + request_phrase
                mail_type, _, _ = target.rule_classify("", body, keywords)
                _, _, _, _, resource_context_score, _ = target.score_text(
                    "", body, keywords
                )

                self.assertEqual(resource_context_score, 0.0)
                self.assertEqual(mail_type, "project")

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

        previous = {
            record["message_id"]: record["mail_type"]
            for record in _read_jsonl(target._STEP_DIR / "01_result" / target.OUTPUT_CLASSIFIED)
        }
        self.assertEqual(previous[TARGET_MESSAGE_ID], "project")
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
        current_types = {}

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
            current_types[message_id] = mail_type
            if mail_type != previous[message_id]:
                changes[message_id] = (previous[message_id], mail_type)

        self.assertEqual(len(input_records), 3400)
        self.assertEqual(
            changes,
            {
                message_id: ("resource", "project")
                for message_id in PROJECT_REQUEST_MESSAGE_IDS
            },
        )
        self.assertEqual(current_types[HELD_AMBIGUOUS_MESSAGE_ID], "ambiguous")
        self.assertEqual(
            sum(mail_type == "ambiguous" for mail_type in previous.values()),
            sum(mail_type == "ambiguous" for mail_type in current_types.values()),
        )


if __name__ == "__main__":
    unittest.main()

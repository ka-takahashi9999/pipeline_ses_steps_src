import json
import sys
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parents[1]
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

import classify_type_project_resource as target


PROJECT_TO_RESOURCE_ID = (
    "s3_59ca8a650009c8ef59c65325352c9888fb286f4f9f4ac5fcdfb3b6ad778f6b62"
)
AMBIGUOUS_TO_RESOURCE_ID = (
    "s3_d417e0e7c6b315b5729033e8fc1ca2838bc269e2b40c2439f38468dc1e253ea5"
)


def _read_jsonl(path):
    with Path(path).open(encoding="utf-8") as file_obj:
        return [json.loads(line) for line in file_obj if line.strip()]


class LimitedResourceProfileRescueTest(unittest.TestCase):
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

    def test_two_actual_single_resource_profiles_are_rescued(self):
        self.assertEqual(self._classify_actual(PROJECT_TO_RESOURCE_ID), "resource")
        self.assertEqual(self._classify_actual(AMBIGUOUS_TO_RESOURCE_ID), "resource")

    def test_auxiliary_labels_never_rescue_without_all_and_conditions(self):
        empty_keywords = target.KeywordDict(resource={}, project={})
        cases = (
            (
                "Java開発案件",
                "年齢：50歳以下\n単価：80万円\n稼働：11月\n場所：東京\n時期：11月",
            ),
            (
                "会社情報のご案内",
                "会社名：サンプル\n所在地：東京都\n設立時期：2020年\nサービスのご案内",
            ),
            (
                "【要員募集】開発案件",
                "氏名：ご提案時に記載\n単価：70万円\n稼働：11月\n時期：11月から開始",
            ),
            (
                "【即日要員】営業サービス",
                "所在地：東京都\nイベント時期：11月\n業務経歴書：不要",
            ),
            (
                "セミナーのご案内",
                "年齢：不問\n単価：無料\n所在地：会場案内\n業務経歴書：サービス名",
            ),
        )

        for subject, body in cases:
            with self.subTest(subject=subject):
                mail_type, _, _ = target.rule_classify(
                    subject, body, empty_keywords
                )
                self.assertNotEqual(mail_type, "resource")

    def test_existing_four_label_resource_rule_is_unchanged(self):
        empty_keywords = target.KeywordDict(resource={}, project={})
        body = "氏名：A.B\n年齢：30\n所属：弊社社員\n最寄駅：東京駅"
        mail_type, _, _ = target.rule_classify("", body, empty_keywords)
        self.assertEqual(mail_type, "resource")

    def test_latest_input_changes_only_the_two_targets(self):
        previous = {
            record["message_id"]: record["mail_type"]
            for record in _read_jsonl(
                target._STEP_DIR / "01_result" / target.OUTPUT_CLASSIFIED
            )
        }
        previous.update(
            {
                record["message_id"]: record["mail_type"]
                for record in _read_jsonl(
                    target._STEP_DIR / "01_result" / target.OUTPUT_UNKNOWN
                )
            }
        )
        input_records = _read_jsonl(target.INPUT_PREV)
        current = {}

        for record in input_records:
            message_id = record["message_id"]
            mail = self.master.get(message_id, {})
            clean_record = self.cleaned.get(message_id, {})
            body = clean_record.get("body_text") or mail.get("body_text") or ""
            current[message_id] = target.rule_classify(
                mail.get("subject") or "",
                body,
                self.keywords,
                has_attachment=bool(mail.get("attachments") or []),
            )[0]

        changes = {
            message_id: (previous[message_id], mail_type)
            for message_id, mail_type in current.items()
            if previous[message_id] != mail_type
        }
        self.assertEqual(len(input_records), 3417)
        self.assertEqual(
            changes,
            {
                PROJECT_TO_RESOURCE_ID: ("project", "resource"),
                AMBIGUOUS_TO_RESOURCE_ID: ("ambiguous", "resource"),
            },
        )
        self.assertEqual(
            sum(mail_type == "unknown" for mail_type in previous.values()),
            sum(mail_type == "unknown" for mail_type in current.values()),
        )


if __name__ == "__main__":
    unittest.main()

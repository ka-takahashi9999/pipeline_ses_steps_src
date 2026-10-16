#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Step 01-3 P1 Subject detector focused tests."""

import importlib.util
import json
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent / "remove_individual_email.py"
SPEC = importlib.util.spec_from_file_location("remove_individual_email", MODULE_PATH)
detector = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(detector)


TARGET_MESSAGE_IDS = {
    "s3_e7ee031d20a1208cb200cbc9fa93c09121a37f6f46d62f75ac445d441e783714",
    "s3_03f29cb1d364deddcb05a947b73c378f1d1592bc72ae4224160d778a9c53aa55",
    "s3_cb13590660797356952558fc6c517b5f0081e1594eec7dee7798bdfa972d8485",
    "s3_b09d2f2b96b0e89e75d0a2deed11ae65fb3fb0c8daf09ef6ecce64de029822d5",
    "s3_b31ce10994e8d8be217dcc6f1374565eb1ac5bd23f3961272a0c876d64a436be",
    "s3_8ffa35e51f48dfcf57146534d2389c22ad6f1df62b039fdb0a1f031c6912fcda",
}
SINGLE_RESOURCE_MESSAGE_ID = (
    "s3_5b87003fa08ad697a86720411baa226b4746a0e7320ecde6b70fa747dd27ccdf"
)
MULTIPLE_PROJECT_SHARE_MESSAGE_ID = (
    "s3_0e1e7d9a7ff41e2e0301c444445d29a7772563fb85e671a16a25ee94e18c7e67"
)


def _read_jsonl(path):
    with Path(path).open(encoding="utf-8") as file_obj:
        return [json.loads(line) for line in file_obj if line.strip()]


def _read_master_subjects():
    subjects = {}
    with Path(detector.INPUT_MASTER).open(encoding="utf-8") as file_obj:
        for line in file_obj:
            if not line.strip():
                continue
            record = json.loads(line)
            subjects[record["message_id"]] = record.get("subject") or ""
    return subjects


class SubjectDetectorTest(unittest.TestCase):
    def assert_excluded(self, subject: str) -> None:
        self.assertIsNotNone(detector.detect_p1_exclusion_reason(subject), subject)

    def assert_allowed(self, subject: str) -> None:
        self.assertIsNone(detector.detect_p1_exclusion_reason(subject), subject)

    def test_explicit_multiple_resource_grammars(self) -> None:
        for subject in (
            "Java要員3名のご紹介",
            "要員：3名",
            "3名の要員",
            "2名で1人月",
            "プロパー２名",
        ):
            with self.subTest(subject=subject):
                self.assert_excluded(subject)

    def test_resource_separator_is_narrow(self) -> None:
        for subject in ("要員・3名", "要員1名", "二名の要員"):
            with self.subTest(subject=subject):
                self.assert_allowed(subject)

    def test_resource_multiplier_grammar(self) -> None:
        for subject in (
            "エンジニア×2名",
            "エンジニアx3名",
            "要員X4名",
        ):
            with self.subTest(subject=subject):
                self.assert_excluded(subject)

    def test_role_set_grammar_requires_resource_context(self) -> None:
        for subject in (
            "【要員】Java:SE2名セット",
            "人材情報 SE 2名セット",
        ):
            with self.subTest(subject=subject):
                self.assert_excluded(subject)

        for subject in ("SE2名セット", "Java:SE 2名セット"):
            with self.subTest(subject=subject):
                self.assert_allowed(subject)

    def test_single_and_unrelated_headcounts_are_allowed(self) -> None:
        for subject in (
            "エンジニア×1名",
            "【要員】SE1名セット",
            "2名募集",
            "2名体制",
            "2名参画",
            "PM 2名募集",
            "エンジニア2名募集",
        ):
            with self.subTest(subject=subject):
                self.assert_allowed(subject)

    def test_recruitment_veto(self) -> None:
        for subject in (
            "Javaエンジニア募集(2名)",
            "IBM COBOL要員の募集/SE、PG各2名の募集",
            "2名募集",
            "3名募集",
            "複数名募集",
            "募集枠2名",
            "増員2名",
            "要員3名のご紹介・別案件も募集",
        ):
            with self.subTest(subject=subject):
                self.assert_allowed(subject)

    def test_management_scale_veto(self) -> None:
        for subject in (
            "要員100名規模のマネジメント実績",
            "要員3名の管理経験あり",
        ):
            with self.subTest(subject=subject):
                self.assert_allowed(subject)

    def test_list_as_primary_fullmatch(self) -> None:
        for subject in (
            "弊社営業中要員リストの送付",
            "【要員情報】弊社営業中要員リストの送付",
            "RE: [共有] 要員共有8/19",
            "Fw:【案件】案件・要員まとめ更新",
        ):
            with self.subTest(subject=subject):
                self.assert_excluded(subject)

    def test_list_word_contains_is_allowed(self) -> None:
        for subject in (
            "田中様ご紹介＋その他人材一覧",
            "Javaエンジニアのご紹介/弊社要員一覧はこちら",
            "○○様のご紹介と営業中人材一覧",
            "要員一覧を添付しましたので田中様をご紹介します",
        ):
            with self.subTest(subject=subject):
                self.assert_allowed(subject)

    def test_explicit_multiple_project_grammars(self) -> None:
        self.assert_excluded("案件3件")
        self.assert_excluded("3件の案件")
        self.assert_allowed("案件1件")
        self.assert_allowed("M365案件")
        self.assert_allowed("3案件")

    def test_multiple_project_share_or_intro_grammars(self) -> None:
        for subject in (
            "3案件ご共有",
            "3案件共有",
            "2案件ご紹介",
            "2案件紹介",
            "３案件ご共有",
        ):
            with self.subTest(subject=subject):
                self.assertEqual(
                    detector.detect_p1_exclusion_reason(subject),
                    "multiple_project_explicit_subject_count",
                )

    def test_single_and_unrelated_project_phrases_are_allowed(self) -> None:
        for subject in (
            "1案件ご共有",
            "1案件紹介",
            "3案件の参画経験があります",
            "過去2案件を担当",
            "3案件目から参画",
            "2案件について検討",
            "案件3をご共有",
        ):
            with self.subTest(subject=subject):
                self.assert_allowed(subject)

    def test_body_urls_and_attachments_are_not_detector_inputs(self) -> None:
        record = {
            "subject": "単一要員のご紹介",
            "body_text": "要員3名 https://example.com/a https://example.com/b",
            "attachments": [{"filename": "A.xlsx"}, {"filename": "B.xlsx"}],
            "html_links": ["https://example.com/a", "https://example.com/b"],
        }
        self.assertIsNone(
            detector.determine_exclusion_reason(record, set(), []), record["subject"]
        )

    def test_manual_exclusion_is_evaluated_first(self) -> None:
        record = {"from": "manual@example.com", "subject": "案件3件"}
        self.assertEqual(
            detector.determine_exclusion_reason(record, {"manual@example.com"}, []),
            "manual_exclude_list",
        )

    def test_confirmed_six_messages_use_existing_p1_reason(self) -> None:
        subjects = _read_master_subjects()

        for message_id in TARGET_MESSAGE_IDS:
            with self.subTest(message_id=message_id):
                self.assertEqual(
                    detector.detect_p1_exclusion_reason(subjects[message_id]),
                    "multiple_resource_explicit_subject_count",
                )

    def test_latest_survivors_have_only_target_new_exclusion(self) -> None:
        subjects = _read_master_subjects()
        survivor_ids = {
            record["message_id"]
            for record in _read_jsonl(
                detector._STEP_DIR / "01_result" / detector.OUTPUT_FILTERED
            )
        }
        newly_excluded = {
            message_id
            for message_id in survivor_ids
            if detector.detect_p1_exclusion_reason(subjects[message_id]) is not None
        }

        self.assertEqual(len(survivor_ids), 3417)
        self.assertEqual(newly_excluded, {MULTIPLE_PROJECT_SHARE_MESSAGE_ID})

    def test_actual_single_resource_remains_allowed(self) -> None:
        subjects = _read_master_subjects()

        self.assertIsNone(
            detector.detect_p1_exclusion_reason(
                subjects[SINGLE_RESOURCE_MESSAGE_ID]
            )
        )


if __name__ == "__main__":
    unittest.main()

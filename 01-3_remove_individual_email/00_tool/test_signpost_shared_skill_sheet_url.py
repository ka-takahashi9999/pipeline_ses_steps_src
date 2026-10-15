#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Signpost共有skillsheet URL許容のfocused tests。"""

import copy
import json
import sys
import unittest
from pathlib import Path


STEP = Path(__file__).resolve().parent.parent
ROOT = STEP.parent
sys.path.insert(0, str(STEP / "00_tool"))

import remove_individual_email as target


TARGET_IDS = {
    "s3_6333748d1e11a11bd2d49da02bbcfb76e048aee6bbe6ef3fa46679418bbbf51b",
    "s3_c3e31e5f5ecf2b2417b342c236a62cfc5cea08d980a92861403fbea2ea36c897",
}
EXISTING_TWO_PROFILE_IDS = {
    "s3_63092b20f93c5130da5f17a9a26588dd67f46e95e4dcbb694c20d345c7478216",
    "s3_2285cf95bb67999c38e7aa460cb84896209a943942e7509261634ae1b98a905e",
}
SINGLE_PROFILE_ID = (
    "s3_2ae222ad87d25078331bd2c5f02eaeca815029f26f92ca2446dc19accf05ef41"
)
DECISION = ("multi_item_mail", "signpost_multi_resource_intro_v1")


def _normalized_body(record):
    return target.unicodedata.normalize(
        "NFKC", record.get("body_text") or ""
    ).replace("\r\n", "\n").strip()


def _load_selected_records(message_ids):
    records = {}
    with Path(target.INPUT_MASTER).open(encoding="utf-8") as file_obj:
        for line in file_obj:
            if not line.strip():
                continue
            record = json.loads(line)
            message_id = record.get("message_id")
            if message_id in message_ids:
                records[message_id] = record
    if set(records) != set(message_ids):
        raise AssertionError("focused fixtureがmail masterに存在しません")
    return records


class SignpostSharedSkillSheetUrlTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture_ids = TARGET_IDS | EXISTING_TWO_PROFILE_IDS | {SINGLE_PROFILE_ID}
        cls.records = _load_selected_records(fixture_ids)

    def test_two_new_messages_are_excluded_with_existing_decision(self):
        expected_counts = {
            "s3_6333748d1e11a11bd2d49da02bbcfb76e048aee6bbe6ef3fa46679418bbbf51b": 3,
            "s3_c3e31e5f5ecf2b2417b342c236a62cfc5cea08d980a92861403fbea2ea36c897": 2,
        }
        for message_id, profile_count in expected_counts.items():
            with self.subTest(message_id=message_id):
                record = self.records[message_id]
                body = _normalized_body(record)
                self.assertEqual(
                    len(target._SIGNPOST_NAME_RE.findall(body)), profile_count
                )
                self.assertEqual(
                    len(target._SIGNPOST_SKILL_SHEET_URL_RE.findall(body)), 1
                )
                self.assertEqual(target.detect_template_exclusion(record), DECISION)
                self.assertEqual(
                    target.determine_exclusion_reason(record, set(), []),
                    "multi_item_mail",
                )
                detail = target.build_detail_record(record, *DECISION)
                self.assertEqual(detail["reason"], DECISION[0])
                self.assertEqual(detail["rule_id"], DECISION[1])
                self.assertEqual(
                    {
                        key: value
                        for key, value in detail.items()
                        if key not in ("reason", "rule_id")
                    },
                    record,
                )

    def test_existing_two_and_three_url_cases_remain_excluded(self):
        for message_id in EXISTING_TWO_PROFILE_IDS:
            with self.subTest(message_id=message_id, profile_count=2):
                record = self.records[message_id]
                body = _normalized_body(record)
                self.assertEqual(len(target._SIGNPOST_NAME_RE.findall(body)), 2)
                self.assertEqual(
                    len(target._SIGNPOST_SKILL_SHEET_URL_RE.findall(body)), 2
                )
                self.assertEqual(target.detect_template_exclusion(record), DECISION)

        three_profiles = copy.deepcopy(
            self.records[
                "s3_6333748d1e11a11bd2d49da02bbcfb76e048aee6bbe6ef3fa46679418bbbf51b"
            ]
        )
        three_profiles["body_text"] = _normalized_body(three_profiles) + (
            "\nスキルシート: https://skills.example.invalid/person/2"
            "\nスキルシート: https://skills.example.invalid/person/3"
        )
        body = _normalized_body(three_profiles)
        self.assertEqual(len(target._SIGNPOST_NAME_RE.findall(body)), 3)
        self.assertEqual(len(target._SIGNPOST_SKILL_SHEET_URL_RE.findall(body)), 3)
        self.assertEqual(target.detect_template_exclusion(three_profiles), DECISION)

    def test_single_profile_is_kept(self):
        record = self.records[SINGLE_PROFILE_ID]
        body = _normalized_body(record)
        self.assertEqual(len(target._SIGNPOST_NAME_RE.findall(body)), 1)
        self.assertEqual(len(target._SIGNPOST_SKILL_SHEET_URL_RE.findall(body)), 1)
        self.assertIsNone(target.detect_template_exclusion(record))

    def test_zero_and_excess_urls_are_kept(self):
        base = copy.deepcopy(
            self.records[
                "s3_c3e31e5f5ecf2b2417b342c236a62cfc5cea08d980a92861403fbea2ea36c897"
            ]
        )
        no_url = copy.deepcopy(base)
        no_url["body_text"] = target._SIGNPOST_SKILL_SHEET_URL_RE.sub(
            "", _normalized_body(no_url)
        )
        self.assertEqual(
            len(target._SIGNPOST_SKILL_SHEET_URL_RE.findall(_normalized_body(no_url))),
            0,
        )
        self.assertIsNone(target.detect_template_exclusion(no_url))

        excess_urls = copy.deepcopy(base)
        excess_urls["body_text"] = _normalized_body(excess_urls) + (
            "\nスキルシート: https://skills.example.invalid/person/2"
            "\nスキルシート: https://skills.example.invalid/person/3"
        )
        self.assertEqual(
            len(
                target._SIGNPOST_SKILL_SHEET_URL_RE.findall(
                    _normalized_body(excess_urls)
                )
            ),
            3,
        )
        self.assertIsNone(target.detect_template_exclusion(excess_urls))

    def test_profile_field_count_mismatch_is_kept(self):
        mismatched = copy.deepcopy(
            self.records[
                "s3_c3e31e5f5ecf2b2417b342c236a62cfc5cea08d980a92861403fbea2ea36c897"
            ]
        )
        mismatched["body_text"] = target._SIGNPOST_AFFILIATION_RE.sub(
            "", _normalized_body(mismatched), count=1
        )
        body = _normalized_body(mismatched)
        self.assertEqual(len(target._SIGNPOST_NAME_RE.findall(body)), 2)
        self.assertEqual(len(target._SIGNPOST_AFFILIATION_RE.findall(body)), 1)
        self.assertEqual(len(target._SIGNPOST_SKILL_SHEET_URL_RE.findall(body)), 1)
        self.assertIsNone(target.detect_template_exclusion(mismatched))

    def test_latest_survivors_add_only_the_two_target_messages(self):
        with (STEP / "01_result" / target.OUTPUT_FILTERED).open(
            encoding="utf-8"
        ) as file_obj:
            survivor_ids = {
                json.loads(line)["message_id"]
                for line in file_obj
                if line.strip()
            }
        detected_ids = set()
        with Path(target.INPUT_MASTER).open(encoding="utf-8") as file_obj:
            for line in file_obj:
                if not line.strip():
                    continue
                record = json.loads(line)
                message_id = record.get("message_id")
                if (
                    message_id in survivor_ids
                    and target.detect_template_exclusion(record) == DECISION
                ):
                    detected_ids.add(message_id)

        self.assertEqual(len(survivor_ids), 3400)
        self.assertEqual(detected_ids, TARGET_IDS)


if __name__ == "__main__":
    unittest.main()

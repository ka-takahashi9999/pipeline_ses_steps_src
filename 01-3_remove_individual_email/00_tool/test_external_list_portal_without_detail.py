#!/usr/bin/env python3
"""外部一覧portalのみを案内するメールのFocused Test。"""

import copy
import json
import sys
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parents[1]
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

import remove_individual_email as target


HARUMINA_ID = "s3_d96bda471fc03c389bc975f26aa4dd4a9398e6d19116e038f7a9307f4e9f77b1"
IDENTITY_RUBY_ID = "s3_e635c4322a294a34f82129787296b86ce7c7b62fd95f59e36f5ad85810092694"
DECISION = (
    "list_or_portal_notice",
    "external_list_portal_without_individual_detail_v1",
)


def _read_jsonl(path):
    with Path(path).open(encoding="utf-8") as file_obj:
        return [json.loads(line) for line in file_obj if line.strip()]


def _record(subject, body, attachments=None, html_links=None):
    return {
        "message_id": "fixture",
        "from": "Example <sales@example.com>",
        "subject": subject,
        "body_text": body,
        "attachments": attachments or [],
        "html_links": html_links or [],
    }


class ExternalListPortalWithoutDetailTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.master = {
            record["message_id"]: record
            for record in _read_jsonl(target.INPUT_MASTER)
        }

    def test_actual_two_list_only_records_are_excluded(self):
        for message_id in (HARUMINA_ID, IDENTITY_RUBY_ID):
            with self.subTest(message_id=message_id):
                self.assertEqual(
                    target.detect_template_exclusion(self.master[message_id]),
                    DECISION,
                )

    def test_company_prefix_and_role_names_are_not_allowlisted(self):
        company_prefixes = (
            ("(株)Example_要員リスト", "要員リスト", "resources"),
            ("株式会社Example 要員リスト", "要員リスト", "resources"),
            ("Example_人材一覧", "人材一覧", "people"),
            ("Example_案件一覧", "案件一覧", "projects"),
        )
        for subject, label, path in company_prefixes:
            with self.subTest(subject=subject):
                record = _record(
                    subject,
                    "{}\nhttps://portal.example.com/{}".format(label, path),
                )
                self.assertEqual(target.detect_template_exclusion(record), DECISION)

        for role in ("Ruby", "Java", "PMO"):
            with self.subTest(role=role):
                record = _record(
                    "ご提案可能な営業中{}人材".format(role),
                    "営業中{}人材のご案内\n■人材一覧リスト\n"
                    "--------------------------------\n"
                    "https://portal.example.com/people".format(role),
                )
                self.assertEqual(target.detect_template_exclusion(record), DECISION)

    def test_individual_resource_and_project_details_are_kept(self):
        profile = _record(
            "Javaエンジニアのご紹介",
            "氏名:A.B\n年齢:35歳\n最寄駅:新宿駅\n所属:弊社社員\n"
            "希望単価:70万円\n人材一覧\nhttps://portal.example.com/people",
        )
        project = _record(
            "Java開発案件",
            "案件名:基幹開発\n業務内容:API開発\n必須スキル:Java\n"
            "勤務地:東京\n単価:80万円\n案件一覧\nhttps://portal.example.com/projects",
        )
        self.assertIsNone(target.detect_template_exclusion(profile))
        self.assertIsNone(target.detect_template_exclusion(project))

    def test_skill_sheet_links_and_attachments_are_kept(self):
        individual_skill_sheet = _record(
            "A.B / 35歳 / Javaエンジニア",
            "スキルシート\nhttps://drive.google.com/file/d/individual",
        )
        sharepoint_resume = _record(
            "C.D / 40歳 / PMO",
            "職務経歴書\nhttps://example.sharepoint.com/individual-resume",
        )
        self.assertIsNone(target.detect_template_exclusion(individual_skill_sheet))
        self.assertIsNone(target.detect_template_exclusion(sharepoint_resume))

        for filename in ("skill.xlsx", "resume.pdf", "career.docx"):
            with self.subTest(filename=filename):
                attached = _record(
                    "営業中Java人材",
                    "人材一覧\nhttps://portal.example.com/people",
                    attachments=[{"filename": filename}],
                )
                self.assertIsNone(target.detect_template_exclusion(attached))

    def test_company_and_signature_urls_alone_are_kept(self):
        company = _record(
            "会社情報のご案内",
            "弊社サービスのご案内\nhttps://www.example.com/",
        )
        signature = _record(
            "ご挨拶",
            "よろしくお願いいたします。\n株式会社Example\nhttps://www.example.com/",
        )
        list_text_with_company_url = _record(
            "営業情報",
            "人材一覧をご希望の場合はご連絡ください。\n"
            "株式会社Example\nhttps://www.example.com/",
        )
        for record in (company, signature, list_text_with_company_url):
            self.assertIsNone(target.detect_template_exclusion(record))

    def test_latest_survivors_add_only_the_two_confirmed_records(self):
        survivor_ids = {
            record["message_id"]
            for record in _read_jsonl(
                target._STEP_DIR / "01_result" / target.OUTPUT_FILTERED
            )
        }
        generic_ids = {
            message_id
            for message_id in survivor_ids
            if target.detect_template_exclusion(self.master[message_id]) == DECISION
        }
        self.assertEqual(generic_ids, {HARUMINA_ID, IDENTITY_RUBY_ID})


if __name__ == "__main__":
    unittest.main()

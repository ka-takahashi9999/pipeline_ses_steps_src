"""09-5 static sales index rendererのfocused test。"""

import re
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from render_sales_index import render_sales_index


class RenderSalesIndexTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.output_base = Path(self.temporary.name) / "01_result"
        self.preview_dir = self.output_base / "reply_preview_20260820"
        self.preview_dir.mkdir(parents=True)

    def tearDown(self):
        self.temporary.cleanup()

    def _record(self, resource_id, queue, checks, priority=None, initial=False, previous=False):
        refs = []
        for direction in ("reply_to_project", "reply_to_resource"):
            preview = f"reply_preview_20260820/band/{resource_id}_{direction}.txt"
            note = f"reply_preview_20260820/band/note/{resource_id}_{direction}_note.txt"
            for path_text in (preview, note):
                artifact = self.output_base / path_text
                artifact.parent.mkdir(parents=True, exist_ok=True)
                artifact.write_text("fixture", encoding="utf-8")
            refs.append({"draft_direction": direction, "preview_file_path": preview, "note_file_path": note})
        record = {
            "project_message_id": "project-1",
            "resource_message_id": resource_id,
            "queue": queue,
            "required_skill_checks": checks,
            "draft_refs": refs,
            "previous_candidate": previous,
            "previous_candidate_date": "20260819",
        }
        if queue == "human_review":
            record.update(
                {
                    "review_priority": priority,
                    "initial_review": initial,
                    "normalized_review_items": ["phase"],
                }
            )
        return record

    @staticmethod
    def _checks(matched, total, sales_confirmation=0, review=0):
        checks = []
        grounded = matched - sales_confirmation
        for index in range(grounded):
            checks.append(
                {
                    "skill": f"技術条件{index}",
                    "confidence": "confirmed",
                    "evidence": f"実務根拠{index}",
                    "reason": "経歴に明記",
                }
            )
        for index in range(sales_confirmation):
            checks.append(
                {
                    "skill": f"人物条件{index}",
                    "confidence": "confirmed",
                    "evidence": "",
                    "reason": "営業確認前提で固定true",
                }
            )
        for index in range(review):
            checks.append(
                {
                    "skill": f"要確認条件{index}",
                    "confidence": "human_review",
                    "evidence": "関連記載",
                    "reason": "断定不可",
                }
            )
        while len(checks) < total:
            checks.append(
                {
                    "skill": f"不足条件{len(checks)}",
                    "confidence": "not_confirmed",
                    "evidence": "",
                    "reason": "記載なし",
                }
            )
        return checks

    @staticmethod
    def _candidate(record):
        candidate = {
            "project_message_id": record["project_message_id"],
            "resource_message_id": record["resource_message_id"],
            "project_subject": "案件 <script>alert(1)</script>",
            "project_sender_company": "A&B株式会社",
            "project_sender_name": "<営業>",
            "resource_subject": f"要員 <img src=x> {record['resource_message_id']}",
            "resource_sender_company": "R&D株式会社",
            "resource_sender_name": "担当 > 氏名",
            "category_match": "match",
        }
        if record["resource_message_id"] == "resource-100":
            candidate["resource_skillsheet_available"] = True
            candidate["resource_skillsheet_source"] = "url"
            candidate["resource_skillsheet_url"] = "https://drive.example.test/skillsheet"
        elif record["resource_message_id"] == "resource-previous":
            candidate["resource_skillsheet_available"] = True
            candidate["resource_skillsheet_source"] = "attachment"
        elif record["resource_message_id"] == "resource-90":
            candidate["resource_skillsheet_available"] = False
            candidate["resource_skillsheet_url"] = "https://tracking.example.test/not-fetched"
        return candidate

    @staticmethod
    def _recheck(record, optional=0.0, duplicate=False):
        return {
            "project_info": {"message_id": record["project_message_id"]},
            "resource_info": {"message_id": record["resource_message_id"]},
            "match_info": {"optional_skills_match_rate": optional},
            "required_skill_checks": record["required_skill_checks"],
            "duplicate_proposal_check": duplicate,
        }

    def _fixture(self):
        required_100_optional_0 = self._record(
            "resource-100", "proposal_ready", self._checks(2, 2, sales_confirmation=1)
        )
        required_90 = self._record(
            "resource-90", "human_review", self._checks(9, 10, review=1), "HIGH", initial=True
        )
        sales_confirmation = self._record(
            "resource-sales", "human_review", self._checks(2, 2, sales_confirmation=1), "OTHER"
        )
        required_80_optional_100 = self._record(
            "resource-80", "human_review", self._checks(4, 5), "HIGH", initial=False
        )
        previous = self._record(
            "resource-previous", "proposal_ready", self._checks(2, 2), previous=True
        )
        all_records = [
            required_100_optional_0,
            sales_confirmation,
            required_90,
            required_80_optional_100,
            previous,
        ]
        return (
            [required_100_optional_0, previous],
            [required_80_optional_100, required_90, sales_confirmation],
            [self._candidate(record) for record in all_records],
            [
                self._recheck(required_100_optional_0, optional=0.0, duplicate=True),
                self._recheck(sales_confirmation, optional=0.0),
                self._recheck(required_90, optional=0.5),
                self._recheck(required_80_optional_100, optional=1.0),
                self._recheck(previous, optional=1.0),
            ],
        )

    def test_new_previous_required_order_evidence_and_escape(self):
        proposal, human, candidates, rechecks = self._fixture()
        document, summary = render_sales_index(
            proposal, human, candidates, rechecks, self.preview_dir, self.output_base, "20260820"
        )
        sections = ("new_ready", "new_sales_review", "new_additional", "previous")
        positions = [document.index(f'data-section="{section}"') for section in sections]
        self.assertEqual(sorted(positions), positions)
        self.assertIn('<details class="sales-section previous"', document)
        self.assertEqual(1, document.count("[前回も候補: 20260819]"))
        self.assertNotIn("営業済み", document)
        self.assertIn("過去AI判定を再利用", document)
        for label in ("○ 根拠あり", "○ 営業確認前提", "△ 要確認", "× 不足"):
            self.assertIn(label, document)
        self.assertIn("根拠: 「実務根拠0」", document)
        self.assertIn(">Skillsheet</a>", document)
        self.assertIn("Skillsheet: attachmentから取得済み（リンクなし）", document)
        self.assertIn("Skillsheet参照なし", document)
        self.assertNotIn("tracking.example.test/not-fetched", document)
        self.assertNotIn("<script>alert(1)</script>", document)
        self.assertNotIn("<img src=x>", document)
        new_document = document[: document.index('data-section="previous"')]
        ids = re.findall(r'data-resource-id="([^"]+)"', new_document)
        self.assertEqual(["resource-100", "resource-sales", "resource-90", "resource-80"], ids)
        self.assertEqual(1, summary["new_ready"])
        self.assertEqual(1, summary["new_sales_review"])
        self.assertEqual(2, summary["new_additional"])
        self.assertEqual(1, summary["previous"])
        self.assertEqual(4, summary["new_total"])
        self.assertEqual(5, summary["total"])
        self.assertEqual(0, summary["duplicate"])
        self.assertEqual(20, summary["draft_link_count"])

    def test_optional_100_never_overtakes_required_100_optional_0(self):
        proposal, human, candidates, rechecks = self._fixture()
        document, _summary = render_sales_index(
            proposal, human, candidates, rechecks, self.preview_dir, self.output_base, "20260820"
        )
        self.assertLess(document.index("resource-100"), document.index("resource-80"))

    def test_missing_or_escaping_draft_ref_fails(self):
        proposal, human, candidates, rechecks = self._fixture()
        proposal[0]["draft_refs"][0]["preview_file_path"] = "reply_preview_20260820/../outside.txt"
        with self.assertRaises(ValueError):
            render_sales_index(
                proposal, human, candidates, rechecks, self.preview_dir, self.output_base, "20260820"
            )

    def test_candidate_loss_fails(self):
        proposal, human, candidates, rechecks = self._fixture()
        with self.assertRaises(ValueError):
            render_sales_index(
                proposal, human, candidates[:-1], rechecks, self.preview_dir, self.output_base, "20260820"
            )


if __name__ == "__main__":
    unittest.main()

"""09-5 static sales index rendererのfocused test。"""

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

    def _record(
        self,
        resource_id,
        queue,
        priority=None,
        rank=0,
        initial=False,
        previous=False,
        review_items=None,
    ):
        refs = []
        for direction in ("reply_to_project", "reply_to_resource"):
            preview = f"reply_preview_20260820/band/{resource_id}_{direction}.txt"
            note = f"reply_preview_20260820/band/note/{resource_id}_{direction}_note.txt"
            for path_text in (preview, note):
                artifact = self.output_base / path_text
                artifact.parent.mkdir(parents=True, exist_ok=True)
                artifact.write_text("fixture", encoding="utf-8")
            refs.append(
                {
                    "draft_direction": direction,
                    "preview_file_path": preview,
                    "note_file_path": note,
                }
            )
        record = {
            "project_message_id": "project-1",
            "resource_message_id": resource_id,
            "queue": queue,
            "required_skill_recheck_info": {
                "required_skill_count": 2,
                "confirmed_count": 1,
            },
            "draft_refs": refs,
            "previous_candidate": previous,
            "previous_candidate_date": "20260819",
        }
        if queue == "human_review":
            record.update(
                {
                    "review_priority": priority,
                    "high_project_rank": rank,
                    "initial_review": initial,
                    "normalized_review_items": review_items or ["phase"],
                }
            )
        return record

    @staticmethod
    def _candidate(record):
        return {
            "project_message_id": record["project_message_id"],
            "resource_message_id": record["resource_message_id"],
            "project_subject": "案件 <script>alert(1)</script>",
            "project_sender_company": "A&B株式会社",
            "project_sender_name": "<営業>",
            "resource_subject": f"要員 <img src=x> {record['resource_message_id']}",
            "resource_sender_company": "R&D株式会社",
            "resource_sender_name": "担当 > 氏名",
        }

    @staticmethod
    def _recheck(record):
        return {
            "project_info": {"message_id": record["project_message_id"]},
            "resource_info": {"message_id": record["resource_message_id"]},
            "match_info": {"required_skills_match_rate": 0.8},
        }

    def _fixture(self):
        proposal = self._record("resource-p", "proposal_ready", previous=True)
        high_rank_2 = self._record(
            "resource-h2", "human_review", "HIGH", rank=2, initial=True, review_items=["role"]
        )
        high_rank_1 = self._record(
            "resource-h1", "human_review", "HIGH", rank=1, initial=True, previous=True
        )
        additional = self._record(
            "resource-ha", "human_review", "HIGH", rank=3, initial=False
        )
        other = self._record(
            "resource-o", "human_review", "OTHER", review_items=["unknown-raw-token<script>"]
        )
        all_records = [proposal, high_rank_2, high_rank_1, additional, other]
        return (
            [proposal],
            [other, additional, high_rank_2, high_rank_1],
            [self._candidate(record) for record in all_records],
            [self._recheck(record) for record in all_records],
        )

    def test_order_grouping_rank_collapsed_badge_labels_and_escape(self):
        proposal, human, candidates, rechecks = self._fixture()
        document, summary = render_sales_index(
            proposal,
            human,
            candidates,
            rechecks,
            self.preview_dir,
            self.output_base,
            "20260820",
        )

        positions = [
            document.index(f'data-section="{section}"')
            for section in ("proposal_ready", "high_initial", "high_additional", "other")
        ]
        self.assertEqual(sorted(positions), positions)
        self.assertIn('<details class="sales-section high_additional"', document)
        self.assertIn('<details class="sales-section other"', document)
        self.assertLess(document.index('data-resource-id="resource-h1"'), document.index('data-resource-id="resource-h2"'))
        self.assertIn("役割・PL・顧客調整", document)
        self.assertIn("工程", document)
        self.assertIn("その他確認項目", document)
        self.assertNotIn("unknown-raw-token", document)
        self.assertNotIn("<script>alert(1)</script>", document)
        self.assertNotIn("<img src=x>", document)
        self.assertIn("案件 &lt;script&gt;alert(1)&lt;/script&gt;", document)
        self.assertEqual(2, document.count("[前回も候補: 20260819]"))
        self.assertNotIn("前回提案済", document)
        self.assertNotIn("確認済み", document)
        self.assertIn("80%", document)
        self.assertIn("1 / 2", document)
        self.assertIn("proposal_ready 1人 ／ HIGH 3人 ／ OTHER 1人", document)
        self.assertEqual(1, summary["proposal_ready"])
        self.assertEqual(2, summary["high_initial"])
        self.assertEqual(1, summary["high_additional"])
        self.assertEqual(1, summary["other"])
        self.assertEqual(3, summary["initial"])
        self.assertEqual(5, summary["total"])
        self.assertEqual(0, summary["duplicate"])
        self.assertEqual(0, summary["candidate_loss"])
        self.assertEqual(20, summary["draft_link_count"])
        self.assertEqual(1, summary["previous_proposal"])
        self.assertEqual(1, summary["previous_high_initial"])
        self.assertEqual(2, summary["previous_initial"])

    def test_missing_or_escaping_draft_ref_fails(self):
        proposal, human, candidates, rechecks = self._fixture()
        proposal[0]["draft_refs"][0]["preview_file_path"] = "reply_preview_20260820/../outside.txt"
        with self.assertRaises(ValueError):
            render_sales_index(
                proposal,
                human,
                candidates,
                rechecks,
                self.preview_dir,
                self.output_base,
                "20260820",
            )

    def test_candidate_loss_fails(self):
        proposal, human, candidates, rechecks = self._fixture()
        with self.assertRaises(ValueError):
            render_sales_index(
                proposal,
                human,
                candidates[:-1],
                rechecks,
                self.preview_dir,
                self.output_base,
                "20260820",
            )


if __name__ == "__main__":
    unittest.main()

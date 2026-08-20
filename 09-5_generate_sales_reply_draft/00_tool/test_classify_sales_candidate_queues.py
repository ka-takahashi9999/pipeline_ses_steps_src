"""09-5 pair queue分類のfocused test。"""

import copy
import sys
import unittest
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOL_DIR))

from classify_sales_candidate_queues import (
    classify_candidate_pairs,
    has_explicit_review_reason,
)


def candidate(project_id="project-1", resource_id="resource-1"):
    return {
        "project_message_id": project_id,
        "resource_message_id": resource_id,
        "pair_file_name": f"{project_id}_{resource_id}.json",
        "score_band": "high",
    }


def recheck(
    project_id="project-1",
    resource_id="resource-1",
    status="required_skill_confirmed",
    confidence="confirmed",
    category_match="match",
    evidence="Java",
    reason="fixture",
    original_match=True,
):
    confirmed = 1 if confidence == "confirmed" else 0
    human_review = 1 if confidence == "human_review" else 0
    not_confirmed = 1 if confidence == "not_confirmed" else 0
    return {
        "project_info": {"message_id": project_id},
        "resource_info": {"message_id": resource_id},
        "recheck_info": {
            "recheck_status": status,
            "required_skill_count": 1,
            "confirmed_count": confirmed,
            "human_review_count": human_review,
            "not_confirmed_count": not_confirmed,
        },
        "required_skill_checks": [
            {
                "skill": "JavaまたはKotlin",
                "confidence": confidence,
                "reason": reason,
                "evidence": evidence,
                "original_match": original_match,
            }
        ],
        "category_match": category_match,
        "category_note": "fixture",
    }


def draft(direction, needs_review=False, notes=None, project_id="project-1", resource_id="resource-1"):
    return {
        "project_message_id": project_id,
        "resource_message_id": resource_id,
        "pair_file_name": f"{project_id}_{resource_id}.json",
        "draft_direction": direction,
        "mail_mode": direction,
        "reply_subject": "subject",
        "draft_mail_text": "draft",
        "refined_mail_text": "refined",
        "to_recipients": ["sales@example.com"],
        "preview_file_path": f"preview/{direction}.txt",
        "note_file_path": f"preview/note/{direction}.txt",
        "needs_human_review": needs_review,
        "review_notes": list(notes or []),
    }


def both_drafts():
    return [draft("reply_to_project"), draft("reply_to_resource")]


class SalesCandidateQueueClassifierTest(unittest.TestCase):
    def classify(self, candidates=None, drafts=None, rechecks=None, errors=None):
        return classify_candidate_pairs(
            candidates or [candidate()],
            drafts if drafts is not None else both_drafts(),
            rechecks or [recheck()],
            errors or [],
        )

    def test_matching_strict_and_both_sales_ready_is_proposal_ready(self):
        proposal, human = self.classify()
        self.assertEqual(1, len(proposal))
        self.assertEqual([], human)
        self.assertTrue(proposal[0]["matching_strict"])
        self.assertTrue(proposal[0]["sales_ready"])
        self.assertTrue(proposal[0]["evidence_ready"])

    def test_empty_string_evidence_is_human_review(self):
        proposal, human = self.classify(rechecks=[recheck(evidence="")])
        self.assertEqual([], proposal)
        self.assertFalse(human[0]["evidence_ready"])
        self.assertIn("matching_evidence_empty", human[0]["review_reasons"])

    def test_null_evidence_is_human_review(self):
        proposal, human = self.classify(rechecks=[recheck(evidence=None)])
        self.assertEqual([], proposal)
        self.assertIn("matching_evidence_empty", human[0]["review_reasons"])

    def test_whitespace_evidence_is_human_review(self):
        proposal, human = self.classify(rechecks=[recheck(evidence="   ")])
        self.assertEqual([], proposal)
        self.assertIn("matching_evidence_empty", human[0]["review_reasons"])

    def test_explicit_review_reason_is_human_review(self):
        for reason in (
            "営業確認前提",
            "要確認",
            "確認必要",
            "確認が必要",
            "未確認",
            "不明",
            "根拠なし",
            "記載なし",
        ):
            with self.subTest(reason=reason):
                proposal, human = self.classify(rechecks=[recheck(reason=reason)])
                self.assertEqual([], proposal)
                self.assertFalse(human[0]["evidence_ready"])
                self.assertIn(
                    "matching_evidence_review_required", human[0]["review_reasons"]
                )

    def test_limited_unknown_is_explicit_review_reason_and_human_review(self):
        for reason in (
            "限定的な不明",
            "一部条件は限定的な不明",
        ):
            with self.subTest(reason=reason):
                self.assertTrue(has_explicit_review_reason(reason))
                proposal, human = self.classify(rechecks=[recheck(reason=reason)])
                self.assertEqual([], proposal)
                self.assertFalse(human[0]["evidence_ready"])
                self.assertIn(
                    "matching_evidence_review_required", human[0]["review_reasons"]
                )

    def test_positive_confirmation_phrase_is_not_review_reason(self):
        for reason in (
            "確認済み",
            "確認できた",
            "要件を確認済み",
            "スキルシートで確認できた",
            "不明点を確認済み",
            "不明点は確認済み",
            "不明点は解消済み",
        ):
            with self.subTest(reason=reason):
                self.assertFalse(has_explicit_review_reason(reason))
                proposal, human = self.classify(rechecks=[recheck(reason=reason)])
                self.assertEqual(1, len(proposal))
                self.assertEqual([], human)

    def test_one_empty_check_makes_pair_human_review(self):
        record = recheck()
        record["required_skill_checks"].append(
            {
                "skill": "AWS",
                "confidence": "confirmed",
                "reason": "fixture",
                "evidence": " ",
                "original_match": True,
            }
        )
        record["recheck_info"].update(
            {"required_skill_count": 2, "confirmed_count": 2}
        )
        proposal, human = self.classify(rechecks=[record])
        self.assertEqual([], proposal)
        self.assertIn("matching_evidence_empty", human[0]["review_reasons"])

    def test_original_match_false_with_strong_evidence_stays_proposal_ready(self):
        proposal, human = self.classify(
            rechecks=[
                recheck(
                    original_match=False,
                    evidence="Javaで詳細設計を5年間担当",
                    reason="Javaの詳細設計経験を確認できた",
                )
            ]
        )
        self.assertEqual(1, len(proposal))
        self.assertEqual([], human)

    def test_category_unclear_is_human_review(self):
        proposal, human = self.classify(rechecks=[recheck(category_match="unclear")])
        self.assertEqual([], proposal)
        self.assertIn("category_unclear", human[0]["review_reasons"])

    def test_08_5_human_review_is_human_review(self):
        proposal, human = self.classify(
            rechecks=[recheck(status="required_skill_human_review", confidence="human_review")]
        )
        self.assertEqual([], proposal)
        self.assertIn("required_skill_review_required", human[0]["review_reasons"])

    def test_08_5_error_is_human_review(self):
        proposal, human = self.classify(errors=[recheck()])
        self.assertEqual([], proposal)
        self.assertTrue(human[0]["has_08_5_error"])
        self.assertIn("08_5_error", human[0]["review_reasons"])

    def test_one_direction_review_required_is_pair_human_review(self):
        drafts = [draft("reply_to_project", True, ["返信先を確認"]), draft("reply_to_resource")]
        proposal, human = self.classify(drafts=drafts)
        self.assertEqual([], proposal)
        self.assertIn("sales_review_required", human[0]["review_reasons"])

    def test_both_directions_without_review_are_proposal_ready(self):
        proposal, human = self.classify(drafts=both_drafts())
        self.assertEqual(1, len(proposal))
        self.assertEqual(0, len(human))

    def test_missing_one_direction_is_human_review(self):
        proposal, human = self.classify(drafts=[draft("reply_to_project")])
        self.assertEqual([], proposal)
        self.assertIn("draft_missing", human[0]["review_reasons"])

    def test_partition_has_no_duplicate_and_union_is_complete(self):
        candidates = [candidate("p-1", "r-1"), candidate("p-2", "r-2")]
        rechecks = [recheck("p-1", "r-1"), recheck("p-2", "r-2", category_match="unclear")]
        drafts = [
            draft("reply_to_project", project_id="p-1", resource_id="r-1"),
            draft("reply_to_resource", project_id="p-1", resource_id="r-1"),
            draft("reply_to_project", project_id="p-2", resource_id="r-2"),
            draft("reply_to_resource", project_id="p-2", resource_id="r-2"),
        ]
        proposal, human = self.classify(candidates, drafts, rechecks)
        proposal_keys = {(row["project_message_id"], row["resource_message_id"]) for row in proposal}
        human_keys = {(row["project_message_id"], row["resource_message_id"]) for row in human}
        self.assertEqual(2, len(proposal_keys | human_keys))
        self.assertFalse(proposal_keys & human_keys)
        self.assertEqual(len(proposal), len(proposal_keys))
        self.assertEqual(len(human), len(human_keys))

    def test_not_confirmed_is_not_resurrected_as_human_review(self):
        with self.assertRaisesRegex(ValueError, "not_confirmed"):
            self.classify(
                rechecks=[
                    recheck(
                        status="required_skill_not_confirmed",
                        confidence="not_confirmed",
                    )
                ]
            )

    def test_category_mismatch_never_becomes_proposal_ready(self):
        proposal, human = self.classify(rechecks=[recheck(category_match="mismatch")])
        self.assertEqual([], proposal)
        self.assertIn("category_mismatch", human[0]["review_reasons"])

    def test_empty_draft_body_is_human_review(self):
        drafts = both_drafts()
        drafts[1]["refined_mail_text"] = ""
        proposal, human = self.classify(drafts=drafts)
        self.assertEqual([], proposal)
        self.assertIn("draft_missing", human[0]["review_reasons"])

    def test_canonical_draft_records_are_not_modified(self):
        candidates = [candidate()]
        drafts = both_drafts()
        rechecks = [recheck()]
        before = copy.deepcopy((candidates, drafts, rechecks))
        self.classify(candidates=candidates, drafts=drafts, rechecks=rechecks)
        self.assertEqual(before, (candidates, drafts, rechecks))


if __name__ == "__main__":
    unittest.main()

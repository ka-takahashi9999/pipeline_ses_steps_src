"""09-5 pair queue分類のfocused test。"""

import copy
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

TOOL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOL_DIR))

import classify_sales_candidate_queues as target
from classify_sales_candidate_queues import (
    classify_candidate_pairs,
    generate_candidate_queues,
    has_explicit_review_reason,
)


def candidate(
    project_id="project-1",
    resource_id="resource-1",
    previous_candidate=False,
    previous_candidate_date="",
):
    return {
        "project_message_id": project_id,
        "resource_message_id": resource_id,
        "pair_file_name": f"{project_id}_{resource_id}.json",
        "score_band": "high",
        "previous_candidate": previous_candidate,
        "previous_candidate_date": previous_candidate_date,
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
    required_score=1.0,
    total_score=2.0,
    skillsheet_chars=100,
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
            "skillsheet_chars_used": skillsheet_chars,
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
        "match_info": {
            "required_skills_match_rate": required_score,
            "total_skills_match_rate": total_score,
        },
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


def priority_recheck(
    project_id,
    resource_id,
    confirmed_count=0,
    required_score=1.0,
    total_score=2.0,
):
    record = recheck(
        project_id=project_id,
        resource_id=resource_id,
        status="required_skill_human_review",
        confidence="human_review",
        required_score=required_score,
        total_score=total_score,
    )
    confirmed_checks = [
        {
            "skill": f"confirmed-{index}",
            "confidence": "confirmed",
            "reason": "確認済み",
            "evidence": "evidence",
            "original_match": True,
        }
        for index in range(confirmed_count)
    ]
    record["required_skill_checks"] = confirmed_checks + record["required_skill_checks"]
    record["recheck_info"].update(
        {
            "required_skill_count": confirmed_count + 1,
            "confirmed_count": confirmed_count,
            "human_review_count": 1,
        }
    )
    return record


def pair_drafts(project_id, resource_id, notes=None):
    needs_review = bool(notes)
    return [
        draft(
            "reply_to_project",
            needs_review,
            notes,
            project_id=project_id,
            resource_id=resource_id,
        ),
        draft(
            "reply_to_resource",
            needs_review,
            notes,
            project_id=project_id,
            resource_id=resource_id,
        ),
    ]


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
        self.assertNotIn("review_priority", proposal[0])
        self.assertNotIn("normalized_review_items", proposal[0])

    def test_generic_sales_reason_from_required_skill_is_one_normalized_item(self):
        drafts = pair_drafts(
            "project-1",
            "resource-1",
            ["必須スキルに人間確認項目があります"],
        )
        proposal, human = self.classify(
            drafts=drafts,
            rechecks=[
                recheck(
                    status="required_skill_human_review",
                    confidence="human_review",
                )
            ],
        )
        self.assertEqual([], proposal)
        self.assertEqual(1, human[0]["normalized_review_item_count"])
        self.assertEqual(["technology_semantic"], human[0]["normalized_review_items"])

    def test_all_high_conditions_make_high_priority(self):
        proposal, human = self.classify(
            rechecks=[
                recheck(
                    status="required_skill_human_review",
                    confidence="human_review",
                    required_score=0.8,
                )
            ]
        )
        self.assertEqual([], proposal)
        self.assertEqual("HIGH", human[0]["review_priority"])
        self.assertEqual(1, human[0]["high_project_rank"])
        self.assertTrue(human[0]["initial_review"])

    def test_two_normalized_items_make_other_priority(self):
        drafts = pair_drafts(
            "project-1",
            "resource-1",
            [
                "必須スキルに人間確認項目があります",
                "案件本文に開始時期シグナルがありますがproject_start_dateを抽出できませんでした",
            ],
        )
        proposal, human = self.classify(
            drafts=drafts,
            rechecks=[
                recheck(
                    status="required_skill_human_review",
                    confidence="human_review",
                )
            ],
        )
        self.assertEqual([], proposal)
        self.assertEqual(2, human[0]["normalized_review_item_count"])
        self.assertEqual("OTHER", human[0]["review_priority"])

    def test_empty_string_evidence_is_human_review(self):
        proposal, human = self.classify(rechecks=[recheck(evidence="")])
        self.assertEqual([], proposal)
        self.assertFalse(human[0]["evidence_ready"])
        self.assertIn("matching_evidence_empty", human[0]["review_reasons"])
        self.assertEqual("OTHER", human[0]["review_priority"])

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
        self.assertEqual("OTHER", human[0]["review_priority"])

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
        self.assertEqual("OTHER", human[0]["review_priority"])

    def test_missing_skillsheet_is_other_priority(self):
        proposal, human = self.classify(
            rechecks=[
                recheck(
                    status="required_skill_human_review",
                    confidence="human_review",
                    skillsheet_chars=0,
                )
            ]
        )
        self.assertEqual([], proposal)
        self.assertIn("skillsheet", human[0]["normalized_review_items"])
        self.assertEqual("OTHER", human[0]["review_priority"])

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
        self.assertEqual("OTHER", human[0]["review_priority"])

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

    def test_high_ranking_uses_confirmed_rate_then_required_and_total_score(self):
        project_id = "project-ranking"
        resources = ["resource-rate", "resource-required", "resource-total"]
        candidates = [candidate(project_id, resource_id) for resource_id in resources]
        rechecks = [
            priority_recheck(project_id, "resource-rate", 3, 0.8, 1.0),
            priority_recheck(project_id, "resource-required", 1, 1.0, 1.0),
            priority_recheck(project_id, "resource-total", 1, 0.8, 2.0),
        ]
        drafts = [
            row
            for resource_id in resources
            for row in pair_drafts(project_id, resource_id)
        ]
        proposal, human = self.classify(candidates, drafts, rechecks)
        ranks = {row["resource_message_id"]: row["high_project_rank"] for row in human}
        self.assertEqual([], proposal)
        self.assertEqual(
            {"resource-rate": 1, "resource-required": 2, "resource-total": 3},
            ranks,
        )

    def test_high_ranking_tie_is_stable_by_resource_message_id(self):
        project_id = "project-tie"
        resources = ["resource-c", "resource-a", "resource-b"]
        candidates = [candidate(project_id, resource_id) for resource_id in resources]
        rechecks = [priority_recheck(project_id, resource_id) for resource_id in resources]
        drafts = [
            row
            for resource_id in resources
            for row in pair_drafts(project_id, resource_id)
        ]
        _, human = self.classify(candidates, drafts, rechecks)
        ordered = sorted(human, key=lambda row: row["high_project_rank"])
        self.assertEqual(sorted(resources), [row["resource_message_id"] for row in ordered])

    def test_five_high_candidates_rank_all_and_initial_review_only_top_three(self):
        project_id = "project-five"
        resources = [f"resource-{index}" for index in range(5, 0, -1)]
        candidates = [candidate(project_id, resource_id) for resource_id in resources]
        rechecks = [priority_recheck(project_id, resource_id) for resource_id in resources]
        drafts = [
            row
            for resource_id in resources
            for row in pair_drafts(project_id, resource_id)
        ]
        _, human = self.classify(candidates, drafts, rechecks)
        self.assertEqual([1, 2, 3, 4, 5], sorted(row["high_project_rank"] for row in human))
        self.assertEqual(3, sum(row["initial_review"] for row in human))

    def test_canonical_draft_records_are_not_modified(self):
        candidates = [candidate()]
        drafts = both_drafts()
        rechecks = [recheck()]
        before = copy.deepcopy((candidates, drafts, rechecks))
        self.classify(candidates=candidates, drafts=drafts, rechecks=rechecks)
        self.assertEqual(before, (candidates, drafts, rechecks))

    def test_previous_candidate_fields_propagate_to_proposal_ready(self):
        proposal, human = self.classify(
            candidates=[candidate(previous_candidate=True, previous_candidate_date="20260819")]
        )
        self.assertEqual([], human)
        self.assertTrue(proposal[0]["previous_candidate"])
        self.assertEqual("20260819", proposal[0]["previous_candidate_date"])

    def test_previous_candidate_fields_propagate_to_human_review(self):
        proposal, human = self.classify(
            candidates=[candidate(previous_candidate=True, previous_candidate_date="20260819")],
            rechecks=[recheck(category_match="unclear")],
        )
        self.assertEqual([], proposal)
        self.assertTrue(human[0]["previous_candidate"])
        self.assertEqual("20260819", human[0]["previous_candidate_date"])

    def test_empty_subject_candidate_uses_common_identity_and_continues(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            candidate_dir = root / "candidates"
            result_dir = root / "01_result"
            candidate_dir.mkdir()
            result_dir.mkdir()
            current = candidate()
            current.update({
                "project_from": "p@example.com",
                "project_subject": "案件",
                "resource_from": "r@example.com",
                "resource_subject": "",
                "pair_file_name": "project-1_resource-1_前回出力済.json",
            })
            previous = dict(current)
            paths_and_rows = (
                (candidate_dir / "sales_proposal_candidates_20260915.jsonl", [previous]),
                (candidate_dir / "sales_proposal_candidates_20260916.jsonl", [current]),
                (result_dir / "generate_sales_reply_draft_20260916.jsonl", both_drafts()),
                (root / "recheck.jsonl", [recheck()]),
            )
            for path, rows in paths_and_rows:
                path.write_text(
                    "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
                    encoding="utf-8",
                )

            class CaptureLogger:
                def __init__(self):
                    self.warnings = []
                    self.infos = []
                def warn(self, message):
                    self.warnings.append(message)
                def info(self, message):
                    self.infos.append(message)
                def ok(self, _message):
                    pass
                def error(self, _message):
                    pass

            logger = CaptureLogger()
            with patch.object(target, "INPUT_09_4_DIR", candidate_dir), \
                 patch.object(target, "STEP_DIR", root), \
                 patch.object(target, "RECHECK_ALL_PATH", root / "recheck.jsonl"), \
                 patch.object(target, "RECHECK_ERROR_PATH", root / "no_errors.jsonl"), \
                 patch.object(target, "get_logger", return_value=logger):
                summary = generate_candidate_queues("20260916")
            proposal = [
                json.loads(line)
                for line in (result_dir / "proposal_ready_20260916.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(summary["subject_fallback_count"], 2)
            self.assertEqual(summary["proposal_ready"], 1)
            self.assertTrue(proposal[0]["previous_candidate"])
            self.assertEqual(current["resource_subject"], "")
            self.assertEqual(previous["resource_subject"], "")
            self.assertEqual(len(logger.warnings), 2)
            self.assertTrue(all("side=resource" in message and "resource-1" in message for message in logger.warnings))
            self.assertTrue(any("fallback件数=2 project=0 resource=2" in message for message in logger.infos))

            confirm_path = TOOL_DIR.parents[0] / "02_confirm/confirm_sales_candidate_queues.py"
            spec = importlib.util.spec_from_file_location("isolated_09_5_confirm", confirm_path)
            confirm = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(confirm)
            result_path = root / "confirm_result.txt"
            with patch.object(confirm, "INPUT_09_4_DIR", candidate_dir), \
                 patch.object(confirm, "STEP_DIR", root), \
                 patch.object(confirm, "RECHECK_ALL_PATH", root / "recheck.jsonl"), \
                 patch.object(confirm, "RECHECK_ERROR_PATH", root / "no_errors.jsonl"), \
                 patch.object(confirm, "CONFIRM_RESULT", result_path), \
                 patch.object(confirm, "get_logger", return_value=logger), \
                 patch.object(sys, "argv", ["confirm_sales_candidate_queues.py", "--target-date", "20260916"]):
                try:
                    self.assertIsNone(confirm.main())
                except SystemExit:
                    failed_lines = [
                        line for line in result_path.read_text(encoding="utf-8").splitlines()
                        if "[NG]" in line
                    ]
                    self.fail("isolated confirm NG: " + " / ".join(failed_lines))
            self.assertIn("【結果】OK", result_path.read_text(encoding="utf-8"))

    def test_success_cache_marker_pair_file_name_is_preserved(self):
        marked = candidate()
        marked["pair_file_name"] = "pair_前回出力済.json"
        proposal, human = self.classify(candidates=[marked])
        self.assertEqual([], human)
        self.assertEqual("pair_前回出力済.json", proposal[0]["pair_file_name"])


if __name__ == "__main__":
    unittest.main()

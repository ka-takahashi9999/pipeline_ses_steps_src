"""CHECK-07のleader/API-IF直接証拠救済guard focused tests。"""

import copy
import sys
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parents[1]
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

import batch_minimal_safety_guard as guard
import high_score_required_skill_recheck as legacy


def check(skill, confidence="not_confirmed"):
    return {
        "skill": skill,
        "original_match": False,
        "recheck_match": confidence != "not_confirmed",
        "confidence": confidence,
        "reason": "fixture",
        "evidence": "",
    }


def apply(skill, skillsheet, confidence="not_confirmed"):
    checks, category, metadata = guard.apply_minimal_safety_guard(
        [check(skill, confidence)], "match", True, skillsheet
    )
    return checks[0], category, metadata


class LeaderDirectEvidenceRescueTest(unittest.TestCase):
    def test_project_leader_is_human_review(self):
        result, _, metadata = apply(
            "リーダー経験", "実質的なプロジェクトリーダーとして進行管理を担当"
        )
        self.assertEqual("human_review", result["confidence"])
        self.assertIn("leader_direct_evidence_rescue:1", metadata["guard_reasons"])

    def test_practical_lead_and_progress_management_is_human_review(self):
        result, _, _ = apply(
            "リーダー経験", "役割: リーダーとして実質リード、進行管理とタスク管理を担当"
        )
        self.assertEqual("human_review", result["confidence"])

    def test_team_participation_is_not_rescued(self):
        result, _, _ = apply("リーダー経験", "5名チームへメンバーとして参加")
        self.assertEqual("not_confirmed", result["confidence"])

    def test_pl_is_counterparty_not_role_is_not_rescued(self):
        result, _, _ = apply(
            "リーダー経験", "PL・SEとの仕様確認、担当メンバーとして実装"
        )
        self.assertEqual("not_confirmed", result["confidence"])

    def test_year_requirement_is_not_rescued(self):
        result, _, _ = apply("PL経験3年以上", "役割: PLとして1年")
        self.assertEqual("not_confirmed", result["confidence"])


class ApiIfDirectEvidenceRescueTest(unittest.TestCase):
    def test_common_if_design_is_human_review(self):
        result, _, metadata = apply("API/IF設計の経験", "共通IF設計とインターフェース定義を担当")
        self.assertEqual("human_review", result["confidence"])
        self.assertIn("api_if_design_direct_evidence_rescue:1", metadata["guard_reasons"])

    def test_api_spec_design_is_human_review(self):
        result, _, _ = apply("API設計経験", "REST API仕様設計を担当")
        self.assertEqual("human_review", result["confidence"])

    def test_api_use_or_implementation_is_not_rescued(self):
        for skillsheet in ("外部APIを利用", "Web API実装とAPIテストを担当"):
            with self.subTest(skillsheet=skillsheet):
                result, _, _ = apply("API/IF設計の経験", skillsheet)
                self.assertEqual("not_confirmed", result["confidence"])

    def test_year_requirement_is_not_rescued(self):
        result, _, _ = apply("API設計経験3年以上", "API仕様設計を1年担当")
        self.assertEqual("not_confirmed", result["confidence"])


class RescueRegressionTest(unittest.TestCase):
    def test_confirmed_and_human_review_are_not_changed(self):
        for confidence in ("confirmed", "human_review"):
            with self.subTest(confidence=confidence):
                result, _, metadata = apply(
                    "API設計経験", "API仕様設計を担当", confidence
                )
                self.assertEqual(confidence, result["confidence"])
                self.assertNotIn(
                    "api_if_design_direct_evidence_rescue:1",
                    metadata["guard_reasons"],
                )

    def test_legacy_and_replay_use_same_rescue(self):
        source = {
            "required_skill_checks": [check("API/IF設計の経験")],
            "recheck_info": {
                "recheck_status": legacy.STATUS_NOT_CONFIRMED,
                "required_skill_count": 1,
                "confirmed_count": 0,
                "human_review_count": 0,
                "not_confirmed_count": 1,
            },
            "category_match": "match",
        }
        original = copy.deepcopy(source)
        guarded, metadata = legacy.apply_legacy_safety_guard_to_result(
            source, "共通IF設計を担当"
        )
        self.assertEqual(original, source)
        self.assertEqual(legacy.STATUS_HUMAN_REVIEW, guarded["recheck_info"]["recheck_status"])
        self.assertIn("api_if_design_direct_evidence_rescue:1", metadata["guard_reasons"])

        replayed, _, _ = legacy._replay_postprocessing_record(
            source, "共通IF設計を担当"
        )
        self.assertEqual(legacy.STATUS_HUMAN_REVIEW, replayed["recheck_info"]["recheck_status"])


if __name__ == "__main__":
    unittest.main()

"""CHECK-06年数矛盾/Git compound局所guardのfocused tests。"""

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


def _check(skill, evidence="fixture", confidence="confirmed"):
    return {
        "skill": skill,
        "original_match": True,
        "recheck_match": confidence != "not_confirmed",
        "confidence": confidence,
        "reason": "fixture reason",
        "evidence": evidence,
    }


def _apply(skill, skillsheet, evidence="fixture", confidence="confirmed"):
    checks, metadata = guard.apply_local_false_positive_guard(
        [_check(skill, evidence, confidence)], skillsheet
    )
    return checks[0], metadata


class ExplicitYearsContradictionGuardTest(unittest.TestCase):
    def test_two_years_required_one_year_is_human_review(self):
        check, metadata = _apply(
            "TypeScriptを用いた開発経験2年以上",
            "TypeScript | 実務経験あり | 1年0ヶ月",
            "TypeScript 1年0ヶ月",
        )
        self.assertEqual("human_review", check["confidence"])
        self.assertIn("explicit_years_contradicted:1", metadata["guard_reasons"])

    def test_equal_or_longer_duration_keeps_confirmed(self):
        for duration in ("2年0ヶ月", "3年"):
            with self.subTest(duration=duration):
                check, _ = _apply(
                    "TypeScriptを用いた開発経験2年以上",
                    f"TypeScript | 実務経験あり | {duration}",
                    f"TypeScript {duration}",
                )
                self.assertEqual("confirmed", check["confidence"])

    def test_missing_duration_does_not_downgrade(self):
        check, metadata = _apply(
            "TypeScriptを用いた開発経験2年以上",
            "TypeScriptで開発を担当",
            "TypeScript開発経験",
        )
        self.assertEqual("confirmed", check["confidence"])
        self.assertEqual([], metadata["guard_reasons"])

    def test_other_technology_duration_is_not_compared(self):
        check, _ = _apply(
            "Java開発経験2年以上",
            "JavaScript 1年\nTypeScript 5年\nJavaは使用期間不明",
            "Java開発経験",
        )
        self.assertEqual("confirmed", check["confidence"])


class GitCompoundGuardTest(unittest.TestCase):
    STRICT = "Git/GitHubを利用したチーム開発経験(プルリクエストでのレビュー運用)"
    SIMPLE = "Gitを用いたチーム開発経験"

    def test_github_copilot_only_is_human_review(self):
        check, _ = _apply(self.SIMPLE, "10名チーム\nGitHub Copilot", "GitHub Copilot")
        self.assertEqual("human_review", check["confidence"])

    def test_team_lead_only_is_human_review(self):
        check, _ = _apply(self.SIMPLE, "約10名チームのテックリード", "テックリード")
        self.assertEqual("human_review", check["confidence"])

    def test_git_without_pull_request_is_human_review_for_strict_required(self):
        check, _ = _apply(self.STRICT, "Gitによるソース管理\n5名チーム開発", "Git")
        self.assertEqual("human_review", check["confidence"])

    def test_simple_git_without_team_evidence_is_human_review(self):
        check, _ = _apply(self.SIMPLE, "Gitによるソース管理", "Git")
        self.assertEqual("human_review", check["confidence"])

    def test_simple_git_with_structured_team_size_keeps_confirmed(self):
        check, _ = _apply(
            self.SIMPLE,
            "番 | 作業概要 | 役割 | 担当工程\n人数\nGitを利用したソース管理",
            "Gitを利用したソース管理",
        )
        self.assertEqual("confirmed", check["confidence"])

    def test_github_copilot_required_is_not_source_control_compound(self):
        check, metadata = _apply(
            "GitHub Copilotを活用したチーム開発経験",
            "5名チームでGitHub Copilotを利用",
            "GitHub Copilot",
        )
        self.assertEqual("confirmed", check["confidence"])
        self.assertEqual([], metadata["guard_reasons"])

    def test_git_team_pull_request_keeps_confirmed(self):
        check, _ = _apply(
            self.STRICT,
            "GitHubによるソース管理\n5名チーム開発\nPull Requestでcode review",
            "GitHub / Pull Request / code review",
        )
        self.assertEqual("confirmed", check["confidence"])

    def test_gitlab_team_merge_request_keeps_confirmed(self):
        check, _ = _apply(
            self.STRICT,
            "GitLabによるソース管理\nScrumチーム\nMerge Requestのレビュー運用",
            "GitLab / Merge Request",
        )
        self.assertEqual("confirmed", check["confidence"])


class LocalGuardRegressionTest(unittest.TestCase):
    def test_non_target_and_existing_non_confirmed_are_unchanged(self):
        fixtures = [
            (_check("コミュニケーション良好"), ""),
            (_check("AWS業務経験", "AWS資格"), "AWSソリューションアーキテクト"),
            (_check("Gitを用いたチーム開発経験", confidence="human_review"), ""),
            (_check("Gitを用いたチーム開発経験", confidence="not_confirmed"), ""),
            (_check("Cisco8000シリーズの設計構築", "Cisco一般", "human_review"), "Cisco Catalyst"),
            (_check("Cisco SD-WANの構築", "WAN一般", "not_confirmed"), "WAN設計"),
        ]
        for original, skillsheet in fixtures:
            with self.subTest(skill=original["skill"], confidence=original["confidence"]):
                checks, metadata = guard.apply_local_false_positive_guard(
                    [original], skillsheet
                )
                self.assertEqual([original], checks)
                self.assertEqual([], metadata["guard_reasons"])

    def test_legacy_wrapper_refreshes_status_without_mutating_input(self):
        result = {
            "required_skill_checks": [
                _check("TypeScriptを用いた開発経験2年以上", "TypeScript 1年")
            ],
            "recheck_info": {
                "recheck_status": legacy.STATUS_CONFIRMED,
                "required_skill_count": 1,
                "confirmed_count": 1,
                "human_review_count": 0,
                "not_confirmed_count": 0,
            },
        }
        original = copy.deepcopy(result)
        guarded, metadata = legacy.apply_legacy_safety_guard_to_result(
            result, "TypeScript | 1年0ヶ月"
        )
        self.assertEqual(original, result)
        self.assertEqual(legacy.STATUS_HUMAN_REVIEW, guarded["recheck_info"]["recheck_status"])
        self.assertEqual(0, guarded["recheck_info"]["confirmed_count"])
        self.assertEqual(1, guarded["recheck_info"]["human_review_count"])
        self.assertIn("explicit_years_contradicted:1", metadata["guard_reasons"])

    def test_postprocess_replay_uses_same_local_guard(self):
        result = {
            "required_skill_checks": [
                _check("TypeScriptを用いた開発経験2年以上", "TypeScript 1年")
            ],
            "recheck_info": {
                "recheck_status": legacy.STATUS_CONFIRMED,
                "required_skill_count": 1,
                "confirmed_count": 1,
                "human_review_count": 0,
                "not_confirmed_count": 0,
            },
            "category_match": "match",
        }
        replayed, _, _ = legacy._replay_postprocessing_record(
            result, "TypeScript | 1年0ヶ月"
        )
        self.assertEqual(
            legacy.STATUS_HUMAN_REVIEW,
            replayed["recheck_info"]["recheck_status"],
        )

    def test_batch_guard_uses_same_compound_guard(self):
        checks, _, metadata = guard.apply_minimal_safety_guard(
            [_check("Gitを用いたチーム開発経験", "GitHub Copilot")],
            "match",
            True,
            "5名チーム\nGitHub Copilot",
        )
        self.assertEqual("human_review", checks[0]["confidence"])
        self.assertIn("git_compound_evidence_missing:1", metadata["guard_reasons"])


if __name__ == "__main__":
    unittest.main()

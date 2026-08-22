"""08-5 Batch minimal safety guardのfocused tests。"""

import copy
import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("_test_batch_minimal_safety_guard.py")
SPEC = importlib.util.spec_from_file_location("batch_minimal_safety_guard", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("Batch minimal safety guard module import failed")
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)


class BatchMinimalSafetyGuardUnitTest(unittest.TestCase):
    @staticmethod
    def _shadow(skill, confidence="confirmed", category_match="match"):
        return {
            "ordinal": 1,
            "custom_id": "fixture-1",
            "project_message_id": "project-1",
            "resource_message_id": "resource-1",
            "schema_valid": True,
            "category_match": category_match,
            "required_skill_checks": [
                {
                    "skill": skill,
                    "confidence": confidence,
                    "reason": "fixture",
                    "evidence": "対象経験の記載あり",
                }
            ],
            "saved_direct_result": {
                "recheck_status": "required_skill_human_review",
                "category_match": "match",
            },
        }

    @staticmethod
    def _manifest(skill):
        return {"required_skill_texts": [skill]}

    def _apply(self, skill, skillsheet, confidence="confirmed", category_match="match"):
        return guard.apply_guard_to_pair(
            self._shadow(skill, confidence, category_match),
            self._manifest(skill),
            skillsheet,
        )

    def test_welcome_condition_not_confirmed_becomes_human_review(self):
        result = self._apply("勘定系経験者は特に歓迎", "金融システム経験", "not_confirmed")
        self.assertEqual("human_review", result["after"]["status"])
        self.assertIn("optional_condition:1", result["guard_reasons"])

    def test_preferred_condition_not_confirmed_becomes_human_review(self):
        result = self._apply("AWS経験があれば尚可", "Linux構築経験", "not_confirmed")
        self.assertEqual("human_review", result["after"]["status"])
        self.assertIn("optional_condition:1", result["guard_reasons"])

    def test_schema_invalid_category_mismatch_becomes_human_review_unclear(self):
        shadow = self._shadow("AWS構築経験", "not_confirmed", "mismatch")
        shadow["schema_valid"] = False
        shadow["required_skill_checks"] = [
            {
                "skill": "不正な別skill",
                "confidence": "not_confirmed",
                "reason": "invalid response",
                "evidence": "",
            }
        ]
        result = guard.apply_guard_to_pair(
            shadow, self._manifest("AWS構築経験"), "AWS構築経験あり"
        )
        self.assertEqual("human_review", result["before"]["status"])
        self.assertEqual("human_review", result["after"]["status"])
        self.assertEqual("unclear", result["after"]["category_match"])

    def test_two_years_react_native_one_year_three_months_blocks_confirmed(self):
        skill = "Flutter or ReactNative実装経験2年以上"
        skillsheet = (
            "1 | 2024 | 年 | 1 | 月 | モバイルアプリ | React Native\n"
            "React Nativeで実装\n規模\n1 | 年 | 3 | ヶ月\n"
            "■スキル(評価レベル)\nReact Native | B\n"
        )
        result = self._apply(skill, skillsheet)
        self.assertEqual("human_review", result["after"]["status"])
        self.assertIn("explicit_years_unproven:1", result["guard_reasons"])

    def test_two_years_flutter_six_and_react_native_seven_months_blocks_confirmed(self):
        skill = "Flutter or ReactNative実装経験2年以上"
        skillsheet = (
            "1 | 2024 | 年 | 1 | 月 | モバイルアプリ | Flutter\n"
            "Flutterで実装\n規模\n0 | 年 | 6 | ヶ月\n"
            "2 | 2024 | 年 | 7 | 月 | ゲームアプリ | React Native\n"
            "React Nativeで実装\n規模\n0 | 年 | 7 | ヶ月\n"
            "■スキル(評価レベル)\nFlutter | B\nReact Native | B\n"
        )
        result = self._apply(skill, skillsheet)
        self.assertEqual("human_review", result["after"]["status"])

    def test_explicit_duration_satisfied_is_not_downgraded(self):
        skill = "React Native実装経験2年以上"
        skillsheet = (
            "1 | 2023 | 年 | 1 | 月 | モバイルアプリ | React Native\n"
            "React Nativeで実装\n規模\n2 | 年 | 3 | ヶ月\n"
            "■スキル(評価レベル)\nReact Native | B\n"
        )
        result = self._apply(skill, skillsheet)
        self.assertEqual("confirmed", result["after"]["status"])
        self.assertEqual([], result["guard_reasons"])

    def test_other_technology_duration_is_not_reused(self):
        skill = "Java開発経験2年以上"
        result = self._apply(skill, "Python経験5年\nJavaは学習経験のみ")
        self.assertEqual("human_review", result["after"]["status"])
        decision = result["explicit_years_decisions"][0]
        self.assertEqual(0, decision["proof_months_by_target"]["java"])

    def test_user_definition_requires_narrow_neighbor_facts(self):
        shadow = self._shadow("ユーザー定義", "not_confirmed", "unclear")
        shadow["required_skill_checks"].extend(
            [
                {
                    "skill": "要件定義",
                    "confidence": "confirmed",
                    "reason": "fixture",
                    "evidence": "要件定義・設計・実装・テスト",
                }
            ]
        )
        manifest = {"required_skill_texts": ["ユーザー定義", "要件定義"]}
        result = guard.apply_guard_to_pair(
            shadow,
            manifest,
            "要件定義を行い、ログ分析画面の画面設計・実装を担当",
        )
        self.assertEqual("human_review", result["after"]["status"])

    def test_fixture_objects_are_not_mutated(self):
        skill = "Java開発経験2年以上"
        shadow = self._shadow(skill)
        original = copy.deepcopy(shadow)
        guard.apply_guard_to_pair(shadow, self._manifest(skill), "Python経験5年")
        self.assertEqual(original, shadow)


class BatchMinimalSafetyGuardSavedReplayTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report = guard.replay_run(write=False)

    def test_clear_keep_five_fixtures_are_retained(self):
        self.assertEqual(0, self.report["clear_keep_before_retained"])
        self.assertEqual(5, self.report["clear_keep_after_retained"])
        self.assertTrue(
            all(
                item["after_retained"] and item["after_status"] == "human_review"
                for item in self.report["clear_keep_cases"]
            )
        )

    def test_clear_false_positive_two_fixtures_are_human_review(self):
        self.assertEqual(2, self.report["clear_false_positive_before_confirmed"])
        self.assertEqual(0, self.report["clear_false_positive_after_confirmed"])
        self.assertTrue(
            all(
                item["after_status"] == "human_review"
                and not item["after_proposal_ready"]
                for item in self.report["clear_false_positive_cases"]
            )
        )

    def test_saved_678_replay_integrity_and_baseline(self):
        self.assertEqual(678, self.report["sample_size"])
        self.assertEqual(64, self.report["before"]["candidate_loss"])
        self.assertEqual(0, self.report["proposal_ready_false_positive_after"])
        self.assertEqual(0, self.report["schema_fallback_erroneous_promotion"])
        self.assertEqual(0, self.report["new_llm_call"])
        self.assertEqual(0, self.report["production_write"])

    def test_quality_passes_with_bounded_state_changes(self):
        self.assertTrue(self.report["quality_pass"])
        self.assertLessEqual(
            self.report["affected_pairs"], guard.MAX_AFFECTED_PAIRS
        )


if __name__ == "__main__":
    unittest.main()

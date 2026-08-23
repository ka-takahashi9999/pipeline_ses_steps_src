"""07-1 test専用minimal retention guardのfocused tests。"""

import copy
import importlib.util
import unittest
from pathlib import Path


TOOL_DIR = Path(__file__).resolve().parent
TARGET_PATH = TOOL_DIR / "_test_07_1_candidate_retention_guard.py"


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


target = load_module("test_07_1_candidate_retention_guard_target", TARGET_PATH)


def skill(name="Pythonコーディング経験", match=False, note="Python経験は1ヶ月のみ"):
    return {"skill": name, "match": match, "note": note}


def record(required_skills, optional_skills=None, project="project-1", resource="resource-1"):
    return {
        "project_info": {"message_id": project},
        "resource_info": {"message_id": resource},
        "required_skills": required_skills,
        "optional_skills": optional_skills or [],
    }


class GuardConditionTest(unittest.TestCase):
    def test_no_year_requirement_and_direct_python_work_is_retained(self):
        rows = [record([skill()])]
        before = copy.deepcopy(rows)
        audits, retained = target.collect_retention_candidates(
            rows, {"resource-1": "社内業務でPythonスクレイピングを実装"}
        )
        self.assertEqual(len(audits), 1)
        self.assertEqual(len(retained), 1)
        self.assertEqual(rows, before)

    def test_explicit_three_year_requirement_is_not_retained(self):
        value = skill(name="Python 3年以上", note="Python経験は1ヶ月のみ")
        self.assertIsNone(
            target.evaluate_required_skill(value, "実務でPython機能を実装")
        )

    def test_missing_python_evidence_is_not_retained(self):
        self.assertIsNone(target.evaluate_required_skill(skill(), "担当業務の記載なし"))

    def test_other_technology_evidence_is_not_retained(self):
        self.assertIsNone(
            target.evaluate_required_skill(skill(), "業務でJava機能を実装")
        )

    def test_compound_partial_match_is_not_retained(self):
        value = skill(name="C# + SQLの実務経験", note="C#経験は1ヶ月のみ")
        self.assertIsNone(
            target.evaluate_required_skill(value, "業務システムをC#で実装")
        )

    def test_mixed_or_and_partial_match_is_not_retained(self):
        value = skill(
            name="PythonまたはJavaとSQLの開発経験",
            note="Python経験は1ヶ月のみ",
        )
        self.assertIsNone(
            target.evaluate_required_skill(value, "Pythonのみ業務で開発")
        )

    def test_and_partial_match_is_not_retained(self):
        value = skill(name="PythonとSQLの開発経験", note="Python経験は1ヶ月のみ")
        self.assertIsNone(
            target.evaluate_required_skill(value, "Pythonのみ業務で開発")
        )

    def test_rhel_windows_partial_match_is_not_duration_reason_or_retained(self):
        value = skill(name="OS:RHEL、Windows", note="Windows経験のみの記載")
        self.assertIsNone(
            target.evaluate_required_skill(value, "業務システムをWindowsで開発")
        )

    def test_multiple_middleware_partial_match_is_not_retained(self):
        value = skill(
            name="Apache、Tomcatの構築経験", note="Apache経験は6ヶ月のみ"
        )
        self.assertIsNone(
            target.evaluate_required_skill(value, "業務でApache環境を構築")
        )

    def test_concurrent_true_is_out_of_scope(self):
        value = skill(match=True, note="Python実務経験あり")
        self.assertIsNone(
            target.evaluate_required_skill(value, "業務でPython機能を実装")
        )

    def test_linux_build_concurrent_true_is_unchanged(self):
        value = skill(
            name="Linux環境の構築経験", match=True, note="Linux構築経験あり"
        )
        self.assertIsNone(
            target.evaluate_required_skill(value, "業務でLinux環境を構築")
        )

    def test_optional_skill_is_not_examined(self):
        rows = [record([], optional_skills=[skill()])]
        audits, retained = target.collect_retention_candidates(
            rows, {"resource-1": "業務でPython機能を実装"}
        )
        self.assertEqual(audits, [])
        self.assertEqual(retained, [])

    def test_guard_never_promotes_false_to_true(self):
        rows = [record([skill()])]
        before = copy.deepcopy(rows)
        target.collect_retention_candidates(
            rows, {"resource-1": "業務でPython機能を実装"}
        )
        self.assertEqual(target.count_false_to_true(before, rows), 0)
        self.assertFalse(rows[0]["required_skills"][0]["match"])

    def test_guard_does_not_write_production_files(self):
        before = target.snapshot_production_files()
        target.collect_retention_candidates(
            [record([skill()])], {"resource-1": "業務でPython機能を実装"}
        )
        self.assertEqual(target.snapshot_production_files(), before)

    def test_non_duration_false_reason_is_not_retained(self):
        value = skill(note="Python実装経験の記載なし")
        self.assertIsNone(
            target.evaluate_required_skill(value, "業務でPython機能を実装")
        )

    def test_duration_and_unclear_practical_reason_is_not_retained(self):
        value = skill(note="Python経験は1ヶ月のみで、業務経験も不明確")
        self.assertIsNone(
            target.evaluate_required_skill(value, "業務でPython機能を実装")
        )

    def test_duration_and_missing_design_reason_is_not_retained(self):
        value = skill(note="経験1ヶ月、設計経験も確認できない")
        self.assertIsNone(
            target.evaluate_required_skill(value, "業務でPython機能を実装")
        )

    def test_duration_reason_without_numeric_value_is_retained(self):
        value = skill(note="Pythonの経験年数不足")
        self.assertIsNotNone(
            target.evaluate_required_skill(value, "業務でPython機能を実装")
        )

    def test_self_study_is_not_direct_practical_evidence(self):
        self.assertIsNone(
            target.evaluate_required_skill(skill(), "Pythonを独学し資格を取得")
        )

    def test_required_action_needs_direct_support(self):
        value = skill(
            name="AWSクラウド環境のシステム設計経験",
            note="AWS経験は3ヶ月のみ",
        )
        self.assertIsNone(
            target.evaluate_required_skill(value, "AWSへ業務アプリをデプロイ")
        )

    def test_alternative_technology_can_use_same_noted_technology(self):
        value = skill(
            name="Nest.jsまたはNode.jsを用いたバックエンド開発経験",
            note="Node.js経験は1カ月のみ",
        )
        self.assertIsNotNone(
            target.evaluate_required_skill(value, "業務システムをNode.jsで改修対応")
        )

    def test_single_python_duration_only_reason_is_retained(self):
        value = skill(note="Python経験は1ヶ月のみ")
        self.assertIsNotNone(
            target.evaluate_required_skill(
                value, "社内業務でPythonスクレイピングを実装"
            )
        )

    def test_pure_or_duration_only_reason_is_retained(self):
        value = skill(
            name="Nest.jsまたはNode.jsを用いたバックエンド開発経験",
            note="Node.js経験期間が短い",
        )
        self.assertIsNotNone(
            target.evaluate_required_skill(value, "業務システムをNode.jsで改修対応")
        )


class DownstreamRetentionTest(unittest.TestCase):
    def test_below_gate_pair_is_added_without_result_change(self):
        rows = [
            record(
                [
                    skill(),
                    skill("AI知見", False, "記載なし"),
                    skill("顧客折衝", False, "記載なし"),
                    skill("資料作成", False, "記載なし"),
                    skill("構築", False, "記載なし"),
                    skill("推進", True, "固定true"),
                ]
            )
        ]
        before = copy.deepcopy(rows)
        _, retained = target.collect_retention_candidates(
            rows, {"resource-1": "社内業務でPythonスクレイピングを実装"}
        )
        self.assertEqual(len(retained), 1)
        self.assertEqual(retained[0]["retention_destination"], "08-5_recheck_only")
        self.assertFalse(retained[0]["proposal_ready_direct"])
        self.assertEqual(rows, before)


if __name__ == "__main__":
    unittest.main()

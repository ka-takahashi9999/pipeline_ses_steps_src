"""Focused offline tests for the 07-1 candidate-retention guard."""

import copy
import importlib.util
import sys
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


def record(required_skills, project="project-1", resource="resource-1"):
    return {
        "project_info": {"message_id": project},
        "resource_info": {"message_id": resource},
        "required_skills": required_skills,
        "optional_skills": [],
    }


class GuardConditionTest(unittest.TestCase):
    def test_no_required_years_and_direct_one_month_practical_evidence_is_rescued(self):
        value = skill()
        evidence = "社内業務でPython/Seleniumによるスクレイピングを実装"
        guarded, audit = target.apply_guard_to_record(record([value]), evidence)
        self.assertTrue(guarded["required_skills"][0]["match"])
        self.assertEqual(len(audit), 1)

    def test_explicit_three_year_requirement_is_not_rescued(self):
        value = skill(name="Python 3年以上", note="Python経験は1ヶ月のみ")
        guarded, audit = target.apply_guard_to_record(
            record([value]), "実務でPythonを使い機能を実装"
        )
        self.assertFalse(guarded["required_skills"][0]["match"])
        self.assertEqual(audit, [])

    def test_other_explicit_minimum_year_forms_are_not_rescued(self):
        for name in ("Python 3年超", "Python 最低2年", "Python 経験3年~5年"):
            with self.subTest(name=name):
                value = skill(name=name, note="Python経験は1ヶ月のみ")
                guarded, audit = target.apply_guard_to_record(
                    record([value]), "実務でPythonを使い機能を実装"
                )
                self.assertFalse(guarded["required_skills"][0]["match"])
                self.assertEqual(audit, [])

    def test_self_study_only_is_not_rescued(self):
        guarded, audit = target.apply_guard_to_record(
            record([skill()]), "Pythonを自己学習し資格を取得"
        )
        self.assertFalse(guarded["required_skills"][0]["match"])
        self.assertEqual(audit, [])

    def test_other_technology_only_is_not_rescued(self):
        guarded, audit = target.apply_guard_to_record(
            record([skill()]), "Java/Seleniumによる業務システムを実装"
        )
        self.assertFalse(guarded["required_skills"][0]["match"])
        self.assertEqual(audit, [])

    def test_existing_true_is_not_changed(self):
        original = skill(match=True, note="Python実務経験あり")
        guarded, audit = target.apply_guard_to_record(
            record([original]), "業務でPythonを使用して実装"
        )
        self.assertEqual(guarded["required_skills"][0], original)
        self.assertEqual(audit, [])

    def test_indeterminate_evidence_is_not_rescued(self):
        guarded, audit = target.apply_guard_to_record(
            record([skill()]), "Python | 1ヶ月"
        )
        self.assertFalse(guarded["required_skills"][0]["match"])
        self.assertEqual(audit, [])

    def test_non_duration_failure_is_not_rescued(self):
        value = skill(note="Python経験1ヶ月のみ、設計経験の記載なし")
        guarded, audit = target.apply_guard_to_record(
            record([value]), "Pythonで業務ツールを実装"
        )
        self.assertFalse(guarded["required_skills"][0]["match"])
        self.assertEqual(audit, [])

    def test_guard_is_not_python_specific(self):
        value = skill(
            name="Node.jsを用いたバックエンド開発経験",
            note="Node.js経験は1カ月のみ",
        )
        guarded, audit = target.apply_guard_to_record(
            record([value]), "ユーザー情報システムをNode.jsで改修対応"
        )
        self.assertTrue(guarded["required_skills"][0]["match"])
        self.assertEqual(len(audit), 1)

    def test_required_work_type_must_have_direct_evidence(self):
        value = skill(
            name="AWSクラウド環境のシステム設計経験",
            note="AWS経験は3ヶ月のみ",
        )
        guarded, audit = target.apply_guard_to_record(
            record([value]), "AWSへアプリをデプロイした経験あり"
        )
        self.assertFalse(guarded["required_skills"][0]["match"])
        self.assertEqual(audit, [])


class ReplayIntegrityTest(unittest.TestCase):
    def test_retention_path_recovers_known_gate_loss_without_proposal_ready(self):
        key_args = {"project": "project-loss", "resource": "resource-loss"}
        before = record(
            [
                skill(),
                {"skill": "設計経験", "match": False, "note": "記載なし"},
                {"skill": "顧客折衝", "match": False, "note": "記載なし"},
                {"skill": "資料作成", "match": False, "note": "記載なし"},
                {"skill": "AI知見", "match": False, "note": "記載なし"},
                {"skill": "推進力", "match": True, "note": "固定true"},
            ],
            **key_args
        )
        direct = copy.deepcopy(before)
        for index in (0, 2, 3, 4, 5):
            direct["required_skills"][index]["match"] = True
        after, audits = target.apply_guard(
            [before], {"resource-loss": "業務でPythonスクレイピングを実装"}
        )
        membership = {("project-loss", "resource-loss"): "human_review"}
        simulation = target.simulate_downstream(
            [before], after, [direct], audits, membership
        )
        self.assertEqual(simulation["candidate_loss_before"], 1)
        self.assertEqual(simulation["candidate_loss_after"], 0)
        self.assertEqual(simulation["recovered_saved_status"], {"human_review": 1})
        self.assertEqual(simulation["proposal_ready_false_positive_after"], 0)

    def test_duplicate_and_order_are_preserved_and_production_is_unchanged(self):
        rows = [
            record([skill()], "project-1", "resource-1"),
            record([skill()], "project-1", "resource-2"),
        ]
        before_snapshot = target.snapshot_production_files()
        guarded, audits = target.apply_guard(
            rows,
            {
                "resource-1": "Pythonで業務処理を実装",
                "resource-2": "Pythonを自己学習",
            },
        )
        after_snapshot = target.snapshot_production_files()
        self.assertEqual(
            [target.pair_key(row) for row in guarded],
            [target.pair_key(row) for row in rows],
        )
        self.assertEqual(len(set(target.pair_key(row) for row in guarded)), 2)
        self.assertEqual(len(audits), 1)
        self.assertEqual(before_snapshot, after_snapshot)


if __name__ == "__main__":
    unittest.main()

"""Focused offline tests for the 08-5 legacy/Batch mode switch."""

import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import Mock


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parents[1]
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

import config
import high_score_required_skill_recheck as legacy
import run_high_score_required_skill_recheck as runner
from common.json_utils import read_jsonl_as_list, write_jsonl


def _record(project_id, resource_id, skills):
    return {
        "project_info": {
            "message_id": project_id,
            "required_skills": [
                {"skill": skill, "match": False, "note": "fixture"}
                for skill in skills
            ],
            "project_name": f"project-{project_id}",
        },
        "resource_info": {
            "message_id": resource_id,
            "resource_name": f"resource-{resource_id}",
        },
        "match_info": {"required_skills_match_rate": 0.9},
    }


class ExecutionModeResolutionTest(unittest.TestCase):
    def test_resolution_matrix(self):
        cases = [
            ({"STEP_08_5_EXECUTION_MODE": "legacy"}, "legacy"),
            ({"STEP_08_5_EXECUTION_MODE": "batch"}, "batch"),
            ({"ENABLE_08_5_BATCH_ORCHESTRATION": "0"}, "legacy"),
            ({"ENABLE_08_5_BATCH_ORCHESTRATION": "1"}, "batch"),
            (
                {
                    "STEP_08_5_EXECUTION_MODE": "legacy",
                    "ENABLE_08_5_BATCH_ORCHESTRATION": "0",
                },
                "legacy",
            ),
            (
                {
                    "STEP_08_5_EXECUTION_MODE": "batch",
                    "ENABLE_08_5_BATCH_ORCHESTRATION": "1",
                },
                "batch",
            ),
            ({}, "batch"),
        ]
        for environ, expected in cases:
            with self.subTest(environ=environ):
                self.assertEqual(expected, config.resolve_execution_mode(environ))

    def test_conflicts_and_invalid_values_fail(self):
        invalid = [
            {
                "STEP_08_5_EXECUTION_MODE": "legacy",
                "ENABLE_08_5_BATCH_ORCHESTRATION": "1",
            },
            {
                "STEP_08_5_EXECUTION_MODE": "batch",
                "ENABLE_08_5_BATCH_ORCHESTRATION": "0",
            },
            {"STEP_08_5_EXECUTION_MODE": "direct"},
            {"ENABLE_08_5_BATCH_ORCHESTRATION": "true"},
            {"STEP_08_5_EXECUTION_MODE": ""},
        ]
        for environ in invalid:
            with self.subTest(environ=environ):
                with self.assertRaises(config.ExecutionModeError):
                    config.resolve_execution_mode(environ)

    def test_entrypoint_stops_before_execution_on_conflict(self):
        environment = {
            "STEP_08_5_EXECUTION_MODE": "legacy",
            "ENABLE_08_5_BATCH_ORCHESTRATION": "1",
        }
        with patch.dict(os.environ, environment, clear=True), patch.object(
            runner, "run_phase_a"
        ) as phase_a, patch.object(
            sys,
            "argv",
            [
                "run_high_score_required_skill_recheck.py",
                "--pipeline-run-id",
                "conflict-run",
                "--run-date",
                "20260911",
            ],
        ):
            self.assertEqual(2, runner.main())
        phase_a.assert_not_called()


class LegacyIsolatedRunTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="08_5_legacy_focused_")
        self.root = Path(self.temporary.name)
        self.inputs = self.root / "inputs"
        self.outputs = self.root / "outputs"
        self.inputs.mkdir()
        self.score100 = self.inputs / "100.jsonl"
        self.score80 = self.inputs / "80.jsonl"
        self.sidecar = self.inputs / "retention.jsonl"
        self.skillsheets = self.inputs / "skillsheets.jsonl"
        self.emails = self.inputs / "emails.jsonl"
        self.concurrent_config = self.inputs / "config.py"

        first = _record("p1", "r1", ["Python開発", "AWS構築"])
        second = _record("p2", "r2", ["Java開発"])
        retained = copy.deepcopy(first)
        retained["retention_guard"] = {"destination": "08-5_recheck_only"}
        write_jsonl(str(self.score100), [first])
        write_jsonl(str(self.score80), [])
        write_jsonl(str(self.sidecar), [retained, second])
        write_jsonl(
            str(self.skillsheets),
            [
                {"message_id": "r1", "success": True, "skillsheet": "Python AWS"},
                {"message_id": "r2", "success": True, "skillsheet": "Java"},
            ],
        )
        write_jsonl(
            str(self.emails),
            [
                {"message_id": "p1", "body": "Python AWS案件"},
                {"message_id": "p2", "body": "Java案件"},
            ],
        )
        self.concurrent_config.write_text("ENABLE_07_1_CONCURRENT = True\n", encoding="utf-8")

        self.legacy_patches = [
            patch.object(
                legacy,
                "INPUT_SCORE_FILES",
                (("100percent", self.score100), ("80to99percent", self.score80)),
            ),
            patch.object(legacy, "RETENTION_SIDECAR", self.sidecar),
            patch.object(legacy, "INPUT_SKILLSHEETS", self.skillsheets),
            patch.object(legacy, "INPUT_CLEANED_EMAILS", self.emails),
            patch.object(legacy, "CONCURRENT_CONFIG", self.concurrent_config),
            patch.object(legacy, "OUTPUT_ALL", self.outputs / legacy.OUTPUT_ALL.name),
            patch.object(
                legacy, "OUTPUT_CONFIRMED", self.outputs / legacy.OUTPUT_CONFIRMED.name
            ),
            patch.object(
                legacy,
                "OUTPUT_HUMAN_REVIEW",
                self.outputs / legacy.OUTPUT_HUMAN_REVIEW.name,
            ),
            patch.object(
                legacy,
                "OUTPUT_NOT_CONFIRMED",
                self.outputs / legacy.OUTPUT_NOT_CONFIRMED.name,
            ),
            patch.object(legacy, "OUTPUT_ERROR", self.outputs / legacy.OUTPUT_ERROR.name),
        ]
        self.runner_patches = [
            patch.object(runner, "RESULT_DIR", self.outputs),
            patch.object(runner, "EXECUTION_TIME_DIR", self.root / "execution_time"),
            patch.object(runner, "CONTEXT_ROOT", self.outputs / "_execution_context"),
            patch.object(
                runner,
                "OWNER_PATH",
                self.outputs / "_execution_context/current_owner.json",
            ),
            patch.object(
                runner,
                "LOCK_PATH",
                self.outputs / "_execution_context/.context.lock",
            ),
            patch.object(runner, "COMMIT_MARKER", self.outputs / "production_commit.json"),
        ]
        for item in self.legacy_patches + self.runner_patches:
            item.start()

    def tearDown(self):
        for item in reversed(self.legacy_patches + self.runner_patches):
            item.stop()
        self.temporary.cleanup()

    def test_legacy_isolated_atomic_publish_and_no_batch_dependencies(self):
        calls = []

        def fake_llm(**kwargs):
            calls.append(kwargs)
            skills = kwargs["response_schema"]["required_skill_checks"]
            if len(calls) == 2:
                raise RuntimeError("focused injected LLM failure")
            return {
                "required_skill_checks": [
                    {
                        "skill": row["skill"],
                        "confidence": "confirmed",
                        "reason": "固定入力で根拠あり",
                        "evidence": row["skill"],
                    }
                    for row in skills
                ],
                "category_match": "match",
                "category_note": "案件: 開発 / 要員: 開発",
            }

        sys.modules.pop("batch_aws_orchestration", None)
        with patch.object(legacy, "call_llm", side_effect=fake_llm):
            exit_code = runner.run_phase_a(
                "legacy-focused-run", "20260911", "legacy", limit=None
            )

        self.assertEqual(0, exit_code)
        self.assertNotIn("batch_aws_orchestration", sys.modules)
        self.assertFalse((self.outputs / "_batch_runtime").exists())
        all_rows = read_jsonl_as_list(str(legacy.OUTPUT_ALL))
        confirmed = read_jsonl_as_list(str(legacy.OUTPUT_CONFIRMED))
        human_review = read_jsonl_as_list(str(legacy.OUTPUT_HUMAN_REVIEW))
        not_confirmed = read_jsonl_as_list(str(legacy.OUTPUT_NOT_CONFIRMED))
        errors = read_jsonl_as_list(str(legacy.OUTPUT_ERROR))
        self.assertEqual(2, len(all_rows))
        self.assertEqual(1, len(confirmed))
        self.assertEqual(1, len(human_review))
        self.assertEqual(0, len(not_confirmed))
        self.assertEqual(len(all_rows), len(confirmed) + len(human_review) + len(not_confirmed))
        self.assertEqual(1, len(errors))
        self.assertEqual(2, len(calls))
        self.assertEqual(
            ["Python開発", "AWS構築"],
            [row["skill"] for row in all_rows[0]["required_skill_checks"]],
        )
        self.assertEqual("project-p1", all_rows[0]["project_info"]["project_name"])
        marker = json.loads(runner.COMMIT_MARKER.read_text(encoding="utf-8"))
        self.assertEqual("legacy", marker["execution_mode"])
        self.assertEqual(2, marker["artifacts"]["all"]["records"])
        contexts = list((runner.CONTEXT_ROOT / "20260911").glob("*.json"))
        self.assertEqual(1, len(contexts))
        context = json.loads(contexts[0].read_text(encoding="utf-8"))
        self.assertTrue(context["artifacts_committed"])
        self.assertEqual("legacy", context["execution_mode"])

    def test_same_run_mode_input_and_owner_changes_are_rejected(self):
        first = runner.prepare_execution_context("guarded-run", "20260911", "legacy")
        with self.assertRaises(runner.ExecutionContextError):
            runner.prepare_execution_context("guarded-run", "20260911", "batch")

        write_jsonl(str(self.score80), [_record("p3", "r3", ["Go開発"])])
        with self.assertRaises(runner.ExecutionContextError):
            runner.require_execution_context("guarded-run", "20260911", "legacy")

        write_jsonl(str(self.score80), [])
        runner.prepare_execution_context("new-owner-run", "20260911", "legacy")
        with self.assertRaises(runner.ExecutionContextError):
            runner.assert_publish_owner(first)

    def test_batch_route_uses_existing_phase_a_and_suspends(self):
        contract = self.root / "suspend_contract"
        current_step = self.root / "current_step"
        fake_orchestration = Mock()
        fake_orchestration.phase_a.return_value = {
            "contract": "SUSPENDED",
            "current_step": "08-5_BATCH_WAIT",
        }
        context = {"artifacts_committed": False}
        environment = {
            "PIPELINE_SUSPEND_CONTRACT_FILE": str(contract),
            "PIPELINE_CURRENT_STEP_FILE": str(current_step),
            "PIPELINE_SUSPEND_EXIT_CODE": "85",
        }
        with patch.object(
            runner, "prepare_execution_context", return_value=context
        ), patch.dict(
            sys.modules, {"batch_aws_orchestration": fake_orchestration}
        ), patch.dict(os.environ, environment, clear=False):
            exit_code = runner.run_phase_a(
                "batch-focused-run", "20260911", "batch", limit=None
            )
        self.assertEqual(85, exit_code)
        fake_orchestration.phase_a.assert_called_once_with(
            "batch-focused-run", "20260911"
        )
        self.assertEqual("08-5_BATCH_WAIT", current_step.read_text().strip())
        self.assertEqual(
            "SUSPENDED:BATCH_WAIT:08-5_BATCH_WAIT", contract.read_text().strip()
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)

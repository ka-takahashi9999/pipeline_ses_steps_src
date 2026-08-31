#!/usr/bin/env python3
"""Focused mocked tests for Issue 2 AWS orchestration integration."""

import importlib.util
import io
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock


TOOL_DIR = Path(__file__).resolve().parent
STEP_DIR = TOOL_DIR.parent
ROOT = STEP_DIR.parent

import sys

sys.path.insert(0, str(TOOL_DIR))
import batch_aws_orchestration as orchestration  # noqa: E402
import high_score_required_skill_recheck_batch as engine  # noqa: E402

ORIGINAL_PREPARE_RUN = engine.prepare_run


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


status_lambda = load_module("issue2_status_lambda", STEP_DIR / "aws/lambda_function.py")
asl_patch = load_module("issue2_asl_patch", STEP_DIR / "aws/state_machine_patch.py")


class MissingKey(Exception):
    def __init__(self):
        super().__init__("NoSuchKey")
        self.response = {"Error": {"Code": "NoSuchKey"}}


class MemoryS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, **_kwargs):
        self.objects[(Bucket, Key)] = bytes(Body)
        return {"ETag": '"fixture"'}

    def get_object(self, Bucket, Key):
        try:
            payload = self.objects[(Bucket, Key)]
        except KeyError as error:
            raise MissingKey() from error
        return {"Body": io.BytesIO(payload)}

    def json(self, bucket, key):
        return json.loads(self.objects[(bucket, key)].decode("utf-8"))


class BatchClient:
    def __init__(self):
        self.upload_calls = 0
        self.create_calls = 0
        self.create_error = None
        self.file_value = {
            "id": "file-input-issue2",
            "purpose": "batch",
            "status": "processed",
        }

    def upload_input(self, _path):
        self.upload_calls += 1
        return "file-input-issue2"

    def retrieve_file(self, _file_id):
        return self.file_value

    def create_batch(self, _input_file_id, metadata):
        self.create_calls += 1
        if self.create_error:
            raise self.create_error
        return {
            "id": "batch-issue2",
            "status": "validating",
            "input_file_id": "file-input-issue2",
            "metadata": metadata,
            "request_counts": {"total": 1, "completed": 0, "failed": 0},
        }

    def retrieve_batch(self, _batch_id):
        return {
            "id": "batch-issue2",
            "status": "completed",
            "request_counts": {"total": 1, "completed": 1, "failed": 0},
            "output_file_id": "file-output-issue2",
        }


def context():
    record = {
        "project_info": {
            "message_id": "project-issue2",
            "required_skills": [
                {"skill": "Python開発経験", "match": True, "note": "fixture"}
            ],
        },
        "resource_info": {"message_id": "resource-issue2"},
        "match_info": {"score": 100},
    }
    return engine._build_request_context(
        record,
        "100percent",
        1,
        {
            "resource-issue2": {
                "success": True,
                "skillsheet": "Python開発経験3年",
            }
        },
        {"project-issue2": "Python開発案件"},
    )


def prepare_side_effect(run_id, runtime_root=engine.RUNTIME_ROOT):
    return ORIGINAL_PREPARE_RUN(run_id, runtime_root=runtime_root, contexts=[context()])


def base_pipeline_status(run_id="sfn-issue2-fixture", run_date="20260823"):
    return {
        "schema_version": "1.0",
        "run_id": run_id,
        "run_date": run_date,
        "status": "RUNNING",
        "started_at": "2026-08-23T09:00:00Z",
        "finished_at": None,
        "finished_at_source": "not_finished",
        "exit_code": None,
        "exit_code_source": "pending",
        "current_step": "08-5_BATCH_WAIT",
        "error_message": "",
        "log_s3_uri": "s3://technoverse/pipeline.log",
        "updated_at": "2026-08-23T10:00:00Z",
    }


class AwsOrchestrationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.runtime_root = Path(self.temporary.name) / "runtime"
        self.s3 = MemoryS3()
        self.pipeline_run_id = "sfn-issue2-fixture"
        self.run_date = "20260823"
        self.bucket = "technoverse"
        self.prefix = (
            "pipeline_ses_steps/batch-state/08-5/20260823/sfn-issue2-fixture"
        )
        self.client = BatchClient()
        self.zero_stabilization = mock.patch.object(
            engine, "FILE_STABILIZATION_WINDOW_SECONDS", 0.0
        )
        self.zero_stabilization.start()

    def tearDown(self):
        self.zero_stabilization.stop()
        self.temporary.cleanup()

    def phase_a(self):
        with mock.patch.object(
            orchestration.ENGINE, "prepare_run", side_effect=prepare_side_effect
        ):
            return orchestration.phase_a(
                self.pipeline_run_id,
                self.run_date,
                s3=self.s3,
                runtime_root=self.runtime_root,
                client=self.client,
            )

    def test_phase_a_returns_suspended_batch_wait_not_success(self):
        result = self.phase_a()
        self.assertEqual(result["contract"], "SUSPENDED")
        self.assertEqual(result["current_step"], "08-5_BATCH_WAIT")
        self.assertNotIn("SUCCEEDED", result.values())

    def test_phase_a_retry_does_not_duplicate_submit(self):
        first = self.phase_a()
        second = self.phase_a()
        self.assertEqual(self.client.create_calls, 1)
        self.assertEqual(first["batch_id"], second["batch_id"])
        self.assertTrue(second["resumed"])

    def test_s3_state_restores_after_local_runtime_is_removed(self):
        first = self.phase_a()
        shutil.rmtree(self.runtime_root)
        second = self.phase_a()
        self.assertEqual(first["manifest_sha256"], second["manifest_sha256"])
        self.assertEqual(self.client.create_calls, 1)

    def test_pending_reconciliation_is_persisted_without_resubmit(self):
        self.client.create_error = TimeoutError("fixture timeout")
        result = self.phase_a()
        state = self.s3.json(self.bucket, self.prefix + "/state.json")
        self.assertEqual(result["state"], "PENDING_RECONCILIATION")
        self.assertEqual(state["state"], "PENDING_RECONCILIATION")
        self.assertEqual(self.client.create_calls, 1)

    def test_file_readiness_failure_is_persisted_without_batch_create(self):
        self.client.file_value = {
            "id": "file-input-issue2",
            "purpose": "batch",
            "status": "error",
        }
        with self.assertRaises(engine.FileReadinessError):
            self.phase_a()
        state = self.s3.json(self.bucket, self.prefix + "/state.json")
        self.assertEqual("SAFE_STOPPED", state["state"])
        self.assertEqual("file_status_error", state["file_readiness"]["readiness_result"])
        self.assertEqual(0, self.client.create_calls)

    def _mark_remote_completed(self):
        state_key = self.prefix + "/state.json"
        state = self.s3.json(self.bucket, state_key)
        state.update(
            {
                "state": "COMPLETED",
                "batch_status": "completed",
                "request_counts": {"total": 1, "completed": 1, "failed": 0},
                "output_file_id": "file-output-issue2",
                "state_revision": int(state["state_revision"]) + 1,
            }
        )
        self.s3.put_object(
            Bucket=self.bucket,
            Key=state_key,
            Body=(json.dumps(state) + "\n").encode("utf-8"),
        )

    def _commit_side_effect(self, batch_run_id, _client, runtime_root, publish):
        self.assertTrue(publish)
        run_dir = engine._run_dir(batch_run_id, runtime_root)
        store = engine.FileStateStore(run_dir)
        state, etag = store.load()
        state["state"] = "COMMITTED"
        store.cas(etag, state)
        return {"collector_retry": False}

    def test_phase_b_requires_expected_run_and_manifest_marker(self):
        self.phase_a()
        self._mark_remote_completed()
        with mock.patch.object(
            orchestration.ENGINE, "collect_run", side_effect=self._commit_side_effect
        ), mock.patch.object(
            orchestration.ENGINE,
            "validate_commit_marker",
            return_value={
                "run_id": orchestration.batch_run_id_for(self.pipeline_run_id),
                "manifest_sha256": self.s3.json(
                    self.bucket, self.prefix + "/state.json"
                )["manifest_sha256"],
            },
        ) as marker:
            result = orchestration.phase_b(
                self.pipeline_run_id,
                self.run_date,
                s3=self.s3,
                runtime_root=self.runtime_root,
                client=self.client,
            )
        marker.assert_called_once_with(
            expected_run_id=orchestration.batch_run_id_for(self.pipeline_run_id),
            expected_manifest_sha256=result["manifest_sha256"],
        )
        self.assertEqual(result["gate"], "PRODUCTION_COMMIT_VALID")

    def test_invalid_marker_blocks_phase_b_gate(self):
        self.phase_a()
        self._mark_remote_completed()
        with mock.patch.object(
            orchestration.ENGINE, "collect_run", side_effect=self._commit_side_effect
        ), mock.patch.object(
            orchestration.ENGINE,
            "validate_commit_marker",
            side_effect=engine.PublishError("fixture marker mismatch"),
        ):
            with self.assertRaises(engine.PublishError):
                orchestration.phase_b(
                    self.pipeline_run_id,
                    self.run_date,
                    s3=self.s3,
                    runtime_root=self.runtime_root,
                    client=self.client,
                )

    def test_phase_b_retry_uses_committed_state_idempotently(self):
        self.phase_a()
        self._mark_remote_completed()
        marker_value = {
            "run_id": orchestration.batch_run_id_for(self.pipeline_run_id),
            "manifest_sha256": self.s3.json(
                self.bucket, self.prefix + "/state.json"
            )["manifest_sha256"],
        }
        with mock.patch.object(
            orchestration.ENGINE, "collect_run", side_effect=self._commit_side_effect
        ) as collector, mock.patch.object(
            orchestration.ENGINE, "validate_commit_marker", return_value=marker_value
        ):
            orchestration.phase_b(
                self.pipeline_run_id,
                self.run_date,
                s3=self.s3,
                runtime_root=self.runtime_root,
                client=self.client,
            )
            orchestration.phase_b(
                self.pipeline_run_id,
                self.run_date,
                s3=self.s3,
                runtime_root=self.runtime_root,
                client=self.client,
            )
        self.assertEqual(collector.call_count, 2)
        state = self.s3.json(self.bucket, self.prefix + "/state.json")
        self.assertEqual(state["state"], "COMMITTED")


class StatusLambdaTest(unittest.TestCase):
    def setUp(self):
        self.s3 = MemoryS3()
        self.run_id = "sfn-status-fixture"
        self.run_date = "20260823"
        self.state_key = (
            "pipeline_ses_steps/batch-state/08-5/20260823/sfn-status-fixture/state.json"
        )
        self.pipeline_key = (
            "pipeline_ses_steps/pipeline-status/20260823/sfn-status-fixture/status.json"
        )
        self.state = {
            "pipeline_run_id": self.run_id,
            "run_date": self.run_date,
            "run_id": "p" + "1" * 23,
            "manifest_sha256": "a" * 64,
            "submission_nonce": "nonce",
            "input_file_id": "file-input",
            "batch_id": "batch-1",
            "batch_status": "validating",
            "state": "SUBMITTED",
            "state_revision": 1,
        }
        self._put(self.state_key, self.state)
        self._put(self.pipeline_key, base_pipeline_status(self.run_id, self.run_date))

    def _put(self, key, value):
        self.s3.put_object(
            Bucket="technoverse",
            Key=key,
            Body=(json.dumps(value) + "\n").encode("utf-8"),
        )

    def test_lambda_mocked_wait_and_completed_transition(self):
        waiting = {"id": "batch-1", "status": "in_progress", "request_counts": {"total": 1, "completed": 0, "failed": 0}}
        completed = {
            "id": "batch-1",
            "status": "completed",
            "request_counts": {"total": 1, "completed": 1, "failed": 0},
            "output_file_id": "file-output",
            "completed_at": 1787450000,
        }
        with mock.patch.object(status_lambda, "_openai_get", side_effect=[waiting, completed]):
            first = status_lambda.check_status(
                {"run_id": self.run_id, "run_date": self.run_date},
                s3=self.s3,
                api_key="fixture",
            )
            second = status_lambda.check_status(
                {"run_id": self.run_id, "run_date": self.run_date},
                s3=self.s3,
                api_key="fixture",
            )
        self.assertEqual(first["outcome"], "WAIT")
        self.assertEqual(second["outcome"], "COMPLETED")
        state = self.s3.json("technoverse", self.state_key)
        self.assertEqual(state["state"], "COMPLETED")
        self.assertEqual(state["completed_at"], 1787450000)

    def test_lambda_pending_reconciliation_adopts_exact_match_only(self):
        pending = dict(self.state)
        pending.update({"state": "PENDING_RECONCILIATION", "batch_id": None})
        self._put(self.state_key, pending)
        match = {
            "id": "batch-adopted",
            "status": "validating",
            "input_file_id": "file-input",
            "metadata": {
                "run_id": pending["run_id"],
                "submission_nonce": "nonce",
                "manifest_sha256": "a" * 64,
            },
        }
        retrieved = dict(match)
        retrieved["request_counts"] = {"total": 1, "completed": 0, "failed": 0}
        with mock.patch.object(
            status_lambda, "_openai_get", side_effect=[{"data": [match]}, retrieved]
        ):
            result = status_lambda.check_status(
                {"run_id": self.run_id, "run_date": self.run_date},
                s3=self.s3,
                api_key="fixture",
            )
        self.assertEqual(result["outcome"], "WAIT")
        state = self.s3.json("technoverse", self.state_key)
        self.assertEqual(state["batch_id"], "batch-adopted")

    def test_lambda_terminal_statuses_fail_without_production_processing(self):
        for status in ("failed", "expired", "cancelled"):
            with self.subTest(status=status):
                self._put(self.state_key, dict(self.state))
                self._put(
                    self.pipeline_key,
                    base_pipeline_status(self.run_id, self.run_date),
                )
                observed = {"id": "batch-1", "status": status, "request_counts": {"total": 1, "completed": 0, "failed": 1}}
                with mock.patch.object(status_lambda, "_openai_get", return_value=observed):
                    result = status_lambda.check_status(
                        {"run_id": self.run_id, "run_date": self.run_date},
                        s3=self.s3,
                        api_key="fixture",
                    )
                self.assertEqual(result["outcome"], "FAILED")
                pipeline = self.s3.json("technoverse", self.pipeline_key)
                self.assertEqual(pipeline["status"], "FAILED")
                self.assertEqual(pipeline["current_step"], "08-5_BATCH_WAIT")

    def test_lambda_terminal_errors_are_sanitized_into_state(self):
        observed = {
            "id": "batch-1",
            "status": "failed",
            "request_counts": {"total": 0, "completed": 0, "failed": 0},
            "errors": {
                "data": [
                    {
                        "code": "invalid_request",
                        "message": "Cannot find file; Bearer sk-secret123456789",
                        "param": "file_id",
                        "line": 1,
                        "api_key": "must-not-persist",
                    }
                ]
            },
        }
        with mock.patch.object(status_lambda, "_openai_get", return_value=observed):
            status_lambda.check_status(
                {"run_id": self.run_id, "run_date": self.run_date},
                s3=self.s3,
                api_key="fixture",
            )
        state = self.s3.json("technoverse", self.state_key)
        self.assertEqual(
            {
                "code": "invalid_request",
                "message": "Cannot find file; Bearer [REDACTED]",
                "param": "file_id",
                "line": 1,
            },
            state["batch_errors"][0],
        )
        self.assertNotIn("api_key", state["batch_errors"][0])

    def test_lambda_bounded_reconciliation_never_resubmits(self):
        pending = dict(self.state)
        pending.update(
            {
                "state": "PENDING_RECONCILIATION",
                "batch_id": None,
                "reconciliation_checks": 0,
            }
        )
        self._put(self.state_key, pending)
        with mock.patch.object(
            status_lambda, "_openai_get", return_value={"data": []}
        ) as openai_get:
            outcomes = [
                status_lambda.check_status(
                    {"run_id": self.run_id, "run_date": self.run_date},
                    s3=self.s3,
                    api_key="fixture",
                )["outcome"]
                for _ in range(3)
            ]
        self.assertEqual(outcomes, ["WAIT", "WAIT", "FAILED"])
        self.assertEqual(openai_get.call_count, 3)


class WiringAndAslTest(unittest.TestCase):
    def _base_asl(self):
        return {
            "StartAt": "ValidateRunDateInput",
            "TimeoutSeconds": 72000,
            "States": {
                "ValidateRunDateInput": {"Type": "Succeed"},
                "ValidateRunningDocument": {
                    "Type": "Choice",
                    "Choices": [{"And": [], "Next": "CheckRunningWaitLimit"}],
                },
                "CheckRunningWaitLimit": {"Type": "Succeed"},
                "StopEC2AfterFailure": {"Type": "Succeed"},
                "PublishFailureNotification": {"Type": "Succeed"},
                "ListPipelineStatusObject": {"Type": "Succeed"},
            },
        }

    def test_asl_wait_loop_completed_and_failure_routes(self):
        definition = asl_patch.apply_patch(self._base_asl())
        asl_patch.validate_graph(definition)
        states = definition["States"]
        self.assertEqual(states["WaitForBatchStatus"]["Seconds"], 300)
        routes = {
            choice["StringEquals"]: choice["Next"]
            for choice in states["CheckBatchStatusOutcome"]["Choices"]
        }
        self.assertEqual(routes["WAIT"], "WaitForBatchStatus")
        self.assertEqual(routes["COMPLETED"], "PreparePhaseBStart")
        self.assertEqual(routes["FAILED"], "SetBatchTerminalFailure")

    def test_asl_phase_b_command_and_scheduler_contract_untouched(self):
        original = self._base_asl()
        definition = asl_patch.apply_patch(original)
        command = definition["States"]["SendPhaseBLauncherCommand"]["Parameters"][
            "Parameters"
        ]["commands.$"]
        self.assertIn("PIPELINE_PHASE=B", command)
        self.assertEqual(definition["StartAt"], "ValidateRunDateInput")
        self.assertGreaterEqual(definition["TimeoutSeconds"], 108000)

    def test_phase_b_script_has_commit_gate_before_09_and_no_early_steps(self):
        text = (ROOT / "00_pipeline/00_tool/run_full_pipeline_phase_b.sh").read_text(
            encoding="utf-8"
        )
        gate = text.index("08-5_batch_collect_commit_gate")
        first_09 = text.index('run_step "09-1_')
        current = text.index('run_step "80-75_')
        self.assertLess(gate, first_09)
        self.assertLess(first_09, current)
        self.assertNotIn('run_step "01-', text)
        self.assertNotIn('run_step "08-4_', text)
        self.assertNotIn("high_score_required_skill_recheck.py\"", text)

    def test_nightly_batch_activation_is_deployed_disabled(self):
        config = (ROOT / "00_pipeline/00_tool/pipeline_s3_config.env").read_text(
            encoding="utf-8"
        )
        phase_a = (ROOT / "00_pipeline/00_tool/run_full_pipeline.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("ENABLE_08_5_BATCH_ORCHESTRATION:=0", config)
        self.assertIn('== "1"', phase_a)
        self.assertIn("high_score_required_skill_recheck.py", phase_a)

    def test_lambda_iam_has_no_ec2_send_command_or_wide_s3(self):
        policy = json.loads(
            (STEP_DIR / "aws/lambda-permissions-policy.json.template").read_text(
                encoding="utf-8"
            )
        )
        actions = []
        resources = []
        for statement in policy["Statement"]:
            action = statement["Action"]
            actions.extend(action if isinstance(action, list) else [action])
            resource = statement["Resource"]
            resources.extend(resource if isinstance(resource, list) else [resource])
        self.assertFalse(any(action.startswith("ec2:") for action in actions))
        self.assertNotIn("ssm:SendCommand", actions)
        self.assertNotIn("arn:aws:s3:::technoverse/*", resources)

    def test_managed_wrapper_suspension_keeps_running_status(self):
        run_id = "issue2-wrapper-fixture"
        managed_state = ROOT / "00_pipeline/01_result/managed/20260823" / run_id
        with tempfile.TemporaryDirectory() as temp_name:
            temp = Path(temp_name)
            trace = temp / "trace.jsonl"
            config = temp / "config.env"
            config.write_text(
                'PIPELINE_S3_BUCKET="technoverse"\n'
                'PIPELINE_S3_BASE_PREFIX="pipeline_ses_steps"\n'
                'PIPELINE_STATUS_PREFIX="pipeline-status"\n'
                'PIPELINE_LOG_PREFIX="pipeline-logs"\n'
                'PIPELINE_AWS_REGION="ap-northeast-1"\n'
                'PIPELINE_SYSTEMD_USER=""\n',
                encoding="utf-8",
            )
            child = temp / "child.sh"
            child.write_text(
                '#!/usr/bin/env bash\n'
                'printf "%s\\n" "08-5_BATCH_WAIT" > "$PIPELINE_CURRENT_STEP_FILE"\n'
                'printf "%s\\n" "SUSPENDED:BATCH_WAIT:08-5_BATCH_WAIT" > "$PIPELINE_SUSPEND_CONTRACT_FILE"\n'
                'exit "$PIPELINE_SUSPEND_EXIT_CODE"\n',
                encoding="utf-8",
            )
            child.chmod(0o755)
            writer = temp / "writer.py"
            writer.write_text(
                'import json, os, sys\n'
                'args=sys.argv[1:]\n'
                'pairs={args[i]: args[i+1] for i in range(0,len(args)-1,2) if args[i].startswith("--") and not args[i+1].startswith("--")}\n'
                'with open(os.environ["TRACE"],"a",encoding="utf-8") as f: f.write(json.dumps({"args":args,"pairs":pairs})+"\\n")\n',
                encoding="utf-8",
            )
            fake_aws = temp / "aws"
            fake_aws.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            fake_aws.chmod(0o755)
            env = dict(os.environ)
            env.update(
                {
                    "RUN_ID": run_id,
                    "RUN_DATE": "20260823",
                    "PIPELINE_S3_CONFIG_FILE": str(config),
                    "PIPELINE_SCRIPT": str(child),
                    "PIPELINE_STATUS_WRITER": str(writer),
                    "PIPELINE_PYTHON_BIN": sys.executable,
                    "PIPELINE_AWS_BIN": str(fake_aws),
                    "PIPELINE_LOCK_FILE": str(temp / "lock"),
                    "TRACE": str(trace),
                }
            )
            try:
                completed = subprocess.run(
                    ["bash", str(ROOT / "00_pipeline/00_tool/run_full_pipeline_managed.sh")],
                    env=env,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    timeout=30,
                    check=False,
                )
                calls = [json.loads(line) for line in trace.read_text().splitlines()]
            finally:
                if managed_state.exists():
                    shutil.rmtree(managed_state)
        self.assertEqual(completed.returncode, 0, completed.stdout)
        self.assertTrue(calls)
        self.assertTrue(all(call["pairs"].get("--status") == "RUNNING" for call in calls))
        final_args = calls[-1]["args"]
        self.assertNotIn("--finished-at", final_args)
        self.assertNotIn("--exit-code", final_args)


if __name__ == "__main__":
    unittest.main(verbosity=2)

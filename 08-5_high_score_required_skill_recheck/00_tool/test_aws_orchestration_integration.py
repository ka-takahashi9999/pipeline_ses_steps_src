#!/usr/bin/env python3
"""Focused mocked tests for Issue 2 AWS orchestration integration."""

import importlib.util
import hashlib
import io
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
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


class PreconditionFailed(Exception):
    def __init__(self):
        super().__init__("PreconditionFailed")
        self.response = {"Error": {"Code": "PreconditionFailed"}}


class SimulatedCrash(BaseException):
    pass


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.before_put = None
        self.lock = threading.Lock()

    def put_object(self, Bucket, Key, Body, **kwargs):
        if self.before_put is not None:
            self.before_put(Bucket, Key, bytes(Body), kwargs)
        with self.lock:
            if kwargs.get("IfNoneMatch") == "*" and (Bucket, Key) in self.objects:
                raise PreconditionFailed()
            if "IfMatch" in kwargs:
                current = self.objects.get((Bucket, Key))
                current_etag = (
                    '"' + hashlib.md5(current).hexdigest() + '"'
                    if current is not None
                    else None
                )
                if current_etag != kwargs["IfMatch"]:
                    raise PreconditionFailed()
            self.objects[(Bucket, Key)] = bytes(Body)
        return {"ETag": '"' + hashlib.md5(bytes(Body)).hexdigest() + '"'}

    def get_object(self, Bucket, Key):
        try:
            payload = self.objects[(Bucket, Key)]
        except KeyError as error:
            raise MissingKey() from error
        return {
            "Body": io.BytesIO(payload),
            "ETag": '"' + hashlib.md5(payload).hexdigest() + '"',
        }

    def json(self, bucket, key):
        return json.loads(self.objects[(bucket, key)].decode("utf-8"))


class BatchClient:
    def __init__(self):
        self.upload_calls = 0
        self.create_calls = 0
        self.create_error = None
        self.upload_id = "file-input-issue2"
        self.batch_id = "batch-issue2"
        self.file_value = {
            "id": "file-input-issue2",
            "purpose": "batch",
            "status": "processed",
        }

    def upload_input(self, _path):
        self.upload_calls += 1
        return self.upload_id

    def retrieve_file(self, _file_id):
        return self.file_value

    def create_batch(self, _input_file_id, metadata):
        self.create_calls += 1
        if self.create_error:
            raise self.create_error
        return {
            "id": self.batch_id,
            "status": "validating",
            "input_file_id": self.upload_id,
            "metadata": metadata,
            "request_counts": {"total": 1, "completed": 0, "failed": 0},
        }

    def retrieve_batch(self, _batch_id):
        return {
            "id": self.batch_id,
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

    def prepare_recovery(self):
        self.phase_a()
        state_key = self.prefix + "/state.json"
        state = self.s3.json(self.bucket, state_key)
        terminal_error = {
            "code": "invalid_request",
            "param": "file_id",
            "message": "Cannot find file",
            "line": None,
        }
        state.update(
            {
                "state": "RECOVERY_REQUIRED",
                "batch_status": "failed",
                "batch_errors": [terminal_error],
                "request_counts": {"total": 0, "completed": 0, "failed": 0},
                "in_progress_at": None,
                "output_file_id": None,
                "error_file_id": None,
                "recovery_attempt_count": 0,
                "recovery_reason": "file_visibility_validation_failure",
                "recovery_eligible": True,
                "original_file_id": state["input_file_id"],
                "original_batch_id": state["batch_id"],
                "original_terminal_error": [terminal_error],
                "original_request_counts": {
                    "total": 0,
                    "completed": 0,
                    "failed": 0,
                },
                "recovery_nonce": "fixture-recovery-nonce",
                "recovery_state": "RECOVERY_REQUIRED",
                "state_revision": int(state["state_revision"]) + 1,
            }
        )
        self.s3.put_object(
            Bucket=self.bucket,
            Key=state_key,
            Body=(json.dumps(state) + "\n").encode("utf-8"),
        )
        shutil.rmtree(self.runtime_root)
        self.client.upload_id = "file-recovery-issue2"
        self.client.batch_id = "batch-recovery-issue2"
        self.client.file_value = {
            "id": "file-recovery-issue2",
            "purpose": "batch",
            "status": "processed",
        }
        self.client.upload_calls = 0
        self.client.create_calls = 0
        return state_key

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

    def test_phase_recovery_uses_s3_claim_and_submits_once(self):
        self.phase_a()
        state_key = self.prefix + "/state.json"
        state = self.s3.json(self.bucket, state_key)
        state.update(
            {
                "state": "RECOVERY_REQUIRED",
                "batch_status": "failed",
                "batch_errors": [
                    {
                        "code": "invalid_request",
                        "param": "file_id",
                        "message": "Cannot find file",
                        "line": None,
                    }
                ],
                "request_counts": {"total": 0, "completed": 0, "failed": 0},
                "in_progress_at": None,
                "recovery_attempt_count": 0,
                "recovery_reason": "file_visibility_validation_failure",
                "recovery_eligible": True,
                "original_file_id": state["input_file_id"],
                "original_batch_id": state["batch_id"],
                "original_terminal_error": [
                    {
                        "code": "invalid_request",
                        "param": "file_id",
                        "message": "Cannot find file",
                        "line": None,
                    }
                ],
                "original_request_counts": {
                    "total": 0,
                    "completed": 0,
                    "failed": 0,
                },
                "recovery_nonce": "fixture-recovery-nonce",
                "recovery_state": "RECOVERY_REQUIRED",
                "state_revision": int(state["state_revision"]) + 1,
            }
        )
        self.s3.put_object(
            Bucket=self.bucket,
            Key=state_key,
            Body=(json.dumps(state) + "\n").encode("utf-8"),
        )
        shutil.rmtree(self.runtime_root)
        self.client.upload_id = "file-recovery-issue2"
        self.client.batch_id = "batch-recovery-issue2"
        self.client.file_value = {
            "id": "file-recovery-issue2",
            "purpose": "batch",
            "status": "processed",
        }
        first = orchestration.phase_recovery(
            self.pipeline_run_id,
            self.run_date,
            s3=self.s3,
            runtime_root=self.runtime_root,
            client=self.client,
        )
        second = orchestration.phase_recovery(
            self.pipeline_run_id,
            self.run_date,
            s3=self.s3,
            runtime_root=self.runtime_root,
            client=self.client,
        )
        recovered = self.s3.json(self.bucket, state_key)
        self.assertEqual("batch-recovery-issue2", first["batch_id"])
        self.assertTrue(second["resumed"])
        self.assertEqual(1, recovered["recovery_attempt_count"])
        self.assertEqual("RECOVERY_SUBMITTED", recovered["recovery_state"])
        self.assertIn(
            (self.bucket, self.prefix + "/recovery.claim"), self.s3.objects
        )
        self.assertEqual(2, self.client.upload_calls)
        self.assertEqual(2, self.client.create_calls)

    def test_remote_recovery_claim_is_create_only(self):
        payload = b'{"recovery_nonce":"fixture"}\n'
        orchestration._acquire_remote_recovery_claim(
            self.s3, self.bucket, self.prefix, payload
        )
        with self.assertRaises(engine.SubmissionBlocked):
            orchestration._acquire_remote_recovery_claim(
                self.s3, self.bucket, self.prefix, payload
            )

    def test_recovery_attempt_checkpoint_failure_calls_no_external_api(self):
        state_key = self.prepare_recovery()

        def fail_claimed_checkpoint(_bucket, key, body, _kwargs):
            if key != state_key:
                return
            state = json.loads(body.decode("utf-8"))
            if state.get("recovery_state") == engine.RECOVERY_CLAIMED:
                raise RuntimeError("fixture S3 checkpoint failure")

        self.s3.before_put = fail_claimed_checkpoint
        with self.assertRaises(RuntimeError):
            orchestration.phase_recovery(
                self.pipeline_run_id,
                self.run_date,
                s3=self.s3,
                runtime_root=self.runtime_root,
                client=self.client,
            )
        remote = self.s3.json(self.bucket, state_key)
        self.assertEqual(0, remote["recovery_attempt_count"])
        self.assertEqual(0, self.client.upload_calls)
        self.assertEqual(0, self.client.create_calls)
        self.assertNotIn(
            (self.bucket, self.prefix + "/recovery.claim"), self.s3.objects
        )

    def test_claim_creation_crash_has_durable_attempt_and_owned_resume(self):
        state_key = self.prepare_recovery()
        original = orchestration._acquire_remote_recovery_claim

        def crash_after_claim(*args, **kwargs):
            original(*args, **kwargs)
            raise SimulatedCrash("fixture crash after remote claim")

        with mock.patch.object(
            orchestration,
            "_acquire_remote_recovery_claim",
            side_effect=crash_after_claim,
        ), self.assertRaises(SimulatedCrash):
            orchestration.phase_recovery(
                self.pipeline_run_id,
                self.run_date,
                s3=self.s3,
                runtime_root=self.runtime_root,
                client=self.client,
            )
        checkpoint = self.s3.json(self.bucket, state_key)
        self.assertEqual(1, checkpoint["recovery_attempt_count"])
        self.assertEqual(engine.RECOVERY_CLAIMED, checkpoint["recovery_state"])
        self.assertTrue(checkpoint["recovery_claim_id"])
        for field in (
            "original_file_id",
            "original_batch_id",
            "input_sha256",
            "manifest_sha256",
            "manifest_file_sha256",
        ):
            self.assertTrue(checkpoint[field])
        claim = self.s3.json(self.bucket, self.prefix + "/recovery.claim")
        for field in engine.RECOVERY_CLAIM_IDENTITY_FIELDS:
            self.assertEqual(str(checkpoint[field]), str(claim[field]))
        self.assertEqual(0, self.client.upload_calls)
        self.assertEqual(0, self.client.create_calls)

        shutil.rmtree(self.runtime_root)
        resumed = orchestration.phase_recovery(
            self.pipeline_run_id,
            self.run_date,
            s3=self.s3,
            runtime_root=self.runtime_root,
            client=self.client,
        )
        self.assertTrue(resumed["resumed"])
        self.assertEqual(1, self.client.upload_calls)
        self.assertEqual(1, self.client.create_calls)

    def test_existing_remote_claim_nonce_mismatch_denies_resume(self):
        self.prepare_recovery()
        original = orchestration._acquire_remote_recovery_claim

        def crash_after_claim(*args, **kwargs):
            original(*args, **kwargs)
            raise SimulatedCrash("fixture crash after remote claim")

        with mock.patch.object(
            orchestration,
            "_acquire_remote_recovery_claim",
            side_effect=crash_after_claim,
        ), self.assertRaises(SimulatedCrash):
            orchestration.phase_recovery(
                self.pipeline_run_id,
                self.run_date,
                s3=self.s3,
                runtime_root=self.runtime_root,
                client=self.client,
            )
        claim_key = self.prefix + "/recovery.claim"
        claim = self.s3.json(self.bucket, claim_key)
        claim["recovery_nonce"] = "different-nonce"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=claim_key,
            Body=(json.dumps(claim) + "\n").encode("utf-8"),
        )
        shutil.rmtree(self.runtime_root)
        with self.assertRaises(engine.SubmissionBlocked):
            orchestration.phase_recovery(
                self.pipeline_run_id,
                self.run_date,
                s3=self.s3,
                runtime_root=self.runtime_root,
                client=self.client,
            )
        self.assertEqual(0, self.client.upload_calls)
        self.assertEqual(0, self.client.create_calls)

    def test_recovery_file_checkpoint_restart_skips_reupload(self):
        state_key = self.prepare_recovery()
        original = orchestration._persist_recovery_checkpoint

        def crash_after_file(*args, **kwargs):
            revision = original(*args, **kwargs)
            state = args[3]
            if state.get("recovery_state") == engine.RECOVERY_FILE_UPLOADED:
                raise SimulatedCrash("fixture crash after recovery file checkpoint")
            return revision

        with mock.patch.object(
            orchestration,
            "_persist_recovery_checkpoint",
            side_effect=crash_after_file,
        ), self.assertRaises(SimulatedCrash):
            orchestration.phase_recovery(
                self.pipeline_run_id,
                self.run_date,
                s3=self.s3,
                runtime_root=self.runtime_root,
                client=self.client,
            )
        checkpoint = self.s3.json(self.bucket, state_key)
        self.assertEqual(
            engine.RECOVERY_FILE_UPLOADED, checkpoint["recovery_state"]
        )
        self.assertEqual(1, self.client.upload_calls)
        shutil.rmtree(self.runtime_root)
        orchestration.phase_recovery(
            self.pipeline_run_id,
            self.run_date,
            s3=self.s3,
            runtime_root=self.runtime_root,
            client=self.client,
        )
        self.assertEqual(1, self.client.upload_calls)
        self.assertEqual(1, self.client.create_calls)

    def test_pending_checkpoint_is_s3_durable_before_provider_create(self):
        state_key = self.prepare_recovery()
        original_create = self.client.create_batch

        def assert_pending_then_create(input_file_id, metadata):
            remote = self.s3.json(self.bucket, state_key)
            self.assertEqual(
                engine.RECOVERY_PENDING_RECONCILIATION,
                remote["state"],
            )
            self.assertEqual(
                engine.RECOVERY_PENDING_RECONCILIATION,
                remote["recovery_state"],
            )
            return original_create(input_file_id, metadata)

        with mock.patch.object(
            self.client, "create_batch", side_effect=assert_pending_then_create
        ):
            orchestration.phase_recovery(
                self.pipeline_run_id,
                self.run_date,
                s3=self.s3,
                runtime_root=self.runtime_root,
                client=self.client,
            )
        self.assertEqual(1, self.client.create_calls)

    def test_pending_checkpoint_failure_calls_provider_create_zero(self):
        state_key = self.prepare_recovery()

        def fail_pending_checkpoint(_bucket, key, body, _kwargs):
            if key != state_key:
                return
            state = json.loads(body.decode("utf-8"))
            if state.get("recovery_state") == engine.RECOVERY_PENDING_RECONCILIATION:
                raise RuntimeError("fixture pending S3 failure")

        self.s3.before_put = fail_pending_checkpoint
        with self.assertRaises(RuntimeError):
            orchestration.phase_recovery(
                self.pipeline_run_id,
                self.run_date,
                s3=self.s3,
                runtime_root=self.runtime_root,
                client=self.client,
            )
        remote = self.s3.json(self.bucket, state_key)
        self.assertEqual(engine.RECOVERY_FILE_UPLOADED, remote["recovery_state"])
        self.assertEqual(0, self.client.create_calls)

    def test_create_response_persist_crash_reconciles_without_duplicate(self):
        state_key = self.prepare_recovery()
        created = {}
        original_create = self.client.create_batch

        def remember_create(input_file_id, metadata):
            created.update(original_create(input_file_id, metadata))
            return dict(created)

        def fail_submitted_checkpoint(_bucket, key, body, _kwargs):
            if key != state_key:
                return
            state = json.loads(body.decode("utf-8"))
            if state.get("recovery_state") == engine.RECOVERY_SUBMITTED:
                raise RuntimeError("fixture response persist crash")

        self.s3.before_put = fail_submitted_checkpoint
        with mock.patch.object(
            self.client, "create_batch", side_effect=remember_create
        ), self.assertRaises(RuntimeError):
            orchestration.phase_recovery(
                self.pipeline_run_id,
                self.run_date,
                s3=self.s3,
                runtime_root=self.runtime_root,
                client=self.client,
            )
        pending = self.s3.json(self.bucket, state_key)
        self.assertEqual(
            engine.RECOVERY_PENDING_RECONCILIATION, pending["state"]
        )
        self.assertEqual(1, self.client.create_calls)

        self.s3.before_put = None
        shutil.rmtree(self.runtime_root)
        resumed = orchestration.phase_recovery(
            self.pipeline_run_id,
            self.run_date,
            s3=self.s3,
            runtime_root=self.runtime_root,
            client=self.client,
        )
        self.assertTrue(resumed["resumed"])
        self.assertEqual(1, self.client.create_calls)

        def openai_get(path, _api_key, _query=None):
            if path == "/batches":
                return {"data": [dict(created)]}
            if path == "/batches/batch-recovery-issue2":
                return dict(created)
            raise AssertionError(path)

        with mock.patch.object(status_lambda, "_openai_get", side_effect=openai_get):
            status = status_lambda.check_status(
                {"run_id": self.pipeline_run_id, "run_date": self.run_date},
                s3=self.s3,
                api_key="fixture",
            )
        reconciled = self.s3.json(self.bucket, state_key)
        self.assertEqual("WAIT", status["outcome"])
        self.assertEqual(engine.RECOVERY_SUBMITTED, reconciled["recovery_state"])
        self.assertEqual("batch-recovery-issue2", reconciled["batch_id"])
        self.assertEqual(1, self.client.create_calls)

    def test_create_response_persist_failure_same_ec2_syncs_batch_id(self):
        state_key = self.prepare_recovery()

        def fail_submitted_checkpoint(_bucket, key, body, _kwargs):
            if key != state_key:
                return
            state = json.loads(body.decode("utf-8"))
            if state.get("recovery_state") == engine.RECOVERY_SUBMITTED:
                raise RuntimeError("fixture response persist failure")

        self.s3.before_put = fail_submitted_checkpoint
        with self.assertRaises(RuntimeError):
            orchestration.phase_recovery(
                self.pipeline_run_id,
                self.run_date,
                s3=self.s3,
                runtime_root=self.runtime_root,
                client=self.client,
            )
        self.assertEqual(1, self.client.create_calls)
        self.assertEqual(
            engine.RECOVERY_PENDING_RECONCILIATION,
            self.s3.json(self.bucket, state_key)["state"],
        )

        self.s3.before_put = None
        resumed = orchestration.phase_recovery(
            self.pipeline_run_id,
            self.run_date,
            s3=self.s3,
            runtime_root=self.runtime_root,
            client=self.client,
        )
        durable = self.s3.json(self.bucket, state_key)
        self.assertTrue(resumed["resumed"])
        self.assertEqual(engine.RECOVERY_SUBMITTED, durable["recovery_state"])
        self.assertEqual("batch-recovery-issue2", durable["batch_id"])
        self.assertEqual(1, self.client.create_calls)

    def test_two_recovery_processes_create_one_batch_by_s3_revision_cas(self):
        self.prepare_recovery()
        runtime_a = Path(self.temporary.name) / "runtime-a"
        runtime_b = Path(self.temporary.name) / "runtime-b"
        barrier = threading.Barrier(2)
        original = orchestration._persist_recovery_checkpoint

        def synchronize_claimed(*args, **kwargs):
            state = args[3]
            if state.get("recovery_state") == engine.RECOVERY_CLAIMED:
                barrier.wait(timeout=2)
            return original(*args, **kwargs)

        def invoke(runtime_root):
            try:
                orchestration.phase_recovery(
                    self.pipeline_run_id,
                    self.run_date,
                    s3=self.s3,
                    runtime_root=runtime_root,
                    client=self.client,
                )
                return "submitted"
            except Exception:
                return "blocked"

        with mock.patch.object(
            orchestration,
            "_persist_recovery_checkpoint",
            side_effect=synchronize_claimed,
        ), ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = list(executor.map(invoke, (runtime_a, runtime_b)))
        self.assertEqual(1, outcomes.count("submitted"))
        self.assertEqual(1, self.client.upload_calls)
        self.assertEqual(1, self.client.create_calls)

    def test_old_local_pending_remote_required_is_synced_without_resubmit(self):
        state_key = self.prepare_recovery()
        run_dir = orchestration.restore_run(
            self.s3,
            self.bucket,
            self.prefix,
            self.pipeline_run_id,
            self.run_date,
            self.runtime_root,
        )
        store = engine.FileStateStore(run_dir)
        state, etag = store.load()
        state.update(
            {
                "recovery_attempt_count": 1,
                "recovery_state": engine.RECOVERY_CLAIMED,
                "recovery_claim_id": "fixture-owned-claim",
                "recovery_started_at": engine.utc_now(),
            }
        )
        store.cas(etag, state)
        state, etag = store.load()
        store.acquire_recovery_claim(
            state["recovery_nonce"], engine._recovery_claim_identity(state)
        )
        orchestration._acquire_remote_recovery_claim(
            self.s3,
            self.bucket,
            self.prefix,
            store.recovery_claim_path.read_bytes(),
        )
        state["recovery_file_id"] = "file-recovery-issue2"
        state["recovery_state"] = engine.RECOVERY_FILE_UPLOADED
        store.cas(etag, state)
        state, etag = store.load()
        state.update(
            {
                "state": engine.RECOVERY_PENDING_RECONCILIATION,
                "recovery_state": engine.RECOVERY_PENDING_RECONCILIATION,
                "batch_id": None,
            }
        )
        store.cas(etag, state)
        self.client.create_calls = 1

        result = orchestration.phase_recovery(
            self.pipeline_run_id,
            self.run_date,
            s3=self.s3,
            runtime_root=self.runtime_root,
            client=self.client,
        )
        durable = self.s3.json(self.bucket, state_key)
        self.assertTrue(result["resumed"])
        self.assertEqual(
            engine.RECOVERY_PENDING_RECONCILIATION, durable["state"]
        )
        self.assertEqual(1, self.client.create_calls)

        with mock.patch.object(
            status_lambda, "_openai_get", return_value={"data": []}
        ):
            for _ in range(3):
                status_lambda._reconcile_pending(durable, "fixture")
        self.assertEqual(engine.STATE_SAFE_STOPPED, durable["state"])
        self.assertNotEqual(engine.STATE_RECOVERY_REQUIRED, durable["state"])

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

    def test_lambda_eligible_failure_requests_ec2_recovery_without_pipeline_fail(self):
        eligible_state = dict(self.state)
        eligible_state["recovery_attempt_count"] = 0
        self._put(self.state_key, eligible_state)
        observed = {
            "id": "batch-1",
            "status": "failed",
            "request_counts": {"total": 0, "completed": 0, "failed": 0},
            "in_progress_at": None,
            "output_file_id": None,
            "error_file_id": None,
            "errors": {
                "data": [
                    {
                        "code": "invalid_request",
                        "message": "Cannot find file or organization does not have access",
                        "param": "file_id",
                    }
                ]
            },
        }
        with mock.patch.object(
            status_lambda, "_openai_get", return_value=observed
        ) as openai_get:
            result = status_lambda.check_status(
                {"run_id": self.run_id, "run_date": self.run_date},
                s3=self.s3,
                api_key="fixture",
            )
            first_nonce = self.s3.json("technoverse", self.state_key)[
                "recovery_nonce"
            ]
            repeated = status_lambda.check_status(
                {"run_id": self.run_id, "run_date": self.run_date},
                s3=self.s3,
                api_key="fixture",
            )
        state = self.s3.json("technoverse", self.state_key)
        pipeline = self.s3.json("technoverse", self.pipeline_key)
        self.assertEqual("RECOVERY_REQUIRED", result["outcome"])
        self.assertEqual("RECOVERY_REQUIRED", repeated["outcome"])
        self.assertEqual(1, openai_get.call_count)
        self.assertEqual(first_nonce, state["recovery_nonce"])
        self.assertEqual("RECOVERY_REQUIRED", state["state"])
        self.assertEqual("RUNNING", pipeline["status"])
        self.assertEqual("file-input", state["original_file_id"])
        self.assertEqual("batch-1", state["original_batch_id"])

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
        self.assertEqual(routes["RECOVERY_REQUIRED"], "PrepareBatchRecoveryStart")

    def test_asl_recovery_runs_ec2_command_then_returns_to_batch_wait(self):
        definition = asl_patch.apply_patch(self._base_asl())
        states = definition["States"]
        command = states["SendBatchRecoveryCommand"]["Parameters"]["Parameters"][
            "commands.$"
        ]
        self.assertIn("phase-recovery", command)
        self.assertEqual(
            "StopEC2AfterBatchRecovery",
            states["RouteAfterBatchCommandSuccess"]["Choices"][0]["Next"],
        )
        self.assertEqual(
            "WaitForBatchStatus", states["StopEC2AfterBatchRecovery"]["Next"]
        )
        for name in (
            "IncrementPhaseBEC2Wait",
            "IncrementPhaseBSSMWait",
            "IncrementPhaseBLauncherWait",
        ):
            self.assertEqual(
                "$.batch_recovery", states[name]["Parameters"]["batch_recovery.$"]
            )

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

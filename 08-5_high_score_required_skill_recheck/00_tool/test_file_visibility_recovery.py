"""Focused tests for one-time 08-5 file visibility recovery (no live API/AWS)."""

import importlib.util
import json
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock


TOOL_DIR = Path(__file__).resolve().parent
STEP_DIR = TOOL_DIR.parent
sys.path.insert(0, str(TOOL_DIR))

import high_score_required_skill_recheck_batch as engine  # noqa: E402


def _load_lambda():
    path = STEP_DIR / "aws/lambda_function.py"
    spec = importlib.util.spec_from_file_location("recovery_status_lambda", str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


status_lambda = _load_lambda()


def _context():
    record = {
        "project_info": {
            "message_id": "project-recovery",
            "required_skills": [
                {"skill": "Python開発経験", "match": True, "note": "fixture"}
            ],
        },
        "resource_info": {"message_id": "resource-recovery"},
        "match_info": {"score": 100},
    }
    return engine._build_request_context(
        record,
        "100percent",
        1,
        {
            "resource-recovery": {
                "success": True,
                "skillsheet": "Python開発経験3年",
            }
        },
        {"project-recovery": "Python開発案件"},
    )


def _visibility_failure(**updates):
    value = {
        "id": "batch-original",
        "status": "failed",
        "request_counts": {"total": 0, "completed": 0, "failed": 0},
        "in_progress_at": None,
        "output_file_id": None,
        "error_file_id": None,
        "errors": {
            "data": [
                {
                    "code": "invalid_request",
                    "param": "file_id",
                    "message": (
                        "Cannot find file file-original or organization "
                        "does not have access"
                    ),
                }
            ]
        },
    }
    value.update(updates)
    return value


def _eligibility_state(attempt=0):
    return {
        "recovery_attempt_count": attempt,
        "input_file_id": "file-original",
        "batch_id": "batch-original",
    }


class RecoveryClient:
    def __init__(self):
        self.upload_calls = 0
        self.create_calls = 0
        self.upload_id = "file-recovery"
        self.file_value = {
            "id": self.upload_id,
            "purpose": "batch",
            "status": "processed",
        }
        self.create_value = {
            "id": "batch-recovery",
            "status": "validating",
            "input_file_id": self.upload_id,
        }
        self.list_value = []
        self.upload_delay = 0.0
        self.lock = threading.Lock()

    def upload_input(self, _path):
        with self.lock:
            self.upload_calls += 1
        if self.upload_delay:
            time.sleep(self.upload_delay)
        return self.upload_id

    def retrieve_file(self, _file_id):
        return dict(self.file_value)

    def create_batch(self, input_file_id, metadata):
        with self.lock:
            self.create_calls += 1
        result = dict(self.create_value)
        result["input_file_id"] = input_file_id
        result["metadata"] = metadata
        return result

    def list_batches(self):
        return [dict(item) for item in self.list_value]


class OneTimeFileVisibilityRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.run_id = "recovery-fixture"
        engine.prepare_run(self.run_id, self.root, [_context()])
        self.run_dir = self.root / self.run_id
        self._mark_recovery_required()
        self.zero_stabilization = mock.patch.object(
            engine, "FILE_STABILIZATION_WINDOW_SECONDS", 0.0
        )
        self.zero_stabilization.start()

    def tearDown(self):
        self.zero_stabilization.stop()
        self.temporary.cleanup()

    def _mark_recovery_required(self):
        store = engine.FileStateStore(self.run_dir)
        state, etag = store.load()
        state.update(
            {
                "state": engine.STATE_RECOVERY_REQUIRED,
                "input_file_id": "file-original",
                "batch_id": "batch-original",
                "batch_status": "failed",
                "batch_errors": _visibility_failure()["errors"]["data"],
                "request_counts": {"total": 0, "completed": 0, "failed": 0},
                "in_progress_at": None,
                "recovery_attempt_count": 0,
                "recovery_reason": "file_visibility_validation_failure",
                "recovery_eligible": True,
                "original_file_id": "file-original",
                "original_batch_id": "batch-original",
                "original_terminal_error": _visibility_failure()["errors"]["data"],
                "original_request_counts": {
                    "total": 0,
                    "completed": 0,
                    "failed": 0,
                },
                "recovery_nonce": "recovery-nonce",
                "recovery_state": engine.STATE_RECOVERY_REQUIRED,
            }
        )
        store.cas(etag, state)

    def test_a_file_id_zero_request_failure_is_eligible(self):
        report = status_lambda._recovery_eligibility(
            _eligibility_state(), _visibility_failure()
        )
        self.assertEqual(
            {"eligible": True, "reason": "file_visibility_validation_failure"},
            report,
        )

    def test_b_same_input_reupload_stabilize_and_one_new_batch(self):
        client = RecoveryClient()
        result = engine.recover_file_visibility_failure(
            self.run_id, client, self.root
        )
        state, _ = engine.FileStateStore(self.run_dir).load()
        self.assertTrue(result["recovered"])
        self.assertEqual(1, client.upload_calls)
        self.assertEqual(1, client.create_calls)
        self.assertEqual("file-recovery", state["recovery_file_id"])
        self.assertEqual("batch-recovery", state["recovery_batch_id"])
        self.assertNotEqual(state["original_file_id"], state["recovery_file_id"])
        self.assertNotEqual(state["original_batch_id"], state["recovery_batch_id"])
        self.assertEqual("ready", state["recovery_file_readiness"]["readiness_result"])

    def test_c_processed_request_forbids_recovery(self):
        failure = _visibility_failure(
            request_counts={"total": 1, "completed": 1, "failed": 0}
        )
        self.assertFalse(
            status_lambda._recovery_eligibility(
                _eligibility_state(), failure
            )["eligible"]
        )

    def test_d_non_file_id_param_forbids_recovery(self):
        failure = _visibility_failure()
        failure["errors"]["data"][0]["param"] = "model"
        self.assertFalse(
            status_lambda._recovery_eligibility(
                _eligibility_state(), failure
            )["eligible"]
        )

    def test_e_unknown_error_forbids_recovery(self):
        failure = _visibility_failure(errors={})
        self.assertFalse(
            status_lambda._recovery_eligibility(
                _eligibility_state(), failure
            )["eligible"]
        )

    def test_f_attempt_one_forbids_recovery(self):
        self.assertFalse(
            status_lambda._recovery_eligibility(
                _eligibility_state(1), _visibility_failure()
            )["eligible"]
        )

    def test_g_input_digest_mismatch_denies_before_upload(self):
        with (self.run_dir / "input.jsonl").open("ab") as target:
            target.write(b"{}\n")
        client = RecoveryClient()
        with self.assertRaises(engine.SubmissionBlocked):
            engine.recover_file_visibility_failure(self.run_id, client, self.root)
        self.assertEqual(0, client.upload_calls)
        self.assertEqual(0, client.create_calls)

    def test_h_manifest_digest_mismatch_denies_before_upload(self):
        with (self.run_dir / "manifest.jsonl").open("ab") as target:
            target.write(b"{}\n")
        client = RecoveryClient()
        with self.assertRaises(engine.SubmissionBlocked):
            engine.recover_file_visibility_failure(self.run_id, client, self.root)
        self.assertEqual(0, client.upload_calls)
        self.assertEqual(0, client.create_calls)

    def test_i_recovery_stabilization_failure_creates_no_batch(self):
        client = RecoveryClient()
        client.file_value = {
            "id": "file-recovery",
            "purpose": "fine-tune",
            "status": "processed",
        }
        with self.assertRaises(engine.FileReadinessError):
            engine.recover_file_visibility_failure(self.run_id, client, self.root)
        state, _ = engine.FileStateStore(self.run_dir).load()
        self.assertEqual(0, client.create_calls)
        self.assertEqual(1, state["recovery_attempt_count"])
        self.assertEqual(engine.STATE_SAFE_STOPPED, state["state"])

    def test_j_recovery_batch_same_failure_safe_stops_without_second_recovery(self):
        state = _eligibility_state(1)
        report = status_lambda._recovery_eligibility(state, _visibility_failure())
        self.assertFalse(report["eligible"])
        observed_state = {
            "batch_id": "batch-recovery",
            "state": "SUBMITTED",
            "recovery_attempt_count": 1,
            "recovery_state": "RECOVERY_SUBMITTED",
        }
        failure = _visibility_failure(id="batch-recovery")
        status_lambda._observe(observed_state, failure)
        self.assertEqual("SAFE_STOPPED", observed_state["state"])
        self.assertEqual(1, observed_state["recovery_attempt_count"])

    def test_k_restart_preserves_attempt_count_and_resumes_submitted_batch(self):
        client = RecoveryClient()
        first = engine.recover_file_visibility_failure(self.run_id, client, self.root)
        reloaded, _ = engine.FileStateStore(self.run_dir).load()
        self.assertEqual(1, reloaded["recovery_attempt_count"])
        resumed_client = RecoveryClient()
        second = engine.recover_file_visibility_failure(
            self.run_id, resumed_client, self.root
        )
        self.assertEqual(first["batch_id"], second["batch_id"])
        self.assertTrue(second["resumed"])
        self.assertEqual(0, resumed_client.upload_calls)
        self.assertEqual(0, resumed_client.create_calls)

    def test_l_parallel_processes_submit_recovery_once(self):
        client = RecoveryClient()
        client.upload_delay = 0.05

        def invoke():
            try:
                engine.recover_file_visibility_failure(
                    self.run_id, client, self.root
                )
                return "submitted"
            except engine.BatchEngineError:
                return "blocked"

        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = list(executor.map(lambda _index: invoke(), range(2)))
        self.assertEqual(1, outcomes.count("submitted"))
        self.assertEqual(1, client.upload_calls)
        self.assertEqual(1, client.create_calls)

    def test_o_attempt_checkpoint_failure_calls_no_external_api(self):
        client = RecoveryClient()

        def fail_checkpoint(state):
            self.assertEqual(1, state["recovery_attempt_count"])
            self.assertEqual(engine.RECOVERY_CLAIMED, state["recovery_state"])
            raise RuntimeError("fixture checkpoint failure")

        with self.assertRaises(RuntimeError):
            engine.recover_file_visibility_failure(
                self.run_id,
                client,
                self.root,
                checkpoint_callback=fail_checkpoint,
            )
        state, _ = engine.FileStateStore(self.run_dir).load()
        self.assertEqual(1, state["recovery_attempt_count"])
        self.assertFalse((self.run_dir / "recovery.claim").exists())
        self.assertEqual(0, client.upload_calls)
        self.assertEqual(0, client.create_calls)

    def test_p_same_claim_owned_resume_after_claim_crash(self):
        first_client = RecoveryClient()

        def crash_after_claim(_payload, _state):
            raise RuntimeError("fixture crash after claim")

        with self.assertRaises(RuntimeError):
            engine.recover_file_visibility_failure(
                self.run_id,
                first_client,
                self.root,
                claim_callback=crash_after_claim,
            )
        state, _ = engine.FileStateStore(self.run_dir).load()
        self.assertEqual(1, state["recovery_attempt_count"])
        self.assertEqual(engine.RECOVERY_CLAIMED, state["recovery_state"])
        self.assertEqual(0, first_client.upload_calls)

        resumed_client = RecoveryClient()
        result = engine.recover_file_visibility_failure(
            self.run_id, resumed_client, self.root
        )
        self.assertTrue(result["resumed"])
        self.assertEqual(1, resumed_client.upload_calls)
        self.assertEqual(1, resumed_client.create_calls)

    def test_q_existing_claim_nonce_mismatch_denies(self):
        client = RecoveryClient()

        with self.assertRaises(RuntimeError):
            engine.recover_file_visibility_failure(
                self.run_id,
                client,
                self.root,
                claim_callback=lambda _payload, _state: (_ for _ in ()).throw(
                    RuntimeError("fixture crash after claim")
                ),
            )
        claim = json.loads(
            (self.run_dir / "recovery.claim").read_text(encoding="utf-8")
        )
        claim["recovery_nonce"] = "different-nonce"
        engine._atomic_write_json(self.run_dir / "recovery.claim", claim)
        resumed_client = RecoveryClient()
        with self.assertRaises(engine.SubmissionBlocked):
            engine.recover_file_visibility_failure(
                self.run_id, resumed_client, self.root
            )
        self.assertEqual(0, resumed_client.upload_calls)
        self.assertEqual(0, resumed_client.create_calls)

    def test_r_file_checkpoint_resume_reuses_file_id(self):
        first_client = RecoveryClient()

        def crash_after_file_checkpoint(state):
            if state.get("recovery_state") == engine.RECOVERY_FILE_UPLOADED:
                raise RuntimeError("fixture crash after file checkpoint")

        with self.assertRaises(RuntimeError):
            engine.recover_file_visibility_failure(
                self.run_id,
                first_client,
                self.root,
                checkpoint_callback=crash_after_file_checkpoint,
            )
        self.assertEqual(1, first_client.upload_calls)
        self.assertEqual(0, first_client.create_calls)
        resumed_client = RecoveryClient()
        result = engine.recover_file_visibility_failure(
            self.run_id, resumed_client, self.root
        )
        self.assertTrue(result["resumed"])
        self.assertEqual(0, resumed_client.upload_calls)
        self.assertEqual(1, resumed_client.create_calls)

    def test_s_pending_checkpoint_precedes_create(self):
        client = RecoveryClient()
        observed = []

        def observe_checkpoint(state):
            if state.get("recovery_state") == engine.RECOVERY_PENDING_RECONCILIATION:
                observed.append((state["state"], client.create_calls))

        engine.recover_file_visibility_failure(
            self.run_id,
            client,
            self.root,
            checkpoint_callback=observe_checkpoint,
        )
        self.assertEqual(
            [(engine.RECOVERY_PENDING_RECONCILIATION, 0)], observed
        )
        self.assertEqual(1, client.create_calls)

    def test_t_pending_checkpoint_failure_never_creates_batch(self):
        client = RecoveryClient()

        def fail_pending(state):
            if state.get("recovery_state") == engine.RECOVERY_PENDING_RECONCILIATION:
                raise RuntimeError("fixture pending persist failure")

        with self.assertRaises(RuntimeError):
            engine.recover_file_visibility_failure(
                self.run_id,
                client,
                self.root,
                checkpoint_callback=fail_pending,
            )
        self.assertEqual(1, client.upload_calls)
        self.assertEqual(0, client.create_calls)
        restart_client = RecoveryClient()
        with self.assertRaises(engine.PendingReconciliation):
            engine.recover_file_visibility_failure(
                self.run_id, restart_client, self.root
            )
        self.assertEqual(0, restart_client.upload_calls)
        self.assertEqual(0, restart_client.create_calls)

    def test_u_recovery_pending_reconciliation_adopts_exact_match(self):
        client = RecoveryClient()

        def stop_at_pending(state):
            if state.get("recovery_state") == engine.RECOVERY_PENDING_RECONCILIATION:
                raise RuntimeError("fixture stop at pending")

        with self.assertRaises(RuntimeError):
            engine.recover_file_visibility_failure(
                self.run_id,
                client,
                self.root,
                checkpoint_callback=stop_at_pending,
            )
        pending, _ = engine.FileStateStore(self.run_dir).load()
        client.list_value = [
            {
                "id": "batch-recovery",
                "status": "validating",
                "input_file_id": pending["recovery_file_id"],
                "metadata": engine._batch_metadata(pending, recovery=True),
            }
        ]
        report = engine.reconcile_pending(
            self.run_id, client, self.root, max_checks=1
        )
        reconciled, _ = engine.FileStateStore(self.run_dir).load()
        self.assertEqual(1, report["match_count"])
        self.assertEqual(engine.RECOVERY_SUBMITTED, reconciled["recovery_state"])
        self.assertEqual("batch-recovery", reconciled["recovery_batch_id"])
        self.assertEqual(0, client.create_calls)

    def test_m_normal_completed_path_remains_completed(self):
        state = {"batch_id": "batch-original", "state": "SUBMITTED"}
        status = status_lambda._observe(
            state,
            {
                "id": "batch-original",
                "status": "completed",
                "request_counts": {"total": 1, "completed": 1, "failed": 0},
                "output_file_id": "file-output",
            },
        )
        self.assertEqual("completed", status)
        self.assertEqual("COMPLETED", state["state"])

    def test_n_ordinary_terminal_failure_remains_no_recovery(self):
        state = {
            "batch_id": "batch-original",
            "state": "SUBMITTED",
            "recovery_attempt_count": 0,
        }
        failure = _visibility_failure()
        failure["errors"]["data"][0].update(
            {"param": "model", "message": "Invalid model parameter"}
        )
        status_lambda._observe(state, failure)
        self.assertEqual("SAFE_STOPPED", state["state"])
        self.assertFalse(state["recovery_eligible"])


if __name__ == "__main__":
    unittest.main()

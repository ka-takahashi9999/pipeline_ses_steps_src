"""08-5 production Batch engineのfocused/integrity tests。"""

import json
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import batch_minimal_safety_guard as guard
import high_score_required_skill_recheck as direct
import high_score_required_skill_recheck_batch as batch
import high_score_required_skill_recheck_core as core


SAVED_RUN = (
    Path(__file__).resolve().parents[1]
    / "_test_batch_api_canary/canary678-20260822-01"
)


def load_batch_confirm():
    path = Path(__file__).resolve().parents[1] / "02_confirm/confirm_batch_production_engine.py"
    spec = importlib.util.spec_from_file_location("confirm_batch_production_engine_test", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Batch confirm import failed")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeClient:
    def __init__(self):
        self.upload_calls = 0
        self.file_retrieve_calls = 0
        self.create_calls = 0
        self.retrieve_calls = 0
        self.list_calls = 0
        self.upload_id = "file-input-1"
        self.file_values = [{
            "id": "file-input-1",
            "purpose": "batch",
            "status": "processed",
        }]
        self.file_lookup_error = None
        self.events = []
        self.create_value = {
            "id": "batch-1",
            "status": "validating",
            "input_file_id": "file-input-1",
        }
        self.create_error = None
        self.retrieve_value = {
            "id": "batch-1",
            "status": "in_progress",
            "input_file_id": "file-input-1",
        }
        self.list_values = []
        self.list_callback = None
        self.downloads = {}

    def upload_input(self, _path):
        self.upload_calls += 1
        self.events.append("upload")
        return self.upload_id

    def retrieve_file(self, _file_id):
        self.file_retrieve_calls += 1
        if self.file_lookup_error:
            self.events.append("file:lookup_failed")
            raise self.file_lookup_error
        value = self.file_values[0]
        if len(self.file_values) > 1:
            self.file_values.pop(0)
        if isinstance(value, Exception):
            self.events.append("file:lookup_failed")
            raise value
        self.events.append(f"file:{value.get('status')}")
        return value

    def create_batch(self, _input_file_id, _metadata):
        self.create_calls += 1
        self.events.append("create")
        if self.create_error:
            raise self.create_error
        return self.create_value

    def retrieve_batch(self, _batch_id):
        self.retrieve_calls += 1
        return self.retrieve_value

    def list_batches(self):
        self.list_calls += 1
        if self.list_callback is not None:
            callback = self.list_callback
            self.list_callback = None
            callback()
        return list(self.list_values)

    def download_file(self, file_id):
        return self.downloads[file_id]


def source_record(index=1, skill="Python開発経験"):
    return {
        "project_info": {
            "message_id": f"project-{index}",
            "required_skills": [{"skill": skill, "match": True, "note": "fixture"}],
        },
        "resource_info": {"message_id": f"resource-{index}"},
        "match_info": {"score": 100},
    }


def context(index=1, skill="Python開発経験"):
    record = source_record(index, skill)
    return batch._build_request_context(
        record,
        "100percent",
        index,
        {
            f"resource-{index}": {
                "success": True,
                "skillsheet": f"{skill}\nPython経験3年",
            }
        },
        {f"project-{index}": "Python開発案件"},
    )


def manifest_entry(index=1, skill="Python開発経験"):
    item, _ = batch._manifest_entry("fixture", index, context(index, skill))
    return item


def success_row(
    entry, confidence="confirmed", evidence="Python経験3年", finish_reason="stop"
):
    parsed = {
        "required_skill_checks": [
            {
                "skill": entry["required_skills"][0]["skill"],
                "confidence": confidence,
                "reason": "fixture reason",
                "evidence": evidence,
            }
        ],
        "category_match": "match",
        "category_note": "案件: Python / 要員: Python",
    }
    return {
        "custom_id": entry["custom_id"],
        "response": {
            "status_code": 200,
            "body": {
                "choices": [{
                    "message": {"content": json.dumps(parsed)},
                    "finish_reason": finish_reason,
                }]
            },
        },
    }


def jsonl_bytes(records):
    return b"".join(
        (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8")
        for record in records
    )


def set_state(run_dir, **updates):
    store = batch.FileStateStore(run_dir)
    state, etag = store.load()
    state.update(updates)
    store.cas(etag, state)


class SharedCoreDirectRegressionTest(unittest.TestCase):
    def test_saved_canary_request_matches_shared_request_builder(self):
        saved_input = next(iter(batch.read_jsonl(str(SAVED_RUN / "input.jsonl"))))
        production_context = batch.load_production_contexts()[0]
        self.assertEqual(saved_input["body"], production_context["body"])
        self.assertEqual(direct.RECHECK_LLM_MODEL, saved_input["body"]["model"])

    def test_shared_validator_keeps_direct_output_contract(self):
        required = [{"skill": "Python", "match": True}]
        raw = [{"skill": "Python", "confidence": "confirmed", "reason": "ok", "evidence": "Python"}]
        direct_value = direct._validate_required_skill_checks(required, raw)
        core_value = core.normalize_required_skill_checks(
            required, raw, direct.VALID_CONFIDENCES, direct._skill_text
        )
        self.assertEqual(direct_value, core_value)

    def test_full_prepare_keeps_four_local_fallbacks_and_678_batch_requests(self):
        contexts = batch.load_production_contexts()
        self.assertEqual(682, len(contexts))
        self.assertEqual(
            4, sum(item["dispatch"] == "local_fallback" for item in contexts)
        )
        with tempfile.TemporaryDirectory() as temporary:
            report = batch.prepare_run("offline-678", Path(temporary), contexts)
            self.assertEqual(682, report["manifest_count"])
            self.assertEqual(678, report["request_count"])
            self.assertTrue(report["custom_id_unique"])


class ProductionGuardTest(unittest.TestCase):
    @staticmethod
    def apply(skill, skillsheet, confidence="confirmed", schema_valid=True, category="match"):
        checks = [{
            "skill": skill,
            "original_match": False,
            "recheck_match": confidence != "not_confirmed",
            "confidence": confidence,
            "reason": "fixture",
            "evidence": "対象経験あり",
        }]
        return guard.apply_minimal_safety_guard(
            checks, category, schema_valid, skillsheet
        )

    def test_optional_welcome_and_preferred_are_human_review(self):
        for skill in ("金融経験者は歓迎", "AWS経験があれば尚可", "クラウド経験推奨"):
            with self.subTest(skill=skill):
                checks, _, metadata = self.apply(skill, "Linux経験", "not_confirmed")
                self.assertEqual("human_review", checks[0]["confidence"])
                self.assertIn("optional_condition:1", metadata["guard_reasons"])

    def test_schema_invalid_mismatch_becomes_unclear(self):
        checks, category, metadata = self.apply(
            "AWS経験", "AWS経験", "human_review", False, "mismatch"
        )
        self.assertEqual("human_review", checks[0]["confidence"])
        self.assertEqual("unclear", category)
        self.assertIn("schema_invalid_category_mismatch", metadata["guard_reasons"])

    def test_two_years_one_year_three_months_blocks_confirmed(self):
        skillsheet = (
            "1 | 2024 | 年 | 1 | 月 | React Native\nReact Native実装\n"
            "規模\n1 | 年 | 3 | ヶ月\n■スキル\nReact Native | B"
        )
        checks, _, _ = self.apply(
            "Flutter or ReactNative実装経験2年以上", skillsheet
        )
        self.assertEqual("human_review", checks[0]["confidence"])

    def test_or_technology_periods_are_not_summed(self):
        skillsheet = (
            "1 | 2024 | 年 | 1 | 月 | Flutter\nFlutter実装\n規模\n0 | 年 | 6 | ヶ月\n"
            "2 | 2024 | 年 | 7 | 月 | React Native\nReact Native実装\n規模\n0 | 年 | 7 | ヶ月"
        )
        checks, _, _ = self.apply(
            "Flutter or ReactNative実装経験2年以上", skillsheet
        )
        self.assertEqual("human_review", checks[0]["confidence"])

    def test_satisfied_target_duration_is_kept(self):
        skillsheet = (
            "1 | 2023 | 年 | 1 | 月 | React Native\nReact Native実装\n"
            "規模\n2 | 年 | 3 | ヶ月"
        )
        checks, _, metadata = self.apply("React Native実装経験2年以上", skillsheet)
        self.assertEqual("confirmed", checks[0]["confidence"])
        self.assertEqual([], metadata["guard_reasons"])

    def test_other_technology_and_sample_duration_are_not_used(self):
        for skillsheet in (
            "Python経験5年\nJavaは学習のみ",
            "Javaは学習のみ\n記入例\nJava経験5年",
        ):
            with self.subTest(skillsheet=skillsheet):
                checks, _, _ = self.apply("Java開発経験2年以上", skillsheet)
                self.assertEqual("human_review", checks[0]["confidence"])

        checks = [{
            "skill": "Java開発経験2年以上", "original_match": False,
            "recheck_match": True, "confidence": "confirmed", "reason": "fixture",
            "evidence": "Java経験5年",
        }]
        guarded, _, _ = guard.apply_minimal_safety_guard(
            checks, "match", True, "Javaは学習のみ\n記入例\nJava経験5年"
        )
        self.assertEqual("human_review", guarded[0]["confidence"])

    def test_guard_never_promotes_to_confirmed(self):
        for confidence in ("human_review", "not_confirmed"):
            checks, _, metadata = self.apply("Python経験", "Python経験3年", confidence)
            self.assertNotEqual("confirmed", checks[0]["confidence"])
            self.assertEqual(0, metadata["promoted_to_confirmed"])


class StateAndSubmissionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.zero_stabilization = patch.object(
            batch, "FILE_STABILIZATION_WINDOW_SECONDS", 0.0
        )
        self.zero_stabilization.start()

    def tearDown(self):
        self.zero_stabilization.stop()
        self.temp.cleanup()

    def prepare(self, run_id="run1"):
        batch.prepare_run(run_id, self.root, [context()])
        return self.root / run_id

    def test_prepare_custom_id_unique_and_manifest_hash(self):
        report = batch.prepare_run("unique", self.root, [context(1), context(2)])
        self.assertEqual(2, report["request_count"])
        self.assertTrue(report["custom_id_unique"])
        self.assertEqual(64, len(report["manifest_sha256"]))

    def test_claim_conflict_blocks_submit(self):
        run_dir = self.prepare()
        store = batch.FileStateStore(run_dir)
        state, _ = store.load()
        store.acquire_submit_claim(state["submission_nonce"])
        with self.assertRaises(batch.SubmissionBlocked):
            batch.submit_run("run1", FakeClient(), self.root)

    def test_cas_conflict_is_detected(self):
        run_dir = self.prepare()
        store = batch.FileStateStore(run_dir)
        state, stale = store.load()
        state["x"] = 1
        store.cas(stale, state)
        with self.assertRaises(batch.CASConflict):
            store.cas(stale, state)

    def test_successful_submit_retry_resumes_without_duplicate_create(self):
        self.prepare()
        client = FakeClient()
        first = batch.submit_run("run1", client, self.root)
        self.assertEqual("batch-1", first["batch_id"])
        second = batch.submit_run("run1", client, self.root)
        self.assertTrue(second["resumed"])
        self.assertEqual(1, client.upload_calls)
        self.assertEqual(1, client.create_calls)

    def test_readiness_already_processed_completes_zero_window_fixture(self):
        run_dir = self.prepare()
        client = FakeClient()
        batch.submit_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        audit = state["file_readiness"]
        self.assertEqual(1, client.file_retrieve_calls)
        self.assertEqual(1, client.create_calls)
        self.assertEqual("batch", audit["purpose"])
        self.assertEqual("processed", audit["initial_status"])
        self.assertEqual("processed", audit["final_status"])
        self.assertEqual(0, audit["poll_count"])
        self.assertEqual("ready", audit["readiness_result"])

    def test_processed_does_not_create_before_stabilization_complete(self):
        self.prepare()
        client = FakeClient()

        def interrupt_during_window(_seconds):
            self.assertEqual(0, client.create_calls)
            raise RuntimeError("fixture interrupts stabilization")

        with patch.object(
            batch, "FILE_STABILIZATION_WINDOW_SECONDS", 30.0
        ), patch.object(
            batch.time, "monotonic", side_effect=[0.0, 0.0]
        ), patch.object(
            batch.time, "sleep", side_effect=interrupt_during_window
        ):
            with self.assertRaisesRegex(RuntimeError, "interrupts stabilization"):
                batch.submit_run("run1", client, self.root)
        self.assertEqual(0, client.create_calls)
        self.assertEqual(["upload", "file:processed"], client.events)

    def test_processed_stable_window_completes_before_single_create(self):
        run_dir = self.prepare()
        client = FakeClient()
        clock = [0.0, 0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0]
        with patch.object(
            batch, "FILE_STABILIZATION_WINDOW_SECONDS", 30.0
        ), patch.object(
            batch.time, "monotonic", side_effect=clock
        ), patch.object(batch.time, "sleep", return_value=None) as sleep:
            batch.submit_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        audit = state["file_readiness"]
        self.assertEqual(7, client.file_retrieve_calls)
        self.assertEqual(6, sleep.call_count)
        self.assertEqual(6, audit["poll_count"])
        self.assertEqual(["processed"] * 7, audit["observed_statuses"])
        self.assertEqual(30.0, audit["stabilization_seconds"])
        self.assertIsNotNone(audit["first_processed_at"])
        self.assertIsNotNone(audit["stabilization_started_at"])
        self.assertIsNotNone(audit["stabilization_completed_at"])
        self.assertEqual("ready", audit["readiness_result"])
        self.assertEqual(1, client.create_calls)
        self.assertEqual("create", client.events[-1])

    def test_stabilization_status_change_fails_closed(self):
        run_dir = self.prepare()
        client = FakeClient()
        client.file_values = [
            {"id": client.upload_id, "purpose": "batch", "status": "processed"},
            {"id": client.upload_id, "purpose": "batch", "status": "uploaded"},
        ]
        with patch.object(
            batch, "FILE_STABILIZATION_WINDOW_SECONDS", 30.0
        ), patch.object(
            batch.time, "monotonic", side_effect=[0.0, 0.0, 5.0]
        ), patch.object(batch.time, "sleep", return_value=None):
            with self.assertRaises(batch.FileReadinessError):
                batch.submit_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        self.assertEqual(batch.STATE_SAFE_STOPPED, state["state"])
        self.assertEqual(0, client.create_calls)
        self.assertEqual(
            "stabilization_status_changed",
            state["file_readiness"]["readiness_result"],
        )

    def test_stabilization_lookup_failure_fails_closed(self):
        run_dir = self.prepare()
        client = FakeClient()
        client.file_values = [
            {"id": client.upload_id, "purpose": "batch", "status": "processed"},
            batch.BatchEngineError("fixture lookup failure"),
        ]
        with patch.object(
            batch, "FILE_STABILIZATION_WINDOW_SECONDS", 30.0
        ), patch.object(
            batch.time, "monotonic", side_effect=[0.0, 0.0, 5.0]
        ), patch.object(batch.time, "sleep", return_value=None):
            with self.assertRaises(batch.FileReadinessError):
                batch.submit_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        self.assertEqual(batch.STATE_SAFE_STOPPED, state["state"])
        self.assertEqual(0, client.create_calls)
        self.assertEqual(
            "file_lookup_failed", state["file_readiness"]["readiness_result"]
        )

    def test_stabilization_purpose_change_fails_closed(self):
        run_dir = self.prepare()
        client = FakeClient()
        client.file_values = [
            {"id": client.upload_id, "purpose": "batch", "status": "processed"},
            {"id": client.upload_id, "purpose": "fine-tune", "status": "processed"},
        ]
        with patch.object(
            batch, "FILE_STABILIZATION_WINDOW_SECONDS", 30.0
        ), patch.object(
            batch.time, "monotonic", side_effect=[0.0, 0.0, 5.0]
        ), patch.object(batch.time, "sleep", return_value=None):
            with self.assertRaises(batch.FileReadinessError):
                batch.submit_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        self.assertEqual(batch.STATE_SAFE_STOPPED, state["state"])
        self.assertEqual(0, client.create_calls)
        self.assertEqual(
            "purpose_not_batch", state["file_readiness"]["readiness_result"]
        )

    def test_stabilization_invalid_metadata_fails_closed(self):
        run_dir = self.prepare()
        client = FakeClient()
        client.file_values = [
            {"id": client.upload_id, "purpose": "batch", "status": "processed"},
            {"id": client.upload_id, "status": "processed"},
        ]
        with patch.object(
            batch, "FILE_STABILIZATION_WINDOW_SECONDS", 30.0
        ), patch.object(
            batch.time, "monotonic", side_effect=[0.0, 0.0, 5.0]
        ), patch.object(batch.time, "sleep", return_value=None):
            with self.assertRaises(batch.FileReadinessError):
                batch.submit_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        self.assertEqual(batch.STATE_SAFE_STOPPED, state["state"])
        self.assertEqual(0, client.create_calls)
        self.assertEqual(
            "unexpected_metadata", state["file_readiness"]["readiness_result"]
        )

    def test_stabilization_file_id_change_fails_closed(self):
        run_dir = self.prepare()
        client = FakeClient()
        client.file_values = [
            {"id": client.upload_id, "purpose": "batch", "status": "processed"},
            {"id": "file-other", "purpose": "batch", "status": "processed"},
        ]
        with patch.object(
            batch, "FILE_STABILIZATION_WINDOW_SECONDS", 30.0
        ), patch.object(
            batch.time, "monotonic", side_effect=[0.0, 0.0, 5.0]
        ), patch.object(batch.time, "sleep", return_value=None):
            with self.assertRaises(batch.FileReadinessError):
                batch.submit_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        self.assertEqual(batch.STATE_SAFE_STOPPED, state["state"])
        self.assertEqual(0, client.create_calls)
        self.assertEqual(
            "unexpected_metadata", state["file_readiness"]["readiness_result"]
        )

    def test_readiness_uploaded_then_processed_polls_before_create(self):
        run_dir = self.prepare()
        client = FakeClient()
        client.file_values = [
            {"id": client.upload_id, "purpose": "batch", "status": "uploaded"},
            {"id": client.upload_id, "purpose": "batch", "status": "processed"},
        ]
        with patch.object(batch.time, "sleep", return_value=None):
            batch.submit_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        audit = state["file_readiness"]
        self.assertEqual(2, client.file_retrieve_calls)
        self.assertEqual(1, audit["poll_count"])
        self.assertEqual("uploaded", audit["initial_status"])
        self.assertEqual("processed", audit["final_status"])
        self.assertEqual(1, client.create_calls)

    def test_readiness_error_status_stops_without_create(self):
        run_dir = self.prepare()
        client = FakeClient()
        client.file_values = [{
            "id": client.upload_id, "purpose": "batch", "status": "error"
        }]
        with self.assertRaises(batch.FileReadinessError):
            batch.submit_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        self.assertEqual(0, client.create_calls)
        self.assertEqual(batch.STATE_SAFE_STOPPED, state["state"])
        self.assertEqual("input_file_file_status_error", state["safe_stop_reason"])

    def test_readiness_timeout_stops_without_create(self):
        run_dir = self.prepare()
        client = FakeClient()
        client.file_values = [{
            "id": client.upload_id, "purpose": "batch", "status": "uploaded"
        }]
        with patch.object(batch.time, "sleep", return_value=None), patch.object(
            batch.time, "monotonic", side_effect=[0.0, 0.0, 60.0]
        ):
            with self.assertRaises(batch.FileReadinessError):
                batch.submit_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        self.assertEqual(2, client.file_retrieve_calls)
        self.assertEqual(0, client.create_calls)
        self.assertEqual("readiness_timeout", state["file_readiness"]["readiness_result"])
        self.assertEqual(1, state["file_readiness"]["poll_count"])

    def test_readiness_wrong_purpose_stops_without_create(self):
        run_dir = self.prepare()
        client = FakeClient()
        client.file_values = [{
            "id": client.upload_id, "purpose": "fine-tune", "status": "processed"
        }]
        with self.assertRaises(batch.FileReadinessError):
            batch.submit_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        self.assertEqual(0, client.create_calls)
        self.assertEqual("purpose_not_batch", state["file_readiness"]["readiness_result"])

    def test_readiness_lookup_failure_stops_without_create(self):
        run_dir = self.prepare()
        client = FakeClient()
        client.file_lookup_error = batch.BatchEngineError("fixture lookup failure")
        with self.assertRaises(batch.FileReadinessError):
            batch.submit_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        self.assertEqual(0, client.create_calls)
        self.assertEqual("file_lookup_failed", state["file_readiness"]["readiness_result"])

    def test_batch_create_event_occurs_only_after_processed(self):
        self.prepare()
        client = FakeClient()
        client.file_values = [
            {"id": client.upload_id, "purpose": "batch", "status": "uploaded"},
            {"id": client.upload_id, "purpose": "batch", "status": "processed"},
        ]
        with patch.object(batch.time, "sleep", return_value=None):
            batch.submit_run("run1", client, self.root)
        self.assertEqual(["upload", "file:uploaded", "file:processed", "create"], client.events)

    def test_readiness_unexpected_metadata_stops_without_create(self):
        run_dir = self.prepare()
        client = FakeClient()
        client.file_values = [{"id": client.upload_id, "status": "processed"}]
        with self.assertRaises(batch.FileReadinessError):
            batch.submit_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        self.assertEqual(0, client.create_calls)
        self.assertEqual("unexpected_metadata", state["file_readiness"]["readiness_result"])

    def test_create_timeout_stays_pending_and_never_resubmits(self):
        run_dir = self.prepare()
        client = FakeClient()
        client.create_error = TimeoutError("unknown response")
        with self.assertRaises(batch.PendingReconciliation):
            batch.submit_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        self.assertEqual(batch.STATE_PENDING_RECONCILIATION, state["state"])
        with self.assertRaises(batch.PendingReconciliation):
            batch.submit_run("run1", client, self.root)
        self.assertEqual(1, client.create_calls)

    def test_terminal_batch_errors_are_sanitized_into_state(self):
        run_dir = self.prepare()
        set_state(
            run_dir,
            batch_id="batch-1",
            batch_status="validating",
            state=batch.STATE_SUBMITTED,
        )
        client = FakeClient()
        client.retrieve_value = {
            "id": "batch-1",
            "status": "failed",
            "errors": {
                "data": [
                    {
                        "code": "invalid_request",
                        "message": "Cannot use Bearer sk-proj-secret123456789",
                        "param": "file_id",
                        "line": 7,
                        "api_key": "must-not-persist",
                    }
                ]
            },
        }
        batch.resume_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        self.assertEqual(
            {
                "code": "invalid_request",
                "message": "Cannot use Bearer [REDACTED]",
                "param": "file_id",
                "line": 7,
            },
            state["batch_errors"][0],
        )
        self.assertNotIn("api_key", state["batch_errors"][0])

    def _pending(self):
        run_dir = self.prepare()
        client = FakeClient()
        client.create_error = TimeoutError()
        with self.assertRaises(batch.PendingReconciliation):
            batch.submit_run("run1", client, self.root)
        state, _ = batch.FileStateStore(run_dir).load()
        metadata = batch._batch_metadata(state)
        return run_dir, client, state, metadata

    def test_pending_reconciliation_one_match_adopts(self):
        _, client, state, metadata = self._pending()
        client.list_values = [{
            "id": "batch-recovered", "status": "validating",
            "input_file_id": state["input_file_id"], "metadata": metadata,
        }]
        report = batch.reconcile_pending("run1", client, self.root)
        self.assertEqual(1, report["match_count"])
        self.assertEqual("batch-recovered", report["batch_id"])

    def test_pending_reconciliation_zero_and_multiple_safe_stop(self):
        for multiple in (False, True):
            with self.subTest(multiple=multiple):
                self.tearDown()
                self.setUp()
                run_dir, client, state, metadata = self._pending()
                if multiple:
                    client.list_values = [
                        {"id": f"batch-{i}", "status": "validating", "input_file_id": state["input_file_id"], "metadata": metadata}
                        for i in (1, 2)
                    ]
                with self.assertRaises(batch.ReconciliationFailed):
                    batch.reconcile_pending("run1", client, self.root, max_checks=2)
                stopped, _ = batch.FileStateStore(run_dir).load()
                self.assertEqual(batch.STATE_SAFE_STOPPED, stopped["state"])

    def test_stale_zero_match_cannot_overwrite_concurrent_adoption(self):
        run_dir, stale_client, state, metadata = self._pending()

        def worker_a_adopts():
            worker_a = FakeClient()
            worker_a.list_values = [{
                "id": "batch-adopted",
                "status": "validating",
                "input_file_id": state["input_file_id"],
                "metadata": metadata,
            }]
            batch.reconcile_pending("run1", worker_a, self.root, max_checks=1)
            stale_client.retrieve_value = {
                "id": "batch-adopted",
                "status": "validating",
                "input_file_id": state["input_file_id"],
            }

        stale_client.list_callback = worker_a_adopts
        report = batch.reconcile_pending(
            "run1", stale_client, self.root, max_checks=1
        )
        final_state, _ = batch.FileStateStore(run_dir).load()
        self.assertEqual("batch-adopted", report["batch_id"])
        self.assertEqual("batch-adopted", final_state["batch_id"])
        self.assertNotEqual(batch.STATE_SAFE_STOPPED, final_state["state"])


class CollectorIntegrityTest(unittest.TestCase):
    def entries(self, count=2):
        return [manifest_entry(index) for index in range(1, count + 1)]

    def test_shuffled_output_restores_ordinal(self):
        entries = self.entries()
        output = jsonl_bytes([success_row(entries[1]), success_row(entries[0])])
        result = batch.collect_records(entries, output, b"")
        self.assertEqual([1, 2], [pair["ordinal"] for pair in result["audit_pairs"]])
        self.assertEqual("project-1", result["records"][0]["project_info"]["message_id"])

    def test_duplicate_missing_unknown_and_malformed_are_blocking(self):
        entry = self.entries(1)[0]
        cases = (
            jsonl_bytes([success_row(entry), success_row(entry)]),
            b"",
            jsonl_bytes([{**success_row(entry), "custom_id": "unknown"}]),
            b"{not-json}\n",
        )
        for payload in cases:
            with self.subTest(payload=payload[:20]):
                with self.assertRaises(batch.CollectorIntegrityError):
                    batch.collect_records([entry], payload, b"")

    def test_output_error_union_and_per_request_fallback(self):
        entries = self.entries()
        error = {"custom_id": entries[1]["custom_id"], "error": {"code": "bad"}}
        result = batch.collect_records(
            entries, jsonl_bytes([success_row(entries[0])]), jsonl_bytes([error])
        )
        self.assertEqual(2, len(result["records"]))
        self.assertEqual(1, result["per_request_fallback_count"])
        self.assertEqual("human_review", result["audit_pairs"][1]["after"]["status"])

    def test_schema_invalid_falls_back_and_cannot_promote(self):
        entry = self.entries(1)[0]
        invalid = success_row(entry)
        parsed = json.loads(invalid["response"]["body"]["choices"][0]["message"]["content"])
        parsed["required_skill_checks"][0]["skill"] = "wrong"
        parsed["category_match"] = "mismatch"
        invalid["response"]["body"]["choices"][0]["message"]["content"] = json.dumps(parsed)
        result = batch.collect_records([entry], jsonl_bytes([invalid]), b"")
        pair = result["audit_pairs"][0]
        self.assertFalse(pair["schema_valid"])
        self.assertEqual("human_review", pair["after"]["status"])
        self.assertEqual("unclear", pair["after"]["category_match"])

    def test_finish_reason_length_falls_back_even_with_valid_confirmed_json(self):
        entry = self.entries(1)[0]
        result = batch.collect_records(
            [entry], jsonl_bytes([success_row(entry, finish_reason="length")]), b""
        )
        pair = result["audit_pairs"][0]
        self.assertFalse(pair["schema_valid"])
        self.assertEqual("human_review", pair["after"]["status"])
        self.assertFalse(
            pair["schema_valid"]
            and pair["after"]["status"] == "confirmed"
            and pair["after"]["category_match"] == "match"
        )
        self.assertEqual("batch_output_truncated", result["errors"][0]["error_type"])

    def test_finish_reason_stop_remains_confirmed(self):
        entry = self.entries(1)[0]
        result = batch.collect_records(
            [entry], jsonl_bytes([success_row(entry, finish_reason="stop")]), b""
        )
        self.assertTrue(result["audit_pairs"][0]["schema_valid"])
        self.assertEqual("confirmed", result["audit_pairs"][0]["after"]["status"])

    def test_same_custom_id_across_output_and_error_is_duplicate(self):
        entry = self.entries(1)[0]
        with self.assertRaises(batch.CollectorIntegrityError):
            batch.collect_records(
                [entry], jsonl_bytes([success_row(entry)]),
                jsonl_bytes([{"custom_id": entry["custom_id"], "error": {}}]),
            )


class CollectorRetryAndPublishTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def _completed_run(self, count=1):
        run_dir = self.root / "runtime"
        batch.prepare_run(
            "run1", run_dir, [context(index) for index in range(1, count + 1)]
        )
        actual = run_dir / "run1"
        manifest = list(batch.read_jsonl(str(actual / "manifest.jsonl")))
        set_state(actual, batch_id="batch-1", state=batch.STATE_SUBMITTED)
        client = FakeClient()
        client.retrieve_value = {
            "id": "batch-1", "status": "completed",
            "request_counts": {"total": count, "completed": count, "failed": 0},
            "output_file_id": "output-1", "error_file_id": None,
        }
        client.downloads["output-1"] = jsonl_bytes(
            [success_row(entry) for entry in manifest]
        )
        return run_dir, actual, client

    def test_collector_retry_is_idempotent_before_publish(self):
        runtime, _, client = self._completed_run()
        first = batch.collect_run("run1", client, runtime, publish=False)
        second = batch.collect_run("run1", client, runtime, publish=False)
        self.assertEqual(first["record_count"], second["record_count"])
        self.assertEqual(batch.STATE_COLLECTED, second["state"])

    def test_partial_batch_never_reaches_stage_or_publish(self):
        runtime, actual, client = self._completed_run()
        client.retrieve_value["status"] = "in_progress"
        before = sorted(path.name for path in actual.iterdir())
        with self.assertRaises(batch.CollectorIntegrityError):
            batch.collect_run("run1", client, runtime, publish=True)
        after = sorted(path.name for path in actual.iterdir())
        self.assertEqual(before, after)
        self.assertFalse((actual / "stage").exists())

    def test_request_counts_two_completed_but_output_one_error_one_fails(self):
        runtime, actual, client = self._completed_run(count=2)
        manifest = list(batch.read_jsonl(str(actual / "manifest.jsonl")))
        client.retrieve_value.update({
            "request_counts": {"total": 2, "completed": 2, "failed": 0},
            "output_file_id": "output-1",
            "error_file_id": "error-1",
        })
        client.downloads["output-1"] = jsonl_bytes([success_row(manifest[0])])
        client.downloads["error-1"] = jsonl_bytes([
            {"custom_id": manifest[1]["custom_id"], "error": {"code": "bad"}}
        ])
        with self.assertRaises(batch.CollectorIntegrityError):
            batch.collect_run("run1", client, runtime, publish=True)
        self.assertFalse((actual / "stage").exists())

    def test_request_counts_one_completed_one_failed_matches_files(self):
        runtime, actual, client = self._completed_run(count=2)
        manifest = list(batch.read_jsonl(str(actual / "manifest.jsonl")))
        client.retrieve_value.update({
            "request_counts": {"total": 2, "completed": 1, "failed": 1},
            "output_file_id": "output-1",
            "error_file_id": "error-1",
        })
        client.downloads["output-1"] = jsonl_bytes([success_row(manifest[0])])
        client.downloads["error-1"] = jsonl_bytes([
            {"custom_id": manifest[1]["custom_id"], "error": {"code": "bad"}}
        ])
        report = batch.collect_run("run1", client, runtime, publish=False)
        self.assertEqual(2, report["record_count"])
        self.assertEqual(1, report["production_error_count"])

    def test_completed_positive_requires_output_file_id(self):
        runtime, actual, client = self._completed_run()
        client.retrieve_value["output_file_id"] = None
        with self.assertRaises(batch.CollectorIntegrityError):
            batch.collect_run("run1", client, runtime, publish=True)
        self.assertFalse((actual / "stage").exists())

    def test_failed_positive_requires_error_file_id(self):
        runtime, actual, client = self._completed_run(count=2)
        client.retrieve_value.update({
            "request_counts": {"total": 2, "completed": 1, "failed": 1},
            "error_file_id": None,
        })
        with self.assertRaises(batch.CollectorIntegrityError):
            batch.collect_run("run1", client, runtime, publish=True)
        self.assertFalse((actual / "stage").exists())

    def _stage_and_targets(self):
        entry = manifest_entry()
        collected = batch.collect_records([entry], jsonl_bytes([success_row(entry)]), b"")
        stage = batch.write_stage(
            self.root / "stage", collected["records"], collected["errors"]
        )
        targets = {
            name: self.root / "production" / path.name
            for name, path in batch.ARTIFACT_PATHS.items()
        }
        marker = self.root / "production" / "production_commit.json"
        return stage, targets, marker

    def test_publish_failure_before_marker_rolls_back_all_artifacts(self):
        stage, targets, marker = self._stage_and_targets()
        targets["all"].parent.mkdir(parents=True)
        for path in targets.values():
            path.write_bytes(b"old\n")
        with self.assertRaises(batch.PublishError):
            batch.transactional_publish(
                stage, "run1", "m" * 64, "e" * 64,
                targets, marker, fail_after_artifacts=3,
            )
        self.assertFalse(marker.exists())
        self.assertTrue(all(path.read_bytes() == b"old\n" for path in targets.values()))

    def test_commit_marker_failure_rolls_back(self):
        stage, targets, marker = self._stage_and_targets()
        with patch.object(batch, "_atomic_write_json", side_effect=OSError("marker fail")):
            with self.assertRaises(batch.PublishError):
                batch.transactional_publish(
                    stage, "run1", "m" * 64, "e" * 64, targets, marker
                )
        self.assertFalse(marker.exists())
        self.assertTrue(all(not path.exists() for path in targets.values()))

    def test_commit_marker_contains_and_validates_all_hashes(self):
        stage, targets, marker = self._stage_and_targets()
        result = batch.transactional_publish(
            stage, "run1", "m" * 64, "e" * 64, targets, marker
        )
        self.assertEqual(set(targets), set(result["artifacts"]))
        self.assertEqual(
            result,
            batch.validate_commit_marker("run1", "m" * 64, marker, targets),
        )
        targets["all"].write_bytes(b"tampered\n")
        with self.assertRaises(batch.PublishError):
            batch.validate_commit_marker("run1", "m" * 64, marker, targets)

    def test_commit_marker_rejects_stale_run_id(self):
        stage, targets, marker = self._stage_and_targets()
        batch.transactional_publish(
            stage, "old-run", "m" * 64, "e" * 64, targets, marker
        )
        with self.assertRaises(batch.PublishError):
            batch.validate_commit_marker("current-run", "m" * 64, marker, targets)

    def test_commit_marker_rejects_manifest_hash_mismatch(self):
        stage, targets, marker = self._stage_and_targets()
        batch.transactional_publish(
            stage, "run1", "m" * 64, "e" * 64, targets, marker
        )
        with self.assertRaises(batch.PublishError):
            batch.validate_commit_marker("run1", "x" * 64, marker, targets)


class Saved678ProductionRegressionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report = batch.offline_replay_saved_canary(SAVED_RUN)
        cls.by_id = {
            pair["custom_id"]: pair for pair in cls.report["audit_pairs"]
        }

    def test_saved_678_and_known_distribution(self):
        self.assertEqual(678, self.report["sample_count"])
        self.assertEqual(
            {"confirmed": 76, "human_review": 368, "not_confirmed": 234},
            {key: self.report["after"][key] for key in ("confirmed", "human_review", "not_confirmed")},
        )
        self.assertEqual(0, self.report["new_batch_submit"])
        self.assertEqual(0, self.report["new_llm_call"])
        self.assertEqual(0, self.report["production_write"])

    def test_clear_keep_five_and_false_positive_two(self):
        keep = (
            "c-canary678-20260822-01-0352-b5d37b77ca8c",
            "c-canary678-20260822-01-0253-7d7f85cca0da",
            "c-canary678-20260822-01-0303-839007bc06a6",
            "c-canary678-20260822-01-0341-1bb1d4d605b3",
            "c-canary678-20260822-01-0342-18f4b14de8ad",
        )
        false_positive = (
            "c-canary678-20260822-01-0085-961ae7fbe465",
            "c-canary678-20260822-01-0131-7c68e13f19c1",
        )
        retained = sum(
            self.by_id[item]["after"]["status"] != "not_confirmed"
            and self.by_id[item]["after"]["category_match"] != "mismatch"
            for item in keep
        )
        self.assertEqual(5, retained)
        self.assertTrue(
            all(self.by_id[item]["after"]["status"] == "human_review" for item in false_positive)
        )

    def test_proposal_ready_set_is_blocking_identical(self):
        report = load_batch_confirm().run_confirm(write=False)
        self.assertTrue(report["quality_pass"])
        self.assertEqual(26, report["proposal_ready_before"])
        self.assertEqual(26, report["proposal_ready_after"])
        self.assertEqual(0, report["proposal_ready_added"])
        self.assertEqual(0, report["proposal_ready_removed"])


if __name__ == "__main__":
    unittest.main()

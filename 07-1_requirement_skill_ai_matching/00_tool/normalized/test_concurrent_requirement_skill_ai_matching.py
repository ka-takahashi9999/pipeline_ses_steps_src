"""07-1 production concurrent pathのoffline focused tests。"""

import copy
import importlib.util
import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch


TOOL_DIR = Path(__file__).resolve().parent
STEP_DIR = TOOL_DIR.parents[1]
PROJECT_ROOT = STEP_DIR.parent
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

import requirement_skill_ai_matching as target
import retention_guard


SAVED_500 = (
    STEP_DIR / "_test_07_1_speedup/test_20260823_speedup_500_v1"
)
SAVED_RETENTION_500 = (
    STEP_DIR
    / "_test_07_1_candidate_retention_guard/replay_20260824_500_v2"
)


class Logger:
    def info(self, *_args, **_kwargs):
        pass

    def warn(self, *_args, **_kwargs):
        pass

    def ok(self, *_args, **_kwargs):
        pass

    def error(self, *_args, **_kwargs):
        pass


def fixture_inputs(project_count=2, resources_per_project=3):
    pairs = []
    projects = {}
    skillsheets = {}
    for project_index in range(project_count):
        project_mid = "project-{}".format(project_index)
        projects[project_mid] = {
            "message_id": project_mid,
            "required_skills": [{"skill": "Python開発経験"}],
            "optional_skills": [],
        }
        for resource_index in range(resources_per_project):
            resource_mid = "resource-{}-{}".format(project_index, resource_index)
            pairs.append(
                {
                    "project_info": {"message_id": project_mid},
                    "resource_info": {"message_id": resource_mid},
                }
            )
            skillsheets[resource_mid] = {
                "message_id": resource_mid,
                "success": True,
                "source": "offline",
                "skillsheet": "業務でPython開発を担当 {}".format(resource_mid),
            }
    return pairs, projects, skillsheets


def success_response(kwargs):
    response = copy.deepcopy(kwargs["response_schema"])
    for field in ("required_skills", "optional_skills"):
        for skill in response[field]:
            skill["match"] = True
            skill["note"] = "実務経験あり"
    return response


def success_checkpoint(item, concurrency=1):
    return {
        **target.concurrent_manifest_record(item),
        "completion_state": "completed",
        "status": "success",
        "result": {
            "project_info": {"message_id": item["project_message_id"]},
            "resource_info": {"message_id": item["resource_message_id"]},
            "required_skills": [
                {
                    "skill": "Python開発経験",
                    "match": True,
                    "note": "実務経験あり",
                }
            ],
            "optional_skills": [],
            "evaluation_meta": {
                "skillsheet_source": "offline",
                "llm_model": "gpt-4o-mini",
                "soft_skill_auto_true_count": 0,
            },
        },
        "error": None,
        "concurrency_at_submit": concurrency,
        "note_truncated_count": 0,
        "telemetry": {
            "latency_seconds": 0.01,
            "attempts": [],
            "retry_count": 0,
            "rate_limit_429_count": 0,
            "api_failure": False,
            "rate_limit_headers": {
                "x-ratelimit-remaining-requests": "1000",
                "x-ratelimit-remaining-tokens": "1000000",
            },
            "request_body_mismatch": False,
        },
    }


class FlagAndRequestContractTest(unittest.TestCase):
    def test_feature_flag_default_is_off_and_limits_are_fixed(self):
        self.assertFalse(target.ENABLE_07_1_CONCURRENT)
        self.assertEqual(target.CONCURRENT_INITIAL, 2)
        self.assertEqual(target.CONCURRENT_MAX, 4)
        self.assertEqual(target.MAX_CONCURRENCY_HARD_LIMIT, 4)
        with self.assertRaises(ValueError):
            target.AdaptiveConcurrency(2, 5)

    def test_flag_zero_main_uses_serial_path_only(self):
        pairs, projects, skillsheets = fixture_inputs(1, 2)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_pairs = root / "pairs.jsonl"
            input_projects = root / "projects.jsonl"
            input_skillsheets = root / "skillsheets.jsonl"
            output_result = root / "result.jsonl"
            output_error = root / "error.jsonl"
            output_metadata = root / "metadata.json"
            target.write_jsonl(str(input_pairs), pairs)
            target.write_jsonl(str(input_projects), list(projects.values()))
            target.write_jsonl(str(input_skillsheets), list(skillsheets.values()))

            def fake_llm(**kwargs):
                return success_response(kwargs)

            with patch.object(target, "ENABLE_07_1_CONCURRENT", False), patch.object(
                target, "INPUT_PAIRS", input_pairs
            ), patch.object(target, "INPUT_PROJECT_SKILLS", input_projects), patch.object(
                target, "INPUT_SKILLSHEETS", input_skillsheets
            ), patch.object(target, "OUTPUT_RESULT", output_result), patch.object(
                target, "OUTPUT_ERROR", output_error
            ), patch.object(target, "OUTPUT_RUN_METADATA", output_metadata), patch.object(
                target, "call_llm", side_effect=fake_llm
            ), patch.object(target, "_run_concurrent") as concurrent, patch.object(
                target, "write_execution_time"
            ), patch.object(sys, "argv", ["requirement_skill_ai_matching.py"]):
                target.main()

            concurrent.assert_not_called()
            self.assertEqual(len(list(target.read_jsonl(str(output_result)))), 2)
            self.assertEqual(len(list(target.read_jsonl(str(output_error)))), 0)
            metadata = json.loads(output_metadata.read_text(encoding="utf-8"))
            self.assertNotIn("concurrent_execution", metadata)

    def test_flag_one_routes_only_to_concurrent_path(self):
        pairs, projects, skillsheets = fixture_inputs(1, 1)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            paths = {
                "pairs": root / "pairs.jsonl",
                "projects": root / "projects.jsonl",
                "skillsheets": root / "skillsheets.jsonl",
            }
            target.write_jsonl(str(paths["pairs"]), pairs)
            target.write_jsonl(str(paths["projects"]), list(projects.values()))
            target.write_jsonl(str(paths["skillsheets"]), list(skillsheets.values()))
            with patch.object(target, "ENABLE_07_1_CONCURRENT", True), patch.object(
                target, "INPUT_PAIRS", paths["pairs"]
            ), patch.object(target, "INPUT_PROJECT_SKILLS", paths["projects"]), patch.object(
                target, "INPUT_SKILLSHEETS", paths["skillsheets"]
            ), patch.object(target, "_run_concurrent") as concurrent, patch.object(
                sys, "argv", ["requirement_skill_ai_matching.py"]
            ):
                target.main()
            concurrent.assert_called_once()

    def test_saved_500_request_body_hashes_match_production(self):
        pairs = list(target.read_jsonl(str(target.INPUT_PAIRS)))
        projects = {
            str(row["message_id"]): row
            for row in target.read_jsonl(str(target.INPUT_PROJECT_SKILLS))
        }
        skillsheets = {
            str(row["message_id"]): row
            for row in target.read_jsonl(str(target.INPUT_SKILLSHEETS))
        }
        saved = list(target.read_jsonl(str(SAVED_500 / "manifest.jsonl")))
        matched = 0
        for row in saved:
            call_kwargs, error = target.capture_request_contract(
                pairs[row["ordinal"]], projects, skillsheets, Logger()
            )
            self.assertIsNone(error)
            self.assertEqual(
                target._request_body_hash(call_kwargs), row["request_body_sha256"]
            )
            matched += 1
        self.assertEqual(matched, 500)

    def test_saved_500_production_collector_integrity(self):
        saved_manifest = list(
            target.read_jsonl(str(SAVED_500 / "manifest.jsonl"))
        )
        saved_checkpoints = list(
            target.read_jsonl(str(SAVED_500 / "checkpoint.jsonl"))
        )
        checkpoint_by_identity = {
            row["request_identity"]: row for row in saved_checkpoints
        }
        self.assertEqual(len(saved_manifest), 500)
        self.assertEqual(len(checkpoint_by_identity), 500)

        manifest = []
        checkpoints = []
        for saved in saved_manifest:
            self.assertEqual(
                saved["request_identity"],
                target._request_identity(
                    saved["ordinal"],
                    saved["project_message_id"],
                    saved["resource_message_id"],
                ),
            )
            item = {
                **saved,
                "api_request": True,
                "skill_contract_sha256": target._skill_contract_hash(
                    checkpoint_by_identity[saved["request_identity"]]["result"][
                        "required_skills"
                    ],
                    checkpoint_by_identity[saved["request_identity"]]["result"][
                        "optional_skills"
                    ],
                ),
            }
            manifest.append(item)
            checkpoint = checkpoint_by_identity[saved["request_identity"]]
            checkpoints.append(
                {
                    **item,
                    "completion_state": "completed",
                    "status": checkpoint["status"],
                    "result": checkpoint["result"],
                    "error": checkpoint["error"],
                    "concurrency_at_submit": checkpoint[
                        "concurrency_at_submit"
                    ],
                    "note_truncated_count": 0,
                    "telemetry": checkpoint["telemetry"],
                }
            )

        collected = target.collect_concurrent_checkpoints(
            manifest, list(reversed(checkpoints))
        )
        self.assertEqual(len(collected["ordered"]), 500)
        self.assertEqual(collected["duplicate"], [])
        self.assertEqual(collected["unknown"], [])
        self.assertEqual(collected["malformed"], [])
        self.assertEqual(collected["missing"], [])
        self.assertEqual(
            [row["ordinal"] for row in collected["ordered"]],
            sorted(row["ordinal"] for row in manifest),
        )


class SchedulerAndCheckpointTest(unittest.TestCase):
    def setUp(self):
        self.pairs, self.projects, self.skillsheets = fixture_inputs(3, 3)
        self.items = target.build_concurrent_items(
            self.pairs, self.projects, self.skillsheets, Logger()
        )
        self.temporary = tempfile.TemporaryDirectory(prefix="prod_07_1_concurrent_")
        self.checkpoint = Path(self.temporary.name) / "checkpoint.jsonl"

    def tearDown(self):
        self.temporary.cleanup()

    def test_warm_one_then_fan_out_ordering_and_max_four(self):
        lock = threading.Lock()
        events = []
        active = 0
        peak = 0

        def worker(item, *_args):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
                events.append(("start", item["request_identity"], time.monotonic()))
            time.sleep(0.01)
            with lock:
                events.append(("end", item["request_identity"], time.monotonic()))
                active -= 1
            return success_checkpoint(item)

        with patch.object(target, "_concurrent_worker", side_effect=worker):
            checkpoints, controller, stopped = target.run_concurrent_scheduler(
                self.items,
                self.projects,
                self.skillsheets,
                Logger(),
                self.checkpoint,
            )
        self.assertFalse(stopped)
        self.assertEqual(len(checkpoints), len(self.items))
        self.assertLessEqual(peak, 4)
        self.assertLessEqual(controller.current, 4)
        event_map = {(kind, identity): stamp for kind, identity, stamp in events}
        by_project = {}
        for item in self.items:
            by_project.setdefault(item["project_message_id"], []).append(item)
        for rows in by_project.values():
            rows.sort(key=lambda row: row["ordinal"])
            leader_end = event_map[("end", rows[0]["request_identity"])]
            for follower in rows[1:]:
                self.assertLessEqual(
                    leader_end,
                    event_map[("start", follower["request_identity"])],
                )

    def test_no_headers_never_increase_and_429_decreases(self):
        controller = target.AdaptiveConcurrency(2, 4)
        for _ in range(10):
            checkpoint = success_checkpoint(self.items[0])
            checkpoint["telemetry"]["rate_limit_headers"] = {}
            controller.observe(checkpoint)
        self.assertEqual(controller.current, 2)
        checkpoint = success_checkpoint(self.items[0])
        checkpoint["telemetry"]["rate_limit_429_count"] = 1
        controller.observe(checkpoint)
        self.assertEqual(controller.current, 1)

    def test_high_latency_decreases_concurrency(self):
        controller = target.AdaptiveConcurrency(2, 4)
        checkpoint = success_checkpoint(self.items[0])
        checkpoint["telemetry"]["latency_seconds"] = 45.01
        controller.observe(checkpoint)
        self.assertEqual(controller.current, 1)

    def test_resume_does_not_resend_completed_request(self):
        existing = success_checkpoint(self.items[0])
        called = []

        def worker(item, *_args):
            called.append(item["request_identity"])
            return success_checkpoint(item)

        with patch.object(target, "_concurrent_worker", side_effect=worker):
            checkpoints, _, stopped = target.run_concurrent_scheduler(
                self.items,
                self.projects,
                self.skillsheets,
                Logger(),
                self.checkpoint,
                [existing],
            )
        self.assertFalse(stopped)
        self.assertNotIn(self.items[0]["request_identity"], called)
        self.assertEqual(len(checkpoints), len(self.items))

    def test_collector_detects_duplicate_and_missing_and_restores_order(self):
        manifest = [target.concurrent_manifest_record(item) for item in self.items]
        shuffled = [success_checkpoint(item) for item in reversed(self.items)]
        collected = target.collect_concurrent_checkpoints(manifest, shuffled)
        self.assertEqual(
            [row["ordinal"] for row in collected["ordered"]],
            sorted(row["ordinal"] for row in collected["ordered"]),
        )
        with self.assertRaisesRegex(ValueError, "duplicate=1"):
            target.collect_concurrent_checkpoints(
                manifest, shuffled + [shuffled[0]]
            )
        with self.assertRaisesRegex(ValueError, "missing=1"):
            target.collect_concurrent_checkpoints(manifest, shuffled[:-1])

    def test_collector_rejects_invalid_result_and_error_payloads(self):
        manifest = [target.concurrent_manifest_record(self.items[0])]
        invalid_success = success_checkpoint(self.items[0])
        invalid_success["result"] = None
        with self.assertRaisesRegex(ValueError, "malformed=1"):
            target.collect_concurrent_checkpoints(manifest, [invalid_success])

        invalid_error = success_checkpoint(self.items[0])
        invalid_error["status"] = "error"
        invalid_error["result"] = None
        invalid_error["error"] = {"unexpected": True}
        with self.assertRaisesRegex(ValueError, "malformed=1"):
            target.collect_concurrent_checkpoints(manifest, [invalid_error])

        changed_skill = success_checkpoint(self.items[0])
        changed_skill["result"]["required_skills"][0]["skill"] = "CHANGED_SKILL"
        with self.assertRaisesRegex(ValueError, "malformed=1"):
            target.collect_concurrent_checkpoints(manifest, [changed_skill])

        long_note = success_checkpoint(self.items[0])
        long_note["result"]["required_skills"][0]["note"] = "x" * 31
        with self.assertRaisesRegex(ValueError, "malformed=1"):
            target.collect_concurrent_checkpoints(manifest, [long_note])

        invalid_meta = success_checkpoint(self.items[0])
        invalid_meta["result"]["evaluation_meta"] = {}
        with self.assertRaisesRegex(ValueError, "malformed=1"):
            target.collect_concurrent_checkpoints(manifest, [invalid_meta])

    def test_worker_enables_bounded_retry_backoff(self):
        captured = {}

        def fake_call(**kwargs):
            captured.update(kwargs)
            kwargs["response_observer"](
                {
                    "attempt": 1,
                    "status_code": 200,
                    "success": True,
                    "error_type": "",
                    "rate_limit_headers": {},
                }
            )
            return success_response(kwargs)

        with patch.object(target, "call_llm", side_effect=fake_call):
            checkpoint = target._concurrent_worker(
                self.items[0],
                self.projects,
                self.skillsheets,
                Logger(),
                1,
            )
        self.assertEqual(checkpoint["status"], "success")
        self.assertTrue(captured["use_bounded_retry_backoff"])

    def test_http_200_invalid_schema_is_pair_error_not_api_failure(self):
        def fake_call(**kwargs):
            kwargs["response_observer"]({
                "attempt": 1, "status_code": 200, "success": True,
                "error_type": "", "rate_limit_headers": {},
            })
            response = success_response(kwargs)
            response["required_skills"][0]["skill"] = "変更されたskill"
            return response

        with patch.object(target, "call_llm", side_effect=fake_call):
            checkpoint = target._concurrent_worker(
                self.items[0], self.projects, self.skillsheets, Logger(), 1
            )
        self.assertEqual(checkpoint["status"], "error")
        self.assertIsNone(checkpoint["result"])
        self.assertEqual(checkpoint["error"]["error_type"], "invalid_output_schema")
        self.assertFalse(checkpoint["telemetry"]["api_failure"])
        self.assertEqual(checkpoint["telemetry"]["attempts"][0]["status_code"], 200)
        controller = target.AdaptiveConcurrency(2, 4)
        controller.observe(checkpoint)
        self.assertEqual(controller.current, 2)

    def test_worker_error_does_not_stop_or_block_same_project_followers(self):
        called = []

        def worker(item, *_args):
            called.append(item["request_identity"])
            if item["ordinal"] == 0:
                raise RuntimeError("individual worker failure")
            return success_checkpoint(item)

        with patch.object(target, "_concurrent_worker", side_effect=worker):
            checkpoints, _, stopped = target.run_concurrent_scheduler(
                self.items, self.projects, self.skillsheets, Logger(), self.checkpoint
            )
        self.assertFalse(stopped)
        self.assertEqual(len(called), len(self.items))
        self.assertEqual(len(checkpoints), len(self.items))
        first = next(row for row in checkpoints if row["ordinal"] == 0)
        self.assertEqual(first["status"], "error")
        self.assertEqual(first["error"]["error_type"], "llm_call_error")
        self.assertTrue(first["telemetry"]["api_failure"])
        self.assertIn(self.items[1]["request_identity"], called)

    def test_legacy_validation_error_checkpoint_does_not_block_resume(self):
        leader = self.items[0]
        follower = self.items[1]
        old_error = success_checkpoint(leader)
        old_error["status"] = "error"
        old_error["result"] = None
        old_error["error"] = target._make_error(
            leader["project_message_id"], leader["resource_message_id"],
            "invalid_output_schema", "skill changed",
        )
        old_error["telemetry"]["api_failure"] = True
        called = []

        def worker(item, *_args):
            called.append(item["request_identity"])
            return success_checkpoint(item)

        with patch.object(target, "_concurrent_worker", side_effect=worker):
            checkpoints, _, stopped = target.run_concurrent_scheduler(
                [leader, follower], self.projects, self.skillsheets, Logger(),
                self.checkpoint, [old_error],
            )
        self.assertFalse(stopped)
        self.assertEqual(called, [follower["request_identity"]])
        self.assertEqual(len(checkpoints), 2)

    def test_retry_limit_still_stops_scheduler(self):
        def worker(item, *_args):
            checkpoint = success_checkpoint(item)
            checkpoint["telemetry"]["retry_count"] = 3
            return checkpoint

        with patch.object(target, "_concurrent_worker", side_effect=worker):
            checkpoints, _, stopped = target.run_concurrent_scheduler(
                self.items, self.projects, self.skillsheets, Logger(), self.checkpoint
            )
        self.assertTrue(stopped)
        self.assertLess(len(checkpoints), len(self.items))

    def test_api_failure_checkpoint_still_blocks_resume(self):
        item = self.items[0]
        failed = success_checkpoint(item)
        failed["status"] = "error"
        failed["result"] = None
        failed["error"] = target._make_error(
            item["project_message_id"], item["resource_message_id"],
            "llm_call_error", "API request failed",
        )
        failed["telemetry"]["api_failure"] = True
        with self.assertRaisesRegex(ValueError, "API error checkpoint"):
            target.run_concurrent_scheduler(
                [item], self.projects, self.skillsheets, Logger(),
                self.checkpoint, [failed],
            )

    def test_scheduler_never_writes_canonical_output(self):
        result_path = Path(self.temporary.name) / "canonical.jsonl"
        result_path.write_text("sentinel\n", encoding="utf-8")

        def worker(item, *_args):
            return success_checkpoint(item)

        with patch.object(target, "OUTPUT_RESULT", result_path), patch.object(
            target, "_concurrent_worker", side_effect=worker
        ):
            target.run_concurrent_scheduler(
                self.items,
                self.projects,
                self.skillsheets,
                Logger(),
                self.checkpoint,
            )
        self.assertEqual(result_path.read_text(encoding="utf-8"), "sentinel\n")


class ConcurrentOutputTest(unittest.TestCase):
    def run_isolated_main(self, invalid_leader):
        pairs, projects, skillsheets = fixture_inputs(2, 3)
        with tempfile.TemporaryDirectory(prefix="07_1_pair_error_") as temporary:
            root = Path(temporary)
            result_dir = root / "01_result"
            pair_path = root / "pairs.jsonl"
            project_path = root / "projects.jsonl"
            skillsheet_path = root / "skillsheets.jsonl"
            result_path = result_dir / "requirement_skill_ai_matching.jsonl"
            error_path = result_dir / "99_error_requirement_skill_ai_matching.jsonl"
            metadata_path = result_dir / "run_metadata.json"
            checkpoint_path = result_dir / "concurrent_checkpoints/fixture/checkpoint.jsonl"
            confirm_path = root / "02_confirm/confirm_result.txt"
            target.write_jsonl(str(pair_path), pairs)
            target.write_jsonl(str(project_path), list(projects.values()))
            target.write_jsonl(str(skillsheet_path), list(skillsheets.values()))

            def fake_llm(**kwargs):
                kwargs["response_observer"]({
                    "attempt": 1, "status_code": 200, "success": True,
                    "error_type": "", "rate_limit_headers": {
                        "x-ratelimit-remaining-requests": "1000",
                        "x-ratelimit-remaining-tokens": "1000000",
                    },
                })
                response = success_response(kwargs)
                if invalid_leader and "resource-0-0" in kwargs["user_prompt"]:
                    response["required_skills"][0]["skill"] = "変更されたskill"
                return response

            with patch.object(target, "STEP_DIR", root), patch.object(
                target, "INPUT_PAIRS", pair_path
            ), patch.object(target, "INPUT_PROJECT_SKILLS", project_path), patch.object(
                target, "INPUT_SKILLSHEETS", skillsheet_path
            ), patch.object(target, "OUTPUT_RESULT", result_path), patch.object(
                target, "OUTPUT_ERROR", error_path
            ), patch.object(target, "OUTPUT_RUN_METADATA", metadata_path), patch.object(
                target, "OUTPUT_RETENTION_SIDECAR", result_dir / "retention.jsonl"
            ), patch.object(
                target, "CONCURRENT_CHECKPOINT_ROOT", result_dir / "concurrent_checkpoints"
            ), patch.object(target, "ENABLE_07_1_CONCURRENT", True), patch.object(
                target, "cache_hit_results_for_run", return_value=[]
            ), patch.object(target, "call_llm", side_effect=fake_llm), patch.object(
                target, "get_logger", return_value=Logger()
            ), patch.object(sys, "argv", ["requirement_skill_ai_matching.py", "--concurrent-run-id", "fixture"]):
                self.assertIsNone(target.main())  # process-equivalent exit code 0

            results = list(target.read_jsonl(str(result_path)))
            errors = list(target.read_jsonl(str(error_path)))
            checkpoints = list(target.read_jsonl(str(checkpoint_path)))
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            self.assertEqual(len(checkpoints), 6)
            self.assertEqual(metadata["input_count"], 6)
            self.assertEqual(metadata["processed_count"], 6)
            self.assertGreaterEqual(metadata["peak_concurrency"], 2)
            self.assertEqual(len(results) + len(errors), 6)
            self.assertEqual(
                target.collect_concurrent_checkpoints(
                    list(target.read_jsonl(str(result_dir / "concurrent_checkpoints/fixture/manifest.jsonl"))),
                    checkpoints,
                )["missing"], []
            )
            if invalid_leader:
                self.assertEqual((len(results), len(errors)), (5, 1))
                self.assertEqual(errors[0]["error_type"], "invalid_output_schema")
                self.assertEqual(errors[0]["resource_info"]["message_id"], "resource-0-0")
                self.assertTrue(any(
                    row["resource_info"]["message_id"] == "resource-0-1"
                    for row in results
                ))
                leader = next(row for row in checkpoints if row["ordinal"] == 0)
                self.assertEqual(leader["status"], "error")
                self.assertFalse(leader["telemetry"]["api_failure"])
            else:
                self.assertEqual((len(results), len(errors)), (6, 0))
                self.assertTrue(all(row["status"] == "success" for row in checkpoints))

            confirm_script = STEP_DIR / "02_confirm/confirm_requirement_skill_ai_matching.py"
            spec = importlib.util.spec_from_file_location("isolated_07_1_confirm", confirm_script)
            confirm = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(confirm)
            with patch.object(confirm, "INPUT_PAIRS", pair_path), patch.object(
                confirm, "INPUT_PROJECT_SKILLS", project_path
            ), patch.object(confirm, "OUTPUT_RESULT", result_path), patch.object(
                confirm, "OUTPUT_ERROR", error_path
            ), patch.object(confirm, "RUN_METADATA", metadata_path), patch.object(
                confirm, "CONFIRM_RESULT", confirm_path
            ), patch.object(confirm, "get_logger", return_value=Logger()):
                confirm.main()
            confirm_text = confirm_path.read_text(encoding="utf-8")
            self.assertIn("【結果】OK", confirm_text)
            if invalid_leader:
                self.assertIn("invalid_output_schema: 1件", confirm_text)

    def test_validation_error_continues_and_exits_zero(self):
        self.run_isolated_main(invalid_leader=True)

    def test_all_success_concurrent_output_unchanged(self):
        self.run_isolated_main(invalid_leader=False)


class RetentionProductionRegressionTest(unittest.TestCase):
    def test_limited_run_does_not_mix_all_cache_hits_into_sidecar(self):
        with patch.object(target, "load_current_cache_hit_results") as load_hits:
            self.assertEqual(target.cache_hit_results_for_run(10), [])
        load_hits.assert_not_called()

    def test_cache_hit_is_read_only_and_remains_retention_reachable(self):
        with tempfile.TemporaryDirectory(prefix="07_1_cache_hit_guard_") as temporary:
            root = Path(temporary)
            duplicates = root / "duplicates.jsonl"
            diff_file = root / "diff.jsonl"
            cache_file = root / "success_cache.jsonl"
            target.write_jsonl(
                str(duplicates),
                [
                    {
                        "project_info": {"message_id": "current-project"},
                        "resource_info": {"message_id": "current-resource"},
                    }
                ],
            )
            target.write_jsonl(
                str(diff_file),
                [
                    {
                        "project_info": {
                            "message_id": "current-project",
                            "from": "project@example.com",
                            "subject": "project-subject",
                        },
                        "resource_info": {
                            "message_id": "current-resource",
                            "from": "resource@example.com",
                            "subject": "resource-subject",
                        },
                    }
                ],
            )
            target.write_jsonl(
                str(cache_file),
                [
                    {
                        "cache_version": 1,
                        "comparison_key": {
                            "project_from": "project@example.com",
                            "project_subject": "project-subject",
                            "resource_from": "resource@example.com",
                            "resource_subject": "resource-subject",
                        },
                        "source_message_ids": {
                            "project_message_id": "old-project",
                            "resource_message_id": "old-resource",
                        },
                        "required_skills": [
                            {
                                "skill": "Pythonコーディング経験",
                                "match": False,
                                "note": "Python経験は1ヶ月のみ",
                            }
                        ],
                        "optional_skills": [],
                        "evaluation_meta": {"llm_model": "gpt-4o-mini"},
                    }
                ],
            )
            cache_before = cache_file.read_bytes()
            with patch.object(target, "INPUT_DUPLICATE_PAIRS", duplicates), patch.object(
                target, "INPUT_DIFF_FILE", diff_file
            ), patch.object(target, "SUCCESS_CACHE_FILE", cache_file):
                restored = target.load_current_cache_hit_results()

            self.assertEqual(cache_file.read_bytes(), cache_before)
            self.assertEqual(len(restored), 1)
            self.assertTrue(restored[0]["duplicate_proposal_check"])
            self.assertEqual(
                restored[0]["project_info"]["message_id"], "current-project"
            )
            retained, stats = retention_guard.build_retention_sidecar(
                restored,
                {
                    "current-resource": {
                        "skillsheet": "Python業務でスクレイピングを実装"
                    }
                },
            )
            self.assertEqual(len(retained), 1)
            self.assertEqual(stats["guard_false_to_true"], 0)
            self.assertTrue(retained[0]["duplicate_proposal_check"])

    def test_guard_fixtures_fail_closed_and_positive_whitelist(self):
        mixed = {
            "skill": "PythonまたはJavaとSQLの開発経験",
            "match": False,
            "note": "Python経験は1ヶ月のみ",
        }
        non_duration = {
            "skill": "Pythonコーディング経験",
            "match": False,
            "note": "Python経験は1ヶ月のみで、業務経験も不明確",
        }
        positive = {
            "skill": "Pythonコーディング経験",
            "match": False,
            "note": "Python経験は1ヶ月のみ",
        }
        self.assertIsNone(
            retention_guard.evaluate_required_skill(mixed, "Python業務開発")
        )
        self.assertIsNone(
            retention_guard.evaluate_required_skill(non_duration, "Python業務開発")
        )
        self.assertIsNotNone(
            retention_guard.evaluate_required_skill(positive, "Python業務で実装")
        )

    def test_saved_500_retains_four_and_rescues_known_loss_without_mutation(self):
        rows = list(target.read_jsonl(str(SAVED_500 / "results.jsonl")))
        before = copy.deepcopy(rows)
        skillsheets = {
            str(row["message_id"]): row
            for row in target.read_jsonl(str(target.INPUT_SKILLSHEETS))
        }
        retained, stats = retention_guard.build_retention_sidecar(rows, skillsheets)
        keys = {
            (
                row["project_info"]["message_id"],
                row["resource_info"]["message_id"],
            )
            for row in retained
        }
        expected_keys = {
            (
                str(row["project_message_id"]),
                str(row["resource_message_id"]),
            )
            for row in target.read_jsonl(
                str(SAVED_RETENTION_500 / "retained_pairs.jsonl")
            )
        }
        self.assertEqual(len(retained), 4)
        self.assertEqual(keys, expected_keys)
        self.assertIn(("1a0230d86a22a79b", "1a0225959a74d90a"), keys)
        self.assertEqual(stats["mixed_or_and_retained"], 0)
        self.assertEqual(stats["non_duration_reason_retained"], 0)
        self.assertEqual(stats["guard_false_to_true"], 0)
        self.assertEqual(stats["proposal_ready_direct_promotion"], 0)
        self.assertEqual(rows, before)
        self.assertTrue(
            all(
                row["retention_guard"]["proposal_ready_direct"] is False
                for row in retained
            )
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""Focused/offline tests for _test_07_1_speedup.py.  No network calls."""

import importlib.util
import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import requests


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parents[1]
TARGET_PATH = TOOL_DIR / "_test_07_1_speedup.py"
sys.path.insert(0, str(PROJECT_ROOT))


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


target = load_module("test_07_1_speedup_target", TARGET_PATH)


def fixture_inputs(project_count=2, resources_per_project=3):
    pairs = []
    projects = {}
    skillsheets = {}
    for p_index in range(project_count):
        project_mid = "project-{}".format(p_index)
        projects[project_mid] = {
            "message_id": project_mid,
            "required_skills": [{"skill": "Python"}],
            "optional_skills": [{"skill": "AWS"}],
        }
        for r_index in range(resources_per_project):
            resource_mid = "resource-{}-{}".format(p_index, r_index)
            skillsheets[resource_mid] = {
                "message_id": resource_mid,
                "success": True,
                "skillsheet": "PythonとAWSの実務経験あり\n案件 {}-{}".format(
                    p_index, r_index
                ),
                "source": "offline-test",
            }
            pairs.append(
                {
                    "project_info": {"message_id": project_mid},
                    "resource_info": {"message_id": resource_mid},
                }
            )
    return pairs, projects, skillsheets


class FakeResponse:
    def __init__(self, status_code, body, headers=None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}
        self.text = json.dumps(body, ensure_ascii=False)

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(
                "status={}".format(self.status_code), response=self
            )


class FakeConcurrentClient:
    def __init__(self, delay=0.005, fail_set_expected=False):
        self.delay = delay
        self.fail_set_expected = fail_set_expected
        self.local = threading.local()
        self.lock = threading.Lock()
        self.events = []
        self.active = 0
        self.peak = 0
        self.call_count = 0

    def set_expected(self, request_hash):
        if self.fail_set_expected:
            raise RuntimeError("worker setup failure")
        self.local.request_hash = request_hash
        self.local.metadata = {}

    def pop_metadata(self):
        value = self.local.metadata
        self.local.metadata = {}
        return value

    def __call__(self, **kwargs):
        request_hash = self.local.request_hash
        with self.lock:
            self.call_count += 1
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.events.append(("start", request_hash, time.monotonic()))
        time.sleep(self.delay)
        response = target._valid_response_from_schema(kwargs["response_schema"])
        with self.lock:
            self.events.append(("end", request_hash, time.monotonic()))
            self.active -= 1
        self.local.metadata = {
            "latency_seconds": self.delay,
            "attempts": [
                {
                    "attempt": 1,
                    "status_code": 200,
                    "latency_seconds": self.delay,
                    "error_type": "",
                    "rate_limit_headers": {
                        "x-ratelimit-remaining-requests": "1000",
                        "x-ratelimit-remaining-tokens": "1000000",
                    },
                    "usage": {
                        "input_tokens": 100,
                        "cached_input_tokens": 50,
                        "output_tokens": 10,
                        "total_tokens": 110,
                        "usage_available": True,
                    },
                }
            ],
            "retry_count": 0,
            "rate_limit_429_count": 0,
            "api_failure": False,
            "usage": {
                "input_tokens": 100,
                "cached_input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 110,
                "usage_available": True,
            },
            "rate_limit_headers": {
                "x-ratelimit-remaining-requests": "1000",
                "x-ratelimit-remaining-tokens": "1000000",
            },
            "request_body_sha256": request_hash,
        }
        return response


class RequestParityTest(unittest.TestCase):
    def test_direct_payload_equals_production_call_llm_payload(self):
        pairs, projects, skillsheets = fixture_inputs(1, 1)
        kwargs = target.capture_production_request(pairs[0], projects, skillsheets)
        expected_payload = target.build_request_payload(kwargs)
        schema_response = target._valid_response_from_schema(kwargs["response_schema"])
        response_body = {
            "choices": [
                {
                    "message": {"content": json.dumps(schema_response, ensure_ascii=False)},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 10,
                "total_tokens": 110,
            },
        }
        captured = {}

        def fake_post(url, headers, json, timeout):
            captured["url"] = url
            captured["headers"] = headers
            captured["json"] = json
            captured["timeout"] = timeout
            return FakeResponse(200, response_body)

        call_kwargs = dict(kwargs)
        call_kwargs["telemetry_context"] = None
        import common.llm_client as llm_client

        with patch.object(llm_client, "_get_api_key", return_value="offline-key"), patch.object(
            llm_client, "_enforce_rate_limit"
        ), patch.object(llm_client.requests, "post", side_effect=fake_post):
            llm_client.call_llm(**call_kwargs)

        self.assertEqual(captured["json"], expected_payload)
        self.assertEqual(expected_payload["model"], target.production.LLM_MODEL)
        self.assertEqual(expected_payload["temperature"], 0.0)
        self.assertEqual(expected_payload["max_tokens"], 2048)
        self.assertEqual(expected_payload["response_format"], {"type": "json_object"})


class SamplingAndCollectionTest(unittest.TestCase):
    def setUp(self):
        self.pairs, self.projects, self.skillsheets = fixture_inputs(3, 4)

    def test_sampling_is_deterministic_and_contains_repeated_projects(self):
        first = target.deterministic_sample(
            self.pairs, self.projects, self.skillsheets, 8, "fixed-seed"
        )
        second = target.deterministic_sample(
            self.pairs, self.projects, self.skillsheets, 8, "fixed-seed"
        )
        self.assertEqual(
            [row["request_identity"] for row in first],
            [row["request_identity"] for row in second],
        )
        counts = {}
        for row in first:
            counts[row["project_message_id"]] = counts.get(row["project_message_id"], 0) + 1
        self.assertGreaterEqual(len(counts), 2)
        self.assertTrue(all(count >= 2 for count in counts.values()))

    def test_sampling_expands_projects_for_40_100_and_300(self):
        pairs, projects, skillsheets = fixture_inputs(20, 20)
        production_before = target.snapshot_production_outputs()

        for sample_size in (40, 100, 300):
            first = target.deterministic_sample(
                pairs, projects, skillsheets, sample_size, "scale-seed"
            )
            second = target.deterministic_sample(
                pairs, projects, skillsheets, sample_size, "scale-seed"
            )
            first_identities = [row["request_identity"] for row in first]
            second_identities = [row["request_identity"] for row in second]
            self.assertEqual(len(first), sample_size)
            self.assertEqual(first_identities, second_identities)
            self.assertEqual(len(first_identities), len(set(first_identities)))
            self.assertEqual(
                [row["original_ordinal"] for row in first],
                sorted(row["original_ordinal"] for row in first),
            )

            rows_by_project = {}
            for row in first:
                project_mid = row["project_message_id"]
                rows_by_project.setdefault(project_mid, []).append(row)
                self.assertEqual(
                    project_mid,
                    row["pair"]["project_info"]["message_id"],
                )
            for rows in rows_by_project.values():
                warm_rows = [row for row in rows if row["is_project_warm_one"]]
                self.assertEqual(len(warm_rows), 1)
                self.assertEqual(
                    warm_rows[0]["original_ordinal"],
                    min(row["original_ordinal"] for row in rows),
                )

            if sample_size == 40:
                self.assertEqual(len(rows_by_project), 4)
            else:
                self.assertGreater(len(rows_by_project), 4)

        with self.assertRaises(ValueError):
            target.deterministic_sample(
                pairs,
                projects,
                skillsheets,
                target.MAX_LIVE_SAMPLE_SIZE + 1,
                "scale-seed",
            )
        self.assertEqual(production_before, target.snapshot_production_outputs())

    def test_collector_restores_ordinal_after_shuffled_completion(self):
        sample = target.deterministic_sample(
            self.pairs, self.projects, self.skillsheets, 6
        )
        manifest = [target.manifest_record(row) for row in sample]
        checkpoints = [
            self._checkpoint(row) for row in reversed(manifest)
        ]
        collected = target.collect_checkpoints(manifest, checkpoints)
        ordinals = [row["ordinal"] for row in collected["ordered"]]
        self.assertEqual(ordinals, sorted(ordinals))

    def test_duplicate_missing_unknown_and_malformed_are_detected(self):
        sample = target.deterministic_sample(
            self.pairs, self.projects, self.skillsheets, 4
        )
        manifest = [target.manifest_record(row) for row in sample]
        valid = [self._checkpoint(manifest[0])]
        duplicate = dict(valid[0])
        unknown = dict(valid[0], request_identity="unknown")
        malformed = dict(self._checkpoint(manifest[1]), ordinal=-1)
        with self.assertRaisesRegex(
            ValueError, "duplicate=1 unknown=1 malformed=1 missing=3"
        ):
            target.collect_checkpoints(
                manifest, valid + [duplicate, unknown, malformed]
            )

    @staticmethod
    def _checkpoint(manifest_row):
        return {
            **manifest_row,
            "status": "success",
            "result": {},
            "error": None,
            "telemetry": {},
        }


class RetryAndAdaptiveTest(unittest.TestCase):
    def test_429_retries_with_backoff_and_records_usage(self):
        calls = []
        response_schema = {
            "required_skills": [{"skill": "Python", "match": False, "note": ""}],
            "optional_skills": [],
        }
        success_content = target._valid_response_from_schema(response_schema)
        responses = [
            FakeResponse(429, {"error": {"message": "limited"}}, {"Retry-After": "0"}),
            FakeResponse(
                200,
                {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(success_content, ensure_ascii=False)
                            },
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 100,
                        "completion_tokens": 10,
                        "total_tokens": 110,
                        "prompt_tokens_details": {"cached_tokens": 50},
                    },
                },
                {
                    "x-ratelimit-remaining-requests": "100",
                    "x-ratelimit-remaining-tokens": "100000",
                },
            ),
        ]

        def fake_post(*args, **kwargs):
            calls.append(kwargs["json"])
            return responses.pop(0)

        client = target.DirectTestClient(
            "offline-key",
            post=fake_post,
            rate_limiter=target.LaunchRateLimiter(0.0),
        )
        kwargs = {
            "system_prompt": "system",
            "user_prompt": "user",
            "response_schema": response_schema,
            "model": "gpt-4o-mini",
            "temperature": 0.0,
            "max_tokens": 2048,
            "max_retries": 3,
        }
        payload = target.build_request_payload(kwargs)
        client.set_expected(target._json_hash(payload))
        result = client(**kwargs)
        metadata = client.pop_metadata()
        self.assertEqual(result, success_content)
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(metadata["retry_count"], 1)
        self.assertEqual(metadata["rate_limit_429_count"], 1)
        self.assertEqual(metadata["usage"]["cached_input_tokens"], 50)

    def test_no_headers_never_increases_concurrency(self):
        controller = target.AdaptiveConcurrency(2, 4)
        for _ in range(10):
            controller.observe(
                {
                    "status": "success",
                    "telemetry": {
                        "api_failure": False,
                        "latency_seconds": 1.0,
                        "rate_limit_headers": {},
                    },
                }
            )
        self.assertEqual(controller.current, 2)

    def test_429_decreases_and_upper_bound_is_enforced(self):
        controller = target.AdaptiveConcurrency(2, 3)
        good = {
            "status": "success",
            "telemetry": {
                "api_failure": False,
                "latency_seconds": 1.0,
                "rate_limit_429_count": 0,
                "rate_limit_headers": {
                    "x-ratelimit-remaining-requests": "1000",
                    "x-ratelimit-remaining-tokens": "1000000",
                },
            },
        }
        for _ in range(10):
            controller.observe(good)
        self.assertEqual(controller.current, 3)
        controller.observe(
            {
                "status": "success",
                "telemetry": {"rate_limit_429_count": 1},
            }
        )
        self.assertEqual(controller.current, 2)
        with self.assertRaises(ValueError):
            target.AdaptiveConcurrency(1, target.MAX_CONCURRENCY_HARD_LIMIT + 1)


class SchedulerTest(unittest.TestCase):
    def setUp(self):
        self.pairs, self.projects, self.skillsheets = fixture_inputs(6, 2)
        self.sample = target.deterministic_sample(
            self.pairs, self.projects, self.skillsheets, 12
        )
        self.temp = tempfile.TemporaryDirectory(prefix="test_07_1_speedup_")
        self.checkpoint = Path(self.temp.name) / "checkpoint.jsonl"

    def tearDown(self):
        self.temp.cleanup()

    def test_warm_one_then_fan_out_and_concurrency_bound(self):
        client = FakeConcurrentClient(delay=0.01)
        checkpoints, controller, stopped = target.run_scheduler(
            self.sample,
            self.projects,
            self.skillsheets,
            client,
            self.checkpoint,
            initial_concurrency=2,
            max_concurrency=3,
        )
        self.assertFalse(stopped)
        self.assertEqual(len(checkpoints), len(self.sample))
        self.assertLessEqual(client.peak, 3)
        event_times = {
            (kind, request_hash): timestamp
            for kind, request_hash, timestamp in client.events
        }
        by_project = {}
        for row in self.sample:
            by_project.setdefault(row["project_message_id"], []).append(row)
        for rows in by_project.values():
            rows.sort(key=lambda row: row["original_ordinal"])
            leader_end = event_times[("end", rows[0]["request_body_sha256"])]
            for follower in rows[1:]:
                self.assertLessEqual(
                    leader_end,
                    event_times[("start", follower["request_body_sha256"])],
                )
        self.assertLessEqual(max(row["concurrency"] for row in controller.history), 3)

    def test_checkpoint_resume_only_calls_missing_requests(self):
        first_client = FakeConcurrentClient(delay=0.0)
        leader = next(row for row in self.sample if row["is_project_warm_one"])
        original_call_llm = target.production.call_llm
        target.production.call_llm = first_client
        try:
            existing = target._worker(
                leader, self.projects, self.skillsheets, first_client, 1
            )
        finally:
            target.production.call_llm = original_call_llm
        target.write_jsonl(str(self.checkpoint), [existing])
        resume_client = FakeConcurrentClient(delay=0.0)
        checkpoints, _, stopped = target.run_scheduler(
            self.sample,
            self.projects,
            self.skillsheets,
            resume_client,
            self.checkpoint,
            initial_concurrency=2,
            max_concurrency=2,
            existing_checkpoints=[existing],
        )
        self.assertFalse(stopped)
        self.assertEqual(resume_client.call_count, len(self.sample) - 1)
        collected = target.collect_checkpoints(
            [target.manifest_record(row) for row in self.sample], checkpoints
        )
        self.assertEqual(len(collected["ordered"]), len(self.sample))

    def test_worker_exception_is_checkpointed_and_stops_new_work(self):
        client = FakeConcurrentClient(fail_set_expected=True)
        checkpoints, _, stopped = target.run_scheduler(
            self.sample,
            self.projects,
            self.skillsheets,
            client,
            self.checkpoint,
            initial_concurrency=1,
            max_concurrency=1,
        )
        self.assertTrue(stopped)
        self.assertEqual(len(checkpoints), 1)
        self.assertEqual(checkpoints[0]["status"], "error")
        self.assertIn("worker setup failure", checkpoints[0]["worker_exception"])

    def test_offline_run_writes_zero_production_outputs(self):
        before = target.snapshot_production_outputs()
        client = FakeConcurrentClient(delay=0.0)
        target.run_scheduler(
            self.sample,
            self.projects,
            self.skillsheets,
            client,
            self.checkpoint,
            initial_concurrency=2,
            max_concurrency=2,
        )
        after = target.snapshot_production_outputs()
        self.assertEqual(before, after)


class CostAndCliGuardTest(unittest.TestCase):
    def test_usage_and_cost_include_cached_discount(self):
        checkpoint = {
            "telemetry": {
                "attempts": [
                    {
                        "usage": {
                            "input_tokens": 1000,
                            "cached_input_tokens": 400,
                            "output_tokens": 100,
                            "total_tokens": 1100,
                        }
                    }
                ]
            }
        }
        usage = target._aggregate_usage([checkpoint])
        expected = (600 * 0.15 + 400 * 0.075 + 100 * 0.60) / 1_000_000
        self.assertAlmostEqual(usage["estimated_cost_usd"], expected)
        self.assertEqual(usage["cache_rate"], 0.4)

    def test_network_flag_and_sample_hard_limit(self):
        self.assertEqual(target.MAX_LIVE_SAMPLE_SIZE, 300)
        with self.assertRaises(SystemExit):
            target.main(["--sample-size", "30"])
        with self.assertRaises(SystemExit):
            target.main(
                [
                    "--sample-size",
                    str(target.MAX_LIVE_SAMPLE_SIZE + 1),
                    "--allow-network",
                ]
            )

    def test_run_directory_cannot_escape_test_root(self):
        with self.assertRaises(ValueError):
            target._safe_run_dir("../01_result")


if __name__ == "__main__":
    unittest.main(verbosity=2)

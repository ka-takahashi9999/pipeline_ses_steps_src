"""Focused fixture/unit tests for _test_batch_api_canary.py (API calls: zero)."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("_test_batch_api_canary.py")
SPEC = importlib.util.spec_from_file_location("batch_api_canary", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("canary module import failed")
canary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(canary)


class BatchApiCanaryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.candidates, cls.excluded = canary._load_candidates()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "_test_batch_api_canary"

    def tearDown(self):
        self.temporary.cleanup()

    def _prepare(self, run_id, sample_size):
        result = canary.prepare_run(
            run_id,
            sample_size,
            root=self.root,
            candidates=self.candidates,
            excluded=self.excluded,
        )
        return Path(result["run_dir"]), result

    def _manifest(self, run_dir):
        return list(canary.read_jsonl(str(run_dir / "manifest.jsonl")))

    @staticmethod
    def _fixture_response(entry):
        checks = [
            {
                "skill": "fixture skill",
                "confidence": "human_review",
                "reason": "fixture reason",
                "evidence": "",
            }
            for _ in range(entry["required_skill_count"])
        ]
        content = {
            "required_skill_checks": checks,
            "category_match": "unclear",
            "category_note": "fixture",
        }
        return {
            "id": "batch_req_fixture",
            "custom_id": entry["custom_id"],
            "response": {
                "status_code": 200,
                "request_id": "req_fixture",
                "body": {
                    "choices": [{"message": {"content": json.dumps(content)}}],
                    "usage": {
                        "prompt_tokens": 100,
                        "prompt_tokens_details": {"cached_tokens": 20},
                        "completion_tokens": 30,
                        "total_tokens": 130,
                    },
                },
            },
            "error": None,
        }

    def test_prepare_50_300_678_and_manifest_counts(self):
        for sample_size in (50, 300, 678):
            with self.subTest(sample_size=sample_size):
                run_dir, result = self._prepare(f"unit-{sample_size}", sample_size)
                self.assertEqual(sample_size, result["input_count"])
                self.assertEqual(sample_size, result["manifest_count"])
                self.assertTrue(result["custom_id_unique"])
                self.assertEqual(0, result["production_write"])
                self.assertEqual(0, result["api_calls"])
                self.assertTrue((run_dir / "batch_state.json").exists())

    def test_sampling_is_deterministic_and_distribution_aware(self):
        first = canary._sample_candidates(self.candidates, 50)
        second = canary._sample_candidates(self.candidates, 50)
        first_ids = [item["source_record_sha256"] for item in first]
        second_ids = [item["source_record_sha256"] for item in second]
        self.assertEqual(first_ids, second_ids)
        self.assertGreater(len({item["score_band"] for item in first}), 1)
        self.assertGreater(len({canary._skill_bucket(item["required_skill_count"]) for item in first}), 1)

    def test_custom_id_unique_deterministic_and_manifest_integrity(self):
        run_dir, _ = self._prepare("manifest-check", 50)
        manifest = self._manifest(run_dir)
        ids = [entry["custom_id"] for entry in manifest]
        self.assertEqual(len(ids), len(set(ids)))
        first_again = canary._custom_id("manifest-check", 1, canary._sample_candidates(self.candidates, 50)[0])
        self.assertEqual(ids[0], first_again)
        validation = canary._validate_prepared(run_dir)
        self.assertEqual(50, validation["input_count"])

    def test_batch_request_matches_current_08_5_contract(self):
        run_dir, _ = self._prepare("contract", 50)
        request = next(canary.read_jsonl(str(run_dir / "input.jsonl")))
        manifest_entry = self._manifest(run_dir)[0]
        selected_candidate = next(
            item
            for item in self.candidates
            if item["source_record_sha256"]
            == manifest_entry["source_record_sha256"]
        )
        body = request["body"]
        self.assertEqual(selected_candidate["body"], body)
        self.assertEqual(
            {"model", "messages", "temperature", "max_tokens", "response_format"},
            set(body),
        )
        self.assertEqual(canary.PRODUCTION.RECHECK_LLM_MODEL, body["model"])
        self.assertEqual(0.0, body["temperature"])
        self.assertEqual(4096, body["max_tokens"])
        self.assertEqual({"type": "json_object"}, body["response_format"])
        self.assertTrue(body["messages"][0]["content"].startswith(canary.PRODUCTION.SYSTEM_PROMPT))
        schema = selected_candidate["response_schema"]
        serialized_schema = json.dumps(schema, ensure_ascii=False)
        self.assertNotIn('"original_match"', serialized_schema)
        self.assertNotIn('"recheck_match"', serialized_schema)

    def test_production_path_write_and_overwrite_are_refused(self):
        with self.assertRaises(ValueError):
            canary._assert_canary_path(canary.PRODUCTION_RESULT_DIR / "forbidden.json")
        self._prepare("no-overwrite", 50)
        with self.assertRaises(FileExistsError):
            self._prepare("no-overwrite", 50)

    def test_response_shuffle_is_restored_to_ordinal_order(self):
        run_dir, _ = self._prepare("shuffle", 50)
        manifest = self._manifest(run_dir)
        outputs = [self._fixture_response(entry) for entry in manifest]
        outputs = list(reversed(outputs))
        result = canary._validate_responses(manifest, outputs, [])
        self.assertTrue(result["integrity_ok"])
        self.assertEqual(50, result["success_count"])
        self.assertEqual(list(range(1, 51)), [item["ordinal"] for item in result["shadow_results"]])

    def test_duplicate_custom_id_is_detected(self):
        run_dir, _ = self._prepare("duplicate", 50)
        manifest = self._manifest(run_dir)
        responses = [self._fixture_response(entry) for entry in manifest]
        responses.append(responses[0])
        result = canary._validate_responses(manifest, responses, [])
        self.assertEqual(1, result["duplicate_count"])
        self.assertFalse(result["integrity_ok"])

    def test_missing_custom_id_is_detected(self):
        run_dir, _ = self._prepare("missing", 50)
        manifest = self._manifest(run_dir)
        responses = [self._fixture_response(entry) for entry in manifest[:-1]]
        result = canary._validate_responses(manifest, responses, [])
        self.assertEqual(1, result["missing_count"])
        self.assertFalse(result["integrity_ok"])

    def test_unknown_custom_id_is_detected(self):
        run_dir, _ = self._prepare("unknown", 50)
        manifest = self._manifest(run_dir)
        responses = [self._fixture_response(entry) for entry in manifest]
        unknown = self._fixture_response(manifest[0])
        unknown["custom_id"] = "unknown-id"
        responses.append(unknown)
        result = canary._validate_responses(manifest, responses, [])
        self.assertEqual(1, result["unknown_count"])
        self.assertFalse(result["integrity_ok"])

    def test_malformed_json_is_detected(self):
        run_dir, _ = self._prepare("malformed", 50)
        path = run_dir / "output_raw.jsonl"
        path.write_text('{"custom_id":"ok"}\n{malformed\n', encoding="utf-8")
        records, errors = canary._read_jsonl_strict(path)
        self.assertEqual(1, len(records))
        self.assertEqual(1, len(errors))
        self.assertIn("malformed JSON", errors[0])

    def test_manifest_mismatch_is_refused(self):
        run_dir, _ = self._prepare("mismatch", 50)
        manifest_path = run_dir / "manifest.jsonl"
        manifest = self._manifest(run_dir)
        manifest[0]["request_sha256"] = "0" * 64
        canary.write_jsonl(str(manifest_path), manifest)
        with self.assertRaisesRegex(ValueError, "manifest mismatch"):
            canary._validate_prepared(run_dir)

    def test_source_record_manifest_mismatch_is_refused(self):
        run_dir, _ = self._prepare("source-mismatch", 50)
        manifest_path = run_dir / "manifest.jsonl"
        manifest = self._manifest(run_dir)
        manifest[0]["source_record_sha256"] = "0" * 64
        canary.write_jsonl(str(manifest_path), manifest)
        with self.assertRaisesRegex(ValueError, "source record SHA-256"):
            canary._validate_prepared(
                run_dir, canary._source_hash_map(self.candidates)
            )

    def test_fixture_report_usage_latency_and_shadow_output(self):
        run_dir, _ = self._prepare("report", 50)
        manifest = self._manifest(run_dir)
        responses = [self._fixture_response(entry) for entry in reversed(manifest)]
        canary.write_jsonl(str(run_dir / "output_raw.jsonl"), responses)
        state_path = run_dir / "batch_state.json"
        state = canary._read_json(state_path)
        state["batch_id"] = "batch_fixture"
        state["batch_submitted_at"] = "2026-08-22T00:00:00Z"
        state["last_status"] = "completed"
        state["request_counts"] = {"total": 50, "completed": 50, "failed": 0}
        state["batch_timestamps"]["in_progress_at"] = 1787360400
        state["batch_timestamps"]["completed_at"] = 1787364000
        canary._write_json(state_path, state, root=self.root)
        report = canary.report_run("report", root=self.root)
        self.assertTrue(report["integrity_ok"])
        self.assertEqual(50, report["completed"])
        self.assertEqual(5000, report["usage"]["input_tokens"])
        self.assertEqual(1000, report["usage"]["cached_input_tokens"])
        self.assertEqual(1500, report["usage"]["output_tokens"])
        self.assertEqual(6500, report["usage"]["total_tokens"])
        self.assertEqual(list(range(1, 51)), [row["ordinal"] for row in report["shadow_results"]])
        self.assertTrue((run_dir / "report.json").exists())
        self.assertTrue((run_dir / "report.txt").exists())

    def test_status_history_uses_api_status_and_timestamps(self):
        state = {"status_history": [], "batch_timestamps": {}}
        canary._update_batch_state(
            state,
            {
                "status": "finalizing",
                "created_at": 100,
                "in_progress_at": 200,
                "finalizing_at": 300,
                "request_counts": {"total": 50, "completed": 50, "failed": 0},
                "usage": {
                    "input_tokens": 10,
                    "input_tokens_details": {"cached_tokens": 2},
                    "output_tokens": 3,
                    "total_tokens": 13,
                },
            },
        )
        self.assertEqual("finalizing", state["last_status"])
        self.assertEqual("finalizing", state["status_history"][0]["status"])
        self.assertEqual(300, state["batch_timestamps"]["finalizing_at"])
        self.assertEqual(13, state["batch_usage"]["total_tokens"])

    def test_secret_patterns_are_rejected_and_artifacts_are_clean(self):
        run_dir, _ = self._prepare("secrets", 50)
        canary._ensure_no_secrets(list(run_dir.iterdir()))
        secret_file = run_dir / "fixture-secret.txt"
        secret_file.write_text("Authorization: Bearer sk-fixture-secret-123456789", encoding="utf-8")
        with self.assertRaises(ValueError):
            canary._ensure_no_secrets([secret_file])

    def test_api_modes_require_explicit_network_permission(self):
        with self.assertRaises(PermissionError):
            canary._require_network(False)

    def test_invalid_run_id_and_sample_limit_are_refused(self):
        for run_id in ("../escape", "has space", "x" * 25):
            with self.subTest(run_id=run_id), self.assertRaises(ValueError):
                canary._validate_run_id(run_id)
        with self.assertRaises(ValueError):
            canary._sample_candidates(self.candidates, 679)


if __name__ == "__main__":
    unittest.main(verbosity=2)

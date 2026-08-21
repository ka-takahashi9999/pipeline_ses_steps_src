import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from common import llm_client


class FakeResponse:
    status_code = 200
    text = ""

    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class CallLlmTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.telemetry_context = {
            "step": "test_step",
            "output_dir": self.temp_dir.name,
            "run_id": "test-run",
            "run_date": "20260821",
        }

    def tearDown(self):
        self.temp_dir.cleanup()

    def usage_records(self):
        usage_path = Path(self.temp_dir.name) / "llm_usage_test-run.jsonl"
        if not usage_path.exists():
            return []
        return [json.loads(line) for line in usage_path.read_text(encoding="utf-8").splitlines()]

    def call(
        self,
        response,
        max_retries=3,
        system_prompt="system",
        user_prompt="user",
        telemetry_context=None,
    ):
        post_result = response if not isinstance(response, list) else None
        post_side_effect = response if isinstance(response, list) else None
        with patch.object(llm_client, "_get_api_key", return_value="test-key"), patch.object(
            llm_client, "_enforce_rate_limit"
        ), patch.object(
            llm_client.requests,
            "post",
            return_value=post_result,
            side_effect=post_side_effect,
        ) as post_mock, patch.object(
            llm_client.time, "sleep"
        ):
            try:
                result = llm_client.call_llm(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    response_schema={"result": ""},
                    max_tokens=10,
                    max_retries=max_retries,
                    retry_wait_seconds=0,
                    telemetry_context=(
                        self.telemetry_context
                        if telemetry_context is None
                        else telemetry_context
                    ),
                )
                return result, post_mock.call_count, None, self.usage_records()
            except Exception as error:
                return None, post_mock.call_count, error, self.usage_records()

    def test_finish_reason_length_is_not_retried(self):
        response = FakeResponse(
            {
                "choices": [
                    {
                        "finish_reason": "length",
                        "message": {"content": '{"result":"unfinished'},
                    }
                ]
            }
        )

        _, call_count, error, records = self.call(response)

        self.assertIsInstance(error, llm_client.LLMOutputTruncatedError)
        self.assertEqual(call_count, 1)
        self.assertEqual(len(records), 1)
        self.assertFalse(records[0]["success"])
        self.assertEqual(records[0]["error_type"], "LLMOutputTruncatedError")

    def test_normal_json_parse_error_still_retries(self):
        response = FakeResponse(
            {
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"content": '{"result":"unfinished'},
                    }
                ]
            }
        )

        _, call_count, error, records = self.call(response)

        self.assertIsInstance(error, ValueError)
        self.assertNotIsInstance(error, llm_client.LLMOutputTruncatedError)
        self.assertEqual(call_count, 3)
        self.assertEqual(len(records), 3)

    def test_success_response_contract_is_unchanged(self):
        response = FakeResponse(
            {
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"content": '{"result":"ok"}'},
                    }
                ]
            }
        )

        result, call_count, error, records = self.call(response)

        self.assertIsNone(error)
        self.assertEqual(result, {"result": "ok"})
        self.assertEqual(call_count, 1)
        self.assertEqual(len(records), 1)

    def test_usage_available_success_is_recorded_with_required_schema(self):
        response = FakeResponse(
            {
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"content": '{"result":"ok"}'},
                    }
                ],
                "usage": {
                    "prompt_tokens": 120,
                    "completion_tokens": 8,
                    "total_tokens": 128,
                },
            }
        )

        result, _, error, records = self.call(response)

        self.assertIsNone(error)
        self.assertEqual(result, {"result": "ok"})
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(
            set(record),
            {
                "run_id",
                "run_date",
                "step",
                "model",
                "call_number",
                "attempt_number",
                "input_tokens",
                "cached_input_tokens",
                "output_tokens",
                "total_tokens",
                "usage_available",
                "success",
                "error_type",
            },
        )
        self.assertEqual(record["input_tokens"], 120)
        self.assertEqual(record["cached_input_tokens"], 0)
        self.assertEqual(record["output_tokens"], 8)
        self.assertEqual(record["total_tokens"], 128)
        self.assertEqual(
            record["total_tokens"],
            record["input_tokens"] + record["output_tokens"],
        )
        self.assertTrue(record["usage_available"])
        self.assertTrue(record["success"])
        self.assertEqual(record["error_type"], "")
        for key in (
            "call_number",
            "attempt_number",
            "input_tokens",
            "cached_input_tokens",
            "output_tokens",
            "total_tokens",
        ):
            self.assertGreaterEqual(record[key], 0)

    def test_usage_missing_does_not_change_success_result(self):
        response = FakeResponse(
            {
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"content": '{"result":"ok"}'},
                    }
                ]
            }
        )

        result, _, error, records = self.call(response)

        self.assertIsNone(error)
        self.assertEqual(result, {"result": "ok"})
        self.assertFalse(records[0]["usage_available"])
        self.assertEqual(
            [
                records[0]["input_tokens"],
                records[0]["cached_input_tokens"],
                records[0]["output_tokens"],
                records[0]["total_tokens"],
            ],
            [0, 0, 0, 0],
        )

    def test_cached_tokens_are_read_from_known_nested_usage_field(self):
        response = FakeResponse(
            {
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"content": '{"result":"ok"}'},
                    }
                ],
                "usage": {
                    "prompt_tokens": 200,
                    "completion_tokens": 10,
                    "total_tokens": 210,
                    "prompt_tokens_details": {"cached_tokens": 150},
                },
            }
        )

        _, _, error, records = self.call(response)

        self.assertIsNone(error)
        self.assertEqual(records[0]["cached_input_tokens"], 150)

    def test_retry_records_two_attempts_with_same_call_number(self):
        responses = [
            FakeResponse(
                {
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {"content": '{"result":"broken"'},
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 10,
                        "completion_tokens": 2,
                        "total_tokens": 12,
                    },
                }
            ),
            FakeResponse(
                {
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {"content": '{"result":"ok"}'},
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 11,
                        "completion_tokens": 3,
                        "total_tokens": 14,
                    },
                }
            ),
        ]

        result, call_count, error, records = self.call(responses, max_retries=2)

        self.assertIsNone(error)
        self.assertEqual(result, {"result": "ok"})
        self.assertEqual(call_count, 2)
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["call_number"], records[1]["call_number"])
        self.assertEqual([item["attempt_number"] for item in records], [1, 2])
        self.assertEqual([item["success"] for item in records], [False, True])
        self.assertEqual(records[0]["error_type"], "JSONDecodeError")
        self.assertTrue(all(item["usage_available"] for item in records))

    def test_telemetry_write_failure_warns_and_returns_llm_result(self):
        response = FakeResponse(
            {
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"content": '{"result":"ok"}'},
                    }
                ],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            }
        )
        with patch.object(llm_client, "_get_api_key", return_value="test-key"), patch.object(
            llm_client, "_enforce_rate_limit"
        ), patch.object(llm_client.requests, "post", return_value=response), patch.object(
            llm_client, "_append_telemetry_record", side_effect=OSError("disk failure")
        ), patch.object(llm_client._logger, "warn") as warn_mock:
            result = llm_client.call_llm(
                system_prompt="system",
                user_prompt="user",
                response_schema={"result": ""},
                max_retries=1,
                telemetry_context=self.telemetry_context,
            )

        self.assertEqual(result, {"result": "ok"})
        self.assertTrue(
            any("telemetry書き込み失敗" in call.args[0] for call in warn_mock.call_args_list)
        )

    def test_prompts_body_secret_and_extra_context_are_not_persisted(self):
        forbidden_values = [
            "SYSTEM_PROMPT_SECRET",
            "EMAIL_BODY_SECRET",
            "SKILLSHEET_BODY_SECRET",
            "PROJECT_BODY_SECRET",
            "Bearer API_KEY_SECRET",
        ]
        context = dict(self.telemetry_context)
        context.update(
            {
                "skillsheet": forbidden_values[2],
                "project": forbidden_values[3],
                "authorization": forbidden_values[4],
            }
        )
        response = FakeResponse(
            {
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"content": '{"result":"ok"}'},
                    }
                ],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            }
        )

        _, _, error, _ = self.call(
            response,
            system_prompt=forbidden_values[0],
            user_prompt=forbidden_values[1],
            telemetry_context=context,
        )

        self.assertIsNone(error)
        persisted = (Path(self.temp_dir.name) / "llm_usage_test-run.jsonl").read_text(
            encoding="utf-8"
        )
        for value in forbidden_values:
            self.assertNotIn(value, persisted)

    def test_step_identity_distinguishes_07_1_and_08_5(self):
        response = FakeResponse(
            {
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"content": '{"result":"ok"}'},
                    }
                ],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            }
        )
        for step in (
            "07-1_requirement_skill_ai_matching",
            "08-5_high_score_required_skill_recheck",
        ):
            context = dict(self.telemetry_context)
            context["step"] = step
            _, _, error, _ = self.call(response, telemetry_context=context)
            self.assertIsNone(error)

        self.assertEqual(
            [record["step"] for record in self.usage_records()],
            [
                "07-1_requirement_skill_ai_matching",
                "08-5_high_score_required_skill_recheck",
            ],
        )


if __name__ == "__main__":
    unittest.main()

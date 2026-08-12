import sys
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
    def call(self, response, max_retries=3):
        with patch.object(llm_client, "_get_api_key", return_value="test-key"), patch.object(
            llm_client, "_enforce_rate_limit"
        ), patch.object(llm_client.requests, "post", return_value=response) as post_mock, patch.object(
            llm_client.time, "sleep"
        ):
            try:
                result = llm_client.call_llm(
                    system_prompt="system",
                    user_prompt="user",
                    response_schema={"result": ""},
                    max_tokens=10,
                    max_retries=max_retries,
                    retry_wait_seconds=0,
                )
                return result, post_mock.call_count, None
            except Exception as error:
                return None, post_mock.call_count, error

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

        _, call_count, error = self.call(response)

        self.assertIsInstance(error, llm_client.LLMOutputTruncatedError)
        self.assertEqual(call_count, 1)

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

        _, call_count, error = self.call(response)

        self.assertIsInstance(error, ValueError)
        self.assertNotIsInstance(error, llm_client.LLMOutputTruncatedError)
        self.assertEqual(call_count, 3)

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

        result, call_count, error = self.call(response)

        self.assertIsNone(error)
        self.assertEqual(result, {"result": "ok"})
        self.assertEqual(call_count, 1)


if __name__ == "__main__":
    unittest.main()

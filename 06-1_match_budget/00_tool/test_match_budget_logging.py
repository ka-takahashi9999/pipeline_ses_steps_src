"""06-1の判定結果を変えず、通常ログを集約できていることを確認する。"""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name("match_budget.py")
SPEC = importlib.util.spec_from_file_location("match_budget_under_test", MODULE_PATH)
match_budget = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(match_budget)
CONFIRM_PATH = MODULE_PATH.parents[1] / "02_confirm" / "confirm_match_budget.py"
CONFIRM_SPEC = importlib.util.spec_from_file_location("confirm_match_budget_under_test", CONFIRM_PATH)
confirm_match_budget = importlib.util.module_from_spec(CONFIRM_SPEC)
CONFIRM_SPEC.loader.exec_module(confirm_match_budget)


class RecordingLogger:
    def __init__(self):
        self.infos = []
        self.errors = []

    def info(self, message, message_id=None):
        self.infos.append(message)

    def error(self, message, message_id=None):
        self.errors.append(message)

    def ok(self, message, message_id=None):
        self.infos.append(message)


def _write_jsonl(path, records):
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _read_jsonl(path):
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


class MatchBudgetLoggingTest(unittest.TestCase):
    def test_outputs_and_order_are_unchanged_while_pair_logs_are_suppressed(self):
        with tempfile.TemporaryDirectory(prefix="match_budget_logging_") as temp_name:
            temp = Path(temp_name)
            result = temp / "01_result"
            pairs_path = temp / "pairs.jsonl"
            projects_path = temp / "projects.jsonl"
            resources_path = temp / "resources.jsonl"
            matched_path = result / "matched.jsonl"
            rejected_path = result / "rejected.jsonl"

            pairs = [
                {"project_info": {"message_id": "p1"}, "resource_info": {"message_id": "r1"}},
                {"project_info": {"message_id": "p1"}, "resource_info": {"message_id": "r2"}},
                {"project_info": {"message_id": "p2"}, "resource_info": {"message_id": "r1"}},
                {"project_info": {"message_id": "p2"}, "resource_info": {"message_id": "r2"}},
                {"project_info": {"message_id": "p3"}, "resource_info": {"message_id": "r1"}},
            ]
            _write_jsonl(pairs_path, pairs)
            _write_jsonl(
                projects_path,
                [
                    {"message_id": "p1", "unit_price": 800_000},
                    {"message_id": "p2", "unit_price": None},
                    {"message_id": "p3", "unit_price": 700_000},
                ],
            )
            _write_jsonl(
                resources_path,
                [
                    {"message_id": "r1", "desired_unit_price": 600_000},
                    {"message_id": "r2", "desired_unit_price": 750_000},
                ],
            )

            logger = RecordingLogger()
            with patch.object(match_budget, "STEP_DIR", temp), patch.object(
                match_budget, "INPUT_PAIRS", pairs_path
            ), patch.object(match_budget, "INPUT_PROJECT_BUDGET", projects_path), patch.object(
                match_budget, "INPUT_RESOURCE_BUDGET", resources_path
            ), patch.object(match_budget, "OUTPUT_MATCHED", matched_path), patch.object(
                match_budget, "OUTPUT_NO_MATCHED", rejected_path
            ), patch.object(match_budget, "PROGRESS_INTERVAL", 2), patch.object(
                match_budget, "get_logger", return_value=logger
            ):
                match_budget.main()

            matched = _read_jsonl(matched_path)
            rejected = _read_jsonl(rejected_path)
            self.assertEqual(
                [("p1", "r1"), ("p2", "r1"), ("p2", "r2")],
                [
                    (item["project_info"]["message_id"], item["resource_info"]["message_id"])
                    for item in matched
                ],
            )
            self.assertEqual(
                [("p1", "r2"), ("p3", "r1")],
                [
                    (item["project_info"]["message_id"], item["resource_info"]["message_id"])
                    for item in rejected
                ],
            )
            self.assertTrue(all("match_budget" in item["match_info"] for item in matched + rejected))
            self.assertFalse(any(line.startswith("MATCH:") for line in logger.infos))
            self.assertFalse(any(line.startswith("NO_MATCH:") for line in logger.infos))
            self.assertTrue(any("進捗: 2/5件" in line for line in logger.infos))
            self.assertTrue(any("null既定通過=2" in line for line in logger.infos))
            self.assertEqual([], logger.errors)

            confirm_result = temp / "confirm.txt"
            confirm_logger = RecordingLogger()
            with patch.object(confirm_match_budget, "INPUT_PAIRS", pairs_path), patch.object(
                confirm_match_budget, "OUTPUT_MATCHED", matched_path
            ), patch.object(confirm_match_budget, "OUTPUT_NO_MATCHED", rejected_path), patch.object(
                confirm_match_budget, "CONFIRM_RESULT", confirm_result
            ), patch.object(confirm_match_budget, "get_logger", return_value=confirm_logger):
                confirm_match_budget.main()
            self.assertIn("【結果】OK", confirm_result.read_text(encoding="utf-8"))
            self.assertEqual([], confirm_logger.errors)


if __name__ == "__main__":
    unittest.main()

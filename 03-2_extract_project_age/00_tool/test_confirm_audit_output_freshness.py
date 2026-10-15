#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""03-2 confirm専用99成果物のfreshnessに限定したfocused test。"""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


TOOL_DIR = Path(__file__).resolve().parent
STEP_DIR = TOOL_DIR.parent
PROJECT_ROOT = STEP_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


extractor = _load_module(
    "extract_project_age_freshness", TOOL_DIR / "extract_project_age.py"
)
confirm = _load_module(
    "confirm_extract_project_age_freshness",
    STEP_DIR / "02_confirm" / "confirm_extract_project_age.py",
)


def _write_jsonl(path, records):
    from common.json_utils import write_jsonl

    write_jsonl(str(path), records)


def _read_jsonl(path):
    with Path(path).open(encoding="utf-8") as file_obj:
        return [json.loads(line) for line in file_obj if line.strip()]


class ConfirmAuditOutputFreshnessTest(unittest.TestCase):
    def test_scheduled_start_clears_only_two_confirm_outputs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            result_dir = Path(temp_dir)
            signal_path = result_dir / extractor.OUTPUT_DEFAULT_SIGNAL
            classified_path = result_dir / extractor.OUTPUT_CLASSIFIED
            other_path = result_dir / extractor.OUTPUT_NULL
            dummy = [{"message_id": "stale"}]
            _write_jsonl(signal_path, dummy)
            _write_jsonl(classified_path, dummy)
            _write_jsonl(other_path, dummy)

            with patch.object(
                extractor,
                "ensure_result_dirs",
                return_value={"result": result_dir},
            ), patch.object(
                extractor, "INPUT_PROJECTS", str(result_dir / "missing_projects.jsonl")
            ), patch.object(
                extractor, "INPUT_CLEANED", str(result_dir / "missing_cleaned.jsonl")
            ), patch.object(extractor, "logger"):
                with self.assertRaises(SystemExit):
                    extractor.main()

            self.assertEqual(_read_jsonl(signal_path), [])
            self.assertEqual(_read_jsonl(classified_path), [])
            self.assertEqual(_read_jsonl(other_path), dummy)

    def test_zero_candidate_confirm_overwrites_stale_outputs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            result_dir = Path(temp_dir)
            for name in (confirm.OUTPUT_DEFAULT_SIGNAL, confirm.OUTPUT_CLASSIFIED):
                _write_jsonl(result_dir / name, [{"message_id": "stale"}])

            with patch.object(confirm, "logger"):
                confirm.check_default_source(
                    [{
                        "message_id": "current",
                        "age_max": 100,
                        "age_max_source": "default",
                        "age_raw": None,
                    }],
                    {"current": {"body_text": "案件概要のみ"}},
                    result_dir,
                )

            self.assertEqual(
                _read_jsonl(result_dir / confirm.OUTPUT_DEFAULT_SIGNAL), []
            )
            self.assertEqual(
                _read_jsonl(result_dir / confirm.OUTPUT_CLASSIFIED), []
            )

    def test_candidate_confirm_still_writes_records(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            result_dir = Path(temp_dir)
            with patch.object(confirm, "logger"):
                confirm.check_default_source(
                    [{
                        "message_id": "candidate",
                        "age_max": 100,
                        "age_max_source": "default",
                        "age_raw": None,
                    }],
                    {"candidate": {"body_text": "年齢: 45歳まで"}},
                    result_dir,
                )

            signal = _read_jsonl(result_dir / confirm.OUTPUT_DEFAULT_SIGNAL)
            classified = _read_jsonl(result_dir / confirm.OUTPUT_CLASSIFIED)
            self.assertEqual(len(signal), 1)
            self.assertEqual(signal[0]["message_id"], "candidate")
            self.assertEqual(len(classified), 1)
            self.assertEqual(classified[0]["message_id"], "candidate")
            self.assertEqual(classified[0]["category"], "genuine_missing")

    def test_latest_project_age_output_is_unchanged(self):
        projects = _read_jsonl(extractor.INPUT_PROJECTS)
        cleaned = {
            record["message_id"]: record
            for record in _read_jsonl(extractor.INPUT_CLEANED)
        }
        previous = {
            record["message_id"]: record
            for record in _read_jsonl(
                STEP_DIR / "01_result" / extractor.OUTPUT_EXTRACTED
            )
        }
        current = {
            record["message_id"]: extractor.build_record(
                record["message_id"],
                (cleaned.get(record["message_id"]) or {}).get("body_text", ""),
            )
            for record in projects
        }

        self.assertEqual(len(projects), 1174)
        self.assertEqual(current, previous)


if __name__ == "__main__":
    unittest.main()

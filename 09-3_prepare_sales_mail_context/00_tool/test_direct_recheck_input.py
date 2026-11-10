"""08-5正本から09-3へ直接接続するfocused test。"""

import json
import hashlib
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import prepare_sales_mail_context as target


class _Logger:
    def info(self, _message):
        pass


class DirectRecheckInputTest(unittest.TestCase):
    def _record(self, project_id, resource_id, status, band="100percent"):
        return {
            "project_info": {"message_id": project_id},
            "resource_info": {"message_id": resource_id},
            "source_score_band": band,
            "recheck_info": {"recheck_status": status},
        }

    def _write_jsonl(self, path, records):
        path.write_text(
            "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records),
            encoding="utf-8",
        )

    def test_confirmed_and_human_review_only_are_loaded(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "all.jsonl"
            self._write_jsonl(
                path,
                [
                    self._record("p1", "r1", "required_skill_confirmed"),
                    self._record("p2", "r2", "required_skill_human_review", "retention_guard"),
                    self._record("p3", "r3", "required_skill_not_confirmed"),
                ],
            )
            with patch.object(target, "RECHECK_ALL_PATH", path):
                records = target.load_pair_records(_Logger())
        self.assertEqual(2, len(records))
        self.assertEqual(["p1", "p2"], [record["project_message_id"] for record in records])
        self.assertEqual("retention_guard", records[1]["score_band"])
        self.assertTrue(all(record["pair_file_name"].startswith("08-5_pair_") for record in records))

    def test_duplicate_pair_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "all.jsonl"
            record = self._record("p1", "r1", "required_skill_confirmed")
            self._write_jsonl(path, [record, record])
            with patch.object(target, "RECHECK_ALL_PATH", path):
                with self.assertRaises(ValueError):
                    target.load_pair_records(_Logger())

    def test_production_commit_hash_and_record_count_are_verified(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            all_path = directory / "all.jsonl"
            record = self._record("p1", "r1", "required_skill_confirmed")
            self._write_jsonl(all_path, [record])
            content = all_path.read_bytes()
            commit_path = directory / "production_commit.json"
            commit_path.write_text(
                json.dumps(
                    {
                        "run_date": "20261109",
                        "artifacts": {
                            "all": {
                                "records": 1,
                                "sha256": hashlib.sha256(content).hexdigest(),
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            with patch.object(target, "RECHECK_ALL_PATH", all_path), patch.object(
                target, "RECHECK_COMMIT_PATH", commit_path
            ):
                self.assertEqual("20261109", target.resolve_recheck_run_date("20261109"))
                commit = json.loads(commit_path.read_text(encoding="utf-8"))
                commit["artifacts"]["all"]["records"] = 2
                commit_path.write_text(json.dumps(commit), encoding="utf-8")
                with self.assertRaises(ValueError):
                    target.resolve_recheck_run_date("20261109")


if __name__ == "__main__":
    unittest.main()

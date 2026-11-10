"""80-7 active 09 retention focused test (local fixture + fake S3 status)."""

import argparse
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import manage_09_result_retention as target  # noqa: E402
from common.logger import get_logger  # noqa: E402

BUCKET = "technoverse"
STATUS_PREFIX = "pipeline_ses_steps/pipeline-status"
CURRENT = "20261110"
PREVIOUS = "20261109"
OLD = "20261106"
ROOT_ZIPS = {
    "pipeline_ses_steps/mail_display_extract_20261106.zip": b"historical-1106",
    "pipeline_ses_steps/mail_display_extract_20261109.zip": b"historical-1109",
}


class _Body:
    def __init__(self, value):
        self.value = value

    def read(self):
        return self.value


class _Paginator:
    def __init__(self, client):
        self.client = client

    def paginate(self, Bucket, Prefix, Delimiter=None):  # noqa: N803
        self.client.list_calls.append((Bucket, Prefix, Delimiter))
        keys = sorted(key for key in self.client.objects if key.startswith(Prefix))
        if Delimiter:
            common_prefixes = sorted(
                {
                    Prefix + key[len(Prefix) :].split(Delimiter, 1)[0] + Delimiter
                    for key in keys
                    if Delimiter in key[len(Prefix) :]
                }
            )
            yield {"CommonPrefixes": [{"Prefix": value} for value in common_prefixes]}
            return
        yield {"Contents": [{"Key": key, "Size": len(self.client.objects[key])} for key in keys]}


class FakeS3Client:
    def __init__(self, objects):
        self.objects = dict(objects)
        self.list_calls = []
        self.delete_calls = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _Paginator(self)

    def get_object(self, Bucket, Key):  # noqa: N803
        return {"Body": _Body(self.objects[Key])}

    def delete_object(self, Bucket, Key):  # noqa: N803
        self.delete_calls.append((Bucket, Key))
        self.objects.pop(Key, None)


def status_key(run_date, run_id):
    return f"{STATUS_PREFIX}/{run_date}/{run_id}/status.json"


def status_document(run_date, run_id, status="SUCCEEDED", exit_code=0):
    return {
        "schema_version": "1.0",
        "run_id": run_id,
        "run_date": run_date,
        "status": status,
        "started_at": "2026-11-09T00:00:00+00:00",
        "finished_at": None if status == "RUNNING" else "2026-11-09T06:00:00+00:00",
        "finished_at_source": "managed_wrapper",
        "exit_code": None if status == "RUNNING" else exit_code,
        "exit_code_source": "managed_wrapper",
        "current_step": "run_full_pipeline",
        "error_message": "" if status != "FAILED" else "failed",
        "log_s3_uri": "s3://technoverse/pipeline-logs/x.log",
        "updated_at": "2026-11-09T06:00:01+00:00",
    }


def status_objects(entries):
    objects = {}
    for run_date, run_id, status, exit_code in entries:
        objects[status_key(run_date, run_id)] = json.dumps(
            status_document(run_date, run_id, status, exit_code)
        ).encode("utf-8")
    return objects


class RetentionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="test_80_7_"))
        self.step_dir = self.tmp / "80-7_manage_09_result_retention"
        (self.step_dir / "01_result").mkdir(parents=True)
        (self.step_dir / "99_execution_time").mkdir(parents=True)
        self.root = self.tmp / "pipeline"
        self.logger = get_logger("test_80_7")
        self.original_build = target.build_s3_client
        self.original_step_dir = target.STEP_DIR
        target.STEP_DIR = self.step_dir
        self._build_fixture()

    def tearDown(self):
        target.build_s3_client = self.original_build
        target.STEP_DIR = self.original_step_dir
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _file(self, step, name, value="x"):
        path = self.root / step / "01_result" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value, encoding="utf-8")
        return path

    def _build_fixture(self):
        for run_date in (OLD, PREVIOUS, CURRENT):
            self._file(
                "09-3_prepare_sales_mail_context",
                f"prepare_sales_mail_context_{run_date}.jsonl",
            )
            self._file(
                "09-4_remove_category_mismatch_sales_candidates",
                f"sales_proposal_candidates_{run_date}.jsonl",
            )
            self._file(
                "09-4_remove_category_mismatch_sales_candidates",
                f"99_excluded_category_mismatch_sales_candidates_{run_date}.jsonl",
            )
            for prefix in ("generate_sales_reply_draft", "proposal_ready", "human_review"):
                self._file(
                    "09-5_generate_sales_reply_draft",
                    f"{prefix}_{run_date}.jsonl",
                )
            self._file(
                "09-5_generate_sales_reply_draft",
                f"reply_preview_{run_date}/index.html",
            )

        self.retired_paths = [
            self._file("09-1_mail_display_format", f"mail_display_format_{OLD}/a.txt", "legacy"),
            self._file(
                "09-2_extract_high_score_mail_display",
                f"mail_display_extract_{OLD}.zip",
                "legacy-zip",
            ),
            self._file(
                "09-3_prepare_sales_proposal_input",
                f"proposal_input_{OLD}.jsonl",
                "legacy-proposal",
            ),
        ]

    def args(self, apply_mode=True):
        return argparse.Namespace(
            dry_run=not apply_mode,
            apply=apply_mode,
            run_date=CURRENT,
            pipeline_root=str(self.root),
            bucket=BUCKET,
            base_prefix="pipeline_ses_steps",
            status_prefix=STATUS_PREFIX,
            region="ap-northeast-1",
        )

    def use_s3(self, objects):
        client = FakeS3Client(objects)
        target.build_s3_client = lambda _region: client
        return client

    def active_dates(self):
        artifacts, _holds = target.scan_artifacts(self.root, CURRENT, self.logger)
        return sorted({artifact["run_date"] for artifact in artifacts})

    def test_current_root_zip_is_not_required(self):
        objects = dict(ROOT_ZIPS)
        objects.update(status_objects([(PREVIOUS, "sfn-prev", "SUCCEEDED", 0)]))
        client = self.use_s3(objects)
        summary = target.run(self.args(), self.logger)
        self.assertEqual(summary["keep_run_dates"], [PREVIOUS, CURRENT])
        self.assertEqual(client.delete_calls, [])

    def test_previous_successful_root_zip_is_not_required(self):
        client = self.use_s3(status_objects([(PREVIOUS, "sfn-prev", "SUCCEEDED", 0)]))
        summary = target.run(self.args(), self.logger)
        self.assertEqual(summary["previous_successful_run_date"], PREVIOUS)
        self.assertEqual(client.delete_calls, [])

    def test_historical_root_zips_are_not_listed_or_deleted(self):
        objects = dict(ROOT_ZIPS)
        objects.update(status_objects([(PREVIOUS, "sfn-prev", "SUCCEEDED", 0)]))
        client = self.use_s3(objects)
        before = dict(client.objects)
        target.run(self.args(), self.logger)
        self.assertEqual(client.objects, before)
        self.assertEqual(client.delete_calls, [])
        self.assertTrue(all(call[1].startswith(STATUS_PREFIX + "/") for call in client.list_calls))

    def test_active_retention_keeps_current_and_previous(self):
        self.use_s3(status_objects([(PREVIOUS, "sfn-prev", "SUCCEEDED", 0)]))
        summary = target.run(self.args(), self.logger)
        self.assertEqual(self.active_dates(), [PREVIOUS, CURRENT])
        self.assertEqual(summary["planned_entry_count"], 7)
        self.assertEqual(summary["planned_delete_files"], 7)
        self.assertEqual(summary["deleted_files"], 7)
        self.assertEqual(sum(row["entry_count"] for row in summary["delete_breakdown"]), 7)

    def test_retired_local_artifacts_are_preserved(self):
        self.use_s3(status_objects([(PREVIOUS, "sfn-prev", "SUCCEEDED", 0)]))
        before = {path: path.read_bytes() for path in self.retired_paths}
        summary = target.run(self.args(), self.logger)
        self.assertEqual({path: path.read_bytes() for path in self.retired_paths}, before)
        self.assertEqual(summary["retired_artifacts_policy"], "excluded_from_scheduled_retention")

    def test_dry_run_changes_nothing(self):
        self.use_s3(status_objects([(PREVIOUS, "sfn-prev", "SUCCEEDED", 0)]))
        before = self.active_dates()
        summary = target.run(self.args(apply_mode=False), self.logger)
        self.assertEqual(self.active_dates(), before)
        self.assertEqual(summary["deleted_files"], 0)

    def test_unknown_active_entry_fails_before_delete(self):
        unknown = self._file("09-3_prepare_sales_mail_context", "unexpected.txt")
        self.use_s3(status_objects([(PREVIOUS, "sfn-prev", "SUCCEEDED", 0)]))
        with self.assertRaises(target.RetentionError):
            target.run(self.args(), self.logger)
        self.assertTrue(unknown.exists())
        unknown.unlink()
        self.assertEqual(self.active_dates(), [OLD, PREVIOUS, CURRENT])

    def test_unknown_retired_entry_is_outside_contract(self):
        unknown = self._file("09-2_extract_high_score_mail_display", "historical-note.txt")
        self.use_s3(status_objects([(PREVIOUS, "sfn-prev", "SUCCEEDED", 0)]))
        target.run(self.args(), self.logger)
        self.assertTrue(unknown.exists())

    def test_failed_status_is_skipped_for_latest_success(self):
        self.use_s3(
            status_objects(
                [
                    (PREVIOUS, "sfn-failed", "FAILED", 1),
                    (OLD, "sfn-success", "SUCCEEDED", 0),
                ]
            )
        )
        summary = target.run(self.args(apply_mode=False), self.logger)
        self.assertEqual(summary["previous_successful_run_date"], OLD)

    def test_no_previous_success_with_old_active_artifacts_fails_closed(self):
        self.use_s3(status_objects([(PREVIOUS, "sfn-failed", "FAILED", 1)]))
        with self.assertRaises(target.RetentionError):
            target.run(self.args(), self.logger)
        self.assertEqual(self.active_dates(), [OLD, PREVIOUS, CURRENT])

    def test_retention_target_contract_is_active_sales_flow_only(self):
        self.assertEqual(
            target.TARGET_STEPS,
            (
                "09-3_prepare_sales_mail_context",
                "09-4_remove_category_mismatch_sales_candidates",
                "09-5_generate_sales_reply_draft",
            ),
        )
        text = Path(target.__file__).read_text(encoding="utf-8")
        self.assertNotIn("execute_root_distribution_zip_retention", text)
        self.assertNotIn("delete_object(", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)

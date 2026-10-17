"""Focused tests for bounded local Pipeline artifact retention."""

import datetime
import json
import os
import tempfile
import time
import unittest
from pathlib import Path

import manage_local_pipeline_retention as retention


class FakeS3:
    def head_object(self, Bucket, Key):
        run_id = Key.rsplit("/", 2)[1]
        return {"ContentLength": {"old": 3, "previous": 4, "current": 5}[run_id]}


class ConstantS3:
    def __init__(self, size):
        self.size = size

    def head_object(self, Bucket, Key):
        return {"ContentLength": self.size}


def _managed_run(root, run_date, run_id, size, status="SUCCEEDED"):
    run = root / run_date / run_id
    run.mkdir(parents=True)
    (run / "pipeline.log").write_bytes(b"x" * size)
    (run / "status.json").write_text(
        json.dumps(
            {
                "run_date": run_date,
                "run_id": run_id,
                "status": status,
                "log_s3_uri": (
                    "s3://technoverse/pipeline_ses_steps/pipeline-logs/"
                    f"{run_date}/{run_id}/pipeline.log"
                ),
            }
        ),
        encoding="utf-8",
    )
    return run


def _daily_run(root, run_date):
    pipeline = root / run_date / "pipeline"
    raw = pipeline / "raw"
    raw.mkdir(parents=True)
    (raw / "mail.eml").write_bytes(b"mail")
    (pipeline / "selection_manifest.jsonl").write_text(
        json.dumps(
            {
                "bucket": "technoverse-ses-mail",
                "key": f"raw/{run_date}/mail.eml",
                "status": "success",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return pipeline


def _invalid_daily_run(root, run_date):
    pipeline = root / run_date / "pipeline"
    pipeline.mkdir(parents=True)
    (pipeline / "selection_manifest.jsonl").write_text(
        json.dumps(
            {
                "bucket": "unknown-source",
                "key": f"raw/{run_date}/mail.eml",
                "status": "success",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return pipeline


def _named_validation(root, run_date, name):
    generation = root / run_date / name
    generation.mkdir(parents=True)
    (generation / "result.jsonl").write_text("{}\n", encoding="utf-8")
    return generation


def _checkpoint(root, run_id):
    run = root / run_id
    run.mkdir(parents=True)
    (run / "checkpoint.jsonl").write_text("{}\n", encoding="utf-8")
    return run


def _pipeline_status(root, run_date, run_id, status):
    run = root / run_date / run_id
    run.mkdir(parents=True)
    (run / "status.json").write_text(
        json.dumps({"run_date": run_date, "run_id": run_id, "status": status}) + "\n",
        encoding="utf-8",
    )


def _legacy_runtime(runtime_root, run_date, run_id, artifacts_committed=True):
    identity_hash = retention._legacy_runtime_name(run_date, run_id)
    context = runtime_root / "_execution_context" / run_date / f"{identity_hash}.json"
    context.parent.mkdir(parents=True, exist_ok=True)
    context.write_text(
        json.dumps(
            {
                "run_date": run_date,
                "run_id": run_id,
                "execution_mode": "legacy",
                "artifacts_committed": artifacts_committed,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    stage = runtime_root / "_legacy_runtime" / identity_hash / "stage"
    stage.mkdir(parents=True)
    (stage / "result.jsonl").write_text("{}\n", encoding="utf-8")
    return context, stage.parent


class LocalRetentionTest(unittest.TestCase):
    def test_dry_run_preserves_files_and_apply_removes_only_old_artifacts(self):
        with tempfile.TemporaryDirectory(prefix="pipeline_retention_") as temp_name:
            temp = Path(temp_name)
            managed = temp / "managed"
            daily = temp / "daily"
            old_run = _managed_run(managed, "20260101", "old", 3)
            _managed_run(managed, "20260102", "previous", 4)
            _managed_run(managed, "20260103", "current", 5)
            old_daily = _daily_run(daily, "2026-01-01")
            _daily_run(daily, "2026-01-02")
            _daily_run(daily, "2026-01-03")
            named_review = daily / "2026-01-01" / "manual-review"
            named_review.mkdir()
            (named_review / "keep.txt").write_text("keep", encoding="utf-8")

            _, managed_dry, _ = retention.cleanup_managed_logs(managed, 2, False, FakeS3())
            _, daily_dry, _ = retention.cleanup_s3_daily(daily, 2, False)
            self.assertEqual(1, len(managed_dry))
            self.assertEqual(1, len(daily_dry))
            self.assertTrue((old_run / "pipeline.log").exists())
            self.assertTrue(old_daily.exists())

            retention.cleanup_managed_logs(managed, 2, True, FakeS3())
            retention.cleanup_s3_daily(daily, 2, True)
            self.assertFalse((old_run / "pipeline.log").exists())
            self.assertTrue((old_run / "status.json").exists())
            self.assertFalse(old_daily.exists())
            self.assertTrue((named_review / "keep.txt").exists())

    def test_unconfirmed_or_nonterminal_managed_log_is_preserved(self):
        with tempfile.TemporaryDirectory(prefix="pipeline_retention_guard_") as temp_name:
            managed = Path(temp_name) / "managed"
            pending = _managed_run(managed, "20260101", "pending", 2, status="RUNNING")
            _managed_run(managed, "20260102", "previous", 3)
            _managed_run(managed, "20260103", "current", 4)
            retention.cleanup_managed_logs(
                managed,
                2,
                True,
                FakeS3(),
                today=datetime.date(2026, 1, 10),
            )
            self.assertTrue((pending / "pipeline.log").exists())

    def test_terminal_confirmed_joins_normal_retention(self):
        with tempfile.TemporaryDirectory(prefix="terminal_confirmed_retention_") as temp_name:
            managed = Path(temp_name) / "managed"
            old = _managed_run(managed, "20260101", "old-confirmed", 2, status="RUNNING")
            for index in range(1, 5):
                _managed_run(managed, f"2026010{index + 1}", f"terminal-{index}", 2)
            _, would_delete, skipped = retention.cleanup_managed_logs(
                managed,
                3,
                False,
                ConstantS3(2),
                {"old-confirmed"},
                today=datetime.date(2026, 1, 10),
            )
            self.assertEqual([], skipped)
            self.assertTrue(any(path == old for path, _, _ in would_delete))

    def test_unknown_managed_log_expires_after_thirty_days(self):
        with tempfile.TemporaryDirectory(prefix="stale_managed_retention_") as temp_name:
            temp = Path(temp_name)
            managed = temp / "managed"
            history = temp / "STATUS.md"
            old = _managed_run(managed, "20260101", "unknown-old", 2, status="RUNNING")
            recent = _managed_run(managed, "20260201", "unknown-recent", 2, status="RUNNING")
            retention.cleanup_managed_logs(
                managed,
                3,
                True,
                ConstantS3(2),
                stale_days=30,
                today=datetime.date(2026, 2, 15),
                history_path=history,
            )
            self.assertFalse((old / "pipeline.log").exists())
            self.assertTrue((recent / "pipeline.log").exists())
            self.assertIn("unknown-old", history.read_text(encoding="utf-8"))

    def test_managed_metadata_is_bounded_after_log_retention(self):
        with tempfile.TemporaryDirectory(prefix="managed_metadata_retention_") as temp_name:
            temp = Path(temp_name)
            managed = temp / "managed"
            history = temp / "STATUS.md"
            terminal = _managed_run(managed, "20260101", "terminal", 2)
            (terminal / "pipeline.log").unlink()
            confirmed = _managed_run(managed, "20260102", "confirmed", 2, status="RUNNING")
            (confirmed / "pipeline.log").unlink()
            active = _managed_run(managed, "20260103", "active", 2, status="RUNNING")
            (active / "pipeline.log").unlink()
            _, would_delete, skipped = retention.cleanup_managed_metadata(
                managed,
                False,
                {"confirmed"},
                {"active"},
                today=datetime.date(2026, 1, 10),
            )
            self.assertEqual(2, len(would_delete))
            self.assertEqual([], skipped)
            retention.cleanup_managed_metadata(
                managed,
                True,
                {"confirmed"},
                {"active"},
                today=datetime.date(2026, 1, 10),
                history_path=history,
            )
            self.assertFalse(terminal.exists())
            self.assertFalse(confirmed.exists())
            self.assertTrue(active.exists())
            self.assertIn("RUN_ID=terminal", history.read_text(encoding="utf-8"))

    def test_four_day_retention_removes_only_fifth_and_older_regenerable_runs(self):
        with tempfile.TemporaryDirectory(prefix="pipeline_retention_four_days_") as temp_name:
            daily = Path(temp_name) / "daily"
            dates = [f"2026-01-{day:02d}" for day in range(1, 6)]
            pipelines = {run_date: _daily_run(daily, run_date) for run_date in dates}
            unknown = _invalid_daily_run(daily, "2025-12-31")

            protected, would_delete, skipped = retention.cleanup_s3_daily(
                daily, 4, False
            )
            self.assertEqual(dates[-4:][::-1], [run_date for run_date, _ in protected])
            self.assertEqual(
                dates[:1][::-1],
                [path.parent.name for path, _, _ in would_delete],
            )
            self.assertEqual("unexpected_source_bucket", skipped[0][2])
            self.assertTrue(all(path.exists() for path in pipelines.values()))
            self.assertTrue(unknown.exists())

            retention.cleanup_s3_daily(daily, 4, True)
            self.assertFalse(pipelines[dates[0]].exists())
            self.assertTrue(all(pipelines[run_date].exists() for run_date in dates[-4:]))
            self.assertTrue(unknown.exists())

    def test_nonterminal_daily_expires_after_thirty_days_but_current_is_preserved(self):
        with tempfile.TemporaryDirectory(prefix="nonterminal_daily_retention_") as temp_name:
            temp = Path(temp_name)
            daily = temp / "daily"
            history = temp / "STATUS.md"
            old = _invalid_daily_run(daily, "2026-01-01")
            recent = _invalid_daily_run(daily, "2026-02-01")
            current = _invalid_daily_run(daily, "2025-12-01")
            retention.cleanup_nonterminal_s3_daily(
                daily,
                30,
                True,
                {"20251201"},
                today=datetime.date(2026, 2, 15),
                history_path=history,
            )
            self.assertFalse(old.exists())
            self.assertTrue(recent.exists())
            self.assertTrue(current.exists())

    def test_named_validation_keeps_latest_four_of_six_without_manifest_checks(self):
        with tempfile.TemporaryDirectory(prefix="named_validation_retention_") as temp_name:
            root = Path(temp_name)
            generations = [
                _named_validation(root, f"2026-01-{day:02d}", f"review-{day:02d}")
                for day in range(1, 7)
            ]
            protected, would_delete, skipped = retention.cleanup_named_validations(
                root, 4, False
            )
            self.assertEqual(4, len(protected))
            self.assertEqual(2, len(would_delete))
            self.assertEqual([], skipped)
            self.assertTrue(all(path.exists() for path in generations))

            retention.cleanup_named_validations(root, 4, True)
            self.assertFalse(generations[0].exists())
            self.assertFalse(generations[1].exists())
            self.assertTrue(all(path.exists() for path in generations[-4:]))

    def test_checkpoints_keep_latest_three_of_six(self):
        with tempfile.TemporaryDirectory(prefix="checkpoint_retention_") as temp_name:
            root = Path(temp_name)
            runs = [_checkpoint(root, f"run-{index:02d}") for index in range(6)]
            protected, would_delete, skipped = retention.cleanup_checkpoints(
                root, 3, False
            )
            self.assertEqual(3, len(protected))
            self.assertEqual(3, len(would_delete))
            self.assertEqual([], skipped)

            retention.cleanup_checkpoints(root, 3, True)
            self.assertTrue(all(not path.exists() for path in runs[:3]))
            self.assertTrue(all(path.exists() for path in runs[-3:]))

    def test_active_checkpoint_outside_latest_three_is_preserved(self):
        with tempfile.TemporaryDirectory(prefix="active_checkpoint_retention_") as temp_name:
            root = Path(temp_name)
            runs = [_checkpoint(root, f"run-{index:02d}") for index in range(6)]
            retention.cleanup_checkpoints(root, 3, True, {"run-00"})
            self.assertTrue(runs[0].exists())
            self.assertTrue(all(path.exists() for path in runs[-3:]))

    def test_llm_usage_keeps_latest_thirty_of_thirty_five_runs(self):
        with tempfile.TemporaryDirectory(prefix="usage_retention_") as temp_name:
            root = Path(temp_name)
            usage_files = []
            for index in range(35):
                path = root / f"llm_usage_run-{index:02d}.jsonl"
                path.write_text("{}\n", encoding="utf-8")
                path.with_suffix(".counter").write_text("1\n", encoding="utf-8")
                usage_files.append(path)

            protected, would_delete, skipped = retention.cleanup_usage_logs(
                root, 30, False
            )
            self.assertEqual(30, len(protected))
            self.assertEqual(5, len(would_delete))
            self.assertEqual([], skipped)

            retention.cleanup_usage_logs(root, 30, True)
            self.assertTrue(all(not path.exists() for path in usage_files[:5]))
            self.assertTrue(all(path.exists() for path in usage_files[-30:]))
            self.assertTrue(
                all(not path.with_suffix(".counter").exists() for path in usage_files[:5])
            )

    def test_runtime_keeps_latest_three_terminal_and_preserves_running(self):
        with tempfile.TemporaryDirectory(prefix="runtime_retention_") as temp_name:
            temp = Path(temp_name)
            managed = temp / "managed"
            runtime = temp / "runtime"
            terminal_artifacts = []
            for index in range(6):
                run_date = f"202601{index + 1:02d}"
                run_id = f"terminal-{index:02d}"
                _pipeline_status(managed, run_date, run_id, "SUCCEEDED")
                terminal_artifacts.append(_legacy_runtime(runtime, run_date, run_id))
            _pipeline_status(managed, "20260107", "running-00", "RUNNING")
            running_artifacts = _legacy_runtime(runtime, "20260107", "running-00")

            protected, would_delete, skipped = retention.cleanup_runtime_context(
                runtime,
                managed,
                3,
                False,
                today=datetime.date(2026, 1, 10),
            )
            self.assertEqual(3, len(protected))
            self.assertEqual(3, len(would_delete))
            self.assertEqual(1, len(skipped))
            self.assertIn("nonterminal_or_unknown_status:RUNNING", skipped[0][2])

            retention.cleanup_runtime_context(
                runtime,
                managed,
                3,
                True,
                today=datetime.date(2026, 1, 10),
            )
            for context, run_dir in terminal_artifacts[:3]:
                self.assertFalse(context.exists())
                self.assertFalse(run_dir.exists())
            for context, run_dir in terminal_artifacts[-3:]:
                self.assertTrue(context.exists())
                self.assertTrue(run_dir.exists())
            self.assertTrue(running_artifacts[0].exists())
            self.assertTrue(running_artifacts[1].exists())

    def test_stale_runtime_expires_but_recent_and_active_are_preserved(self):
        with tempfile.TemporaryDirectory(prefix="stale_runtime_retention_") as temp_name:
            temp = Path(temp_name)
            managed = temp / "managed"
            runtime = temp / "runtime"
            history = temp / "STATUS.md"
            old = _legacy_runtime(runtime, "20260101", "unknown-old", False)
            recent = _legacy_runtime(runtime, "20260201", "unknown-recent", False)
            active = _legacy_runtime(runtime, "20260101", "active-old", False)
            retention.cleanup_runtime_context(
                runtime,
                managed,
                3,
                True,
                {"active-old"},
                stale_days=30,
                today=datetime.date(2026, 2, 15),
                history_path=history,
            )
            self.assertFalse(old[0].exists())
            self.assertFalse(old[1].exists())
            self.assertTrue(recent[0].exists())
            self.assertTrue(active[0].exists())

    def test_committed_runtime_metadata_is_terminal_without_managed_status(self):
        with tempfile.TemporaryDirectory(prefix="committed_runtime_retention_") as temp_name:
            temp = Path(temp_name)
            managed = temp / "managed"
            runtime = temp / "runtime"
            artifacts = [
                _legacy_runtime(runtime, f"2026010{index + 1}", f"committed-{index}")
                for index in range(4)
            ]
            protected, would_delete, skipped = retention.cleanup_runtime_context(
                runtime,
                managed,
                3,
                False,
                today=datetime.date(2026, 1, 10),
            )
            self.assertEqual(3, len(protected))
            self.assertEqual(1, len(would_delete))
            self.assertEqual([], skipped)
            self.assertEqual(artifacts[0][0], would_delete[0][0])

    def test_stale_generation_expiry_preserves_recent_and_active(self):
        with tempfile.TemporaryDirectory(prefix="stale_generation_retention_") as temp_name:
            temp = Path(temp_name)
            root = temp / "histories"
            old = _named_validation(root, "", "old")
            recent = _named_validation(root, "", "recent")
            active = _named_validation(root, "", "active-run")
            old_time = time.time() - 31 * 86400
            os.utime(old / "result.jsonl", (old_time, old_time))
            os.utime(old, (old_time, old_time))
            retention.cleanup_stale_generations(
                (root,),
                30,
                True,
                {"active-run"},
                now=time.time(),
                history_path=temp / "STATUS.md",
            )
            self.assertFalse(old.exists())
            self.assertTrue(recent.exists())
            self.assertTrue(active.exists())

    def test_script_log_rotation_threshold_and_three_generations(self):
        with tempfile.TemporaryDirectory(prefix="script_log_rotation_") as temp_name:
            temp = Path(temp_name)
            small = temp / "small.log"
            small.write_bytes(b"12345")
            protected, rotated, skipped = retention.rotate_script_log(
                small, 10, 3, True, active_grace_seconds=0
            )
            self.assertEqual(1, len(protected))
            self.assertEqual([], rotated)
            self.assertEqual([], skipped)

            log = temp / "pipeline_script_exec.log"
            log.write_bytes(b"x" * 20)
            for index in range(1, 5):
                Path(f"{log}.{index}.gz").write_bytes(f"old-{index}".encode())
            protected, rotated, skipped = retention.rotate_script_log(
                log, 10, 3, True, active_grace_seconds=0
            )
            self.assertEqual([], protected)
            self.assertEqual([], skipped)
            self.assertEqual(0, log.stat().st_size)
            self.assertTrue(Path(f"{log}.1.gz").exists())
            self.assertTrue(Path(f"{log}.2.gz").exists())
            self.assertTrue(Path(f"{log}.3.gz").exists())
            self.assertFalse(Path(f"{log}.4.gz").exists())
            self.assertTrue(any(reason == "rotation_generation_limit" for _, _, reason in rotated))


if __name__ == "__main__":
    unittest.main()

"""
80-7_manage_09_result_retention focused test

本番成果物には触れず、一時ディレクトリ上のfixtureと fake S3 client で検証する。
full Pipeline実行・AWS実アクセスは行わない。

実行:
  python3 80-7_manage_09_result_retention/00_tool/test_manage_09_result_retention.py
"""

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

BUCKET = "test-bucket"
STATUS_PREFIX = "pipeline_ses_steps/pipeline-status"

# 13 RUN_DATE（金曜→月曜・祝日跨ぎを含む）
ALL_RUN_DATES = [
    "20260418",
    "20260419",
    "20260422",
    "20260424",
    "20260427",
    "20260501",
    "20260531",
    "20260806",
    "20260809",
    "20260810",
    "20260812",
    "20260813",
    "20260814",
]


class FakeS3Client:
    """list_objects_v2 / get_object だけを提供する最小のfake。"""

    def __init__(self, objects, fail_get_keys=(), fail_list=False):
        self.objects = dict(objects)
        self.fail_get_keys = set(fail_get_keys)
        self.fail_list = fail_list

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _FakePaginator(self)

    def get_object(self, Bucket, Key):  # noqa: N803 - boto3互換シグネチャ
        if Key in self.fail_get_keys:
            raise RuntimeError(f"simulated GET failure: {Key}")
        return {"Body": _FakeBody(self.objects[Key])}


class _FakeBody:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


class _FakePaginator:
    def __init__(self, client):
        self.client = client

    def paginate(self, Bucket, Prefix, Delimiter=None):  # noqa: N803
        if self.client.fail_list:
            raise RuntimeError("simulated LIST failure")
        keys = sorted(k for k in self.client.objects if k.startswith(Prefix))
        if Delimiter:
            prefixes = set()
            for key in keys:
                rest = key[len(Prefix) :]
                if Delimiter in rest:
                    prefixes.add(Prefix + rest.split(Delimiter, 1)[0] + Delimiter)
            yield {"CommonPrefixes": [{"Prefix": p} for p in sorted(prefixes)]}
            return
        # 2件ずつ返してpaginationを再現する
        for i in range(0, len(keys), 2) or [0]:
            chunk = keys[i : i + 2]
            yield {"Contents": [{"Key": k, "Size": len(self.client.objects[k])} for k in chunk]}
        if not keys:
            yield {"Contents": []}


def status_document(run_date, run_id, status="SUCCEEDED", exit_code=0, **overrides):
    document = {
        "schema_version": "1.0",
        "run_id": run_id,
        "run_date": run_date,
        "status": status,
        "started_at": f"2026-{run_date[4:6]}-{run_date[6:8]}T09:00:00Z",
        "finished_at": None if status == "RUNNING" else f"2026-{run_date[4:6]}-{run_date[6:8]}T12:00:00Z",
        "finished_at_source": "managed_wrapper",
        "exit_code": None if status == "RUNNING" else exit_code,
        "exit_code_source": "managed_wrapper",
        "current_step": "run_suggest_and_cleanup",
        "error_message": "" if status != "FAILED" else "boom",
        "log_s3_uri": "s3://x/y",
        "updated_at": f"2026-{run_date[4:6]}-{run_date[6:8]}T12:00:01Z",
    }
    document.update(overrides)
    return document


def status_key(run_date, run_id):
    return f"{STATUS_PREFIX}/{run_date}/{run_id}/status.json"


def status_objects(entries):
    """entries: [(run_date, run_id, status, exit_code, overrides)]"""
    objects = {}
    for run_date, run_id, status, exit_code, overrides in entries:
        document = status_document(run_date, run_id, status, exit_code, **overrides)
        objects[status_key(run_date, run_id)] = json.dumps(document).encode("utf-8")
    return objects


def raw_status_objects(entries):
    """entries: [(run_date, run_id, raw_bytes)]"""
    return {status_key(rd, rid): raw for rd, rid, raw in entries}


class RetentionTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="test_80_7_"))
        self.step_dir = self.tmp / "80-7_manage_09_result_retention"
        (self.step_dir / "01_result").mkdir(parents=True)
        (self.step_dir / "99_execution_time").mkdir(parents=True)
        self.logger = get_logger("test_80-7")
        self.original_build = target.build_s3_client
        self.original_step_dir = target.STEP_DIR
        target.STEP_DIR = self.step_dir

    def tearDown(self):
        target.build_s3_client = self.original_build
        target.STEP_DIR = self.original_step_dir
        shutil.rmtree(self.tmp, ignore_errors=True)

    def build_fixture(self, run_dates=ALL_RUN_DATES):
        root = self.tmp / "pipeline"
        for step in target.TARGET_STEPS:
            (root / step / "01_result").mkdir(parents=True)

        for run_date in run_dates:
            self._make_dir(root, "09-1_mail_display_format", f"mail_display_format_{run_date}")
            self._make_dir(
                root, "09-2_extract_high_score_mail_display", f"mail_display_extract_{run_date}"
            )
            self._make_file(
                root,
                "09-2_extract_high_score_mail_display",
                f"mail_display_extract_{run_date}.zip",
            )
            self._make_file(
                root, "09-3_prepare_sales_proposal_input", f"proposal_input_{run_date}.jsonl"
            )
            self._make_file(
                root,
                "09-3_prepare_sales_mail_context",
                f"prepare_sales_mail_context_{run_date}.jsonl",
            )
            self._make_file(
                root,
                "09-4_remove_category_mismatch_sales_candidates",
                f"sales_proposal_candidates_{run_date}.jsonl",
            )
            self._make_file(
                root,
                "09-4_remove_category_mismatch_sales_candidates",
                f"99_excluded_category_mismatch_sales_candidates_{run_date}.jsonl",
            )
            self._make_file(
                root,
                "09-5_generate_sales_reply_draft",
                f"generate_sales_reply_draft_{run_date}.jsonl",
            )
            self._make_dir(root, "09-5_generate_sales_reply_draft", f"reply_preview_{run_date}")

        # HOLD対象の運用ログ
        self._make_file(
            root, "09-2_extract_high_score_mail_display", "error_20260418_145453.log"
        )
        return root

    def _make_dir(self, root, step, name):
        path = root / step / "01_result" / name
        path.mkdir(parents=True)
        (path / "a.txt").write_text("aaa", encoding="utf-8")
        (path / "sub").mkdir()
        (path / "sub" / "b.txt").write_text("bb", encoding="utf-8")

    def _make_file(self, root, step, name):
        (root / step / "01_result" / name).write_text("x" * 10, encoding="utf-8")

    def make_args(self, root, run_date, apply_mode=False):
        return argparse.Namespace(
            dry_run=not apply_mode,
            apply=apply_mode,
            run_date=run_date,
            pipeline_root=str(root),
            bucket=BUCKET,
            status_prefix=STATUS_PREFIX,
            region="ap-northeast-1",
        )

    def set_s3(self, objects, **kwargs):
        client = FakeS3Client(objects, **kwargs)
        target.build_s3_client = lambda region: client
        return client

    def present_run_dates(self, root, current_run_date):
        artifacts, _holds = target.scan_artifacts(root, current_run_date, self.logger)
        return sorted({a["run_date"] for a in artifacts})


class TestPreviousSuccessfulRunDate(RetentionTestBase):
    def test_13_run_dates_keep_current_and_previous_success(self):
        root = self.build_fixture()
        self.set_s3(
            status_objects(
                [(rd, f"sfn-{rd}", "SUCCEEDED", 0, {}) for rd in ALL_RUN_DATES]
            )
        )
        summary = target.run(self.make_args(root, "20260817", apply_mode=True), self.logger)
        self.assertEqual(summary["previous_successful_run_date"], "20260814")
        self.assertEqual(summary["keep_run_dates"], ["20260814", "20260817"])
        self.assertEqual(self.present_run_dates(root, "20260817"), ["20260814"])

    def test_friday_to_monday_and_holiday_gap(self):
        """金曜(0814)成功のみ。土日(0815/0816)にrunが無くても直前正常RUN_DATEを選べる。"""
        root = self.build_fixture()
        self.set_s3(status_objects([("20260814", "sfn-a", "SUCCEEDED", 0, {})]))
        summary = target.run(self.make_args(root, "20260817"), self.logger)
        self.assertEqual(summary["previous_successful_run_date"], "20260814")
        self.assertNotIn("20260815", summary["keep_run_dates"])

    def test_failed_and_succeeded_mixed_with_multiple_run_ids(self):
        """0814はFAILEDのみ。0813は同日2 RUN_IDでうち1件が正常 → 0813を選ぶ。"""
        root = self.build_fixture()
        self.set_s3(
            status_objects(
                [
                    ("20260814", "sfn-fail", "FAILED", 3, {}),
                    ("20260813", "sfn-fail2", "FAILED", 1, {}),
                    ("20260813", "sfn-ok", "SUCCEEDED", 0, {}),
                    ("20260812", "sfn-ok2", "SUCCEEDED", 0, {}),
                ]
            )
        )
        summary = target.run(self.make_args(root, "20260817", apply_mode=True), self.logger)
        self.assertEqual(summary["previous_successful_run_date"], "20260813")
        self.assertEqual(self.present_run_dates(root, "20260817"), ["20260813"])

    def test_running_status_is_not_treated_as_success(self):
        root = self.build_fixture()
        self.set_s3(
            status_objects(
                [
                    ("20260814", "sfn-running", "RUNNING", 0, {}),
                    ("20260813", "sfn-ok", "SUCCEEDED", 0, {}),
                ]
            )
        )
        summary = target.run(self.make_args(root, "20260817"), self.logger)
        self.assertEqual(summary["previous_successful_run_date"], "20260813")


class TestStatusFailureDoesNotDelete(RetentionTestBase):
    def assert_no_delete_and_fail(self, root, args):
        before = self.present_run_dates(root, args.run_date)
        with self.assertRaises(target.RetentionError):
            target.run(args, self.logger)
        self.assertEqual(self.present_run_dates(root, args.run_date), before)

    def test_list_failure_fails_without_deleting(self):
        root = self.build_fixture()
        self.set_s3(status_objects([("20260814", "a", "SUCCEEDED", 0, {})]), fail_list=True)
        self.assert_no_delete_and_fail(root, self.make_args(root, "20260817", apply_mode=True))

    def test_get_failure_fails_without_deleting(self):
        root = self.build_fixture()
        objects = status_objects([("20260814", "a", "SUCCEEDED", 0, {})])
        self.set_s3(objects, fail_get_keys=[status_key("20260814", "a")])
        self.assert_no_delete_and_fail(root, self.make_args(root, "20260817", apply_mode=True))

    def test_broken_json_fails_without_deleting(self):
        root = self.build_fixture()
        self.set_s3(raw_status_objects([("20260814", "a", b"{not json")]))
        self.assert_no_delete_and_fail(root, self.make_args(root, "20260817", apply_mode=True))

    def test_invalid_schema_fails_without_deleting(self):
        root = self.build_fixture()
        broken = status_document("20260814", "a")
        del broken["exit_code"]
        self.set_s3({status_key("20260814", "a"): json.dumps(broken).encode("utf-8")})
        self.assert_no_delete_and_fail(root, self.make_args(root, "20260817", apply_mode=True))

    def test_succeeded_with_non_zero_exit_code_is_schema_error(self):
        root = self.build_fixture()
        self.set_s3(status_objects([("20260814", "a", "SUCCEEDED", 7, {})]))
        self.assert_no_delete_and_fail(root, self.make_args(root, "20260817", apply_mode=True))

    def test_invalid_finished_at_fails_without_deleting(self):
        root = self.build_fixture()
        self.set_s3(
            status_objects([("20260814", "a", "SUCCEEDED", 0, {"finished_at": "2026-08-14 12:00"})])
        )
        self.assert_no_delete_and_fail(root, self.make_args(root, "20260817", apply_mode=True))

    def test_no_previous_success_with_old_artifacts_fails(self):
        root = self.build_fixture()
        self.set_s3(status_objects([("20260814", "a", "FAILED", 2, {})]))
        self.assert_no_delete_and_fail(root, self.make_args(root, "20260817", apply_mode=True))

    def test_no_previous_success_without_old_artifacts_succeeds_with_zero_delete(self):
        root = self.build_fixture(run_dates=["20260817"])
        self.set_s3(status_objects([("20260814", "a", "FAILED", 2, {})]))
        summary = target.run(self.make_args(root, "20260817", apply_mode=True), self.logger)
        self.assertIsNone(summary["previous_successful_run_date"])
        self.assertEqual(summary["deleted_files"], 0)


class TestScanValidation(RetentionTestBase):
    def setUp(self):
        super().setUp()
        self.root = self.build_fixture()
        self.set_s3(status_objects([("20260814", "a", "SUCCEEDED", 0, {})]))

    def test_unknown_entry_fails_before_delete(self):
        before = self.present_run_dates(self.root, "20260817")
        unknown = self.root / "09-1_mail_display_format" / "01_result" / "mystery.txt"
        unknown.write_text("x")
        with self.assertRaises(target.RetentionError):
            target.run(self.make_args(self.root, "20260817", apply_mode=True), self.logger)
        unknown.unlink()
        self.assertEqual(self.present_run_dates(self.root, "20260817"), before)

    def test_invalid_date_fails_before_delete(self):
        (
            self.root / "09-1_mail_display_format" / "01_result" / "mail_display_format_20261345"
        ).mkdir()
        with self.assertRaises(target.RetentionError):
            target.run(self.make_args(self.root, "20260817", apply_mode=True), self.logger)

    def test_symlink_fails_before_delete(self):
        link = self.root / "09-1_mail_display_format" / "01_result" / "mail_display_format_20260701"
        link.symlink_to(self.root / "09-1_mail_display_format" / "01_result" / "mail_display_format_20260418")
        with self.assertRaises(target.RetentionError):
            target.run(self.make_args(self.root, "20260817", apply_mode=True), self.logger)

    def test_symlink_inside_delete_candidate_fails(self):
        inner = (
            self.root
            / "09-1_mail_display_format"
            / "01_result"
            / "mail_display_format_20260418"
            / "link.txt"
        )
        inner.symlink_to(self.root / "09-1_mail_display_format" / "01_result")
        with self.assertRaises(target.RetentionError):
            target.run(self.make_args(self.root, "20260817", apply_mode=True), self.logger)

    def test_future_run_date_artifact_fails(self):
        (
            self.root / "09-1_mail_display_format" / "01_result" / "mail_display_format_20261231"
        ).mkdir()
        with self.assertRaises(target.RetentionError):
            target.run(self.make_args(self.root, "20260817", apply_mode=True), self.logger)

    def test_hold_log_is_kept(self):
        target.run(self.make_args(self.root, "20260817", apply_mode=True), self.logger)
        hold = (
            self.root
            / "09-2_extract_high_score_mail_display"
            / "01_result"
            / "error_20260418_145453.log"
        )
        self.assertTrue(hold.is_file())


class TestDryRunAndIdempotency(RetentionTestBase):
    def test_dry_run_deletes_nothing_then_apply_then_rerun_is_zero(self):
        root = self.build_fixture()
        self.set_s3(status_objects([(rd, f"sfn-{rd}", "SUCCEEDED", 0, {}) for rd in ALL_RUN_DATES]))
        before = self.present_run_dates(root, "20260817")

        dry = target.run(self.make_args(root, "20260817"), self.logger)
        self.assertEqual(dry["deleted_files"], 0)
        self.assertEqual(dry["deleted_bytes"], 0)
        self.assertGreater(dry["planned_delete_files"], 0)
        self.assertEqual(self.present_run_dates(root, "20260817"), before)

        applied = target.run(self.make_args(root, "20260817", apply_mode=True), self.logger)
        self.assertEqual(applied["deleted_files"], dry["planned_delete_files"])
        self.assertEqual(applied["deleted_bytes"], dry["planned_delete_bytes"])

        again = target.run(self.make_args(root, "20260817", apply_mode=True), self.logger)
        self.assertEqual(again["planned_delete_files"], 0)
        self.assertEqual(again["deleted_files"], 0)
        self.assertEqual(self.present_run_dates(root, "20260817"), ["20260814"])

    def test_deleted_directories_are_removed(self):
        root = self.build_fixture()
        self.set_s3(status_objects([(rd, f"sfn-{rd}", "SUCCEEDED", 0, {}) for rd in ALL_RUN_DATES]))
        target.run(self.make_args(root, "20260817", apply_mode=True), self.logger)
        removed = root / "09-1_mail_display_format" / "01_result" / "mail_display_format_20260418"
        kept = root / "09-1_mail_display_format" / "01_result" / "mail_display_format_20260814"
        self.assertFalse(removed.exists())
        self.assertTrue(kept.is_dir())

    def test_summary_is_fixed_name_and_not_generational(self):
        root = self.build_fixture()
        self.set_s3(status_objects([(rd, f"sfn-{rd}", "SUCCEEDED", 0, {}) for rd in ALL_RUN_DATES]))
        args = self.make_args(root, "20260817")
        sys_argv = sys.argv
        sys.argv = [
            "manage_09_result_retention.py",
            "--dry-run",
            "--run-date",
            "20260817",
            "--pipeline-root",
            str(root),
            "--bucket",
            BUCKET,
            "--status-prefix",
            STATUS_PREFIX,
            "--region",
            "ap-northeast-1",
        ]
        try:
            self.assertEqual(target.main(), 0)
            self.assertEqual(target.main(), 0)
        finally:
            sys.argv = sys_argv
        outputs = sorted(p.name for p in (self.step_dir / "01_result").iterdir())
        self.assertEqual(outputs, [target.SUMMARY_FILENAME])
        self.assertTrue(args.dry_run)


if __name__ == "__main__":
    unittest.main(verbosity=2)

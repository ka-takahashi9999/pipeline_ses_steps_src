"""
80-9_portal_s3_sync focused test

AWS CLI実行とS3 LISTはすべてmockし、production Portal prefixへは一切書き込まない。
full Pipeline実行は行わない。

実行:
  python3 80-9_portal_s3_sync/00_tool/test_portal_s3_sync.py
"""

import argparse
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import portal_s3_sync as target  # noqa: E402
from common.json_utils import write_jsonl  # noqa: E402
from common.logger import get_logger  # noqa: E402

BUCKET = "technoverse"
PORTAL_PREFIX = "pipeline_ses_steps/pipeline_ses_steps"


class FakeS3Client:
    def __init__(self, objects, fail_list=False, truncated_without_token=False):
        self.objects = dict(objects)
        self.fail_list = fail_list
        self.truncated_without_token = truncated_without_token

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _FakePaginator(self)


class _FakePaginator:
    def __init__(self, client):
        self.client = client

    def paginate(self, Bucket, Prefix):  # noqa: N803 - boto3互換シグネチャ
        if self.client.fail_list:
            raise RuntimeError("simulated LIST failure")
        keys = sorted(k for k in self.client.objects if k.startswith(Prefix))
        if self.client.truncated_without_token:
            yield {"Contents": [], "IsTruncated": True}
            return
        # 2件ずつ返して全ページLISTを再現する
        for i in range(0, max(len(keys), 1), 2):
            chunk = keys[i : i + 2]
            yield {"Contents": [{"Key": k, "Size": self.client.objects[k]} for k in chunk]}


class SyncTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="test_80_9_"))
        self.root = self.tmp / "pipeline"
        self.prepare_dir = self.tmp / "prepare"
        self.step_dir = self.tmp / "out"
        self.logger = get_logger("test_80-9")
        self.sleep_calls = []
        self.sync_calls = []

        self.expected = {
            "01-1_fetch_gmail/01_result/fetch_gmail.jsonl": 10,
            "06-80_duplicate_proposal_check/01_result/dup.jsonl": 20,
            "09-1_mail_display_format/01_result/mail_display_format_20260814/a.txt": 5,
        }
        self.step_dirs = [
            "01-1_fetch_gmail",
            "06-80_duplicate_proposal_check",
            "09-1_mail_display_format",
        ]
        self._write_prepare_outputs()
        self.root.mkdir(parents=True, exist_ok=True)

        self.original_sleep = target.time.sleep
        target.time.sleep = self._fake_sleep
        self.original_run_sync = target.run_sync
        self.original_build = target.build_s3_client
        os.environ["PORTAL_S3_VERIFY_WAIT_SEC"] = "0"

    def tearDown(self):
        target.time.sleep = self.original_sleep
        target.run_sync = self.original_run_sync
        target.build_s3_client = self.original_build
        os.environ.pop("PORTAL_S3_VERIFY_WAIT_SEC", None)
        os.environ.pop("PORTAL_S3_PREFIX", None)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _fake_sleep(self, seconds):
        self.sleep_calls.append(seconds)

    def _write_prepare_outputs(self):
        result_dir = self.prepare_dir / "01_result"
        result_dir.mkdir(parents=True, exist_ok=True)
        write_jsonl(
            str(result_dir / target.MANIFEST_FILENAME),
            [{"relative_path": p, "size": s} for p, s in sorted(self.expected.items())],
        )
        with open(result_dir / target.PREPARE_SUMMARY_FILENAME, "w", encoding="utf-8") as f:
            json.dump({"selected_step_dirs": self.step_dirs}, f)

    def make_args(self, dry_run=False):
        return argparse.Namespace(
            dry_run=dry_run,
            pipeline_root=str(self.root),
            step_dir=str(self.step_dir),
            prepare_dir=str(self.prepare_dir),
        )

    def stub_sync(self, fail=False):
        def _run_sync(argv, logger):
            self.sync_calls.append(argv)
            if fail:
                raise target.SyncError("aws s3 sync が失敗しました (exit=1)")

        target.run_sync = _run_sync

    def stub_s3(self, actual_sizes, **kwargs):
        objects = {f"{PORTAL_PREFIX}/{path}": size for path, size in actual_sizes.items()}
        client = FakeS3Client(objects, **kwargs)
        target.build_s3_client = lambda region: client
        return client


class TestSyncArgv(SyncTestBase):
    def test_argv_shape_and_required_flags(self):
        argv = target.build_sync_argv(
            self.root, BUCKET, PORTAL_PREFIX, "ap-northeast-1", self.step_dirs, dry_run=False
        )
        self.assertIsInstance(argv, list)
        self.assertEqual(argv[:3], [target.AWS_BIN, "s3", "sync"])
        self.assertEqual(argv[3], str(self.root))
        self.assertEqual(argv[4], f"s3://{BUCKET}/{PORTAL_PREFIX}/")
        self.assertIn("--delete", argv)
        self.assertIn("--no-follow-symlinks", argv)
        self.assertIn("--region", argv)
        self.assertNotIn("--dryrun", argv)
        # shell文字列を組み立てていないこと
        for item in argv:
            self.assertNotIn("&&", item)
            self.assertNotIn("|", item)

    def test_filters_are_positive_selection_then_explicit_excludes(self):
        argv = target.build_sync_argv(
            self.root, BUCKET, PORTAL_PREFIX, "ap-northeast-1", self.step_dirs, dry_run=False
        )
        filters = [
            (argv[i], argv[i + 1])
            for i in range(len(argv) - 1)
            if argv[i] in ("--include", "--exclude")
        ]
        self.assertEqual(filters[0], ("--exclude", "*"))
        includes = [value for flag, value in filters if flag == "--include"]
        self.assertEqual(includes, [f"{s}/01_result/*" for s in self.step_dirs])

        excludes = [value for flag, value in filters if flag == "--exclude"]
        for pattern in target.EXCLUDE_FILTERS:
            self.assertIn(pattern, excludes)
        # 除外は include より後ろ（後勝ちのため）
        first_exclude_after = min(
            i for i, (flag, value) in enumerate(filters) if flag == "--exclude" and value != "*"
        )
        last_include = max(i for i, (flag, _v) in enumerate(filters) if flag == "--include")
        self.assertGreater(first_exclude_after, last_include)

    def test_self_steps_and_master_file_are_excluded(self):
        argv = target.build_sync_argv(
            self.root, BUCKET, PORTAL_PREFIX, "ap-northeast-1", self.step_dirs, dry_run=False
        )
        joined = " ".join(argv)
        self.assertIn("01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl", joined)
        self.assertIn("80-7_manage_09_result_retention/*", joined)
        self.assertIn("80-8_portal_s3_prepare/*", joined)
        self.assertIn("80-9_portal_s3_sync/*", joined)
        self.assertIn("*.bak_*", joined)
        self.assertIn("*/01_result/.gitkeep", joined)

    def test_dry_run_adds_dryrun_flag(self):
        argv = target.build_sync_argv(
            self.root, BUCKET, PORTAL_PREFIX, "ap-northeast-1", self.step_dirs, dry_run=True
        )
        self.assertIn("--dryrun", argv)

    def test_destination_stays_inside_portal_prefix(self):
        argv = target.build_sync_argv(
            self.root, BUCKET, PORTAL_PREFIX, "ap-northeast-1", self.step_dirs, dry_run=False
        )
        self.assertTrue(argv[4].startswith("s3://technoverse/pipeline_ses_steps/pipeline_ses_steps/"))
        for reserved in ("pipeline-logs", "pipeline-status"):
            self.assertNotIn(reserved, argv[4])


class TestWaitValidation(SyncTestBase):
    def test_valid_wait_values(self):
        for value in ("0", "30", "60", 0, 30, 60, " 45 "):
            self.assertEqual(target.parse_wait_seconds(value), int(str(value).strip()))

    def test_invalid_wait_values_fail(self):
        for value in ("-1", "1.5", "abc", "", "30s", None):
            with self.assertRaises(target.SyncError, msg=repr(value)):
                target.parse_wait_seconds(value)

    def test_wait_is_performed_before_verify(self):
        os.environ["PORTAL_S3_VERIFY_WAIT_SEC"] = "60"
        self.stub_sync()
        self.stub_s3(self.expected)
        summary = target.run(self.make_args(), self.logger)
        self.assertEqual(self.sleep_calls, [60])
        self.assertTrue(summary["wait_performed"])
        self.assertEqual(summary["verify_wait_sec"], 60)

    def test_invalid_wait_config_fails_before_sync(self):
        os.environ["PORTAL_S3_VERIFY_WAIT_SEC"] = "-5"
        self.stub_sync()
        self.stub_s3(self.expected)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])


class TestVerify(SyncTestBase):
    def test_exact_match_succeeds(self):
        self.stub_sync()
        self.stub_s3(self.expected)
        summary = target.run(self.make_args(), self.logger)
        verify = summary["verify"]
        self.assertTrue(verify["verified"])
        self.assertEqual(verify["missing_count"], 0)
        self.assertEqual(verify["extra_count"], 0)
        self.assertEqual(verify["size_mismatch_count"], 0)
        self.assertEqual(verify["expected_file_count"], verify["actual_file_count"])
        self.assertEqual(verify["expected_total_bytes"], verify["actual_total_bytes"])

    def test_missing_fails(self):
        actual = dict(self.expected)
        actual.pop("06-80_duplicate_proposal_check/01_result/dup.jsonl")
        self.stub_sync()
        self.stub_s3(actual)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)

    def test_extra_fails(self):
        actual = dict(self.expected)
        actual["01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl"] = 999
        self.stub_sync()
        self.stub_s3(actual)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)

    def test_size_mismatch_fails(self):
        actual = dict(self.expected)
        actual["01-1_fetch_gmail/01_result/fetch_gmail.jsonl"] = 11
        self.stub_sync()
        self.stub_s3(actual)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)

    def test_list_failure_fails(self):
        self.stub_sync()
        self.stub_s3(self.expected, fail_list=True)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)

    def test_broken_pagination_fails(self):
        self.stub_sync()
        self.stub_s3(self.expected, truncated_without_token=True)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)

    def test_pagination_collects_all_pages(self):
        many = {f"01-1_fetch_gmail/01_result/f{i:03d}.jsonl": i for i in range(1, 11)}
        self.expected = many
        self._write_prepare_outputs()
        self.stub_sync()
        self.stub_s3(many)
        summary = target.run(self.make_args(), self.logger)
        self.assertEqual(summary["verify"]["actual_file_count"], 10)
        self.assertTrue(summary["verify"]["verified"])

    def test_only_three_samples_are_reported(self):
        actual = {}
        self.stub_sync()
        self.stub_s3(actual)
        result = target.verify(self.expected, {}, self.logger)
        self.assertEqual(len(result["missing_samples"]), 3)
        self.assertFalse(result["verified"])


class TestExitCodes(SyncTestBase):
    def run_main(self, dry_run=False):
        sys_argv = sys.argv
        sys.argv = [
            "portal_s3_sync.py",
            "--pipeline-root",
            str(self.root),
            "--step-dir",
            str(self.step_dir),
            "--prepare-dir",
            str(self.prepare_dir),
        ]
        if dry_run:
            sys.argv.append("--dry-run")
        try:
            return target.main()
        finally:
            sys.argv = sys_argv

    def test_success_returns_zero(self):
        self.stub_sync()
        self.stub_s3(self.expected)
        self.assertEqual(self.run_main(), 0)

    def test_sync_failure_returns_non_zero(self):
        self.stub_sync(fail=True)
        self.stub_s3(self.expected)
        self.assertEqual(self.run_main(), 1)
        with open(self.step_dir / "01_result" / target.SYNC_SUMMARY_FILENAME, encoding="utf-8") as f:
            summary = json.load(f)
        self.assertEqual(summary["sync_status"], "FAILED")

    def test_verify_failure_returns_non_zero(self):
        self.stub_sync()
        self.stub_s3({})
        self.assertEqual(self.run_main(), 1)

    def test_missing_manifest_returns_non_zero(self):
        (self.prepare_dir / "01_result" / target.MANIFEST_FILENAME).unlink()
        self.stub_sync()
        self.stub_s3(self.expected)
        self.assertEqual(self.run_main(), 1)
        self.assertEqual(self.sync_calls, [])

    def test_dry_run_does_not_wait_or_verify(self):
        self.stub_sync()
        self.stub_s3(self.expected)
        self.assertEqual(self.run_main(dry_run=True), 0)
        self.assertEqual(self.sleep_calls, [])
        self.assertEqual(len(self.sync_calls), 1)
        self.assertIn("--dryrun", self.sync_calls[0])


if __name__ == "__main__":
    unittest.main(verbosity=2)

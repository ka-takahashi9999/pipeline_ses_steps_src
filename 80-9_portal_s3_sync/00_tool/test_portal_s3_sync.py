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

BUCKET = target.EXPECTED_BUCKET
BASE_PREFIX = target.EXPECTED_BASE_PREFIX
PORTAL_PREFIX = target.EXPECTED_PORTAL_PREFIX
DESTINATION = target.EXPECTED_DESTINATION_URI


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
        self.step_dir.mkdir(parents=True, exist_ok=True)
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
        self._write_source_tree()
        self._write_prepare_outputs()

        self.original_sleep = target.time.sleep
        target.time.sleep = self._fake_sleep
        self.original_run_sync = target.run_sync
        self.original_build = target.build_s3_client
        os.environ["PORTAL_S3_VERIFY_WAIT_SEC"] = "0"

    def tearDown(self):
        target.time.sleep = self.original_sleep
        target.run_sync = self.original_run_sync
        target.build_s3_client = self.original_build
        for name in ("PORTAL_S3_VERIFY_WAIT_SEC", "PORTAL_S3_PREFIX", "PIPELINE_S3_BUCKET",
                     "PIPELINE_S3_BASE_PREFIX"):
            os.environ.pop(name, None)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _fake_sleep(self, seconds):
        self.sleep_calls.append(seconds)

    def _write_source_tree(self):
        """manifest記載ファイルの実体と、manifest外のノイズを作る。"""
        for relative_path, size in self.expected.items():
            path = self.root / relative_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"x" * size)
        # manifest外（stagingにもS3にも入ってはならない）
        for noise in (
            "01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl",
            "01-1_fetch_gmail/01_result/.gitkeep",
            "09-1_mail_display_format/01_result/mail_display_format_20260814/.gitkeep",
            "09-1_mail_display_format/01_result/nested/deep/.gitkeep",
            "06-80_duplicate_proposal_check/01_result/dup.jsonl.bak_20260424",
            "80-8_portal_s3_prepare/01_result/portal_s3_manifest.jsonl",
        ):
            path = self.root / noise
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("noise", encoding="utf-8")

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

    def stub_sync(self, fail=False, capture_tree=False):
        def _run_sync(argv, logger):
            record = {"argv": argv}
            if capture_tree:
                stage_root = Path(argv[3])
                tree = {}
                for dirpath, _dirnames, filenames in os.walk(str(stage_root)):
                    for name in filenames:
                        child = Path(dirpath) / name
                        tree[str(child.relative_to(stage_root))] = child.stat().st_size
                record["tree"] = tree
                record["stage_root"] = str(stage_root)
            self.sync_calls.append(record)
            if fail:
                raise target.SyncError("aws s3 sync が失敗しました (exit=1)")

        target.run_sync = _run_sync

    def stub_s3(self, actual_sizes, **kwargs):
        objects = {f"{PORTAL_PREFIX}/{path}": size for path, size in actual_sizes.items()}
        client = FakeS3Client(objects, **kwargs)
        target.build_s3_client = lambda region: client
        return client


# ---------------------------------------------------------------------------
# 1. destination安全ロック
# ---------------------------------------------------------------------------


class TestDestinationLock(SyncTestBase):
    def test_valid_destination_passes(self):
        self.assertEqual(lock := target.lock_destination(BUCKET, BASE_PREFIX, PORTAL_PREFIX), DESTINATION)
        self.assertEqual(lock, "s3://technoverse/pipeline_ses_steps/pipeline_ses_steps/")

    def test_rejected_portal_prefixes(self):
        rejected = (
            "",
            "/",
            "pipeline_ses_steps",
            "pipeline_ses_steps/",
            "/pipeline_ses_steps/pipeline_ses_steps",
            "pipeline_ses_steps/..",
            "pipeline_ses_steps/pipeline_ses_steps/",
            "pipeline_ses_steps/pipeline_ses_steps/../",
            "pipeline_ses_steps/pipeline_ses_steps/..",
            "pipeline_ses_steps/pipeline_ses_steps/../other",
            "pipeline_ses_steps/pipeline_ses_steps_other",
            "other/pipeline_ses_steps",
            "pipeline-logs",
            "pipeline-status",
            None,
            123,
        )
        for prefix in rejected:
            with self.assertRaises(target.SyncError, msg=repr(prefix)):
                target.lock_destination(BUCKET, BASE_PREFIX, prefix)

    def test_rejected_buckets(self):
        for bucket in ("", "other-bucket", "technoverse2", "Technoverse", None):
            with self.assertRaises(target.SyncError, msg=repr(bucket)):
                target.lock_destination(bucket, BASE_PREFIX, PORTAL_PREFIX)

    def test_rejected_base_prefixes(self):
        for base in ("", "pipeline_ses_steps/", "other", None):
            with self.assertRaises(target.SyncError, msg=repr(base)):
                target.lock_destination(BUCKET, base, PORTAL_PREFIX)

    def test_env_override_to_parent_prefix_fails_before_sync(self):
        """設定値で上位prefixへ --delete を向けられないこと。"""
        os.environ["PORTAL_S3_PREFIX"] = "pipeline_ses_steps"
        self.stub_sync()
        self.stub_s3(self.expected)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [], msg="lock違反時にsyncが呼ばれてはいけない")

    def test_env_override_bucket_fails_before_sync(self):
        os.environ["PIPELINE_S3_BUCKET"] = "someone-elses-bucket"
        self.stub_sync()
        self.stub_s3(self.expected)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_env_override_with_dotdot_fails_before_sync(self):
        os.environ["PORTAL_S3_PREFIX"] = "pipeline_ses_steps/pipeline_ses_steps/.."
        self.stub_sync()
        self.stub_s3(self.expected)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_build_sync_argv_rejects_foreign_destination(self):
        stage = self.tmp / "stage"
        stage.mkdir()
        for uri in ("s3://technoverse/pipeline_ses_steps/", "s3://other/x/", ""):
            with self.assertRaises(target.SyncError, msg=uri):
                target.build_sync_argv(stage, uri, "ap-northeast-1", False)

    def test_run_sync_rejects_foreign_destination(self):
        argv = [target.AWS_BIN, "s3", "sync", "/tmp/x", "s3://technoverse/pipeline_ses_steps/", "--delete"]
        with self.assertRaises(target.SyncError):
            target.run_sync(argv, self.logger)


# ---------------------------------------------------------------------------
# 4. staging tree方式
# ---------------------------------------------------------------------------


class TestStagingTree(SyncTestBase):
    def test_staging_tree_matches_manifest_exactly(self):
        self.stub_sync(capture_tree=True)
        self.stub_s3(self.expected)
        target.run(self.make_args(), self.logger)
        self.assertEqual(len(self.sync_calls), 1)
        self.assertEqual(self.sync_calls[0]["tree"], self.expected)

    def test_manifest_outsiders_are_not_staged(self):
        self.stub_sync(capture_tree=True)
        self.stub_s3(self.expected)
        target.run(self.make_args(), self.logger)
        staged = set(self.sync_calls[0]["tree"])
        for noise in (
            "01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl",
            "01-1_fetch_gmail/01_result/.gitkeep",
            "09-1_mail_display_format/01_result/mail_display_format_20260814/.gitkeep",
            "09-1_mail_display_format/01_result/nested/deep/.gitkeep",
            "06-80_duplicate_proposal_check/01_result/dup.jsonl.bak_20260424",
            "80-8_portal_s3_prepare/01_result/portal_s3_manifest.jsonl",
        ):
            self.assertNotIn(noise, staged)

    def test_sync_argv_has_no_cli_filters(self):
        self.stub_sync(capture_tree=True)
        self.stub_s3(self.expected)
        target.run(self.make_args(), self.logger)
        argv = self.sync_calls[0]["argv"]
        self.assertNotIn("--include", argv)
        self.assertNotIn("--exclude", argv)
        self.assertIn("--delete", argv)
        self.assertIn("--no-follow-symlinks", argv)
        self.assertEqual(argv[4], DESTINATION)
        self.assertEqual(argv[3], self.sync_calls[0]["stage_root"])
        self.assertNotEqual(argv[3], str(self.root), msg="sync sourceはstaging rootであること")

    def test_staging_is_cleaned_up_on_success(self):
        self.stub_sync(capture_tree=True)
        self.stub_s3(self.expected)
        target.run(self.make_args(), self.logger)
        self.assertFalse(Path(self.sync_calls[0]["stage_root"]).exists())
        self.assertEqual(self._stage_dirs(), [])

    def test_staging_is_cleaned_up_on_sync_failure(self):
        self.stub_sync(fail=True, capture_tree=True)
        self.stub_s3(self.expected)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self._stage_dirs(), [])

    def test_source_files_survive_staging_cleanup(self):
        self.stub_sync()
        self.stub_s3(self.expected)
        target.run(self.make_args(), self.logger)
        for relative_path, size in self.expected.items():
            self.assertEqual((self.root / relative_path).stat().st_size, size)

    def _stage_dirs(self):
        return [p.name for p in self.step_dir.iterdir() if p.name.startswith(target.STAGE_DIR_PREFIX)]

    def test_missing_source_file_fails_before_sync(self):
        (self.root / "06-80_duplicate_proposal_check/01_result/dup.jsonl").unlink()
        self.stub_sync()
        self.stub_s3(self.expected)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])
        self.assertEqual(self._stage_dirs(), [])

    def test_source_size_mismatch_fails_before_sync(self):
        path = self.root / "06-80_duplicate_proposal_check/01_result/dup.jsonl"
        path.write_bytes(b"y" * 999)
        self.stub_sync()
        self.stub_s3(self.expected)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_symlink_source_fails_before_sync(self):
        path = self.root / "06-80_duplicate_proposal_check/01_result/dup.jsonl"
        path.unlink()
        path.symlink_to(self.root / "01-1_fetch_gmail/01_result/fetch_gmail.jsonl")
        self.stub_sync()
        self.stub_s3(self.expected)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_unsafe_manifest_paths_fail_before_sync(self):
        for bad in ("/etc/passwd", "../outside.jsonl", "a/../../b.jsonl", "a//b.jsonl", ""):
            with self.assertRaises(target.SyncError, msg=bad):
                target.validate_relative_path(bad)

    def test_manifest_with_absolute_path_fails_before_sync(self):
        write_jsonl(
            str(self.prepare_dir / "01_result" / target.MANIFEST_FILENAME),
            [{"relative_path": "/etc/passwd", "size": 1}],
        )
        self.stub_sync()
        self.stub_s3(self.expected)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_manifest_with_parent_traversal_fails_before_sync(self):
        write_jsonl(
            str(self.prepare_dir / "01_result" / target.MANIFEST_FILENAME),
            [{"relative_path": "01-1_fetch_gmail/../../secret.jsonl", "size": 1}],
        )
        self.stub_sync()
        self.stub_s3(self.expected)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_duplicate_manifest_path_fails_before_sync(self):
        manifest = self.prepare_dir / "01_result" / target.MANIFEST_FILENAME
        with open(manifest, "a", encoding="utf-8") as f:
            f.write(json.dumps({"relative_path": "01-1_fetch_gmail/01_result/fetch_gmail.jsonl", "size": 10}) + "\n")
        self.stub_sync()
        self.stub_s3(self.expected)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])


# ---------------------------------------------------------------------------
# 5. verify（directory marker含む）
# ---------------------------------------------------------------------------


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

    def test_directory_marker_is_detected_as_extra(self):
        actual = dict(self.expected)
        actual["01-1_fetch_gmail/01_result/"] = 0
        self.stub_sync()
        self.stub_s3(actual)
        with self.assertRaises(target.SyncError):
            target.run(self.make_args(), self.logger)

    def test_nested_directory_marker_is_detected_as_extra(self):
        actual = dict(self.expected)
        actual["09-1_mail_display_format/01_result/mail_display_format_20260814/"] = 0
        self.stub_sync()
        self.stub_s3(actual)
        result = target.verify(self.expected, target.list_portal_objects(
            self.stub_s3(actual), BUCKET, PORTAL_PREFIX), self.logger)
        self.assertEqual(result["extra_count"], 1)
        self.assertFalse(result["verified"])

    def test_prefix_object_itself_is_ignored(self):
        client = FakeS3Client(
            {f"{PORTAL_PREFIX}/": 0, **{f"{PORTAL_PREFIX}/{p}": s for p, s in self.expected.items()}}
        )
        actual = target.list_portal_objects(client, BUCKET, PORTAL_PREFIX)
        self.assertEqual(actual, self.expected)

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
        self._write_source_tree()
        self._write_prepare_outputs()
        self.stub_sync()
        self.stub_s3(many)
        summary = target.run(self.make_args(), self.logger)
        self.assertEqual(summary["verify"]["actual_file_count"], 10)
        self.assertTrue(summary["verify"]["verified"])

    def test_only_three_samples_are_reported(self):
        result = target.verify(self.expected, {}, self.logger)
        self.assertEqual(len(result["missing_samples"]), 3)
        self.assertFalse(result["verified"])


# ---------------------------------------------------------------------------
# wait / exit code
# ---------------------------------------------------------------------------


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

    def test_destination_lock_violation_returns_non_zero(self):
        os.environ["PORTAL_S3_PREFIX"] = "pipeline_ses_steps"
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
        self.assertIn("--dryrun", self.sync_calls[0]["argv"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

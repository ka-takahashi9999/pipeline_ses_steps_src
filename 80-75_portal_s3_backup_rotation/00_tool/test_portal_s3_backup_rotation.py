"""
80-75_portal_s3_backup_rotation focused test

AWS CLI実行・S3 LIST・S3 GETはすべてmockし、production S3へは一切書き込まない。
full Pipeline実行は行わない。

カバー範囲:
  bk1 destination lock  (1)-(7)
  backup verify         (8)-(13)
  previous CURRENT guard (14)-(19)
  bootstrap             (20)-(23)

実行:
  python3 80-75_portal_s3_backup_rotation/00_tool/test_portal_s3_backup_rotation.py
"""

import argparse
import copy
import io
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

import portal_s3_backup_rotation as target  # noqa: E402
from common.logger import get_logger  # noqa: E402

BUCKET = target.EXPECTED_BUCKET
BASE_PREFIX = target.EXPECTED_BASE_PREFIX
CURRENT_PREFIX = target.EXPECTED_CURRENT_PREFIX
BACKUP_PREFIX = target.EXPECTED_BACKUP_PREFIX
SOURCE_URI = target.EXPECTED_SOURCE_URI
DESTINATION_URI = target.EXPECTED_DESTINATION_URI

PREV_RUN_DATE = "20260818"
PREV_RUN_ID = "sfn-9b6ab8c1-6089-4121-8ffc-e460affae951"
CURRENT_RUN_ID = "sfn-current-run"


class FakeS3Client:
    """
    LIST / GET のみをmockする最小のS3クライアント。
    objects: {key: size} / status_docs: {key: dict} / LIST metadataを保持する。
    """

    def __init__(self, objects, status_docs, fail_list_prefixes=(), truncated_prefixes=(),
                 fail_get=False):
        self.objects = dict(objects)
        self.status_docs = dict(status_docs)
        self.fail_list_prefixes = tuple(fail_list_prefixes)
        self.truncated_prefixes = tuple(truncated_prefixes)
        self.fail_get = fail_get
        self.last_modified = {}
        self.etags = {}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _FakePaginator(self)

    def get_object(self, Bucket, Key):  # noqa: N803 - boto3互換シグネチャ
        if self.fail_get:
            raise RuntimeError("simulated GET failure")
        if Key not in self.status_docs:
            raise RuntimeError(f"NoSuchKey: {Key}")
        body = json.dumps(self.status_docs[Key]).encode("utf-8")
        return {"Body": io.BytesIO(body)}


class _FakePaginator:
    def __init__(self, client):
        self.client = client

    def paginate(self, Bucket, Prefix):  # noqa: N803 - boto3互換シグネチャ
        for bad in self.client.fail_list_prefixes:
            if Prefix.startswith(bad):
                raise RuntimeError("simulated LIST failure")
        for bad in self.client.truncated_prefixes:
            if Prefix.startswith(bad):
                yield {"Contents": [], "IsTruncated": True}
                return
        keys = sorted(k for k in self.client.objects if k.startswith(Prefix))
        for i in range(0, max(len(keys), 1), 2):
            chunk = keys[i : i + 2]
            yield {
                "Contents": [
                    {
                        "Key": k,
                        "Size": self.client.objects[k],
                        "LastModified": self.client.last_modified.get(k, 0),
                        "ETag": self.client.etags.get(k, f'"etag-{k}"'),
                    }
                    for k in chunk
                ]
            }


def make_sync_summary(**overrides):
    summary = {
        "step": "80-9_portal_s3_sync",
        "executed_at": "2026-08-19 06:29:58",
        "mode": "apply",
        "run_date": PREV_RUN_DATE,
        "run_date_source": "env",
        "run_id": PREV_RUN_ID,
        "run_id_source": "env",
        "s3_destination": SOURCE_URI,
        "s3_destination_locked": True,
        "sync_status": "SUCCEEDED",
        "verify": {
            "expected_file_count": 3,
            "actual_file_count": 3,
            "expected_total_bytes": 35,
            "actual_total_bytes": 35,
            "missing_count": 0,
            "extra_count": 0,
            "size_mismatch_count": 0,
            "missing_samples": [],
            "extra_samples": [],
            "size_mismatch_samples": [],
            "verified": True,
        },
        "verify_wait_sec": 30,
        "wait_performed": True,
    }
    summary.update(overrides)
    return summary


class RotationTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="test_80_75_"))
        self.step_dir = self.tmp / "out"
        self.sync_dir = self.tmp / "sync"
        (self.sync_dir / "01_result").mkdir(parents=True)
        self.step_dir.mkdir(parents=True)
        self.logger = get_logger("test_80-75")
        self.sleep_calls = []
        self.sync_calls = []
        self.client = None

        self.current = {
            "01-1_fetch_gmail/01_result/fetch_gmail.jsonl": 10,
            "06-80_duplicate_proposal_check/01_result/dup.jsonl": 20,
            "09-1_mail_display_format/01_result/20260818/a.txt": 5,
        }
        self.current_etags = {path: '"etag-v1"' for path in self.current}
        self.current_last_modified = {path: 100 for path in self.current}
        self.backup = dict(self.current)
        self.write_sync_summary(make_sync_summary())

        self.original_sleep = target.time.sleep
        target.time.sleep = self._fake_sleep
        self.original_run_sync = target.run_sync
        self.original_build = target.build_s3_client
        os.environ["PORTAL_S3_VERIFY_WAIT_SEC"] = "0"
        os.environ.pop("RUN_ID", None)
        os.environ.pop("RUN_DATE", None)

    def tearDown(self):
        target.time.sleep = self.original_sleep
        target.run_sync = self.original_run_sync
        target.build_s3_client = self.original_build
        for name in (
            "PORTAL_S3_VERIFY_WAIT_SEC",
            "PORTAL_S3_PREFIX",
            "PORTAL_S3_BACKUP_PREFIX",
            "PIPELINE_S3_BUCKET",
            "PIPELINE_S3_BASE_PREFIX",
            "PIPELINE_STATUS_PREFIX",
            "RUN_ID",
            "RUN_DATE",
        ):
            os.environ.pop(name, None)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _fake_sleep(self, seconds):
        self.sleep_calls.append(seconds)

    def write_sync_summary(self, summary):
        path = self.sync_dir / "01_result" / target.SYNC_SUMMARY_FILENAME
        with open(path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False)

    def make_args(self, bootstrap=False, dry_run=False, current_run_id=None):
        return argparse.Namespace(
            bootstrap=bootstrap,
            dry_run=dry_run,
            step_dir=str(self.step_dir),
            sync_dir=str(self.sync_dir),
            current_run_id=current_run_id,
        )

    def stub_sync(self, fail=False, apply_to_backup=True, mutate_current=None):
        def _run_sync(argv, logger):
            self.sync_calls.append({"argv": argv})
            if fail:
                raise target.RotationError("aws s3 sync が失敗しました (exit=1)")
            if apply_to_backup and "--dryrun" not in argv:
                self.backup = dict(self.current)
                self._refresh_client()
            if mutate_current is not None and "--dryrun" not in argv:
                mutate_current()
                self._refresh_client()

        target.run_sync = _run_sync

    def stub_s3(self, status_runs=None, **kwargs):
        self._status_runs = status_runs if status_runs is not None else [
            (PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0),
        ]
        self._client_kwargs = kwargs
        return self._refresh_client()

    def _refresh_client(self):
        """
        fake S3の状態を更新する。run()は最初に取得したclientを使い続けるため、
        既存clientがあれば同一インスタンスを in-place 更新する（sync後のLISTへ反映させる）。
        """
        objects = {f"{CURRENT_PREFIX}/{p}": s for p, s in self.current.items()}
        objects.update({f"{BACKUP_PREFIX}/{p}": s for p, s in self.backup.items()})
        status_docs = {}
        last_modified = {}
        etags = {}
        for path in self.current:
            key = f"{CURRENT_PREFIX}/{path}"
            last_modified[key] = self.current_last_modified.get(path, 100)
            etags[key] = self.current_etags.get(path, '"etag-v1"')
        for index, (run_date, run_id, status, exit_code) in enumerate(self._status_runs):
            key = f"{BASE_PREFIX}/pipeline-status/{run_date}/{run_id}/status.json"
            objects[key] = 500
            last_modified[key] = index + 1
            status_docs[key] = {
                "run_id": run_id,
                "run_date": run_date,
                "status": status,
                "exit_code": exit_code,
                "finished_at": None if status == "RUNNING" else "2026-08-19T00:00:00Z",
                "finished_at_source": "not_finished" if status == "RUNNING" else "managed_wrapper",
                "exit_code_source": "pending" if status == "RUNNING" else "managed_wrapper",
            }
        client = getattr(self, "client", None)
        if client is None:
            client = FakeS3Client(objects, status_docs, **self._client_kwargs)
        else:
            client.objects = objects
            client.status_docs = status_docs
        client.last_modified = last_modified
        client.etags = etags
        self.client = client
        target.build_s3_client = lambda region: client
        return client


# ---------------------------------------------------------------------------
# (1)-(7) bk1 destination lock
# ---------------------------------------------------------------------------


class TestDestinationLock(RotationTestBase):
    def test_01_canonical_current_to_bk1_passes(self):
        source, destination = target.lock_backup_route(
            BUCKET, BASE_PREFIX, CURRENT_PREFIX, BACKUP_PREFIX
        )
        self.assertEqual(source, "s3://technoverse/pipeline_ses_steps/pipeline_ses_steps/")
        self.assertEqual(
            destination, "s3://technoverse/pipeline_ses_steps/pipeline_ses_steps_bk1/"
        )

    def test_02_wrong_source_fails(self):
        for prefix in (
            "",
            "/",
            "pipeline_ses_steps",
            "pipeline_ses_steps/",
            "pipeline_ses_steps/pipeline_ses_steps/",
            "pipeline_ses_steps/pipeline_ses_steps/..",
            "pipeline_ses_steps/pipeline_ses_steps_bk1",
            "pipeline_ses_steps/private",
            "other/pipeline_ses_steps",
            None,
            123,
        ):
            with self.assertRaises(target.RotationError, msg=repr(prefix)):
                target.lock_backup_route(BUCKET, BASE_PREFIX, prefix, BACKUP_PREFIX)

    def test_03_wrong_destination_fails(self):
        for prefix in (
            "",
            "pipeline_ses_steps/pipeline_ses_steps",
            "pipeline_ses_steps/pipeline_ses_steps_bk1/",
            "pipeline_ses_steps/pipeline_ses_steps_bk2",
            "pipeline_ses_steps/pipeline_ses_steps_bk3",
            "pipeline_ses_steps/pipeline_ses_steps_bk1/..",
            "pipeline_ses_steps/pipeline_ses_steps_bk1_other",
            None,
        ):
            with self.assertRaises(target.RotationError, msg=repr(prefix)):
                target.lock_backup_route(BUCKET, BASE_PREFIX, CURRENT_PREFIX, prefix)

    def test_04_bucket_root_fails(self):
        for base in ("", "/", ".", "..", None):
            with self.assertRaises(target.RotationError, msg=repr(base)):
                target.lock_backup_route(BUCKET, base, CURRENT_PREFIX, BACKUP_PREFIX)
        with self.assertRaises(target.RotationError):
            target.build_sync_argv(f"s3://{BUCKET}/", DESTINATION_URI, "ap-northeast-1", False)
        with self.assertRaises(target.RotationError):
            target.build_sync_argv(SOURCE_URI, f"s3://{BUCKET}/", "ap-northeast-1", False)

    def test_05_private_prefix_fails(self):
        with self.assertRaises(target.RotationError):
            target.lock_backup_route(BUCKET, BASE_PREFIX, CURRENT_PREFIX, "pipeline_ses_steps/private")
        with self.assertRaises(target.RotationError):
            target.lock_backup_route(
                BUCKET, BASE_PREFIX, "pipeline_ses_steps/private/mail_master", BACKUP_PREFIX
            )
        with self.assertRaises(target.RotationError):
            target.build_sync_argv(
                SOURCE_URI, "s3://technoverse/pipeline_ses_steps/private/", "ap-northeast-1", False
            )

    def test_06_status_and_logs_prefix_fails(self):
        for prefix in ("pipeline_ses_steps/pipeline-status", "pipeline_ses_steps/pipeline-logs"):
            with self.assertRaises(target.RotationError, msg=prefix):
                target.lock_backup_route(BUCKET, BASE_PREFIX, CURRENT_PREFIX, prefix)
            with self.assertRaises(target.RotationError, msg=prefix):
                target.lock_backup_route(BUCKET, BASE_PREFIX, prefix, BACKUP_PREFIX)
        for prefix in ("pipeline-logs", "", "pipeline-status/", None):
            with self.assertRaises(target.RotationError, msg=repr(prefix)):
                target.lock_status_prefix(prefix)

    def test_07_arbitrary_uri_fails(self):
        for bucket in ("", "other-bucket", "technoverse2", "Technoverse", None):
            with self.assertRaises(target.RotationError, msg=repr(bucket)):
                target.lock_backup_route(bucket, BASE_PREFIX, CURRENT_PREFIX, BACKUP_PREFIX)
        for prefix in ("pipeline_ses_steps/*", "pipeline_ses_steps/pipeline_ses_steps_bk?"):
            with self.assertRaises(target.RotationError, msg=prefix):
                target.lock_backup_route(BUCKET, BASE_PREFIX, CURRENT_PREFIX, prefix)
        for uri in ("s3://other/x/", "", "s3://technoverse/pipeline_ses_steps/"):
            with self.assertRaises(target.RotationError, msg=uri):
                target.build_sync_argv(SOURCE_URI, uri, "ap-northeast-1", False)
        with self.assertRaises(target.RotationError):
            target.run_sync(
                [target.AWS_BIN, "s3", "sync", SOURCE_URI, "s3://technoverse/pipeline_ses_steps/",
                 "--delete"],
                self.logger,
            )

    def test_env_override_cannot_widen_destination(self):
        os.environ["PORTAL_S3_BACKUP_PREFIX"] = "pipeline_ses_steps"
        self.stub_sync()
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [], msg="lock違反時にsyncが呼ばれてはいけない")

    def test_sync_argv_has_no_cli_filters(self):
        argv = target.build_sync_argv(SOURCE_URI, DESTINATION_URI, "ap-northeast-1", False)
        self.assertEqual(argv[3], SOURCE_URI)
        self.assertEqual(argv[4], DESTINATION_URI)
        self.assertIn("--delete", argv)
        self.assertNotIn("--include", argv)
        self.assertNotIn("--exclude", argv)


# ---------------------------------------------------------------------------
# (8)-(13) backup verify
# ---------------------------------------------------------------------------


class TestBackupVerify(RotationTestBase):
    def test_08_exact_key_and_size_passes(self):
        self.stub_sync()
        self.stub_s3()
        summary = target.run(self.make_args(), self.logger)
        verify = summary["verify"]
        self.assertTrue(verify["verified"])
        self.assertEqual(verify["missing_count"], 0)
        self.assertEqual(verify["extra_count"], 0)
        self.assertEqual(verify["size_mismatch_count"], 0)
        self.assertEqual(verify["expected_file_count"], verify["actual_file_count"])
        self.assertEqual(verify["expected_total_bytes"], verify["actual_total_bytes"])
        self.assertEqual(summary["backup_status"], "SUCCEEDED")

    def test_09_missing_fails(self):
        self.stub_sync(apply_to_backup=False)
        self.backup = dict(self.current)
        self.backup.pop("06-80_duplicate_proposal_check/01_result/dup.jsonl")
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)

    def test_10_extra_fails(self):
        self.stub_sync(apply_to_backup=False)
        self.backup = dict(self.current)
        self.backup["09-1_mail_display_format/01_result/20260817/old.txt"] = 7
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)

    def test_11_size_mismatch_fails(self):
        self.stub_sync(apply_to_backup=False)
        self.backup = dict(self.current)
        self.backup["01-1_fetch_gmail/01_result/fetch_gmail.jsonl"] = 11
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)

    def test_12_sync_failure_fails(self):
        self.stub_sync(fail=True)
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)

    def test_13_list_failure_fails(self):
        self.stub_sync()
        self.stub_s3(fail_list_prefixes=(f"{BACKUP_PREFIX}/",))
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [], msg="LIST失敗時にsyncへ進んではいけない")

    def test_broken_pagination_fails(self):
        self.stub_sync()
        self.stub_s3(truncated_prefixes=(f"{CURRENT_PREFIX}/",))
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)

    def test_directory_marker_in_backup_is_extra(self):
        result = target.compare_sets(
            self.current, {**self.current, "01-1_fetch_gmail/01_result/": 0}, self.logger
        )
        self.assertEqual(result["extra_count"], 1)
        self.assertFalse(result["verified"])

    def test_finding_fingerprint_path_added_fails(self):
        self.stub_sync(
            mutate_current=lambda: self.current.update({"new/01_result/added.jsonl": 4})
        )
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)

    def test_finding_fingerprint_path_deleted_fails(self):
        path = next(iter(self.current))
        self.stub_sync(mutate_current=lambda: self.current.pop(path))
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)

    def test_finding_fingerprint_size_changed_fails(self):
        path = next(iter(self.current))
        self.stub_sync(mutate_current=lambda: self.current.update({path: self.current[path] + 1}))
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)

    def test_finding_fingerprint_same_size_etag_changed_fails(self):
        path = next(iter(self.current))
        self.stub_sync(mutate_current=lambda: self.current_etags.update({path: '"etag-v2"'}))
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)

    def test_finding_fingerprint_same_size_last_modified_changed_fails(self):
        path = next(iter(self.current))
        self.stub_sync(
            mutate_current=lambda: self.current_last_modified.update(
                {path: self.current_last_modified[path] + 1}
            )
        )
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)

    def test_finding_fingerprint_unchanged_passes(self):
        self.stub_sync()
        self.stub_s3()
        summary = target.run(self.make_args(), self.logger)
        self.assertTrue(summary["verify"]["verified"])

    def test_current_changed_during_backup_fails(self):
        def _run_sync(argv, logger):
            self.sync_calls.append({"argv": argv})
            self.backup = dict(self.current)
            self.current["09-1_mail_display_format/01_result/20260819/new.txt"] = 9
            self._refresh_client()

        target.run_sync = _run_sync
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)

    def test_empty_current_fails(self):
        self.current = {}
        self.backup = {}
        self.stub_sync()
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_wait_is_performed_before_verify(self):
        os.environ["PORTAL_S3_VERIFY_WAIT_SEC"] = "60"
        self.stub_sync()
        self.stub_s3()
        summary = target.run(self.make_args(), self.logger)
        self.assertEqual(self.sleep_calls, [60])
        self.assertTrue(summary["wait_performed"])

    def test_invalid_wait_config_fails_before_sync(self):
        os.environ["PORTAL_S3_VERIFY_WAIT_SEC"] = "-5"
        self.stub_sync()
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])


# ---------------------------------------------------------------------------
# (14)-(19) previous CURRENT guard
# ---------------------------------------------------------------------------


class TestPreviousCurrentGuard(RotationTestBase):
    def test_14_previous_verified_passes(self):
        self.stub_sync()
        self.stub_s3()
        summary = target.run(self.make_args(), self.logger)
        self.assertEqual(summary["previous_current"]["run_date"], PREV_RUN_DATE)
        self.assertEqual(summary["previous_current"]["run_id"], PREV_RUN_ID)
        self.assertTrue(summary["verify"]["verified"])

    def test_15_verified_false_fails(self):
        summary = make_sync_summary()
        summary["verify"]["verified"] = False
        self.write_sync_summary(summary)
        self.stub_sync()
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_16_sync_status_not_succeeded_fails(self):
        for status in ("FAILED", "RUNNING", None):
            self.write_sync_summary(make_sync_summary(sync_status=status))
            self.stub_sync()
            self.stub_s3()
            with self.assertRaises(target.RotationError, msg=repr(status)):
                target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_16b_mode_dry_run_fails(self):
        self.write_sync_summary(make_sync_summary(mode="dry-run"))
        self.stub_sync()
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_17_wrong_destination_in_summary_fails(self):
        for destination in (
            DESTINATION_URI,
            "s3://technoverse/pipeline_ses_steps/",
            "s3://other/pipeline_ses_steps/pipeline_ses_steps/",
            None,
        ):
            self.write_sync_summary(make_sync_summary(s3_destination=destination))
            self.stub_sync()
            self.stub_s3()
            with self.assertRaises(target.RotationError, msg=repr(destination)):
                target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_18_pipeline_status_failed_fails(self):
        self.stub_sync()
        self.stub_s3(status_runs=[(PREV_RUN_DATE, PREV_RUN_ID, "FAILED", 1)])
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_18b_stale_success_summary_with_newer_failed_run_fails(self):
        """古い成功summaryだけが残り、直近runが80-9途中で落ちたケースをbackupしない。"""
        self.stub_sync()
        self.stub_s3(
            status_runs=[
                (PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0),
                ("20260819", "sfn-newer-failed", "FAILED", 1),
            ]
        )
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_18c_own_run_is_excluded_from_status_check(self):
        os.environ["RUN_DATE"] = "20260819"
        os.environ["RUN_ID"] = CURRENT_RUN_ID
        self.stub_sync()
        self.stub_s3(
            status_runs=[
                (PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0),
                ("20260819", CURRENT_RUN_ID, "RUNNING", None),
            ]
        )
        summary = target.run(self.make_args(), self.logger)
        self.assertTrue(summary["verify"]["verified"])

    def test_finding_current_cli_without_managed_env_fails(self):
        self.stub_sync()
        self.stub_s3(
            status_runs=[
                (PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0),
                ("20260819", CURRENT_RUN_ID, "RUNNING", None),
            ]
        )
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(current_run_id=CURRENT_RUN_ID), self.logger)

    def test_finding_current_cli_must_match_managed_env(self):
        os.environ["RUN_DATE"] = "20260819"
        os.environ["RUN_ID"] = CURRENT_RUN_ID
        self.stub_sync()
        self.stub_s3(
            status_runs=[
                (PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0),
                ("20260819", CURRENT_RUN_ID, "RUNNING", None),
            ]
        )
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(current_run_id="sfn-arbitrary"), self.logger)

    def test_finding_current_failed_cannot_be_excluded_by_cli(self):
        os.environ["RUN_DATE"] = "20260819"
        os.environ["RUN_ID"] = CURRENT_RUN_ID
        self.stub_sync()
        self.stub_s3(
            status_runs=[
                (PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0),
                ("20260819", CURRENT_RUN_ID, "FAILED", 1),
            ]
        )
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(current_run_id=CURRENT_RUN_ID), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_finding_current_succeeded_cannot_be_excluded(self):
        os.environ["RUN_DATE"] = "20260819"
        os.environ["RUN_ID"] = CURRENT_RUN_ID
        self.stub_sync()
        self.stub_s3(
            status_runs=[
                (PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0),
                ("20260819", CURRENT_RUN_ID, "SUCCEEDED", 0),
            ]
        )
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_finding_current_document_run_date_mismatch_fails(self):
        os.environ["RUN_DATE"] = "20260819"
        os.environ["RUN_ID"] = CURRENT_RUN_ID
        client = self.stub_s3(
            status_runs=[
                (PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0),
                ("20260819", CURRENT_RUN_ID, "RUNNING", None),
            ]
        )
        key = f"{BASE_PREFIX}/pipeline-status/20260819/{CURRENT_RUN_ID}/status.json"
        client.status_docs[key]["run_date"] = "20260820"
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)

    def test_finding_current_document_run_id_mismatch_fails(self):
        os.environ["RUN_DATE"] = "20260819"
        os.environ["RUN_ID"] = CURRENT_RUN_ID
        client = self.stub_s3(
            status_runs=[
                (PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0),
                ("20260819", CURRENT_RUN_ID, "RUNNING", None),
            ]
        )
        key = f"{BASE_PREFIX}/pipeline-status/20260819/{CURRENT_RUN_ID}/status.json"
        client.status_docs[key]["run_id"] = "sfn-tampered"
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)

    def test_finding_running_with_exit_code_fails(self):
        os.environ["RUN_DATE"] = "20260819"
        os.environ["RUN_ID"] = CURRENT_RUN_ID
        client = self.stub_s3(
            status_runs=[
                (PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0),
                ("20260819", CURRENT_RUN_ID, "RUNNING", None),
            ]
        )
        key = f"{BASE_PREFIX}/pipeline-status/20260819/{CURRENT_RUN_ID}/status.json"
        client.status_docs[key]["exit_code"] = 1
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)

    def test_finding_running_with_terminal_timestamp_fails(self):
        os.environ["RUN_DATE"] = "20260819"
        os.environ["RUN_ID"] = CURRENT_RUN_ID
        client = self.stub_s3(
            status_runs=[
                (PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0),
                ("20260819", CURRENT_RUN_ID, "RUNNING", None),
            ]
        )
        key = f"{BASE_PREFIX}/pipeline-status/20260819/{CURRENT_RUN_ID}/status.json"
        client.status_docs[key]["finished_at"] = "2026-08-19T01:00:00Z"
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)

    def test_finding_stale_summary_failed_cli_bypass_is_closed(self):
        os.environ["RUN_DATE"] = "20260819"
        os.environ["RUN_ID"] = "sfn-newer-failed"
        self.stub_sync()
        self.stub_s3(
            status_runs=[
                (PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0),
                ("20260819", "sfn-newer-failed", "FAILED", 1),
            ]
        )
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(current_run_id="sfn-newer-failed"), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_19_run_id_mismatch_fails(self):
        self.write_sync_summary(make_sync_summary(run_id="sfn-other-run"))
        self.stub_sync()
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_19b_run_date_mismatch_fails(self):
        self.write_sync_summary(make_sync_summary(run_date="20260101"))
        self.stub_sync()
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_19c_provenance_source_not_env_fails(self):
        for overrides in (
            {"run_id_source": "cli", "run_id": "sfn-cli"},
            {"run_date_source": "cli", "run_date": "20260819"},
            {"run_id_source": "default", "run_id": "unknown"},
            {"run_date_source": "default", "run_date": "unknown"},
        ):
            self.write_sync_summary(make_sync_summary(**overrides))
            self.stub_sync()
            self.stub_s3()
            with self.assertRaises(target.RotationError, msg=repr(overrides)):
                target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_19d_status_document_run_mismatch_fails(self):
        client = self.stub_s3()
        key = f"{BASE_PREFIX}/pipeline-status/{PREV_RUN_DATE}/{PREV_RUN_ID}/status.json"
        client.status_docs[key]["run_id"] = "sfn-tampered"
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_current_size_mismatch_with_summary_fails(self):
        """CURRENT実体が80-9 summaryのactualと一致しない場合はbackupしない。"""
        self.current["01-1_fetch_gmail/01_result/fetch_gmail.jsonl"] = 999
        self.backup = dict(self.current)
        self.stub_sync()
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_missing_sync_summary_fails(self):
        (self.sync_dir / "01_result" / target.SYNC_SUMMARY_FILENAME).unlink()
        self.stub_sync()
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])


# ---------------------------------------------------------------------------
# (20)-(23) bootstrap
# ---------------------------------------------------------------------------


class TestBootstrap(RotationTestBase):
    def test_20_bootstrap_dry_run_passes_when_bk1_absent(self):
        self.backup = {}
        self.stub_sync()
        self.stub_s3()
        summary = target.run(self.make_args(bootstrap=True, dry_run=True), self.logger)
        self.assertEqual(summary["operation"], "bootstrap")
        self.assertEqual(summary["mode"], "dry-run")
        self.assertEqual(summary["backup_before"]["file_count"], 0)
        self.assertEqual(summary["expected_backup"]["file_count"], len(self.current))
        self.assertEqual(summary["expected_backup"]["total_bytes"], sum(self.current.values()))
        self.assertEqual(summary["verify"]["skipped_reason"], "dry-run")
        self.assertIn("--dryrun", self.sync_calls[0]["argv"])
        self.assertEqual(self.sleep_calls, [])

    def test_21_bootstrap_fails_when_bk1_exists(self):
        self.stub_sync()
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(bootstrap=True), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_22_bootstrap_fails_when_current_absent(self):
        self.current = {}
        self.backup = {}
        self.stub_sync()
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(bootstrap=True), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_23_bootstrap_fails_on_invalid_previous_state(self):
        summary = make_sync_summary()
        summary["verify"]["missing_count"] = 2
        self.write_sync_summary(summary)
        self.backup = {}
        self.stub_sync()
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(bootstrap=True), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_rotation_fails_when_bk1_absent(self):
        """初回作成を通常rotation経路へ混ぜない。"""
        self.backup = {}
        self.stub_sync()
        self.stub_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])


# ---------------------------------------------------------------------------
# exit code / summary出力
# ---------------------------------------------------------------------------


class TestExitCodes(RotationTestBase):
    def run_main(self, bootstrap=False, dry_run=False):
        sys_argv = sys.argv
        sys.argv = [
            "portal_s3_backup_rotation.py",
            "--step-dir",
            str(self.step_dir),
            "--sync-dir",
            str(self.sync_dir),
        ]
        if bootstrap:
            sys.argv.append("--bootstrap")
        if dry_run:
            sys.argv.append("--dry-run")
        try:
            return target.main()
        finally:
            sys.argv = sys_argv

    def read_summary(self):
        path = self.step_dir / "01_result" / target.BACKUP_SUMMARY_FILENAME
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    def test_success_returns_zero(self):
        self.stub_sync()
        self.stub_s3()
        self.assertEqual(self.run_main(), 0)
        self.assertEqual(self.read_summary()["backup_status"], "SUCCEEDED")

    def test_guard_failure_returns_non_zero(self):
        self.write_sync_summary(make_sync_summary(sync_status="FAILED"))
        self.stub_sync()
        self.stub_s3()
        self.assertEqual(self.run_main(), 1)
        self.assertEqual(self.read_summary()["backup_status"], "FAILED")
        self.assertEqual(self.sync_calls, [])

    def test_verify_failure_returns_non_zero(self):
        self.stub_sync(apply_to_backup=False)
        self.backup = {}
        self.stub_s3()
        # bk1が0件だと rotation経路自体がFAILするため bootstrap経路で verify失敗を確認する
        self.assertEqual(self.run_main(bootstrap=True), 1)
        self.assertEqual(self.read_summary()["backup_status"], "FAILED")

    def test_bootstrap_dry_run_returns_zero(self):
        self.backup = {}
        self.stub_sync()
        self.stub_s3()
        self.assertEqual(self.run_main(bootstrap=True, dry_run=True), 0)
        summary = self.read_summary()
        self.assertEqual(summary["operation"], "bootstrap")
        self.assertEqual(summary["mode"], "dry-run")


class TestSummaryValidationUnits(RotationTestBase):
    def test_validate_returns_provenance(self):
        provenance = target.validate_previous_sync_summary(make_sync_summary())
        self.assertEqual(provenance["run_date"], PREV_RUN_DATE)
        self.assertEqual(provenance["run_id"], PREV_RUN_ID)
        self.assertEqual(provenance["file_count"], 3)
        self.assertEqual(provenance["total_bytes"], 35)

    def test_validate_rejects_nonzero_counters(self):
        for key in ("missing_count", "extra_count", "size_mismatch_count"):
            summary = copy.deepcopy(make_sync_summary())
            summary["verify"][key] = 1
            with self.assertRaises(target.RotationError, msg=key):
                target.validate_previous_sync_summary(summary)

    def test_validate_rejects_expected_actual_mismatch(self):
        summary = copy.deepcopy(make_sync_summary())
        summary["verify"]["actual_file_count"] = 2
        with self.assertRaises(target.RotationError):
            target.validate_previous_sync_summary(summary)

    def test_validate_rejects_zero_file_count(self):
        summary = copy.deepcopy(make_sync_summary())
        summary["verify"]["expected_file_count"] = 0
        summary["verify"]["actual_file_count"] = 0
        summary["verify"]["expected_total_bytes"] = 0
        summary["verify"]["actual_total_bytes"] = 0
        with self.assertRaises(target.RotationError):
            target.validate_previous_sync_summary(summary)

    def test_validate_rejects_unlocked_destination(self):
        with self.assertRaises(target.RotationError):
            target.validate_previous_sync_summary(make_sync_summary(s3_destination_locked=False))

    def test_wait_seconds_validation(self):
        for value in ("0", "30", 60, " 45 "):
            self.assertEqual(target.parse_wait_seconds(value), int(str(value).strip()))
        for value in ("-1", "1.5", "abc", "", "30s", None):
            with self.assertRaises(target.RotationError, msg=repr(value)):
                target.parse_wait_seconds(value)


if __name__ == "__main__":
    unittest.main(verbosity=2)

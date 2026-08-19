#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
01-1 mail master private S3 upload の focused test

AWSへの実writeは行わない（subprocess / boto3 clientはすべてmock）。
確認内容:
  ① destination安全ロック（RUN_DATE / prefix / bucket / Portal prefix / 上位prefix / `..`）
  ② upload前のlocal file検査（不存在 / symlink / empty / path不一致 / record 0件）
  ③ aws s3 cp のargv（recursive / sync / delete / wildcardが無いこと）
  ④ upload失敗 / head-object失敗 / size mismatch で非0終了すること
  ⑤ 冪等性（同一RUN_DATE再実行で同一key）
  ⑥ runner組込み（01-1 → private upload → 01-2 / run_step経由 / 2runner一致）
  ⑦ regression（80-8のmail master除外維持 / 80-9・fetch_gmail.py 無変更）

実行:
  python3 01-1_fetch_gmail/00_tool/test_upload_mail_master_private_s3.py
"""

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

STEP_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = STEP_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

GIT_ROOT = Path("/home/ec2-user/pipeline_ses_steps_src")
RUNNER = PROJECT_ROOT / "00_pipeline/00_tool/run_full_pipeline.sh"
RUNNER_MASTER = PROJECT_ROOT / "00_pipeline/00_tool/run_full_pipeline_master.sh"
CONFIG_ENV = PROJECT_ROOT / "00_pipeline/00_tool/pipeline_s3_config.env"
PREPARE_PY = PROJECT_ROOT / "80-8_portal_s3_prepare/00_tool/portal_s3_prepare.py"

_SPEC = importlib.util.spec_from_file_location(
    "upload_mail_master_private_s3", str(Path(__file__).resolve().parent / "upload_mail_master_private_s3.py")
)
up = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(up)

VALID_RUN_DATE = "20260818"
EXPECTED_KEY = "pipeline_ses_steps/private/mail_master/20260818/fetch_gmail_mail_master.jsonl"
EXPECTED_URI = "s3://technoverse/" + EXPECTED_KEY


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def make_step_dir(tmp: str, records: int = 2, filename: str = up.MAIL_MASTER_FILENAME) -> Path:
    step_dir = Path(tmp) / "01-1_fetch_gmail"
    (step_dir / "01_result").mkdir(parents=True)
    path = step_dir / "01_result" / filename
    with open(path, "w", encoding="utf-8") as f:
        for i in range(records):
            f.write(json.dumps({"message_id": "mid{0}".format(i), "subject": "s"}) + "\n")
    return step_dir


class FakeS3Client:
    def __init__(self, content_length=None, error=None):
        self.content_length = content_length
        self.error = error
        self.calls = []

    def head_object(self, Bucket, Key):  # noqa: N803 - boto3互換シグネチャ
        self.calls.append((Bucket, Key))
        if self.error is not None:
            raise self.error
        return {"ContentLength": self.content_length, "ETag": '"dummy-etag"'}


def fake_completed(returncode: int = 0, output: bytes = b""):
    return subprocess.CompletedProcess(args=["dummy"], returncode=returncode, stdout=output)


# ---------------------------------------------------------------------------
# ① destination安全ロック
# ---------------------------------------------------------------------------


class TestDestinationLock(unittest.TestCase):
    def test_expected_values_pass(self):
        up.lock_destination(
            "technoverse",
            "pipeline_ses_steps",
            "pipeline_ses_steps/private",
            "pipeline_ses_steps/private/mail_master",
        )

    def test_rejected_destinations(self):
        cases = [
            ("空prefix", "technoverse", "pipeline_ses_steps", "", ""),
            ("スラッシュのみ", "technoverse", "pipeline_ses_steps", "/", "/"),
            ("親参照", "technoverse", "pipeline_ses_steps", "pipeline_ses_steps/private",
             "pipeline_ses_steps/private/../mail_master"),
            ("別bucket", "other-bucket", "pipeline_ses_steps", "pipeline_ses_steps/private",
             "pipeline_ses_steps/private/mail_master"),
            ("Portal prefix", "technoverse", "pipeline_ses_steps", "pipeline_ses_steps/private",
             "pipeline_ses_steps/pipeline_ses_steps"),
            ("bucket root", "technoverse", "pipeline_ses_steps", "", ""),
            ("base直下", "technoverse", "pipeline_ses_steps", "pipeline_ses_steps", "pipeline_ses_steps"),
            ("末尾スラッシュ", "technoverse", "pipeline_ses_steps", "pipeline_ses_steps/private/",
             "pipeline_ses_steps/private/mail_master/"),
            ("absolute相当", "technoverse", "pipeline_ses_steps", "/pipeline_ses_steps/private",
             "/pipeline_ses_steps/private/mail_master"),
            ("非str", "technoverse", "pipeline_ses_steps", "pipeline_ses_steps/private", None),
        ]
        for label, bucket, base, private, mail_master in cases:
            with self.subTest(label):
                with self.assertRaises(up.UploadError):
                    up.lock_destination(bucket, base, private, mail_master)

    def test_run_date_valid(self):
        self.assertEqual(up.validate_run_date(VALID_RUN_DATE), VALID_RUN_DATE)

    def test_run_date_invalid(self):
        for raw in ("", "/", "..", "2026818", "202608181", "abcdefgh", "2026/08/18",
                    "20261301", "20260230", " 20260818", "20260818\n", "２０２６０８１８", None, 20260818):
            with self.subTest(repr(raw)):
                with self.assertRaises(up.UploadError):
                    up.validate_run_date(raw)

    def test_object_key_and_uri(self):
        key = up.build_object_key(up.EXPECTED_MAIL_MASTER_PREFIX, VALID_RUN_DATE)
        self.assertEqual(key, EXPECTED_KEY)
        self.assertEqual(up.build_destination_uri("technoverse", key, VALID_RUN_DATE), EXPECTED_URI)

    def test_object_key_rejects_tampered_prefix(self):
        for prefix in ("pipeline_ses_steps", "pipeline_ses_steps/pipeline_ses_steps",
                       "pipeline_ses_steps/private", "pipeline_ses_steps/private/mail_master/extra", ""):
            with self.subTest(prefix):
                with self.assertRaises(up.UploadError):
                    up.build_object_key(prefix, VALID_RUN_DATE)

    def test_destination_uri_rejects_other_bucket(self):
        key = up.build_object_key(up.EXPECTED_MAIL_MASTER_PREFIX, VALID_RUN_DATE)
        with self.assertRaises(up.UploadError):
            up.build_destination_uri("other-bucket", key, VALID_RUN_DATE)

    def test_run_id(self):
        self.assertEqual(up.validate_run_id("run-01"), ("run-01", "provided"))
        self.assertEqual(up.validate_run_id(""), ("unset", "default"))
        for raw in ("bad id", "a,b", "a=b", "-lead"):
            with self.subTest(raw):
                with self.assertRaises(up.UploadError):
                    up.validate_run_id(raw)


# ---------------------------------------------------------------------------
# ② local file検査
# ---------------------------------------------------------------------------


class TestLocalValidation(unittest.TestCase):
    def test_valid_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            path = up.expected_mail_master_path(step_dir)
            stat_result = up.validate_local_file(path, path)
            self.assertGreater(stat_result["size"], 0)
            scan = up.scan_mail_master(path)
            self.assertEqual(scan["record_count"], 2)

    def test_missing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = Path(tmp) / "01-1_fetch_gmail"
            (step_dir / "01_result").mkdir(parents=True)
            path = up.expected_mail_master_path(step_dir)
            with self.assertRaises(up.UploadError):
                up.validate_local_file(path, path)

    def test_symlink_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp, filename="real_master.jsonl")
            path = up.expected_mail_master_path(step_dir)
            os.symlink(str(step_dir / "01_result" / "real_master.jsonl"), str(path))
            with self.assertRaises(up.UploadError):
                up.validate_local_file(path, path)

    def test_empty_file_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp, records=0)
            path = up.expected_mail_master_path(step_dir)
            with self.assertRaises(up.UploadError):
                up.validate_local_file(path, path)

    def test_zero_record_file_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp, records=0)
            path = up.expected_mail_master_path(step_dir)
            with open(path, "w", encoding="utf-8") as f:
                f.write("\n\n")
            up.validate_local_file(path, path)
            with self.assertRaises(up.UploadError):
                up.scan_mail_master(path)

    def test_empty_message_id_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp, records=0)
            path = up.expected_mail_master_path(step_dir)
            with open(path, "w", encoding="utf-8") as f:
                f.write(json.dumps({"message_id": ""}) + "\n")
            with self.assertRaises(up.UploadError):
                up.scan_mail_master(path)

    def test_path_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            path = up.expected_mail_master_path(step_dir)
            other = step_dir / "01_result" / "other.jsonl"
            other.write_text("{}\n", encoding="utf-8")
            with self.assertRaises(up.UploadError):
                up.validate_local_file(other, path)


# ---------------------------------------------------------------------------
# ③ aws s3 cp argv
# ---------------------------------------------------------------------------


class TestUploadArgv(unittest.TestCase):
    def build(self, dry_run=False):
        return up.build_cp_argv(
            Path("/tmp/x/fetch_gmail_mail_master.jsonl"),
            EXPECTED_URI,
            "ap-northeast-1",
            dry_run,
            "run-date=20260818,run-id=unset,record-count=2919",
        )

    def test_argv_shape(self):
        argv = self.build()
        self.assertEqual(argv[:3], [up.AWS_BIN, "s3", "cp"])
        self.assertEqual(argv[3], "/tmp/x/fetch_gmail_mail_master.jsonl")
        self.assertEqual(argv[4], EXPECTED_URI)
        self.assertIn("--region", argv)
        self.assertIn("ap-northeast-1", argv)
        self.assertNotIn("--dryrun", argv)

    def test_argv_has_no_dangerous_options(self):
        argv = self.build()
        for token in ("sync", "mv", "rm", "--recursive", "--delete", "--include", "--exclude"):
            self.assertNotIn(token, argv[1:2] + argv[5:])
        for token in argv:
            self.assertNotIn("*", token)
            self.assertNotIn("?", token)

    def test_dry_run_argv(self):
        self.assertIn("--dryrun", self.build(dry_run=True))

    def test_argv_guard_rejects_wrong_destination(self):
        with self.assertRaises(up.UploadError):
            up.assert_safe_argv(
                [up.AWS_BIN, "s3", "cp", "/tmp/x", "s3://technoverse/pipeline_ses_steps/pipeline_ses_steps/x"],
                Path("/tmp/x"),
                EXPECTED_URI,
            )

    def test_argv_guard_rejects_recursive(self):
        with self.assertRaises(up.UploadError):
            up.assert_safe_argv(
                [up.AWS_BIN, "s3", "cp", "/tmp/x", EXPECTED_URI, "--recursive"],
                Path("/tmp/x"),
                EXPECTED_URI,
            )


# ---------------------------------------------------------------------------
# ④⑤ run / main の異常系・冪等性
# ---------------------------------------------------------------------------


class TestRun(unittest.TestCase):
    def _args(self, step_dir, dry_run=False, run_date=VALID_RUN_DATE):
        return up.parse_args(
            ["--run-date", run_date, "--step-dir", str(step_dir)] + (["--dry-run"] if dry_run else [])
        )

    def test_success_with_exact_size(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            size = up.expected_mail_master_path(step_dir).stat().st_size
            client = FakeS3Client(content_length=size)
            with mock.patch.object(up.subprocess, "run", return_value=fake_completed()) as run_mock, \
                    mock.patch.object(up, "build_s3_client", return_value=client):
                summary = up.run(self._args(step_dir), up.get_logger("test"))
            self.assertTrue(summary["verified"])
            self.assertEqual(summary["s3_key"], EXPECTED_KEY)
            self.assertEqual(summary["s3_bytes"], size)
            self.assertEqual(summary["local_bytes"], size)
            self.assertEqual(client.calls, [("technoverse", EXPECTED_KEY)])
            argv = run_mock.call_args[0][0]
            self.assertEqual(argv[4], EXPECTED_URI)
            self.assertFalse(run_mock.call_args[1]["shell"])

    def test_upload_subprocess_failure_exits_non_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            with mock.patch.object(up.subprocess, "run", return_value=fake_completed(1, b"err")), \
                    mock.patch.object(sys, "argv", ["x", "--run-date", VALID_RUN_DATE, "--step-dir", str(step_dir)]):
                self.assertEqual(up.main(), 1)
            summary = json.loads((step_dir / "01_result" / up.SUMMARY_FILENAME).read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "FAILED")
            self.assertFalse(summary["verified"])

    def test_head_object_failure_exits_non_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            client = FakeS3Client(error=RuntimeError("404"))
            with mock.patch.object(up.subprocess, "run", return_value=fake_completed()), \
                    mock.patch.object(up, "build_s3_client", return_value=client), \
                    mock.patch.object(sys, "argv", ["x", "--run-date", VALID_RUN_DATE, "--step-dir", str(step_dir)]):
                self.assertEqual(up.main(), 1)

    def test_size_mismatch_exits_non_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            client = FakeS3Client(content_length=1)
            with mock.patch.object(up.subprocess, "run", return_value=fake_completed()), \
                    mock.patch.object(up, "build_s3_client", return_value=client), \
                    mock.patch.object(sys, "argv", ["x", "--run-date", VALID_RUN_DATE, "--step-dir", str(step_dir)]):
                self.assertEqual(up.main(), 1)

    def test_invalid_run_date_exits_non_zero_without_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            with mock.patch.object(up.subprocess, "run") as run_mock, \
                    mock.patch.object(sys, "argv", ["x", "--run-date", "..", "--step-dir", str(step_dir)]):
                self.assertEqual(up.main(), 1)
            run_mock.assert_not_called()

    def test_missing_local_file_exits_non_zero_without_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = Path(tmp) / "01-1_fetch_gmail"
            (step_dir / "01_result").mkdir(parents=True)
            with mock.patch.object(up.subprocess, "run") as run_mock, \
                    mock.patch.object(sys, "argv", ["x", "--run-date", VALID_RUN_DATE, "--step-dir", str(step_dir)]):
                self.assertEqual(up.main(), 1)
            run_mock.assert_not_called()

    def test_dry_run_does_not_verify(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            with mock.patch.object(up.subprocess, "run", return_value=fake_completed()) as run_mock, \
                    mock.patch.object(up, "build_s3_client") as client_mock:
                summary = up.run(self._args(step_dir, dry_run=True), up.get_logger("test"))
            self.assertIn("--dryrun", run_mock.call_args[0][0])
            client_mock.assert_not_called()
            self.assertFalse(summary["verified"])
            self.assertEqual(summary["mode"], "dry-run")

    def test_idempotent_same_run_date_same_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            size = up.expected_mail_master_path(step_dir).stat().st_size
            keys = []
            for _ in range(2):
                client = FakeS3Client(content_length=size)
                with mock.patch.object(up.subprocess, "run", return_value=fake_completed()), \
                        mock.patch.object(up, "build_s3_client", return_value=client):
                    summary = up.run(self._args(step_dir), up.get_logger("test"))
                keys.append(summary["s3_key"])
                self.assertTrue(summary["verified"])
            self.assertEqual(keys, [EXPECTED_KEY, EXPECTED_KEY])

    def test_summary_has_no_mail_content(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            size = up.expected_mail_master_path(step_dir).stat().st_size
            client = FakeS3Client(content_length=size)
            with mock.patch.object(up.subprocess, "run", return_value=fake_completed()), \
                    mock.patch.object(up, "build_s3_client", return_value=client):
                summary = up.run(self._args(step_dir), up.get_logger("test"))
            text = json.dumps(summary, ensure_ascii=False)
            # mail本文・message_id値・credential等が混入していないこと
            # （集計値である empty_message_id_count / record_count は含まれてよい）
            for token in ("mid0", "mid1", "body_text", "subject", "credential", "refresh_token"):
                self.assertNotIn(token, text)


# ---------------------------------------------------------------------------
# ⑥ runner組込み / 設定
# ---------------------------------------------------------------------------


class TestRunnerWiring(unittest.TestCase):
    def setUp(self):
        self.text = RUNNER.read_text(encoding="utf-8")

    def test_runners_are_identical(self):
        self.assertEqual(sha256(RUNNER), sha256(RUNNER_MASTER))

    def test_upload_runs_between_01_1_and_01_2(self):
        markers = [
            "01-1_fetch_gmail/00_tool/fetch_gmail.py",
            "01-1_fetch_gmail/00_tool/upload_mail_master_private_s3.py",
            "01-2_remove_duplicate_emails/00_tool/remove_duplicate_emails.py",
        ]
        positions = []
        for marker in markers:
            index = self.text.find(marker)
            self.assertNotEqual(index, -1, msg="runnerに {0} がありません".format(marker))
            positions.append(index)
        self.assertEqual(positions, sorted(positions), msg="順序が不正です: {0}".format(positions))

    def test_upload_uses_run_step_with_run_date(self):
        lines = [l for l in self.text.splitlines() if "upload_mail_master_private_s3.py" in l]
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith("run_step "))
        self.assertIn('--run-date "$RUN_DATE"', lines[0])

    def test_config_has_private_prefixes_without_duplicating_bucket(self):
        text = CONFIG_ENV.read_text(encoding="utf-8")
        self.assertEqual(text.count("PIPELINE_S3_BUCKET:="), 1)
        self.assertIn("${PIPELINE_S3_BASE_PREFIX}/private", text)
        self.assertIn("${PIPELINE_PRIVATE_PREFIX}/mail_master", text)

    def test_resolved_config_values(self):
        from common.pipeline_s3_env import load_pipeline_s3_config

        config = load_pipeline_s3_config()
        self.assertEqual(config["PIPELINE_S3_BUCKET"], "technoverse")
        self.assertEqual(config["PIPELINE_PRIVATE_PREFIX"], "pipeline_ses_steps/private")
        self.assertEqual(config["MAIL_MASTER_S3_PREFIX"], "pipeline_ses_steps/private/mail_master")
        # Portal prefix と混ざっていないこと
        self.assertNotEqual(config["MAIL_MASTER_S3_PREFIX"], config["PORTAL_S3_PREFIX"])


# ---------------------------------------------------------------------------
# ⑦ regression
# ---------------------------------------------------------------------------


class TestRegression(unittest.TestCase):
    def test_80_8_still_excludes_mail_master(self):
        text = PREPARE_PY.read_text(encoding="utf-8")
        self.assertIn('"01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl"', text)

    def test_frozen_files_have_no_diff(self):
        if not (GIT_ROOT / ".git").is_dir():
            self.skipTest("_src が存在しないため比較をスキップ: {0}".format(GIT_ROOT))
        frozen = (
            "01-1_fetch_gmail/00_tool/fetch_gmail.py",
            "80-8_portal_s3_prepare/00_tool/portal_s3_prepare.py",
            "80-9_portal_s3_sync/00_tool/portal_s3_sync.py",
        )
        diffs = []
        for relative in frozen:
            counterpart = GIT_ROOT / relative
            if not counterpart.is_file():
                diffs.append("_srcに存在しない: {0}".format(relative))
                continue
            if sha256(PROJECT_ROOT / relative) != sha256(counterpart):
                diffs.append("差分あり: {0}".format(relative))
        self.assertEqual(diffs, [], msg="凍結領域に差分があります: {0}".format(diffs[:3]))


if __name__ == "__main__":
    unittest.main(verbosity=2)

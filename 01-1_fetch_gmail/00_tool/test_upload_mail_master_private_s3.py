#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
01-1 mail master private S3 upload の focused test

AWSへの実writeは行わない（subprocess / boto3 clientはすべてmock）。
確認内容:
  ① source安全ロック（production sourceのみ / 別dir / 別filename / symlink / CLIで差し替え不可）
  ② destination安全ロック（RUN_DATE / prefix / bucket / Portal prefix / 上位prefix / `..`）
  ③ subprocess直前の最終ロック（run_upload()へ任意destinationを渡しても実行されないこと）
  ④ aws s3 cp のargv（recursive / sync / delete / wildcardが無いこと）
  ⑤ upload失敗 / head-object失敗 / size mismatch で非0終了すること
  ⑥ 冪等性（同一RUN_DATE再実行で同一key）
  ⑦ confirm（bucket一致 / 不正bucket / 実在しないRUN_DATE）
  ⑧ runner組込み（01-1 → private upload → 01-2 / run_step経由 / 2runner一致）
  ⑨ regression（frozen fileをbaseline commit由来の固定digestと比較する）

実行:
  python3 01-1_fetch_gmail/00_tool/test_upload_mail_master_private_s3.py
"""

import contextlib
import hashlib
import importlib.util
import io
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
SYNC_PY = PROJECT_ROOT / "80-9_portal_s3_sync/00_tool/portal_s3_sync.py"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    # confirm側の `from upload_mail_master_private_s3 import ...` と同一instanceを共有させる
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


up = load_module("upload_mail_master_private_s3", Path(__file__).resolve().parent / "upload_mail_master_private_s3.py")
confirm_mod = load_module(
    "confirm_mail_master_private_s3", STEP_DIR / "02_confirm" / "confirm_mail_master_private_s3.py"
)

VALID_RUN_DATE = "20260818"
EXPECTED_KEY = "pipeline_ses_steps/private/mail_master/20260818/fetch_gmail_mail_master.jsonl"
EXPECTED_URI = "s3://technoverse/" + EXPECTED_KEY

# ---- regression用 baseline -------------------------------------------------
# 下記digestは baseline commit dc9ea70（本対応の直前commit）のfile内容。
# 「今回変更しない」と宣言した領域が実際に変わっていないことを検知するための固定値であり、
# 意図的に変更する場合のみ、変更理由と影響範囲を明示したうえで更新する。
BASELINE_COMMIT = "dc9ea70"
FROZEN_SHA256 = {
    "01-1_fetch_gmail/00_tool/fetch_gmail.py":
        "f4cf9a6e632abbd81807a37d3678fc1d18e09d6e7a0937666b9b3d26360bd44d",
    "80-8_portal_s3_prepare/00_tool/portal_s3_prepare.py":
        "db45a0eb8893328b7572c47f39a09f91acad0dcb8001f02702b96113eceb7e74",
    "80-9_portal_s3_sync/00_tool/portal_s3_sync.py":
        "2cf99ed1118fc356c4f14086256da68f3952a350923430a72bb322c232519361",
}
# 01-1の起動行はGmail取得ロジックの入口。意図しない変更を検知するため固定する。
FETCH_GMAIL_RUN_STEP_LINE = (
    'run_step "01-1_fetch_gmail" "$ROOT/01-1_fetch_gmail/00_tool/fetch_gmail.py" '
    '--after "$FETCH_AFTER" --before "$FETCH_BEFORE" --max 3000'
)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def make_step_dir(tmp: str, records: int = 2, filename: str = up.MAIL_MASTER_FILENAME) -> Path:
    """canonical sourceと同じ階層構造（<step>/01_result/<file>）のtmp stepディレクトリを作る。"""
    step_dir = Path(tmp).resolve() / up.PRODUCTION_STEP_DIR_NAME
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


def canonical_argv(source: str, destination: str = EXPECTED_URI):
    return [
        up.AWS_BIN, "s3", "cp", source, destination,
        "--only-show-errors", "--region", "ap-northeast-1",
        "--metadata", "run-date=20260818,run-id=unset,record-count=2",
    ]


# ---------------------------------------------------------------------------
# ① source安全ロック
# ---------------------------------------------------------------------------


class TestSourceLock(unittest.TestCase):
    def test_canonical_source_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            with mock.patch.object(up, "_STEP_DIR", step_dir):
                path = up.canonical_source_path()
                self.assertEqual(path, step_dir / "01_result" / up.MAIL_MASTER_FILENAME)
                self.assertGreater(up.validate_local_file(path)["size"], 0)

    def test_arbitrary_step_dir_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as other_tmp:
            step_dir = make_step_dir(tmp)
            other_step_dir = make_step_dir(other_tmp)
            other_source = other_step_dir / "01_result" / up.MAIL_MASTER_FILENAME
            with mock.patch.object(up, "_STEP_DIR", step_dir):
                with self.assertRaises(up.UploadError):
                    up.validate_local_file(other_source)
                with self.assertRaises(up.UploadError):
                    up.assert_canonical_source_path(other_source)

    def test_production_cli_has_no_step_dir_option(self):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as ctx:
                up.parse_args(["--run-date", VALID_RUN_DATE, "--step-dir", "/tmp/attacker"])
        self.assertEqual(ctx.exception.code, 2)
        self.assertNotIn("--step-dir", up.parse_args(["--run-date", VALID_RUN_DATE]).__dict__)

    def test_wrong_filename_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp, filename="other_master.jsonl")
            with mock.patch.object(up, "_STEP_DIR", step_dir):
                with self.assertRaises(up.UploadError):
                    up.validate_local_file(step_dir / "01_result" / "other_master.jsonl")

    def test_symlink_source_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp, filename="real_master.jsonl")
            source = step_dir / "01_result" / up.MAIL_MASTER_FILENAME
            os.symlink(str(step_dir / "01_result" / "real_master.jsonl"), str(source))
            with mock.patch.object(up, "_STEP_DIR", step_dir):
                with self.assertRaises(up.UploadError):
                    up.validate_local_file(source)

    def test_symlinked_result_dir_is_rejected(self):
        """01_resultをsymlinkにして正規pathに見せる経路も拒否する。"""
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = Path(tmp).resolve() / up.PRODUCTION_STEP_DIR_NAME
            real_dir = step_dir / "real_result"
            real_dir.mkdir(parents=True)
            (real_dir / up.MAIL_MASTER_FILENAME).write_text('{"message_id":"m"}\n', encoding="utf-8")
            os.symlink(str(real_dir), str(step_dir / "01_result"))
            with mock.patch.object(up, "_STEP_DIR", step_dir):
                with self.assertRaises(up.UploadError):
                    up.validate_local_file(up.canonical_source_path())

    def test_missing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = Path(tmp).resolve() / up.PRODUCTION_STEP_DIR_NAME
            (step_dir / "01_result").mkdir(parents=True)
            with mock.patch.object(up, "_STEP_DIR", step_dir):
                with self.assertRaises(up.UploadError):
                    up.validate_local_file(up.canonical_source_path())

    def test_empty_file_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp, records=0)
            with mock.patch.object(up, "_STEP_DIR", step_dir):
                with self.assertRaises(up.UploadError):
                    up.validate_local_file(up.canonical_source_path())

    def test_zero_record_and_empty_message_id_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp, records=0)
            path = step_dir / "01_result" / up.MAIL_MASTER_FILENAME
            path.write_text("\n\n", encoding="utf-8")
            with mock.patch.object(up, "_STEP_DIR", step_dir):
                up.validate_local_file(path)
                with self.assertRaises(up.UploadError):
                    up.scan_mail_master(path)
                path.write_text(json.dumps({"message_id": ""}) + "\n", encoding="utf-8")
                with self.assertRaises(up.UploadError):
                    up.scan_mail_master(path)


# ---------------------------------------------------------------------------
# ② destination安全ロック
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

    def test_rebuild_expected_destination(self):
        expected = up.rebuild_expected_destination(VALID_RUN_DATE)
        self.assertEqual(expected["bucket"], "technoverse")
        self.assertEqual(expected["key"], EXPECTED_KEY)
        self.assertEqual(expected["uri"], EXPECTED_URI)

    def test_rebuild_rejects_invalid_run_date(self):
        for raw in ("20260230", "..", "", "2026081"):
            with self.subTest(raw):
                with self.assertRaises(up.UploadError):
                    up.rebuild_expected_destination(raw)

    def test_run_id(self):
        self.assertEqual(up.validate_run_id("run-01"), ("run-01", "provided"))
        self.assertEqual(up.validate_run_id(""), ("unset", "default"))
        for raw in ("bad id", "a,b", "a=b", "-lead"):
            with self.subTest(raw):
                with self.assertRaises(up.UploadError):
                    up.validate_run_id(raw)


# ---------------------------------------------------------------------------
# ③ subprocess直前の最終ロック（低レベル関数を直接呼んでも拒否されること）
# ---------------------------------------------------------------------------


class TestFinalUploadLock(unittest.TestCase):
    @contextlib.contextmanager
    def _tmp_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            with mock.patch.object(up, "_STEP_DIR", step_dir):
                yield str(step_dir / "01_result" / up.MAIL_MASTER_FILENAME)

    def test_canonical_argv_is_executed(self):
        with self._tmp_source() as source:
            with mock.patch.object(up.subprocess, "run", return_value=fake_completed()) as run_mock:
                expected = up.run_upload(canonical_argv(source), VALID_RUN_DATE, up.get_logger("test"))
            run_mock.assert_called_once()
            self.assertEqual(expected["uri"], EXPECTED_URI)

    def test_unsafe_destinations_are_rejected(self):
        cases = [
            ("bucket root", "s3://technoverse/"),
            ("bucket rootのみ", "s3://technoverse"),
            ("base prefix root", "s3://technoverse/pipeline_ses_steps/"),
            ("Portal prefix", "s3://technoverse/pipeline_ses_steps/pipeline_ses_steps/"
                              "20260818/fetch_gmail_mail_master.jsonl"),
            ("別bucket", "s3://other-bucket/pipeline_ses_steps/private/mail_master/"
                         "20260818/fetch_gmail_mail_master.jsonl"),
            ("別RUN_DATE", "s3://technoverse/pipeline_ses_steps/private/mail_master/"
                           "20250101/fetch_gmail_mail_master.jsonl"),
            ("別filename", "s3://technoverse/pipeline_ses_steps/private/mail_master/"
                           "20260818/other.jsonl"),
            ("親参照", "s3://technoverse/pipeline_ses_steps/private/mail_master/../"
                       "20260818/fetch_gmail_mail_master.jsonl"),
            ("別private prefix", "s3://technoverse/pipeline_ses_steps/private/other/"
                                 "20260818/fetch_gmail_mail_master.jsonl"),
            ("空", ""),
            ("任意destination", "s3://attacker-bucket/anything"),
            ("local path", "/tmp/attacker/out.jsonl"),
        ]
        with self._tmp_source() as source:
            for label, destination in cases:
                with self.subTest(label):
                    with mock.patch.object(up.subprocess, "run") as run_mock:
                        with self.assertRaises(up.UploadError):
                            up.run_upload(
                                canonical_argv(source, destination), VALID_RUN_DATE, up.get_logger("test")
                            )
                        run_mock.assert_not_called()

    def test_unsafe_source_is_rejected(self):
        with tempfile.TemporaryDirectory() as other_tmp:
            other = str(make_step_dir(other_tmp) / "01_result" / up.MAIL_MASTER_FILENAME)
            with self._tmp_source():
                with mock.patch.object(up.subprocess, "run") as run_mock:
                    with self.assertRaises(up.UploadError):
                        up.run_upload(canonical_argv(other), VALID_RUN_DATE, up.get_logger("test"))
                    run_mock.assert_not_called()

    def test_invalid_run_date_blocks_upload(self):
        with self._tmp_source() as source:
            for raw in ("20260230", "..", "", "20260818/../20250101"):
                with self.subTest(raw):
                    with mock.patch.object(up.subprocess, "run") as run_mock:
                        with self.assertRaises(up.UploadError):
                            up.run_upload(canonical_argv(source), raw, up.get_logger("test"))
                        run_mock.assert_not_called()

    def test_forbidden_options_block_upload(self):
        with self._tmp_source() as source:
            for extra in (["--recursive"], ["--delete"], ["--include", "x"], ["--exclude", "x"]):
                with self.subTest(str(extra)):
                    with mock.patch.object(up.subprocess, "run") as run_mock:
                        with self.assertRaises(up.UploadError):
                            up.run_upload(
                                canonical_argv(source) + extra, VALID_RUN_DATE, up.get_logger("test")
                            )
                        run_mock.assert_not_called()

    def test_sync_command_blocks_upload(self):
        with self._tmp_source() as source:
            argv = canonical_argv(source)
            argv[2] = "sync"
            with mock.patch.object(up.subprocess, "run") as run_mock:
                with self.assertRaises(up.UploadError):
                    up.run_upload(argv, VALID_RUN_DATE, up.get_logger("test"))
                run_mock.assert_not_called()

    def test_wildcard_blocks_upload(self):
        with self._tmp_source() as source:
            argv = canonical_argv(source)
            argv[3] = str(Path(source).parent / "*.jsonl")
            with mock.patch.object(up.subprocess, "run") as run_mock:
                with self.assertRaises(up.UploadError):
                    up.run_upload(argv, VALID_RUN_DATE, up.get_logger("test"))
                run_mock.assert_not_called()


# ---------------------------------------------------------------------------
# ④ argv構造
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


# ---------------------------------------------------------------------------
# ⑤⑥ run / main の異常系・冪等性
# ---------------------------------------------------------------------------


class TestRun(unittest.TestCase):
    def _args(self, dry_run=False, run_date=VALID_RUN_DATE):
        return up.parse_args(["--run-date", run_date] + (["--dry-run"] if dry_run else []))

    def test_success_with_exact_size(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            size = (step_dir / "01_result" / up.MAIL_MASTER_FILENAME).stat().st_size
            client = FakeS3Client(content_length=size)
            with mock.patch.object(up, "_STEP_DIR", step_dir), \
                    mock.patch.object(up.subprocess, "run", return_value=fake_completed()) as run_mock, \
                    mock.patch.object(up, "build_s3_client", return_value=client):
                summary = up.run(self._args(), up.get_logger("test"))
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
            with mock.patch.object(up, "_STEP_DIR", step_dir), \
                    mock.patch.object(up.subprocess, "run", return_value=fake_completed(1, b"err")), \
                    mock.patch.object(sys, "argv", ["x", "--run-date", VALID_RUN_DATE]):
                self.assertEqual(up.main(), 1)
            summary = json.loads((step_dir / "01_result" / up.SUMMARY_FILENAME).read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "FAILED")
            self.assertFalse(summary["verified"])

    def test_head_object_failure_exits_non_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            client = FakeS3Client(error=RuntimeError("404"))
            with mock.patch.object(up, "_STEP_DIR", step_dir), \
                    mock.patch.object(up.subprocess, "run", return_value=fake_completed()), \
                    mock.patch.object(up, "build_s3_client", return_value=client), \
                    mock.patch.object(sys, "argv", ["x", "--run-date", VALID_RUN_DATE]):
                self.assertEqual(up.main(), 1)

    def test_size_mismatch_exits_non_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            client = FakeS3Client(content_length=1)
            with mock.patch.object(up, "_STEP_DIR", step_dir), \
                    mock.patch.object(up.subprocess, "run", return_value=fake_completed()), \
                    mock.patch.object(up, "build_s3_client", return_value=client), \
                    mock.patch.object(sys, "argv", ["x", "--run-date", VALID_RUN_DATE]):
                self.assertEqual(up.main(), 1)

    def test_invalid_run_date_exits_non_zero_without_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            with mock.patch.object(up, "_STEP_DIR", step_dir), \
                    mock.patch.object(up.subprocess, "run") as run_mock, \
                    mock.patch.object(sys, "argv", ["x", "--run-date", ".."]):
                self.assertEqual(up.main(), 1)
            run_mock.assert_not_called()

    def test_missing_local_file_exits_non_zero_without_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = Path(tmp).resolve() / up.PRODUCTION_STEP_DIR_NAME
            (step_dir / "01_result").mkdir(parents=True)
            with mock.patch.object(up, "_STEP_DIR", step_dir), \
                    mock.patch.object(up.subprocess, "run") as run_mock, \
                    mock.patch.object(sys, "argv", ["x", "--run-date", VALID_RUN_DATE]):
                self.assertEqual(up.main(), 1)
            run_mock.assert_not_called()

    def test_dry_run_does_not_verify(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            with mock.patch.object(up, "_STEP_DIR", step_dir), \
                    mock.patch.object(up.subprocess, "run", return_value=fake_completed()) as run_mock, \
                    mock.patch.object(up, "build_s3_client") as client_mock:
                summary = up.run(self._args(dry_run=True), up.get_logger("test"))
            self.assertIn("--dryrun", run_mock.call_args[0][0])
            client_mock.assert_not_called()
            self.assertFalse(summary["verified"])
            self.assertEqual(summary["mode"], "dry-run")

    def test_idempotent_same_run_date_same_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            size = (step_dir / "01_result" / up.MAIL_MASTER_FILENAME).stat().st_size
            keys = []
            for _ in range(2):
                client = FakeS3Client(content_length=size)
                with mock.patch.object(up, "_STEP_DIR", step_dir), \
                        mock.patch.object(up.subprocess, "run", return_value=fake_completed()), \
                        mock.patch.object(up, "build_s3_client", return_value=client):
                    summary = up.run(self._args(), up.get_logger("test"))
                keys.append(summary["s3_key"])
                self.assertTrue(summary["verified"])
            self.assertEqual(keys, [EXPECTED_KEY, EXPECTED_KEY])

    def test_summary_has_no_mail_content(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            size = (step_dir / "01_result" / up.MAIL_MASTER_FILENAME).stat().st_size
            client = FakeS3Client(content_length=size)
            with mock.patch.object(up, "_STEP_DIR", step_dir), \
                    mock.patch.object(up.subprocess, "run", return_value=fake_completed()), \
                    mock.patch.object(up, "build_s3_client", return_value=client):
                summary = up.run(self._args(), up.get_logger("test"))
            text = json.dumps(summary, ensure_ascii=False)
            # mail本文・message_id値・credential等が混入していないこと
            # （集計値である empty_message_id_count / record_count は含まれてよい）
            for token in ("mid0", "mid1", "body_text", "subject", "credential", "refresh_token"):
                self.assertNotIn(token, text)


# ---------------------------------------------------------------------------
# ⑦ confirm
# ---------------------------------------------------------------------------


class TestConfirm(unittest.TestCase):
    def _summary(self, step_dir, **overrides):
        source = step_dir / "01_result" / up.MAIL_MASTER_FILENAME
        size = source.stat().st_size
        summary = {
            "status": "SUCCEEDED",
            "mode": "apply",
            "run_date": VALID_RUN_DATE,
            "local_path": str(source),
            "local_bytes": size,
            "record_count": 2,
            "s3_bucket": "technoverse",
            "s3_key": EXPECTED_KEY,
            "s3_uri": EXPECTED_URI,
            "s3_bytes": size,
            "verified": True,
        }
        summary.update(overrides)
        return summary

    def test_correct_bucket_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            with mock.patch.object(up, "_STEP_DIR", step_dir):
                self.assertEqual(confirm_mod.confirm(self._summary(step_dir)), [])

    def test_wrong_bucket_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            with mock.patch.object(up, "_STEP_DIR", step_dir):
                errors = confirm_mod.confirm(self._summary(step_dir, s3_bucket="attacker-bucket"))
            self.assertTrue(any("s3_bucket" in message for message in errors))

    def test_impossible_run_date_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            bad_key = EXPECTED_KEY.replace(VALID_RUN_DATE, "20260230")
            with mock.patch.object(up, "_STEP_DIR", step_dir):
                errors = confirm_mod.confirm(
                    self._summary(step_dir, run_date="20260230", s3_key=bad_key)
                )
            self.assertTrue(any("run_date" in message for message in errors))

    def test_portal_key_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            portal_key = "pipeline_ses_steps/pipeline_ses_steps/{0}/{1}".format(
                VALID_RUN_DATE, up.MAIL_MASTER_FILENAME
            )
            with mock.patch.object(up, "_STEP_DIR", step_dir):
                errors = confirm_mod.confirm(self._summary(step_dir, s3_key=portal_key))
            self.assertTrue(any("s3_key" in message for message in errors))

    def test_size_mismatch_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            step_dir = make_step_dir(tmp)
            with mock.patch.object(up, "_STEP_DIR", step_dir):
                errors = confirm_mod.confirm(self._summary(step_dir, s3_bytes=1))
            self.assertTrue(any("s3_bytes" in message for message in errors))


# ---------------------------------------------------------------------------
# ⑧ runner組込み / 設定
# ---------------------------------------------------------------------------


class TestRunnerWiring(unittest.TestCase):
    def setUp(self):
        self.text = RUNNER.read_text(encoding="utf-8")

    def test_runners_are_identical(self):
        self.assertEqual(sha256_file(RUNNER), sha256_file(RUNNER_MASTER))

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
        self.assertNotIn("--step-dir", lines[0])

    def test_fetch_gmail_invocation_is_unchanged(self):
        self.assertIn(FETCH_GMAIL_RUN_STEP_LINE, self.text)

    def test_portal_steps_still_wired(self):
        for marker in ("80-8_portal_s3_prepare/00_tool/portal_s3_prepare.py",
                       "80-9_portal_s3_sync/00_tool/portal_s3_sync.py"):
            self.assertIn(marker, self.text)

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
# ⑨ regression（自己比較ではなく baseline由来の固定digestと比較する）
# ---------------------------------------------------------------------------


class TestRegression(unittest.TestCase):
    def test_frozen_files_match_baseline_digest(self):
        """workspaceの現在内容を baseline commit 由来の固定digestと比較する。"""
        diffs = []
        for relative, digest in sorted(FROZEN_SHA256.items()):
            actual = sha256_file(PROJECT_ROOT / relative)
            if actual != digest:
                diffs.append("{0} (actual={1})".format(relative, actual))
        self.assertEqual(diffs, [], msg="凍結領域が変更されています: {0}".format(diffs[:3]))

    def test_pinned_digests_come_from_baseline_commit(self):
        """
        固定digestが baseline commit の実内容であることを確認する。
        （固定値を現在fileから作り直した自己比較になっていないことの担保）
        """
        if not (GIT_ROOT / ".git").is_dir():
            self.skipTest("_src が存在しないため baseline比較をスキップ: {0}".format(GIT_ROOT))
        mismatches = []
        for relative, digest in sorted(FROZEN_SHA256.items()):
            completed = subprocess.run(
                ["git", "-C", str(GIT_ROOT), "show", "{0}:{1}".format(BASELINE_COMMIT, relative)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                check=False,
            )
            if completed.returncode != 0:
                self.skipTest("baseline commit {0} を参照できません".format(BASELINE_COMMIT))
            if sha256_bytes(completed.stdout) != digest:
                mismatches.append(relative)
        self.assertEqual(mismatches, [], msg="固定digestがbaselineと不一致: {0}".format(mismatches))

    def test_80_8_still_excludes_mail_master(self):
        text = PREPARE_PY.read_text(encoding="utf-8")
        self.assertIn('"01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl"', text)

    def test_80_9_destination_is_portal_only(self):
        text = SYNC_PY.read_text(encoding="utf-8")
        self.assertIn('EXPECTED_PORTAL_PREFIX = f"{EXPECTED_BASE_PREFIX}/{EXPECTED_PORTAL_LEAF}"', text)
        self.assertNotIn("private", text)
        self.assertNotIn("mail_master", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)

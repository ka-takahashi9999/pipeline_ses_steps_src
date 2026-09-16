"""Manual receipt / tail contract focused tests. All AWS clients and syncs are fake."""
import argparse
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from datetime import datetime, timezone

import test_portal_s3_backup_rotation as fixtures

target = fixtures.target


class S3:
    def __init__(self, inventory, status_key, status):
        self.objects = {}
        for prefix in (target.EXPECTED_CURRENT_PREFIX, target.EXPECTED_BACKUP_PREFIX):
            for path, size in inventory.items():
                self.objects[prefix + "/" + path] = self.object(prefix + "/" + path, size)
        self.objects[status_key] = self.object(status_key, 500)
        self.documents = {status_key: status}
        self.pages_override = None

    @staticmethod
    def object(key, size):
        return {"Key": key, "Size": size, "ETag": '"etag"',
                "LastModified": datetime(2026, 8, 20, tzinfo=timezone.utc)}

    def get_paginator(self, name):
        return self

    def paginate(self, Bucket, Prefix):
        if self.pages_override is not None:
            yield from self.pages_override
        else:
            objects = [v for k, v in sorted(self.objects.items()) if k.startswith(Prefix)]
            yield {"Contents": objects[:1], "IsTruncated": True, "NextContinuationToken": "next"}
            yield {"Contents": objects[1:], "IsTruncated": False}

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(json.dumps(self.documents[Key]).encode())}


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestManualReceipt(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.tmp = Path(self.temp.name)
        self.inventory = {"01-1_fetch_gmail/01_result/fetch_gmail.jsonl": 10,
                          "06-80_duplicate_proposal_check/01_result/dup.jsonl": 20,
                          "09-1_mail_display_format/01_result/mail_display_format_20260818/a.txt": 5}
        self.failed = fixtures.make_status_document(fixtures.PREV_RUN_DATE, fixtures.PREV_RUN_ID, "FAILED", 1)
        self.key = f"{target.EXPECTED_BASE_PREFIX}/pipeline-status/{fixtures.PREV_RUN_DATE}/{fixtures.PREV_RUN_ID}/status.json"
        self.s3 = S3(self.inventory, self.key, self.failed)
        self.spec = fixtures.make_failed_execution_spec(fixtures.PREV_RUN_DATE, fixtures.PREV_RUN_ID)
        self.sfn = fixtures.FakeStepFunctionsClient([self.spec])
        self.context = {"recovery_id": "manual-september11", "run_id": fixtures.PREV_RUN_ID,
                        "run_date": fixtures.PREV_RUN_DATE, "execution_arn": self.spec["executionArn"]}
        self.manifest = self.tmp / "manifest.jsonl"
        self.manifest.write_text("".join(json.dumps({"relative_path": p, "size": s}) + "\n"
                                         for p, s in self.inventory.items()))
        self.summary = fixtures.make_sync_summary(run_id_source="cli", run_date_source="cli",
                                                  manifest_path=str(self.manifest))
        self.summary["manifest_provenance"] = {k: self.summary[k] for k in
                                               ("run_id", "run_date", "run_id_source", "run_date_source")}
        self.receipt_dir_patch = patch.object(target, "RECEIPT_DIR", self.tmp / "receipts")
        self.receipt_dir_patch.start()
        self.addCleanup(self.receipt_dir_patch.stop)
        self.path = target.finalize_manual_receipt(self.context, self.summary, self.manifest, self.s3, self.sfn)
        self.receipt = json.loads(self.path.read_text())
        self.logger = Mock()

    def tearDown(self):
        self.temp.cleanup()

    def write_receipt(self, receipt):
        self.path.write_text(json.dumps(receipt) + "\n")

    def args(self):
        return argparse.Namespace(bootstrap=False, dry_run=False, recovery_run_date=None,
                                  recovery_run_id=None, current_run_id=None,
                                  manual_recovery_receipt=self.path, create_manual_receipt=False,
                                  manual_recovery_id="manual-new-publication",
                                  manual_source_run_id=self.context["run_id"],
                                  manual_source_run_date=self.context["run_date"],
                                  manual_source_execution_arn=self.context["execution_arn"])

    def rotate(self, args=None):
        with patch.object(target, "build_s3_client", return_value=self.s3), \
             patch.object(target, "build_stepfunctions_client", return_value=self.sfn), \
             patch.object(target, "guard_manual_lock"), \
             patch.object(target, "run_sync") as sync, patch.object(target.time, "sleep"):
            try:
                return target.run(args or self.args(), self.logger)
            except Exception:
                sync.assert_not_called()
                raise

    def test_valid_receipt_rotation_and_cli_provenance_preserved(self):
        result = self.rotate()
        self.assertTrue(result["verify"]["verified"])
        self.assertEqual(result["previous_current"]["run_id_source"], "cli")
        self.assertEqual(self.s3.documents[self.key], self.failed)
        self.assertEqual(json.loads(self.path.read_text()), self.receipt)

    def test_receipt_missing_rejected_before_sync(self):
        self.path.unlink()
        with self.assertRaises(Exception):
            self.rotate()

    def test_receipt_snapshots_and_identity_mutations_rejected(self):
        mutations = {
            "source_run": lambda r: r["source"].update(run_id="wrong-run"),
            "source_execution": lambda r: r["source"].update(execution_arn=self.spec["executionArn"] + "wrong"),
            "source_date": lambda r: r["source"].update(run_date="20260914"),
            "recovery_id": lambda r: r.update(recovery_id="different-id"),
            "summary_snapshot": lambda r: r.update(summary_snapshot=r["summary_snapshot"] + " "),
            "manifest_snapshot": lambda r: r.update(manifest_snapshot=r["manifest_snapshot"] + " "),
            "hash": lambda r: r.update(manifest_sha256="0" * 64),
            "destination": lambda r: r.update(destination="s3://other/"),
            "bk1_destination": lambda r: r.update(backup_destination="s3://other/"),
            "fingerprint": lambda r: next(iter(r["current_fingerprint"].values())).update(etag='"wrong"'),
            "bk1": lambda r: next(iter(r["bk1_fingerprint"].values())).update(size=7),
            "empty": lambda r: r.update(current_fingerprint={}),
            "flag": lambda r: r["current_verification"].update(list_complete=False),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                record = copy.deepcopy(self.receipt)
                mutate(record)
                self.write_receipt(record)
                with self.assertRaises(Exception):
                    self.rotate()

    def test_current_path_size_fingerprint_and_bk1_live_mismatch(self):
        original = copy.deepcopy(self.s3.objects)
        key = target.EXPECTED_CURRENT_PREFIX + "/" + next(iter(self.inventory))
        for kind in ("path", "size", "etag", "last_modified", "bk1"):
            with self.subTest(kind=kind):
                self.s3.objects = copy.deepcopy(original)
                if kind == "path":
                    item = self.s3.objects.pop(key)
                    item["Key"] = key + "-wrong"
                    self.s3.objects[item["Key"]] = item
                elif kind == "size":
                    self.s3.objects[key]["Size"] += 1
                elif kind == "etag":
                    self.s3.objects[key]["ETag"] = '"changed"'
                elif kind == "last_modified":
                    self.s3.objects[key]["LastModified"] = datetime.now(timezone.utc)
                else:
                    self.s3.objects[key.replace(target.EXPECTED_CURRENT_PREFIX, target.EXPECTED_BACKUP_PREFIX)]["Size"] += 1
                with self.assertRaises(target.RotationError):
                    self.rotate()

    def test_incomplete_list_rejected(self):
        for pages in ([], [{"Contents": []}], [{"IsTruncated": True}],
                      [{"IsTruncated": True, "NextContinuationToken": "a"}],
                      [{"IsTruncated": True, "NextContinuationToken": "a"},
                       {"IsTruncated": True, "NextContinuationToken": "a"}]):
            with self.subTest(pages=pages):
                self.s3.pages_override = pages
                with self.assertRaises(target.RotationError):
                    self.rotate()

    def test_failed_status_and_execution_identity_rechecked(self):
        for mutation in ("status", "history", "redrive"):
            with self.subTest(mutation=mutation):
                self.s3.documents[self.key] = copy.deepcopy(self.failed)
                spec = copy.deepcopy(self.spec)
                if mutation == "status":
                    self.s3.documents[self.key]["status"] = "SUCCEEDED"
                elif mutation == "history":
                    event = spec["events"][1]["stateExitedEventDetails"]
                    event["output"] = json.dumps({"run_id": "wrong", "run_date": fixtures.PREV_RUN_DATE})
                else:
                    spec["description"]["redriveCount"] = 1
                self.sfn = fixtures.FakeStepFunctionsClient([spec])
                with self.assertRaises(target.RotationError):
                    self.rotate()

    def test_receipt_id_is_immutable(self):
        with self.assertRaises(target.RotationError):
            target.finalize_manual_receipt(self.context, self.summary, self.manifest, self.s3, self.sfn)
        self.assertEqual(json.loads(self.path.read_text()), self.receipt)

    def test_snapshot_survives_original_files_replacement(self):
        self.manifest.write_text("new manifest from 20260914\n")
        self.assertTrue(self.rotate()["verify"]["verified"])

    def test_receipt_does_not_implicitly_enable_cli(self):
        with self.assertRaises(target.RotationError):
            target.validate_previous_sync_summary(self.summary)

    def test_partial_manual_context_rejected(self):
        args = self.args()
        args.manual_source_execution_arn = None
        with self.assertRaises(target.RotationError):
            self.rotate(args)

    def test_active_run_and_incomplete_status_list_rejected(self):
        target.guard_no_active_runs(self.s3, self.sfn)
        with self.assertRaises(target.RotationError):
            target.guard_no_active_runs(self.s3, self.sfn, self.context["run_id"])
        running = fixtures.make_execution_spec(fixtures.CURRENT_RUN_DATE, fixtures.CURRENT_RUN_ID)
        with self.assertRaises(target.RotationError):
            target.guard_no_active_runs(self.s3, fixtures.FakeStepFunctionsClient([self.spec, running]))
        self.s3.documents[self.key]["status"] = "RUNNING"
        with self.assertRaises(target.RotationError):
            target.guard_no_active_runs(self.s3, self.sfn)
        self.s3.pages_override = [{"IsTruncated": True, "NextContinuationToken": "unfinished"}]
        with self.assertRaises(target.RotationError):
            target.guard_no_active_runs(self.s3, self.sfn)

    def test_manual_lock_owner_and_identity(self):
        lock = self.tmp / "00_pipeline/01_result/run_full_pipeline.lock"
        lock.parent.mkdir(parents=True)
        lock.write_text(self.context["recovery_id"])
        with lock.open("r+") as stream, patch.object(target, "project_root", self.tmp), \
             patch.object(target.os, "fstat", return_value=lock.stat()), \
             patch.object(target.fcntl, "flock") as flock, \
             patch.dict(os.environ, {"RUN_ID": self.context["recovery_id"], "RUN_DATE": self.context["run_date"]}):
            target.guard_manual_lock(self.context, self.s3, self.sfn)
            flock.assert_called_once_with(9, target.fcntl.LOCK_EX | target.fcntl.LOCK_NB)
            lock.write_text("other-owner")
            with self.assertRaises(target.RotationError):
                target.guard_manual_lock(self.context, self.s3, self.sfn)

    def test_archive_receipt_main_never_overwrites_existing_summary(self):
        root = self.tmp / "archive-root"
        (root / "00_pipeline/01_result").mkdir(parents=True)
        sync_dir, prepare_dir = self.tmp / "sync-original", self.tmp / "prepare-original"
        (sync_dir / "01_result").mkdir(parents=True)
        (prepare_dir / "01_result").mkdir(parents=True)
        manifest = prepare_dir / "01_result" / target.PREVIOUS_MANIFEST_FILENAME
        manifest.write_text(self.manifest.read_text())
        summary = {**self.summary, "manifest_path": str(manifest)}
        original = sync_dir / "01_result" / target.SYNC_SUMMARY_FILENAME
        original.write_text(json.dumps(summary))
        sentinel = self.tmp / "rotation-original/01_result" / target.BACKUP_SUMMARY_FILENAME
        sentinel.parent.mkdir(parents=True)
        sentinel.write_text("existing failed rotation summary")
        args = self.args()
        args.create_manual_receipt, args.manual_recovery_receipt = True, None
        args.manual_recovery_id = "archive-new-id"
        args.sync_dir, args.prepare_dir, args.step_dir = str(sync_dir), str(prepare_dir), str(sentinel.parent.parent)
        with patch.object(target, "project_root", root), patch.object(target, "parse_args", return_value=args), \
             patch.object(target, "build_s3_client", return_value=self.s3), \
             patch.object(target, "build_stepfunctions_client", return_value=self.sfn), \
             patch.object(target, "run_sync") as sync:
            self.assertEqual(target.main(), 0)
            self.assertEqual(target.main(), 1)  # ID再利用も既存summaryを変更しない
            sync.assert_not_called()
        self.assertEqual(sentinel.read_text(), "existing failed rotation summary")
        self.assertEqual(json.loads(original.read_text()), summary)

    def test_manual_rotation_confirm_good_and_bad(self):
        summary = self.rotate()
        confirm = fixtures.confirm_target
        output = self.tmp / "rotation-summary.json"
        result = self.tmp / "confirm.txt"
        with patch.object(confirm, "BACKUP_SUMMARY_PATH", output), patch.object(confirm, "CONFIRM_RESULT", result):
            output.write_text(json.dumps(summary))
            confirm.main()
            self.assertIn("【結果】OK", result.read_text())
            summary["manual_recovery"]["receipt"]["manifest_sha256"] = "bad"
            output.write_text(json.dumps(summary))
            with self.assertRaises(SystemExit) as caught:
                confirm.main()
            self.assertEqual(caught.exception.code, 1)
            self.assertIn("【結果】NG", result.read_text())

    def test_managed_caller_keeps_execution_guard(self):
        args = self.args()
        for name in ("manual_recovery_id", "manual_source_run_id", "manual_source_run_date", "manual_source_execution_arn"):
            setattr(args, name, None)
        run_id, run_date = fixtures.CURRENT_RUN_ID, fixtures.CURRENT_RUN_DATE
        key = f"{target.EXPECTED_BASE_PREFIX}/pipeline-status/{run_date}/{run_id}/status.json"
        self.s3.objects[key] = S3.object(key, 500)
        self.s3.documents[key] = fixtures.make_status_document(run_date, run_id, "RUNNING", None)
        self.s3.documents[self.key]["updated_at"] = "2026-08-19T01:00:00Z"
        self.receipt["failed_status_snapshot"] = json.dumps(self.s3.documents[self.key])
        self.receipt["failed_status_sha256"] = target.sha256_text(self.receipt["failed_status_snapshot"])
        self.write_receipt(self.receipt)
        current_spec = fixtures.make_execution_spec(run_date, run_id)
        self.sfn = fixtures.FakeStepFunctionsClient([self.spec, current_spec])
        with patch.dict(os.environ, {"RUN_ID": run_id, "RUN_DATE": run_date}):
            result = self.rotate(args)
            self.assertEqual(result["current_execution_guard"]["validation_result"], "PASS")
            current_spec["description"]["redriveCount"] = 1
            with self.assertRaises(target.RotationError):
                self.rotate(args)

    def test_manual_publish_finalizes_receipt_and_confirm(self):
        sync = load_module(target.project_root / "80-9_portal_s3_sync/00_tool/portal_s3_sync.py", "sync_manual_test")
        root, prepare = self.tmp / "pipeline", self.tmp / "prepare"
        (prepare / "01_result").mkdir(parents=True)
        for path, size in self.inventory.items():
            file = root / path
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_bytes(b"x" * size)
        manifest = prepare / "01_result" / sync.MANIFEST_FILENAME
        manifest.write_text(self.manifest.read_text())
        context = {**self.context, "recovery_id": "new-publish"}
        prepare_summary = {"selected_step_dirs": [p.split("/")[0] for p in self.inventory],
                           "file_count": 3, "total_bytes": 35,
                           "run_id": context["recovery_id"], "run_date": context["run_date"],
                           "run_id_source": "cli", "run_date_source": "cli"}
        prepare_summary_path = prepare / "01_result" / sync.PREPARE_SUMMARY_FILENAME
        prepare_summary_path.write_text(json.dumps(prepare_summary))
        args = self.args()
        args.manual_recovery_id = context["recovery_id"]
        args.pipeline_root, args.prepare_dir, args.step_dir = str(root), str(prepare), str(self.tmp / "sync")
        Path(args.step_dir).mkdir()
        args.run_id, args.run_date = context["recovery_id"], context["run_date"]
        original_failed = copy.deepcopy(self.s3.documents)
        with patch.object(sync, "recovery_module", return_value=target), \
             patch.object(sync, "build_s3_client", return_value=self.s3), \
             patch.object(target, "build_stepfunctions_client", return_value=self.sfn), \
             patch.object(target, "guard_manual_lock"), patch.object(sync, "run_sync", return_value=[]) as publish, \
             patch.object(sync.time, "sleep"), patch.dict(os.environ, {"PORTAL_S3_VERIFY_WAIT_SEC": "0"}):
            publish.side_effect = sync.SyncError("simulated publication failure")
            with self.assertRaises(sync.SyncError):
                sync.run(args, self.logger)
            self.assertFalse(target.receipt_path(context).exists())
            publish.side_effect = None
            # 正常syncでもCURRENT検証が失敗した場合はreceiptを確定しない。
            key = target.EXPECTED_CURRENT_PREFIX + "/" + next(iter(self.inventory))
            self.s3.objects[key]["Size"] += 1
            with self.assertRaises(sync.SyncError):
                sync.run(args, self.logger)
            self.assertFalse(target.receipt_path(context).exists())
            self.s3.objects[key]["Size"] -= 1
            summary = sync.run(args, self.logger)
        self.assertEqual(self.s3.documents, original_failed)
        path = Path(summary["manual_recovery_receipt"])
        self.assertTrue(path.is_file())
        receipt, provenance, _ = target.verify_manual_receipt(path, self.s3, self.sfn)
        self.assertEqual(provenance["run_id"], "new-publish")
        self.assertEqual(receipt["source"]["run_id"], self.context["run_id"])
        confirm = load_module(target.project_root / "80-9_portal_s3_sync/02_confirm/confirm_portal_s3_sync.py", "sync_confirm_manual_test")
        output = self.tmp / "sync-summary.json"
        result = self.tmp / "sync-confirm.txt"
        output.write_text(json.dumps(summary))
        with patch.object(confirm, "SYNC_SUMMARY_PATH", output), \
             patch.object(confirm, "PREPARE_SUMMARY_PATH", prepare_summary_path), \
             patch.object(confirm, "CONFIRM_RESULT", result):
            confirm.main()
            self.assertIn("【結果】OK", result.read_text())
            path.unlink()
            with self.assertRaises(SystemExit):
                confirm.main()
            self.assertIn("【結果】NG", result.read_text())


if __name__ == "__main__":
    unittest.main()

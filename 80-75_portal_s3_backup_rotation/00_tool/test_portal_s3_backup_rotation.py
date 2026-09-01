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
import importlib.util
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import portal_s3_backup_rotation as target  # noqa: E402
from common.logger import get_logger  # noqa: E402

STATUS_WRITER_PATH = (
    project_root / "99-9_publish_pipeline_status" / "00_tool" / "publish_pipeline_status.py"
)
STATUS_WRITER_SPEC = importlib.util.spec_from_file_location(
    "publish_pipeline_status_for_80_75_test", str(STATUS_WRITER_PATH)
)
status_writer = importlib.util.module_from_spec(STATUS_WRITER_SPEC)
STATUS_WRITER_SPEC.loader.exec_module(status_writer)

CONFIRM_PATH = (
    project_root
    / "80-75_portal_s3_backup_rotation"
    / "02_confirm"
    / "confirm_portal_s3_backup_rotation.py"
)
CONFIRM_SPEC = importlib.util.spec_from_file_location(
    "confirm_portal_s3_backup_rotation_for_test", str(CONFIRM_PATH)
)
confirm_target = importlib.util.module_from_spec(CONFIRM_SPEC)
CONFIRM_SPEC.loader.exec_module(confirm_target)

BUCKET = target.EXPECTED_BUCKET
BASE_PREFIX = target.EXPECTED_BASE_PREFIX
CURRENT_PREFIX = target.EXPECTED_CURRENT_PREFIX
BACKUP_PREFIX = target.EXPECTED_BACKUP_PREFIX
SOURCE_URI = target.EXPECTED_SOURCE_URI
DESTINATION_URI = target.EXPECTED_DESTINATION_URI

PREV_RUN_DATE = "20260818"
PREV_RUN_ID = "sfn-9b6ab8c1-6089-4121-8ffc-e460affae951"
CURRENT_RUN_ID = "sfn-current-run"
CURRENT_RUN_DATE = "20260819"
RECOVERY_FAILED_RUN_ID = "sfn-recovery-target-failed"


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


def _execution_arn(name):
    return (
        "arn:aws:states:ap-northeast-1:166714029268:execution:"
        f"auto-match-llm-classifier-pipeline-orchestration:{name}"
    )


def make_execution_spec(
    run_date,
    run_id,
    name="focused-current",
    event_types=(),
    description_overrides=None,
):
    execution_arn = _execution_arn(name)
    events = [
        {"id": 1, "type": "ExecutionStarted"},
        {
            "id": 2,
            "type": "PassStateExited",
            "stateExitedEventDetails": {
                "name": target.PREPARE_RUN_CONTEXT_STATE,
                "output": json.dumps({"run_date": run_date, "run_id": run_id}),
            },
        },
    ]
    for event_type in event_types:
        events.append({"id": len(events) + 1, "type": event_type})
    description = {
        "executionArn": execution_arn,
        "stateMachineArn": target.EXPECTED_STATE_MACHINE_ARN,
        "status": "RUNNING",
        "redriveCount": 0,
        "startDate": datetime(2026, 8, 18, 10, 0, tzinfo=timezone.utc),
    }
    description.update(description_overrides or {})
    return {
        "executionArn": execution_arn,
        "description": description,
        "events": events,
    }


def make_failed_execution_spec(
    run_date,
    run_id,
    name=None,
    boundary_reached=False,
    event_types=(),
    description_overrides=None,
):
    spec = make_execution_spec(
        run_date,
        run_id,
        name=name or f"failed-{run_date}-{run_id}",
        description_overrides={"status": "FAILED", "redriveCount": 0},
    )
    spec["events"][1]["stateExitedEventDetails"]["outputDetails"] = {
        "truncated": False
    }
    if boundary_reached:
        spec["events"].append(
            {
                "id": len(spec["events"]) + 1,
                "type": "TaskStateEntered",
                "stateEnteredEventDetails": {
                    "name": target.PUBLICATION_BOUNDARY_STATE,
                },
            }
        )
    for event_type in event_types:
        spec["events"].append(
            {"id": len(spec["events"]) + 1, "type": event_type}
        )
    spec["events"].append(
        {"id": len(spec["events"]) + 1, "type": "ExecutionFailed"}
    )
    spec["description"].setdefault(
        "stopDate", datetime(2026, 8, 18, 10, 30, tzinfo=timezone.utc)
    )
    spec["description"].update(description_overrides or {})
    return spec


def make_succeeded_execution_spec(
    run_date,
    run_id,
    name="focused-authority",
    description_overrides=None,
):
    spec = make_execution_spec(
        run_date,
        run_id,
        name=name,
        description_overrides={
            "status": "SUCCEEDED",
            "redriveCount": 0,
            "startDate": datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc),
            "stopDate": datetime(2026, 8, 18, 1, 1, tzinfo=timezone.utc),
        },
    )
    spec["events"][1]["stateExitedEventDetails"]["outputDetails"] = {
        "truncated": False
    }
    spec["events"].append(
        {"id": len(spec["events"]) + 1, "type": "ExecutionSucceeded"}
    )
    spec["description"].update(description_overrides or {})
    return spec


class FakeStepFunctionsClient:
    """List/Describe/Historyだけを提供するread-only focused test client。"""

    def __init__(
        self,
        specs=(),
        fail_operation=None,
        paginate_list=False,
        paginate_history=False,
        repeat_list_token=False,
        repeat_history_token=False,
    ):
        self.specs = list(specs)
        self.fail_operation = fail_operation
        self.paginate_list = paginate_list
        self.paginate_history = paginate_history
        self.repeat_list_token = repeat_list_token
        self.repeat_history_token = repeat_history_token
        self.calls = []

    def list_executions(self, **kwargs):
        self.calls.append(("list_executions", kwargs))
        if self.fail_operation == "list":
            raise RuntimeError("simulated ListExecutions failure")
        status_filter = kwargs.get("statusFilter")
        summaries = [
            {
                "executionArn": spec["executionArn"],
                "status": spec["description"].get("status"),
                **{
                    key: spec["description"][key]
                    for key in ("startDate", "stopDate")
                    if key in spec["description"]
                },
            }
            for spec in self.specs
            if status_filter is None
            or spec["description"].get("status") == status_filter
        ]
        token = kwargs.get("nextToken")
        if not self.paginate_list or len(summaries) <= 1:
            return {"executions": summaries}
        if token is None:
            return {"executions": summaries[:1], "nextToken": "list-page-2"}
        response = {"executions": summaries[1:]}
        if self.repeat_list_token:
            response["nextToken"] = "list-page-2"
        return response

    def describe_execution(self, **kwargs):
        self.calls.append(("describe_execution", kwargs))
        if self.fail_operation == "describe":
            raise RuntimeError("simulated DescribeExecution failure")
        arn = kwargs["executionArn"]
        for spec in self.specs:
            if spec["executionArn"] == arn:
                return copy.deepcopy(spec["description"])
        raise RuntimeError(f"unknown execution: {arn}")

    def get_execution_history(self, **kwargs):
        self.calls.append(("get_execution_history", kwargs))
        if self.fail_operation == "history":
            raise RuntimeError("simulated GetExecutionHistory failure")
        arn = kwargs["executionArn"]
        spec = next(item for item in self.specs if item["executionArn"] == arn)
        events = copy.deepcopy(spec["events"])
        token = kwargs.get("nextToken")
        if not self.paginate_history or len(events) <= 1:
            return {"events": events}
        if token is None:
            return {"events": events[:1], "nextToken": "history-page-2"}
        response = {"events": events[1:]}
        if self.repeat_history_token:
            response["nextToken"] = "history-page-2"
        return response


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


def make_status_document(run_date, run_id, status, exit_code):
    """99-9正本のbuild_documentで実schema documentを生成する。"""
    running = status == "RUNNING"
    args = argparse.Namespace(
        run_id=run_id,
        run_date=run_date,
        status=status,
        started_at="2026-08-19T00:00:00Z",
        finished_at=None if running else "2026-08-19T01:00:00Z",
        exit_code=exit_code,
        current_step="INITIALIZING" if running else "PIPELINE_END",
        error_message="failed" if status == "FAILED" else "",
        log_s3_uri=f"s3://{BUCKET}/{BASE_PREFIX}/pipeline-logs/{run_date}/{run_id}/pipeline.log",
        bucket=BUCKET,
        base_prefix=BASE_PREFIX,
        log_prefix="pipeline-logs",
    )
    return status_writer.build_document(args)


class RotationTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="test_80_75_"))
        self.step_dir = self.tmp / "out"
        self.sync_dir = self.tmp / "sync"
        self.prepare_dir = self.tmp / "prepare"
        (self.sync_dir / "01_result").mkdir(parents=True)
        (self.prepare_dir / "01_result").mkdir(parents=True)
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
        self.backup_last_modified = {path: 100 for path in self.backup}
        self.manifest_path = (
            self.prepare_dir / "01_result" / target.PREVIOUS_MANIFEST_FILENAME
        )
        self.write_manifest()
        self.write_sync_summary(
            make_sync_summary(manifest_path=str(self.manifest_path.resolve()))
        )

        self.original_sleep = target.time.sleep
        target.time.sleep = self._fake_sleep
        self.original_run_sync = target.run_sync
        self.original_build = target.build_s3_client
        self.original_build_stepfunctions = target.build_stepfunctions_client
        self.sfn_client = None
        target.build_stepfunctions_client = self._build_stepfunctions_client
        os.environ["PORTAL_S3_VERIFY_WAIT_SEC"] = "0"
        os.environ["RUN_DATE"] = CURRENT_RUN_DATE
        os.environ["RUN_ID"] = CURRENT_RUN_ID

    def tearDown(self):
        target.time.sleep = self.original_sleep
        target.run_sync = self.original_run_sync
        target.build_s3_client = self.original_build
        target.build_stepfunctions_client = self.original_build_stepfunctions
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

    def _build_stepfunctions_client(self, region):
        if self.sfn_client is None:
            run_date = (os.environ.get("RUN_DATE") or CURRENT_RUN_DATE).strip()
            run_id = (os.environ.get("RUN_ID") or CURRENT_RUN_ID).strip()
            self.sfn_client = FakeStepFunctionsClient(
                [make_execution_spec(run_date, run_id)]
            )
        return self.sfn_client

    def write_sync_summary(self, summary):
        path = self.sync_dir / "01_result" / target.SYNC_SUMMARY_FILENAME
        with open(path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False)

    def write_manifest(self, entries=None):
        records = entries if entries is not None else self.current
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            for relative_path, size in sorted(records.items()):
                f.write(
                    json.dumps(
                        {"relative_path": relative_path, "size": size},
                        ensure_ascii=False,
                    )
                    + "\n"
                )

    def make_args(
        self,
        bootstrap=False,
        dry_run=False,
        current_run_id=None,
        recovery_run_date=None,
        recovery_run_id=None,
    ):
        return argparse.Namespace(
            bootstrap=bootstrap,
            dry_run=dry_run,
            step_dir=str(self.step_dir),
            sync_dir=str(self.sync_dir),
            current_run_id=current_run_id,
            recovery_run_date=recovery_run_date,
            recovery_run_id=recovery_run_id,
            prepare_dir=str(self.prepare_dir),
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
            (CURRENT_RUN_DATE, CURRENT_RUN_ID, "RUNNING", None),
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
        for path in self.backup:
            key = f"{BACKUP_PREFIX}/{path}"
            last_modified[key] = self.backup_last_modified.get(path, 100)
            etags[key] = f'"backup-etag-{path}"'
        for index, (run_date, run_id, status, exit_code) in enumerate(self._status_runs):
            key = f"{BASE_PREFIX}/pipeline-status/{run_date}/{run_id}/status.json"
            objects[key] = 500
            last_modified[key] = index + 1
            status_docs[key] = make_status_document(run_date, run_id, status, exit_code)
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

    def stub_current_running(self):
        """正常な99-9 RUNNING documentを持つmanaged current runを用意する。"""
        os.environ["RUN_DATE"] = CURRENT_RUN_DATE
        os.environ["RUN_ID"] = CURRENT_RUN_ID
        client = self.stub_s3(
            status_runs=[
                (PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0),
                (CURRENT_RUN_DATE, CURRENT_RUN_ID, "RUNNING", None),
            ]
        )
        key = f"{BASE_PREFIX}/pipeline-status/{CURRENT_RUN_DATE}/{CURRENT_RUN_ID}/status.json"
        return client, key

    def stub_recovery_s3(
        self,
        failed_run_date=CURRENT_RUN_DATE,
        failed_run_id=RECOVERY_FAILED_RUN_ID,
        failed_step=None,
        include_previous=True,
    ):
        # 限定recoveryは既存のdry-run非Production経路としてidentity非必須。
        os.environ.pop("RUN_DATE", None)
        os.environ.pop("RUN_ID", None)
        for path, last_modified in list(self.current_last_modified.items()):
            if not isinstance(last_modified, datetime):
                self.current_last_modified[path] = datetime(
                    2026, 8, 19, 0, 30, tzinfo=timezone.utc
                )
        status_runs = []
        if include_previous:
            status_runs.append((PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0))
        status_runs.append((failed_run_date, failed_run_id, "FAILED", 1))
        client = self.stub_s3(status_runs=status_runs)
        key = f"{BASE_PREFIX}/pipeline-status/{failed_run_date}/{failed_run_id}/status.json"
        client.status_docs[key]["current_step"] = failed_step or (
            f"{target.RECOVERY_FAILED_STEP_NAME}(RUN_DATE={failed_run_date})"
        )
        return client, key

    def write_previous_backup_summary(self):
        result_dir = self.step_dir / "01_result"
        result_dir.mkdir(parents=True, exist_ok=True)
        summary = {
            "step": target.STEP_NAME,
            "operation": "rotation",
            "mode": "apply",
            "backup_status": "SUCCEEDED",
            "s3_source": SOURCE_URI,
            "s3_destination": DESTINATION_URI,
            "s3_destination_locked": True,
            "verify": {
                "verified": True,
                "expected_file_count": len(self.backup),
                "actual_file_count": len(self.backup),
                "expected_total_bytes": sum(self.backup.values()),
                "actual_total_bytes": sum(self.backup.values()),
                "missing_count": 0,
                "extra_count": 0,
                "size_mismatch_count": 0,
            },
        }
        with open(result_dir / target.BACKUP_SUMMARY_FILENAME, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False)

    def stub_prepublication_recovery(
        self,
        failures=None,
        current_run_date=None,
        authority_run_date=None,
        authority_run_id=None,
    ):
        """authority -> publication前FAILED(s) -> Fresh RUNNING runを用意する。"""
        failures = failures or [
            {
                "run_date": CURRENT_RUN_DATE,
                "run_id": RECOVERY_FAILED_RUN_ID,
            }
        ]
        current_run_date = current_run_date or CURRENT_RUN_DATE
        authority_run_date = authority_run_date or PREV_RUN_DATE
        authority_run_id = authority_run_id or PREV_RUN_ID
        os.environ["RUN_DATE"] = current_run_date
        os.environ["RUN_ID"] = CURRENT_RUN_ID
        authority_finished = datetime(2026, 8, 18, 1, 0, tzinfo=timezone.utc)
        object_modified = datetime(2026, 8, 18, 0, 30, tzinfo=timezone.utc)
        self.current_last_modified = {path: object_modified for path in self.current}
        self.backup_last_modified = {path: object_modified for path in self.backup}
        self.write_sync_summary(
            make_sync_summary(
                run_date=authority_run_date,
                run_id=authority_run_id,
                manifest_path=str(self.manifest_path.resolve()),
            )
        )
        self.write_previous_backup_summary()

        status_runs = [(authority_run_date, authority_run_id, "SUCCEEDED", 0)]
        status_runs.extend(
            (item["run_date"], item["run_id"], "FAILED", item.get("exit_code", 86))
            for item in failures
        )
        status_runs.append((current_run_date, CURRENT_RUN_ID, "RUNNING", None))
        client = self.stub_s3(status_runs=status_runs)

        authority_key = (
            f"{BASE_PREFIX}/pipeline-status/{authority_run_date}/{authority_run_id}/status.json"
        )
        authority = client.status_docs[authority_key]
        authority["started_at"] = "2026-08-18T00:00:00Z"
        authority["finished_at"] = "2026-08-18T01:00:00Z"
        authority["updated_at"] = "2026-08-18T01:00:01Z"
        client.last_modified[authority_key] = datetime(
            2026, 8, 18, 1, 0, 2, tzinfo=timezone.utc
        )

        for index, item in enumerate(failures):
            key = (
                f"{BASE_PREFIX}/pipeline-status/{item['run_date']}/{item['run_id']}/status.json"
            )
            document = client.status_docs[key]
            start_hour = 2 + (index * 2)
            document["started_at"] = f"2026-08-18T{start_hour:02d}:00:00Z"
            document["finished_at"] = f"2026-08-18T{start_hour:02d}:30:00Z"
            document["updated_at"] = f"2026-08-18T{start_hour:02d}:30:01Z"
            document["current_step"] = item.get(
                "current_step", "08-5_BATCH_WAIT"
            )
            document["finished_at_source"] = item.get(
                "finished_at_source", "batch_status_lambda"
            )
            document["exit_code_source"] = item.get(
                "exit_code_source", "batch_status_lambda"
            )
            if "schema_version" in item:
                document["schema_version"] = item["schema_version"]
            client.last_modified[key] = datetime(
                2026, 8, 18, start_hour, 30, 2, tzinfo=timezone.utc
            )

        current_key = (
            f"{BASE_PREFIX}/pipeline-status/{current_run_date}/{CURRENT_RUN_ID}/status.json"
        )
        current = client.status_docs[current_key]
        current_hour = 2 + (len(failures) * 2)
        current["started_at"] = f"2026-08-18T{current_hour:02d}:00:00Z"
        current["updated_at"] = f"2026-08-18T{current_hour:02d}:00:01Z"
        client.last_modified[current_key] = datetime(
            2026, 8, 18, current_hour, 0, 2, tzinfo=timezone.utc
        )
        self.assertLess(authority_finished, datetime.fromisoformat(current["started_at"][:-1] + "+00:00"))
        current_execution_start = datetime(
            2026, 8, 18, current_hour, 0, tzinfo=timezone.utc
        )
        specs = [
            make_execution_spec(
                current_run_date,
                CURRENT_RUN_ID,
                description_overrides={"startDate": current_execution_start},
            ),
            make_succeeded_execution_spec(
                authority_run_date,
                authority_run_id,
                description_overrides={
                    "startDate": datetime(
                        2026, 8, 18, 0, 0, tzinfo=timezone.utc
                    ),
                    "stopDate": datetime(
                        2026, 8, 18, 1, 1, tzinfo=timezone.utc
                    ),
                },
            ),
        ]
        specs.extend(
            make_failed_execution_spec(
                item["run_date"],
                item["run_id"],
                boundary_reached=item.get("boundary_reached", False),
                event_types=item.get("history_event_types", ()),
                description_overrides={
                    "startDate": datetime(
                        2026, 8, 18, 2 + (index * 2), 0, tzinfo=timezone.utc
                    ),
                    "stopDate": datetime(
                        2026, 8, 18, 2 + (index * 2), 30, tzinfo=timezone.utc
                    ),
                    **(item.get("description_overrides") or {}),
                },
            )
            for index, item in enumerate(failures)
        )
        self.sfn_client = FakeStepFunctionsClient(specs)
        return client


# ---------------------------------------------------------------------------
# immutable Step Functions execution history guard
# ---------------------------------------------------------------------------


class TestImmutableExecutionHistoryGuard(RotationTestBase):
    def guard(self, client):
        return target.guard_current_execution_history(
            client,
            {"run_date": CURRENT_RUN_DATE, "run_id": CURRENT_RUN_ID, "source": "env"},
            self.logger,
        )

    def assert_guard_denied(self, client):
        with self.assertRaises(target.RotationError):
            self.guard(client)

    def test_fresh_same_date_execution_passes(self):
        client = FakeStepFunctionsClient(
            [make_execution_spec(CURRENT_RUN_DATE, CURRENT_RUN_ID)]
        )
        evidence = self.guard(client)
        self.assertEqual(evidence["validation_result"], "PASS")
        self.assertEqual(evidence["immutable_execution_guard_result"], "PASS")
        self.assertEqual(evidence["execution_status"], "RUNNING")
        self.assertTrue(evidence["run_identity_match"])
        self.assertEqual(evidence["redrive_count"], 0)
        self.assertFalse(evidence["redrive_date_present"])
        self.assertFalse(evidence["execution_redriven_event_present"])
        self.assertFalse(evidence["prior_terminal_event_present"])
        self.assertEqual(evidence["prior_terminal_event_count"], 0)

    def test_redrive_count_missing_is_denied(self):
        spec = make_execution_spec(CURRENT_RUN_DATE, CURRENT_RUN_ID)
        spec["description"].pop("redriveCount")
        self.assert_guard_denied(FakeStepFunctionsClient([spec]))

    def test_redrive_count_malformed_is_denied(self):
        for value in (None, "0", False, 0.0, -1):
            with self.subTest(value=value):
                spec = make_execution_spec(
                    CURRENT_RUN_DATE,
                    CURRENT_RUN_ID,
                    description_overrides={"redriveCount": value},
                )
                self.assert_guard_denied(FakeStepFunctionsClient([spec]))

    def test_redrive_count_is_denied(self):
        spec = make_execution_spec(
            CURRENT_RUN_DATE,
            CURRENT_RUN_ID,
            description_overrides={"redriveCount": 1},
        )
        self.assert_guard_denied(FakeStepFunctionsClient([spec]))

    def test_redrive_date_is_denied(self):
        spec = make_execution_spec(
            CURRENT_RUN_DATE,
            CURRENT_RUN_ID,
            description_overrides={"redriveDate": datetime(2026, 8, 19, tzinfo=timezone.utc)},
        )
        self.assert_guard_denied(FakeStepFunctionsClient([spec]))

    def test_execution_redriven_event_is_denied(self):
        spec = make_execution_spec(
            CURRENT_RUN_DATE, CURRENT_RUN_ID, event_types=("ExecutionRedriven",)
        )
        self.assert_guard_denied(FakeStepFunctionsClient([spec]))

    def test_prior_terminal_events_are_denied(self):
        for event_type in ("ExecutionFailed", "ExecutionAborted", "ExecutionTimedOut"):
            with self.subTest(event_type=event_type):
                spec = make_execution_spec(
                    CURRENT_RUN_DATE, CURRENT_RUN_ID, event_types=(event_type,)
                )
                self.assert_guard_denied(FakeStepFunctionsClient([spec]))

    def test_zero_matching_execution_is_denied(self):
        spec = make_execution_spec(CURRENT_RUN_DATE, "sfn-other-run")
        self.assert_guard_denied(FakeStepFunctionsClient([spec]))

    def test_multiple_matching_executions_are_denied(self):
        specs = [
            make_execution_spec(CURRENT_RUN_DATE, CURRENT_RUN_ID, name="match-1"),
            make_execution_spec(CURRENT_RUN_DATE, CURRENT_RUN_ID, name="match-2"),
        ]
        self.assert_guard_denied(FakeStepFunctionsClient(specs))

    def test_all_list_and_history_pages_are_checked(self):
        specs = [
            make_execution_spec(CURRENT_RUN_DATE, "sfn-other-run", name="other"),
            make_execution_spec(CURRENT_RUN_DATE, CURRENT_RUN_ID, name="current"),
        ]
        client = FakeStepFunctionsClient(
            specs, paginate_list=True, paginate_history=True
        )
        evidence = self.guard(client)
        self.assertEqual(evidence["list_pages_checked"], 2)
        self.assertEqual(evidence["history_pages_checked"], 2)

    def test_api_failures_are_denied(self):
        spec = make_execution_spec(CURRENT_RUN_DATE, CURRENT_RUN_ID)
        for operation in ("list", "describe", "history"):
            with self.subTest(operation=operation):
                self.assert_guard_denied(
                    FakeStepFunctionsClient([spec], fail_operation=operation)
                )

    def test_pagination_token_cycles_are_denied(self):
        specs = [
            make_execution_spec(CURRENT_RUN_DATE, "sfn-other-run", name="other"),
            make_execution_spec(CURRENT_RUN_DATE, CURRENT_RUN_ID, name="current"),
        ]
        self.assert_guard_denied(
            FakeStepFunctionsClient(
                specs, paginate_list=True, repeat_list_token=True
            )
        )
        self.assert_guard_denied(
            FakeStepFunctionsClient(
                [specs[1]], paginate_history=True, repeat_history_token=True
            )
        )

    def test_malformed_prepare_run_context_is_denied(self):
        spec = make_execution_spec(CURRENT_RUN_DATE, CURRENT_RUN_ID)
        spec["events"][1]["stateExitedEventDetails"]["output"] = "not-json"
        self.assert_guard_denied(FakeStepFunctionsClient([spec]))

    def test_status_overwrite_reproduction_is_denied_before_sync(self):
        # 同一run_idの過去FAILED status.jsonが現RUNNING documentで上書き済み。
        # S3だけなら自run除外後にprevious SUCCEEDEDが最新となるが、historyで拒否する。
        self.stub_sync()
        self.stub_current_running()
        self.sfn_client = FakeStepFunctionsClient(
            [
                make_execution_spec(
                    CURRENT_RUN_DATE,
                    CURRENT_RUN_ID,
                    event_types=("ExecutionFailed",),
                )
            ]
        )
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_apply_missing_managed_identity_is_denied_before_sync(self):
        cases = (
            (None, CURRENT_RUN_ID),
            (CURRENT_RUN_DATE, None),
            (None, None),
            ("", ""),
        )
        for run_date, run_id in cases:
            with self.subTest(run_date=run_date, run_id=run_id):
                if run_date is None:
                    os.environ.pop("RUN_DATE", None)
                else:
                    os.environ["RUN_DATE"] = run_date
                if run_id is None:
                    os.environ.pop("RUN_ID", None)
                else:
                    os.environ["RUN_ID"] = run_id
                self.sync_calls = []
                self.stub_sync()
                self.stub_s3()
                with self.assertRaises(target.RotationError):
                    target.run(self.make_args(), self.logger)
                self.assertEqual(self.sync_calls, [])

    def test_apply_malformed_managed_identity_is_denied_before_sync(self):
        for run_date, run_id in (("20260230", CURRENT_RUN_ID), (CURRENT_RUN_DATE, "bad/id")):
            with self.subTest(run_date=run_date, run_id=run_id):
                os.environ["RUN_DATE"] = run_date
                os.environ["RUN_ID"] = run_id
                self.sync_calls = []
                self.stub_sync()
                with self.assertRaises(target.RotationError):
                    target.run(self.make_args(), self.logger)
                self.assertEqual(self.sync_calls, [])

    def test_external_identity_without_matching_execution_is_denied_before_sync(self):
        self.stub_sync()
        self.stub_current_running()
        self.sfn_client = FakeStepFunctionsClient(
            [make_execution_spec(CURRENT_RUN_DATE, "sfn-other-run")]
        )
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])


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
        snapshot = summary["previous_current"]
        self.assertEqual(snapshot["run_date"], PREV_RUN_DATE)
        self.assertEqual(snapshot["run_id"], PREV_RUN_ID)
        self.assertEqual(snapshot["run_date_source"], "env")
        self.assertEqual(snapshot["run_id_source"], "env")
        self.assertEqual(snapshot["destination"], SOURCE_URI)
        self.assertTrue(snapshot["verified"])
        self.assertEqual(snapshot["sync_step"], "80-9_portal_s3_sync")
        self.assertEqual(snapshot["file_count"], 3)
        self.assertEqual(snapshot["total_bytes"], 35)
        self.assertEqual(
            summary["immutable_execution_guard_contract_version"],
            target.IMMUTABLE_EXECUTION_GUARD_CONTRACT_VERSION,
        )
        guard = summary["current_execution_guard"]
        self.assertEqual(guard["immutable_execution_guard_result"], "PASS")
        self.assertEqual(guard["execution_status"], "RUNNING")
        self.assertTrue(guard["run_identity_match"])
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
        self.stub_sync()
        self.stub_current_running()
        summary = target.run(self.make_args(), self.logger)
        self.assertTrue(summary["verify"]["verified"])

    def test_finding_schema_version_unsupported_fails(self):
        client, key = self.stub_current_running()
        client.status_docs[key]["schema_version"] = "unsupported"
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_finding_schema_version_missing_fails(self):
        client, key = self.stub_current_running()
        client.status_docs[key].pop("schema_version")
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_finding_required_status_key_missing_fails(self):
        client, key = self.stub_current_running()
        client.status_docs[key].pop("current_step")
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_finding_running_schema_exact_13_keys_passes(self):
        client, key = self.stub_current_running()
        self.assertEqual(set(client.status_docs[key]), target.STATUS_REQUIRED_KEYS)
        self.assertEqual(len(client.status_docs[key]), 13)
        self.stub_sync()
        summary = target.run(self.make_args(), self.logger)
        self.assertTrue(summary["verify"]["verified"])

    def test_finding_running_schema_12_keys_fails(self):
        client, key = self.stub_current_running()
        client.status_docs[key].pop("current_step")
        self.assertEqual(len(client.status_docs[key]), 12)
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_finding_running_schema_14_keys_fails(self):
        client, key = self.stub_current_running()
        client.status_docs[key]["future_schema_field"] = "must-fail"
        self.assertEqual(len(client.status_docs[key]), 14)
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_finding_running_unknown_extra_field_fails(self):
        client, key = self.stub_current_running()
        client.status_docs[key]["unknown_extra_field"] = True
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_finding_started_at_invalid_fails(self):
        for invalid in ("", "arbitrary text", "2026-02-30T09:00:00Z", "2026-08-19T09:00:00"):
            with self.subTest(started_at=invalid):
                client, key = self.stub_current_running()
                client.status_docs[key]["started_at"] = invalid
                self.stub_sync()
                with self.assertRaises(target.RotationError):
                    target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_finding_started_at_missing_fails(self):
        client, key = self.stub_current_running()
        client.status_docs[key].pop("started_at")
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_finding_finished_at_source_must_match_99_9(self):
        client, key = self.stub_current_running()
        client.status_docs[key]["finished_at_source"] = "default"
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_finding_exit_code_source_must_match_99_9(self):
        client, key = self.stub_current_running()
        client.status_docs[key]["exit_code_source"] = "managed_wrapper"
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_finding_managed_env_document_identity_mismatch_fails(self):
        client, key = self.stub_current_running()
        os.environ["RUN_DATE"] = "20260820"
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(client.status_docs[key]["run_date"], CURRENT_RUN_DATE)
        self.assertEqual(self.sync_calls, [])

    def test_finding_invalid_running_does_not_fallback_to_older_succeeded(self):
        client, key = self.stub_current_running()
        client.status_docs[key]["schema_version"] = "unsupported"
        self.stub_sync()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_finding_current_cli_without_managed_env_fails(self):
        os.environ.pop("RUN_DATE", None)
        os.environ.pop("RUN_ID", None)
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
# 限定tail recovery
# ---------------------------------------------------------------------------


class TestLimitedRecovery(RotationTestBase):
    def recovery_args(self, **overrides):
        values = {
            "dry_run": True,
            "recovery_run_date": CURRENT_RUN_DATE,
            "recovery_run_id": RECOVERY_FAILED_RUN_ID,
        }
        values.update(overrides)
        return self.make_args(**values)

    def test_recovery_a_normal_mode_latest_failed_still_fails(self):
        self.stub_sync()
        self.stub_recovery_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.make_args(dry_run=True), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_recovery_b_exact_failed_run_and_verified_current_pass(self):
        self.stub_sync()
        self.stub_recovery_s3()
        summary = target.run(self.recovery_args(), self.logger)
        recovery = summary["recovery"]
        self.assertTrue(recovery["eligible"])
        self.assertTrue(recovery["inventory_verified"])
        self.assertEqual(recovery["target_run_date"], CURRENT_RUN_DATE)
        self.assertEqual(recovery["target_run_id"], RECOVERY_FAILED_RUN_ID)
        self.assertEqual(recovery["previous_verified_run_date"], PREV_RUN_DATE)
        self.assertEqual(len(self.sync_calls), 1)
        self.assertIn("--dryrun", self.sync_calls[0]["argv"])

    def test_recovery_c_run_date_mismatch_fails(self):
        self.stub_sync()
        self.stub_recovery_s3()
        with self.assertRaises(target.RotationError):
            target.run(
                self.recovery_args(recovery_run_date="20260820"),
                self.logger,
            )
        self.assertEqual(self.sync_calls, [])

    def test_recovery_d_failed_run_id_mismatch_fails(self):
        self.stub_sync()
        self.stub_recovery_s3()
        with self.assertRaises(target.RotationError):
            target.run(
                self.recovery_args(recovery_run_id="sfn-other-failed-run"),
                self.logger,
            )
        self.assertEqual(self.sync_calls, [])

    def test_recovery_e_failure_step_mismatch_fails(self):
        self.stub_sync()
        self.stub_recovery_s3(failed_step="80-8_portal_s3_prepare")
        with self.assertRaises(target.RotationError):
            target.run(self.recovery_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_recovery_f_current_summary_count_or_size_mismatch_fails(self):
        self.current["01-1_fetch_gmail/01_result/fetch_gmail.jsonl"] = 11
        self.backup = dict(self.current)
        self.stub_sync()
        self.stub_recovery_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.recovery_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_recovery_g_manifest_inventory_mismatch_fails(self):
        self.write_sync_summary(
            make_sync_summary(manifest_path=str(self.tmp / "wrong_manifest.jsonl"))
        )
        self.stub_sync()
        self.stub_recovery_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.recovery_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_recovery_g2_current_modified_after_previous_success_fails(self):
        path = "01-1_fetch_gmail/01_result/fetch_gmail.jsonl"
        self.current_last_modified[path] = datetime(
            2026, 8, 19, 2, 0, tzinfo=timezone.utc
        )
        self.stub_sync()
        self.stub_recovery_s3()
        with self.assertRaises(target.RotationError):
            target.run(self.recovery_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_recovery_h_previous_successful_run_unresolvable_fails(self):
        self.stub_sync()
        self.stub_recovery_s3(include_previous=False)
        with self.assertRaises(target.RotationError):
            target.run(self.recovery_args(), self.logger)
        self.assertEqual(self.sync_calls, [])

    def test_recovery_i_no_option_does_not_enter_manifest_exception_path(self):
        self.write_manifest({"unrelated/path.txt": sum(self.current.values())})
        self.stub_sync()
        self.stub_s3()
        summary = target.run(self.make_args(dry_run=True), self.logger)
        self.assertNotIn("recovery", summary)
        self.assertEqual(len(self.sync_calls), 1)

    def test_recovery_requires_date_and_run_id_together(self):
        self.stub_sync()
        self.stub_recovery_s3()
        for args in (
            self.make_args(dry_run=True, recovery_run_date=CURRENT_RUN_DATE),
            self.make_args(dry_run=True, recovery_run_id=RECOVERY_FAILED_RUN_ID),
        ):
            with self.subTest(args=args):
                with self.assertRaises(target.RotationError):
                    target.run(args, self.logger)
        self.assertEqual(self.sync_calls, [])


class TestPrePublicationFailureRecovery(RotationTestBase):
    def run_recovery(self):
        self.stub_sync()
        return target.run(self.make_args(dry_run=True), self.logger)

    def assert_denied_before_sync(self):
        with self.assertRaises(target.RotationError):
            self.run_recovery()
        self.assertEqual(self.sync_calls, [])

    def add_old_failed_without_prepare_run_context(self):
        old = make_failed_execution_spec(
            "20260810",
            "sfn-old-without-prepare",
            name="45d851d4-5431-4c3d-9746-bb6e371c10aa",
            description_overrides={
                "startDate": datetime(
                    2026, 8, 17, 20, 0, tzinfo=timezone.utc
                ),
                "stopDate": datetime(
                    2026, 8, 17, 20, 30, tzinfo=timezone.utc
                ),
            },
        )
        old["events"] = [
            {"id": 1, "type": "ExecutionStarted"},
            {"id": 2, "type": "ExecutionFailed"},
        ]
        self.sfn_client.specs.append(old)
        return old

    def test_01_current_three_prepublication_failures_allow_fresh_run(self):
        failures = [
            {
                "run_date": "20260828",
                "run_id": "sfn-failed-20260828",
                "current_step": "08-5_BATCH_WAIT",
                "finished_at_source": "batch_status_lambda",
                "exit_code_source": "batch_status_lambda",
                "exit_code": 86,
            },
            {
                "run_date": "20260829",
                "run_id": "sfn-failed-20260829",
                "current_step": "08-1_restore_and_merge_requirement_skill_ai_matching",
                "finished_at_source": "managed_wrapper",
                "exit_code_source": "managed_wrapper",
                "exit_code": 1,
            },
            {
                "run_date": "20260831",
                "run_id": "sfn-failed-20260831",
                "current_step": "08-5_BATCH_WAIT",
                "finished_at_source": "batch_status_lambda",
                "exit_code_source": "batch_status_lambda",
                "exit_code": 86,
            },
        ]
        self.stub_prepublication_recovery(
            failures,
            current_run_date="20260901",
            authority_run_date="20260827",
            authority_run_id="sfn-authority-20260827",
        )
        summary = self.run_recovery()
        recovery = summary["recovery"]
        self.assertEqual(recovery["recovery_mode"], target.PREPUBLICATION_RECOVERY_MODE)
        self.assertEqual(recovery["rotation_authority_run_date"], "20260827")
        self.assertEqual(len(recovery["intervening_runs"]), 3)
        self.assertTrue(recovery["all_intervening_runs_checked"])
        self.assertFalse(
            recovery["publication_guard"]["failure_reason_allowlist_used"]
        )
        self.assertTrue(
            all(
                item["publication_boundary_reached"] is False
                and item["execution_evidence"]["publication_boundary_reached"] is False
                for item in recovery["intervening_runs"]
            )
        )
        self.assertTrue(recovery["current_unchanged"]["manifest_inventory_match"])
        self.assertTrue(recovery["bk1_unchanged"]["previous_80_75_summary_match"])
        self.assertEqual(len(self.sync_calls), 1)

    def test_01a_old_failed_without_identity_before_authority_does_not_block(self):
        failures = [
            {"run_date": "20260828", "run_id": "sfn-failed-20260828"},
            {"run_date": "20260829", "run_id": "sfn-failed-20260829"},
            {"run_date": "20260831", "run_id": "sfn-failed-20260831"},
        ]
        self.stub_prepublication_recovery(
            failures,
            current_run_date="20260901",
            authority_run_date="20260827",
            authority_run_id="sfn-authority-20260827",
        )
        old = self.add_old_failed_without_prepare_run_context()

        summary = self.run_recovery()

        self.assertEqual(len(summary["recovery"]["intervening_runs"]), 3)
        old_history_calls = [
            call
            for call in self.sfn_client.calls
            if call[0] == "get_execution_history"
            and call[1]["executionArn"] == old["executionArn"]
        ]
        self.assertEqual(old_history_calls, [])
        list_call_indexes = {
            call[1]["statusFilter"]: index
            for index, call in enumerate(self.sfn_client.calls)
            if call[0] == "list_executions"
        }
        first_candidate_history = next(
            index
            for index, call in enumerate(self.sfn_client.calls)
            if call[0] == "get_execution_history"
            and call[1]["executionArn"].startswith(
                target.EXPECTED_STATE_MACHINE_ARN.replace(
                    ":stateMachine:", ":execution:"
                )
                + ":failed-"
            )
        )
        self.assertLess(list_call_indexes["SUCCEEDED"], list_call_indexes["RUNNING"])
        self.assertLess(list_call_indexes["RUNNING"], list_call_indexes["FAILED"])
        self.assertLess(list_call_indexes["FAILED"], first_candidate_history)

    def test_01b_old_failed_is_audited_as_outside_recovery_window(self):
        self.stub_prepublication_recovery()
        old = self.add_old_failed_without_prepare_run_context()

        summary = self.run_recovery()
        outside = summary["recovery"]["execution_window"][
            "outside_recovery_window"
        ]

        self.assertIn(
            {
                "execution_arn": old["executionArn"],
                "classification": "OUTSIDE_RECOVERY_WINDOW",
                "reason": "COMPLETED_BEFORE_AUTHORITY",
                "execution_start_date": "2026-08-17T20:00:00+00:00",
                "execution_stop_date": "2026-08-17T20:30:00+00:00",
            },
            outside,
        )

    def test_01c_window_candidate_without_prepare_run_context_is_denied(self):
        self.stub_prepublication_recovery()
        failed = next(
            spec
            for spec in self.sfn_client.specs
            if spec["description"]["status"] == "FAILED"
        )
        failed["events"] = [
            {"id": 1, "type": "ExecutionStarted"},
            {"id": 2, "type": "ExecutionFailed"},
        ]
        self.assert_denied_before_sync()

    def test_01d_window_candidate_without_history_is_denied(self):
        self.stub_prepublication_recovery()
        failed = next(
            spec
            for spec in self.sfn_client.specs
            if spec["description"]["status"] == "FAILED"
        )
        failed["events"] = []
        self.assert_denied_before_sync()

    def test_01e_execution_crossing_authority_boundary_is_denied(self):
        self.stub_prepublication_recovery()
        failed = next(
            spec
            for spec in self.sfn_client.specs
            if spec["description"]["status"] == "FAILED"
        )
        failed["description"]["startDate"] = datetime(
            2026, 8, 18, 0, 30, tzinfo=timezone.utc
        )
        failed["description"]["stopDate"] = datetime(
            2026, 8, 18, 2, 30, tzinfo=timezone.utc
        )
        self.assert_denied_before_sync()

    def test_01f_execution_after_current_is_not_a_candidate(self):
        self.stub_prepublication_recovery()
        future = make_failed_execution_spec(
            "20260820",
            "sfn-after-current",
            name="after-current",
            description_overrides={
                "startDate": datetime(
                    2026, 8, 18, 5, 0, tzinfo=timezone.utc
                ),
                "stopDate": datetime(
                    2026, 8, 18, 5, 30, tzinfo=timezone.utc
                ),
            },
        )
        future["events"] = [
            {"id": 1, "type": "ExecutionStarted"},
            {"id": 2, "type": "ExecutionFailed"},
        ]
        self.sfn_client.specs.append(future)

        summary = self.run_recovery()
        window = summary["recovery"]["execution_window"]

        self.assertEqual(window["candidate_execution_count"], 1)
        self.assertTrue(
            any(
                item["execution_arn"] == future["executionArn"]
                and item["reason"] == "STARTED_AFTER_CURRENT"
                for item in window["outside_recovery_window"]
            )
        )
        self.assertFalse(
            any(
                call[0] == "get_execution_history"
                and call[1]["executionArn"] == future["executionArn"]
                for call in self.sfn_client.calls
            )
        )

    def test_02_80_75_reached_then_failed_is_denied(self):
        self.stub_prepublication_recovery(
            [{
                "run_date": "20260828",
                "run_id": "sfn-rotation-failed",
                "current_step": "08-5_BATCH_WAIT",
                "boundary_reached": True,
            }]
        )
        self.assert_denied_before_sync()

    def test_03_bk1_rotation_started_then_failed_is_denied(self):
        self.stub_prepublication_recovery(
            [{
                "run_date": "20260828",
                "run_id": "sfn-bk1-rotation-started",
                "current_step": "80-75_portal_s3_backup_rotation",
                "boundary_reached": True,
            }]
        )
        self.assert_denied_before_sync()

    def test_04_80_8_or_80_9_failure_is_denied(self):
        for step in ("80-8_portal_s3_prepare", "80-9_portal_s3_sync"):
            with self.subTest(step=step):
                self.sync_calls = []
                self.stub_prepublication_recovery(
                    [{
                        "run_date": "20260828",
                        "run_id": f"sfn-{step[:4]}-failed",
                        "current_step": step,
                        "boundary_reached": True,
                    }]
                )
                self.assert_denied_before_sync()

    def test_05_current_change_is_denied(self):
        self.stub_prepublication_recovery()
        path = next(iter(self.current))
        self.current.pop(path)
        self.current_last_modified.pop(path)
        self._refresh_client()
        self.assert_denied_before_sync()

    def test_06_bk1_change_is_denied(self):
        self.stub_prepublication_recovery()
        path = next(iter(self.backup))
        self.backup_last_modified[path] = datetime(
            2026, 8, 18, 1, 30, tzinfo=timezone.utc
        )
        self._refresh_client()
        self.assert_denied_before_sync()

    def test_07_unknown_failure_step_is_denied(self):
        self.stub_prepublication_recovery(
            [{
                "run_date": "20260828",
                "run_id": "sfn-unknown-step",
                "current_step": "UNKNOWN",
            }]
        )
        self.assert_denied_before_sync()

    def test_08_fresh_execution_passes(self):
        self.stub_prepublication_recovery()
        summary = self.run_recovery()
        guard = summary["current_execution_guard"]
        self.assertEqual(guard["execution_status"], "RUNNING")
        self.assertEqual(guard["redrive_count"], 0)
        self.assertFalse(guard["execution_redriven_event_present"])

    def test_09_historical_redrive_is_denied(self):
        self.stub_prepublication_recovery(
            [{
                "run_date": "20260828",
                "run_id": "sfn-redriven",
                "description_overrides": {"redriveCount": 1},
            }]
        )
        self.assert_denied_before_sync()

    def test_10_normal_previous_succeeded_new_run_passes(self):
        self.stub_current_running()
        summary = self.run_recovery()
        self.assertNotIn("recovery", summary)
        self.assertEqual(len(self.sync_calls), 1)

    def test_11_publication_history_unknown_is_denied(self):
        self.stub_prepublication_recovery()
        self.sfn_client = FakeStepFunctionsClient(
            [make_execution_spec(CURRENT_RUN_DATE, CURRENT_RUN_ID)]
        )
        self.assert_denied_before_sync()

    def test_12_current_or_bk1_baseline_mismatch_is_denied(self):
        for changed in ("current", "bk1"):
            with self.subTest(changed=changed):
                self.sync_calls = []
                self.stub_prepublication_recovery()
                path = next(iter(self.current))
                if changed == "current":
                    self.current[path] += 1
                else:
                    self.backup[path] += 1
                self._refresh_client()
                self.assert_denied_before_sync()

    def test_13_same_run_id_redrive_is_denied(self):
        os.environ["RUN_DATE"] = CURRENT_RUN_DATE
        os.environ["RUN_ID"] = RECOVERY_FAILED_RUN_ID
        client = self.stub_s3(
            status_runs=[
                (PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0),
                (CURRENT_RUN_DATE, RECOVERY_FAILED_RUN_ID, "FAILED", 86),
            ]
        )
        key = (
            f"{BASE_PREFIX}/pipeline-status/{CURRENT_RUN_DATE}/"
            f"{RECOVERY_FAILED_RUN_ID}/status.json"
        )
        client.status_docs[key]["current_step"] = "08-5_BATCH_WAIT"
        self.assert_denied_before_sync()

    def test_14_unknown_history_ordering_is_denied(self):
        self.stub_prepublication_recovery()
        failed_spec = next(
            spec
            for spec in self.sfn_client.specs
            if spec["description"]["status"] == "FAILED"
        )
        failed_spec["events"][1]["id"] = 3
        failed_spec["events"][2]["id"] = 2
        self.assert_denied_before_sync()


class TestTailRecoveryRunnerContract(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        self.runner = project_root / "00_pipeline" / "00_tool" / "run_tail_recovery.sh"
        self.call_log = self.temp_path / "calls.log"
        self.call_count = self.temp_path / "call_count"
        fake_python = self.temp_path / "python3"
        fake_python.write_text(
            """#!/usr/bin/env bash
set -euo pipefail
count=0
if [[ -f "$CALL_COUNT_FILE" ]]; then
  read -r count < "$CALL_COUNT_FILE"
fi
count=$((count + 1))
printf '%s\n' "$count" > "$CALL_COUNT_FILE"
printf '%s\n' "$*" >> "$CALL_LOG"
if [[ "${FAIL_CALL_INDEX:-0}" -eq "$count" ]]; then
  exit 9
fi
""",
            encoding="utf-8",
        )
        fake_python.chmod(0o755)

    def tearDown(self):
        self.temp_dir.cleanup()

    def run_runner(self, fail_call_index=0):
        for path in (self.call_log, self.call_count):
            if path.exists():
                path.unlink()
        env = os.environ.copy()
        env.update({
            "PATH": "{}:{}".format(self.temp_path, env["PATH"]),
            "RUN_DATE": CURRENT_RUN_DATE,
            "RUN_ID": "recovery-runner-test",
            "RECOVERY_FAILED_RUN_DATE": CURRENT_RUN_DATE,
            "RECOVERY_FAILED_RUN_ID": RECOVERY_FAILED_RUN_ID,
            "PIPELINE_LOG": str(self.temp_path / "runner.log"),
            "CALL_LOG": str(self.call_log),
            "CALL_COUNT_FILE": str(self.call_count),
            "FAIL_CALL_INDEX": str(fail_call_index),
        })
        env.pop("PIPELINE_CURRENT_STEP_FILE", None)
        env.pop("PIPELINE_STATUS_WRITER", None)
        result = subprocess.run(
            ["bash", str(self.runner)],
            cwd=str(project_root),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        calls = []
        if self.call_log.exists():
            calls = self.call_log.read_text(encoding="utf-8").splitlines()
        return result, calls

    def test_tail_runner_has_only_required_steps_in_order(self):
        text = self.runner.read_text(encoding="utf-8")
        preflight = text.index("portal_s3_backup_rotation.py")
        retention = text.index(
            "80-7_manage_09_result_retention/00_tool/manage_09_result_retention.py"
        )
        rotation = text.index("portal_s3_backup_rotation.py", preflight + 1)
        prepare = text.index("80-8_portal_s3_prepare/00_tool/portal_s3_prepare.py")
        publish = text.index("80-9_portal_s3_sync/00_tool/portal_s3_sync.py")
        positions = [preflight, retention, rotation, prepare, publish]
        self.assertEqual(positions, sorted(positions))
        self.assertEqual(text.count("run_step \\\n"), 5)
        self.assertEqual(
            text.count('--recovery-run-date "$RECOVERY_FAILED_RUN_DATE"'), 2
        )
        self.assertEqual(text.count('--recovery-run-id "$RECOVERY_FAILED_RUN_ID"'), 2)
        self.assertIn('--apply --run-date "$RUN_DATE"', text)
        self.assertNotIn("01-1_fetch_gmail", text)

    def test_tail_runner_preflights_before_all_actual_steps(self):
        result, calls = self.run_runner()
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(len(calls), 5)
        self.assertIn("portal_s3_backup_rotation.py --dry-run", calls[0])
        self.assertIn("manage_09_result_retention.py --apply", calls[1])
        self.assertIn("portal_s3_backup_rotation.py --recovery-run-date", calls[2])
        self.assertNotIn("--dry-run", calls[2])
        self.assertIn("portal_s3_prepare.py", calls[3])
        self.assertIn("portal_s3_sync.py", calls[4])
        for call in (calls[0], calls[2]):
            self.assertIn("--recovery-run-date {}".format(CURRENT_RUN_DATE), call)
            self.assertIn("--recovery-run-id {}".format(RECOVERY_FAILED_RUN_ID), call)

    def test_tail_runner_stops_at_each_failed_step(self):
        cases = (
            (1, 1),  # preflight failure: no actual step starts
            (2, 2),  # 80-7 failure: no 80-75 actual or later step starts
            (3, 3),  # 80-75 actual failure: no 80-8 or 80-9 starts
            (4, 4),  # 80-8 failure: no 80-9 starts
        )
        for fail_call_index, expected_calls in cases:
            with self.subTest(fail_call_index=fail_call_index):
                result, calls = self.run_runner(fail_call_index)
                self.assertEqual(result.returncode, 9, result.stdout)
                self.assertEqual(len(calls), expected_calls)
                self.assertIn("portal_s3_backup_rotation.py --dry-run", calls[0])
                if fail_call_index == 1:
                    self.assertFalse(any("manage_09_result_retention.py" in c for c in calls))


# ---------------------------------------------------------------------------
# (20)-(23) bootstrap
# ---------------------------------------------------------------------------


class TestBootstrap(RotationTestBase):
    def test_20_bootstrap_dry_run_passes_when_bk1_absent(self):
        os.environ.pop("RUN_DATE", None)
        os.environ.pop("RUN_ID", None)
        self.backup = {}
        self.stub_sync()
        self.stub_s3(status_runs=[(PREV_RUN_DATE, PREV_RUN_ID, "SUCCEEDED", 0)])
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
        summary = self.read_summary()
        self.assertEqual(summary["backup_status"], "SUCCEEDED")
        self.assertEqual(
            summary["immutable_execution_guard_contract_version"],
            target.IMMUTABLE_EXECUTION_GUARD_CONTRACT_VERSION,
        )

    def test_guard_failure_returns_non_zero(self):
        self.write_sync_summary(make_sync_summary(sync_status="FAILED"))
        self.stub_sync()
        self.stub_s3()
        self.assertEqual(self.run_main(), 1)
        summary = self.read_summary()
        self.assertEqual(summary["backup_status"], "FAILED")
        self.assertEqual(
            summary["immutable_execution_guard_contract_version"],
            target.IMMUTABLE_EXECUTION_GUARD_CONTRACT_VERSION,
        )
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


class TestConfirmUsesRotationSnapshot(unittest.TestCase):
    """confirmが後続80-9ではなく80-75 summary内snapshotだけを正本にすること。"""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="test_80_75_confirm_"))
        self.summary_path = self.tmp / "portal_s3_backup_rotation_summary.json"
        self.result_path = self.tmp / "confirm_result.txt"
        self.original_summary_path = confirm_target.BACKUP_SUMMARY_PATH
        self.original_result_path = confirm_target.CONFIRM_RESULT
        confirm_target.BACKUP_SUMMARY_PATH = self.summary_path
        confirm_target.CONFIRM_RESULT = self.result_path

    def tearDown(self):
        confirm_target.BACKUP_SUMMARY_PATH = self.original_summary_path
        confirm_target.CONFIRM_RESULT = self.original_result_path
        shutil.rmtree(self.tmp, ignore_errors=True)

    def make_backup_summary(self):
        return {
            "operation": "rotation",
            "mode": "apply",
            "immutable_execution_guard_contract_version": (
                target.IMMUTABLE_EXECUTION_GUARD_CONTRACT_VERSION
            ),
            "s3_source": SOURCE_URI,
            "s3_destination": DESTINATION_URI,
            "s3_destination_locked": True,
            "backup_method": "aws s3 sync CURRENT -> BK1 --delete (no CLI filters)",
            "backup_status": "SUCCEEDED",
            "current_execution_guard": {
                "validation_result": "PASS",
                "immutable_execution_guard_result": "PASS",
                "evidence_source": "stepfunctions_execution_history",
                "state_machine_arn": target.EXPECTED_STATE_MACHINE_ARN,
                "execution_arn": _execution_arn("confirm-current"),
                "current_execution_arn": _execution_arn("confirm-current"),
                "execution_status": "RUNNING",
                "execution_start_date": "2026-08-19T10:00:00+00:00",
                "run_date": CURRENT_RUN_DATE,
                "run_id": CURRENT_RUN_ID,
                "run_identity_match": True,
                "prepare_run_context_matches": 1,
                "redrive_count": 0,
                "redrive_date_present": False,
                "execution_redriven_event_present": False,
                "prior_terminal_event_present": False,
                "execution_redriven_event_count": 0,
                "prior_terminal_event_count": 0,
                "list_pages_checked": 1,
                "history_pages_checked": 1,
                "history_event_count": 2,
            },
            "verify_wait_sec": 0,
            "wait_performed": True,
            "previous_current": {
                "run_date": PREV_RUN_DATE,
                "run_id": PREV_RUN_ID,
                "run_date_source": "env",
                "run_id_source": "env",
                "destination": SOURCE_URI,
                "verified": True,
                "sync_step": "80-9_portal_s3_sync",
                "file_count": 100,
                "total_bytes": 1000,
                "status_key": (
                    f"pipeline_ses_steps/pipeline-status/{PREV_RUN_DATE}/"
                    f"{PREV_RUN_ID}/status.json"
                ),
            },
            "verify": {
                "verified": True,
                "missing_count": 0,
                "extra_count": 0,
                "size_mismatch_count": 0,
                "expected_file_count": 100,
                "actual_file_count": 100,
                "expected_total_bytes": 1000,
                "actual_total_bytes": 1000,
            },
        }

    def write_summary(self, summary):
        with open(self.summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False)

    def assert_confirm_fails(self, summary):
        self.write_summary(summary)
        with self.assertRaises(SystemExit) as caught:
            confirm_target.main()
        self.assertEqual(caught.exception.code, 1)
        self.assertIn("【結果】NG", self.result_path.read_text(encoding="utf-8"))

    def test_snapshot_100_bk1_100_passes_after_new_current_summary_120(self):
        # 後続80-9が120件で上書きされた想定のfileはconfirmの入力にしない。
        latest_summary = make_sync_summary()
        latest_summary["verify"].update(
            {
                "expected_file_count": 120,
                "actual_file_count": 120,
                "expected_total_bytes": 1200,
                "actual_total_bytes": 1200,
            }
        )
        with open(self.tmp / "latest_80_9_summary.json", "w", encoding="utf-8") as f:
            json.dump(latest_summary, f)

        self.write_summary(self.make_backup_summary())
        confirm_target.main()
        self.assertIn("【結果】OK", self.result_path.read_text(encoding="utf-8"))
        self.assertFalse(hasattr(confirm_target, "SYNC_SUMMARY_PATH"))

    def test_snapshot_100_bk1_99_fails(self):
        summary = self.make_backup_summary()
        summary["verify"]["actual_file_count"] = 99
        self.assert_confirm_fails(summary)

    def test_snapshot_bytes_mismatch_fails(self):
        summary = self.make_backup_summary()
        summary["verify"]["actual_total_bytes"] = 999
        self.assert_confirm_fails(summary)

    def test_snapshot_provenance_missing_fails(self):
        summary = self.make_backup_summary()
        summary["previous_current"].pop("run_id_source")
        self.assert_confirm_fails(summary)

    def test_prepublication_recovery_complete_audit_passes(self):
        summary = self.make_backup_summary()
        summary["recovery"] = {
            "enabled": True,
            "eligible": True,
            "recovery_mode": target.PREPUBLICATION_RECOVERY_MODE,
            "rotation_authority_run_date": PREV_RUN_DATE,
            "rotation_authority_run_id": PREV_RUN_ID,
            "current_run_date": CURRENT_RUN_DATE,
            "current_run_id": CURRENT_RUN_ID,
            "all_intervening_runs_checked": True,
            "failed_execution_list_pages_checked": 1,
            "execution_window": {
                "ordering_source": "stepfunctions_execution_metadata",
                "authority_execution_arn": _execution_arn("confirm-authority"),
                "authority_stop_date": "2026-08-19T01:00:00+00:00",
                "current_execution_arn": _execution_arn("confirm-current"),
                "current_start_date": "2026-08-19T10:00:00+00:00",
                "candidate_execution_count": 1,
                "outside_recovery_window_count": 0,
                "outside_recovery_window": [],
            },
            "publication_guard": {
                "terminal_status": "FAILED",
                "publication_boundary_step": target.PUBLICATION_BOUNDARY_STEP_NAME,
                "publication_boundary_state": target.PUBLICATION_BOUNDARY_STATE,
                "publication_boundary_reached": False,
                "failure_reason_allowlist_used": False,
            },
            "intervening_runs": [{
                "status": "FAILED",
                "validation_result": "PASS",
                "current_step": "08-1_restore_and_merge_requirement_skill_ai_matching",
                "step_order_verified": True,
                "before_publication_boundary": True,
                "publication_boundary_reached": False,
                "execution_evidence": {
                    "validation_result": "PASS",
                    "evidence_source": "stepfunctions_execution_history",
                    "execution_arn": _execution_arn("confirm-failed"),
                    "execution_status": "FAILED",
                    "execution_start_date": "2026-08-19T02:00:00+00:00",
                    "execution_stop_date": "2026-08-19T02:30:00+00:00",
                    "recovery_window_candidate": True,
                    "run_identity_match": True,
                    "redrive_count": 0,
                    "redrive_date_present": False,
                    "execution_redriven_event_present": False,
                    "history_pages_checked": 1,
                    "history_event_count": 3,
                    "publication_boundary_state": target.PUBLICATION_BOUNDARY_STATE,
                    "publication_boundary_reached": False,
                },
            }],
            "current_unchanged": {
                "verified": True,
                "manifest_inventory_match": True,
                "unchanged_since_rotation_authority": True,
            },
            "bk1_unchanged": {
                "verified": True,
                "previous_80_75_summary_match": True,
                "unchanged_since_rotation_authority": True,
            },
        }
        self.write_summary(summary)
        confirm_target.main()
        self.assertIn("【結果】OK", self.result_path.read_text(encoding="utf-8"))

    def test_prepublication_recovery_incomplete_audit_fails(self):
        summary = self.make_backup_summary()
        summary["recovery"] = {
            "enabled": True,
            "eligible": True,
            "recovery_mode": target.PREPUBLICATION_RECOVERY_MODE,
            "rotation_authority_run_date": PREV_RUN_DATE,
            "rotation_authority_run_id": PREV_RUN_ID,
            "all_intervening_runs_checked": True,
            "intervening_runs": [{"validation_result": "PASS"}],
        }
        self.assert_confirm_fails(summary)

    def test_immutable_execution_guard_redrive_evidence_fails(self):
        summary = self.make_backup_summary()
        summary["current_execution_guard"]["redrive_count"] = 1
        self.assert_confirm_fails(summary)

    def test_new_contract_missing_guard_fails(self):
        summary = self.make_backup_summary()
        summary.pop("current_execution_guard")
        self.assert_confirm_fails(summary)

    def test_new_contract_missing_redrive_count_fails(self):
        summary = self.make_backup_summary()
        summary["current_execution_guard"].pop("redrive_count")
        self.assert_confirm_fails(summary)

    def test_new_contract_guard_result_fail_fails(self):
        summary = self.make_backup_summary()
        summary["current_execution_guard"]["immutable_execution_guard_result"] = "FAIL"
        self.assert_confirm_fails(summary)

    def test_historical_legacy_summary_without_guard_passes(self):
        summary = self.make_backup_summary()
        summary.pop("immutable_execution_guard_contract_version")
        summary.pop("current_execution_guard")
        self.write_summary(summary)
        confirm_target.main()
        self.assertIn("【結果】OK", self.result_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main(verbosity=2)

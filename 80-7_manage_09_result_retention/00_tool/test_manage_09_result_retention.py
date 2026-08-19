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
    """80-7が使うlist/get/deleteだけを提供する最小のfake。"""

    def __init__(self, objects, fail_get_keys=(), fail_list=False, fail_delete_keys=()):
        self.objects = dict(objects)
        self.fail_get_keys = set(fail_get_keys)
        self.fail_list = fail_list
        self.fail_delete_keys = set(fail_delete_keys)

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _FakePaginator(self)

    def get_object(self, Bucket, Key):  # noqa: N803 - boto3互換シグネチャ
        if Key in self.fail_get_keys:
            raise RuntimeError(f"simulated GET failure: {Key}")
        return {"Body": _FakeBody(self.objects[Key])}

    def delete_object(self, Bucket, Key):  # noqa: N803 - boto3互換シグネチャ
        if Key in self.fail_delete_keys:
            raise RuntimeError(f"simulated DELETE failure: {Key}")
        self.objects.pop(Key, None)
        return {}


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
            contents = []
            for key in keys:
                rest = key[len(Prefix) :]
                if Delimiter in rest:
                    prefixes.add(Prefix + rest.split(Delimiter, 1)[0] + Delimiter)
                else:
                    contents.append({"Key": key, "Size": len(self.client.objects[key])})
            yield {
                "CommonPrefixes": [{"Prefix": p} for p in sorted(prefixes)],
                "Contents": contents,
            }
            return
        # 2件ずつ返してpaginationを再現する
        for i in range(0, len(keys), 2) or [0]:
            chunk = keys[i : i + 2]
            yield {"Contents": [{"Key": k, "Size": len(self.client.objects[k])} for k in chunk]}
        if not keys:
            yield {"Contents": []}


def status_document(run_date, run_id, status="SUCCEEDED", exit_code=0, overrides=None):
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
    document.update(overrides or {})
    return document


def status_key(run_date, run_id):
    return f"{STATUS_PREFIX}/{run_date}/{run_id}/status.json"


def status_objects(entries):
    """entries: [(run_date, run_id, status, exit_code, overrides)]"""
    objects = {}
    for run_date, run_id, status, exit_code, overrides in entries:
        document = status_document(run_date, run_id, status, exit_code, overrides)
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
            root_zip_only=False,
        )

    def set_s3(self, objects, **kwargs):
        objects = dict(objects)
        # run()のproduction contractに合わせ、currentと成功status日のroot ZIPを用意する。
        objects.setdefault("pipeline_ses_steps/mail_display_extract_20260817.zip", b"current")
        for key, raw in list(objects.items()):
            if not key.endswith("/status.json"):
                continue
            try:
                document = json.loads(raw.decode("utf-8"))
            except Exception:
                continue
            if document.get("status") == "SUCCEEDED" and document.get("exit_code") == 0:
                run_date = document.get("run_date")
                if isinstance(run_date, str):
                    objects.setdefault(
                        f"pipeline_ses_steps/mail_display_extract_{run_date}.zip", b"previous"
                    )
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

    def test_multiple_backup_generations_skip_failed_only_run_date(self):
        """世代数を増やしてもFAILED-only日はprevious successfulに採用しない。"""
        client = self.set_s3(
            status_objects(
                [
                    ("20260818", "sfn-fail", "FAILED", 1, {}),
                    ("20260817", "sfn-ok-1", "SUCCEEDED", 0, {}),
                    ("20260816", "sfn-fail-2", "FAILED", 2, {}),
                    ("20260815", "sfn-ok-2", "SUCCEEDED", 0, {}),
                ]
            )
        )
        selected = target.resolve_previous_successful_run_dates(
            client,
            BUCKET,
            STATUS_PREFIX,
            "20260819",
            self.logger,
            backup_generations=2,
        )
        self.assertEqual(selected, ["20260817", "20260815"])


class TestRootDistributionZipRetentionPlan(RetentionTestBase):
    """外部配布root ZIPのproduction rotation contractを検証する。"""

    BASE_PREFIX = "pipeline_ses_steps"
    CURRENT = "20260819"

    def test_current_only_without_previous_success_passes(self):
        current_key = "pipeline_ses_steps/mail_display_extract_20260819.zip"
        plan = target.plan_root_distribution_zip_retention(
            [current_key],
            self.BASE_PREFIX,
            self.CURRENT,
            [],
        )
        self.assertEqual(plan["keep_keys"], [current_key])
        self.assertEqual(plan["delete_candidate_keys"], [])

    def test_exact_pattern_keeps_current_and_previous_successful(self):
        keys = [
            "pipeline_ses_steps/mail_display_extract_20260819.zip",
            "pipeline_ses_steps/mail_display_extract_20260818.zip",
            "pipeline_ses_steps/mail_display_extract_20260817.zip",
            "pipeline_ses_steps/mail_display_extract_20260814.zip",
            "pipeline_ses_steps/mail_display_extract_20260230.zip",
            "pipeline_ses_steps/mail_display_format_20260413.zip",
            (
                "pipeline_ses_steps/pipeline_ses_steps/"
                "09-2_extract_high_score_mail_display/01_result/"
                "mail_display_extract_20260819.zip"
            ),
        ]

        plan = target.plan_root_distribution_zip_retention(
            keys,
            self.BASE_PREFIX,
            self.CURRENT,
            ["20260818"],
        )

        self.assertEqual(plan["backup_generations"], 1)
        self.assertEqual(plan["keep_run_dates"], ["20260818", "20260819"])
        self.assertEqual(
            plan["keep_keys"],
            [
                "pipeline_ses_steps/mail_display_extract_20260818.zip",
                "pipeline_ses_steps/mail_display_extract_20260819.zip",
            ],
        )
        self.assertEqual(
            plan["delete_candidate_keys"],
            [
                "pipeline_ses_steps/mail_display_extract_20260814.zip",
                "pipeline_ses_steps/mail_display_extract_20260817.zip",
            ],
        )
        self.assertNotIn(
            "pipeline_ses_steps/mail_display_format_20260413.zip", plan["target_keys"]
        )
        self.assertNotIn(
            "pipeline_ses_steps/mail_display_extract_20260230.zip", plan["target_keys"]
        )
        self.assertEqual(len(plan["target_keys"]), 4)

    def test_current_is_protected_and_failed_old_zip_is_delete_candidate_when_previous_exists(self):
        client = self.set_s3(
            status_objects(
                [
                    ("20260818", "sfn-ok", "SUCCEEDED", 0, {}),
                    ("20260817", "sfn-failed-only", "FAILED", 1, {}),
                ]
            )
        )
        previous = target.resolve_previous_successful_run_dates(
            client,
            BUCKET,
            STATUS_PREFIX,
            self.CURRENT,
            self.logger,
        )
        plan = target.plan_root_distribution_zip_retention(
            [
                "pipeline_ses_steps/mail_display_extract_20260819.zip",
                "pipeline_ses_steps/mail_display_extract_20260818.zip",
                "pipeline_ses_steps/mail_display_extract_20260817.zip",
            ],
            self.BASE_PREFIX,
            self.CURRENT,
            previous,
        )
        self.assertIn(
            "pipeline_ses_steps/mail_display_extract_20260819.zip", plan["keep_keys"]
        )
        self.assertIn(
            "pipeline_ses_steps/mail_display_extract_20260817.zip",
            plan["delete_candidate_keys"],
        )

    def test_no_previous_success_with_one_old_canonical_fails_closed(self):
        with self.assertRaises(target.RetentionError):
            target.plan_root_distribution_zip_retention(
                [
                    "pipeline_ses_steps/mail_display_extract_20260819.zip",
                    "pipeline_ses_steps/mail_display_extract_20260818.zip",
                ],
                self.BASE_PREFIX,
                self.CURRENT,
                [],
            )

    def test_no_previous_success_with_multiple_old_canonical_fails_closed(self):
        with self.assertRaises(target.RetentionError):
            target.plan_root_distribution_zip_retention(
                [
                    "pipeline_ses_steps/mail_display_extract_20260819.zip",
                    "pipeline_ses_steps/mail_display_extract_20260818.zip",
                    "pipeline_ses_steps/mail_display_extract_20260817.zip",
                ],
                self.BASE_PREFIX,
                self.CURRENT,
                [],
            )

    def test_failed_only_status_with_old_canonical_fails_closed(self):
        client = self.set_s3(
            status_objects([("20260818", "sfn-failed-only", "FAILED", 1, {})])
        )
        previous = target.resolve_previous_successful_run_dates(
            client,
            BUCKET,
            STATUS_PREFIX,
            self.CURRENT,
            self.logger,
        )
        self.assertEqual(previous, [])
        with self.assertRaises(target.RetentionError):
            target.plan_root_distribution_zip_retention(
                [
                    "pipeline_ses_steps/mail_display_extract_20260819.zip",
                    "pipeline_ses_steps/mail_display_extract_20260818.zip",
                ],
                self.BASE_PREFIX,
                self.CURRENT,
                previous,
            )

    def test_invalid_status_with_old_canonical_fails_before_planning(self):
        broken = status_document("20260818", "sfn-broken")
        del broken["exit_code"]
        client = self.set_s3(
            {
                status_key("20260818", "sfn-broken"): json.dumps(broken).encode(
                    "utf-8"
                )
            }
        )
        with self.assertRaises(target.RetentionError):
            previous = target.resolve_previous_successful_run_dates(
                client,
                BUCKET,
                STATUS_PREFIX,
                self.CURRENT,
                self.logger,
            )
            target.plan_root_distribution_zip_retention(
                ["pipeline_ses_steps/mail_display_extract_20260818.zip"],
                self.BASE_PREFIX,
                self.CURRENT,
                previous,
            )

    def test_legacy_only_without_previous_success_does_not_fail(self):
        plan = target.plan_root_distribution_zip_retention(
            [
                "pipeline_ses_steps/mail_display_format_20260413.zip",
                "pipeline_ses_steps/arbitrary.zip",
                (
                    "pipeline_ses_steps/pipeline_ses_steps/"
                    "09-2_extract_high_score_mail_display/01_result/"
                    "mail_display_extract_20260818.zip"
                ),
            ],
            self.BASE_PREFIX,
            self.CURRENT,
            [],
        )
        self.assertEqual(plan["target_keys"], [])
        self.assertEqual(plan["delete_candidate_keys"], [])

    def test_current_and_invalid_date_without_previous_passes(self):
        current_key = "pipeline_ses_steps/mail_display_extract_20260819.zip"
        invalid_key = "pipeline_ses_steps/mail_display_extract_20260230.zip"
        plan = target.plan_root_distribution_zip_retention(
            [current_key, invalid_key],
            self.BASE_PREFIX,
            self.CURRENT,
            [],
        )
        self.assertEqual(plan["target_keys"], [current_key])
        self.assertEqual(plan["keep_keys"], [current_key])
        self.assertEqual(plan["delete_candidate_keys"], [])

    def test_invalid_date_only_is_noncanonical_and_does_not_fail(self):
        invalid_key = "pipeline_ses_steps/mail_display_extract_20260230.zip"
        plan = target.plan_root_distribution_zip_retention(
            [invalid_key],
            self.BASE_PREFIX,
            self.CURRENT,
            [],
        )
        self.assertEqual(plan["target_keys"], [])
        self.assertEqual(plan["keep_keys"], [])
        self.assertEqual(plan["delete_candidate_keys"], [])

    def test_backup_generations_can_expand_without_changing_pattern(self):
        plan = target.plan_root_distribution_zip_retention(
            [
                "pipeline_ses_steps/mail_display_extract_20260819.zip",
                "pipeline_ses_steps/mail_display_extract_20260818.zip",
                "pipeline_ses_steps/mail_display_extract_20260817.zip",
                "pipeline_ses_steps/mail_display_extract_20260816.zip",
            ],
            self.BASE_PREFIX,
            self.CURRENT,
            ["20260818", "20260817", "20260816"],
            backup_generations=2,
        )
        self.assertEqual(plan["keep_run_dates"], ["20260817", "20260818", "20260819"])
        self.assertEqual(
            plan["delete_candidate_keys"],
            ["pipeline_ses_steps/mail_display_extract_20260816.zip"],
        )

    def test_dry_run_then_apply_deletes_only_old_valid_canonical(self):
        objects = {
            "pipeline_ses_steps/mail_display_extract_20260819.zip": b"current",
            "pipeline_ses_steps/mail_display_extract_20260818.zip": b"previous",
            "pipeline_ses_steps/mail_display_extract_20260817.zip": b"old",
            "pipeline_ses_steps/mail_display_extract_20260230.zip": b"invalid-date",
            "pipeline_ses_steps/mail_display_format_20260413.zip": b"legacy",
            "pipeline_ses_steps/arbitrary.zip": b"arbitrary",
            (
                "pipeline_ses_steps/pipeline_ses_steps/"
                "09-2_extract_high_score_mail_display/01_result/"
                "mail_display_extract_20260817.zip"
            ): b"current-mirror",
        }
        client = FakeS3Client(objects)

        dry = target.execute_root_distribution_zip_retention(
            client,
            BUCKET,
            self.BASE_PREFIX,
            self.CURRENT,
            ["20260818"],
            False,
            self.logger,
        )
        self.assertEqual(
            dry["delete_candidate_keys"],
            ["pipeline_ses_steps/mail_display_extract_20260817.zip"],
        )
        self.assertEqual(client.objects, objects)

        applied = target.execute_root_distribution_zip_retention(
            client,
            BUCKET,
            self.BASE_PREFIX,
            self.CURRENT,
            ["20260818"],
            True,
            self.logger,
        )
        self.assertTrue(applied["verified"])
        self.assertEqual(applied["deleted_keys"], dry["delete_candidate_keys"])
        self.assertNotIn("pipeline_ses_steps/mail_display_extract_20260817.zip", client.objects)
        for key in (
            "pipeline_ses_steps/mail_display_extract_20260819.zip",
            "pipeline_ses_steps/mail_display_extract_20260818.zip",
            "pipeline_ses_steps/mail_display_extract_20260230.zip",
            "pipeline_ses_steps/mail_display_format_20260413.zip",
            "pipeline_ses_steps/arbitrary.zip",
        ):
            self.assertIn(key, client.objects)
        self.assertIn(
            "pipeline_ses_steps/pipeline_ses_steps/"
            "09-2_extract_high_score_mail_display/01_result/"
            "mail_display_extract_20260817.zip",
            client.objects,
        )

    def test_missing_current_or_previous_fails_before_delete(self):
        old_key = "pipeline_ses_steps/mail_display_extract_20260817.zip"
        client = FakeS3Client(
            {
                "pipeline_ses_steps/mail_display_extract_20260818.zip": b"previous",
                old_key: b"old",
            }
        )
        with self.assertRaises(target.RetentionError):
            target.execute_root_distribution_zip_retention(
                client,
                BUCKET,
                self.BASE_PREFIX,
                self.CURRENT,
                ["20260818"],
                True,
                self.logger,
            )
        self.assertIn(old_key, client.objects)

    def test_delete_failure_is_not_swallowed(self):
        old_key = "pipeline_ses_steps/mail_display_extract_20260817.zip"
        client = FakeS3Client(
            {
                "pipeline_ses_steps/mail_display_extract_20260819.zip": b"current",
                "pipeline_ses_steps/mail_display_extract_20260818.zip": b"previous",
                old_key: b"old",
            },
            fail_delete_keys=[old_key],
        )
        with self.assertRaises(target.RetentionError):
            target.execute_root_distribution_zip_retention(
                client,
                BUCKET,
                self.BASE_PREFIX,
                self.CURRENT,
                ["20260818"],
                True,
                self.logger,
            )
        self.assertIn(old_key, client.objects)


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


class TestStatusSchemaValidation(RetentionTestBase):
    """
    99-9_publish_pipeline_status が書く schema_version 1.0 の契約に合わせた検証。
    1項目でも不正なら成功runとして採用せず、削除もしない。
    """

    def setUp(self):
        super().setUp()
        self.root = self.build_fixture()

    def assert_rejected(self, objects):
        self.set_s3(objects)
        before = self.present_run_dates(self.root, "20260817")
        with self.assertRaises(target.RetentionError):
            target.run(self.make_args(self.root, "20260817", apply_mode=True), self.logger)
        self.assertEqual(self.present_run_dates(self.root, "20260817"), before)

    def test_valid_status_is_adopted(self):
        self.set_s3(status_objects([("20260814", "sfn-ok", "SUCCEEDED", 0, {})]))
        summary = target.run(self.make_args(self.root, "20260817"), self.logger)
        self.assertEqual(summary["previous_successful_run_date"], "20260814")

    def test_null_run_id_is_rejected(self):
        self.assert_rejected(status_objects([("20260814", "sfn-a", "SUCCEEDED", 0, {"run_id": None})]))

    def test_empty_run_id_is_rejected(self):
        self.assert_rejected(status_objects([("20260814", "sfn-a", "SUCCEEDED", 0, {"run_id": "   "})]))

    def test_run_id_mismatch_with_key_is_rejected(self):
        self.assert_rejected(
            status_objects([("20260814", "sfn-a", "SUCCEEDED", 0, {"run_id": "sfn-other"})])
        )

    def test_run_date_mismatch_with_key_is_rejected(self):
        self.assert_rejected(
            status_objects([("20260814", "sfn-a", "SUCCEEDED", 0, {"run_date": "20260813"})])
        )

    def test_impossible_calendar_date_is_rejected(self):
        # key側も document側も 20260230（実在しない日付）
        document = status_document("20260814", "sfn-a")
        document["run_date"] = "20260230"
        self.assert_rejected(
            {status_key("20260230", "sfn-a"): json.dumps(document).encode("utf-8")}
        )

    def test_unsupported_schema_version_is_rejected(self):
        self.assert_rejected(
            status_objects([("20260814", "sfn-a", "SUCCEEDED", 0, {"schema_version": "2.0"})])
        )

    def test_missing_schema_version_is_rejected(self):
        document = status_document("20260814", "sfn-a")
        del document["schema_version"]
        self.assert_rejected({status_key("20260814", "sfn-a"): json.dumps(document).encode("utf-8")})

    def test_failed_status_is_not_adopted(self):
        self.set_s3(
            status_objects(
                [("20260814", "sfn-a", "FAILED", 5, {}), ("20260813", "sfn-b", "SUCCEEDED", 0, {})]
            )
        )
        summary = target.run(self.make_args(self.root, "20260817"), self.logger)
        self.assertEqual(summary["previous_successful_run_date"], "20260813")

    def test_non_zero_exit_code_is_not_adopted(self):
        # SUCCEEDED + exit_code != 0 は正本schemaの契約違反なのでFAILさせる
        self.assert_rejected(status_objects([("20260814", "sfn-a", "SUCCEEDED", 9, {})]))

    def test_malformed_status_key_is_rejected(self):
        self.assert_rejected(
            {f"{STATUS_PREFIX}/20260814/a/b/status.json": json.dumps(
                status_document("20260814", "a")
            ).encode("utf-8")}
        )


class TestWalkFailClosed(RetentionTestBase):
    def setUp(self):
        super().setUp()
        self.root = self.build_fixture()
        self.set_s3(status_objects([("20260814", "sfn-a", "SUCCEEDED", 0, {})]))
        self.locked = []

    def tearDown(self):
        for path in self.locked:
            try:
                path.chmod(0o755)
            except OSError:
                pass
        super().tearDown()

    def _lock(self, path):
        path.chmod(0o000)
        self.locked.append(path)

    def test_unreadable_result_dir_fails_without_deleting(self):
        result_dir = self.root / "09-1_mail_display_format" / "01_result"
        self._lock(result_dir)
        with self.assertRaises(target.RetentionError):
            target.run(self.make_args(self.root, "20260817", apply_mode=True), self.logger)
        result_dir.chmod(0o755)
        # 他stepの成果物も1件も削除されていない
        self.assertEqual(self.present_run_dates(self.root, "20260817"), ALL_RUN_DATES)

    def test_unreadable_subdir_in_delete_candidate_fails_without_deleting(self):
        sub = (
            self.root
            / "09-1_mail_display_format"
            / "01_result"
            / "mail_display_format_20260418"
            / "sub"
        )
        self._lock(sub)
        with self.assertRaises(target.RetentionError):
            target.run(self.make_args(self.root, "20260817", apply_mode=True), self.logger)
        sub.chmod(0o755)
        self.assertEqual(self.present_run_dates(self.root, "20260817"), ALL_RUN_DATES)
        self.assertTrue((sub / "b.txt").is_file())


class TestCurrentRunDateProtection(RetentionTestBase):
    """current / previous successful の成果物がapplyで一切変化しないこと。"""

    CURRENT = "20260817"
    PREVIOUS = "20260814"

    def setUp(self):
        super().setUp()
        self.root = self.build_fixture(run_dates=ALL_RUN_DATES + [self.CURRENT])
        self.set_s3(status_objects([(rd, f"sfn-{rd}", "SUCCEEDED", 0, {}) for rd in ALL_RUN_DATES]))

    def snapshot(self, run_dates):
        """保持対象RUN_DATEの全path・sizeを収集する。"""
        artifacts, _holds = target.scan_artifacts(self.root, self.CURRENT, self.logger)
        snapshot = {}
        for artifact in artifacts:
            if artifact["run_date"] not in run_dates:
                continue
            path = artifact["path"]
            if artifact["kind"] == "file":
                snapshot[str(path.relative_to(self.root))] = path.stat().st_size
            else:
                for child in sorted(path.rglob("*")):
                    if child.is_file():
                        snapshot[str(child.relative_to(self.root))] = child.stat().st_size
        return snapshot

    def test_current_and_previous_are_untouched_after_apply(self):
        keep = {self.CURRENT, self.PREVIOUS}
        before = self.snapshot(keep)
        self.assertTrue(before)

        summary = target.run(self.make_args(self.root, self.CURRENT, apply_mode=True), self.logger)
        self.assertEqual(summary["keep_run_dates"], [self.PREVIOUS, self.CURRENT])

        after = self.snapshot(keep)
        self.assertEqual(before, after)
        self.assertEqual(
            self.present_run_dates(self.root, self.CURRENT), sorted(keep)
        )

    def test_rerun_after_partial_delete_failure_keeps_current_and_previous(self):
        keep = {self.CURRENT, self.PREVIOUS}
        before = self.snapshot(keep)

        original_unlink = Path.unlink
        state = {"count": 0}

        def flaky_unlink(self_path, *unlink_args, **unlink_kwargs):
            state["count"] += 1
            if state["count"] > 50:
                raise OSError("simulated I/O error during unlink")
            return original_unlink(self_path, *unlink_args, **unlink_kwargs)

        Path.unlink = flaky_unlink
        try:
            with self.assertRaises(OSError):
                target.run(self.make_args(self.root, self.CURRENT, apply_mode=True), self.logger)
        finally:
            Path.unlink = original_unlink

        # 途中失敗後も保持対象は無傷
        self.assertEqual(self.snapshot(keep), before)

        # 再実行で残った古い対象だけが削除される
        summary = target.run(self.make_args(self.root, self.CURRENT, apply_mode=True), self.logger)
        self.assertGreater(summary["deleted_files"], 0)
        self.assertEqual(self.snapshot(keep), before)
        self.assertEqual(self.present_run_dates(self.root, self.CURRENT), sorted(keep))

        again = target.run(self.make_args(self.root, self.CURRENT, apply_mode=True), self.logger)
        self.assertEqual(again["deleted_files"], 0)

    def run_main_apply(self):
        """main() 経由でapplyし、終了コードを返す（非正常終了を直接確認するため）。"""
        sys_argv = sys.argv
        sys.argv = [
            "manage_09_result_retention.py",
            "--apply",
            "--run-date",
            self.CURRENT,
            "--pipeline-root",
            str(self.root),
            "--bucket",
            BUCKET,
            "--status-prefix",
            STATUS_PREFIX,
            "--region",
            "ap-northeast-1",
        ]
        try:
            return target.main()
        finally:
            sys.argv = sys_argv

    def test_rerun_after_rmdir_failure_keeps_current_and_previous(self):
        """
        Path.rmdir() が途中で1回失敗しても、
        current / previous successful を壊さず、再実行で残ったold成果物だけを削除できること。
        """
        keep = {self.CURRENT, self.PREVIOUS}
        before = self.snapshot(keep)
        self.assertTrue(before)
        old_run_dates_before = [
            d for d in self.present_run_dates(self.root, self.CURRENT) if d not in keep
        ]
        self.assertTrue(old_run_dates_before)
        old_files_before = self.snapshot(old_run_dates_before)
        self.assertTrue(old_files_before)

        original_rmdir = Path.rmdir
        state = {"count": 0}

        def flaky_rmdir(self_path, *rmdir_args, **rmdir_kwargs):
            # 何件かのold成果物を削除し終えた「途中」で1回だけ失敗させる
            state["count"] += 1
            if state["count"] == 3:
                raise OSError("simulated I/O error during rmdir")
            return original_rmdir(self_path, *rmdir_args, **rmdir_kwargs)

        # 1回目: rmdir途中失敗 → 非正常終了(exit=1)
        Path.rmdir = flaky_rmdir
        try:
            self.assertEqual(self.run_main_apply(), 1)
        finally:
            Path.rmdir = original_rmdir
        self.assertEqual(state["count"], 3, msg="rmdirは失敗地点で停止すること")

        # 「例外が出ただけ」ではなく、実際に部分削除された状態であること
        old_files_after_failure = self.snapshot(old_run_dates_before)
        self.assertLess(
            len(old_files_after_failure),
            len(old_files_before),
            msg="rmdir失敗前にold成果物が一部削除されている（部分削除状態）こと",
        )
        self.assertTrue(
            [d for d in self.present_run_dates(self.root, self.CURRENT) if d not in keep],
            msg="再実行で削除すべきold成果物が残っていること",
        )

        # 失敗後も current / previous の全path・sizeは不変
        self.assertEqual(self.snapshot(keep), before)

        # 2回目: 通常applyで残ったold成果物だけが削除される
        summary = target.run(self.make_args(self.root, self.CURRENT, apply_mode=True), self.logger)
        self.assertGreater(summary["removed_dirs"], 0)
        self.assertEqual(self.snapshot(keep), before)
        self.assertEqual(self.present_run_dates(self.root, self.CURRENT), sorted(keep))
        for run_date in old_run_dates_before:
            self.assertNotIn(run_date, self.present_run_dates(self.root, self.CURRENT))

        # 3回目: 削除対象0件で正常終了（冪等）
        again = target.run(self.make_args(self.root, self.CURRENT, apply_mode=True), self.logger)
        self.assertEqual(again["planned_delete_files"], 0)
        self.assertEqual(again["deleted_files"], 0)
        self.assertEqual(again["removed_dirs"], 0)
        self.assertEqual(self.snapshot(keep), before)


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

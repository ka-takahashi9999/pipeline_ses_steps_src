"""
80-8_portal_s3_prepare focused test

本番成果物には触れず、一時ディレクトリ上のfixtureで検証する。
S3アクセス・full Pipeline実行は行わない。

実行:
  python3 80-8_portal_s3_prepare/00_tool/test_portal_s3_prepare.py
"""

import argparse
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import portal_s3_prepare as target  # noqa: E402
from common.json_utils import read_jsonl_as_list  # noqa: E402
from common.logger import get_logger  # noqa: E402


class PrepareTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="test_80_8_"))
        self.root = self.tmp / "pipeline"
        self.step_dir = self.tmp / "out"
        self.logger = get_logger("test_80-8")
        self._build_fixture()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write(self, relative_path, content="data"):
        path = self.root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def _build_fixture(self):
        self._write("01-1_fetch_gmail/01_result/fetch_gmail.jsonl", "a")
        self._write("01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl", "SECRET")
        self._write("03-10_extract_project_location/01_result/loc.jsonl", "bb")
        self._write("06-80_duplicate_proposal_check/01_result/dup.jsonl", "ccc")
        self._write("06-80_duplicate_proposal_check/01_result/dup.jsonl.bak_20260424", "old")
        self._write("04-1_fetch_skillsheets_text/01_result/.gitkeep", "")
        self._write("04-1_fetch_skillsheets_text/01_result/sheet.jsonl", "dddd")
        self._write("09-1_mail_display_format/01_result/mail_display_format_20260814/a.txt", "e")
        # 自身の01_result（同期対象外）
        self._write("80-7_manage_09_result_retention/01_result/summary.json", "{}")
        self._write("80-8_portal_s3_prepare/01_result/portal_s3_manifest.jsonl", "{}")
        self._write("80-9_portal_s3_sync/01_result/sync.json", "{}")
        # step形式でないディレクトリ / 01_resultを持たないstep
        self._write("common/file_utils.py", "x")
        self._write("00_pipeline/01_result/pipeline_script_exec.log", "log")
        self._write("99_reference/note.md", "x")
        (self.root / "99-9_publish_pipeline_status" / "00_tool").mkdir(parents=True)
        # step直下の01_result外ファイル（対象外）
        self._write("01-1_fetch_gmail/00_tool/fetch_gmail.py", "code")
        self._write("01-1_fetch_gmail/02_confirm/confirm_result.txt", "x")

    def make_args(self):
        return argparse.Namespace(pipeline_root=str(self.root), step_dir=str(self.step_dir))

    def run_main(self):
        sys_argv = sys.argv
        sys.argv = [
            "portal_s3_prepare.py",
            "--pipeline-root",
            str(self.root),
            "--step-dir",
            str(self.step_dir),
        ]
        try:
            return target.main()
        finally:
            sys.argv = sys_argv

    def manifest_paths(self):
        records = read_jsonl_as_list(str(self.step_dir / "01_result" / target.MANIFEST_FILENAME))
        return [r["relative_path"] for r in records]


class TestPositiveSelection(PrepareTestBase):
    def test_only_step_result_dirs_are_selected(self):
        self.assertEqual(self.run_main(), 0)
        paths = self.manifest_paths()
        self.assertEqual(
            paths,
            [
                "01-1_fetch_gmail/01_result/fetch_gmail.jsonl",
                "03-10_extract_project_location/01_result/loc.jsonl",
                "04-1_fetch_skillsheets_text/01_result/sheet.jsonl",
                "06-80_duplicate_proposal_check/01_result/dup.jsonl",
                "09-1_mail_display_format/01_result/mail_display_format_20260814/a.txt",
            ],
        )

    def test_manifest_is_sorted_and_deterministic(self):
        self.assertEqual(self.run_main(), 0)
        first = self.manifest_paths()
        self.assertEqual(self.run_main(), 0)
        second = self.manifest_paths()
        self.assertEqual(first, second)
        self.assertEqual(first, sorted(first))

    def test_explicit_exclusions_are_not_in_manifest(self):
        self.assertEqual(self.run_main(), 0)
        paths = self.manifest_paths()
        self.assertNotIn("01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl", paths)
        self.assertFalse([p for p in paths if p.endswith(".gitkeep")])
        self.assertFalse([p for p in paths if ".bak_" in p])

    def test_self_steps_and_non_step_dirs_are_excluded(self):
        self.assertEqual(self.run_main(), 0)
        paths = self.manifest_paths()
        for prefix in target.SELF_STEP_DIRS:
            self.assertFalse([p for p in paths if p.startswith(prefix + "/")], prefix)
        self.assertFalse([p for p in paths if p.startswith("00_pipeline/")])
        self.assertFalse([p for p in paths if p.startswith("common/")])
        self.assertFalse([p for p in paths if "/00_tool/" in p or "/02_confirm/" in p])

    def test_summary_counts_match_manifest(self):
        import json

        self.assertEqual(self.run_main(), 0)
        with open(self.step_dir / "01_result" / target.SUMMARY_FILENAME, encoding="utf-8") as f:
            summary = json.load(f)
        records = read_jsonl_as_list(str(self.step_dir / "01_result" / target.MANIFEST_FILENAME))
        self.assertEqual(summary["file_count"], len(records))
        self.assertEqual(summary["total_bytes"], sum(r["size"] for r in records))
        self.assertEqual(summary["excluded_counts"]["gitkeep"], 1)
        self.assertEqual(summary["excluded_counts"]["bak"], 1)
        self.assertEqual(summary["excluded_counts"]["explicit_path"], 1)
        for name in target.SELF_STEP_DIRS:
            self.assertNotIn(name, summary["selected_step_dirs"])

    def test_actual_local_sizes_match_manifest(self):
        self.assertEqual(self.run_main(), 0)
        records = read_jsonl_as_list(str(self.step_dir / "01_result" / target.MANIFEST_FILENAME))
        for record in records:
            self.assertEqual(
                (self.root / record["relative_path"]).stat().st_size, record["size"]
            )


class TestFailureCases(PrepareTestBase):
    def test_symlink_file_fails(self):
        link = self.root / "03-10_extract_project_location" / "01_result" / "linked.jsonl"
        link.symlink_to(self.root / "01-1_fetch_gmail" / "01_result" / "fetch_gmail.jsonl")
        with self.assertRaises(target.PrepareError):
            target.run(self.make_args(), self.logger)
        self.assertEqual(self.run_main(), 1)

    def test_symlink_directory_fails(self):
        link = self.root / "03-10_extract_project_location" / "01_result" / "linkdir"
        link.symlink_to(self.root / "01-1_fetch_gmail" / "01_result")
        with self.assertRaises(target.PrepareError):
            target.run(self.make_args(), self.logger)

    def test_non_regular_file_fails(self):
        fifo = self.root / "03-10_extract_project_location" / "01_result" / "pipe.fifo"
        os.mkfifo(str(fifo))
        with self.assertRaises(target.PrepareError):
            target.run(self.make_args(), self.logger)

    def test_secret_like_filename_fails(self):
        self._write("03-10_extract_project_location/01_result/gmail_credentials.json", "x")
        with self.assertRaises(target.PrepareError):
            target.run(self.make_args(), self.logger)

    def test_zero_target_files_fails(self):
        empty_root = self.tmp / "empty"
        (empty_root / "01-1_fetch_gmail" / "01_result").mkdir(parents=True)
        args = argparse.Namespace(pipeline_root=str(empty_root), step_dir=str(self.step_dir))
        with self.assertRaises(target.PrepareError):
            target.run(args, self.logger)

    def test_no_step_dirs_fails(self):
        empty_root = self.tmp / "nosteps"
        (empty_root / "common").mkdir(parents=True)
        args = argparse.Namespace(pipeline_root=str(empty_root), step_dir=str(self.step_dir))
        with self.assertRaises(target.PrepareError):
            target.run(args, self.logger)

    def test_unsafe_relative_path_is_rejected(self):
        for bad in ("/abs/path", "a/../b", "a//b", "a/./b", ""):
            with self.assertRaises(target.PrepareError, msg=bad):
                target.validate_relative_path(bad)

    def test_size_change_during_scan_fails(self):
        entries = [
            {
                "relative_path": "01-1_fetch_gmail/01_result/fetch_gmail.jsonl",
                "size": 999,
                "_abs_path": str(self.root / "01-1_fetch_gmail/01_result/fetch_gmail.jsonl"),
                "_mtime_ns": 0,
            }
        ]
        with self.assertRaises(target.PrepareError):
            target.recheck_stable(entries)

    def test_stat_failure_is_reported(self):
        entries = [
            {
                "relative_path": "missing/file.jsonl",
                "size": 1,
                "_abs_path": str(self.root / "missing" / "file.jsonl"),
                "_mtime_ns": 0,
            }
        ]
        with self.assertRaises(target.PrepareError):
            target.recheck_stable(entries)


if __name__ == "__main__":
    unittest.main(verbosity=2)

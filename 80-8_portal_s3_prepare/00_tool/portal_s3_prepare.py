#!/usr/bin/env python3
"""
80-8_portal_s3_prepare

Portal向けS3同期の対象fileを確定し、manifestを作成する。
本stepはS3を一切変更しない（ローカル走査とmanifest生成のみ）。

対象:
  <pipeline root>/XX-X_<step名>/01_result/**  の全regular file
  （positive selection。XX-X_ 形式かつ 01_result を持つstepだけ）

除外:
  80-7 / 80-8 / 80-9 自身の 01_result
  */01_result/.gitkeep
  */01_result/*.bak_*
  01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl （資格情報様文字列を含むため必須除外）

出力:
  01_result/portal_s3_manifest.jsonl        1行1file / relative_path辞書順
  01_result/portal_s3_prepare_summary.json  件数・bytes・選定step一覧

usage:
  portal_s3_prepare.py [--pipeline-root PATH]
"""

import argparse
import fnmatch
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_execution_time  # noqa: E402
from common.json_utils import write_jsonl  # noqa: E402
from common.logger import get_logger  # noqa: E402

STEP_NAME = "80-8_portal_s3_prepare"
STEP_DIR = Path(__file__).resolve().parents[1]

MANIFEST_FILENAME = "portal_s3_manifest.jsonl"
SUMMARY_FILENAME = "portal_s3_prepare_summary.json"

RESULT_DIR_NAME = "01_result"
STEP_DIR_RE = re.compile(r"^\d{2}-\d+_[A-Za-z0-9][A-Za-z0-9._-]*$")

# 自身の01_resultはPortal同期対象外
SELF_STEP_DIRS: Tuple[str, ...] = (
    "80-7_manage_09_result_retention",
    "80-8_portal_s3_prepare",
    "80-9_portal_s3_sync",
)

EXCLUDE_BASENAMES: Tuple[str, ...] = (".gitkeep",)
EXCLUDE_BASENAME_GLOBS: Tuple[str, ...] = ("*.bak_*",)
EXCLUDE_RELATIVE_PATHS: Tuple[str, ...] = (
    "01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl",
)

# 新たな秘密情報様ファイルを検出したら異常終了する（Portalへ流さない）
SECRET_NAME_TOKENS: Tuple[str, ...] = (
    "credential",
    "secret",
    "password",
    "passwd",
    "api_key",
    "apikey",
    "private_key",
    "id_rsa",
    ".pem",
    ".p12",
    ".pfx",
)


class PrepareError(Exception):
    """manifestを作らずに異常終了すべき状態。"""


def walk_error(exc: OSError) -> None:
    """
    os.walk の走査失敗を握りつぶさない。
    1ディレクトリでも走査できなければ、不完全なmanifestを正常生成せず即FAILさせる。
    """
    raise PrepareError(f"01_result の走査に失敗しました（manifestを生成しません）: {exc}")


def select_step_dirs(root: Path) -> List[str]:
    """positive selection: XX-X_ 形式かつ 01_result を持つstepディレクトリ名を返す。"""
    selected: List[str] = []
    try:
        entries = sorted(root.iterdir(), key=lambda p: p.name)
    except OSError as exc:
        raise PrepareError(f"pipeline rootの走査に失敗しました: {root} ({exc})") from exc
    for entry in entries:
        if entry.is_symlink() or not entry.is_dir():
            continue
        if not STEP_DIR_RE.match(entry.name):
            continue
        if entry.name in SELF_STEP_DIRS:
            continue
        result_dir = entry / RESULT_DIR_NAME
        if not result_dir.is_dir() or result_dir.is_symlink():
            continue
        selected.append(entry.name)
    return selected


def is_excluded(relative_path: str, basename: str) -> str:
    """除外理由を返す。除外対象でなければ空文字を返す。"""
    if relative_path in EXCLUDE_RELATIVE_PATHS:
        return "explicit_path"
    if basename in EXCLUDE_BASENAMES:
        return "gitkeep"
    for pattern in EXCLUDE_BASENAME_GLOBS:
        if fnmatch.fnmatch(basename, pattern):
            return "bak"
    return ""


def validate_relative_path(relative_path: str) -> None:
    if not relative_path:
        raise PrepareError("relative_pathが空です")
    if relative_path.startswith("/"):
        raise PrepareError(f"absolute pathを検出しました: {relative_path}")
    parts = relative_path.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise PrepareError(f"不正なpath componentを検出しました: {relative_path}")


def check_secret_name(relative_path: str, basename: str) -> None:
    lowered = basename.lower()
    for token in SECRET_NAME_TOKENS:
        if token in lowered:
            raise PrepareError(
                f"秘密情報様のファイル名を検出しました（Portal同期を中止します）: {relative_path}"
            )


def collect_entries(root: Path, step_dirs: List[str], logger) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    entries: List[Dict[str, Any]] = []
    seen: Dict[str, str] = {}
    excluded_counts: Dict[str, int] = {"gitkeep": 0, "bak": 0, "explicit_path": 0}

    for step in step_dirs:
        result_dir = root / step / RESULT_DIR_NAME
        for dirpath, dirnames, filenames in os.walk(
            str(result_dir), followlinks=False, onerror=walk_error
        ):
            current = Path(dirpath)
            for name in sorted(dirnames):
                if (current / name).is_symlink():
                    raise PrepareError(f"symlinkディレクトリを検出しました: {current / name}")
            for name in sorted(filenames):
                child = current / name
                relative_path = str(child.relative_to(root))
                validate_relative_path(relative_path)

                if child.is_symlink():
                    raise PrepareError(f"symlinkを検出しました: {relative_path}")

                reason = is_excluded(relative_path, name)
                if reason:
                    excluded_counts[reason] += 1
                    continue

                if not child.is_file():
                    raise PrepareError(f"regular fileではないエントリを検出しました: {relative_path}")

                check_secret_name(relative_path, name)

                try:
                    stat_result = child.stat()
                except OSError as exc:
                    raise PrepareError(f"statに失敗しました: {relative_path} ({exc})") from exc

                if relative_path in seen:
                    raise PrepareError(f"relative_pathが重複しています: {relative_path}")
                seen[relative_path] = str(child)

                entries.append(
                    {
                        "relative_path": relative_path,
                        "size": stat_result.st_size,
                        "_abs_path": str(child),
                        "_mtime_ns": stat_result.st_mtime_ns,
                    }
                )

    return entries, excluded_counts


def recheck_stable(entries: List[Dict[str, Any]]) -> None:
    """走査途中のsize変化を検出する（変化していたら異常終了）。"""
    for entry in entries:
        path = Path(entry["_abs_path"])
        if path.is_symlink() or not path.is_file():
            raise PrepareError(f"走査中にファイルが変化しました: {entry['relative_path']}")
        try:
            stat_result = path.stat()
        except OSError as exc:
            raise PrepareError(f"再statに失敗しました: {entry['relative_path']} ({exc})") from exc
        if stat_result.st_size != entry["size"]:
            raise PrepareError(
                f"走査中にsizeが変化しました: {entry['relative_path']} "
                f"({entry['size']} -> {stat_result.st_size})"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pipeline-root",
        default=str(project_root),
        help="Pipeline root（focused test用）",
    )
    parser.add_argument(
        "--step-dir",
        default=str(STEP_DIR),
        help="出力先stepディレクトリ（focused test用）",
    )
    return parser.parse_args()


def run(args: argparse.Namespace, logger) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    root = Path(args.pipeline_root).resolve()
    if not root.is_dir():
        raise PrepareError(f"pipeline rootが存在しません: {root}")

    step_dirs = select_step_dirs(root)
    if not step_dirs:
        raise PrepareError("Portal同期対象のstepディレクトリが0件です")
    logger.info(f"対象step: {len(step_dirs)}件")

    entries, excluded_counts = collect_entries(root, step_dirs, logger)
    if not entries:
        raise PrepareError("Portal同期対象fileが0件です")

    recheck_stable(entries)

    entries.sort(key=lambda e: e["relative_path"])
    total_bytes = sum(entry["size"] for entry in entries)

    summary = {
        "step": STEP_NAME,
        "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline_root": str(root),
        "selected_step_dirs": step_dirs,
        "selected_step_dir_count": len(step_dirs),
        "file_count": len(entries),
        "total_bytes": total_bytes,
        "excluded_counts": excluded_counts,
        "excluded_relative_paths": list(EXCLUDE_RELATIVE_PATHS),
        "manifest_filename": MANIFEST_FILENAME,
    }
    return summary, entries


def main() -> int:
    logger = get_logger(STEP_NAME)
    args = parse_args()
    started = time.time()
    dirs = ensure_result_dirs(args.step_dir)

    try:
        summary, entries = run(args, logger)
    except PrepareError as exc:
        logger.error(f"[NG] {exc}")
        return 1
    except Exception as exc:  # noqa: BLE001 - 想定外例外も握りつぶさずFAILさせる
        logger.error(f"[NG] 想定外エラー: {type(exc).__name__}: {exc}")
        return 1

    manifest_path = dirs["result"] / MANIFEST_FILENAME
    summary_path = dirs["result"] / SUMMARY_FILENAME

    records = [
        {"relative_path": entry["relative_path"], "size": entry["size"]} for entry in entries
    ]
    write_jsonl(str(manifest_path), records)

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")

    logger.info(f"manifest: {manifest_path}")
    logger.info(f"files={summary['file_count']} / bytes={summary['total_bytes']}")
    for record in records[:3]:
        logger.info(f"代表: {record['relative_path']} ({record['size']} bytes)")

    write_execution_time(
        str(dirs["execution_time"]), STEP_NAME, time.time() - started, record_count=len(records)
    )
    logger.ok("完了")
    return 0


if __name__ == "__main__":
    sys.exit(main())

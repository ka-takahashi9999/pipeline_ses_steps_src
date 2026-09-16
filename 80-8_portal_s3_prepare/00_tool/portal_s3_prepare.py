#!/usr/bin/env python3
"""
80-8_portal_s3_prepare

Portal向けS3同期の対象fileを確定し、manifestを作成する。
本stepはS3を一切変更しない（ローカル走査とmanifest生成のみ）。

対象:
  <pipeline root>/XX-X_<step名>/01_result/**  の全regular file
  （positive selection。XX-X_ 形式かつ 01_result を持つstepだけ）

除外:
  80-7 / 80-75 / 80-8 / 80-9 自身の 01_result
  */01_result/_batch_runtime/**
  */01_result/_legacy_runtime/**
  */01_result/_execution_context/**
  07-1_requirement_skill_ai_matching/01_result/concurrent_checkpoints/**
  99-1_multi_item_mail_lab/**
  08-1_restore_and_merge_requirement_skill_ai_matching/01_result/bk_merged_*
  03-2_extract_project_age/01_result/99_default_with_age_signal*
  06-80_duplicate_proposal_check/01_result/bk_duplicate_proposal_check_diff_file.jsonl
  */01_result/.gitkeep
  */01_result/*.bak_*
  historical log（error_*.log / nohup*.log）
    ※中央basename ruleで除外する。error JSONL（99_error_*.jsonl 等）は業務成果物のため除外しない
  08-1 Success Cache（Portal成果物ではなくSTATEのため除外）

含める（本方式で変更した点）:
  01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl
    → 通常の 01_result 成果物としてCURRENTへ載せる（RUN_DATE専用partitionは作らない）

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
    "80-75_portal_s3_backup_rotation",
    "80-8_portal_s3_prepare",
    "80-9_portal_s3_sync",
)

# Portal公開対象ではない検証専用step。01_result全体を除外する。
NON_PUBLIC_STEP_DIRS: Tuple[str, ...] = ("99-1_multi_item_mail_lab",)

EXCLUDE_BASENAMES: Tuple[str, ...] = (".gitkeep",)
EXCLUDE_BASENAME_GLOBS: Tuple[str, ...] = ("*.bak_*",)

# Pipeline内部の排他・再実行制御用であり、Portal公開成果物ではない。
# os.walk の dirnames から除外し、配下を走査しない。
INTERNAL_RUNTIME_DIRNAMES: Tuple[str, ...] = (
    "_batch_runtime",
    "_legacy_runtime",
    "_execution_context",
)

# 特定step配下の内部実行成果物。os.walkのdirnamesから除外し、配下を公開しない。
EXCLUDE_RESULT_DIRNAMES_BY_STEP: Dict[str, Dict[str, str]] = {
    "07-1_requirement_skill_ai_matching": {
        "concurrent_checkpoints": "concurrent_checkpoints",
    },
}

# historical log の中央basename rule。
# `.log` 拡張子に限定するため、error JSONL（99_error_*.jsonl / *_error_*.jsonl 等）や
# 「処理対象外」JSONLといった業務成果物は除外されない（"*error*" のような広い除外はしない）。
EXCLUDE_LOG_BASENAME_GLOBS: Tuple[str, ...] = ("error_*.log", "nohup*.log")

# STATE / Portal非対象の明示除外。
# Success CacheはPortal成果物ではなく 06-80/07-1/08-1 のSTATEのため載せない
# （Success Cache本体の動作・保存方式は変更しない）。
EXCLUDE_RELATIVE_PATHS: Tuple[str, ...] = (
    "08-1_restore_and_merge_requirement_skill_ai_matching/01_result/"
    "success_cache_requirement_skill_ai_matching.jsonl",
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

UNKNOWN_PROVENANCE = "unknown"
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
RUN_DATE_RE = re.compile(r"^\d{8}$")


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
        if entry.name in SELF_STEP_DIRS or entry.name in NON_PUBLIC_STEP_DIRS:
            continue
        result_dir = entry / RESULT_DIR_NAME
        if not result_dir.is_dir() or result_dir.is_symlink():
            continue
        selected.append(entry.name)
    return selected


def is_excluded(relative_path: str, basename: str) -> str:
    """除外理由を返す。除外対象でなければ空文字を返す。"""
    parts = relative_path.split("/")
    if parts[0] in NON_PUBLIC_STEP_DIRS:
        return "test_step"
    if len(parts) >= 3:
        step = parts[0]
        result_child = parts[2]
        if (
            step == "08-1_restore_and_merge_requirement_skill_ai_matching"
            and fnmatch.fnmatch(result_child, "bk_merged_*")
        ):
            return "backup"
        if step == "03-2_extract_project_age" and fnmatch.fnmatch(
            result_child, "99_default_with_age_signal*"
        ):
            return "confirm_helper"
    if (
        relative_path
        == "06-80_duplicate_proposal_check/01_result/"
        "bk_duplicate_proposal_check_diff_file.jsonl"
    ):
        return "duplicate_diff_backup"
    if relative_path in EXCLUDE_RELATIVE_PATHS:
        return "explicit_path"
    if basename in EXCLUDE_BASENAMES:
        return "gitkeep"
    for pattern in EXCLUDE_BASENAME_GLOBS:
        if fnmatch.fnmatch(basename, pattern):
            return "bak"
    for pattern in EXCLUDE_LOG_BASENAME_GLOBS:
        if fnmatch.fnmatch(basename, pattern):
            return "historical_log"
    return ""


def count_tree_files(path: Path) -> int:
    """公開対象外tree内のregular file数を数える。走査失敗・symlinkはFAILする。"""
    count = 0
    for dirpath, dirnames, filenames in os.walk(str(path), followlinks=False, onerror=walk_error):
        current = Path(dirpath)
        for name in sorted(dirnames):
            child = current / name
            if child.is_symlink():
                raise PrepareError(f"公開対象外treeにsymlinkディレクトリがあります: {child}")
        for name in sorted(filenames):
            child = current / name
            if child.is_symlink() or not child.is_file():
                raise PrepareError(f"公開対象外treeにregular file以外があります: {child}")
            count += 1
    return count


def resolve_provenance(args: argparse.Namespace) -> Dict[str, str]:
    """CLI引数、環境変数、既定値の順にrun_date/run_idを解決する。"""
    provenance: Dict[str, str] = {}
    for key, cli_value, env_name, pattern in (
        ("run_date", getattr(args, "run_date", None), "RUN_DATE", RUN_DATE_RE),
        ("run_id", getattr(args, "run_id", None), "RUN_ID", RUN_ID_RE),
    ):
        env_value = os.environ.get(env_name)
        if cli_value is not None and str(cli_value).strip():
            raw = str(cli_value).strip()
            source = "cli"
        elif env_value is not None and env_value.strip():
            raw = env_value.strip()
            source = "env"
        else:
            raw = ""
            source = "default"
        if not raw:
            provenance[key] = UNKNOWN_PROVENANCE
            provenance[f"{key}_source"] = source
            continue
        if not pattern.match(raw):
            raise PrepareError(f"{env_name} の形式が不正です: {raw!r}")
        provenance[key] = raw
        provenance[f"{key}_source"] = source
    return provenance


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
    excluded_counts: Dict[str, int] = {
        "gitkeep": 0,
        "bak": 0,
        "explicit_path": 0,
        "historical_log": 0,
        "internal_runtime": 0,
        "concurrent_checkpoints": 0,
        "test_step": 0,
        "backup": 0,
        "confirm_helper": 0,
        "duplicate_diff_backup": 0,
    }

    for step in NON_PUBLIC_STEP_DIRS:
        result_dir = root / step / RESULT_DIR_NAME
        if result_dir.is_dir() and not result_dir.is_symlink():
            excluded_counts["test_step"] += count_tree_files(result_dir)

    for step in step_dirs:
        result_dir = root / step / RESULT_DIR_NAME
        for dirpath, dirnames, filenames in os.walk(
            str(result_dir), followlinks=False, onerror=walk_error
        ):
            current = Path(dirpath)
            if current == result_dir:
                excluded_dir_reasons = {
                    name: "internal_runtime" for name in INTERNAL_RUNTIME_DIRNAMES
                }
                excluded_dir_reasons.update(EXCLUDE_RESULT_DIRNAMES_BY_STEP.get(step, {}))
                for name in sorted(set(dirnames) & set(excluded_dir_reasons)):
                    excluded_counts[excluded_dir_reasons[name]] += count_tree_files(current / name)
                dirnames[:] = [name for name in dirnames if name not in excluded_dir_reasons]
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
    parser.add_argument("--run-date", default=None, help="RUN_DATE（既定は環境変数RUN_DATE）")
    parser.add_argument("--run-id", default=None, help="RUN_ID（既定は環境変数RUN_ID）")
    return parser.parse_args()


def run(args: argparse.Namespace, logger) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    root = Path(args.pipeline_root).resolve()
    if not root.is_dir():
        raise PrepareError(f"pipeline rootが存在しません: {root}")

    provenance = resolve_provenance(args)

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
        "run_date": provenance["run_date"],
        "run_date_source": provenance["run_date_source"],
        "run_id": provenance["run_id"],
        "run_id_source": provenance["run_id_source"],
        "pipeline_root": str(root),
        "selected_step_dirs": step_dirs,
        "selected_step_dir_count": len(step_dirs),
        "file_count": len(entries),
        "total_bytes": total_bytes,
        "excluded_counts": excluded_counts,
        "excluded_step_dirs": list(NON_PUBLIC_STEP_DIRS),
        "excluded_relative_paths": list(EXCLUDE_RELATIVE_PATHS),
        "excluded_basename_globs": list(EXCLUDE_BASENAME_GLOBS),
        "excluded_log_basename_globs": list(EXCLUDE_LOG_BASENAME_GLOBS),
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

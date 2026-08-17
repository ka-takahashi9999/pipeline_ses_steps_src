#!/usr/bin/env python3
"""
80-9_portal_s3_sync

80-8 が作成した manifest と同じ集合を、Portal専用prefixへ同期する。

  ローカル : <pipeline root>/XX-X_<step名>/01_result/**
  S3      : s3://<bucket>/<PORTAL_S3_PREFIX>/XX-X_<step名>/01_result/**

方式:
- `aws s3 sync --delete` （prefix全削除→全uploadはしない）
- AWS CLI は argv 配列で subprocess 実行する（eval / bash -c / sh -c は使わない）
- sync成功後 PORTAL_S3_VERIFY_WAIT_SEC 秒待ってから完全性verifyを行う
- verify は manifest を期待値とし、S3を全ページLISTして path集合とsizeを比較する
- missing / extra / size mismatch / LIST失敗 はすべて異常終了

pipeline-logs / pipeline-status / 既存S3直下ZIP はPortal専用prefix外のため一切触らない。

usage:
  portal_s3_sync.py [--dry-run]
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_execution_time  # noqa: E402
from common.json_utils import read_jsonl_as_list  # noqa: E402
from common.logger import get_logger  # noqa: E402
from common.pipeline_s3_env import get_config_value, load_pipeline_s3_config  # noqa: E402

STEP_NAME = "80-9_portal_s3_sync"
STEP_DIR = Path(__file__).resolve().parents[1]

PREPARE_STEP_DIR_NAME = "80-8_portal_s3_prepare"
MANIFEST_FILENAME = "portal_s3_manifest.jsonl"
PREPARE_SUMMARY_FILENAME = "portal_s3_prepare_summary.json"
SYNC_SUMMARY_FILENAME = "portal_s3_sync_summary.json"

AWS_BIN = "/usr/bin/aws"
RESULT_DIR_NAME = "01_result"

# 80-8 の明示除外と同じ集合になるようCLI filterを構成する
EXCLUDE_FILTERS: Tuple[str, ...] = (
    "*/01_result/.gitkeep",
    "*.bak_*",
    "01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl",
    "80-7_manage_09_result_retention/*",
    "80-8_portal_s3_prepare/*",
    "80-9_portal_s3_sync/*",
)

SAMPLE_LIMIT = 3


class SyncError(Exception):
    """S3を変更しない / verify不成立で異常終了すべき状態。"""


def parse_wait_seconds(raw: Any) -> int:
    """PORTAL_S3_VERIFY_WAIT_SEC を非負整数として解釈する。"""
    text = str(raw).strip()
    if not text.isdigit():
        raise SyncError(f"PORTAL_S3_VERIFY_WAIT_SEC は非負整数のみ指定できます: {raw!r}")
    return int(text)


def normalize_prefix(value: str) -> str:
    normalized = value.strip().strip("/")
    if not normalized or any(part in ("", ".", "..") for part in normalized.split("/")):
        raise SyncError(f"PORTAL_S3_PREFIX が不正です: {value!r}")
    return normalized


def load_manifest(manifest_path: Path) -> Dict[str, int]:
    if not manifest_path.is_file():
        raise SyncError(f"80-8 manifestが存在しません: {manifest_path}")
    expected: Dict[str, int] = {}
    for record in read_jsonl_as_list(str(manifest_path)):
        relative_path = record.get("relative_path")
        size = record.get("size")
        if not isinstance(relative_path, str) or not relative_path:
            raise SyncError(f"manifestのrelative_pathが不正です: {record!r}")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise SyncError(f"manifestのsizeが不正です: {record!r}")
        if relative_path in expected:
            raise SyncError(f"manifestのrelative_pathが重複しています: {relative_path}")
        expected[relative_path] = size
    if not expected:
        raise SyncError(f"80-8 manifestが0件です: {manifest_path}")
    return expected


def load_selected_step_dirs(summary_path: Path) -> List[str]:
    """sync対象prefixを80-8 summaryの選定結果から取得する（選定ロジックを二重管理しない）。"""
    if not summary_path.is_file():
        raise SyncError(f"80-8 summaryが存在しません: {summary_path}")
    with open(summary_path, "r", encoding="utf-8") as f:
        summary = json.load(f)
    step_dirs = summary.get("selected_step_dirs")
    if not isinstance(step_dirs, list) or not step_dirs:
        raise SyncError(f"80-8 summaryのselected_step_dirsが不正です: {summary_path}")
    for name in step_dirs:
        if not isinstance(name, str) or not name or "/" in name or name.startswith("."):
            raise SyncError(f"80-8 summaryのstep名が不正です: {name!r}")
    return list(step_dirs)


def build_sync_argv(
    root: Path, bucket: str, portal_prefix: str, region: str, step_dirs: List[str], dry_run: bool
) -> List[str]:
    """aws s3 sync の argv を組み立てる（shell文字列は使わない）。"""
    argv: List[str] = [
        AWS_BIN,
        "s3",
        "sync",
        str(root),
        f"s3://{bucket}/{portal_prefix}/",
        "--delete",
        "--no-follow-symlinks",
        "--only-show-errors",
        "--region",
        region,
        "--exclude",
        "*",
    ]
    for step in step_dirs:
        argv.extend(["--include", f"{step}/{RESULT_DIR_NAME}/*"])
    for pattern in EXCLUDE_FILTERS:
        argv.extend(["--exclude", pattern])
    if dry_run:
        argv.append("--dryrun")
    return argv


def run_sync(argv: List[str], logger) -> None:
    logger.info(f"aws s3 sync 実行: {len(argv)} args / dest={argv[4]}")
    completed = subprocess.run(  # noqa: S603 - argv配列固定・shell未使用
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=False,
        check=False,
    )
    output = completed.stdout.decode("utf-8", errors="replace").strip()
    if output:
        for line in output.splitlines()[:SAMPLE_LIMIT]:
            logger.info(f"aws出力: {line}")
    if completed.returncode != 0:
        raise SyncError(f"aws s3 sync が失敗しました (exit={completed.returncode})")
    logger.ok("aws s3 sync 成功")


def build_s3_client(region: str):
    """boto3 S3クライアントを生成する（テストから差し替え可能にするため関数化）。"""
    import boto3

    return boto3.client("s3", region_name=region)


def list_portal_objects(s3_client, bucket: str, portal_prefix: str) -> Dict[str, int]:
    """Portal専用prefixを全ページLISTし、{relative_path: size} を返す。"""
    actual: Dict[str, int] = {}
    prefix = f"{portal_prefix}/"
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if page.get("IsTruncated") and not page.get("NextContinuationToken"):
                raise SyncError("S3 LISTのpaginationが不正です（IsTruncatedだが継続トークンなし）")
            for obj in page.get("Contents") or []:
                key = obj.get("Key", "")
                if not key.startswith(prefix):
                    raise SyncError(f"prefix外のkeyが返却されました: {key}")
                relative_path = key[len(prefix) :]
                if not relative_path or relative_path.endswith("/"):
                    continue
                size = obj.get("Size")
                if not isinstance(size, int):
                    raise SyncError(f"S3 objectのSizeが不正です: {key}")
                if relative_path in actual:
                    raise SyncError(f"S3 LISTでkeyが重複しました: {key}")
                actual[relative_path] = size
    except SyncError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise SyncError(f"S3 LISTに失敗しました: {exc}") from exc
    return actual


def verify(expected: Dict[str, int], actual: Dict[str, int], logger) -> Dict[str, Any]:
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    mismatched = sorted(
        path for path in set(expected) & set(actual) if expected[path] != actual[path]
    )

    expected_bytes = sum(expected.values())
    actual_bytes = sum(actual.values())

    for path in missing[:SAMPLE_LIMIT]:
        logger.error(f"[NG] missing: {path}")
    for path in extra[:SAMPLE_LIMIT]:
        logger.error(f"[NG] extra: {path}")
    for path in mismatched[:SAMPLE_LIMIT]:
        logger.error(f"[NG] size mismatch: {path} local={expected[path]} s3={actual[path]}")

    result = {
        "expected_file_count": len(expected),
        "actual_file_count": len(actual),
        "expected_total_bytes": expected_bytes,
        "actual_total_bytes": actual_bytes,
        "missing_count": len(missing),
        "extra_count": len(extra),
        "size_mismatch_count": len(mismatched),
        "missing_samples": missing[:SAMPLE_LIMIT],
        "extra_samples": extra[:SAMPLE_LIMIT],
        "size_mismatch_samples": mismatched[:SAMPLE_LIMIT],
    }
    result["verified"] = (
        not missing
        and not extra
        and not mismatched
        and len(expected) == len(actual)
        and expected_bytes == actual_bytes
    )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="aws s3 sync --dryrun で実行し、S3を変更せず wait / verify もしない",
    )
    parser.add_argument("--pipeline-root", default=str(project_root), help="Pipeline root（focused test用）")
    parser.add_argument("--step-dir", default=str(STEP_DIR), help="出力先stepディレクトリ（focused test用）")
    parser.add_argument("--prepare-dir", default=None, help="80-8 stepディレクトリ（focused test用）")
    return parser.parse_args()


def run(args: argparse.Namespace, logger) -> Dict[str, Any]:
    root = Path(args.pipeline_root).resolve()
    if not root.is_dir():
        raise SyncError(f"pipeline rootが存在しません: {root}")

    prepare_dir = Path(args.prepare_dir) if args.prepare_dir else (root / PREPARE_STEP_DIR_NAME)
    manifest_path = prepare_dir / RESULT_DIR_NAME / MANIFEST_FILENAME
    prepare_summary_path = prepare_dir / RESULT_DIR_NAME / PREPARE_SUMMARY_FILENAME

    config = load_pipeline_s3_config()
    bucket = get_config_value(config, "PIPELINE_S3_BUCKET")
    region = get_config_value(config, "PIPELINE_AWS_REGION")
    portal_prefix = normalize_prefix(get_config_value(config, "PORTAL_S3_PREFIX"))
    wait_seconds = parse_wait_seconds(get_config_value(config, "PORTAL_S3_VERIFY_WAIT_SEC"))

    expected = load_manifest(manifest_path)
    step_dirs = load_selected_step_dirs(prepare_summary_path)

    logger.info(f"同期先: s3://{bucket}/{portal_prefix}/ (region={region})")
    logger.info(f"expected files={len(expected)} / bytes={sum(expected.values())}")
    logger.info(f"PORTAL_S3_VERIFY_WAIT_SEC={wait_seconds}")

    argv = build_sync_argv(root, bucket, portal_prefix, region, step_dirs, args.dry_run)
    run_sync(argv, logger)

    summary: Dict[str, Any] = {
        "step": STEP_NAME,
        "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": "dry-run" if args.dry_run else "apply",
        "pipeline_root": str(root),
        "s3_destination": f"s3://{bucket}/{portal_prefix}/",
        "sync_method": "aws s3 sync --delete",
        "sync_status": "SUCCEEDED",
        "verify_wait_sec": wait_seconds,
        "wait_performed": False,
        "selected_step_dir_count": len(step_dirs),
        "manifest_path": str(manifest_path),
    }

    if args.dry_run:
        logger.warn("dry-runのため wait / verify は実施しません（S3未変更）")
        summary["verify"] = {"verified": False, "skipped_reason": "dry-run"}
        return summary

    logger.info(f"verify前 wait {wait_seconds}秒")
    time.sleep(wait_seconds)
    summary["wait_performed"] = True

    s3_client = build_s3_client(region)
    actual = list_portal_objects(s3_client, bucket, portal_prefix)
    verify_result = verify(expected, actual, logger)
    summary["verify"] = verify_result

    if not verify_result["verified"]:
        raise SyncError(
            "verifyに失敗しました "
            f"(missing={verify_result['missing_count']} / extra={verify_result['extra_count']} / "
            f"size_mismatch={verify_result['size_mismatch_count']})"
        )

    logger.ok(
        f"verify成功: files={verify_result['actual_file_count']} / "
        f"bytes={verify_result['actual_total_bytes']}"
    )
    return summary


def main() -> int:
    logger = get_logger(STEP_NAME)
    args = parse_args()
    started = time.time()
    dirs = ensure_result_dirs(args.step_dir)
    summary_path = dirs["result"] / SYNC_SUMMARY_FILENAME

    summary: Dict[str, Any]
    exit_code = 0
    try:
        summary = run(args, logger)
    except SyncError as exc:
        logger.error(f"[NG] {exc}")
        summary = {
            "step": STEP_NAME,
            "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "dry-run" if args.dry_run else "apply",
            "sync_status": "FAILED",
            "error_message": str(exc),
        }
        exit_code = 1
    except Exception as exc:  # noqa: BLE001 - 想定外例外も握りつぶさずFAILさせる
        logger.error(f"[NG] 想定外エラー: {type(exc).__name__}: {exc}")
        summary = {
            "step": STEP_NAME,
            "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "dry-run" if args.dry_run else "apply",
            "sync_status": "FAILED",
            "error_message": f"{type(exc).__name__}: {exc}",
        }
        exit_code = 1

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    logger.info(f"summary: {summary_path}")

    write_execution_time(
        str(dirs["execution_time"]),
        STEP_NAME,
        time.time() - started,
        record_count=int(summary.get("verify", {}).get("actual_file_count", 0) or 0),
    )
    if exit_code == 0:
        logger.ok("完了")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

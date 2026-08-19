#!/usr/bin/env python3
"""
80-9_portal_s3_sync

80-8 が作成した manifest と同じ集合を、Portal専用prefixへ同期する。

  ローカル : <pipeline root>/XX-X_<step名>/01_result/**
  S3      : s3://technoverse/pipeline_ses_steps/pipeline_ses_steps/XX-X_<step名>/01_result/**

方式:
- destination安全ロック: bucket / base prefix / portal prefix / 完全URI を期待値と
  完全一致比較し、1つでも異なれば sync開始前にFAILする（startswith判定はしない）。
  設定値の書き換えで `--delete` の範囲を上位prefixへ広げられない構造にする。
- staging tree方式: 80-8 manifestに載っているファイルだけで一時staging treeを構築し、
  AWS CLIのinclude/excludeフィルタを使わずに `aws s3 sync --delete` する。
  staging集合 = manifest集合 = S3に存在すべき集合 を保証する。
- AWS CLI は argv 配列で subprocess 実行する（eval / bash -c / sh -c は使わない）
- sync成功後 PORTAL_S3_VERIFY_WAIT_SEC 秒待ってから完全性verifyを行う
- verify は manifest を期待値とし、S3を全ページLISTして path集合とsizeを比較する。
  directory markerを含め、prefix自身を除く全objectをactual集合に含める。
- missing / extra / size mismatch / LIST失敗 はすべて異常終了
- summaryへ provenance（run_date / run_id / s3_destination / sync_status / verified /
  expected・actual の count/bytes / missing / extra / size mismatch）を保持する。
  この情報を 80-75（CURRENT -> bk1 rotation）の previous CURRENT 正常性guardが参照する。

pipeline-logs / pipeline-status / 既存S3直下ZIP はPortal専用prefix外のため一切触らない。

usage:
  portal_s3_sync.py [--dry-run]
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

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

# ---- destination安全ロック（唯一許可する同期先） ----------------------------
# ここを設定ファイル・環境変数で上書きできてはならない。
EXPECTED_BUCKET = "technoverse"
EXPECTED_BASE_PREFIX = "pipeline_ses_steps"
EXPECTED_PORTAL_LEAF = "pipeline_ses_steps"
EXPECTED_PORTAL_PREFIX = f"{EXPECTED_BASE_PREFIX}/{EXPECTED_PORTAL_LEAF}"
EXPECTED_DESTINATION_URI = f"s3://{EXPECTED_BUCKET}/{EXPECTED_PORTAL_PREFIX}/"

STAGE_DIR_PREFIX = ".portal_s3_stage_"

SAMPLE_LIMIT = 3

# ---- provenance（80-75 rotation guardが参照する） ----------------------------
# managed runner（run_full_pipeline_managed.sh）が RUN_ID / RUN_DATE をexportする。
# 取得できない場合は null にせず、既定値 + *_source="default" で「managed run由来ではない」
# ことが分かる形で残す。80-75は *_source="env" のsummaryのみをbackup対象として扱う。
UNKNOWN_PROVENANCE = "unknown"
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
RUN_DATE_RE = re.compile(r"^\d{8}$")


class SyncError(Exception):
    """S3を変更しない / verify不成立で異常終了すべき状態。"""


# ---------------------------------------------------------------------------
# destination安全ロック
# ---------------------------------------------------------------------------


def lock_destination(bucket: Any, base_prefix: Any, portal_prefix: Any) -> str:
    """
    bucket / base prefix / portal prefix / 完全URI を期待値と完全一致比較する。
    1項目でも一致しなければ SyncError を送出する（sync開始前にFAILさせる）。

    生の設定値をそのまま比較するため、末尾スラッシュ・空文字・上位prefix・`..` は
    すべて不一致として拒否される。startswith判定は使わない。
    """
    for name, value, expected in (
        ("PIPELINE_S3_BUCKET", bucket, EXPECTED_BUCKET),
        ("PIPELINE_S3_BASE_PREFIX", base_prefix, EXPECTED_BASE_PREFIX),
        ("PORTAL_S3_PREFIX", portal_prefix, EXPECTED_PORTAL_PREFIX),
    ):
        if not isinstance(value, str) or value != expected:
            raise SyncError(
                f"destination安全ロック違反: {name} が期待値と一致しません "
                f"(actual={value!r} / expected={expected!r})"
            )

    # 期待値定数そのものが壊れていないかも構造として検証する
    components = portal_prefix.split("/")
    if components != [EXPECTED_BASE_PREFIX, EXPECTED_PORTAL_LEAF]:
        raise SyncError(f"destination安全ロック違反: portal prefixの構造が不正です: {portal_prefix!r}")
    if any(component in ("", ".", "..") for component in components):
        raise SyncError(f"destination安全ロック違反: portal prefixに不正componentがあります: {portal_prefix!r}")

    destination_uri = f"s3://{bucket}/{portal_prefix}/"
    if destination_uri != EXPECTED_DESTINATION_URI:
        raise SyncError(
            f"destination安全ロック違反: 同期先URIが期待値と一致しません "
            f"(actual={destination_uri!r} / expected={EXPECTED_DESTINATION_URI!r})"
        )
    return destination_uri


def resolve_provenance(args: argparse.Namespace) -> Dict[str, str]:
    """
    run_date / run_id を解決する。
    優先順位: CLI引数 -> 環境変数（managed runnerがexport） -> 既定値。
    形式不正は黙って捨てず RUN_ID/RUN_DATE 不正としてFAILさせる。
    """
    provenance: Dict[str, str] = {}
    for key, cli_value, env_name, pattern in (
        ("run_date", getattr(args, "run_date", None), "RUN_DATE", RUN_DATE_RE),
        ("run_id", getattr(args, "run_id", None), "RUN_ID", RUN_ID_RE),
    ):
        raw = cli_value or os.environ.get(env_name) or ""
        raw = raw.strip()
        if not raw:
            provenance[key] = UNKNOWN_PROVENANCE
            provenance[f"{key}_source"] = "default"
            continue
        if not pattern.match(raw):
            raise SyncError(f"{env_name} の形式が不正です: {raw!r}")
        provenance[key] = raw
        provenance[f"{key}_source"] = "env"
    return provenance


def parse_wait_seconds(raw: Any) -> int:
    """PORTAL_S3_VERIFY_WAIT_SEC を非負整数として解釈する。"""
    text = str(raw).strip()
    if not text.isdigit():
        raise SyncError(f"PORTAL_S3_VERIFY_WAIT_SEC は非負整数のみ指定できます: {raw!r}")
    return int(text)


# ---------------------------------------------------------------------------
# manifest
# ---------------------------------------------------------------------------


def validate_relative_path(relative_path: Any) -> None:
    if not isinstance(relative_path, str) or not relative_path:
        raise SyncError(f"relative_pathが不正です: {relative_path!r}")
    if relative_path.startswith("/") or os.path.isabs(relative_path):
        raise SyncError(f"absolute pathは許可しません: {relative_path}")
    components = relative_path.split("/")
    if any(component in ("", ".", "..") for component in components):
        raise SyncError(f"不正なpath componentを検出しました: {relative_path}")


def load_manifest(manifest_path: Path) -> Dict[str, int]:
    if not manifest_path.is_file():
        raise SyncError(f"80-8 manifestが存在しません: {manifest_path}")
    expected: Dict[str, int] = {}
    for record in read_jsonl_as_list(str(manifest_path)):
        relative_path = record.get("relative_path")
        size = record.get("size")
        validate_relative_path(relative_path)
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise SyncError(f"manifestのsizeが不正です: {record!r}")
        if relative_path in expected:
            raise SyncError(f"manifestのrelative_pathが重複しています: {relative_path}")
        expected[relative_path] = size
    if not expected:
        raise SyncError(f"80-8 manifestが0件です: {manifest_path}")
    return expected


def load_selected_step_dirs(summary_path: Path) -> List[str]:
    """80-8が選定したstep一覧（summary記録用）。"""
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


# ---------------------------------------------------------------------------
# staging tree
# ---------------------------------------------------------------------------


def create_staging_root(step_dir: Path) -> Path:
    """step配下に一時staging rootを作成する（同一filesystemでhard linkできるようにする）。"""
    step_dir.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=STAGE_DIR_PREFIX, dir=str(step_dir)))


def _assert_inside(child: Path, parent: Path, label: str) -> None:
    try:
        Path(os.path.realpath(str(child))).relative_to(Path(os.path.realpath(str(parent))))
    except ValueError as exc:
        raise SyncError(f"{label}の外を指しています: {child}") from exc


def build_staging_tree(
    root: Path, expected: Dict[str, int], stage_root: Path, logger
) -> Dict[str, Any]:
    """
    manifestに載っているファイルだけでstaging treeを構築する。
    1件でも検証に失敗したら SyncError を送出し、S3 syncへ進ませない。
    """
    staged = 0
    staged_bytes = 0
    linked = 0
    copied = 0
    seen = set()

    for relative_path in sorted(expected):
        size = expected[relative_path]
        validate_relative_path(relative_path)
        if relative_path in seen:
            raise SyncError(f"staging対象のpathが重複しています: {relative_path}")
        seen.add(relative_path)

        source = root / relative_path
        if source.is_symlink():
            raise SyncError(f"symlinkはstagingへ入れません: {relative_path}")
        if not source.is_file():
            raise SyncError(f"regular fileではありません: {relative_path}")
        _assert_inside(source, root, "source root")

        actual_size = source.stat().st_size
        if actual_size != size:
            raise SyncError(
                f"manifestとsourceのsizeが一致しません: {relative_path} "
                f"(manifest={size} / source={actual_size})"
            )

        destination = stage_root / relative_path
        if destination.exists() or destination.is_symlink():
            raise SyncError(f"staging先が既に存在します: {relative_path}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        _assert_inside(destination.parent, stage_root, "staging root")

        try:
            os.link(str(source), str(destination))
            linked += 1
        except OSError:
            # 別filesystem等でhard linkできない場合はcopyへfallbackする
            shutil.copy2(str(source), str(destination))
            copied += 1

        if destination.is_symlink() or not destination.is_file():
            raise SyncError(f"staging結果がregular fileではありません: {relative_path}")
        if destination.stat().st_size != size:
            raise SyncError(f"staging結果のsizeが一致しません: {relative_path}")

        staged += 1
        staged_bytes += size

    if staged != len(expected):
        raise SyncError(f"staging件数がmanifestと一致しません: {staged} != {len(expected)}")

    verify_staging_tree(stage_root, expected)
    logger.info(f"staging構築: files={staged} / bytes={staged_bytes} (link={linked} / copy={copied})")
    return {"file_count": staged, "total_bytes": staged_bytes, "linked": linked, "copied": copied}


def _walk_error(exc: OSError) -> None:
    raise SyncError(f"staging treeの走査に失敗しました: {exc}")


def verify_staging_tree(stage_root: Path, expected: Dict[str, int]) -> None:
    """staging treeの実体がmanifestと完全一致することを確認する。"""
    actual: Dict[str, int] = {}
    for dirpath, dirnames, filenames in os.walk(str(stage_root), followlinks=False, onerror=_walk_error):
        current = Path(dirpath)
        for name in dirnames:
            if (current / name).is_symlink():
                raise SyncError(f"staging treeにsymlinkディレクトリがあります: {current / name}")
        for name in filenames:
            child = current / name
            if child.is_symlink() or not child.is_file():
                raise SyncError(f"staging treeにregular file以外があります: {child}")
            actual[str(child.relative_to(stage_root))] = child.stat().st_size
    if actual != expected:
        missing = sorted(set(expected) - set(actual))[:SAMPLE_LIMIT]
        extra = sorted(set(actual) - set(expected))[:SAMPLE_LIMIT]
        raise SyncError(f"staging treeがmanifestと一致しません (missing={missing} / extra={extra})")


def cleanup_staging(stage_root: Optional[Path], logger) -> None:
    """staging treeを削除する（hard linkのため元ファイルは消えない）。"""
    if stage_root is None:
        return
    path = Path(stage_root)
    if path.is_symlink() or not path.is_dir() or not path.name.startswith(STAGE_DIR_PREFIX):
        logger.warn(f"staging rootとして認識できないためcleanupしません: {path}")
        return
    shutil.rmtree(str(path), ignore_errors=True)
    if path.exists():
        logger.warn(f"staging cleanupが完了しませんでした: {path}")
    else:
        logger.info(f"staging cleanup完了: {path}")


# ---------------------------------------------------------------------------
# aws s3 sync
# ---------------------------------------------------------------------------


def build_sync_argv(stage_root: Path, destination_uri: str, region: str, dry_run: bool) -> List[str]:
    """
    aws s3 sync の argv を組み立てる（shell文字列は使わない）。
    staging tree方式のため include / exclude フィルタは一切使わない。
    """
    if destination_uri != EXPECTED_DESTINATION_URI:
        raise SyncError(f"destination安全ロック違反: {destination_uri!r}")
    argv = [
        AWS_BIN,
        "s3",
        "sync",
        str(stage_root),
        destination_uri,
        "--delete",
        "--no-follow-symlinks",
        "--only-show-errors",
        "--region",
        region,
    ]
    if dry_run:
        argv.append("--dryrun")
    return argv


def run_sync(argv: List[str], logger) -> None:
    if argv[4] != EXPECTED_DESTINATION_URI:
        raise SyncError(f"destination安全ロック違反: {argv[4]!r}")
    if "--include" in argv or "--exclude" in argv:
        raise SyncError("staging tree方式ではinclude/excludeフィルタを使いません")
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


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------


def build_s3_client(region: str):
    """boto3 S3クライアントを生成する（テストから差し替え可能にするため関数化）。"""
    import boto3

    return boto3.client("s3", region_name=region)


def list_portal_objects(s3_client, bucket: str, portal_prefix: str) -> Dict[str, int]:
    """
    Portal専用prefixを全ページLISTし、{relative_path: size} を返す。
    prefix自身のobjectを除き、directory markerを含む全objectをactual集合に含める。
    """
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
                if not relative_path:
                    # prefix自身のobjectのみ除外する（directory markerは除外しない）
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


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


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
    parser.add_argument("--run-date", default=None, help="RUN_DATE（既定は環境変数RUN_DATE）")
    parser.add_argument("--run-id", default=None, help="RUN_ID（既定は環境変数RUN_ID）")
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
    base_prefix = get_config_value(config, "PIPELINE_S3_BASE_PREFIX")
    portal_prefix = get_config_value(config, "PORTAL_S3_PREFIX")
    region = get_config_value(config, "PIPELINE_AWS_REGION")

    # sync開始前に destination を完全固定する
    destination_uri = lock_destination(bucket, base_prefix, portal_prefix)
    wait_seconds = parse_wait_seconds(get_config_value(config, "PORTAL_S3_VERIFY_WAIT_SEC"))
    provenance = resolve_provenance(args)

    expected = load_manifest(manifest_path)
    step_dirs = load_selected_step_dirs(prepare_summary_path)

    logger.info(f"同期先(lock済): {destination_uri} (region={region})")
    logger.info(f"expected files={len(expected)} / bytes={sum(expected.values())}")
    logger.info(f"PORTAL_S3_VERIFY_WAIT_SEC={wait_seconds}")
    logger.info(
        f"run_date={provenance['run_date']}({provenance['run_date_source']}) / "
        f"run_id={provenance['run_id']}({provenance['run_id_source']})"
    )

    summary: Dict[str, Any] = {
        "step": STEP_NAME,
        "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": "dry-run" if args.dry_run else "apply",
        "run_date": provenance["run_date"],
        "run_date_source": provenance["run_date_source"],
        "run_id": provenance["run_id"],
        "run_id_source": provenance["run_id_source"],
        "pipeline_root": str(root),
        "s3_destination": destination_uri,
        "s3_destination_locked": True,
        "sync_method": "staging tree + aws s3 sync --delete (no CLI filters)",
        "sync_status": "SUCCEEDED",
        "verify_wait_sec": wait_seconds,
        "wait_performed": False,
        "selected_step_dir_count": len(step_dirs),
        "manifest_path": str(manifest_path),
    }

    stage_root = None
    try:
        stage_root = create_staging_root(Path(args.step_dir))
        staging = build_staging_tree(root, expected, stage_root, logger)
        summary["staging"] = {
            "file_count": staging["file_count"],
            "total_bytes": staging["total_bytes"],
            "hard_linked": staging["linked"],
            "copied": staging["copied"],
        }
        argv = build_sync_argv(stage_root, destination_uri, region, args.dry_run)
        run_sync(argv, logger)
    finally:
        cleanup_staging(stage_root, logger)

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


def build_failed_summary(args: argparse.Namespace, error_message: str) -> Dict[str, Any]:
    """
    FAILED時のsummary。provenanceは可能な限り残す（形式不正で解決できない場合は既定値）。
    sync_status=FAILED のsummaryは 80-75 rotation guardでbackup対象外として扱われる。
    """
    try:
        provenance = resolve_provenance(args)
    except SyncError:
        provenance = {
            "run_date": UNKNOWN_PROVENANCE,
            "run_date_source": "default",
            "run_id": UNKNOWN_PROVENANCE,
            "run_id_source": "default",
        }
    return {
        "step": STEP_NAME,
        "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": "dry-run" if args.dry_run else "apply",
        "run_date": provenance["run_date"],
        "run_date_source": provenance["run_date_source"],
        "run_id": provenance["run_id"],
        "run_id_source": provenance["run_id_source"],
        "sync_status": "FAILED",
        "error_message": error_message,
    }


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
        summary = build_failed_summary(args, str(exc))
        exit_code = 1
    except Exception as exc:  # noqa: BLE001 - 想定外例外も握りつぶさずFAILさせる
        logger.error(f"[NG] 想定外エラー: {type(exc).__name__}: {exc}")
        summary = build_failed_summary(args, f"{type(exc).__name__}: {exc}")
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

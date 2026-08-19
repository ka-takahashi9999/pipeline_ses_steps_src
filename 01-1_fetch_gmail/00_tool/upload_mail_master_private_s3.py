#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
01-1 mail master private S3 upload

01-1_fetch_gmail が生成した重要成果物 fetch_gmail_mail_master.jsonl を、
Portalとは分離した private prefix へ 1 object だけ保存する。

  ローカル : 01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl
  S3      : s3://technoverse/pipeline_ses_steps/private/mail_master/<RUN_DATE>/fetch_gmail_mail_master.jsonl

方式:
- 01-1本体（fetch_gmail.py）には手を入れず、01-1成功後に本utilityを呼ぶ責務分離とする。
- destination安全ロック: bucket / base prefix / private prefix / mail master prefix /
  RUN_DATE / 最終key / 完全URI を期待値と完全一致比較し、1つでも異なれば upload開始前にFAILする
  （startswith判定はしない）。設定値・環境変数の書き換えで Portal prefix・上位prefix・
  別bucketへ向けることはできない。
- upload前に local file を検査する（expected path完全一致 / regular file / symlink不可 /
  size>0 / JSONL record>0）。record走査は1パスのみ（341MB級を何度も全scanしない）。
- AWS CLI は argv配列で subprocess 実行する（eval / bash -c / sh -c は使わない）。
  `aws s3 cp` で1 objectのみ。sync / --delete / --recursive / wildcard は使わない。
- upload後は head-object で ContentLength == local bytes を確認する。
  ETagはmultipart uploadでMD5と一致しないため、検証には使わない。
- upload / verify のいずれかが失敗したら非0終了する（warningで握りつぶさない）。
  runner側では run_step 経由のため Pipeline FAILED となり、01-2へは進まない。
- 冪等性: 同一RUN_DATEの再実行は同一keyへの上書きになる。`aws s3 cp` は
  転送完了時点で初めて新objectが成立するため、途中状態が正式keyへ露出しない。

AWS credentialはEC2 IAM Role（default credential chain）を使用する。
credential値・mail本文・JSONL内容はlog出力しない。

usage:
  upload_mail_master_private_s3.py --run-date 20260818
  upload_mail_master_private_s3.py --run-date 20260818 --dry-run
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_STEP_DIR = Path(__file__).resolve().parents[1]
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import ensure_result_dirs, write_execution_time  # noqa: E402
from common.json_utils import read_jsonl  # noqa: E402
from common.logger import get_logger  # noqa: E402
from common.pipeline_s3_env import get_config_value, load_pipeline_s3_config  # noqa: E402

STEP_NAME = "01-1_fetch_gmail_private_s3_upload"

RESULT_DIR_NAME = "01_result"
MAIL_MASTER_FILENAME = "fetch_gmail_mail_master.jsonl"
SUMMARY_FILENAME = "mail_master_private_s3_summary.json"

AWS_BIN = "/usr/bin/aws"

# ---- destination安全ロック（唯一許可する保存先） ----------------------------
# ここを設定ファイル・環境変数で上書きできてはならない。
EXPECTED_BUCKET = "technoverse"
EXPECTED_BASE_PREFIX = "pipeline_ses_steps"
EXPECTED_PRIVATE_LEAF = "private"
EXPECTED_MAIL_MASTER_LEAF = "mail_master"
EXPECTED_PRIVATE_PREFIX = "{0}/{1}".format(EXPECTED_BASE_PREFIX, EXPECTED_PRIVATE_LEAF)
EXPECTED_MAIL_MASTER_PREFIX = "{0}/{1}".format(EXPECTED_PRIVATE_PREFIX, EXPECTED_MAIL_MASTER_LEAF)
# Portal専用prefix（80-9が使う領域）。ここへは絶対にuploadしない。
FORBIDDEN_PORTAL_PREFIX = "{0}/pipeline_ses_steps".format(EXPECTED_BASE_PREFIX)

RUN_DATE_RE = re.compile(r"[0-9]{8}")
RUN_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")

# `aws s3 cp` で 1 object だけ扱うため、これらは1つでも現れたらFAILさせる
FORBIDDEN_ARGV_TOKENS = (
    "sync",
    "mv",
    "rm",
    "--recursive",
    "--delete",
    "--include",
    "--exclude",
    "--follow-symlinks",
)
FORBIDDEN_ARGV_CHARS = ("*", "?")

SAMPLE_LIMIT = 3


class UploadError(Exception):
    """S3へuploadせず / verify不成立で異常終了すべき状態。"""


# ---------------------------------------------------------------------------
# destination安全ロック
# ---------------------------------------------------------------------------


def lock_destination(
    bucket: Any, base_prefix: Any, private_prefix: Any, mail_master_prefix: Any
) -> None:
    """
    bucket / base prefix / private prefix / mail master prefix を期待値と完全一致比較する。
    1項目でも一致しなければ UploadError を送出する（upload開始前にFAILさせる）。

    生の設定値をそのまま比較するため、空文字・`/`・`..`・上位prefix・Portal prefix・
    別bucketはすべて不一致として拒否される。startswith判定は使わない。
    """
    for name, value, expected in (
        ("PIPELINE_S3_BUCKET", bucket, EXPECTED_BUCKET),
        ("PIPELINE_S3_BASE_PREFIX", base_prefix, EXPECTED_BASE_PREFIX),
        ("PIPELINE_PRIVATE_PREFIX", private_prefix, EXPECTED_PRIVATE_PREFIX),
        ("MAIL_MASTER_S3_PREFIX", mail_master_prefix, EXPECTED_MAIL_MASTER_PREFIX),
    ):
        if not isinstance(value, str) or value != expected:
            raise UploadError(
                "destination安全ロック違反: {0} が期待値と一致しません "
                "(actual={1!r} / expected={2!r})".format(name, value, expected)
            )

    # 期待値定数そのものが壊れていないかも構造として検証する
    components = mail_master_prefix.split("/")
    if components != [EXPECTED_BASE_PREFIX, EXPECTED_PRIVATE_LEAF, EXPECTED_MAIL_MASTER_LEAF]:
        raise UploadError(
            "destination安全ロック違反: mail master prefixの構造が不正です: {0!r}".format(
                mail_master_prefix
            )
        )
    if any(component in ("", ".", "..") for component in components):
        raise UploadError(
            "destination安全ロック違反: mail master prefixに不正componentがあります: {0!r}".format(
                mail_master_prefix
            )
        )
    if mail_master_prefix == FORBIDDEN_PORTAL_PREFIX or private_prefix == FORBIDDEN_PORTAL_PREFIX:
        raise UploadError("destination安全ロック違反: Portal prefixへはuploadできません")


def validate_run_date(raw: Any) -> str:
    """RUN_DATE を YYYYMMDD の実在日付としてのみ受理する。"""
    if not isinstance(raw, str) or not RUN_DATE_RE.fullmatch(raw):
        raise UploadError("RUN_DATEはYYYYMMDD（半角数字8桁）のみ指定できます: {0!r}".format(raw))
    try:
        datetime.strptime(raw, "%Y%m%d")
    except ValueError as exc:
        raise UploadError("RUN_DATEが実在しない日付です: {0!r}".format(raw)) from exc
    return raw


def validate_run_id(raw: Any) -> Tuple[str, str]:
    """
    RUN_ID を検証する。未設定は許容し、既定値 + run_id_source で根拠を残す。
    戻り値: (run_id, run_id_source)
    """
    if raw is None or raw == "":
        return "unset", "default"
    if not isinstance(raw, str) or not RUN_ID_RE.fullmatch(raw):
        raise UploadError("RUN_IDに使用できない文字が含まれています: {0!r}".format(raw))
    return raw, "provided"


def build_object_key(mail_master_prefix: str, run_date: str) -> str:
    """最終object keyを組み立て、component単位で期待構造と完全一致比較する。"""
    key = "{0}/{1}/{2}".format(mail_master_prefix, run_date, MAIL_MASTER_FILENAME)
    components = key.split("/")
    expected = [
        EXPECTED_BASE_PREFIX,
        EXPECTED_PRIVATE_LEAF,
        EXPECTED_MAIL_MASTER_LEAF,
        run_date,
        MAIL_MASTER_FILENAME,
    ]
    if components != expected:
        raise UploadError(
            "destination安全ロック違反: object keyの構造が不正です "
            "(actual={0!r} / expected={1!r})".format(components, expected)
        )
    if any(component in ("", ".", "..") for component in components):
        raise UploadError("destination安全ロック違反: object keyに不正componentがあります: {0!r}".format(key))
    if key.startswith("/") or os.path.isabs(key):
        raise UploadError("destination安全ロック違反: absolute path相当のkeyです: {0!r}".format(key))
    return key


def build_destination_uri(bucket: str, key: str, run_date: str) -> str:
    """完全URIを組み立て、期待URIと完全一致比較する。"""
    destination_uri = "s3://{0}/{1}".format(bucket, key)
    expected_uri = "s3://{0}/{1}/{2}/{3}".format(
        EXPECTED_BUCKET, EXPECTED_MAIL_MASTER_PREFIX, run_date, MAIL_MASTER_FILENAME
    )
    if destination_uri != expected_uri:
        raise UploadError(
            "destination安全ロック違反: 保存先URIが期待値と一致しません "
            "(actual={0!r} / expected={1!r})".format(destination_uri, expected_uri)
        )
    return destination_uri


# ---------------------------------------------------------------------------
# local file検査
# ---------------------------------------------------------------------------


def expected_mail_master_path(step_dir: Path) -> Path:
    """01-1が出力する唯一のmail master pathを返す。"""
    return Path(step_dir) / RESULT_DIR_NAME / MAIL_MASTER_FILENAME


def validate_local_file(path: Path, expected_path: Path) -> Dict[str, int]:
    """
    upload対象localファイルを検査する。
    expected pathと完全一致 / symlink不可 / regular file / size>0 を満たさなければFAILさせる。
    戻り値: {"size": bytes, "mtime_ns": int}
    """
    if str(path) != str(expected_path):
        raise UploadError(
            "upload対象がexpected local pathと一致しません "
            "(actual={0!r} / expected={1!r})".format(str(path), str(expected_path))
        )
    text = str(path)
    if not text.strip():
        raise UploadError("upload対象pathが空です")
    if ".." in Path(text).parts:
        raise UploadError("upload対象pathに `..` が含まれています: {0}".format(text))

    result_dir = Path(path).parent
    if result_dir.is_symlink():
        raise UploadError("01_resultがsymlinkです: {0}".format(result_dir))
    if Path(path).is_symlink():
        raise UploadError("mail masterがsymlinkです: {0}".format(path))
    if not Path(path).exists():
        raise UploadError("mail masterが存在しません: {0}".format(path))
    if not Path(path).is_file():
        raise UploadError("mail masterがregular fileではありません: {0}".format(path))

    stat_result = Path(path).stat()
    if stat_result.st_size <= 0:
        raise UploadError("mail masterのsizeが0です: {0}".format(path))
    return {"size": stat_result.st_size, "mtime_ns": stat_result.st_mtime_ns}


def scan_mail_master(path: Path) -> Dict[str, int]:
    """
    mail masterを1パスだけ走査し、record数とmessage_id空件数を数える。
    JSONLパース不能行は common.json_utils.read_jsonl が例外を送出するため握りつぶさない。
    """
    record_count = 0
    empty_message_id_count = 0
    try:
        for record in read_jsonl(str(path)):
            record_count += 1
            if not str(record.get("message_id", "")).strip():
                empty_message_id_count += 1
    except UploadError:
        raise
    except Exception as exc:  # noqa: BLE001 - パース不能を黙って捨てない
        raise UploadError("mail masterの走査に失敗しました: {0}: {1}".format(type(exc).__name__, exc)) from exc

    if record_count <= 0:
        raise UploadError("mail masterのrecordが0件です: {0}".format(path))
    if empty_message_id_count > 0:
        raise UploadError(
            "message_idが空のrecordが {0} 件あります: {1}".format(empty_message_id_count, path)
        )
    return {"record_count": record_count, "empty_message_id_count": empty_message_id_count}


def recheck_unchanged(path: Path, before: Dict[str, int]) -> None:
    """走査・upload中にlocalファイルが差し替わっていないことを確認する。"""
    if Path(path).is_symlink() or not Path(path).is_file():
        raise UploadError("mail masterが処理中に変化しました: {0}".format(path))
    stat_result = Path(path).stat()
    if stat_result.st_size != before["size"] or stat_result.st_mtime_ns != before["mtime_ns"]:
        raise UploadError(
            "mail masterが処理中に変化しました: {0} "
            "(size {1} -> {2})".format(path, before["size"], stat_result.st_size)
        )


# ---------------------------------------------------------------------------
# aws s3 cp
# ---------------------------------------------------------------------------


def build_metadata_value(run_date: str, run_id: str, record_count: int) -> str:
    """head-objectで確認できる非機密metadata（mail内容は含めない）。"""
    value = "run-date={0},run-id={1},record-count={2}".format(run_date, run_id, record_count)
    if not all(part.isascii() for part in (run_date, run_id)):
        raise UploadError("metadataにASCII以外が含まれています")
    return value


def build_cp_argv(
    local_path: Path,
    destination_uri: str,
    region: str,
    dry_run: bool,
    metadata_value: str,
) -> List[str]:
    """`aws s3 cp` の argv を組み立てる（shell文字列は使わない / 1 objectのみ）。"""
    argv = [
        AWS_BIN,
        "s3",
        "cp",
        str(local_path),
        destination_uri,
        "--only-show-errors",
        "--region",
        region,
        "--metadata",
        metadata_value,
    ]
    if dry_run:
        argv.append("--dryrun")
    assert_safe_argv(argv, local_path, destination_uri)
    return argv


def assert_safe_argv(argv: List[str], local_path: Path, destination_uri: str) -> None:
    """recursive / sync / delete / wildcard が混入していないことを実行直前に確認する。"""
    if argv[:3] != [AWS_BIN, "s3", "cp"]:
        raise UploadError("aws s3 cp 以外のcommandは実行しません: {0!r}".format(argv[:3]))
    if argv[3] != str(local_path):
        raise UploadError("upload元が期待値と一致しません: {0!r}".format(argv[3]))
    if argv[4] != destination_uri:
        raise UploadError("destination安全ロック違反: {0!r}".format(argv[4]))
    for token in argv[5:]:
        if token in FORBIDDEN_ARGV_TOKENS:
            raise UploadError("禁止オプションが含まれています: {0!r}".format(token))
    for token in argv:
        if any(char in token for char in FORBIDDEN_ARGV_CHARS):
            raise UploadError("wildcardを含む引数は使用できません: {0!r}".format(token))


def run_upload(argv: List[str], logger) -> None:
    """aws s3 cp を argv配列で実行する（shell未使用）。非0終了は握りつぶさない。"""
    logger.info("aws s3 cp 実行: dest={0}".format(argv[4]))
    completed = subprocess.run(  # noqa: S603 - argv配列固定・shell未使用
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=False,
        check=False,
    )
    output = (completed.stdout or b"").decode("utf-8", errors="replace").strip()
    if output:
        for line in output.splitlines()[:SAMPLE_LIMIT]:
            logger.info("aws出力: {0}".format(line))
    if completed.returncode != 0:
        raise UploadError("aws s3 cp が失敗しました (exit={0})".format(completed.returncode))
    logger.ok("aws s3 cp 成功")


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------


def build_s3_client(region: str):
    """boto3 S3クライアントを生成する（テストから差し替え可能にするため関数化）。"""
    import boto3

    return boto3.client("s3", region_name=region)


def verify_uploaded_object(
    s3_client, bucket: str, key: str, expected_key: str, expected_size: int, logger
) -> Dict[str, Any]:
    """
    head-object でobject存在と ContentLength == local bytes を確認する。
    ETagはmultipart uploadでMD5と一致しないため検証には使わない。
    """
    if bucket != EXPECTED_BUCKET:
        raise UploadError("verify対象bucketが期待値と一致しません: {0!r}".format(bucket))
    if key != expected_key:
        raise UploadError("verify対象keyが期待値と一致しません: {0!r}".format(key))

    try:
        head = s3_client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:  # noqa: BLE001 - head失敗は必ずFAILさせる
        raise UploadError(
            "head-objectに失敗しました: s3://{0}/{1} ({2}: {3})".format(
                bucket, key, type(exc).__name__, exc
            )
        ) from exc

    content_length = head.get("ContentLength")
    if not isinstance(content_length, int) or isinstance(content_length, bool):
        raise UploadError("head-objectのContentLengthが不正です: {0!r}".format(content_length))
    if content_length != expected_size:
        raise UploadError(
            "S3 ContentLengthがlocal bytesと一致しません "
            "(local={0} / s3={1})".format(expected_size, content_length)
        )
    logger.ok("verify成功: s3_bytes={0}".format(content_length))
    return {"verified": True, "s3_bytes": content_length}


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="01-1 mail masterのprivate S3保存")
    parser.add_argument("--run-date", default=os.environ.get("RUN_DATE", ""), help="RUN_DATE (YYYYMMDD)")
    parser.add_argument("--run-id", default=os.environ.get("RUN_ID", ""), help="RUN_ID（任意）")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="aws s3 cp --dryrun で実行し、S3を変更せず verify もしない",
    )
    parser.add_argument("--step-dir", default=str(_STEP_DIR), help="01-1 stepディレクトリ（focused test用）")
    return parser.parse_args(argv)


def run(args: argparse.Namespace, logger) -> Dict[str, Any]:
    run_date = validate_run_date(args.run_date)
    run_id, run_id_source = validate_run_id(args.run_id)

    config = load_pipeline_s3_config()
    bucket = get_config_value(config, "PIPELINE_S3_BUCKET")
    base_prefix = get_config_value(config, "PIPELINE_S3_BASE_PREFIX")
    private_prefix = get_config_value(config, "PIPELINE_PRIVATE_PREFIX")
    mail_master_prefix = get_config_value(config, "MAIL_MASTER_S3_PREFIX")
    region = get_config_value(config, "PIPELINE_AWS_REGION")

    # upload開始前に destination を完全固定する
    lock_destination(bucket, base_prefix, private_prefix, mail_master_prefix)
    key = build_object_key(mail_master_prefix, run_date)
    destination_uri = build_destination_uri(bucket, key, run_date)

    step_dir = Path(args.step_dir)
    local_path = expected_mail_master_path(step_dir)
    stat_before = validate_local_file(local_path, expected_mail_master_path(step_dir))
    scan = scan_mail_master(local_path)
    recheck_unchanged(local_path, stat_before)

    logger.info("private S3 upload開始 (RUN_DATE={0} / run_id={1})".format(run_date, run_id))
    logger.info("保存先(lock済): {0} (region={1})".format(destination_uri, region))
    logger.info(
        "local: bytes={0} / records={1}".format(stat_before["size"], scan["record_count"])
    )

    summary: Dict[str, Any] = {
        "step": STEP_NAME,
        "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": "dry-run" if args.dry_run else "apply",
        "status": "SUCCEEDED",
        "run_date": run_date,
        "run_id": run_id,
        "run_id_source": run_id_source,
        "local_path": str(local_path),
        "local_bytes": stat_before["size"],
        "record_count": scan["record_count"],
        "empty_message_id_count": scan["empty_message_id_count"],
        "s3_bucket": bucket,
        "s3_key": key,
        "s3_uri": destination_uri,
        "s3_destination_locked": True,
        "upload_method": "aws s3 cp (single object / no sync / no --delete / no recursive)",
        "s3_bytes": 0,
        "verified": False,
    }

    metadata_value = build_metadata_value(run_date, run_id, scan["record_count"])
    argv = build_cp_argv(local_path, destination_uri, region, args.dry_run, metadata_value)
    run_upload(argv, logger)

    if args.dry_run:
        logger.warn("dry-runのため verify は実施しません（S3未変更）")
        summary["verify_skipped_reason"] = "dry-run"
        return summary

    recheck_unchanged(local_path, stat_before)
    s3_client = build_s3_client(region)
    verify_result = verify_uploaded_object(
        s3_client, bucket, key, key, stat_before["size"], logger
    )
    summary["s3_bytes"] = verify_result["s3_bytes"]
    summary["verified"] = verify_result["verified"]
    return summary


def main() -> int:
    logger = get_logger(STEP_NAME)
    args = parse_args()
    started = time.time()
    dirs = ensure_result_dirs(args.step_dir)
    summary_path = dirs["result"] / SUMMARY_FILENAME

    summary: Dict[str, Any]
    exit_code = 0
    try:
        summary = run(args, logger)
    except UploadError as exc:
        logger.error("[NG] {0}".format(exc))
        summary = {
            "step": STEP_NAME,
            "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "dry-run" if args.dry_run else "apply",
            "status": "FAILED",
            "verified": False,
            "error_message": str(exc),
        }
        exit_code = 1
    except Exception as exc:  # noqa: BLE001 - 想定外例外も握りつぶさずFAILさせる
        logger.error("[NG] 想定外エラー: {0}: {1}".format(type(exc).__name__, exc))
        summary = {
            "step": STEP_NAME,
            "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "dry-run" if args.dry_run else "apply",
            "status": "FAILED",
            "verified": False,
            "error_message": "{0}: {1}".format(type(exc).__name__, exc),
        }
        exit_code = 1

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    logger.info("summary: {0}".format(summary_path))

    write_execution_time(
        str(dirs["execution_time"]),
        STEP_NAME,
        time.time() - started,
        record_count=int(summary.get("record_count", 0) or 0),
    )
    if exit_code == 0:
        logger.ok("完了")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

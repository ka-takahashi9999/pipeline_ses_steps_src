#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
01-1 mail master private S3 upload の confirmスクリプト

01_result/mail_master_private_s3_summary.json を対象に、
localのmail masterと件数・bytesが整合しているか、
保存先bucket / key が正本設定どおりかを確認する。
S3へは一切アクセスしない（read-onlyのlocal整合チェックのみ）。

bucket / prefix は 00_pipeline/00_tool/pipeline_s3_config.env を正本とし、
RUN_DATE検証・key組み立ては upload utility の実装を再利用する（二重管理しない）。

チェック内容:
  1. summaryの存在 / status == SUCCEEDED
  2. run_dateがYYYYMMDDかつ実在日付（例: 20260230 はNG）
  3. s3_bucket == 正本設定のbucket
  4. s3_key == 正本設定 + run_date から組み立てた期待key
  5. local mail masterの存在・regular file・symlinkでない
  6. local bytes == summary.local_bytes / record数 == summary.record_count
  7. apply実行では verified == True かつ s3_bytes == local_bytes

終了コード:
  0: 全チェック OK
  1: チェック NG
"""

import json
import sys
from pathlib import Path

_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
_TOOL_DIR = _STEP_DIR / "00_tool"
for _path in (str(_PROJECT_ROOT), str(_TOOL_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from common.json_utils import count_jsonl
from common.logger import get_logger
from common.pipeline_s3_env import get_config_value, load_pipeline_s3_config
from upload_mail_master_private_s3 import (  # noqa: E402 - 00_tool の実装を再利用する
    MAIL_MASTER_FILENAME,
    SUMMARY_FILENAME,
    UploadError,
    build_object_key,
    canonical_source_path,
    lock_destination,
    validate_run_date,
)

STEP_NAME = "confirm_01-1_mail_master_private_s3"
logger = get_logger(STEP_NAME)


def check_destination(summary, errors):
    """bucket / key を正本設定から再構築して比較する（ハードコードで二重管理しない）。"""
    config = load_pipeline_s3_config()
    bucket = get_config_value(config, "PIPELINE_S3_BUCKET")
    base_prefix = get_config_value(config, "PIPELINE_S3_BASE_PREFIX")
    private_prefix = get_config_value(config, "PIPELINE_PRIVATE_PREFIX")
    mail_master_prefix = get_config_value(config, "MAIL_MASTER_S3_PREFIX")

    try:
        lock_destination(bucket, base_prefix, private_prefix, mail_master_prefix)
    except UploadError as exc:
        errors.append("正本設定のdestinationが不正です: {0}".format(exc))
        return

    if summary.get("s3_bucket") != bucket:
        errors.append(
            "s3_bucketが期待値と一致しません (summary={0!r} / expected={1!r})".format(
                summary.get("s3_bucket"), bucket
            )
        )

    try:
        run_date = validate_run_date(summary.get("run_date"))
    except UploadError as exc:
        errors.append("run_dateが不正です: {0}".format(exc))
        return

    expected_key = build_object_key(mail_master_prefix, run_date)
    if summary.get("s3_key") != expected_key:
        errors.append(
            "s3_keyが期待値と一致しません (summary={0!r} / expected={1!r})".format(
                summary.get("s3_key"), expected_key
            )
        )
    expected_uri = "s3://{0}/{1}".format(bucket, expected_key)
    if summary.get("s3_uri") != expected_uri:
        errors.append(
            "s3_uriが期待値と一致しません (summary={0!r} / expected={1!r})".format(
                summary.get("s3_uri"), expected_uri
            )
        )


def check_local(summary, errors):
    """localのmail masterとsummaryのbytes / record数が整合しているか確認する。"""
    local_path = canonical_source_path()
    if local_path.is_symlink():
        errors.append("mail masterがsymlinkです: {0}".format(local_path))
        return
    if not local_path.is_file():
        errors.append("mail masterが存在しません: {0}".format(local_path))
        return
    if summary.get("local_path") != str(local_path):
        errors.append(
            "local_pathが正規sourceと一致しません (summary={0!r} / expected={1!r})".format(
                summary.get("local_path"), str(local_path)
            )
        )

    local_bytes = local_path.stat().st_size
    if local_bytes != summary.get("local_bytes"):
        errors.append(
            "local bytesがsummaryと一致しません (local={0} / summary={1})".format(
                local_bytes, summary.get("local_bytes")
            )
        )
    record_count = count_jsonl(str(local_path))
    if record_count != summary.get("record_count"):
        errors.append(
            "record数がsummaryと一致しません (local={0} / summary={1})".format(
                record_count, summary.get("record_count")
            )
        )
    logger.info("local: bytes={0} / records={1}".format(local_bytes, record_count))


def confirm(summary):
    """summaryを検証し、NGメッセージのリストを返す。"""
    errors = []

    status = summary.get("status", "")
    if status != "SUCCEEDED":
        errors.append(
            "statusがSUCCEEDEDではありません: {0} ({1})".format(
                status, summary.get("error_message", "")
            )
        )

    check_destination(summary, errors)
    check_local(summary, errors)

    mode = summary.get("mode", "")
    if mode == "apply":
        if not summary.get("verified"):
            errors.append("verifiedがTrueではありません")
        if summary.get("s3_bytes") != summary.get("local_bytes"):
            errors.append(
                "s3_bytesがlocal_bytesと一致しません (s3={0} / local={1})".format(
                    summary.get("s3_bytes"), summary.get("local_bytes")
                )
            )
    else:
        logger.warn("mode={0} のためS3側の整合は未確認です".format(mode))

    return errors


def main() -> None:
    summary_path = _STEP_DIR / "01_result" / SUMMARY_FILENAME
    if not summary_path.is_file():
        logger.error("summaryが存在しません: {0}".format(summary_path))
        sys.exit(1)
    with open(summary_path, "r", encoding="utf-8") as f:
        summary = json.load(f)

    errors = confirm(summary)

    logger.info("--- 確認結果サマリー ---")
    logger.info("mode            : {0}".format(summary.get("mode")))
    logger.info("run_date        : {0}".format(summary.get("run_date")))
    logger.info("s3_bucket       : {0}".format(summary.get("s3_bucket")))
    logger.info("s3_key          : {0}".format(summary.get("s3_key")))
    logger.info("対象file        : {0}".format(MAIL_MASTER_FILENAME))
    logger.info("NG件数          : {0}件".format(len(errors)))

    if errors:
        for message in errors[:3]:
            logger.error("[NG] {0}".format(message))
        sys.exit(1)

    logger.ok("confirm OK: 全チェック通過")


if __name__ == "__main__":
    main()

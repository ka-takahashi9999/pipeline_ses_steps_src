#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
01-1 mail master private S3 upload の confirmスクリプト

01_result/mail_master_private_s3_summary.json を正として、
localのmail masterと件数・bytesが整合しているかを確認する。
S3へは一切アクセスしない（read-onlyのlocal整合チェックのみ）。

チェック内容:
  1. summaryの存在 / status == SUCCEEDED
  2. local mail masterの存在・regular file・symlinkでない
  3. local bytes == summary.local_bytes
  4. JSONL record数 == summary.record_count（1パス再走査）
  5. s3_key が private/mail_master/<RUN_DATE>/fetch_gmail_mail_master.jsonl 形式
  6. apply実行では verified == True かつ s3_bytes == local_bytes

終了コード:
  0: 全チェック OK
  1: チェック NG
"""

import json
import sys
from pathlib import Path

_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import get_result_path
from common.json_utils import count_jsonl
from common.logger import get_logger

STEP_NAME = "confirm_01-1_mail_master_private_s3"
logger = get_logger(STEP_NAME)

SUMMARY_FILENAME = "mail_master_private_s3_summary.json"
MAIL_MASTER_FILENAME = "fetch_gmail_mail_master.jsonl"
EXPECTED_KEY_HEAD = ["pipeline_ses_steps", "private", "mail_master"]


def main() -> None:
    summary_path = Path(get_result_path(str(_STEP_DIR), SUMMARY_FILENAME))
    local_path = Path(get_result_path(str(_STEP_DIR), MAIL_MASTER_FILENAME))

    if not summary_path.is_file():
        logger.error("summaryが存在しません: {0}".format(summary_path))
        sys.exit(1)
    with open(summary_path, "r", encoding="utf-8") as f:
        summary = json.load(f)

    errors = []

    status = summary.get("status", "")
    mode = summary.get("mode", "")
    if status != "SUCCEEDED":
        errors.append("statusがSUCCEEDEDではありません: {0} ({1})".format(status, summary.get("error_message", "")))

    if local_path.is_symlink():
        errors.append("mail masterがsymlinkです")
    elif not local_path.is_file():
        errors.append("mail masterが存在しません: {0}".format(local_path))
    else:
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

    key = summary.get("s3_key", "")
    components = key.split("/") if isinstance(key, str) else []
    if len(components) != 5 or components[:3] != EXPECTED_KEY_HEAD:
        errors.append("s3_keyの形式が不正です: {0!r}".format(key))
    elif components[3] != summary.get("run_date") or components[4] != MAIL_MASTER_FILENAME:
        errors.append("s3_keyがRUN_DATE / ファイル名と一致しません: {0!r}".format(key))

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

    logger.info("--- 確認結果サマリー ---")
    logger.info("mode            : {0}".format(mode))
    logger.info("run_date        : {0}".format(summary.get("run_date")))
    logger.info("s3_key          : {0}".format(key))
    logger.info("NG件数          : {0}件".format(len(errors)))

    if errors:
        for message in errors[:3]:
            logger.error("[NG] {0}".format(message))
        sys.exit(1)

    logger.ok("confirm OK: 全チェック通過")


if __name__ == "__main__":
    main()

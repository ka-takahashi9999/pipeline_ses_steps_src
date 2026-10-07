#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 01-1 confirmスクリプト: fetch_gmail_mail_master.jsonl の整合性チェック

チェック内容:
  1. 出力ファイルの存在確認
  2. 取得件数の表示
  3. 必須フィールドの存在確認
  4. message_id の重複チェック
  5. 件数が0件の場合はNG（Pipelineを停止）

終了コード:
  0: 全チェック OK
  1: チェック NG（Pipeline停止）
"""

import sys
import argparse
from pathlib import Path

# common モジュールのパス解決
_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import get_result_path
from common.json_utils import read_jsonl_as_list
from common.logger import get_logger

STEP_NAME = "confirm_01-1_fetch_gmail"
logger = get_logger(STEP_NAME)

REQUIRED_FIELDS = [
    "message_id", "thread_id", "subject", "from",
    "to", "cc", "reply_to", "date", "body_text", "attachments",
]
OUTPUT_FILENAME = "fetch_gmail_mail_master.jsonl"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=get_result_path(str(_STEP_DIR), OUTPUT_FILENAME))
    parser.add_argument("--manifest")
    parser.add_argument("--after")
    parser.add_argument("--before")
    parser.add_argument("--selection-limit", type=int, default=10)
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument("--allow-needs-review", action="store_true")
    args = parser.parse_args()
    output_path = args.input
    if args.manifest:
        sys.path.insert(0, str(_STEP_DIR / "00_tool"))
        from fetch_s3_mail import build_summary, validate_records
        try:
            if not args.after or not args.before:
                raise ValueError("S3 confirm requires expected after/before")
            records = read_jsonl_as_list(output_path)
            entries = read_jsonl_as_list(args.manifest)
            review_path = Path(args.manifest).parent / "needs_review.jsonl"
            validate_records(records, entries, maximum=args.selection_limit,
                             require_complete=args.require_complete,
                             raw_dir=Path(args.manifest).parent / "raw",
                             expected_after=args.after, expected_before=args.before,
                             manifest_path=args.manifest if args.after else None,
                             allow_needs_review=args.allow_needs_review,
                             review_path=review_path)
            expected_summary = build_summary(
                entries, records, args.manifest, review_path)
            summaries = read_jsonl_as_list(
                str(Path(args.manifest).parent / "fetch_summary.jsonl"))
            if summaries != [expected_summary]:
                raise ValueError("fetch summary mismatch")
        except Exception as error:
            logger.error("S3 confirm NG: " + type(error).__name__)
            sys.exit(1)
        review_count = sum(entry.get("status") == "needs_review" for entry in entries)
        warning_count = sum(entry.get("status") == "success" and
                            bool(entry.get("header_warnings")) for entry in entries)
        result = "要確認ありで完了" if review_count else "全件正常"
        logger.ok(
            f"S3 confirm OK ({result}): selected={len(entries)} "
            f"success={len(records)} warning_success={warning_count} "
            f"needs_review={review_count} failed=0 pending=0 "
            f"review_records={review_path}")
        return

    # 1. ファイル存在確認
    if not Path(output_path).exists():
        logger.error(f"出力ファイルが存在しません: {output_path}")
        sys.exit(1)

    # 2. レコード読み込み
    try:
        records = read_jsonl_as_list(output_path)
    except Exception as e:
        logger.error(f"ファイル読み込みエラー: {e}")
        sys.exit(1)

    total = len(records)
    logger.info(f"取得件数: {total}件")

    # 3. 件数チェック
    if total == 0:
        logger.error("取得件数が0件です。Pipelineを停止します。")
        sys.exit(1)

    # 4. 必須フィールド確認
    missing_field_errors = 0
    for i, rec in enumerate(records):
        for field in REQUIRED_FIELDS:
            if field not in rec:
                mid = rec.get("message_id", f"index_{i}")
                logger.warn(f"必須フィールド '{field}' が欠損しています", message_id=mid)
                missing_field_errors += 1

    # 5. message_id 重複チェック
    message_ids = [rec.get("message_id", "") for rec in records]
    seen = set()
    duplicate_count = 0
    for mid in message_ids:
        if mid in seen:
            logger.warn(f"message_id 重複: {mid}")
            duplicate_count += 1
        seen.add(mid)

    # 6. フィールド型確認（to / attachments はリスト型）
    type_errors = 0
    for rec in records:
        mid = rec.get("message_id", "")
        if not isinstance(rec.get("to"), list):
            logger.warn("'to' フィールドがリスト型ではありません", message_id=mid)
            type_errors += 1
        if not isinstance(rec.get("attachments"), list):
            logger.warn("'attachments' フィールドがリスト型ではありません", message_id=mid)
            type_errors += 1

    # 結果サマリー
    logger.info(f"--- 確認結果サマリー ---")
    logger.info(f"総件数          : {total}件")
    logger.info(f"フィールド欠損  : {missing_field_errors}件")
    logger.info(f"message_id重複  : {duplicate_count}件")
    logger.info(f"型エラー        : {type_errors}件")

    if missing_field_errors > 0 or duplicate_count > 0 or type_errors > 0:
        logger.error("確認NGがあります。Pipelineを停止します。")
        sys.exit(1)

    logger.ok(f"confirm OK: {total}件 全チェック通過")


if __name__ == "__main__":
    main()

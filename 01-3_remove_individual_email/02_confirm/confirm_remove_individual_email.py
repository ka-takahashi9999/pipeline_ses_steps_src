#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 01-3 confirmスクリプト

【チェック①: 件数整合チェック】
  - 01-2 の件数 == 01-3 の件数 + 99_removed 件数（合計一致確認）
  - 01-3 が 0 件の場合はNG

【チェック②: 除外された内容の人間確認用出力】
  - 99_removed_individual_emails_raw.jsonl の message_id で 01-1 を参照
  - 除外メールの from / subject / 本文冒頭100文字を出力して目視確認できるようにする

終了コード:
  0: 全チェック OK
  1: チェック NG（Pipeline停止）
"""

import sys
from pathlib import Path
from typing import Dict, List

# common モジュールのパス解決
_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import get_result_path
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list
from common.logger import get_logger

sys.path.insert(0, str(_STEP_DIR / "00_tool"))
from remove_individual_email import (
    EXCLUDE_LIST_PATH, OUTPUT_DETAIL, TEMPLATE_REASONS, build_detail_record,
    detect_template_exclusion, determine_exclusion_reason, load_exclude_list,
)

STEP_NAME = "confirm_01-3_remove_individual_email"
logger = get_logger(STEP_NAME)

INPUT_MASTER = str(
    _PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)
INPUT_PREV = str(
    _PROJECT_ROOT / "01-2_remove_duplicate_emails" / "01_result" / "remove_duplicate_emails_raw.jsonl"
)
OUTPUT_FILTERED = "remove_individual_emails_raw.jsonl"
OUTPUT_REMOVED = "99_removed_individual_emails_raw.jsonl"


def check_counts(prev_count: int, filtered_count: int, removed_count: int) -> bool:
    """チェック①: 件数整合チェック。"""
    logger.info("=== チェック①: 件数整合 ===")
    logger.info(f"  01-2 入力件数      : {prev_count}件")
    logger.info(f"  01-3 出力件数      : {filtered_count}件")
    logger.info(f"  除外件数           : {removed_count}件")
    logger.info(f"  出力+除外          : {filtered_count + removed_count}件")

    if filtered_count == 0:
        logger.error("01-3 出力が0件です。Pipelineを停止します。")
        return False

    total = filtered_count + removed_count
    if total != prev_count:
        logger.error(
            f"件数不整合: filtered({filtered_count}) + removed({removed_count}) "
            f"= {total} ≠ 01-2({prev_count})"
        )
        return False

    logger.ok(f"件数整合OK: {prev_count} == {filtered_count} + {removed_count}")
    return True


def check_removed_content(
    removed_ids: List[str],
    master: Dict[str, Dict],
) -> None:
    """チェック②: 除外されたメールの内容を人間確認用に出力する。"""
    logger.info("=== チェック②: 除外内容 人間確認用出力 ===")

    if not removed_ids:
        logger.info("  除外されたメールはありません。")
        return

    logger.info(f"  除外件数: {len(removed_ids)}件")
    logger.info("")
    logger.info(f"  {'No.':<5} {'message_id':<25} {'from':<35} {'subject':<35} 本文冒頭100文字")
    logger.info(f"  {'-'*5} {'-'*25} {'-'*35} {'-'*35} {'-'*40}")

    not_found = 0
    for i, mid in enumerate(removed_ids, 1):
        rec = master.get(mid)
        if rec is None:
            logger.warn(f"  [{i}] message_id={mid} が 01-1 に見つかりません")
            not_found += 1
            continue

        from_addr = (rec.get("from") or "")[:33]
        subject = (rec.get("subject") or "")[:33]
        body = (rec.get("body_text") or "").replace("\n", " ").replace("\r", "")[:100]
        mid_short = mid[:23]
        logger.info(f"  {i:<5} {mid_short:<25} {from_addr:<35} {subject:<35} {body}")

    if not_found > 0:
        logger.warn(f"  {not_found}件のmessage_idが01-1に見つかりませんでした。")


def check_detail_records(prev_records, filtered_records, removed_records, details, master) -> bool:
    """ID出力の互換性・分割整合と、新規退避の理由/原文全項目を照合する。"""
    from_only, from_subject = load_exclude_list(EXCLUDE_LIST_PATH)
    expected_filtered, expected_removed, expected_details = [], [], []
    for rec in prev_records:
        mid = rec["message_id"]
        original = master.get(mid)
        reason = determine_exclusion_reason(original, from_only, from_subject) if original else None
        (expected_removed if reason else expected_filtered).append({"message_id": mid})
        if reason in TEMPLATE_REASONS:
            detail_reason, rule_id = detect_template_exclusion(original)
            expected_details.append(build_detail_record(original, detail_reason, rule_id))
    ok = (filtered_records == expected_filtered and removed_records == expected_removed
          and details == expected_details)
    if not ok:
        logger.error("ID出力または詳細退避の不整合（件数/順序/理由/rule_id/元メール全項目）")
        return False
    logger.ok(f"互換性・原文保持OK: 既存除外={len(removed_records) - len(details)}件 / 新規={len(details)}件")
    return True


def main() -> None:
    # ファイル存在確認
    filtered_path = get_result_path(str(_STEP_DIR), OUTPUT_FILTERED)
    removed_path = get_result_path(str(_STEP_DIR), OUTPUT_REMOVED)
    detail_path = get_result_path(str(_STEP_DIR), OUTPUT_DETAIL)

    for path in [INPUT_MASTER, INPUT_PREV, filtered_path, removed_path, detail_path]:
        if not Path(path).exists():
            logger.error(f"ファイルが存在しません: {path}")
            sys.exit(1)

    # データ読み込み
    try:
        master = read_jsonl_as_dict(INPUT_MASTER, key="message_id")
        prev_records = read_jsonl_as_list(INPUT_PREV)
        filtered_records = read_jsonl_as_list(filtered_path)
        removed_records = read_jsonl_as_list(removed_path)
        detail_records = read_jsonl_as_list(detail_path)
    except Exception as e:
        logger.error(f"ファイル読み込みエラー: {e}")
        sys.exit(1)

    prev_count = len(prev_records)
    filtered_count = len(filtered_records)
    removed_ids = [r.get("message_id", "") for r in removed_records]

    # チェック①
    ok = check_counts(prev_count, filtered_count, len(removed_ids))
    detail_ok = check_detail_records(
        prev_records, filtered_records, removed_records, detail_records, master
    )

    # チェック②（件数NGでも内容確認は出力する）
    check_removed_content(removed_ids, master)

    if not (ok and detail_ok):
        logger.error("confirm NG。Pipelineを停止します。")
        sys.exit(1)

    logger.ok(
        f"confirm OK: 入力={prev_count}件 / 出力={filtered_count}件 / 除外={len(removed_ids)}件"
    )


if __name__ == "__main__":
    main()

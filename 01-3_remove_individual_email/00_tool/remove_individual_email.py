#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 01-3: 個別除外処理スクリプト

複数要員メール・キャンペーンメール等を個別除外する。
除外条件は 10_assistance_tool/exclude_list.txt で管理する。

除外リスト形式（1行につき）:
  from のみ    → そのアドレスからの全メールを除外
  from,subject → from + subject が一致するメールを除外
  # で始まる行・空行はスキップ

入力①（本文参照用）:
  01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl

入力②（処理対象のmessage_id）:
  01-2_remove_duplicate_emails/01_result/remove_duplicate_emails_raw.jsonl

出力①: 01_result/remove_individual_emails_raw.jsonl  （除外後の message_id）
出力②: 01_result/99_removed_individual_emails_raw.jsonl （除外された message_id）
"""

import fnmatch
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Set, Tuple

# common モジュールのパス解決
_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "01-3_remove_individual_email"
logger = get_logger(STEP_NAME)

INPUT_MASTER = str(
    _PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)
INPUT_PREV = str(
    _PROJECT_ROOT / "01-2_remove_duplicate_emails" / "01_result" / "remove_duplicate_emails_raw.jsonl"
)
EXCLUDE_LIST_PATH = str(_STEP_DIR / "10_assistance_tool" / "exclude_list.txt")

OUTPUT_FILTERED = "remove_individual_emails_raw.jsonl"
OUTPUT_REMOVED = "99_removed_individual_emails_raw.jsonl"


def normalize(s: str) -> str:
    """比較用に正規化（NFKC + 小文字 + 前後空白除去）。"""
    return unicodedata.normalize("NFKC", s or "").strip().lower()


def extract_email(s: str) -> str:
    """
    'Display Name <email@example.com>' や '<email@example.com>' から
    メールアドレス部分だけを抽出して正規化する。
    '<>' が存在しない場合はそのまま normalize する。
    """
    m = re.search(r"<([^>]+)>", s or "")
    if m:
        return normalize(m.group(1))
    return normalize(s)


def subject_matches(pattern: str, subject: str) -> bool:
    """
    subject の一致判定。
    pattern に * が含まれる場合は fnmatch によるワイルドカード一致。
    * が含まれない場合は完全一致。
    どちらも正規化済みの文字列を受け取ること。
    """
    if "*" in pattern:
        return fnmatch.fnmatch(subject, pattern)
    return pattern == subject


def load_exclude_list(path: str) -> Tuple[Set[str], List[Tuple[str, str]]]:
    """
    除外リストを読み込む。
    戻り値:
      from_only_set   : from のみ指定（正規化済みアドレスのセット）
      from_subj_rules : (from, subject_pattern) のリスト（正規化済み）
                        subject_pattern は * を含む場合はワイルドカードとして扱う
    """
    from_only_set: Set[str] = set()
    from_subj_rules: List[Tuple[str, str]] = []

    exclude_path = Path(path)
    if not exclude_path.exists():
        logger.warn(f"除外リストファイルが存在しません: {path}")
        return from_only_set, from_subj_rules

    with open(exclude_path, encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",", 1)
            if len(parts) == 1:
                # from のみ
                from_only_set.add(normalize(parts[0]))
            else:
                # from,subject（subject は * ありの場合はワイルドカードパターン）
                from_subj_rules.append((normalize(parts[0]), normalize(parts[1])))

    logger.info(
        f"除外リスト読み込み完了: fromのみ={len(from_only_set)}件, "
        f"from+subject={len(from_subj_rules)}件"
    )
    return from_only_set, from_subj_rules


def is_excluded(
    record: Dict,
    from_only_set: Set[str],
    from_subj_rules: List[Tuple[str, str]],
) -> bool:
    """レコードが除外対象かどうかを判定する。"""
    norm_from = extract_email(record.get("from") or "")
    norm_subject = normalize(record.get("subject") or "")

    if norm_from in from_only_set:
        return True
    for rule_from, rule_subj in from_subj_rules:
        if rule_from == norm_from and subject_matches(rule_subj, norm_subject):
            return True
    return False


def main() -> None:
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = str(dirs["result"])

    start_time = time.time()
    filtered_records: List[Dict] = []
    removed_records: List[Dict] = []

    try:
        # 除外リスト読み込み
        from_only_set, from_subj_rules = load_exclude_list(EXCLUDE_LIST_PATH)

        # メールマスタ読み込み（message_id → レコード）
        logger.info(f"メールマスタ読み込み: {INPUT_MASTER}")
        master = read_jsonl_as_dict(INPUT_MASTER, key="message_id")
        logger.info(f"メールマスタ件数: {len(master)}件")

        # 01-2 の出力（処理対象 message_id）読み込み
        logger.info(f"01-2 出力読み込み: {INPUT_PREV}")
        prev_records = read_jsonl_as_list(INPUT_PREV)
        input_count = len(prev_records)
        logger.info(f"01-2 入力件数: {input_count}件")

        not_found_count = 0
        for rec in prev_records:
            mid = rec.get("message_id", "")
            master_rec = master.get(mid)
            if master_rec is None:
                logger.warn(f"メールマスタに存在しない message_id: {mid}")
                not_found_count += 1
                # マスタになくても除外せずに残す（安全側）
                filtered_records.append({"message_id": mid})
                continue

            if is_excluded(master_rec, from_only_set, from_subj_rules):
                removed_records.append({"message_id": mid})
            else:
                filtered_records.append({"message_id": mid})

        if not_found_count > 0:
            logger.warn(f"メールマスタに存在しなかった件数: {not_found_count}件")

        logger.info(
            f"個別除外: {input_count}件 → {len(filtered_records)}件 "
            f"（除外: {len(removed_records)}件）"
        )

        # 出力①: 除外後 message_id
        out_filtered = str(dirs["result"] / OUTPUT_FILTERED)
        write_jsonl(out_filtered, filtered_records)
        logger.ok(f"出力①書き込み完了: {out_filtered} ({len(filtered_records)}件)")

        # 出力②: 除外された message_id
        out_removed = str(dirs["result"] / OUTPUT_REMOVED)
        write_jsonl(out_removed, removed_records)
        logger.ok(f"出力②書き込み完了: {out_removed} ({len(removed_records)}件)")

    except Exception as e:
        write_error_log(result_dir, e, context=f"input={INPUT_PREV}")
        logger.error(f"処理失敗: {e}")
        sys.exit(1)

    finally:
        elapsed = time.time() - start_time
        write_execution_time(
            str(dirs["execution_time"]),
            STEP_NAME,
            elapsed,
            record_count=len(filtered_records),
        )

    logger.ok(
        f"Step完了: 入力={input_count}件 / 出力={len(filtered_records)}件 / "
        f"除外={len(removed_records)}件"
    )


if __name__ == "__main__":
    main()

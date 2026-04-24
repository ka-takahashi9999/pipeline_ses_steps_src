#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 01-2: メール重複除去スクリプト

from（送信元）と subject（件名）が同一のメールを重複とみなし、
最新日付のものを残して除去する。

入力 : 01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl
出力①: 01_result/remove_duplicate_emails_raw.jsonl  （重複除去後の message_id）
出力②: 01_result/99_duplicate_emails_raw.jsonl      （除去された重複の message_id）

重複判定キー: (normalized_from, normalized_subject)
  - Unicode NFKC 正規化
  - Re:/Fw:/Fwd: プレフィックス除去
  - 同一キーが複数ある場合は date が最新のものを残す
"""

import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple

# common モジュールのパス解決
_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "01-2_remove_duplicate_emails"
logger = get_logger(STEP_NAME)

INPUT_PATH = str(
    _PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)
OUTPUT_DEDUPED = "remove_duplicate_emails_raw.jsonl"
OUTPUT_DUPLICATES = "99_duplicate_emails_raw.jsonl"


# ---------------------------------------------------------------------------
# 正規化
# ---------------------------------------------------------------------------

def normalize_key(s: str) -> str:
    """
    重複判定用キー正規化。
    - Unicode NFKC 正規化（全角→半角等）
    - Re:/Fw:/Fwd: プレフィックス除去
    - 前後空白・連続空白の除去
    """
    s = unicodedata.normalize("NFKC", s or "").strip()
    s = re.sub(r"^(re|fw|fwd)\s*:\s*", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s)
    return s.lower()


# ---------------------------------------------------------------------------
# 重複除去
# ---------------------------------------------------------------------------

def deduplicate(
    records: List[Dict],
) -> Tuple[List[Dict], List[Dict]]:
    """
    (from, subject) キーで重複除去する。
    同一キーが複数ある場合は date が最新のものを残す。

    Returns:
        (deduped_records, duplicate_records)
        deduped_records   : 重複除去後のレコード（全フィールド保持）
        duplicate_records : 除去されたレコード（全フィールド保持）
    """
    # キー → 残す候補レコード を管理
    best: Dict[Tuple[str, str], Dict] = {}
    # キー → 除去候補リスト を管理
    dropped: Dict[Tuple[str, str], List[Dict]] = {}

    for rec in records:
        key = (
            normalize_key(rec.get("from") or ""),
            normalize_key(rec.get("subject") or ""),
        )
        if key not in best:
            best[key] = rec
            dropped[key] = []
        else:
            # 既存候補 vs 新規：date 文字列の辞書比較で最新を残す
            existing_date = best[key].get("date") or ""
            new_date = rec.get("date") or ""
            if new_date > existing_date:
                # 新規が新しい → 既存を除去候補に回す
                dropped[key].append(best[key])
                best[key] = rec
            else:
                # 既存が新しい（または同日） → 新規を除去候補に追加
                dropped[key].append(rec)

    deduped = list(best.values())
    duplicates = [r for recs in dropped.values() for r in recs]

    return deduped, duplicates


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = str(dirs["result"])

    start_time = time.time()
    deduped_records: List[Dict] = []
    duplicate_records: List[Dict] = []

    try:
        logger.info(f"入力ファイル読み込み: {INPUT_PATH}")
        all_records = read_jsonl_as_list(INPUT_PATH)
        input_count = len(all_records)
        logger.info(f"入力件数: {input_count}件")

        deduped_records, duplicate_records = deduplicate(all_records)
        logger.info(
            f"重複除去: {input_count}件 → {len(deduped_records)}件 "
            f"（除去: {len(duplicate_records)}件）"
        )

        # 出力①: 重複除去後 message_id のみ
        deduped_out = [{"message_id": r["message_id"]} for r in deduped_records]
        out_path_deduped = str(dirs["result"] / OUTPUT_DEDUPED)
        write_jsonl(out_path_deduped, deduped_out)
        logger.ok(f"出力①書き込み完了: {out_path_deduped} ({len(deduped_out)}件)")

        # 出力②: 除去された重複 message_id のみ
        duplicates_out = [{"message_id": r["message_id"]} for r in duplicate_records]
        out_path_dup = str(dirs["result"] / OUTPUT_DUPLICATES)
        write_jsonl(out_path_dup, duplicates_out)
        logger.ok(f"出力②書き込み完了: {out_path_dup} ({len(duplicates_out)}件)")

    except Exception as e:
        write_error_log(result_dir, e, context=f"input={INPUT_PATH}")
        logger.error(f"処理失敗: {e}")
        sys.exit(1)

    finally:
        elapsed = time.time() - start_time
        write_execution_time(
            str(dirs["execution_time"]),
            STEP_NAME,
            elapsed,
            record_count=len(deduped_records),
        )

    logger.ok(
        f"Step完了: 入力={input_count}件 / 出力={len(deduped_records)}件 / "
        f"除去={len(duplicate_records)}件"
    )


if __name__ == "__main__":
    main()

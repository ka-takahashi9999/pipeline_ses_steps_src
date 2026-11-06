#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 05-9: 要員メールから工程をルールベースで抽出

Feature flag (config.py の ENABLE_PHASE_CATEGORY):
  False（デフォルト）:
    - 全レコードを phases=[], phases_raw=null でpass-through出力
  True:
    - phase_dictionary.txt を使って工程を抽出

辞書形式（YAML）:
  正規工程名（キー）に対して、マッチキーワードリストを持つ。
  キーワードにマッチした場合、正規工程名を phases に追加する。
  同一の正規工程名は重複して追加しない。

マッチングルール:
  ・NFKC正規化後にキーワードマッチング
  ・日本語キーワードは文字境界なし、英数字キーワード（UT/IT/ST/PMO等）はASCII英数字境界を使用

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/resources.jsonl

出力①（抽出結果）:
  01_result/extract_resource_phase_category.jsonl
出力②（nullまたは空フェーズの異常レコード）:
  01_result/99_phase_null_extract_resource_phase_category.jsonl
"""

import sys
import time
from pathlib import Path

_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

sys.path.insert(0, str(_STEP_DIR / "00_tool"))
from config import ENABLE_PHASE_CATEGORY

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger
from common.phase_extractor import (
    PHASE_DICTIONARY_PATH,
    build_extracted_record,
    build_passthrough_record,
    extract_phases,
    is_valid_phase_record,
    load_phase_dictionary,
    normalize_phase_text,
)

STEP_NAME = "05-9_extract_resource_phase_category"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_RESOURCES = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "resources.jsonl"
)
PHASE_DICT_PATH = str(PHASE_DICTIONARY_PATH)
OUTPUT_EXTRACTED = "extract_resource_phase_category.jsonl"
OUTPUT_NULL      = "99_phase_null_extract_resource_phase_category.jsonl"


# 既存のfocused test・呼び出し互換を保つstep公開名。
_n = normalize_phase_text
_is_valid = is_valid_phase_record


# ── メイン ────────────────────────────────────────────────
def main() -> None:
    start = time.time()
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = dirs["result"]

    logger.info(f"ENABLE_PHASE_CATEGORY = {ENABLE_PHASE_CATEGORY}")

    for path in [INPUT_RESOURCES, INPUT_CLEANED]:
        if not Path(path).exists():
            logger.error(f"入力ファイルが存在しません: {path}")
            sys.exit(1)

    try:
        resource_ids = [r["message_id"] for r in read_jsonl_as_list(INPUT_RESOURCES)]
    except Exception as e:
        write_error_log(str(result_dir), e, "入力ファイル読み込みエラー")
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    # feature flag OFF: pass-through
    if not ENABLE_PHASE_CATEGORY:
        logger.info("feature flag OFF: pass-through モードで実行します")
        extracted = [build_passthrough_record(mid) for mid in resource_ids]
        write_jsonl(str(result_dir / OUTPUT_EXTRACTED), extracted)
        write_jsonl(str(result_dir / OUTPUT_NULL), [])
        elapsed = time.time() - start
        write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, len(resource_ids))
        logger.ok(f"Step完了(pass-through): 入力={len(resource_ids)}件 / 出力={len(extracted)}件")
        return

    # feature flag ON: 辞書ロード → 抽出
    if not Path(PHASE_DICT_PATH).exists():
        logger.error(f"工程辞書が存在しません: {PHASE_DICT_PATH}")
        sys.exit(1)

    try:
        import yaml  # noqa: F401
    except ImportError:
        logger.error("PyYAMLがインストールされていません: pip install pyyaml")
        sys.exit(1)

    try:
        phase_dict  = load_phase_dictionary(PHASE_DICT_PATH)
        cleaned_map = read_jsonl_as_dict(INPUT_CLEANED)
    except Exception as e:
        write_error_log(str(result_dir), e, "初期化エラー")
        logger.error(f"初期化エラー: {e}")
        sys.exit(1)

    total_kw = sum(len(v) for v in phase_dict.values())
    logger.info(f"工程辞書ロード完了: {len(phase_dict)}工程 / {total_kw}キーワード")

    extracted_list: list = []
    null_records: list = []

    for mid in resource_ids:
        body = (cleaned_map.get(mid) or {}).get("body_text", "")
        rec  = build_extracted_record(mid, body, phase_dict)

        if not _is_valid(rec):
            null_records.append(rec)
        else:
            extracted_list.append(rec)

        logger.info(
            f"{mid} → phases={rec.get('phases')} raw={str(rec.get('phases_raw', ''))[:40]}",
            message_id=mid,
        )

    write_jsonl(str(result_dir / OUTPUT_EXTRACTED), extracted_list)
    write_jsonl(str(result_dir / OUTPUT_NULL), null_records)

    elapsed = time.time() - start
    total   = len(resource_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    has_phases   = sum(1 for r in extracted_list if r.get("phases"))
    empty_phases = sum(1 for r in extracted_list if not r.get("phases"))
    logger.ok(
        f"Step完了: 入力={total}件 / 工程あり={has_phases}件 / "
        f"工程なし={empty_phases}件 / null={len(null_records)}件"
    )


if __name__ == "__main__":
    main()

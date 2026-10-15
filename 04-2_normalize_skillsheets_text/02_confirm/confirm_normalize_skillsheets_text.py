#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04-2_normalize_skillsheets_text: 確認スクリプト

チェック項目:
  1. 出力ファイル存在確認
  2. 入力件数と出力件数の一致
  3. JSONL 全行 parse 済みで必須キーが存在すること
  4. 04-1 の raw skillsheet が上書きされていないことを件数・参照先で確認
"""

import sys
from pathlib import Path
from typing import Any, Dict, List

STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.json_utils import read_jsonl_as_list
from common.logger import get_logger

logger = get_logger("confirm_04-2")

INPUT_JSONL = (
    PROJECT_ROOT
    / "04-1_fetch_skillsheets_text"
    / "01_result"
    / "fetch_skillsheets_text.jsonl"
)
OUTPUT_JSONL = STEP_DIR / "01_result" / "normalize_skillsheets_text.jsonl"
SUMMARY_TXT = STEP_DIR / "02_confirm" / "result" / "normalize_skillsheets_text_summary.txt"
DETAIL_TSV = STEP_DIR / "02_confirm" / "result" / "normalize_skillsheets_text_detail.tsv"

REQUIRED_KEYS = {
    "message_id",
    "success",
    "source",
    "urls",
    "skillsheet",
    "raw_char_count",
    "clean_char_count",
    "reduction_char_count",
    "reduction_rate",
    "cleanup_flags",
    "cleanup_stats",
    "source_step",
}
MULTI_PERSON_QUALITY_FLAG = "multi_person_skillsheet"


def _validate_record(record: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    missing = REQUIRED_KEYS - set(record.keys())
    if missing:
        errors.append(f"必須キー不足: {sorted(missing)}")
    if not isinstance(record.get("cleanup_flags"), list):
        errors.append("cleanup_flags が list ではありません")
    if not isinstance(record.get("cleanup_stats"), dict):
        errors.append("cleanup_stats が dict ではありません")
    for key in ("raw_char_count", "clean_char_count", "reduction_char_count"):
        if not isinstance(record.get(key), int):
            errors.append(f"{key} が int ではありません")
    if not isinstance(record.get("reduction_rate"), (int, float)):
        errors.append("reduction_rate が数値ではありません")
    if record.get("source_step") != "04-1_fetch_skillsheets_text":
        errors.append("source_step が不正です")
    return errors


def run_confirm() -> bool:
    ok = True
    logger.info("=== 04-2 confirm 開始 ===")

    for path in (INPUT_JSONL, OUTPUT_JSONL, SUMMARY_TXT, DETAIL_TSV):
        if not path.exists():
            logger.error(f"ファイルが見つかりません: {path}")
            ok = False
    if not ok:
        sys.exit(1)

    inputs = read_jsonl_as_list(str(INPUT_JSONL))
    outputs = read_jsonl_as_list(str(OUTPUT_JSONL))

    if len(inputs) != len(outputs):
        logger.error(f"件数不一致: input={len(inputs)} / output={len(outputs)}")
        ok = False
    else:
        logger.ok(f"件数整合OK: {len(outputs)} 件")

    error_count = 0
    for record in outputs:
        errors = _validate_record(record)
        if errors:
            error_count += 1
            logger.error(f"不正レコード message_id={record.get('message_id')}: {errors}")
    if error_count:
        ok = False
        logger.error(f"レコード検証NG: {error_count} 件")
    else:
        logger.ok(f"必須キー・型確認OK: {len(outputs)} 件")

    input_fail_count = sum(1 for r in inputs if r.get("success") is not True)
    output_fail_count = sum(1 for r in outputs if r.get("success") is not True)
    quality_failures = [
        r for r in outputs
        if MULTI_PERSON_QUALITY_FLAG in (r.get("cleanup_flags") or [])
    ]
    invalid_quality_failures = [
        r for r in quality_failures
        if r.get("success") is not False or r.get("skillsheet") != ""
    ]
    if invalid_quality_failures:
        logger.error(
            "multi_person_skillsheetのsuccess/skillsheet不整合: "
            f"{len(invalid_quality_failures)} 件"
        )
        ok = False

    expected_output_fail_count = input_fail_count + len(quality_failures)
    if expected_output_fail_count != output_fail_count:
        logger.error(
            "success=false件数不一致: "
            f"input={input_fail_count} + quality_ng={len(quality_failures)} "
            f"/ output={output_fail_count}"
        )
        ok = False
    else:
        logger.ok(
            "success=false整合OK: "
            f"input={input_fail_count} / quality_ng={len(quality_failures)} "
            f"/ output={output_fail_count}"
        )

    raw_20000 = sum(1 for r in outputs if int(r.get("raw_char_count") or 0) >= 20000)
    clean_20000 = sum(1 for r in outputs if int(r.get("clean_char_count") or 0) >= 20000)
    logger.info(f"2万文字以上: raw={raw_20000} 件 / clean={clean_20000} 件")

    if ok:
        logger.ok("=== confirm 全チェック合格 ===")
        return True

    logger.error("=== confirm 不合格 ===")
    sys.exit(1)


if __name__ == "__main__":
    run_confirm()

"""
09-4_remove_category_mismatch_sales_candidates

09-3 の営業メール文脈から、category_match == "mismatch" の候補だけを除外する。
この step は除外専用とし、メール文面生成やスキル再判定は行わない。
"""

import argparse
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "09-4_remove_category_mismatch_sales_candidates"
STEP_DIR = Path(__file__).resolve().parents[1]
INPUT_BASE_DIR = project_root / "09-3_prepare_sales_mail_context/01_result"
OUTPUT_CANDIDATES_TEMPLATE = "sales_proposal_candidates_{date}.jsonl"
OUTPUT_EXCLUDED_TEMPLATE = "99_excluded_category_mismatch_sales_candidates_{date}.jsonl"


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-date", help="処理対象日付 YYYYMMDD")
    return parser.parse_args()


def resolve_target_date(target_date: Optional[str]) -> Optional[str]:
    if target_date is None:
        return None
    if not re.fullmatch(r"\d{8}", target_date):
        raise ValueError(f"--target-date は YYYYMMDD 形式で指定してください: {target_date}")
    return target_date


def find_latest_input() -> Tuple[Path, str]:
    candidates = sorted(INPUT_BASE_DIR.glob("prepare_sales_mail_context_*.jsonl"))
    if not candidates:
        raise FileNotFoundError(f"09-3入力JSONLが存在しません: {INPUT_BASE_DIR}")
    latest = candidates[-1]
    date_part = latest.stem.replace("prepare_sales_mail_context_", "", 1)
    if not re.fullmatch(r"\d{8}", date_part):
        raise ValueError(f"入力日付を解釈できません: {latest.name}")
    return latest, date_part


def resolve_input(target_date: Optional[str]) -> Tuple[Path, str, str]:
    if target_date:
        input_path = INPUT_BASE_DIR / f"prepare_sales_mail_context_{target_date}.jsonl"
        if not input_path.exists():
            raise FileNotFoundError(f"対象日付の09-3入力JSONLが存在しません: {input_path}")
        return input_path, target_date, "target-date"
    latest, date_part = find_latest_input()
    return latest, date_part, "latest"


def is_category_mismatch(record: Dict[str, Any]) -> bool:
    return normalize_text(record.get("category_match")).lower() == "mismatch"


def build_excluded_record(record: Dict[str, Any]) -> Dict[str, Any]:
    excluded = dict(record)
    excluded["exclusion_info"] = {
        "excluded_step": STEP_NAME,
        "excluded_reason": "category_match_mismatch",
        "category_match": record.get("category_match"),
        "category_note": record.get("category_note", ""),
    }
    return excluded


def main() -> None:
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()
    args = parse_args()

    try:
        target_date = resolve_target_date(args.target_date)
        input_path, date_part, resolve_mode = resolve_input(target_date)
        output_candidates = STEP_DIR / "01_result" / OUTPUT_CANDIDATES_TEMPLATE.format(date=date_part)
        output_excluded = STEP_DIR / "01_result" / OUTPUT_EXCLUDED_TEMPLATE.format(date=date_part)

        logger.info(f"input resolve mode: {resolve_mode}")
        logger.info(f"target date: {date_part}")
        logger.info(f"09-3 input path: {input_path}")

        input_records = read_jsonl_as_list(str(input_path))
        candidates: List[Dict[str, Any]] = []
        excluded: List[Dict[str, Any]] = []

        for record in input_records:
            if is_category_mismatch(record):
                excluded.append(build_excluded_record(record))
            else:
                candidates.append(record)

        write_jsonl(str(output_candidates), candidates)
        write_jsonl(str(output_excluded), excluded)

        elapsed = time.time() - start_time
        write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, len(input_records))
        logger.ok(
            f"カテゴリ不一致除外完了: input={len(input_records)} "
            f"candidates={len(candidates)} excluded={len(excluded)}"
        )
        logger.ok(f"候補出力: {output_candidates}")
        logger.ok(f"除外出力: {output_excluded}")

    except Exception as error:
        write_error_log(str(dirs["result"]), error, context=STEP_NAME)
        logger.error(f"処理失敗: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()

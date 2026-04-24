"""
08-4_match_score_sort
各パーティションファイルをtotal_skills_match_rateの降順でソートする。
同率の場合はrequired_skills_match_rateの降順。
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "08-4_match_score_sort"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_DIR = project_root / "08-3_match_score_partition/01_result"
OUTPUT_DIR = STEP_DIR / "01_result"

# 入力→出力のファイルペア定義
FILE_PAIRS = [
    ("match_score_partition_requir_100percent.jsonl",  "match_score_sort_100percent.jsonl"),
    ("match_score_partition_requir_80to99percent.jsonl", "match_score_sort_80to99percent.jsonl"),
    ("match_score_partition_requir_60to79percent.jsonl", "match_score_sort_60to79percent.jsonl"),
    ("match_score_partition_requir_40to59percent.jsonl", "match_score_sort_40to59percent.jsonl"),
    ("match_score_partition_requir_20to39percent.jsonl", "match_score_sort_20to39percent.jsonl"),
    ("match_score_partition_requir_1to19percent.jsonl",  "match_score_sort_1to19percent.jsonl"),
    ("match_score_partition_requir_0percent.jsonl",      "match_score_sort_0percent.jsonl"),
]


def init_output_files(generated_at: str) -> None:
    """出力7ファイルをno_matchステータスで初期化する。"""
    status_record = json.dumps(
        {"status": "no_match", "generated_at": generated_at, "count": 0},
        ensure_ascii=False,
    )
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for _, out_filename in FILE_PAIRS:
        with open(OUTPUT_DIR / out_filename, "w", encoding="utf-8") as f:
            f.write(status_record + "\n")


def is_no_match_file(records: list) -> bool:
    """レコードリストがno_matchステータスのみかどうかを判定する。"""
    return len(records) == 1 and records[0].get("status") == "no_match"


def sort_key(rec: dict) -> tuple:
    """ソートキー: total降順, required降順（negateで降順化）。"""
    match_info = rec.get("match_info", {})
    total = match_info.get("total_skills_match_rate", 0.0)
    required = match_info.get("required_skills_match_rate", 0.0)
    return (-total, -required)


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    try:
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ① 出力7ファイルを全て初期化（最初に必ず実行）
        init_output_files(generated_at)
        logger.info("出力7ファイルを初期化完了")

        total_input = 0
        total_output = 0

        # ② 各ファイルを読み込み → ソート → 書き込み
        for in_filename, out_filename in FILE_PAIRS:
            in_path = INPUT_DIR / in_filename
            out_path = OUTPUT_DIR / out_filename

            if not in_path.exists():
                logger.warn(f"入力ファイルが存在しない: {in_filename}")
                continue

            records = read_jsonl_as_list(str(in_path))

            if is_no_match_file(records):
                # no_matchファイルはそのまま初期化済みファイルを維持
                logger.info(f"{out_filename}: 0件 (no_match)")
                continue

            total_input += len(records)
            sorted_records = sorted(records, key=sort_key)
            write_jsonl(str(out_path), sorted_records)
            total_output += len(sorted_records)
            logger.info(f"{out_filename}: {len(sorted_records)}件")

        elapsed = time.time() - start_time
        write_execution_time(
            str(dirs["execution_time"]), STEP_NAME, elapsed, total_output
        )
        logger.ok(f"処理完了: 入力合計={total_input} 出力合計={total_output}")

    except Exception as e:
        write_error_log(str(dirs["result"]), e, context=STEP_NAME)
        logger.error(f"処理失敗: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

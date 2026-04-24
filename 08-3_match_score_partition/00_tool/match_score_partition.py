"""
08-3_match_score_partition
必須スキル一致率でファイルを7分割する。
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

STEP_NAME = "08-3_match_score_partition"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_FILE = (
    project_root
    / "08-2_match_score_aggregation/01_result/match_score_aggregation.jsonl"
)

OUTPUT_DIR = STEP_DIR / "01_result"

# 分割定義: (ファイル名, 判定関数)
PARTITIONS = [
    ("match_score_partition_requir_100percent.jsonl",  lambda r: r == 1.0),
    ("match_score_partition_requir_80to99percent.jsonl", lambda r: 0.8 <= r < 1.0),
    ("match_score_partition_requir_60to79percent.jsonl", lambda r: 0.6 <= r < 0.8),
    ("match_score_partition_requir_40to59percent.jsonl", lambda r: 0.4 <= r < 0.6),
    ("match_score_partition_requir_20to39percent.jsonl", lambda r: 0.2 <= r < 0.4),
    ("match_score_partition_requir_1to19percent.jsonl",  lambda r: 0.0 < r < 0.2),
    ("match_score_partition_requir_0percent.jsonl",      lambda r: r == 0.0),
]


def init_output_files(generated_at: str) -> None:
    """出力7ファイルをno_matchステータスで初期化する。"""
    status_record = json.dumps(
        {"status": "no_match", "generated_at": generated_at, "count": 0},
        ensure_ascii=False,
    )
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for filename, _ in PARTITIONS:
        path = OUTPUT_DIR / filename
        with open(path, "w", encoding="utf-8") as f:
            f.write(status_record + "\n")


def classify(rate: float) -> int:
    """required_skills_match_rateをパーティションインデックスに変換する。"""
    for i, (_, predicate) in enumerate(PARTITIONS):
        if predicate(rate):
            return i
    # 浮動小数点誤差のフォールバック: 1.0超は100%, 負は0%
    if rate > 1.0:
        return 0
    return 6


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    try:
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ① 出力7ファイルを全て初期化（最初に必ず実行）
        init_output_files(generated_at)
        logger.info("出力7ファイルを初期化完了")

        # ② 入力データ読み込み・分類
        records = read_jsonl_as_list(str(INPUT_FILE))
        logger.info(f"入力件数={len(records)}")

        buckets = [[] for _ in PARTITIONS]
        for rec in records:
            rate = rec.get("match_info", {}).get("required_skills_match_rate", 0.0)
            idx = classify(rate)
            buckets[idx].append(rec)

        # ③ 該当ファイルに書き込み（1件以上の場合はwrite_jsonlで上書き）
        for i, (filename, _) in enumerate(PARTITIONS):
            path = OUTPUT_DIR / filename
            if buckets[i]:
                write_jsonl(str(path), buckets[i])
                logger.info(f"{filename}: {len(buckets[i])}件")
            else:
                logger.info(f"{filename}: 0件 (no_match)")

        elapsed = time.time() - start_time
        write_execution_time(
            str(dirs["execution_time"]), STEP_NAME, elapsed, len(records)
        )
        logger.ok(f"処理完了: 合計={len(records)}件")

    except Exception as e:
        write_error_log(str(dirs["result"]), e, context=STEP_NAME)
        logger.error(f"処理失敗: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

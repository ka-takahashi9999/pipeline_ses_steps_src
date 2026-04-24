#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 06-0: 全案件×全要員の総当たりペアを生成（06系の起点）

・ambiguous/unknown は分類未確定のためマッチング対象外（使用禁止）
・出力件数 = projects件数 × resources件数

入力①（案件）:
  02-2_classify_output_file_project_resource/01_result/projects.jsonl
入力②（要員）:
  02-2_classify_output_file_project_resource/01_result/resources.jsonl

出力①（全ペア）:
  01_result/matched_pairs_all.jsonl
  {"project_info": {"message_id": "..."}, "resource_info": {"message_id": "..."}}
"""

import sys
import time
from pathlib import Path
from typing import List

_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "06-0_match_all_message_id"
logger = get_logger(STEP_NAME)

INPUT_PROJECTS  = str(_PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl")
INPUT_RESOURCES = str(_PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "resources.jsonl")
OUTPUT_PAIRS    = "matched_pairs_all.jsonl"


def main() -> None:
    start = time.time()
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = dirs["result"]

    for path in [INPUT_PROJECTS, INPUT_RESOURCES]:
        if not Path(path).exists():
            logger.error(f"入力ファイルが存在しません: {path}")
            sys.exit(1)

    try:
        project_ids  = [r["message_id"] for r in read_jsonl_as_list(INPUT_PROJECTS)]
        resource_ids = [r["message_id"] for r in read_jsonl_as_list(INPUT_RESOURCES)]
    except Exception as e:
        write_error_log(str(result_dir), e, "入力ファイル読み込みエラー")
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    n_proj = len(project_ids)
    n_res  = len(resource_ids)
    n_pairs = n_proj * n_res
    logger.info(f"案件={n_proj}件 / 要員={n_res}件 / 総ペア数={n_pairs:,}件")

    # 総当たりペアを生成してストリーム出力（メモリ効率）
    out_path = str(result_dir / OUTPUT_PAIRS)
    pairs: List[dict] = []
    FLUSH_SIZE = 50_000  # 5万件ごとにバッファをフラッシュ

    import json
    written = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for pid in project_ids:
            for rid in resource_ids:
                f.write(json.dumps(
                    {"project_info": {"message_id": pid},
                     "resource_info": {"message_id": rid}},
                    ensure_ascii=False,
                ) + "\n")
                written += 1
                if written % FLUSH_SIZE == 0:
                    f.flush()
                    logger.info(f"  進捗: {written:,} / {n_pairs:,}件")

    elapsed = time.time() - start
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, n_pairs)
    logger.ok(
        f"Step完了: 案件={n_proj}件 × 要員={n_res}件 = {written:,}件出力 / {elapsed:.1f}秒"
    )


if __name__ == "__main__":
    main()

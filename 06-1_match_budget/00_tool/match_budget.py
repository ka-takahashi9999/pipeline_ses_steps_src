"""
06-1_match_budget
案件の単価と要員の希望単価を比較してマッチ判定する。
LLM使用禁止。

判定ロジック：
  案件unit_price >= 要員desired_unit_price + MIN_MARGIN → true
  どちらかがnull → true（デフォルト通過）
  それ以外 → false
"""

import sys
import time
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "06-1_match_budget"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS = project_root / "06-0_match_all_message_id/01_result/matched_pairs_all.jsonl"
INPUT_PROJECT_BUDGET = project_root / "03-1_extract_project_budget/01_result/extract_project_budget.jsonl"
INPUT_RESOURCE_BUDGET = project_root / "05-1_extract_resource_budget/01_result/extract_resource_budget.jsonl"

OUTPUT_MATCHED = STEP_DIR / "01_result/matched_pairs_budget.jsonl"
OUTPUT_NO_MATCHED = STEP_DIR / "01_result/99_no_matched_pairs_budget.jsonl"

# 最低利益閾値（円）: 案件単価が要員希望単価よりこの額以上高い場合のみtrue
MIN_MARGIN = 120_000


def judge_budget_match(unit_price, desired_unit_price) -> bool:
    """
    単価マッチ判定。
    どちらかがnullなら true（デフォルト通過）。
    unit_price >= desired_unit_price + MIN_MARGIN なら true、それ以外 false。
    """
    if unit_price is None or desired_unit_price is None:
        return True
    return unit_price >= desired_unit_price + MIN_MARGIN


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    logger.info("処理開始")

    try:
        # 入力ファイル読み込み
        pairs = read_jsonl_as_list(str(INPUT_PAIRS))
        project_budget_map = read_jsonl_as_dict(str(INPUT_PROJECT_BUDGET), key="message_id")
        resource_budget_map = read_jsonl_as_dict(str(INPUT_RESOURCE_BUDGET), key="message_id")

        logger.info(f"入力ペア数: {len(pairs)}")
        logger.info(f"案件単価レコード数: {len(project_budget_map)}")
        logger.info(f"要員単価レコード数: {len(resource_budget_map)}")

        matched = []
        no_matched = []

        for pair in pairs:
            project_message_id = pair["project_info"]["message_id"]
            resource_message_id = pair["resource_info"]["message_id"]

            project_rec = project_budget_map.get(project_message_id, {})
            resource_rec = resource_budget_map.get(resource_message_id, {})

            unit_price = project_rec.get("unit_price")
            desired_unit_price = resource_rec.get("desired_unit_price")

            is_match = judge_budget_match(unit_price, desired_unit_price)

            record = {
                "project_info": {"message_id": project_message_id},
                "resource_info": {"message_id": resource_message_id},
                "match_info": {
                    "match_budget": is_match,
                },
            }

            if is_match:
                matched.append(record)
                logger.info(
                    f"MATCH: project={project_message_id} resource={resource_message_id}"
                    f" unit_price={unit_price} desired={desired_unit_price}"
                )
            else:
                no_matched.append(record)
                logger.info(
                    f"NO_MATCH: project={project_message_id} resource={resource_message_id}"
                    f" unit_price={unit_price} desired={desired_unit_price}"
                )

        write_jsonl(str(OUTPUT_MATCHED), matched)
        write_jsonl(str(OUTPUT_NO_MATCHED), no_matched)

        elapsed = time.time() - start_time
        write_execution_time(
            str(dirs["execution_time"]),
            STEP_NAME,
            elapsed,
            record_count=len(pairs),
        )

        logger.info(f"処理完了 合計={len(pairs)} マッチ={len(matched)} 除外={len(no_matched)}")

    except Exception as e:
        write_error_log(str(dirs["result"]), e, context=STEP_NAME)
        logger.error(f"処理失敗: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

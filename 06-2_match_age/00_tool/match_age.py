"""
06-2_match_age
案件の年齢制限と要員の年齢を比較してマッチ判定する。
LLM使用禁止。

判定ロジック：
  要員current_age <= 案件age_max → true
  要員current_age=1（デフォルト・年齢不明）→ true（通過）
  それ以外 → false
"""

import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import merge_match_info, read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "06-2_match_age"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS = project_root / "06-1_match_budget/01_result/matched_pairs_budget.jsonl"
INPUT_PROJECT_AGE = project_root / "03-2_extract_project_age/01_result/extract_project_age.jsonl"
INPUT_RESOURCE_AGE = project_root / "05-2_extract_resource_age/01_result/extract_resource_age.jsonl"

OUTPUT_MATCHED = STEP_DIR / "01_result/matched_pairs_age.jsonl"
OUTPUT_NO_MATCHED = STEP_DIR / "01_result/99_no_matched_pairs_age.jsonl"

DEFAULT_AGE = 1  # 年齢不明のデフォルト値


def judge_age_match(current_age, age_max) -> bool:
    """
    年齢マッチ判定。
    current_age=1（デフォルト・年齢不明）は通過。
    current_age <= age_max なら true、それ以外 false。
    """
    if current_age == DEFAULT_AGE:
        return True
    if current_age is None or age_max is None:
        return True
    return current_age <= age_max


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    logger.info("処理開始")

    try:
        pairs = read_jsonl_as_list(str(INPUT_PAIRS))
        project_age_map = read_jsonl_as_dict(str(INPUT_PROJECT_AGE), key="message_id")
        resource_age_map = read_jsonl_as_dict(str(INPUT_RESOURCE_AGE), key="message_id")

        logger.info(f"入力ペア数: {len(pairs)}")
        logger.info(f"案件年齢制限レコード数: {len(project_age_map)}")
        logger.info(f"要員年齢レコード数: {len(resource_age_map)}")

        matched = []
        no_matched = []

        for pair in pairs:
            project_message_id = pair["project_info"]["message_id"]
            resource_message_id = pair["resource_info"]["message_id"]

            project_rec = project_age_map.get(project_message_id, {})
            resource_rec = resource_age_map.get(resource_message_id, {})

            age_max = project_rec.get("age_max")
            current_age = resource_rec.get("current_age")

            is_match = judge_age_match(current_age, age_max)

            record = merge_match_info(pair, {"match_age": is_match})

            if is_match:
                matched.append(record)
            else:
                no_matched.append(record)
                logger.info(
                    f"NO_MATCH: project={project_message_id} resource={resource_message_id}"
                    f" age_max={age_max} current_age={current_age}"
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

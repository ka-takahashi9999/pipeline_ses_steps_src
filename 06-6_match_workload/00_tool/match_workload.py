"""
06-6_match_workload
案件の稼働率制限と要員の稼働率を比較してマッチ判定する。
LLM使用禁止。

判定ロジック（区間オーバーラップ）：
  要員workload_max >= 案件workload_min かつ
  要員workload_min <= 案件workload_max → true
  どちらかがnull → true（デフォルト通過）
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

STEP_NAME = "06-6_match_workload"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS = project_root / "06-5_match_freelance/01_result/matched_pairs_freelance.jsonl"
INPUT_PROJECT_WORKLOAD = project_root / "03-6_extract_project_workload/01_result/extract_project_workload.jsonl"
INPUT_RESOURCE_WORKLOAD = project_root / "05-6_extract_resource_workload/01_result/extract_resource_workload.jsonl"

OUTPUT_MATCHED = STEP_DIR / "01_result/matched_pairs_workload.jsonl"
OUTPUT_NO_MATCHED = STEP_DIR / "01_result/99_no_matched_pairs_workload.jsonl"


def judge_workload_match(p_min, p_max, r_min, r_max) -> bool:
    """
    稼働率マッチ判定（区間オーバーラップ）。
    案件または要員のいずれかがnullの場合はtrue（デフォルト通過）。
    """
    if p_min is None or p_max is None or r_min is None or r_max is None:
        return True
    return r_max >= p_min and r_min <= p_max


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    logger.info("処理開始")

    try:
        pairs = read_jsonl_as_list(str(INPUT_PAIRS))
        project_workload_map = read_jsonl_as_dict(str(INPUT_PROJECT_WORKLOAD), key="message_id")
        resource_workload_map = read_jsonl_as_dict(str(INPUT_RESOURCE_WORKLOAD), key="message_id")

        logger.info(f"入力ペア数: {len(pairs)}")
        logger.info(f"案件稼働率レコード数: {len(project_workload_map)}")
        logger.info(f"要員稼働率レコード数: {len(resource_workload_map)}")

        matched = []
        no_matched = []

        for pair in pairs:
            project_message_id = pair["project_info"]["message_id"]
            resource_message_id = pair["resource_info"]["message_id"]

            project_rec = project_workload_map.get(project_message_id, {})
            resource_rec = resource_workload_map.get(resource_message_id, {})

            p_min = project_rec.get("workload_min")
            p_max = project_rec.get("workload_max")
            r_min = resource_rec.get("workload_min")
            r_max = resource_rec.get("workload_max")

            is_match = judge_workload_match(p_min, p_max, r_min, r_max)

            record = merge_match_info(pair, {"match_workload": is_match})

            if is_match:
                matched.append(record)
            else:
                no_matched.append(record)
                logger.info(
                    f"NO_MATCH: project={project_message_id} resource={resource_message_id}"
                    f" project=[{p_min},{p_max}] resource=[{r_min},{r_max}]"
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

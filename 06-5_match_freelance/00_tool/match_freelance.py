"""
06-5_match_freelance
案件の個人事業主制限と要員の雇用形態を比較してマッチ判定する。
LLM使用禁止。

判定ロジック：
  案件freelance_ok=true  → 要員はすべてtrue
  案件freelance_ok=false → 要員employment_type=employeeのみtrue
  案件がnull/不明        → true（デフォルト通過）
  要員がnull/不明        → true（デフォルト通過）
"""

import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import merge_match_info, read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "06-5_match_freelance"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS = project_root / "06-4_match_foreign/01_result/matched_pairs_foreign.jsonl"
INPUT_PROJECT_FREELANCE = project_root / "03-5_extract_project_freelance/01_result/extract_project_freelance.jsonl"
INPUT_RESOURCE_FREELANCE = project_root / "05-5_extract_resource_freelance/01_result/extract_resource_freelance.jsonl"

OUTPUT_MATCHED = STEP_DIR / "01_result/matched_pairs_freelance.jsonl"
OUTPUT_NO_MATCHED = STEP_DIR / "01_result/99_no_matched_pairs_freelance.jsonl"


def judge_freelance_match(freelance_ok, employment_type) -> bool:
    """
    個人事業主マッチ判定。
    案件または要員がnullの場合はtrue（デフォルト通過）。
    """
    if freelance_ok is None or employment_type is None:
        return True
    if freelance_ok:
        return True
    return employment_type == "employee"


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    logger.info("処理開始")

    try:
        pairs = read_jsonl_as_list(str(INPUT_PAIRS))
        project_freelance_map = read_jsonl_as_dict(str(INPUT_PROJECT_FREELANCE), key="message_id")
        resource_freelance_map = read_jsonl_as_dict(str(INPUT_RESOURCE_FREELANCE), key="message_id")

        logger.info(f"入力ペア数: {len(pairs)}")
        logger.info(f"案件個人事業主制限レコード数: {len(project_freelance_map)}")
        logger.info(f"要員雇用形態レコード数: {len(resource_freelance_map)}")

        matched = []
        no_matched = []

        for pair in pairs:
            project_message_id = pair["project_info"]["message_id"]
            resource_message_id = pair["resource_info"]["message_id"]

            project_rec = project_freelance_map.get(project_message_id, {})
            resource_rec = resource_freelance_map.get(resource_message_id, {})

            freelance_ok = project_rec.get("freelance_ok")
            employment_type = resource_rec.get("employment_type")

            is_match = judge_freelance_match(freelance_ok, employment_type)

            record = merge_match_info(pair, {"match_freelance": is_match})

            if is_match:
                matched.append(record)
            else:
                no_matched.append(record)
                logger.info(
                    f"NO_MATCH: project={project_message_id} resource={resource_message_id}"
                    f" freelance_ok={freelance_ok} employment_type={employment_type}"
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

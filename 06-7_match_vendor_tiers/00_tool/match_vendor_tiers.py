"""
06-7_match_vendor_tiers
案件の商流制限と要員の商流を比較してマッチ判定する。
LLM使用禁止。

判定ロジック：
  案件commercial_flow_level=0（制限なし）→ 要員すべてtrue
  案件commercial_flow_level=1 → 要員vendor_flow=10のみtrue
  案件commercial_flow_level=2 → 要員vendor_flow=10/11/20のみtrue
  案件commercial_flow_level=3 → 要員vendor_flow=10/11/12/20/21のみtrue
  上記以外の組み合わせはfalse
"""

import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import merge_match_info, read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "06-7_match_vendor_tiers"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS = project_root / "06-6_match_workload/01_result/matched_pairs_workload.jsonl"
INPUT_PROJECT_VENDOR = project_root / "03-7_extract_project_vendor_tiers/01_result/extract_project_vendor_tiers.jsonl"
INPUT_RESOURCE_VENDOR = project_root / "05-7_extract_resource_vendor_tiers/01_result/extract_resource_vendor_tiers.jsonl"

OUTPUT_MATCHED = STEP_DIR / "01_result/matched_pairs_vendor_tiers.jsonl"
OUTPUT_NO_MATCHED = STEP_DIR / "01_result/99_no_matched_pairs_vendor_tiers.jsonl"

# 案件commercial_flow_levelごとに許可される要員vendor_flowの集合
ALLOWED_VENDOR_FLOWS = {
    0: None,   # 制限なし（すべてtrue）
    1: {10},
    2: {10, 11, 20},
    3: {10, 11, 12, 20, 21},
}


def judge_vendor_tiers_match(commercial_flow_level, vendor_flow) -> bool:
    """
    商流マッチ判定。
    commercial_flow_level=0 は制限なし（常にtrue）。
    """
    if commercial_flow_level is None or vendor_flow is None:
        return True

    allowed = ALLOWED_VENDOR_FLOWS.get(commercial_flow_level)
    if allowed is None:
        # level=0 または未定義レベル → 制限なし
        if commercial_flow_level == 0:
            return True
        return False

    return vendor_flow in allowed


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    logger.info("処理開始")

    try:
        pairs = read_jsonl_as_list(str(INPUT_PAIRS))
        project_vendor_map = read_jsonl_as_dict(str(INPUT_PROJECT_VENDOR), key="message_id")
        resource_vendor_map = read_jsonl_as_dict(str(INPUT_RESOURCE_VENDOR), key="message_id")

        logger.info(f"入力ペア数: {len(pairs)}")
        logger.info(f"案件商流レコード数: {len(project_vendor_map)}")
        logger.info(f"要員商流レコード数: {len(resource_vendor_map)}")

        matched = []
        no_matched = []

        for pair in pairs:
            project_message_id = pair["project_info"]["message_id"]
            resource_message_id = pair["resource_info"]["message_id"]

            project_rec = project_vendor_map.get(project_message_id, {})
            resource_rec = resource_vendor_map.get(resource_message_id, {})

            commercial_flow_level = project_rec.get("commercial_flow_level")
            vendor_flow = resource_rec.get("vendor_flow")

            is_match = judge_vendor_tiers_match(commercial_flow_level, vendor_flow)

            record = merge_match_info(pair, {"match_vendor_tiers": is_match})

            if is_match:
                matched.append(record)
            else:
                no_matched.append(record)
                logger.info(
                    f"NO_MATCH: project={project_message_id} resource={resource_message_id}"
                    f" commercial_flow_level={commercial_flow_level} vendor_flow={vendor_flow}"
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

"""
06-10_match_location
案件と要員のlocation（勤務地）を比較してマッチ判定する。
LLM使用禁止。

判定ロジック：
  案件のremote_typeがfullremote → 要員のlocationに関係なくtrue
  それ以外                      → 案件locationと要員locationが完全一致ならtrue
  locationが不明/null           → true（デフォルト通過）
"""

import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import merge_match_info, read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "06-10_match_location"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS           = project_root / "06-9_match_phase_category/01_result/matched_pairs_phase_category.jsonl"
INPUT_PROJECT_REMOTE  = project_root / "03-3_extract_project_remote/01_result/extract_project_remote.jsonl"
INPUT_PROJECT_LOC     = project_root / "03-10_extract_project_location/01_result/extract_project_location.jsonl"
INPUT_RESOURCE_LOC    = project_root / "05-10_extract_resource_location/01_result/extract_resource_location.jsonl"

OUTPUT_MATCHED    = STEP_DIR / "01_result/matched_pairs_location.jsonl"
OUTPUT_NO_MATCHED = STEP_DIR / "01_result/99_not_matched_pairs_location.jsonl"


def judge_location_match(remote_type: str, project_location: str, resource_location: str) -> bool:
    """
    locationマッチ判定。
    案件がfullremoteの場合はlocationに関係なくtrue。
    案件・要員のいずれかのlocationがnull/空の場合はtrue（デフォルト通過）。
    それ以外は完全一致でtrue。
    """
    if remote_type == "fullremote":
        return True
    if not project_location or not resource_location:
        return True
    return project_location == resource_location


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    logger.info("処理開始")

    try:
        pairs = read_jsonl_as_list(str(INPUT_PAIRS))
        logger.info(f"入力ペア数: {len(pairs)}")

        project_remote_map = read_jsonl_as_dict(str(INPUT_PROJECT_REMOTE), key="message_id")
        project_loc_map    = read_jsonl_as_dict(str(INPUT_PROJECT_LOC),    key="message_id")
        resource_loc_map   = read_jsonl_as_dict(str(INPUT_RESOURCE_LOC),   key="message_id")

        logger.info(f"案件remoteレコード数: {len(project_remote_map)}")
        logger.info(f"案件locationレコード数: {len(project_loc_map)}")
        logger.info(f"要員locationレコード数: {len(resource_loc_map)}")

        matched = []
        no_matched = []

        for pair in pairs:
            project_message_id  = pair["project_info"]["message_id"]
            resource_message_id = pair["resource_info"]["message_id"]

            project_remote_rec = project_remote_map.get(project_message_id, {})
            project_loc_rec    = project_loc_map.get(project_message_id, {})
            resource_loc_rec   = resource_loc_map.get(resource_message_id, {})

            remote_type       = project_remote_rec.get("remote_type")
            project_location  = project_loc_rec.get("location")
            resource_location = resource_loc_rec.get("location")

            is_match = judge_location_match(remote_type, project_location, resource_location)

            record = merge_match_info(pair, {"match_location": is_match})

            if is_match:
                matched.append(record)
            else:
                no_matched.append(record)
                logger.info(
                    f"NO_MATCH: project={project_message_id} resource={resource_message_id}"
                    f" remote_type={remote_type} project_location={project_location}"
                    f" resource_location={resource_location}"
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

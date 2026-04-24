"""
06-3_match_remote
案件のリモート条件と要員のリモート希望を比較してマッチ判定する。
LLM使用禁止。

判定ロジック：
  案件fullremote  → 要員はすべてtrue
  案件hybrid      → 要員がhybridまたはonsiteはtrue、fullremoteはfalse
  案件onsite      → 要員がonsiteのみtrue
  案件がnull/不明 → true（デフォルト通過）
  要員がnull/不明 → true（デフォルト通過）
"""

import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import merge_match_info, read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "06-3_match_remote"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS = project_root / "06-2_match_age/01_result/matched_pairs_age.jsonl"
INPUT_PROJECT_REMOTE = project_root / "03-3_extract_project_remote/01_result/extract_project_remote.jsonl"
INPUT_RESOURCE_REMOTE = project_root / "05-3_extract_resource_remote/01_result/extract_resource_remote.jsonl"

OUTPUT_MATCHED = STEP_DIR / "01_result/matched_pairs_remote.jsonl"
OUTPUT_NO_MATCHED = STEP_DIR / "01_result/99_no_matched_pairs_remote.jsonl"

# マッチ表（案件remote_type → マッチする要員remote_preferenceのセット）
MATCH_TABLE = {
    "fullremote": {"fullremote", "hybrid", "onsite"},
    "hybrid":     {"hybrid", "onsite"},
    "onsite":     {"onsite"},
}


def judge_remote_match(remote_type, remote_preference) -> bool:
    """
    リモートマッチ判定。
    案件または要員がnull/不明の場合はtrue（デフォルト通過）。
    """
    if remote_type is None or remote_preference is None:
        return True
    allowed = MATCH_TABLE.get(remote_type)
    if allowed is None:
        # 案件が未知の値の場合は通過
        return True
    return remote_preference in allowed


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    logger.info("処理開始")

    try:
        pairs = read_jsonl_as_list(str(INPUT_PAIRS))
        project_remote_map = read_jsonl_as_dict(str(INPUT_PROJECT_REMOTE), key="message_id")
        resource_remote_map = read_jsonl_as_dict(str(INPUT_RESOURCE_REMOTE), key="message_id")

        logger.info(f"入力ペア数: {len(pairs)}")
        logger.info(f"案件リモートレコード数: {len(project_remote_map)}")
        logger.info(f"要員リモートレコード数: {len(resource_remote_map)}")

        matched = []
        no_matched = []

        for pair in pairs:
            project_message_id = pair["project_info"]["message_id"]
            resource_message_id = pair["resource_info"]["message_id"]

            project_rec = project_remote_map.get(project_message_id, {})
            resource_rec = resource_remote_map.get(resource_message_id, {})

            remote_type = project_rec.get("remote_type")
            remote_preference = resource_rec.get("remote_preference")

            is_match = judge_remote_match(remote_type, remote_preference)

            record = merge_match_info(pair, {"match_remote": is_match})

            if is_match:
                matched.append(record)
            else:
                no_matched.append(record)
                logger.info(
                    f"NO_MATCH: project={project_message_id} resource={resource_message_id}"
                    f" remote_type={remote_type} remote_preference={remote_preference}"
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

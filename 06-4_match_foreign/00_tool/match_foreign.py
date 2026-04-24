"""
06-4_match_foreign
案件の外国籍制限と要員の国籍を比較してマッチ判定する。
LLM使用禁止。

判定ロジック：
  案件foreign_nationality_ok=true  → 要員はすべてtrue
  案件foreign_nationality_ok=false → 要員nationality=japaneseのみtrue
  案件がnull/不明                  → true（デフォルト通過）
  要員がnull/不明                  → true（デフォルト通過）
"""

import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import merge_match_info, read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "06-4_match_foreign"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS = project_root / "06-3_match_remote/01_result/matched_pairs_remote.jsonl"
INPUT_PROJECT_FOREIGN = project_root / "03-4_extract_project_foreign/01_result/extract_project_foreign.jsonl"
INPUT_RESOURCE_FOREIGN = project_root / "05-4_extract_resource_foreign/01_result/extract_resource_foreign.jsonl"

OUTPUT_MATCHED = STEP_DIR / "01_result/matched_pairs_foreign.jsonl"
OUTPUT_NO_MATCHED = STEP_DIR / "01_result/99_no_matched_pairs_foreign.jsonl"


def judge_foreign_match(foreign_nationality_ok, nationality) -> bool:
    """
    外国籍マッチ判定。
    案件または要員がnullの場合はtrue（デフォルト通過）。
    """
    if foreign_nationality_ok is None or nationality is None:
        return True
    if foreign_nationality_ok:
        return True
    return nationality == "japanese"


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    logger.info("処理開始")

    try:
        pairs = read_jsonl_as_list(str(INPUT_PAIRS))
        project_foreign_map = read_jsonl_as_dict(str(INPUT_PROJECT_FOREIGN), key="message_id")
        resource_foreign_map = read_jsonl_as_dict(str(INPUT_RESOURCE_FOREIGN), key="message_id")

        logger.info(f"入力ペア数: {len(pairs)}")
        logger.info(f"案件外国籍制限レコード数: {len(project_foreign_map)}")
        logger.info(f"要員国籍レコード数: {len(resource_foreign_map)}")

        matched = []
        no_matched = []

        for pair in pairs:
            project_message_id = pair["project_info"]["message_id"]
            resource_message_id = pair["resource_info"]["message_id"]

            project_rec = project_foreign_map.get(project_message_id, {})
            resource_rec = resource_foreign_map.get(resource_message_id, {})

            foreign_nationality_ok = project_rec.get("foreign_nationality_ok")
            nationality = resource_rec.get("nationality")

            is_match = judge_foreign_match(foreign_nationality_ok, nationality)

            record = merge_match_info(pair, {"match_foreign": is_match})

            if is_match:
                matched.append(record)
            else:
                no_matched.append(record)
                logger.info(
                    f"NO_MATCH: project={project_message_id} resource={resource_message_id}"
                    f" foreign_ok={foreign_nationality_ok} nationality={nationality}"
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

"""
06-9_match_phase_category
案件と要員の工程（phases）を比較してマッチ判定する。
LLM使用禁止。

feature flag (config.py) でOFF時はpass-throughのみ行う。

判定ロジック：
  案件・要員の両方にphasesが1つ以上ある場合：
    1つでも一致する値があれば true、1つも一致しなければ false
  どちらかが空/nullの場合：
    true（工程不明は通過）
"""

import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import merge_match_info, read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

from config import ENABLE_PHASE_CATEGORY_MATCH

STEP_NAME = "06-9_match_phase_category"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS = project_root / "06-8_match_skill_category/01_result/matched_pairs_skill_category.jsonl"
INPUT_PROJECT_PHASE = project_root / "03-9_extract_project_phase_category/01_result/extract_project_phase_category.jsonl"
INPUT_RESOURCE_PHASE = project_root / "05-9_extract_resource_phase_category/01_result/extract_resource_phase_category.jsonl"

OUTPUT_MATCHED    = STEP_DIR / "01_result/matched_pairs_phase_category.jsonl"
OUTPUT_NO_MATCHED = STEP_DIR / "01_result/99_no_matched_pairs_phase_category.jsonl"

STRONG_PHASES = {
    "要件定義",
    "基本設計",
    "詳細設計",
    "設計・構築",
    "運用保守",
    "移行",
    "PMO",
    "リリース",
    "要件調査",
}


def judge_phase_category_match(project_phases: list, resource_phases: list) -> bool:
    """
    工程カテゴリマッチ判定。
    どちらかが空/nullの場合は true（工程不明は通過）。
    両方に値がある場合は、strong phase のみを比較対象とする。
    strong phase がどちらかに1つも無ければ false。
    両方にstrong phaseがある場合は、1つ以上共通工程があれば true。
    """
    if not project_phases or not resource_phases:
        return True

    project_strong_phases = [phase for phase in project_phases if phase in STRONG_PHASES]
    resource_strong_phases = [phase for phase in resource_phases if phase in STRONG_PHASES]

    if not project_strong_phases or not resource_strong_phases:
        return False

    return len(set(project_strong_phases) & set(resource_strong_phases)) > 0


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    logger.info("処理開始")
    logger.info(f"ENABLE_PHASE_CATEGORY_MATCH: {ENABLE_PHASE_CATEGORY_MATCH}")

    try:
        pairs = read_jsonl_as_list(str(INPUT_PAIRS))
        logger.info(f"入力ペア数: {len(pairs)}")

        if not ENABLE_PHASE_CATEGORY_MATCH:
            # pass-through: 入力をそのまま出力（match_phase_categoryキーは追加しない）
            logger.info("feature flagがOFFのためpass-through出力")
            write_jsonl(str(OUTPUT_MATCHED), pairs)
            write_jsonl(str(OUTPUT_NO_MATCHED), [])

            elapsed = time.time() - start_time
            write_execution_time(
                str(dirs["execution_time"]),
                STEP_NAME,
                elapsed,
                record_count=len(pairs),
            )
            logger.info(f"処理完了（pass-through） 件数={len(pairs)}")
            return

        # feature flag ON: 工程カテゴリマッチ判定
        project_phase_map = read_jsonl_as_dict(str(INPUT_PROJECT_PHASE), key="message_id")
        resource_phase_map = read_jsonl_as_dict(str(INPUT_RESOURCE_PHASE), key="message_id")

        logger.info(f"案件工程レコード数: {len(project_phase_map)}")
        logger.info(f"要員工程レコード数: {len(resource_phase_map)}")

        matched = []
        no_matched = []

        for pair in pairs:
            project_message_id = pair["project_info"]["message_id"]
            resource_message_id = pair["resource_info"]["message_id"]

            project_rec = project_phase_map.get(project_message_id, {})
            resource_rec = resource_phase_map.get(resource_message_id, {})

            project_phases = project_rec.get("phases") or []
            resource_phases = resource_rec.get("phases") or []

            is_match = judge_phase_category_match(project_phases, resource_phases)

            record = merge_match_info(pair, {"match_phase_category": is_match})

            if is_match:
                matched.append(record)
            else:
                no_matched.append(record)
                logger.info(
                    f"NO_MATCH: project={project_message_id} resource={resource_message_id}"
                    f" project_phases={project_phases} resource_phases={resource_phases}"
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

"""
06-8_match_skill_category
案件と要員のスキルカテゴリを比較してマッチ判定する。
LLM使用禁止。

feature flag (config.py) でOFF時はpass-throughのみ行う。

判定ロジック：
  案件と要員のskillsが1つ以上一致すればtrue
  案件と要員のどちらかのskillsが空（抽出なし）はfalse
"""

import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import merge_match_info, read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

from config import ENABLE_SKILL_CATEGORY_MATCH, SKILL_MATCH_EXCLUDED

STEP_NAME = "06-8_match_skill_category"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS = project_root / "06-7_match_vendor_tiers/01_result/matched_pairs_vendor_tiers.jsonl"
INPUT_PROJECT_SKILL = project_root / "03-8_extract_project_skill_category/01_result/extract_project_skill_category.jsonl"
INPUT_RESOURCE_SKILL = project_root / "05-8_extract_resource_skill_category/01_result/extract_resource_skill_category.jsonl"

OUTPUT_MATCHED = STEP_DIR / "01_result/matched_pairs_skill_category.jsonl"
OUTPUT_NO_MATCHED = STEP_DIR / "01_result/99_no_matched_pairs_skill_category.jsonl"


def _unique_preserve_order(skills: list) -> list:
    seen = set()
    result = []
    for skill in skills or []:
        if skill in seen:
            continue
        seen.add(skill)
        result.append(skill)
    return result


def _filter_excluded_skills(skills: list) -> list:
    excluded = set(SKILL_MATCH_EXCLUDED)
    return [skill for skill in _unique_preserve_order(skills) if skill not in excluded]


def judge_skill_category_match(
    project_skills: list,
    resource_skills: list,
    primary_skills: list = None,
) -> tuple:
    """
    スキルカテゴリマッチ判定。

    primary_skills が指定されている場合:
      - primary_skills（必須スキル由来）と要員スキルの一致を必須にする
      - optional由来スキルだけの一致では通さない
    primary_skills が空 or None の場合:
      - 従来どおり全スキルで判定（フォールバック）

    元のskillsがどちらか空リストの場合はfalse。
    除外後のskillsがどちらか空リストの場合はfalse。
    """
    if not project_skills or not resource_skills:
        return False, [], [], [], []

    filtered_project_skills = _filter_excluded_skills(project_skills)
    filtered_resource_skills = _filter_excluded_skills(resource_skills)

    if not filtered_project_skills or not filtered_resource_skills:
        return False, [], filtered_project_skills, filtered_resource_skills, []

    resource_set = set(filtered_resource_skills)

    # primary_skills がある場合、primary での一致を必須にする
    filtered_primary = _filter_excluded_skills(primary_skills) if primary_skills else []
    if filtered_primary:
        matched_skills = [s for s in filtered_primary if s in resource_set]
        return (
            len(matched_skills) > 0,
            matched_skills,
            filtered_project_skills,
            filtered_resource_skills,
            filtered_primary,
        )

    # フォールバック: 従来どおり全スキルで判定
    matched_skills = [skill for skill in filtered_project_skills if skill in resource_set]
    return (
        len(matched_skills) > 0,
        matched_skills,
        filtered_project_skills,
        filtered_resource_skills,
        filtered_primary,
    )


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    logger.info("処理開始")
    logger.info(f"ENABLE_SKILL_CATEGORY_MATCH: {ENABLE_SKILL_CATEGORY_MATCH}")

    try:
        pairs = read_jsonl_as_list(str(INPUT_PAIRS))
        logger.info(f"入力ペア数: {len(pairs)}")

        if not ENABLE_SKILL_CATEGORY_MATCH:
            # pass-through: 入力をそのまま出力（match_skill_categoryキーは追加しない）
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

        # feature flag ON: スキルカテゴリマッチ判定
        project_skill_map = read_jsonl_as_dict(str(INPUT_PROJECT_SKILL), key="message_id")
        resource_skill_map = read_jsonl_as_dict(str(INPUT_RESOURCE_SKILL), key="message_id")

        logger.info(f"案件スキルレコード数: {len(project_skill_map)}")
        logger.info(f"要員スキルレコード数: {len(resource_skill_map)}")

        matched = []
        no_matched = []

        for pair in pairs:
            project_message_id = pair["project_info"]["message_id"]
            resource_message_id = pair["resource_info"]["message_id"]

            project_rec = project_skill_map.get(project_message_id, {})
            resource_rec = resource_skill_map.get(resource_message_id, {})

            project_skills = project_rec.get("skills", [])
            resource_skills = resource_rec.get("skills", [])
            primary_skills = project_rec.get("primary_skills", [])

            (
                is_match,
                matched_skills,
                filtered_project_skills,
                filtered_resource_skills,
                filtered_primary,
            ) = judge_skill_category_match(project_skills, resource_skills, primary_skills)

            record = merge_match_info(
                pair,
                {
                    "match_skill_category": is_match,
                    "match_detail": {
                        "matched_skills": matched_skills,
                        "project_skills_used": filtered_project_skills,
                        "resource_skills_used": filtered_resource_skills,
                        "excluded_skills": SKILL_MATCH_EXCLUDED,
                        "primary_skills_used": filtered_primary,
                    },
                },
            )

            if is_match:
                matched.append(record)
            else:
                no_matched.append(record)
                logger.info(
                    f"NO_MATCH: project={project_message_id} resource={resource_message_id}"
                    f" project_skills={project_skills} resource_skills={resource_skills}"
                    f" filtered_project_skills={filtered_project_skills}"
                    f" filtered_resource_skills={filtered_resource_skills}"
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

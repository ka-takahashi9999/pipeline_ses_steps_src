"""
08-2_match_score_aggregation
必須スキル一致率・尚可スキル一致率・合計スコアを算出する。
"""

import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "08-2_match_score_aggregation"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_FILE = (
    project_root
    / "08-1_restore_and_merge_requirement_skill_ai_matching/01_result/merged_requirement_skill_ai_matching.jsonl"
)
OUTPUT_FILE = STEP_DIR / "01_result/match_score_aggregation.jsonl"


def calc_match_rate(skills: list) -> float:
    """スキルリストのtrue一致率を返す。スキルが0件の場合は0.0。"""
    if not skills:
        return 0.0
    true_count = sum(1 for s in skills if s.get("match") is True)
    return true_count / len(skills)


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    try:
        records = read_jsonl_as_list(str(INPUT_FILE))
        logger.info(f"入力件数={len(records)}")

        results = []
        for rec in records:
            required_skills = rec.get("project_info", {}).get("required_skills", [])
            optional_skills = rec.get("project_info", {}).get("optional_skills", [])

            required_rate = calc_match_rate(required_skills)
            optional_rate = calc_match_rate(optional_skills)
            total_rate = required_rate + optional_rate

            results.append(
                {
                    "project_info": rec["project_info"],
                    "resource_info": rec["resource_info"],
                    "duplicate_proposal_check": rec.get("duplicate_proposal_check"),
                    "match_info": {
                        "required_skills_match_rate": required_rate,
                        "optional_skills_match_rate": optional_rate,
                        "total_skills_match_rate": total_rate,
                    },
                }
            )

        write_jsonl(str(OUTPUT_FILE), results)

        elapsed = time.time() - start_time
        write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, len(results))
        logger.ok(f"処理完了: {len(results)}件")

    except Exception as e:
        write_error_log(str(dirs["result"]), e, context=STEP_NAME)
        logger.error(f"処理失敗: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

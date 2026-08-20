"""09-5 proposal_ready / human_review pair queueの整合性を確認する。"""

import argparse
import math
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

project_root = Path(__file__).resolve().parents[2]
tool_dir = Path(__file__).resolve().parents[1] / "00_tool"
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(tool_dir))

from common.json_utils import read_jsonl_as_list
from common.logger import get_logger
from classify_sales_candidate_queues import (
    RECHECK_ALL_PATH,
    RECHECK_ERROR_PATH,
    classify_candidate_pairs,
    pair_key,
)

STEP_NAME = "09-5_sales_candidate_queues_confirm"
STEP_DIR = Path(__file__).resolve().parents[1]
INPUT_09_4_DIR = project_root / "09-4_remove_category_mismatch_sales_candidates/01_result"
CONFIRM_RESULT = Path(__file__).resolve().parent / "confirm_result_sales_candidate_queues.txt"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="09-5 sales candidate queue confirm")
    parser.add_argument("--target-date", help="対象日 YYYYMMDD。省略時は09-4最新")
    return parser.parse_args()


def resolve_date(target_date: str) -> str:
    if target_date:
        return target_date
    paths = sorted(INPUT_09_4_DIR.glob("sales_proposal_candidates_*.jsonl"))
    if not paths:
        raise FileNotFoundError(f"09-4候補が存在しません: {INPUT_09_4_DIR}")
    return paths[-1].stem.replace("sales_proposal_candidates_", "", 1)


def unique_keys(records: List[Dict], label: str, errors: List[str]) -> set:
    keys = [pair_key(record) for record in records]
    duplicate_count = len(keys) - len(set(keys))
    if duplicate_count:
        errors.append(f"[NG] {label} pair重複: {duplicate_count}")
    return set(keys)


def p90(values: List[int]) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * 0.9) - 1]


def append_check(lines: List[str], errors: List[str], condition: bool, ok: str, ng: str) -> None:
    if condition:
        lines.append(f"[OK] {ok}")
    else:
        message = f"[NG] {ng}"
        lines.append(message)
        errors.append(message)


def main() -> None:
    logger = get_logger(STEP_NAME)
    errors: List[str] = []
    lines = ["=== 09-5 sales candidate queue confirm結果 ===", ""]
    try:
        date_part = resolve_date(parse_args().target_date)
        candidate_path = INPUT_09_4_DIR / f"sales_proposal_candidates_{date_part}.jsonl"
        draft_path = STEP_DIR / "01_result" / f"generate_sales_reply_draft_{date_part}.jsonl"
        proposal_path = STEP_DIR / "01_result" / f"proposal_ready_{date_part}.jsonl"
        human_path = STEP_DIR / "01_result" / f"human_review_{date_part}.jsonl"
        for path in (candidate_path, draft_path, proposal_path, human_path, RECHECK_ALL_PATH):
            if not path.exists():
                raise FileNotFoundError(f"confirm入力が存在しません: {path}")

        candidates = read_jsonl_as_list(str(candidate_path))
        drafts = read_jsonl_as_list(str(draft_path))
        rechecks = read_jsonl_as_list(str(RECHECK_ALL_PATH))
        error_records = read_jsonl_as_list(str(RECHECK_ERROR_PATH)) if RECHECK_ERROR_PATH.exists() else []
        proposal = read_jsonl_as_list(str(proposal_path))
        human = read_jsonl_as_list(str(human_path))
        expected_proposal, expected_human = classify_candidate_pairs(
            candidates, drafts, rechecks, error_records
        )

        candidate_keys = unique_keys(candidates, "final candidate", errors)
        proposal_keys = unique_keys(proposal, "proposal_ready", errors)
        human_keys = unique_keys(human, "human_review", errors)
        expected_proposal_keys = {pair_key(record) for record in expected_proposal}
        expected_human_keys = {pair_key(record) for record in expected_human}

        lines.extend(
            [
                f"対象日: {date_part}",
                f"final pair count: {len(candidate_keys)}",
                f"matching_strict count: {sum(bool(row.get('matching_strict')) for row in proposal + human)}",
                f"sales_ready count: {sum(bool(row.get('sales_ready')) for row in proposal + human)}",
                f"proposal_ready count: {len(proposal)}",
                f"human_review count: {len(human)}",
            ]
        )
        append_check(
            lines,
            errors,
            proposal_keys | human_keys == candidate_keys,
            "queue unionがfinal candidateと一致",
            "queue unionがfinal candidateと不一致",
        )
        append_check(
            lines,
            errors,
            not (proposal_keys & human_keys),
            "proposal_ready / human_review intersection=0",
            f"queue intersectionあり: {len(proposal_keys & human_keys)}",
        )
        append_check(
            lines,
            errors,
            proposal_keys == expected_proposal_keys and human_keys == expected_human_keys,
            "queue分類がcanonical入力からの再計算結果と一致",
            "queue分類がcanonical入力からの再計算結果と不一致",
        )

        draft_groups = defaultdict(list)
        for draft in drafts:
            draft_groups[pair_key(draft)].append(draft)
        proposal_missing_draft = 0
        proposal_category_mismatch = 0
        proposal_08_5_error = 0
        proposal_needs_review = 0
        proposal_not_strict = 0
        for record in proposal:
            pair_drafts = draft_groups[pair_key(record)]
            directions = {draft.get("draft_direction") for draft in pair_drafts}
            if len(pair_drafts) != 2 or directions != {"reply_to_project", "reply_to_resource"}:
                proposal_missing_draft += 1
            if record.get("category_match") != "match":
                proposal_category_mismatch += 1
            if record.get("has_08_5_error") is not False:
                proposal_08_5_error += 1
            if any(
                draft.get("needs_human_review") is not False or bool(draft.get("review_notes"))
                for draft in pair_drafts
            ):
                proposal_needs_review += 1
            if record.get("matching_strict") is not True or record.get("sales_ready") is not True:
                proposal_not_strict += 1

        append_check(lines, errors, proposal_missing_draft == 0, "proposal_ready draft欠損=0", f"proposal_ready draft欠損={proposal_missing_draft}")
        append_check(lines, errors, proposal_category_mismatch == 0, "proposal_ready category mismatch=0", f"proposal_ready category mismatch={proposal_category_mismatch}")
        append_check(lines, errors, proposal_08_5_error == 0, "proposal_ready 08-5 error=0", f"proposal_ready 08-5 error={proposal_08_5_error}")
        append_check(lines, errors, proposal_needs_review == 0, "proposal_ready needs_human_review/review_notes=0", f"proposal_ready sales review残存={proposal_needs_review}")
        append_check(lines, errors, proposal_not_strict == 0, "proposal_readyはmatching_strictかつsales_ready", f"proposal_ready contract違反={proposal_not_strict}")

        project_counts = Counter(record["project_message_id"] for record in proposal)
        resource_count = len({record["resource_message_id"] for record in proposal})
        counts = list(project_counts.values())
        mean_value = len(proposal) / len(project_counts) if project_counts else 0.0
        median_value = statistics.median(counts) if counts else 0
        p90_value = p90(counts)
        max_value = max(counts) if counts else 0
        top_project_count = max_value
        lines.extend(
            [
                "",
                "【proposal_ready 分布】",
                f"distinct projects: {len(project_counts)}",
                f"distinct resources: {resource_count}",
                f"1案件平均: {mean_value:.2f}",
                f"median: {median_value}",
                f"p90: {p90_value}",
                f"max: {max_value}",
                f"top project count: {top_project_count}",
            ]
        )
    except Exception as error:
        message = f"[NG] confirm実行失敗: {error}"
        lines.append(message)
        errors.append(message)

    lines.extend(["", "【結果】NG" if errors else "【結果】OK"])
    CONFIRM_RESULT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    for line in lines:
        if line.startswith("[NG]") or line == "【結果】NG":
            logger.error(line)
        else:
            logger.info(line)
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()

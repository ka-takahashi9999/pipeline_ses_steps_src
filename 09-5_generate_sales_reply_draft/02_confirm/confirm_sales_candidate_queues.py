"""09-5 proposal_ready / human_review pair queueの整合性を確認する。"""

import argparse
import copy
import hashlib
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
from common.previous_candidate import (
    PREVIOUS_CANDIDATE_DATE_FIELD,
    PREVIOUS_CANDIDATE_FIELD,
    load_and_mark_candidate_records,
)
from classify_sales_candidate_queues import (
    RECHECK_ALL_PATH,
    RECHECK_ERROR_PATH,
    REVIEW_PRIORITY_HIGH,
    REVIEW_PRIORITY_OTHER,
    classify_candidate_pairs,
    has_explicit_review_reason,
    pair_key,
)

STEP_NAME = "09-5_sales_candidate_queues_confirm"
STEP_DIR = Path(__file__).resolve().parents[1]
INPUT_09_4_DIR = project_root / "09-4_remove_category_mismatch_sales_candidates/01_result"
CONFIRM_RESULT = Path(__file__).resolve().parent / "confirm_result_sales_candidate_queues.txt"

KNOWN_A_20260819 = {
    ("1a018372413af805", "1a0176abe9ddce07"),
    ("1a017f3c935121e7", "1a018f8f74c598aa"),
    ("1a018372413af805", "1a0183a59cb1846c"),
    ("1a018372413af805", "1a017a45859f2849"),
    ("1a01797304075e5c", "1a017f715f63da32"),
    ("1a01797304075e5c", "1a017ee4ea593fb2"),
    ("1a01778dfb8be8c6", "1a0182dd105dd3c0"),
    ("1a017693e4de5be7", "1a017e258bc19fb1"),
    ("1a0175888133f07e", "1a018805ae757739"),
    ("1a0180ef558c03fd", "1a01905def58fd94"),
    ("1a017807212f890c", "1a017c2ec8f6f86b"),
    ("1a0177310bfaa66f", "1a0191a78a05fa3e"),
    ("1a0177310bfaa66f", "1a017a3a0a021081"),
    ("1a0175888133f07e", "1a0190e44cb97c2f"),
}
KNOWN_C_20260819 = {
    ("1a017672547b0d2a", "1a0183b199bebcab"),
    ("1a017672547b0d2a", "1a0177891a1d64b5"),
    ("1a018a74ac9122c2", "1a0185f56dde80b2"),
    ("1a0175e73a48db7e", "1a0189950f6977a4"),
    ("1a017f765b3fe445", "1a017a7958168683"),
    ("1a0175e73a48db7e", "1a0182e4f435a28f"),
    ("1a0175e73a48db7e", "1a017630d28e16ab"),
    ("1a017735c71cec5f", "1a017b82b6f66fa9"),
    ("1a0180ef558c03fd", "1a017b82b6f66fa9"),
    ("1a017f765b3fe445", "1a0189881c4505dc"),
    ("1a017d4188b0c166", "1a017a054c59d995"),
    ("1a017807212f890c", "1a0183f8ccf488c0"),
    ("1a0175e73a48db7e", "1a01908f91c4ac5c"),
    ("1a017ee002ba9649", "1a018cf81cf6521c"),
}
KNOWN_EVIDENCE_REVIEW_20260819 = {
    ("1a017672547b0d2a", "1a0183b199bebcab"),
    ("1a017672547b0d2a", "1a0177891a1d64b5"),
    ("1a018a74ac9122c2", "1a0185f56dde80b2"),
    ("1a0175e73a48db7e", "1a0189950f6977a4"),
    ("1a0175e73a48db7e", "1a0182e4f435a28f"),
    ("1a0175e73a48db7e", "1a017630d28e16ab"),
    ("1a017d4188b0c166", "1a017a054c59d995"),
    ("1a0175e73a48db7e", "1a01908f91c4ac5c"),
    ("1a017ee002ba9649", "1a018cf81cf6521c"),
}
KNOWN_MULTIPLE_PROFILE_20260819 = {
    ("1a017735c71cec5f", "1a017b82b6f66fa9"),
    ("1a0180ef558c03fd", "1a017b82b6f66fa9"),
}
KNOWN_PARTIAL_EVIDENCE_20260819 = {
    ("1a017f765b3fe445", "1a017a7958168683"),
    ("1a017f765b3fe445", "1a0189881c4505dc"),
    ("1a017807212f890c", "1a0183f8ccf488c0"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="09-5 sales candidate queue confirm")
    parser.add_argument("--target-date", help="対象日 YYYYMMDD。省略時は09-4最新")
    parser.add_argument(
        "--simulate-read-only",
        action="store_true",
        help="既存queueを上書きせずcanonical入力から分類simulationする",
    )
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
    args = parse_args()
    errors: List[str] = []
    lines = ["=== 09-5 sales candidate queue confirm結果 ===", ""]
    try:
        date_part = resolve_date(args.target_date)
        candidate_path = INPUT_09_4_DIR / f"sales_proposal_candidates_{date_part}.jsonl"
        draft_path = STEP_DIR / "01_result" / f"generate_sales_reply_draft_{date_part}.jsonl"
        proposal_path = STEP_DIR / "01_result" / f"proposal_ready_{date_part}.jsonl"
        human_path = STEP_DIR / "01_result" / f"human_review_{date_part}.jsonl"
        required_paths = [candidate_path, draft_path, RECHECK_ALL_PATH]
        if not args.simulate_read_only:
            required_paths.extend([proposal_path, human_path])
        for path in required_paths:
            if not path.exists():
                raise FileNotFoundError(f"confirm入力が存在しません: {path}")

        canonical_bytes_before = draft_path.read_bytes()
        canonical_hash_before = hashlib.sha256(canonical_bytes_before).hexdigest()
        candidates = read_jsonl_as_list(str(candidate_path))
        candidates, previous_candidate_date = load_and_mark_candidate_records(
            candidates, INPUT_09_4_DIR, date_part
        )
        drafts = read_jsonl_as_list(str(draft_path))
        drafts_before = copy.deepcopy(drafts)
        rechecks = read_jsonl_as_list(str(RECHECK_ALL_PATH))
        recheck_match_info = {
            (
                str((record.get("project_info") or {}).get("message_id") or "").strip(),
                str((record.get("resource_info") or {}).get("message_id") or "").strip(),
            ): record.get("match_info") or {}
            for record in rechecks
        }
        error_records = read_jsonl_as_list(str(RECHECK_ERROR_PATH)) if RECHECK_ERROR_PATH.exists() else []
        expected_proposal, expected_human = classify_candidate_pairs(
            candidates, drafts, rechecks, error_records
        )
        if args.simulate_read_only:
            proposal = expected_proposal
            human = expected_human
        else:
            proposal = read_jsonl_as_list(str(proposal_path))
            human = read_jsonl_as_list(str(human_path))

        candidate_keys = unique_keys(candidates, "final candidate", errors)
        proposal_keys = unique_keys(proposal, "proposal_ready", errors)
        human_keys = unique_keys(human, "human_review", errors)
        expected_proposal_keys = {pair_key(record) for record in expected_proposal}
        expected_human_keys = {pair_key(record) for record in expected_human}
        expected_queue_index = {
            pair_key(record): record for record in expected_proposal + expected_human
        }
        actual_queues = proposal + human
        expected_human_index = {pair_key(record): record for record in expected_human}
        previous_keys = {
            pair_key(record)
            for record in actual_queues
            if record.get(PREVIOUS_CANDIDATE_FIELD) is True
        }
        expected_previous_keys = {
            key
            for key, record in expected_queue_index.items()
            if record.get(PREVIOUS_CANDIDATE_FIELD) is True
        }
        invalid_previous_fields = sum(
            not isinstance(record.get(PREVIOUS_CANDIDATE_FIELD), bool)
            or record.get(PREVIOUS_CANDIDATE_DATE_FIELD) != previous_candidate_date
            for record in actual_queues
        )
        cache_marker_keys = {
            pair_key(record)
            for record in candidates
            if "_前回出力済" in str(record.get("pair_file_name") or "")
        }
        both_keys = previous_keys & cache_marker_keys
        cache_only_keys = cache_marker_keys - previous_keys
        previous_only_keys = previous_keys - cache_marker_keys
        neither_keys = candidate_keys - (previous_keys | cache_marker_keys)
        proposal_previous_count = len(previous_keys & proposal_keys)
        human_previous_count = len(previous_keys & human_keys)
        candidate_duplicate_count = len(candidates) - len(candidate_keys)
        proposal_duplicate_count = len(proposal) - len(proposal_keys)
        human_duplicate_count = len(human) - len(human_keys)
        overlap_count = len(proposal_keys & human_keys)
        candidate_loss_count = len(candidate_keys - (proposal_keys | human_keys))
        high_records = [
            record for record in human if record.get("review_priority") == REVIEW_PRIORITY_HIGH
        ]
        other_records = [
            record for record in human if record.get("review_priority") == REVIEW_PRIORITY_OTHER
        ]
        initial_records = [record for record in human if record.get("initial_review") is True]
        priority_fields = (
            "review_priority",
            "normalized_review_items",
            "normalized_review_item_count",
            "high_project_rank",
            "initial_review",
        )
        priority_fields_match = all(
            key in expected_human_index
            and all(record.get(field) == expected_human_index[key].get(field) for field in priority_fields)
            for record in human
            for key in [pair_key(record)]
        )

        lines.extend(
            [
                f"対象日: {date_part}",
                f"final pair count: {len(candidate_keys)}",
                f"matching_strict count: {sum(bool(row.get('matching_strict')) for row in proposal + human)}",
                f"sales_ready count: {sum(bool(row.get('sales_ready')) for row in proposal + human)}",
                f"evidence_ready count: {sum(bool(row.get('evidence_ready')) for row in proposal + human)}",
                f"proposal_ready count: {len(proposal)}",
                f"human_review count: {len(human)}",
                f"HIGH count: {len(high_records)}",
                f"OTHER count: {len(other_records)}",
                f"initial review count: {len(initial_records)}",
                f"initial sales target count: {len(proposal) + len(initial_records)}",
                f"candidate loss count: {candidate_loss_count}",
                f"duplicate count: {candidate_duplicate_count + proposal_duplicate_count + human_duplicate_count}",
                f"proposal/human overlap count: {overlap_count}",
                f"previous candidate date: {previous_candidate_date or 'none'}",
                f"previous candidate count: {len(previous_keys)}",
                f"proposal previous candidate count: {proposal_previous_count}",
                f"human previous candidate count: {human_previous_count}",
                f"Success Cache marker count: {len(cache_marker_keys)}",
                f"both count: {len(both_keys)}",
                f"cache only count: {len(cache_only_keys)}",
                f"previous only count: {len(previous_only_keys)}",
                f"neither count: {len(neither_keys)}",
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
        append_check(
            lines,
            errors,
            previous_keys == expected_previous_keys and invalid_previous_fields == 0,
            "previous candidate structured fieldが直前final candidateとの4-field joinに一致",
            "previous candidate structured fieldが4-field joinと不一致: "
            f"key差分={len(previous_keys ^ expected_previous_keys)} field不正={invalid_previous_fields}",
        )
        append_check(
            lines,
            errors,
            len(previous_keys) + len(candidate_keys - previous_keys) == len(candidate_keys),
            "final candidate件数 = previous + not previous",
            "previous candidate partitionの件数不整合",
        )
        append_check(
            lines,
            errors,
            proposal_previous_count + len(proposal_keys - previous_keys) == len(proposal_keys),
            "proposal_ready件数 = previous + not previous",
            "proposal_ready previous partitionの件数不整合",
        )
        append_check(
            lines,
            errors,
            human_previous_count + len(human_keys - previous_keys) == len(human_keys),
            "human_review件数 = previous + not previous",
            "human_review previous partitionの件数不整合",
        )
        append_check(
            lines,
            errors,
            not previous_only_keys,
            "previous candidateはすべて既存Success Cache markerにも存在",
            f"previous only={len(previous_only_keys)}",
        )
        append_check(
            lines,
            errors,
            all(key not in previous_keys for key in cache_only_keys),
            "cache onlyにはprevious candidate badgeを付与しない",
            "cache onlyにprevious candidate marker混入",
        )
        append_check(
            lines,
            errors,
            len(high_records) + len(other_records) == len(human),
            "HIGH + OTHER = human_review",
            "HIGH / OTHER partitionの件数不整合",
        )
        append_check(
            lines,
            errors,
            priority_fields_match,
            "priority fieldがcanonical入力からの再計算結果と一致",
            "priority fieldがcanonical入力からの再計算結果と不一致",
        )
        append_check(
            lines,
            errors,
            all(
                isinstance(record.get("normalized_review_items"), list)
                and record.get("normalized_review_item_count")
                == len(record["normalized_review_items"])
                and isinstance(record.get("high_project_rank"), int)
                and not isinstance(record.get("high_project_rank"), bool)
                and isinstance(record.get("initial_review"), bool)
                for record in human
            ),
            "human_review priority fieldの型・件数整合OK",
            "human_review priority fieldの型・件数不整合",
        )
        append_check(
            lines,
            errors,
            all(record.get("normalized_review_item_count") == 1 for record in high_records),
            "HIGHはnormalized review itemが実質1個",
            "HIGHにnormalized review item 1個以外が混入",
        )
        append_check(
            lines,
            errors,
            all(
                isinstance(
                    recheck_match_info.get(pair_key(record), {}).get(
                        "required_skills_match_rate"
                    ),
                    (int, float),
                )
                and not isinstance(
                    recheck_match_info[pair_key(record)]["required_skills_match_rate"],
                    bool,
                )
                and recheck_match_info[pair_key(record)]["required_skills_match_rate"] >= 0.8
                for record in high_records
            ),
            "HIGHのrequired score >= 80%",
            "HIGHにrequired score 80%未満または不正値が混入",
        )
        append_check(
            lines,
            errors,
            all(
                record.get("category_match") == "match"
                and record.get("has_08_5_error") is False
                and (record.get("required_skill_recheck_info") or {}).get("skillsheet_chars_used", 0) > 0
                and all(
                    str(check.get("evidence") or "").strip()
                    for check in record.get("required_skill_checks") or []
                )
                and (record.get("sales_status") or {}).get("both_directions_present") is True
                and (record.get("sales_status") or {}).get("all_drafts_generated") is True
                for record in high_records
            ),
            "HIGHのcategory/error/skillsheet/evidence/draft contract OK",
            "HIGH contract違反あり",
        )
        high_project_groups = defaultdict(list)
        for record in high_records:
            high_project_groups[record["project_message_id"]].append(record)
        append_check(
            lines,
            errors,
            all(
                sorted(record["high_project_rank"] for record in records)
                == list(range(1, len(records) + 1))
                for records in high_project_groups.values()
            ),
            "HIGHの案件内rankは1始まりで一意・連続",
            "HIGHの案件内rankに重複または欠番あり",
        )
        append_check(
            lines,
            errors,
            all(
                record.get("initial_review") is (record.get("high_project_rank", 0) <= 3)
                for record in high_records
            )
            and all(
                record.get("high_project_rank") == 0
                and record.get("initial_review") is False
                for record in other_records
            ),
            "initial_reviewは各案件のHIGH rank 1..3のみ",
            "initial_review / high_project_rank contract違反あり",
        )
        append_check(
            lines,
            errors,
            all(not any(field in record for field in priority_fields) for record in proposal),
            "proposal_ready schema/contractはpriority追加前のまま",
            "proposal_readyへpriority fieldが混入",
        )
        if date_part == "20260820":
            expected_20260820 = {
                "final": 618,
                "proposal": 36,
                "human": 582,
                "previous": 146,
                "proposal_previous": 9,
                "human_previous": 137,
                "high": 114,
                "other": 468,
                "initial": 55,
                "initial_sales_target": 91,
                "cache_marker": 201,
                "both": 146,
                "cache_only": 55,
                "previous_only": 0,
                "neither": 417,
            }
            actual_20260820 = {
                "final": len(candidate_keys),
                "proposal": len(proposal_keys),
                "human": len(human_keys),
                "previous": len(previous_keys),
                "proposal_previous": proposal_previous_count,
                "human_previous": human_previous_count,
                "high": len(high_records),
                "other": len(other_records),
                "initial": len(initial_records),
                "initial_sales_target": len(proposal) + len(initial_records),
                "cache_marker": len(cache_marker_keys),
                "both": len(both_keys),
                "cache_only": len(cache_only_keys),
                "previous_only": len(previous_only_keys),
                "neither": len(neither_keys),
            }
            append_check(
                lines,
                errors,
                actual_20260820 == expected_20260820,
                "20260820 expected count fixtureと一致",
                f"20260820 expected count fixtureと不一致: {actual_20260820}",
            )

        draft_groups = defaultdict(list)
        for draft in drafts:
            draft_groups[pair_key(draft)].append(draft)
        proposal_missing_draft = 0
        proposal_category_mismatch = 0
        proposal_08_5_error = 0
        proposal_needs_review = 0
        proposal_not_strict = 0
        proposal_not_evidence_ready = 0
        proposal_empty_evidence = 0
        proposal_explicit_review_reason = 0
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
            if record.get("evidence_ready") is not True:
                proposal_not_evidence_ready += 1
            proposal_empty_evidence += sum(
                not str(check.get("evidence") or "").strip()
                for check in record.get("required_skill_checks") or []
            )
            proposal_explicit_review_reason += sum(
                has_explicit_review_reason(check.get("reason"))
                for check in record.get("required_skill_checks") or []
            )

        append_check(lines, errors, proposal_missing_draft == 0, "proposal_ready draft欠損=0", f"proposal_ready draft欠損={proposal_missing_draft}")
        append_check(lines, errors, proposal_category_mismatch == 0, "proposal_ready category mismatch=0", f"proposal_ready category mismatch={proposal_category_mismatch}")
        append_check(lines, errors, proposal_08_5_error == 0, "proposal_ready 08-5 error=0", f"proposal_ready 08-5 error={proposal_08_5_error}")
        append_check(lines, errors, proposal_needs_review == 0, "proposal_ready needs_human_review/review_notes=0", f"proposal_ready sales review残存={proposal_needs_review}")
        append_check(lines, errors, proposal_not_strict == 0, "proposal_readyはmatching_strictかつsales_ready", f"proposal_ready contract違反={proposal_not_strict}")
        append_check(lines, errors, proposal_not_evidence_ready == 0, "proposal_ready evidence_ready=true", f"proposal_ready evidence_ready違反={proposal_not_evidence_ready}")
        append_check(lines, errors, proposal_empty_evidence == 0, "proposal_ready evidence空=0", f"proposal_ready evidence空={proposal_empty_evidence}")
        append_check(lines, errors, proposal_explicit_review_reason == 0, "proposal_ready explicit review reason=0", f"proposal_ready explicit review reason={proposal_explicit_review_reason}")

        canonical_hash_after = hashlib.sha256(draft_path.read_bytes()).hexdigest()
        append_check(
            lines,
            errors,
            drafts == drafts_before and canonical_hash_after == canonical_hash_before,
            f"canonical 09-5非回帰: records={len(drafts)} sha256={canonical_hash_after}",
            "canonical 09-5 records/hashが変化",
        )

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
                f"5人以上案件数: {sum(count >= 5 for count in counts)}",
                f"10人以上案件数: {sum(count >= 10 for count in counts)}",
            ]
        )

        human_project_counts = Counter(record["project_message_id"] for record in human)
        max_human_count = max(human_project_counts.values()) if human_project_counts else 0
        max_human_projects = {
            project_id
            for project_id, count in human_project_counts.items()
            if count == max_human_count
        }
        max_project_high = [
            record for record in high_records if record["project_message_id"] in max_human_projects
        ]
        max_project_initial = [
            record for record in initial_records if record["project_message_id"] in max_human_projects
        ]
        lines.extend(
            [
                "",
                "【human_review priority 分布】",
                f"max project human_review count: {max_human_count}",
                f"max project HIGH count: {len(max_project_high)}",
                f"max project initial_review count: {len(max_project_initial)}",
            ]
        )
        if date_part == "20260820":
            append_check(
                lines,
                errors,
                max_human_count == 59,
                "20260820 最大案件のhuman_review=59",
                f"20260820 最大案件のhuman_review={max_human_count}",
            )
        append_check(
            lines,
            errors,
            len(max_project_initial) <= 3 * len(max_human_projects),
            "最大案件も全候補保持・initial_reviewはHIGH上位最大3件",
            "最大案件のinitial_reviewがHIGH上位3件を超過",
        )

        if args.simulate_read_only and date_part == "20260819":
            if not proposal_path.exists() or not human_path.exists():
                raise FileNotFoundError("20260819 before queue fixtureが存在しません")
            before_proposal = read_jsonl_as_list(str(proposal_path))
            before_human = read_jsonl_as_list(str(human_path))
            before_proposal_keys = unique_keys(before_proposal, "before proposal_ready", errors)
            before_human_keys = unique_keys(before_human, "before human_review", errors)
            after_proposal_keys = proposal_keys
            after_human_keys = human_keys
            after_human_index = {pair_key(record): record for record in human}
            moved_to_review = before_proposal_keys - after_proposal_keys
            before_original_false_checks = sum(
                check.get("original_match") is False
                for record in before_proposal
                for check in record.get("required_skill_checks") or []
            )
            before_original_false_pairs = sum(
                any(
                    check.get("original_match") is False
                    for check in record.get("required_skill_checks") or []
                )
                for record in before_proposal
            )
            after_original_false_checks = sum(
                check.get("original_match") is False
                for record in proposal
                for check in record.get("required_skill_checks") or []
            )

            append_check(
                lines,
                errors,
                before_proposal_keys == KNOWN_A_20260819 | KNOWN_C_20260819,
                "independent fixture: before A 14 + C 14と一致",
                "before proposal_readyが独立A/C fixtureと不一致",
            )
            append_check(
                lines,
                errors,
                KNOWN_A_20260819 <= after_proposal_keys,
                "known A false rejection=0",
                f"known A誤移動={len(KNOWN_A_20260819 - after_proposal_keys)}",
            )
            append_check(
                lines,
                errors,
                not (KNOWN_EVIDENCE_REVIEW_20260819 & after_proposal_keys),
                "known empty/review C remaining proposal_ready=0",
                "known empty/review Cがproposal_readyに残存",
            )
            affected_reason_tracking_ok = all(
                key in after_human_index
                and {
                        "matching_evidence_empty",
                        "matching_evidence_review_required",
                    }
                    <= set(after_human_index[key].get("review_reasons") or [])
                for key in KNOWN_EVIDENCE_REVIEW_20260819
            )
            append_check(
                lines,
                errors,
                affected_reason_tracking_ok,
                "移動pairのqueue review reasonを追跡可能",
                "移動pairのqueue review reasonが不足",
            )
            append_check(
                lines,
                errors,
                moved_to_review <= after_human_keys,
                "proposal_readyから外れたpairは1対1でhuman_reviewへ移動",
                "proposal_readyから外れたpairがhuman_reviewに存在しない",
            )
            append_check(
                lines,
                errors,
                before_proposal_keys | before_human_keys == candidate_keys,
                "before queue unionがfinal candidateと一致",
                "before queue unionがfinal candidateと不一致",
            )

            lines.extend(
                [
                    "",
                    "【20260819 before/after独立監査】",
                    f"Before proposal_ready: {len(before_proposal_keys)}",
                    f"Before human_review: {len(before_human_keys)}",
                    f"After proposal_ready: {len(after_proposal_keys)}",
                    f"After human_review: {len(after_human_keys)}",
                    f"known A維持: {len(KNOWN_A_20260819 & after_proposal_keys)}",
                    f"known A誤移動: {len(KNOWN_A_20260819 - after_proposal_keys)}",
                    f"known C→review: {len(KNOWN_C_20260819 & moved_to_review)}",
                    f"known C残存: {len(KNOWN_C_20260819 & after_proposal_keys)}",
                    f"empty/review 9移動: {len(KNOWN_EVIDENCE_REVIEW_20260819 & moved_to_review)}",
                    f"multiple-profile旧2残存: {len(KNOWN_MULTIPLE_PROFILE_20260819 & after_proposal_keys)}",
                    f"partial evidence 3残存: {len(KNOWN_PARTIAL_EVIDENCE_20260819 & after_proposal_keys)}",
                    f"Before original_match=false: {before_original_false_checks} checks / {before_original_false_pairs} pair",
                    f"After proposal_ready original_match=false: {after_original_false_checks} checks",
                ]
            )
            for key in sorted(before_proposal_keys):
                action = "proposal_ready維持" if key in after_proposal_keys else "human_reviewへ移動"
                lines.append(f"pair監査: {key[0]} / {key[1]} / {action}")
    except Exception as error:
        message = f"[NG] confirm実行失敗: {error}"
        lines.append(message)
        errors.append(message)

    lines.extend(["", "【結果】NG" if errors else "【結果】OK"])
    if not args.simulate_read_only:
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

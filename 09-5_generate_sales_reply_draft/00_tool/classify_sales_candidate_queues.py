"""
09-5の既存draftをpair単位でproposal_ready / human_reviewへ分離する。

09-5 canonical outputは変更せず、08-5のmatching判定と09-5両方向の
sales review状態から追加queueだけを生成する。
"""

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Set, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "09-5_classify_sales_candidate_queues"
STEP_DIR = Path(__file__).resolve().parents[1]
INPUT_09_4_DIR = project_root / "09-4_remove_category_mismatch_sales_candidates/01_result"
INPUT_08_5_DIR = project_root / "08-5_high_score_required_skill_recheck/01_result"
RECHECK_ALL_PATH = INPUT_08_5_DIR / "high_score_required_skill_recheck_all.jsonl"
RECHECK_ERROR_PATH = INPUT_08_5_DIR / "99_error_high_score_required_skill_recheck.jsonl"
FINAL_CANDIDATE_TEMPLATE = "sales_proposal_candidates_{date}.jsonl"
DRAFT_TEMPLATE = "generate_sales_reply_draft_{date}.jsonl"
PROPOSAL_READY_TEMPLATE = "proposal_ready_{date}.jsonl"
HUMAN_REVIEW_TEMPLATE = "human_review_{date}.jsonl"
EXPECTED_DIRECTIONS = {"reply_to_project", "reply_to_resource"}

EXPLICIT_REVIEW_REASON_PATTERNS = (
    re.compile(r"営業確認前提"),
    re.compile(r"要確認(?!済み|済)"),
    re.compile(r"確認(?:が)?必要"),
    re.compile(r"未確認(?!ではない|でない)"),
    re.compile(
        r"(?:^|[はが、。:：\s])不明"
        r"(?:$|[、。:：\s]|です|である|のため)"
    ),
    re.compile(r"根拠なし(?!ではない|でない)"),
    re.compile(r"記載なし(?!ではない|でない)"),
)

PairKey = Tuple[str, str]


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def pair_key(record: Dict[str, Any]) -> PairKey:
    project_id = normalize_text(record.get("project_message_id"))
    resource_id = normalize_text(record.get("resource_message_id"))
    if not project_id or not resource_id:
        raise ValueError("pair IDが欠落しています")
    return project_id, resource_id


def recheck_pair_key(record: Dict[str, Any]) -> PairKey:
    project_info = record.get("project_info") or {}
    resource_info = record.get("resource_info") or {}
    return pair_key(
        {
            "project_message_id": project_info.get("message_id"),
            "resource_message_id": resource_info.get("message_id"),
        }
    )


def _index_unique(records: Iterable[Dict[str, Any]], key_function, label: str) -> Dict[PairKey, Dict[str, Any]]:
    result: Dict[PairKey, Dict[str, Any]] = {}
    for record in records:
        key = key_function(record)
        if key in result:
            raise ValueError(f"{label}にpair重複があります: {key[0]} / {key[1]}")
        result[key] = record
    return result


def _matching_status(recheck: Optional[Dict[str, Any]], has_error: bool) -> Dict[str, Any]:
    if recheck is None:
        return {
            "matching_strict": False,
            "required_skill_recheck_status": "missing",
            "all_required_skills_confirmed": False,
            "category_match": "missing",
            "has_08_5_error": has_error,
            "required_skill_recheck_info": {},
            "required_skill_checks": [],
        }

    recheck_info = recheck.get("recheck_info") or {}
    checks = recheck.get("required_skill_checks")
    checks = checks if isinstance(checks, list) else []
    required_count = recheck_info.get("required_skill_count")
    confirmed_count = recheck_info.get("confirmed_count")
    human_review_count = recheck_info.get("human_review_count")
    not_confirmed_count = recheck_info.get("not_confirmed_count")
    status = normalize_text(recheck_info.get("recheck_status"))
    category_match = normalize_text(recheck.get("category_match")) or "missing"
    counts_resolved = (
        isinstance(required_count, int)
        and required_count > 0
        and required_count == len(checks)
        and confirmed_count == required_count
        and human_review_count == 0
        and not_confirmed_count == 0
    )
    all_confirmed = (
        status == "required_skill_confirmed"
        and counts_resolved
        and all(normalize_text(check.get("confidence")) == "confirmed" for check in checks)
    )
    matching_strict = all_confirmed and category_match == "match" and not has_error
    return {
        "matching_strict": matching_strict,
        "required_skill_recheck_status": status,
        "all_required_skills_confirmed": all_confirmed,
        "category_match": category_match,
        "has_08_5_error": has_error,
        "required_skill_recheck_info": recheck_info,
        "required_skill_checks": checks,
    }


def _has_review_notes(value: Any) -> bool:
    if isinstance(value, list):
        return any(normalize_text(item) for item in value)
    return bool(normalize_text(value))


def _draft_generated(draft: Dict[str, Any]) -> bool:
    if any(normalize_text(draft.get(key)) for key in ("error", "error_type", "error_message")):
        return False
    return all(
        (
            normalize_text(draft.get("reply_subject")),
            normalize_text(draft.get("draft_mail_text")),
            normalize_text(draft.get("refined_mail_text")),
            normalize_text(draft.get("preview_file_path")),
            normalize_text(draft.get("note_file_path")),
        )
    ) and bool(draft.get("to_recipients"))


def _sales_status(drafts: List[Dict[str, Any]]) -> Dict[str, Any]:
    direction_counts: Dict[str, int] = defaultdict(int)
    review_notes_by_direction: Dict[str, List[str]] = {}
    draft_refs: List[Dict[str, Any]] = []
    for draft in drafts:
        direction = normalize_text(draft.get("draft_direction") or draft.get("mail_mode"))
        direction_counts[direction] += 1
        notes = draft.get("review_notes")
        if isinstance(notes, list):
            normalized_notes = [normalize_text(note) for note in notes if normalize_text(note)]
        elif normalize_text(notes):
            normalized_notes = [normalize_text(notes)]
        else:
            normalized_notes = []
        review_notes_by_direction[direction] = normalized_notes
        draft_refs.append(
            {
                "draft_direction": direction,
                "pair_file_name": draft.get("pair_file_name"),
                "preview_file_path": draft.get("preview_file_path"),
                "note_file_path": draft.get("note_file_path"),
            }
        )

    both_directions_present = (
        len(drafts) == 2
        and set(direction_counts) == EXPECTED_DIRECTIONS
        and all(count == 1 for count in direction_counts.values())
    )
    all_drafts_generated = both_directions_present and all(_draft_generated(draft) for draft in drafts)
    any_human_review = any(draft.get("needs_human_review") is not False for draft in drafts)
    any_review_notes = any(_has_review_notes(draft.get("review_notes")) for draft in drafts)
    sales_ready = (
        both_directions_present
        and all_drafts_generated
        and not any_human_review
        and not any_review_notes
    )
    return {
        "sales_ready": sales_ready,
        "expected_draft_count": 2,
        "actual_draft_count": len(drafts),
        "both_directions_present": both_directions_present,
        "all_drafts_generated": all_drafts_generated,
        "needs_human_review": any_human_review,
        "review_notes_by_direction": review_notes_by_direction,
        "draft_refs": draft_refs,
    }


def has_explicit_review_reason(reason: Any) -> bool:
    """営業・人手確認が必要と明示する狭いphraseだけを検出する。"""
    normalized_reason = normalize_text(reason)
    return any(pattern.search(normalized_reason) for pattern in EXPLICIT_REVIEW_REASON_PATTERNS)


def _evidence_status(checks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """全required skillが非空evidenceかつ明示review理由なしであるか判定する。"""
    empty_evidence_count = sum(not normalize_text(check.get("evidence")) for check in checks)
    review_required_count = sum(
        has_explicit_review_reason(check.get("reason")) for check in checks
    )
    all_confirmed = bool(checks) and all(
        normalize_text(check.get("confidence")) == "confirmed" for check in checks
    )
    return {
        "evidence_ready": (
            all_confirmed
            and empty_evidence_count == 0
            and review_required_count == 0
        ),
        "empty_evidence_count": empty_evidence_count,
        "review_required_count": review_required_count,
    }


def _review_reasons(
    matching: Dict[str, Any],
    sales: Dict[str, Any],
    evidence: Dict[str, Any],
) -> List[str]:
    reasons: List[str] = []
    status = matching["required_skill_recheck_status"]
    category_match = matching["category_match"]
    if status == "missing":
        reasons.append("08_5_result_missing")
    elif status != "required_skill_confirmed" or not matching["all_required_skills_confirmed"]:
        reasons.append("required_skill_review_required")
    if category_match == "unclear":
        reasons.append("category_unclear")
    elif category_match != "match":
        reasons.append("category_mismatch")
    if matching["has_08_5_error"]:
        reasons.append("08_5_error")
    if not sales["both_directions_present"] or not sales["all_drafts_generated"]:
        reasons.append("draft_missing")
    if sales["needs_human_review"] or any(sales["review_notes_by_direction"].values()):
        reasons.append("sales_review_required")
    if evidence["empty_evidence_count"]:
        reasons.append("matching_evidence_empty")
    if evidence["review_required_count"]:
        reasons.append("matching_evidence_review_required")
    return reasons


def classify_candidate_pairs(
    candidate_records: List[Dict[str, Any]],
    draft_records: List[Dict[str, Any]],
    recheck_records: List[Dict[str, Any]],
    error_records: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    candidate_index = _index_unique(candidate_records, pair_key, "09-4候補")
    recheck_index = _index_unique(recheck_records, recheck_pair_key, "08-5結果")
    error_keys: Set[PairKey] = {recheck_pair_key(record) for record in error_records}
    draft_groups: DefaultDict[PairKey, List[Dict[str, Any]]] = defaultdict(list)
    for draft in draft_records:
        draft_groups[pair_key(draft)].append(draft)

    extra_draft_keys = set(draft_groups) - set(candidate_index)
    if extra_draft_keys:
        first = sorted(extra_draft_keys)[0]
        raise ValueError(f"09-4候補に存在しない09-5 draftがあります: {first[0]} / {first[1]}")

    proposal_ready: List[Dict[str, Any]] = []
    human_review: List[Dict[str, Any]] = []
    for key, candidate in candidate_index.items():
        recheck = recheck_index.get(key)
        matching = _matching_status(recheck, key in error_keys)
        if matching["required_skill_recheck_status"] == "required_skill_not_confirmed":
            raise ValueError(
                f"08-5 not_confirmedが09-4候補に混入しています: {key[0]} / {key[1]}"
            )
        sales = _sales_status(draft_groups.get(key, []))
        evidence = _evidence_status(matching["required_skill_checks"])
        is_proposal_ready = (
            matching["matching_strict"]
            and sales["sales_ready"]
            and evidence["evidence_ready"]
        )
        queue = "proposal_ready" if is_proposal_ready else "human_review"
        output_record = {
            "project_message_id": key[0],
            "resource_message_id": key[1],
            "pair_file_name": candidate.get("pair_file_name"),
            "score_band": candidate.get("score_band"),
            "queue": queue,
            "matching_strict": matching["matching_strict"],
            "sales_ready": sales["sales_ready"],
            "evidence_ready": evidence["evidence_ready"],
            "required_skill_recheck_status": matching["required_skill_recheck_status"],
            "required_skill_recheck_info": matching["required_skill_recheck_info"],
            "required_skill_checks": matching["required_skill_checks"],
            "category_match": matching["category_match"],
            "category_note": (recheck or {}).get("category_note"),
            "has_08_5_error": matching["has_08_5_error"],
            "sales_status": {
                key_name: value
                for key_name, value in sales.items()
                if key_name not in {"sales_ready", "draft_refs"}
            },
            "draft_refs": sales["draft_refs"],
            "review_reasons": (
                []
                if is_proposal_ready
                else _review_reasons(matching, sales, evidence)
            ),
        }
        if is_proposal_ready:
            proposal_ready.append(output_record)
        else:
            human_review.append(output_record)

    return proposal_ready, human_review


def resolve_paths(date_part: str) -> Tuple[Path, Path, Path, Path]:
    candidate_path = INPUT_09_4_DIR / FINAL_CANDIDATE_TEMPLATE.format(date=date_part)
    draft_path = STEP_DIR / "01_result" / DRAFT_TEMPLATE.format(date=date_part)
    proposal_path = STEP_DIR / "01_result" / PROPOSAL_READY_TEMPLATE.format(date=date_part)
    human_path = STEP_DIR / "01_result" / HUMAN_REVIEW_TEMPLATE.format(date=date_part)
    for path in (candidate_path, draft_path, RECHECK_ALL_PATH):
        if not path.exists():
            raise FileNotFoundError(f"classification入力が存在しません: {path}")
    return candidate_path, draft_path, proposal_path, human_path


def generate_candidate_queues(date_part: str) -> Dict[str, int]:
    candidate_path, draft_path, proposal_path, human_path = resolve_paths(date_part)
    candidates = read_jsonl_as_list(str(candidate_path))
    drafts = read_jsonl_as_list(str(draft_path))
    rechecks = read_jsonl_as_list(str(RECHECK_ALL_PATH))
    errors = read_jsonl_as_list(str(RECHECK_ERROR_PATH)) if RECHECK_ERROR_PATH.exists() else []
    proposal_ready, human_review = classify_candidate_pairs(candidates, drafts, rechecks, errors)
    write_jsonl(str(proposal_path), proposal_ready)
    write_jsonl(str(human_path), human_review)
    return {
        "final_candidates": len(candidates),
        "matching_strict": sum(record["matching_strict"] for record in proposal_ready + human_review),
        "sales_ready": sum(record["sales_ready"] for record in proposal_ready + human_review),
        "evidence_ready": sum(record["evidence_ready"] for record in proposal_ready + human_review),
        "proposal_ready": len(proposal_ready),
        "human_review": len(human_review),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="09-5 sales candidate queue classification")
    parser.add_argument("--target-date", required=True, help="対象日 YYYYMMDD")
    return parser.parse_args()


def main() -> None:
    logger = get_logger(STEP_NAME)
    args = parse_args()
    try:
        summary = generate_candidate_queues(args.target_date)
        logger.ok(
            "queue分類完了: "
            f"final={summary['final_candidates']} "
            f"matching_strict={summary['matching_strict']} "
            f"sales_ready={summary['sales_ready']} "
            f"evidence_ready={summary['evidence_ready']} "
            f"proposal_ready={summary['proposal_ready']} "
            f"human_review={summary['human_review']}"
        )
    except Exception as error:
        logger.error(f"queue分類失敗: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()

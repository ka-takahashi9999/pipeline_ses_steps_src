"""08-5 Issue 1 production Batch engine offline confirm。

保存済み678件だけをproduction collectorへreplayし、sales gateをread-onlyで
適用してproposal_ready集合の完全一致をblocking確認する。
"""

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Sequence, Set, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STEP_DIR = Path(__file__).resolve().parents[1]
TOOL_DIR = STEP_DIR / "00_tool"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(TOOL_DIR))

from common.json_utils import read_jsonl  # noqa: E402
import high_score_required_skill_recheck_batch as BATCH  # noqa: E402


SAVED_RUN = STEP_DIR / "_test_batch_api_canary/canary678-20260822-01"
CONFIRM_JSON = STEP_DIR / "02_confirm/confirm_result_batch_production_engine.json"
CONFIRM_TEXT = STEP_DIR / "02_confirm/confirm_result_batch_production_engine.txt"

CLEAR_KEEP_IDS = (
    "c-canary678-20260822-01-0352-b5d37b77ca8c",
    "c-canary678-20260822-01-0253-7d7f85cca0da",
    "c-canary678-20260822-01-0303-839007bc06a6",
    "c-canary678-20260822-01-0341-1bb1d4d605b3",
    "c-canary678-20260822-01-0342-18f4b14de8ad",
)
CLEAR_FALSE_POSITIVE_IDS = (
    "c-canary678-20260822-01-0085-961ae7fbe465",
    "c-canary678-20260822-01-0131-7c68e13f19c1",
)


def _load_sales_gate_module() -> Any:
    path = (
        PROJECT_ROOT
        / "09-5_generate_sales_reply_draft/00_tool/classify_sales_candidate_queues.py"
    )
    spec = importlib.util.spec_from_file_location(
        "classify_sales_candidate_queues_batch_production_confirm", path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("sales gate moduleのread-only importに失敗")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SALES_GATE = _load_sales_gate_module()


def _pair_key(record: Dict[str, Any]) -> Tuple[str, str]:
    return (
        str(record.get("project_message_id") or ""),
        str(record.get("resource_message_id") or ""),
    )


def _load_sales_ready_map(
    keys: Iterable[Tuple[str, str]],
) -> Tuple[str, Dict[Tuple[str, str], bool], int]:
    target = set(keys)
    result_dir = PROJECT_ROOT / "09-5_generate_sales_reply_draft/01_result"
    candidates = []
    for proposal_path in sorted(result_dir.glob("proposal_ready_*.jsonl")):
        date = proposal_path.stem.replace("proposal_ready_", "")
        human_path = result_dir / f"human_review_{date}.jsonl"
        if not human_path.exists():
            continue
        mapping: Dict[Tuple[str, str], bool] = {}
        for path in (proposal_path, human_path):
            for record in read_jsonl(str(path)):
                key = _pair_key(record)
                if key in mapping:
                    raise ValueError(f"sales queue pair重複: {date} {key}")
                mapping[key] = record.get("sales_ready") is True
        candidates.append((len(target & set(mapping)), date, mapping))
    if not candidates:
        raise FileNotFoundError("proposal_ready/human_review queue pairが見つかりません")
    overlap, date, mapping = max(candidates, key=lambda item: (item[0], item[1]))
    return date, mapping, overlap


def _proposal_set(
    pairs: Sequence[Dict[str, Any]],
    side: str,
    sales_ready: Dict[Tuple[str, str], bool],
) -> Set[Tuple[str, str]]:
    result: Set[Tuple[str, str]] = set()
    for pair in pairs:
        state = pair[side]
        evidence = SALES_GATE._evidence_status(state["required_skill_checks"])
        key = _pair_key(pair)
        if (
            pair["schema_valid"] is True
            and state["status"] == "confirmed"
            and state["category_match"] == "match"
            and sales_ready.get(key, False)
            and evidence.get("evidence_ready") is True
        ):
            result.add(key)
    return result


def run_confirm(write: bool = True) -> Dict[str, Any]:
    replay = BATCH.offline_replay_saved_canary(SAVED_RUN)
    pairs = replay["audit_pairs"]
    by_id = {str(pair.get("custom_id") or ""): pair for pair in pairs}
    if len(by_id) != len(pairs):
        raise ValueError("offline replay custom_id重複")
    date, sales_ready, overlap = _load_sales_ready_map(_pair_key(pair) for pair in pairs)
    before_proposals = _proposal_set(pairs, "before", sales_ready)
    after_proposals = _proposal_set(pairs, "after", sales_ready)
    added = after_proposals - before_proposals
    removed = before_proposals - after_proposals

    keep_retained = sum(
        by_id[custom_id]["after"]["status"] != "not_confirmed"
        and by_id[custom_id]["after"]["category_match"] != "mismatch"
        for custom_id in CLEAR_KEEP_IDS
    )
    false_positive_confirmed = sum(
        by_id[custom_id]["after"]["status"] == "confirmed"
        for custom_id in CLEAR_FALSE_POSITIVE_IDS
    )
    false_positive_proposals = sum(
        _pair_key(by_id[custom_id]) in after_proposals
        for custom_id in CLEAR_FALSE_POSITIVE_IDS
    )
    schema_fallback_promotions = sum(
        pair["schema_valid"] is False and _pair_key(pair) in after_proposals
        for pair in pairs
    )
    guard_promotions = sum(
        int(pair["guard"].get("promoted_to_confirmed", -1)) for pair in pairs
    )
    quality_checks = {
        "sample_678": replay["sample_count"] == 678,
        "clear_keep_5_of_5": keep_retained == 5,
        "clear_false_positive_confirmed_0": false_positive_confirmed == 0,
        "proposal_ready_false_positive_0": false_positive_proposals == 0,
        "schema_fallback_promotion_0": schema_fallback_promotions == 0,
        "proposal_ready_count_equal": len(before_proposals) == len(after_proposals),
        "proposal_ready_added_0": not added,
        "proposal_ready_removed_0": not removed,
        "guard_confirmed_promotion_0": guard_promotions == 0,
        "production_write_0": replay["production_write"] == 0,
        "new_batch_submit_0": replay["new_batch_submit"] == 0,
        "new_llm_call_0": replay["new_llm_call"] == 0,
    }
    result = {
        "title": "Issue 1: 08-5 Production Batch Engine offline confirm",
        "sample_count": replay["sample_count"],
        "before": replay["before"],
        "after": replay["after"],
        "clear_keep_retained": keep_retained,
        "clear_false_positive_confirmed": false_positive_confirmed,
        "proposal_ready_false_positive": false_positive_proposals,
        "schema_fallback_erroneous_promotion": schema_fallback_promotions,
        "proposal_ready_before": len(before_proposals),
        "proposal_ready_after": len(after_proposals),
        "proposal_ready_added": len(added),
        "proposal_ready_removed": len(removed),
        "proposal_ready_added_pairs": sorted([list(key) for key in added]),
        "proposal_ready_removed_pairs": sorted([list(key) for key in removed]),
        "new_human_review_rescue": replay["new_human_review_rescue"],
        "confirmed_to_human_review": replay["confirmed_to_human_review"],
        "sales_gate_source_date": date,
        "sales_gate_pair_overlap": overlap,
        "production_write": replay["production_write"],
        "new_batch_submit": replay["new_batch_submit"],
        "new_llm_call": replay["new_llm_call"],
        "quality_checks": quality_checks,
        "quality_pass": all(quality_checks.values()),
    }
    if write:
        CONFIRM_JSON.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        lines = [
            "【Issue 1: 08-5 Production Batch Engine offline confirm】",
            f"sample: {result['sample_count']}",
            f"CLEAR_KEEP: {keep_retained}/5",
            f"CLEAR_FALSE_POSITIVE confirmed: {false_positive_confirmed}",
            f"proposal_ready false positive: {false_positive_proposals}",
            f"proposal_ready before/after: {len(before_proposals)}/{len(after_proposals)}",
            f"proposal_ready added/removed: {len(added)}/{len(removed)}",
            f"schema fallback promotion: {schema_fallback_promotions}",
            f"QUALITY: {'PASS' if result['quality_pass'] else 'FAIL'}",
        ]
        CONFIRM_TEXT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return result


def main() -> None:
    report = run_confirm(write=True)
    print(CONFIRM_TEXT.read_text(encoding="utf-8"), end="")
    if not report["quality_pass"]:
        sys.exit(1)


if __name__ == "__main__":
    main()

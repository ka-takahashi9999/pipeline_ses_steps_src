"""保存済み678件のBatch minimal guard shadow replayを確認する。"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


STEP_DIR = Path(__file__).resolve().parents[1]
CANARY_ROOT = STEP_DIR / "_test_batch_api_canary"
DEFAULT_RUN_ID = "canary678-20260822-01"
REPLAY_FILENAME = "minimal_guard_shadow_replay.json"
CONFIRM_RESULT = Path(__file__).resolve().parent / "confirm_result_batch_minimal_safety_guard.txt"


def _load_json(path: Path) -> Dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON objectではありません: {path}")
    return value


def _check(lines: List[str], errors: List[str], condition: bool, ok: str, ng: str) -> None:
    if condition:
        lines.append(f"[OK] {ok}")
    else:
        message = f"[NG] {ng}"
        lines.append(message)
        errors.append(message)


def confirm(run_id: str) -> Dict[str, Any]:
    replay_path = CANARY_ROOT / run_id / REPLAY_FILENAME
    if not replay_path.is_file():
        raise FileNotFoundError(f"shadow replay artifactがありません: {replay_path}")
    report = _load_json(replay_path)
    pairs = report.get("pairs")
    if not isinstance(pairs, list):
        raise ValueError("pairsがlistではありません")
    errors: List[str] = []
    lines = ["=== 08-5 Batch minimal safety guard confirm ===", ""]

    custom_ids = [str(pair.get("custom_id") or "") for pair in pairs if isinstance(pair, dict)]
    required_pair_keys = {
        "ordinal",
        "custom_id",
        "project_message_id",
        "resource_message_id",
        "schema_valid",
        "before",
        "after",
        "guard_reasons",
    }
    pair_schema_ok = all(
        isinstance(pair, dict)
        and required_pair_keys <= set(pair)
        and isinstance(pair.get("before"), dict)
        and isinstance(pair.get("after"), dict)
        and isinstance(pair.get("guard_reasons"), list)
        for pair in pairs
    )
    allowed_statuses = {"confirmed", "human_review", "not_confirmed"}
    state_ok = all(
        pair[side].get("status") in allowed_statuses
        and isinstance(pair[side].get("required_skill_checks"), list)
        for pair in pairs
        for side in ("before", "after")
    ) if pair_schema_ok else False

    before = report.get("before") if isinstance(report.get("before"), dict) else {}
    after = report.get("after") if isinstance(report.get("after"), dict) else {}
    _check(lines, errors, len(pairs) == report.get("sample_size") == 678, "入力/出力678件整合", f"件数不整合 pairs={len(pairs)} sample={report.get('sample_size')}")
    _check(lines, errors, len(custom_ids) == len(set(custom_ids)) == 678 and all(custom_ids), "custom_id 678件一意", "custom_id欠落または重複")
    _check(lines, errors, pair_schema_ok and state_ok, "pair/state schema整合", "pair/state schema不整合")
    _check(lines, errors, sum(before.get(key, 0) for key in allowed_statuses) == 678, "before status partition=678", "before status partition不整合")
    _check(lines, errors, sum(after.get(key, 0) for key in allowed_statuses) == 678, "after status partition=678", "after status partition不整合")
    _check(lines, errors, report.get("clear_keep_before_retained") == 0 and report.get("clear_keep_after_retained") == 5, "CLEAR_KEEP 0/5→5/5", "CLEAR_KEEP救済不成立")
    _check(lines, errors, report.get("clear_false_positive_before_confirmed") == 2 and report.get("clear_false_positive_after_confirmed") == 0, "CLEAR_FALSE_POSITIVE 2→0", "CLEAR_FALSE_POSITIVE安全化不成立")
    _check(lines, errors, report.get("proposal_ready_false_positive_after") == 0, "proposal_ready false positive=0", "proposal_ready false positiveあり")
    _check(lines, errors, report.get("schema_fallback_erroneous_promotion") == 0, "schema fallback誤昇格=0", "schema fallback誤昇格あり")
    _check(lines, errors, before.get("candidate_loss") == 64 and after.get("candidate_loss", 999) <= 64, "candidate loss 64以下", f"candidate loss不正 before={before.get('candidate_loss')} after={after.get('candidate_loss')}")
    _check(lines, errors, report.get("new_human_review_rescue", 999) <= 15, "新規human_review rescue<=15", "human_review rescueが想定範囲超過")
    _check(lines, errors, report.get("confirmed_to_human_review", 999) <= 15, "confirmed→human_review<=15", "confirmed downgradeが想定範囲超過")
    _check(lines, errors, report.get("affected_pairs", 999) <= 25, "営業状態変更pair<=25", "営業状態変更が想定範囲超過")
    _check(lines, errors, report.get("production_change") == 0 and report.get("production_write") == 0, "production change/write=0", "production change/writeあり")
    _check(lines, errors, report.get("new_llm_call") == 0 and report.get("new_batch_submit") == 0 and report.get("files_upload") == 0 and report.get("aws_write") == 0, "LLM/Batch/upload/AWS write=0", "禁止された外部実行あり")
    _check(lines, errors, report.get("quality_pass") is True and all(report.get("quality_checks", {}).values()), "QUALITY PASS", "quality check NG")

    lines.extend(
        [
            "",
            f"before: {json.dumps(before, ensure_ascii=False, sort_keys=True)}",
            f"after: {json.dumps(after, ensure_ascii=False, sort_keys=True)}",
            f"affected pairs: {report.get('affected_pairs')}",
            f"new human_review rescue: {report.get('new_human_review_rescue')}",
            f"confirmed to human_review: {report.get('confirmed_to_human_review')}",
            "",
            "【結果】NG" if errors else "【結果】OK",
        ]
    )
    text = "\n".join(lines) + "\n"
    CONFIRM_RESULT.write_text(text, encoding="utf-8")
    return {"ok": not errors, "errors": errors, "text": text}


def main() -> None:
    parser = argparse.ArgumentParser(description="08-5 Batch minimal guard replay confirm")
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    args = parser.parse_args()
    try:
        result = confirm(args.run_id)
        print(result["text"], end="")
        if not result["ok"]:
            sys.exit(1)
    except Exception as error:
        print(f"[NG] confirm実行失敗: {type(error).__name__}: {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

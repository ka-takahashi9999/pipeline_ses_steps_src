#!/usr/bin/env python3
"""保存済み20260820成果物だけで07-1/08-5のAI context変更を確認する。"""

import json
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any, Dict, Iterable, List


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from common.skillsheet_ai_context import build_skillsheet_ai_context_result


RAW_PATH = PROJECT_ROOT / "04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl"
NORMALIZED_PATH = (
    PROJECT_ROOT
    / "04-2_normalize_skillsheets_text/01_result/normalize_skillsheets_text.jsonl"
)
INPUT_07 = (
    PROJECT_ROOT
    / "06-80_duplicate_proposal_check/01_result/duplicate_proposal_check.jsonl"
)
OUTPUT_07 = (
    PROJECT_ROOT
    / "07-1_requirement_skill_ai_matching/01_result/requirement_skill_ai_matching.jsonl"
)
ERROR_07 = (
    PROJECT_ROOT
    / "07-1_requirement_skill_ai_matching/01_result/99_error_requirement_skill_ai_matching.jsonl"
)
INPUTS_08 = (
    PROJECT_ROOT / "08-4_match_score_sort/01_result/match_score_sort_100percent.jsonl",
    PROJECT_ROOT / "08-4_match_score_sort/01_result/match_score_sort_80to99percent.jsonl",
)
OUTPUT_08 = (
    PROJECT_ROOT
    / "08-5_high_score_required_skill_recheck/01_result/high_score_required_skill_recheck_all.jsonl"
)
RESULT_PATH = Path(__file__).resolve().parent / "confirm_result_skillsheet_ai_context.txt"

KNOWN_SAMPLE_RESOURCE_ID = "1a01dd3655110b52"
EXPECTED_VALID_EVIDENCE_COUNT = 1399


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                raise ValueError(f"{path.name}:{line_number} empty line")
            record = json.loads(line)
            if not isinstance(record, dict):
                raise ValueError(f"{path.name}:{line_number} is not object")
            records.append(record)
    return records


def _by_message_id(records: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {
        str(record.get("message_id")): record
        for record in records
        if record.get("message_id")
    }


def _resource_id(record: Dict[str, Any]) -> str:
    return str(record.get("resource_info", {}).get("message_id", ""))


def _truncate_07(text: str, max_chars: int = 5000) -> str:
    if len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    last_newline = truncated.rfind("\n")
    if last_newline > int(max_chars * 0.8):
        return truncated[:last_newline] + "\n...(以下省略)"
    return truncated + "...(以下省略)"


def _truncate_08(text: str, max_chars: int = 10000) -> str:
    if len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    last_newline = truncated.rfind("\n")
    if last_newline > int(max_chars * 0.8):
        return truncated[:last_newline]
    return truncated


def _evidence_key(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text)
    return re.sub(r"[\s,。:]+", "", normalized)


def _rate(before: int, after: int) -> float:
    return ((before - after) / before * 100.0) if before else 0.0


def main() -> None:
    errors: List[str] = []
    lines = ["=== skillsheet AI context confirm ===", ""]

    raw_records = _read_jsonl(RAW_PATH)
    normalized_records = _read_jsonl(NORMALIZED_PATH)
    input_07 = _read_jsonl(INPUT_07)
    output_07 = _read_jsonl(OUTPUT_07)
    error_07 = _read_jsonl(ERROR_07)
    input_08 = [record for path in INPUTS_08 for record in _read_jsonl(path)]
    input_08 = [record for record in input_08 if record.get("status") != "no_match"]
    output_08 = _read_jsonl(OUTPUT_08)

    raw_map = _by_message_id(raw_records)
    normalized_map = _by_message_id(normalized_records)
    context_map: Dict[str, str] = {}
    fallback_map: Dict[str, bool] = {}
    for message_id, record in normalized_map.items():
        result = build_skillsheet_ai_context_result(str(record.get("skillsheet") or ""))
        context_map[message_id] = result.text
        fallback_map[message_id] = result.used_fallback

    if len(raw_records) != len(normalized_records):
        errors.append("04-1/04-2 input record count mismatch")

    if len(input_07) != len(output_07) + len(error_07):
        errors.append("07-1 input/output count mismatch")
    if len(input_08) != len(output_08):
        errors.append("08-5 input/output count mismatch")

    ids_07 = [_resource_id(record) for record in input_07]
    ids_08 = [_resource_id(record) for record in input_08]
    before_07 = sum(
        len(_truncate_07(str(normalized_map.get(mid, {}).get("skillsheet") or "")))
        for mid in ids_07
    )
    after_07 = sum(len(_truncate_07(context_map.get(mid, ""))) for mid in ids_07)
    before_08 = sum(
        len(_truncate_08(str(raw_map.get(mid, {}).get("skillsheet") or "")))
        for mid in ids_08
    )
    after_08 = sum(len(_truncate_08(context_map.get(mid, ""))) for mid in ids_08)

    fallback_07 = sum(1 for mid in ids_07 if fallback_map.get(mid, False))
    fallback_08 = sum(1 for mid in ids_08 if fallback_map.get(mid, False))

    source_backed = 0
    retained_valid = 0
    known_invalid_removed = 0
    unexpected_losses: List[str] = []
    for record in output_08:
        message_id = _resource_id(record)
        before_text = _evidence_key(
            _truncate_08(str(raw_map.get(message_id, {}).get("skillsheet") or ""))
        )
        after_text = _evidence_key(_truncate_08(context_map.get(message_id, "")))
        for check in record.get("required_skill_checks") or []:
            evidence = str(check.get("evidence") or "").strip()
            evidence_key = _evidence_key(evidence)
            if not evidence or not evidence_key or evidence_key not in before_text:
                continue
            source_backed += 1
            if evidence_key in after_text:
                retained_valid += 1
                continue
            is_known_invalid = (
                message_id == KNOWN_SAMPLE_RESOURCE_ID
                and "Java11" in evidence
                and "Spring" in evidence
            )
            if is_known_invalid:
                known_invalid_removed += 1
            else:
                unexpected_losses.append(f"resource={message_id} evidence={evidence[:80]}")

    valid_evidence_count = source_backed - known_invalid_removed
    if valid_evidence_count != EXPECTED_VALID_EVIDENCE_COUNT:
        errors.append(
            "valid evidence baseline mismatch: "
            f"expected={EXPECTED_VALID_EVIDENCE_COUNT} actual={valid_evidence_count}"
        )
    if retained_valid != EXPECTED_VALID_EVIDENCE_COUNT:
        errors.append(
            "valid evidence retention mismatch: "
            f"expected={EXPECTED_VALID_EVIDENCE_COUNT} actual={retained_valid}"
        )
    if unexpected_losses:
        errors.append("unexpected valid evidence loss: " + unexpected_losses[0])
    if known_invalid_removed != 1:
        errors.append(
            f"known sample evidence removal mismatch: expected=1 actual={known_invalid_removed}"
        )

    known_record = normalized_map.get(KNOWN_SAMPLE_RESOURCE_ID, {})
    known_before = str(known_record.get("skillsheet") or "")
    known_after = context_map.get(KNOWN_SAMPLE_RESOURCE_ID, "")
    if "Java11(WebAPI)" not in known_before or "Springboot" not in known_before:
        errors.append("known sample fixture evidence missing in before context")
    if "Java11(WebAPI)" in known_after or "Springboot" in known_after:
        errors.append("known sample fixture evidence remains in after context")

    expected_04_2_keys = {
        "message_id",
        "success",
        "source",
        "urls",
        "skillsheet",
        "raw_char_count",
        "clean_char_count",
        "reduction_char_count",
        "reduction_rate",
        "cleanup_flags",
        "cleanup_stats",
        "source_step",
    }
    if any(set(record) != expected_04_2_keys for record in normalized_records):
        errors.append("04-2 JSONL schema mismatch")

    expected_07_keys = {
        "project_info",
        "resource_info",
        "required_skills",
        "optional_skills",
        "evaluation_meta",
    }
    if any(set(record) != expected_07_keys for record in output_07):
        errors.append("07-1 JSONL schema mismatch")

    check_keys = {
        "skill",
        "original_match",
        "recheck_match",
        "confidence",
        "reason",
        "evidence",
    }
    for record in output_08:
        required_checks = record.get("required_skill_checks")
        if not isinstance(required_checks, list) or any(
            not isinstance(check, dict) or set(check) != check_keys
            for check in required_checks
        ):
            errors.append("08-5 JSONL schema mismatch")
            break

    lines.extend(
        [
            f"07-1 input count: {len(input_07)}",
            f"08-5 input count: {len(input_08)}",
            f"07-1 chars: before={before_07} after={after_07} reduction={_rate(before_07, after_07):.4f}%",
            f"08-5 chars: before={before_08} after={after_08} reduction={_rate(before_08, after_08):.4f}%",
            f"fallback pairs: 07-1={fallback_07} 08-5={fallback_08}",
            f"valid evidence retention: {retained_valid}/{valid_evidence_count}",
            f"known invalid sample evidence removed: {known_invalid_removed}",
            "new LLM calls: 0",
            "QUALITY: " + ("FAIL" if errors else "PASS"),
        ]
    )
    if errors:
        lines.append("errors:")
        lines.extend(f"- {error}" for error in errors[:10])

    RESULT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()

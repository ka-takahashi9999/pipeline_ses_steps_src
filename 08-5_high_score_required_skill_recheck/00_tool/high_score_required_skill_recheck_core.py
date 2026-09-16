"""08-5 Direct / Batch 共通の純粋処理。

I/O、OpenAI呼出し、Batch stateは扱わない。Directの既存contractを保ったまま、
request生成、validator、response normalization、record生成だけを共有する。
"""

import hashlib
import json
from copy import deepcopy
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple


OUTPUT_ARTIFACT_ORDER = (
    "all",
    "confirmed",
    "human_review",
    "not_confirmed",
    "error",
)


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def sha256_value(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def build_full_system_prompt(
    system_prompt: str, response_schema: Dict[str, Any]
) -> str:
    schema_text = json.dumps(response_schema, ensure_ascii=False, indent=2)
    return (
        f"{system_prompt}\n\n"
        "必ず以下のJSONスキーマに従ってJSONのみを返すこと。"
        "キー名は変更禁止。値のみ更新可。\n"
        f"```json\n{schema_text}\n```"
    )


def build_batch_request_body(
    system_prompt: str,
    model: str,
    response_schema: Dict[str, Any],
    user_prompt: str,
) -> Dict[str, Any]:
    return {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": build_full_system_prompt(system_prompt, response_schema),
            },
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.0,
        "max_tokens": 4096,
        "response_format": {"type": "json_object"},
    }


def normalize_required_skill_checks(
    required_skills: Sequence[Dict[str, Any]],
    checks: Any,
    valid_confidences: Sequence[str],
    skill_text: Callable[[Dict[str, Any]], str],
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    if not isinstance(checks, list):
        return None, "required_skill_checksがlistでない"
    if len(checks) != len(required_skills):
        return (
            None,
            f"required_skill_checks件数不一致: 入力={len(required_skills)} 出力={len(checks)}",
        )

    normalized: List[Dict[str, Any]] = []
    valid = set(valid_confidences)
    for index, (original, check) in enumerate(zip(required_skills, checks)):
        if not isinstance(check, dict):
            return None, f"required_skill_checks[{index}]がdictでない"
        expected_skill = skill_text(original)
        if check.get("skill") != expected_skill:
            return (
                None,
                f"required_skill_checks[{index}]のskill不一致: "
                f"入力={expected_skill!r} 出力={check.get('skill')!r}",
            )
        confidence = check.get("confidence")
        if confidence not in valid:
            return (
                None,
                f"required_skill_checks[{index}]のconfidence不正: {confidence!r}",
            )
        reason = check.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            return None, f"required_skill_checks[{index}]のreasonが空またはnull"
        evidence = check.get("evidence")
        if evidence is None:
            evidence = ""
        if not isinstance(evidence, str):
            evidence = str(evidence)
        normalized.append(
            {
                "skill": expected_skill,
                "original_match": original.get("match") is True,
                "recheck_match": confidence != "not_confirmed",
                "confidence": confidence,
                "reason": reason.strip(),
                "evidence": evidence.strip(),
            }
        )
    return normalized, None


def normalize_category_fields(
    response: Dict[str, Any], valid_category_matches: Sequence[str]
) -> Tuple[str, str]:
    category_match = str(response.get("category_match", "unclear")).strip().lower()
    if category_match not in set(valid_category_matches):
        category_match = "unclear"
    category_note = str(response.get("category_note") or "").strip()
    if not category_note:
        category_note = "判定不明"
    return category_match, category_note


def decide_recheck_status(
    checks: Sequence[Dict[str, Any]],
    status_confirmed: str,
    status_human_review: str,
    status_not_confirmed: str,
) -> str:
    confidences = [check.get("confidence") for check in checks]
    if "not_confirmed" in confidences:
        return status_not_confirmed
    if "human_review" in confidences:
        return status_human_review
    return status_confirmed


def build_result_record(
    record: Dict[str, Any],
    source_score_band: str,
    checks: List[Dict[str, Any]],
    skillsheet_chars_used: int,
    category_match: str,
    category_note: str,
    model: str,
    status_confirmed: str,
    status_human_review: str,
    status_not_confirmed: str,
) -> Dict[str, Any]:
    result = deepcopy(record)
    status = decide_recheck_status(
        checks,
        status_confirmed,
        status_human_review,
        status_not_confirmed,
    )
    result["source_score_band"] = source_score_band
    result["recheck_info"] = {
        "recheck_status": status,
        "model": model,
        "skillsheet_chars_used": skillsheet_chars_used,
        "required_skill_count": len(checks),
        "confirmed_count": sum(
            1 for check in checks if check.get("confidence") == "confirmed"
        ),
        "human_review_count": sum(
            1 for check in checks if check.get("confidence") == "human_review"
        ),
        "not_confirmed_count": sum(
            1 for check in checks if check.get("confidence") == "not_confirmed"
        ),
    }
    result["required_skill_checks"] = checks
    result["category_match"] = category_match
    result["category_note"] = category_note or ""
    return result


def refresh_result_counts(
    record: Dict[str, Any],
    status_confirmed: str,
    status_human_review: str,
    status_not_confirmed: str,
) -> None:
    checks = record.get("required_skill_checks")
    info = record.get("recheck_info")
    if not isinstance(checks, list) or not isinstance(info, dict):
        raise ValueError("08-5 result record contract不正")
    info["recheck_status"] = decide_recheck_status(
        checks,
        status_confirmed,
        status_human_review,
        status_not_confirmed,
    )
    info["required_skill_count"] = len(checks)
    info["confirmed_count"] = sum(
        1 for check in checks if check.get("confidence") == "confirmed"
    )
    info["human_review_count"] = sum(
        1 for check in checks if check.get("confidence") == "human_review"
    )
    info["not_confirmed_count"] = sum(
        1 for check in checks if check.get("confidence") == "not_confirmed"
    )


def _result_partition(record: Dict[str, Any]) -> str:
    status = record.get("recheck_info", {}).get("recheck_status")
    mapping = {
        "required_skill_confirmed": "confirmed",
        "required_skill_human_review": "human_review",
        "required_skill_not_confirmed": "not_confirmed",
    }
    if status not in mapping:
        raise ValueError(f"recheck_status不正: {status!r}")
    return mapping[status]


def validate_output_contract(
    rows: Dict[str, Sequence[Dict[str, Any]]],
    skill_text: Callable[[Dict[str, Any]], str],
    expected_source_records: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, int]:
    """Validate the five-file contract shared by legacy and Batch output."""
    if set(rows) != set(OUTPUT_ARTIFACT_ORDER):
        raise ValueError("08-5成果物集合不一致")

    all_rows = list(rows["all"])
    expected_partitions: Dict[str, List[Dict[str, Any]]] = {
        "confirmed": [],
        "human_review": [],
        "not_confirmed": [],
    }
    identities = []
    for ordinal, record in enumerate(all_rows, 1):
        if not isinstance(record, dict):
            raise ValueError(f"all ordinal={ordinal}がJSON objectでない")
        project_info = record.get("project_info")
        resource_info = record.get("resource_info")
        if not isinstance(project_info, dict) or not isinstance(resource_info, dict):
            raise ValueError(f"all ordinal={ordinal}の元record必須field型不正")
        project_id = str(project_info.get("message_id") or "")
        resource_id = str(resource_info.get("message_id") or "")
        if not project_id or not resource_id:
            raise ValueError(f"all ordinal={ordinal}のpair message_id欠落")
        identities.append((project_id, resource_id))

        required_skills = project_info.get("required_skills", [])
        checks = record.get("required_skill_checks")
        if not isinstance(required_skills, list) or not isinstance(checks, list):
            raise ValueError(f"all ordinal={ordinal}のrequired skill構造不正")
        if len(checks) != len(required_skills):
            raise ValueError(
                f"required_skill_checks件数不一致: ordinal={ordinal} "
                f"input={len(required_skills)} output={len(checks)}"
            )
        for index, (required_skill, check) in enumerate(
            zip(required_skills, checks), 1
        ):
            normalized_skill = (
                required_skill
                if isinstance(required_skill, dict)
                else {"skill": str(required_skill)}
            )
            if not isinstance(check, dict) or check.get("skill") != skill_text(
                normalized_skill
            ):
                raise ValueError(
                    f"required_skill_checks順序/文言不一致: "
                    f"ordinal={ordinal} check={index}"
                )
            if check.get("confidence") not in {
                "confirmed",
                "human_review",
                "not_confirmed",
            }:
                raise ValueError(
                    f"required_skill_checks confidence不正: "
                    f"ordinal={ordinal} check={index}"
                )
            if not isinstance(check.get("reason"), str) or not check["reason"].strip():
                raise ValueError(
                    f"required_skill_checks reason不正: ordinal={ordinal} check={index}"
                )
        recheck_info = record.get("recheck_info")
        if not isinstance(recheck_info, dict):
            raise ValueError(f"all ordinal={ordinal}のrecheck_info型不正")
        expected_counts = {
            "required_skill_count": len(checks),
            "confirmed_count": sum(
                check.get("confidence") == "confirmed" for check in checks
            ),
            "human_review_count": sum(
                check.get("confidence") == "human_review" for check in checks
            ),
            "not_confirmed_count": sum(
                check.get("confidence") == "not_confirmed" for check in checks
            ),
        }
        for field, expected_value in expected_counts.items():
            if recheck_info.get(field) != expected_value:
                raise ValueError(
                    f"recheck_info件数不一致: ordinal={ordinal} field={field}"
                )
        expected_partitions[_result_partition(record)].append(record)

    if len(identities) != len(set(identities)):
        raise ValueError("有効対象pairの結果が重複しています")
    for name in ("confirmed", "human_review", "not_confirmed"):
        if list(rows[name]) != expected_partitions[name]:
            raise ValueError(f"分類成果物内容不一致: {name}")

    if expected_source_records is not None:
        sources = list(expected_source_records)
        if len(sources) != len(all_rows):
            raise ValueError(
                f"有効対象件数不一致: input={len(sources)} output={len(all_rows)}"
            )
        for ordinal, (source, result) in enumerate(zip(sources, all_rows), 1):
            for field, value in source.items():
                if field in {
                    "source_score_band",
                    "recheck_info",
                    "required_skill_checks",
                    "category_match",
                    "category_note",
                }:
                    continue
                if result.get(field) != value:
                    raise ValueError(
                        f"元record field保持違反: ordinal={ordinal} field={field}"
                    )

    return {name: len(list(rows[name])) for name in OUTPUT_ARTIFACT_ORDER}

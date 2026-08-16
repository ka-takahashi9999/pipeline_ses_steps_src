"""
08-1_restore_and_merge_requirement_skill_ai_matching confirm スクリプト
① merged + diff由来error = 今回diff集合（message_idペア / comparison_key の両方）
② Cache HIT分が今回runのmessage_idへrebindされている
③ Cache MISS分は今回07-1正常結果が採用されている
④ Success Cache の schema正常 / キー一意
⑤ required_skills / optional_skills / duplicate_proposal_check を確認
"""

import sys
from pathlib import Path
from typing import Dict, List, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.json_utils import count_jsonl, read_jsonl_as_list
from common.logger import get_logger
from common.success_cache import (
    SUCCESS_CACHE_PATH,
    comparison_key_from_diff_record,
    load_success_cache,
)

STEP_NAME = "08-1_restore_and_merge_requirement_skill_ai_matching_confirm"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_NEW_PAIRS = (
    project_root / "06-80_duplicate_proposal_check/01_result/duplicate_proposal_check.jsonl"
)
INPUT_DUPLICATE_PAIRS = (
    project_root
    / "06-80_duplicate_proposal_check/01_result/99_duplicate_duplicate_proposal_check.jsonl"
)
INPUT_DIFF_FILE = (
    project_root
    / "06-80_duplicate_proposal_check/01_result/duplicate_proposal_check_diff_file.jsonl"
)
INPUT_NEW_AI_RESULT = (
    project_root
    / "07-1_requirement_skill_ai_matching/01_result/requirement_skill_ai_matching.jsonl"
)

OUTPUT_RESTORED = STEP_DIR / "01_result/restored_requirement_skill_ai_matching.jsonl"
OUTPUT_MERGED = STEP_DIR / "01_result/merged_requirement_skill_ai_matching.jsonl"
OUTPUT_ERROR = STEP_DIR / "01_result/99_error_restore_requirement_skill_ai_matching.jsonl"
SUCCESS_CACHE_FILE = SUCCESS_CACHE_PATH
CONFIRM_RESULT = STEP_DIR / "02_confirm/confirm_result_restore_and_merge_requirement_skill_ai_matching.txt"

# 今回diff集合に対応するerror（merged + これ = diff）
DIFF_ERROR_TYPES = {
    "cache_hit_source_not_found",
    "new_ai_result_not_found",
    "pair_route_unknown",
}


def message_id_key(record: dict) -> Tuple[str, str]:
    return (
        record.get("project_info", {}).get("message_id", ""),
        record.get("resource_info", {}).get("message_id", ""),
    )


def read_if_exists(path: Path) -> List[dict]:
    return read_jsonl_as_list(str(path)) if path.exists() else []


def main() -> None:
    logger = get_logger(STEP_NAME)
    errors: List[str] = []
    lines = ["=== 08-1_restore_and_merge_requirement_skill_ai_matching confirm結果 ===", ""]

    new_pairs = read_if_exists(INPUT_NEW_PAIRS)
    duplicate_pairs = read_if_exists(INPUT_DUPLICATE_PAIRS)
    diff_records = read_if_exists(INPUT_DIFF_FILE)
    new_ai_results = read_if_exists(INPUT_NEW_AI_RESULT)
    merged_records = read_if_exists(OUTPUT_MERGED)
    restored_records = read_if_exists(OUTPUT_RESTORED)
    error_records = read_if_exists(OUTPUT_ERROR)

    try:
        success_cache = load_success_cache(str(SUCCESS_CACHE_FILE))
        cache_load_error = ""
    except Exception as e:
        success_cache = {}
        cache_load_error = str(e)

    diff_error_records = [
        r for r in error_records if r.get("error_type") in DIFF_ERROR_TYPES
    ]
    other_error_records = [
        r for r in error_records if r.get("error_type") not in DIFF_ERROR_TYPES
    ]

    lines += [
        f"06-80 新規件数(Cache MISS): {len(new_pairs)}",
        f"06-80 重複件数(Cache HIT) : {len(duplicate_pairs)}",
        f"06-80 diff_file件数       : {len(diff_records)}",
        f"07-1 正常結果件数         : {len(new_ai_results)}",
        f"08-1 復元件数             : {len(restored_records)}",
        f"08-1 マージ後件数         : {len(merged_records)}",
        f"08-1 error件数            : {len(error_records)}"
        f"（diff由来={len(diff_error_records)} その他={len(other_error_records)}）",
        f"Success Cache 件数        : {len(success_cache)}",
        "",
    ]

    if cache_load_error:
        msg = f"[NG] Success Cacheのschema/一意性NG: {cache_load_error}"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] Success Cache schema正常 / comparison_key一意")
        cache_line_count = sum(
            1
            for line in SUCCESS_CACHE_FILE.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ) if SUCCESS_CACHE_FILE.exists() else 0
        if cache_line_count != len(success_cache):
            msg = (
                "[NG] Success Cacheにcomparison_key重複がある: "
                f"行数={cache_line_count} 一意キー数={len(success_cache)}"
            )
            lines.append(msg)
            errors.append(msg)
        else:
            lines.append(
                f"[OK] Success Cache duplicate keyなし（行数={cache_line_count} = 一意キー数）"
            )

    # ① merged + diff由来error = diff集合
    diff_message_keys = [message_id_key(r) for r in diff_records]
    merged_message_keys = [message_id_key(r) for r in merged_records]
    diff_error_message_keys = [message_id_key(r) for r in diff_error_records]

    if len(merged_records) + len(diff_error_records) != len(diff_records):
        msg = (
            "[NG] merged + diff由来error 件数がdiff件数と不一致: "
            f"merged={len(merged_records)} error={len(diff_error_records)} diff={len(diff_records)}"
        )
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] merged件数 + diff由来error件数 = diff件数")

    if set(merged_message_keys) | set(diff_error_message_keys) != set(diff_message_keys):
        msg = "[NG] merged + error が diff集合と一致しない（message_idペア）"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] merged + error = diff集合（message_idペア）")

    if set(merged_message_keys) & set(diff_error_message_keys):
        msg = (
            "[NG] 同一ペアが merged と error に二重計上されている: "
            f"{len(set(merged_message_keys) & set(diff_error_message_keys))}件"
        )
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] merged と error のペア重複なし")

    diff_key_map: Dict[Tuple[str, str], tuple] = {
        message_id_key(r): comparison_key_from_diff_record(r) for r in diff_records
    }
    merged_comparison_keys = {
        diff_key_map[k] for k in merged_message_keys if k in diff_key_map
    }
    error_comparison_keys = {
        diff_key_map[k] for k in diff_error_message_keys if k in diff_key_map
    }
    diff_comparison_keys = set(diff_key_map.values())
    if merged_comparison_keys | error_comparison_keys != diff_comparison_keys:
        msg = "[NG] merged + error が diff集合と一致しない（comparison_key）"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] merged + error = diff集合（comparison_key）")

    # ② Cache HIT分のrebind確認
    hit_message_keys = {message_id_key(r) for r in duplicate_pairs}
    miss_message_keys = {message_id_key(r) for r in new_pairs}
    merged_hit = [r for r in merged_records if r.get("duplicate_proposal_check") is True]
    merged_miss = [r for r in merged_records if r.get("duplicate_proposal_check") is False]

    rebind_ng = 0
    cache_value_ng = 0
    for record in merged_hit:
        key = message_id_key(record)
        if key not in hit_message_keys:
            rebind_ng += 1
            continue
        comparison_key = diff_key_map.get(key)
        entry = success_cache.get(comparison_key) if comparison_key else None
        if entry is None:
            cache_value_ng += 1
            continue
        if record["project_info"].get("required_skills") != entry.get("required_skills"):
            cache_value_ng += 1
        elif record["project_info"].get("optional_skills") != entry.get("optional_skills"):
            cache_value_ng += 1

    if rebind_ng:
        msg = f"[NG] Cache HIT分のmessage_idが今回runのペアでない: {rebind_ng}件"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append(
            f"[OK] Cache HIT分は今回runのmessage_idへrebind済み（{len(merged_hit)}件）"
        )

    if cache_value_ng:
        msg = f"[NG] Cache HIT分の評価結果がSuccess Cacheと不一致: {cache_value_ng}件"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] Cache HIT分の評価結果はSuccess Cacheと一致")

    # ③ Cache MISS分は今回07-1正常結果を採用
    new_ai_map: Dict[Tuple[str, str], dict] = {
        message_id_key(r): r for r in new_ai_results
    }
    miss_ng = 0
    miss_value_ng = 0
    for record in merged_miss:
        key = message_id_key(record)
        if key not in miss_message_keys:
            miss_ng += 1
            continue
        source = new_ai_map.get(key)
        if source is None:
            miss_ng += 1
            continue
        if record["project_info"].get("required_skills") != source.get("required_skills"):
            miss_value_ng += 1
        elif record["project_info"].get("optional_skills") != source.get("optional_skills"):
            miss_value_ng += 1

    if miss_ng:
        msg = f"[NG] Cache MISS分が07-1正常結果と対応していない: {miss_ng}件"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append(f"[OK] Cache MISS分は今回07-1正常結果を採用（{len(merged_miss)}件）")

    if miss_value_ng:
        msg = f"[NG] Cache MISS分の評価結果が07-1正常結果と不一致: {miss_value_ng}件"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] Cache MISS分の評価結果は07-1正常結果と一致")

    # ④ 採用した07-1正常結果がSuccess Cacheへupsertされている
    not_upserted = 0
    for record in merged_miss:
        comparison_key = diff_key_map.get(message_id_key(record))
        if comparison_key is None or comparison_key not in success_cache:
            not_upserted += 1
    if not_upserted:
        msg = f"[NG] 今回採用した07-1正常結果がSuccess Cacheに未反映: {not_upserted}件"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] 今回07-1正常結果はSuccess Cacheへupsert済み")

    # ⑤ merged スキーマ確認
    missing_required = 0
    missing_optional = 0
    invalid_duplicate = 0
    for record in merged_records:
        project_info = record.get("project_info", {})
        if "required_skills" not in project_info:
            missing_required += 1
        if "optional_skills" not in project_info:
            missing_optional += 1
        if record.get("duplicate_proposal_check") not in (True, False):
            invalid_duplicate += 1

    if missing_required or missing_optional:
        msg = (
            "[NG] required_skills/optional_skills 欠落あり: "
            f"required={missing_required} optional={missing_optional}"
        )
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] required_skills / optional_skills は全件保持")

    if invalid_duplicate:
        msg = f"[NG] duplicate_proposal_check 不正値あり: {invalid_duplicate}件"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] duplicate_proposal_check は全件 true/false")

    if not OUTPUT_MERGED.exists() or count_jsonl(str(OUTPUT_MERGED)) <= 0:
        msg = "[NG] merged_requirement_skill_ai_matching.jsonl が存在しないか0件"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] merged_requirement_skill_ai_matching.jsonl が存在する")

    if other_error_records:
        msg = (
            "[NG] diff集合に対応しないerrorがある: "
            f"{len(other_error_records)}件 "
            f"(先頭type={other_error_records[0].get('error_type')})"
        )
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] diff集合に対応しないerrorなし")

    if diff_error_records:
        lines.append(
            f"[INFO] diff由来error {len(diff_error_records)}件"
            "（07-1 error等。次runでCache MISSとして再評価される）"
        )

    lines += ["", "【結果】NG" if errors else "【結果】OK"]

    text = "\n".join(lines)
    CONFIRM_RESULT.parent.mkdir(parents=True, exist_ok=True)
    CONFIRM_RESULT.write_text(text + "\n", encoding="utf-8")

    for line in lines:
        if "[NG]" in line or line == "【結果】NG":
            logger.error(line)
        else:
            logger.info(line)

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()

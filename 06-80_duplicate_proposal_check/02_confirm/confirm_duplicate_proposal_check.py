"""
06-80_duplicate_proposal_check confirm スクリプト
① 入力件数 = 新規件数 + 重複件数
② duplicate_proposal_check が true/false のみ
③ diff_file 件数整合性を確認
④ HITキーはSuccess Cacheに存在する
⑤ MISSキーはSuccess Cacheに存在しない
⑥ HIT + MISS = 今回pair集合（message_idペア / comparison_key の両方で確認）
"""

import sys
from pathlib import Path
from typing import Dict, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.json_utils import count_jsonl, read_jsonl_as_list
from common.logger import get_logger
from common.success_cache import (
    SUCCESS_CACHE_PATH,
    comparison_key_from_diff_record,
    is_complete_comparison_key,
    load_success_cache,
)

STEP_NAME = "06-80_duplicate_proposal_check_confirm"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS = (
    project_root
    / "06-30_match_contract_type/01_result/matched_pairs_contract_type.jsonl"
)
OUTPUT_NEW = STEP_DIR / "01_result/duplicate_proposal_check.jsonl"
OUTPUT_DUPLICATE = STEP_DIR / "01_result/99_duplicate_duplicate_proposal_check.jsonl"
OUTPUT_DIFF_FILE = STEP_DIR / "01_result/duplicate_proposal_check_diff_file.jsonl"
OUTPUT_BK_DIFF_FILE = STEP_DIR / "01_result/bk_duplicate_proposal_check_diff_file.jsonl"
SUCCESS_CACHE_FILE = SUCCESS_CACHE_PATH
CONFIRM_RESULT = STEP_DIR / "02_confirm/confirm_result_duplicate_proposal_check.txt"


def count_invalid_duplicate_flags(records: list, expected_value: bool) -> int:
    invalid = 0
    for rec in records:
        if rec.get("duplicate_proposal_check") is not expected_value:
            invalid += 1
    return invalid


def message_id_key(record: dict) -> Tuple[str, str]:
    return (
        record.get("project_info", {}).get("message_id", ""),
        record.get("resource_info", {}).get("message_id", ""),
    )


def main() -> None:
    logger = get_logger(STEP_NAME)
    errors = []
    lines = ["=== 06-80_duplicate_proposal_check confirm結果 ===", ""]

    input_count = count_jsonl(str(INPUT_PAIRS)) if INPUT_PAIRS.exists() else 0
    new_records = read_jsonl_as_list(str(OUTPUT_NEW)) if OUTPUT_NEW.exists() else []
    duplicate_records = (
        read_jsonl_as_list(str(OUTPUT_DUPLICATE)) if OUTPUT_DUPLICATE.exists() else []
    )
    diff_records = (
        read_jsonl_as_list(str(OUTPUT_DIFF_FILE)) if OUTPUT_DIFF_FILE.exists() else []
    )
    new_count = len(new_records)
    duplicate_count = len(duplicate_records)
    diff_count = len(diff_records)
    bk_diff_count = count_jsonl(str(OUTPUT_BK_DIFF_FILE)) if OUTPUT_BK_DIFF_FILE.exists() else 0

    try:
        success_cache = load_success_cache(str(SUCCESS_CACHE_FILE))
        cache_load_error = ""
    except Exception as e:
        success_cache = {}
        cache_load_error = str(e)

    lines += [
        f"入力件数                 : {input_count}",
        f"新規件数(Cache MISS)     : {new_count}",
        f"重複件数(Cache HIT)      : {duplicate_count}",
        f"今回 diff_file 件数      : {diff_count}",
        f"前回 bk_diff_file 件数   : {bk_diff_count}（監査用途 / 判定未使用）",
        f"Success Cache 件数       : {len(success_cache)}",
        "",
    ]

    if cache_load_error:
        msg = f"[NG] Success Cacheの読み込みに失敗: {cache_load_error}"
        lines.append(msg)
        errors.append(msg)

    if input_count != new_count + duplicate_count:
        msg = (
            f"[NG] 入力件数不一致: 入力={input_count} "
            f"新規+重複={new_count + duplicate_count}"
        )
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] 入力件数 = 新規件数 + 重複件数")

    if input_count != diff_count:
        msg = f"[NG] diff_file件数不一致: 入力={input_count} diff_file={diff_count}"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] 入力件数 = diff_file件数")

    invalid_new = count_invalid_duplicate_flags(new_records, expected_value=False)
    invalid_duplicate = count_invalid_duplicate_flags(duplicate_records, expected_value=True)

    if invalid_new or invalid_duplicate:
        msg = (
            "[NG] duplicate_proposal_check 不正値あり: "
            f"新規側={invalid_new}件 重複側={invalid_duplicate}件"
        )
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] duplicate_proposal_check は新規=false / 重複=true")

    incomplete_key_count = sum(
        1
        for record in diff_records
        if not is_complete_comparison_key(comparison_key_from_diff_record(record))
    )
    if incomplete_key_count:
        msg = (
            f"[NG] comparison_key空件数 = {incomplete_key_count}"
            "（06-80はfail-fastするため0件でなければならない）"
        )
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] comparison_key空件数 = 0")

    # diff_file から message_id ペア -> comparison_key を引く
    diff_key_map: Dict[Tuple[str, str], tuple] = {}
    for record in diff_records:
        diff_key_map[message_id_key(record)] = comparison_key_from_diff_record(record)

    hit_keys = [diff_key_map.get(message_id_key(r)) for r in duplicate_records]
    miss_keys = [diff_key_map.get(message_id_key(r)) for r in new_records]

    unmapped = sum(1 for k in hit_keys + miss_keys if k is None)
    if unmapped:
        msg = f"[NG] diff_fileにcomparison_keyが見つからないペア: {unmapped}件"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] 新規/重複の全ペアが diff_file の comparison_key と対応")

    hit_not_in_cache = sum(1 for k in hit_keys if k is not None and k not in success_cache)
    miss_in_cache = sum(1 for k in miss_keys if k is not None and k in success_cache)

    if hit_not_in_cache:
        msg = f"[NG] HITキーがSuccess Cacheに存在しない: {hit_not_in_cache}件"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] HITキーはすべてSuccess Cacheに存在する")

    if miss_in_cache:
        msg = f"[NG] MISSキーがSuccess Cacheに存在する: {miss_in_cache}件"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] MISSキーはSuccess Cacheに存在しない")

    diff_message_keys = {message_id_key(r) for r in diff_records}
    hit_message_keys = {message_id_key(r) for r in duplicate_records}
    miss_message_keys = {message_id_key(r) for r in new_records}

    if hit_message_keys | miss_message_keys != diff_message_keys:
        msg = (
            "[NG] HIT + MISS が今回pair集合と一致しない(message_idペア): "
            f"HIT={len(hit_message_keys)} MISS={len(miss_message_keys)} diff={len(diff_message_keys)}"
        )
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] HIT + MISS = 今回pair集合（message_idペア）")

    diff_comparison_keys = {comparison_key_from_diff_record(r) for r in diff_records}
    union_comparison_keys = {k for k in hit_keys + miss_keys if k is not None}
    if union_comparison_keys != diff_comparison_keys:
        msg = (
            "[NG] HIT + MISS が今回pair集合と一致しない(comparison_key): "
            f"union={len(union_comparison_keys)} diff={len(diff_comparison_keys)}"
        )
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] HIT + MISS = 今回pair集合（comparison_key）")

    lines += [
        "",
        "【結果】NG" if errors else "【結果】OK",
    ]

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

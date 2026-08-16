"""
08-1_restore_and_merge_requirement_skill_ai_matching
Cache HIT分は Success Cache から評価結果を復元して今回message_idへrebindし、
Cache MISS分は今回07-1正常結果を採用して、今回diff順にmergeする。
処理後、今回07-1正常結果だけを Success Cache へ upsert する（部分errorでも成功分は反映）。

旧 bk_merged_requirement_skill_ai_matching.jsonl は legacy HOLD（read/更新/削除しない）。
"""

import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, List, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger
from common.success_cache import (
    SUCCESS_CACHE_PATH,
    build_cache_entry,
    comparison_key_from_diff_record,
    comparison_key_to_dict,
    format_comparison_key,
    load_success_cache,
    upsert_success_cache,
)

STEP_NAME = "08-1_restore_and_merge_requirement_skill_ai_matching"
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
SUCCESS_CACHE_FILE = SUCCESS_CACHE_PATH

OUTPUT_RESTORED = STEP_DIR / "01_result/restored_requirement_skill_ai_matching.jsonl"
OUTPUT_MERGED = STEP_DIR / "01_result/merged_requirement_skill_ai_matching.jsonl"
OUTPUT_ERROR = STEP_DIR / "01_result/99_error_restore_requirement_skill_ai_matching.jsonl"
DIAGNOSTICS_OUTPUT = (
    STEP_DIR / "02_confirm/diagnostics_restore_and_merge_requirement_skill_ai_matching.txt"
)

MessageIdPair = Tuple[str, str]
ComparisonKey = Tuple[str, str, str, str]


def build_message_id_key(record: dict) -> MessageIdPair:
    return (
        record.get("project_info", {}).get("message_id", ""),
        record.get("resource_info", {}).get("message_id", ""),
    )


def normalize_new_ai_result(record: dict) -> Tuple[dict, List[str]]:
    """07-1正常結果を merged スキーマへ正規化する。"""
    project_info = record.get("project_info") or {}
    resource_info = record.get("resource_info") or {}
    project_message_id = project_info.get("message_id", "")
    resource_message_id = resource_info.get("message_id", "")
    required_skills = record.get("required_skills", project_info.get("required_skills"))
    optional_skills = record.get("optional_skills", project_info.get("optional_skills"))

    errors: List[str] = []
    if not project_message_id:
        errors.append("project_info.message_id がない")
    if not resource_message_id:
        errors.append("resource_info.message_id がない")
    if required_skills is None:
        errors.append("required_skills がない")
    if optional_skills is None:
        errors.append("optional_skills がない")

    normalized = {
        "project_info": {
            "message_id": project_message_id,
            "required_skills": required_skills if required_skills is not None else [],
            "optional_skills": optional_skills if optional_skills is not None else [],
        },
        "resource_info": {
            "message_id": resource_message_id,
        },
        "duplicate_proposal_check": False,
    }
    if "evaluation_meta" in record:
        normalized["evaluation_meta"] = record["evaluation_meta"]

    return normalized, errors


def build_restored_record(cache_entry: dict, message_key: MessageIdPair) -> dict:
    """Cache HIT の評価結果を今回runのmessage_idへrebindしたmergedレコードを作る。"""
    record = {
        "project_info": {
            "message_id": message_key[0],
            "required_skills": cache_entry.get("required_skills", []),
            "optional_skills": cache_entry.get("optional_skills", []),
        },
        "resource_info": {
            "message_id": message_key[1],
        },
        "duplicate_proposal_check": True,
    }
    if "evaluation_meta" in cache_entry:
        record["evaluation_meta"] = cache_entry["evaluation_meta"]
    return record


def build_error_record(
    project_message_id: str,
    resource_message_id: str,
    duplicate_flag,
    error_type: str,
    error_message: str,
    compare_key: ComparisonKey,
) -> dict:
    return {
        "project_info": {"message_id": project_message_id},
        "resource_info": {"message_id": resource_message_id},
        "duplicate_proposal_check": duplicate_flag,
        "compare_key": comparison_key_to_dict(compare_key),
        "error_type": error_type,
        "error_message": error_message,
    }


def write_restore_diagnostics(
    diff_total: int,
    hit_pair_count: int,
    miss_pair_count: int,
    restored_count: int,
    new_used_count: int,
    merged_count: int,
    diff_error_count: int,
    other_error_count: int,
    cache_count_before: int,
    cache_stats: Dict[str, int],
    unresolved_items: List[dict],
) -> None:
    lines = [
        "=== 08-1_restore_and_merge_requirement_skill_ai_matching diagnostics ===",
        "",
        f"今回 diff_file 件数              : {diff_total}",
        f"Cache HIT(06-80重複) 件数        : {hit_pair_count}",
        f"Cache MISS(06-80新規) 件数       : {miss_pair_count}",
        f"Success Cacheから復元した件数    : {restored_count}",
        f"07-1正常結果を採用した件数       : {new_used_count}",
        f"merged 件数                      : {merged_count}",
        f"diff由来 error 件数              : {diff_error_count}",
        f"その他 error 件数                : {other_error_count}",
        f"merged + diff由来error           : {merged_count + diff_error_count}",
        f"実行前 Success Cache 件数        : {cache_count_before}",
        f"upsert後 Success Cache 件数      : {cache_stats.get('after_count', cache_count_before)}",
        f"upsert 追加 / 更新               : "
        f"{cache_stats.get('inserted', 0)} / {cache_stats.get('updated', 0)}",
        "",
        "解決できなかった先頭10件:",
    ]
    if not unresolved_items:
        lines.append("- なし")
    for item in unresolved_items[:10]:
        lines.append(
            f"- error_type={item['error_type']} | "
            f"message_id_pair={item['message_key'][0]} / {item['message_key'][1]} | "
            f"{format_comparison_key(item['compare_key'])}"
        )

    DIAGNOSTICS_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    DIAGNOSTICS_OUTPUT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    try:
        duplicate_pairs = read_jsonl_as_list(str(INPUT_DUPLICATE_PAIRS))
        new_pairs = read_jsonl_as_list(str(INPUT_NEW_PAIRS))
        new_ai_results = read_jsonl_as_list(str(INPUT_NEW_AI_RESULT))
        diff_records = read_jsonl_as_list(str(INPUT_DIFF_FILE))

        logger.info(
            "入力件数: "
            f"新規ペア={len(new_pairs)} 重複ペア={len(duplicate_pairs)} "
            f"07-1新規結果={len(new_ai_results)} diff_file={len(diff_records)}"
        )

        # Success Cache（不整合時は例外で停止）
        success_cache = load_success_cache(str(SUCCESS_CACHE_FILE))
        cache_count_before = len(success_cache)
        logger.info(f"Success Cache件数={cache_count_before}")

        errors: List[dict] = []
        unresolved_items: List[dict] = []

        # 06-80の仕分け結果（HIT/MISS）を message_id ペアで引く
        hit_message_keys = {build_message_id_key(pair) for pair in duplicate_pairs}
        miss_message_keys = {build_message_id_key(pair) for pair in new_pairs}

        # 07-1正常結果を message_id ペアで引けるようにする
        new_result_queues: DefaultDict[MessageIdPair, List[dict]] = defaultdict(list)
        for record in new_ai_results:
            normalized, normalize_errors = normalize_new_ai_result(record)
            message_key = build_message_id_key(normalized)
            if normalize_errors:
                errors.append(
                    build_error_record(
                        message_key[0],
                        message_key[1],
                        False,
                        "invalid_new_ai_result",
                        " / ".join(normalize_errors),
                        ("", "", "", ""),
                    )
                )
                continue
            new_result_queues[message_key].append(normalized)

        merged_records: List[dict] = []
        restored_records: List[dict] = []
        cache_upsert_entries: List[dict] = []
        diff_error_count = 0
        new_used_count = 0

        for diff_record in diff_records:
            message_key = build_message_id_key(diff_record)
            comparison_key = comparison_key_from_diff_record(diff_record)

            if message_key in hit_message_keys:
                cache_entry = success_cache.get(comparison_key)
                if cache_entry is None:
                    diff_error_count += 1
                    errors.append(
                        build_error_record(
                            message_key[0],
                            message_key[1],
                            True,
                            "cache_hit_source_not_found",
                            "06-80が重複判定したがSuccess Cacheに該当comparison_keyがない",
                            comparison_key,
                        )
                    )
                    unresolved_items.append(
                        {
                            "error_type": "cache_hit_source_not_found",
                            "message_key": message_key,
                            "compare_key": comparison_key,
                        }
                    )
                    continue

                restored = build_restored_record(cache_entry, message_key)
                merged_records.append(restored)

                audit_record = dict(restored)
                audit_record["restore_key_type"] = "success_cache_comparison_key"
                audit_record["restore_source_message_ids"] = cache_entry.get(
                    "source_message_ids", {}
                )
                restored_records.append(audit_record)
                continue

            if message_key not in miss_message_keys:
                diff_error_count += 1
                errors.append(
                    build_error_record(
                        message_key[0],
                        message_key[1],
                        None,
                        "pair_route_unknown",
                        "diff_fileのペアが06-80の新規/重複どちらにも存在しない",
                        comparison_key,
                    )
                )
                unresolved_items.append(
                    {
                        "error_type": "pair_route_unknown",
                        "message_key": message_key,
                        "compare_key": comparison_key,
                    }
                )
                continue

            queue = new_result_queues.get(message_key)
            if not queue:
                diff_error_count += 1
                errors.append(
                    build_error_record(
                        message_key[0],
                        message_key[1],
                        False,
                        "new_ai_result_not_found",
                        "Cache MISSペアに対応する07-1正常結果が存在しない（07-1 error等）",
                        comparison_key,
                    )
                )
                unresolved_items.append(
                    {
                        "error_type": "new_ai_result_not_found",
                        "message_key": message_key,
                        "compare_key": comparison_key,
                    }
                )
                continue

            normalized = queue.pop(0)
            merged_records.append(normalized)
            new_used_count += 1

            cache_upsert_entries.append(
                build_cache_entry(
                    comparison_key,
                    message_key[0],
                    message_key[1],
                    normalized["project_info"]["required_skills"],
                    normalized["project_info"]["optional_skills"],
                    normalized.get("evaluation_meta", {}),
                )
            )

        for message_key, queue in new_result_queues.items():
            for _ in queue:
                errors.append(
                    build_error_record(
                        message_key[0],
                        message_key[1],
                        False,
                        "unused_new_ai_result",
                        "07-1 正常結果が今回diff_fileに対応付けできなかった",
                        ("", "", "", ""),
                    )
                )

        write_jsonl(str(OUTPUT_RESTORED), restored_records)
        write_jsonl(str(OUTPUT_MERGED), merged_records)
        write_jsonl(str(OUTPUT_ERROR), errors)

        diff_total = len(diff_records)
        merged_total = len(merged_records)
        restored_total = len(restored_records)
        error_total = len(errors)
        other_error_count = error_total - diff_error_count

        logger.info(
            "集計: "
            f"復元={restored_total} 07-1採用={new_used_count} merged={merged_total} "
            f"error={error_total}（うちdiff由来={diff_error_count}） diff_file総件数={diff_total}"
        )

        if merged_total <= 0:
            raise RuntimeError(
                "全件完成版の整合性エラー: merged が 0 件のため後続stepへ渡せません "
                f"(diff_file総件数={diff_total} errors={error_total})"
            )

        if merged_total + diff_error_count != diff_total:
            raise RuntimeError(
                "全件完成版の整合性エラー: merged + diff由来error が diff_file総件数と一致しません "
                f"(merged={merged_total} diff由来error={diff_error_count} diff_file総件数={diff_total})"
            )

        if error_total > 0:
            logger.warn(
                "一部エラーを 99_error に退避して続行: "
                f"merged件数={merged_total} error件数={error_total}"
            )

        # 部分errorでも今回07-1正常結果だけはSuccess Cacheへupsertする（自己回復型）
        cache_stats: Dict[str, int] = {}
        if cache_upsert_entries:
            cache_stats = upsert_success_cache(
                str(SUCCESS_CACHE_FILE), cache_upsert_entries
            )
            logger.info(
                "Success Cache upsert: "
                f"対象={len(cache_upsert_entries)} 追加={cache_stats['inserted']} "
                f"更新={cache_stats['updated']} "
                f"件数 {cache_stats['before_count']} -> {cache_stats['after_count']}"
            )
        else:
            logger.warn("今回07-1正常結果が0件のためSuccess Cache upsertをスキップ")

        write_restore_diagnostics(
            diff_total,
            len(hit_message_keys),
            len(miss_message_keys),
            restored_total,
            new_used_count,
            merged_total,
            diff_error_count,
            other_error_count,
            cache_count_before,
            cache_stats,
            unresolved_items,
        )

        elapsed = time.time() - start_time
        write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, merged_total)
        logger.ok(
            "処理完了: "
            f"復元={restored_total} 07-1採用={new_used_count} merged={merged_total} "
            f"errors={error_total} diff_file総件数={diff_total}"
        )

    except Exception as e:
        write_error_log(str(dirs["result"]), e, context=STEP_NAME)
        logger.error(f"処理失敗: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

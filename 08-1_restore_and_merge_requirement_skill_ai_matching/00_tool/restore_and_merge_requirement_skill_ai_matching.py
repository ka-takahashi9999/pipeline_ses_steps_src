"""
08-1_restore_and_merge_requirement_skill_ai_matching
重複ペアを前回完成版から復元し、07-1 の新規評価結果とマージして全件完成版を作る。
"""

import shutil
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, List, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

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
INPUT_MAIL_MASTER = (
    project_root / "01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl"
)
BACKUP_COMPLETED_RESULT = (
    STEP_DIR / "01_result/bk_merged_requirement_skill_ai_matching.jsonl"
)

OUTPUT_RESTORED = STEP_DIR / "01_result/restored_requirement_skill_ai_matching.jsonl"
OUTPUT_MERGED = STEP_DIR / "01_result/merged_requirement_skill_ai_matching.jsonl"
OUTPUT_ERROR = STEP_DIR / "01_result/99_error_restore_requirement_skill_ai_matching.jsonl"
DIAGNOSTICS_OUTPUT = STEP_DIR / "02_confirm/confirm_result_restore_and_merge_requirement_skill_ai_matching.txt"


def build_compare_key_from_diff_record(record: dict) -> Tuple[str, str, str, str]:
    return (
        record.get("project_info", {}).get("from", ""),
        record.get("project_info", {}).get("subject", ""),
        record.get("resource_info", {}).get("from", ""),
        record.get("resource_info", {}).get("subject", ""),
    )


def build_compare_key_from_message_ids(
    project_message_id: str,
    resource_message_id: str,
    mail_master: Dict[str, dict],
) -> Tuple[str, str, str, str]:
    project_mail = mail_master.get(project_message_id, {})
    resource_mail = mail_master.get(resource_message_id, {})
    return (
        project_mail.get("from", ""),
        project_mail.get("subject", ""),
        resource_mail.get("from", ""),
        resource_mail.get("subject", ""),
    )


def build_compare_key_from_pair(pair: dict, mail_master: Dict[str, dict]) -> Tuple[str, str, str, str]:
    return build_compare_key_from_message_ids(
        pair.get("project_info", {}).get("message_id", ""),
        pair.get("resource_info", {}).get("message_id", ""),
        mail_master,
    )


def build_message_id_key(record: dict) -> Tuple[str, str]:
    return (
        record.get("project_info", {}).get("message_id", ""),
        record.get("resource_info", {}).get("message_id", ""),
    )


def normalize_completed_record(
    record: dict,
    duplicate_flag: bool,
) -> Tuple[dict, List[str]]:
    project_info = record.get("project_info") or {}
    resource_info = record.get("resource_info") or {}
    project_message_id = project_info.get("message_id", "")
    resource_message_id = resource_info.get("message_id", "")
    required_skills = project_info.get("required_skills", record.get("required_skills"))
    optional_skills = project_info.get("optional_skills", record.get("optional_skills"))

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
        "duplicate_proposal_check": duplicate_flag,
    }

    if "evaluation_meta" in record:
        normalized["evaluation_meta"] = record["evaluation_meta"]

    return normalized, errors


def build_error_record(
    pair: dict,
    error_type: str,
    error_message: str,
    compare_key: Tuple[str, str, str, str],
) -> dict:
    return {
        "project_info": {
            "message_id": pair.get("project_info", {}).get("message_id", ""),
        },
        "resource_info": {
            "message_id": pair.get("resource_info", {}).get("message_id", ""),
        },
        "duplicate_proposal_check": pair.get("duplicate_proposal_check"),
        "compare_key": {
            "project_from": compare_key[0],
            "project_subject": compare_key[1],
            "resource_from": compare_key[2],
            "resource_subject": compare_key[3],
        },
        "error_type": error_type,
        "error_message": error_message,
    }


def append_queue(
    queue_map: DefaultDict[Tuple[str, str, str, str], List[dict]],
    key: Tuple[str, str, str, str],
    record: dict,
) -> None:
    queue_map[key].append(record)


def append_message_queue(
    queue_map: DefaultDict[Tuple[str, str], List[dict]],
    key: Tuple[str, str],
    record: dict,
) -> None:
    queue_map[key].append(record)


def update_backup_safely(merged_file: Path, backup_file: Path, logger) -> None:
    tmp_file = backup_file.with_suffix(".jsonl.tmp")
    shutil.copy2(str(merged_file), str(tmp_file))
    tmp_file.replace(backup_file)
    logger.info("次回用バックアップを merged 完成版で更新")


def format_compare_key(compare_key: Tuple[str, str, str, str]) -> str:
    return (
        f"project_from={compare_key[0]} / project_subject={compare_key[1]} / "
        f"resource_from={compare_key[2]} / resource_subject={compare_key[3]}"
    )


def write_restore_diagnostics(
    duplicate_pairs: List[dict],
    restored_records: List[dict],
    errors: List[dict],
    backup_record_count: int,
    duplicate_message_key_hit_count: int,
    duplicate_compare_key_hit_count: int,
    unresolved_restore_items: List[dict],
) -> None:
    restore_source_not_found_count = sum(
        1 for error in errors if error.get("error_type") == "restore_source_not_found"
    )
    if len(duplicate_pairs) == len(restored_records):
        verdict = "復元対象はすべて復元済みです。"
    elif duplicate_message_key_hit_count == 0 and duplicate_compare_key_hit_count == 0:
        verdict = "復元元バックアップに対象キーが存在しないため復元不可です。"
    else:
        verdict = "復元元バックアップに一部キーが存在します。未復元分は99_errorの具体理由を確認してください。"

    lines = [
        "=== 08-1_restore_and_merge_requirement_skill_ai_matching diagnostics ===",
        "",
        f"duplicate_pairs 件数: {len(duplicate_pairs)}",
        f"restored_records 件数: {len(restored_records)}",
        f"restore_source_not_found 件数: {restore_source_not_found_count}",
        f"bk_merged_requirement_skill_ai_matching.jsonl 件数: {backup_record_count}",
        f"duplicate_pairs の message_id_pair が bk_merged に存在した件数: {duplicate_message_key_hit_count}",
        f"duplicate_pairs の from/subject compare_key が bk_merged に存在した件数: {duplicate_compare_key_hit_count}",
        f"判定: {verdict}",
        "",
        "復元できなかった先頭10件:",
    ]
    if not unresolved_restore_items:
        lines.append("- なし")
    for item in unresolved_restore_items[:10]:
        message_key = item["message_key"]
        compare_key = item["compare_key"]
        lines.append(
            f"- message_id_pair={message_key[0]} / {message_key[1]} | "
            f"message_id_pair_in_bk={item['message_key_exists']} | "
            f"compare_key_in_bk={item['compare_key_exists']} | "
            f"{format_compare_key(compare_key)}"
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
        mail_master = read_jsonl_as_dict(str(INPUT_MAIL_MASTER), key="message_id")

        logger.info(
            "入力件数: "
            f"新規ペア={len(new_pairs)} 重複ペア={len(duplicate_pairs)} "
            f"07-1新規結果={len(new_ai_results)} diff_file={len(diff_records)}"
        )

        errors: List[dict] = []
        restored_records: List[dict] = []

        new_result_queues: DefaultDict[Tuple[str, str, str, str], List[dict]] = defaultdict(list)
        for record in new_ai_results:
            normalized, normalize_errors = normalize_completed_record(record, duplicate_flag=False)
            key = build_compare_key_from_message_ids(
                normalized["project_info"]["message_id"],
                normalized["resource_info"]["message_id"],
                mail_master,
            )
            if normalize_errors:
                errors.append(
                    build_error_record(
                        normalized,
                        "invalid_new_ai_result",
                        " / ".join(normalize_errors),
                        key,
                    )
                )
                continue
            append_queue(new_result_queues, key, normalized)

        previous_completed_queues: DefaultDict[Tuple[str, str, str, str], List[dict]] = defaultdict(list)
        previous_completed_message_queues: DefaultDict[Tuple[str, str], List[dict]] = defaultdict(list)
        previous_completed_message_keys = set()
        previous_completed_compare_keys = set()
        previous_completed_records_count = 0
        if BACKUP_COMPLETED_RESULT.exists():
            previous_completed_records = read_jsonl_as_list(str(BACKUP_COMPLETED_RESULT))
            previous_completed_records_count = len(previous_completed_records)
            logger.info(f"前回完成版バックアップ件数={len(previous_completed_records)}")
            for record in previous_completed_records:
                normalized, normalize_errors = normalize_completed_record(record, duplicate_flag=True)
                message_key = build_message_id_key(normalized)
                key = build_compare_key_from_message_ids(
                    normalized["project_info"]["message_id"],
                    normalized["resource_info"]["message_id"],
                    mail_master,
                )
                if normalize_errors:
                    errors.append(
                        build_error_record(
                            normalized,
                            "invalid_backup_completed_record",
                            " / ".join(normalize_errors),
                            key,
                        )
                    )
                    continue
                append_queue(previous_completed_queues, key, normalized)
                append_message_queue(previous_completed_message_queues, message_key, normalized)
                previous_completed_message_keys.add(message_key)
                previous_completed_compare_keys.add(key)
        else:
            logger.warn("前回完成版バックアップが存在しません")

        duplicate_message_key_hit_count = 0
        duplicate_compare_key_hit_count = 0
        unresolved_restore_items: List[dict] = []
        for pair in duplicate_pairs:
            message_key = build_message_id_key(pair)
            compare_key = build_compare_key_from_pair(pair, mail_master)
            message_key_exists = message_key in previous_completed_message_keys
            compare_key_exists = compare_key in previous_completed_compare_keys
            if message_key_exists:
                duplicate_message_key_hit_count += 1
            if compare_key_exists:
                duplicate_compare_key_hit_count += 1
            queue = previous_completed_message_queues.get(message_key, [])
            restore_key_type = "message_id_pair"
            if not queue:
                queue = previous_completed_queues.get(compare_key, [])
                restore_key_type = "from_subject_compare_key"
            if not queue:
                reason = (
                    "前回完成版バックアップに一致キーが存在しない "
                    f"(message_id_pair={message_key[0]} / {message_key[1]} は"
                    f"{'存在' if message_key in previous_completed_message_keys else '未存在'}, "
                    "from/subject比較キーも未一致)"
                )
                if not BACKUP_COMPLETED_RESULT.exists():
                    reason = "前回完成版バックアップが存在しないため復元不可"
                unresolved_restore_items.append(
                    {
                        "message_key": message_key,
                        "compare_key": compare_key,
                        "message_key_exists": message_key_exists,
                        "compare_key_exists": compare_key_exists,
                    }
                )
                errors.append(
                    build_error_record(
                        pair,
                        "restore_source_not_found",
                        reason,
                        compare_key,
                    )
                )
                continue

            restored = dict(queue.pop(0))
            if restore_key_type == "message_id_pair":
                compare_queue = previous_completed_queues.get(compare_key, [])
                for index, candidate in enumerate(compare_queue):
                    if build_message_id_key(candidate) == message_key:
                        compare_queue.pop(index)
                        break
            restored["duplicate_proposal_check"] = True
            restored["restore_key_type"] = restore_key_type
            restored_records.append(restored)

        merged_records: List[dict] = []
        restored_queue_for_merge: DefaultDict[Tuple[str, str, str, str], List[dict]] = defaultdict(list)
        for record in restored_records:
            append_queue(
                restored_queue_for_merge,
                build_compare_key_from_message_ids(
                    record["project_info"]["message_id"],
                    record["resource_info"]["message_id"],
                    mail_master,
                ),
                record,
            )

        for diff_record in diff_records:
            key = build_compare_key_from_diff_record(diff_record)

            if restored_queue_for_merge.get(key):
                merged_records.append(restored_queue_for_merge[key].pop(0))
                continue

            if new_result_queues.get(key):
                merged_records.append(new_result_queues[key].pop(0))
                continue

            errors.append(
                build_error_record(
                    diff_record,
                    "merge_source_not_found",
                    "今回 diff_file のキーに対応する新規結果/復元結果が存在しない",
                    key,
                )
            )

        for key, queue in new_result_queues.items():
            for record in queue:
                errors.append(
                    build_error_record(
                        record,
                        "unused_new_ai_result",
                        "07-1 新規結果が diff_file に対応付けできなかった",
                        key,
                    )
                )

        for key, queue in restored_queue_for_merge.items():
            for record in queue:
                errors.append(
                    build_error_record(
                        record,
                        "unused_restored_result",
                        "復元結果が diff_file に対応付けできなかった",
                        key,
                    )
                )

        write_jsonl(str(OUTPUT_RESTORED), restored_records)
        write_jsonl(str(OUTPUT_MERGED), merged_records)
        write_jsonl(str(OUTPUT_ERROR), errors)
        write_restore_diagnostics(
            duplicate_pairs,
            restored_records,
            errors,
            previous_completed_records_count,
            duplicate_message_key_hit_count,
            duplicate_compare_key_hit_count,
            unresolved_restore_items,
        )

        expected_total = len(new_pairs) + len(duplicate_pairs)
        diff_total = len(diff_records)
        merged_total = len(merged_records)
        restored_total = len(restored_records)
        error_total = len(errors)
        diff_gap = diff_total - merged_total

        logger.info(
            "集計: "
            f"新規結果件数={len(new_ai_results)} "
            f"復元件数={restored_total} "
            f"merged件数={merged_total} "
            f"error件数={error_total} "
            f"diff_file総件数={diff_total} "
            f"差分件数={diff_gap}"
        )

        if merged_total <= 0:
            raise RuntimeError(
                "全件完成版の整合性エラー: merged が 0 件のため後続stepへ渡せません "
                f"(期待件数={expected_total} diff_file総件数={diff_total} errors={error_total})"
            )

        if diff_gap != 0 and error_total == 0:
            raise RuntimeError(
                "全件完成版の整合性エラー: diff_file総件数との差分があるのに error が 0 件です "
                f"(期待件数={expected_total} diff_file総件数={diff_total} "
                f"merged={merged_total} restored={restored_total} errors={error_total})"
            )

        if error_total > 0:
            logger.warn(
                "一部エラーを 99_error に退避して続行: "
                f"merged件数={merged_total} error件数={error_total} 差分件数={diff_gap}"
            )

        if merged_total > 0 and error_total == 0:
            update_backup_safely(OUTPUT_MERGED, BACKUP_COMPLETED_RESULT, logger)
        elif error_total > 0:
            logger.warn("error が存在するため次回用バックアップ更新をスキップ")
        else:
            logger.warn("merged_requirement_skill_ai_matching.jsonl が 0 件のためバックアップ更新をスキップ")

        elapsed = time.time() - start_time
        write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, merged_total)
        logger.ok(
            "処理完了: "
            f"新規結果={len(new_ai_results)} 復元={restored_total} "
            f"merged={merged_total} errors={error_total} "
            f"diff_file総件数={diff_total} 差分件数={diff_gap}"
        )

    except Exception as e:
        write_error_log(str(dirs["result"]), e, context=STEP_NAME)
        logger.error(f"処理失敗: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

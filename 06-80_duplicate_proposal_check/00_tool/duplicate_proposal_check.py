"""
06-80_duplicate_proposal_check
06-30 通過ペアの comparison_key を Success Cache と照合し、新規(Cache MISS)/重複(Cache HIT)に仕分けする。

重複判定の正本は Success Cache（再利用可能な07-1成功評価結果）であり、前回diffではない。
前回diff（bk_diff_file）は監査・確認用途としてのみ残す。
"""

import shutil
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger
from common.success_cache import (
    SUCCESS_CACHE_PATH,
    comparison_key_from_diff_record,
    is_complete_comparison_key,
    load_success_cache,
)

STEP_NAME = "06-80_duplicate_proposal_check"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS = (
    project_root
    / "06-30_match_contract_type/01_result/matched_pairs_contract_type.jsonl"
)
INPUT_MAIL_MASTER = (
    project_root / "01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl"
)
INPUT_SUCCESS_CACHE = SUCCESS_CACHE_PATH

OUTPUT_NEW = STEP_DIR / "01_result/duplicate_proposal_check.jsonl"
OUTPUT_DUPLICATE = STEP_DIR / "01_result/99_duplicate_duplicate_proposal_check.jsonl"
OUTPUT_DIFF_FILE = STEP_DIR / "01_result/duplicate_proposal_check_diff_file.jsonl"
OUTPUT_BK_DIFF_FILE = STEP_DIR / "01_result/bk_duplicate_proposal_check_diff_file.jsonl"


def build_compare_key_record(pair: dict, mail_master: Dict[str, dict]) -> dict:
    project_mid = pair.get("project_info", {}).get("message_id", "")
    resource_mid = pair.get("resource_info", {}).get("message_id", "")

    project_mail = mail_master.get(project_mid, {})
    resource_mail = mail_master.get(resource_mid, {})

    return {
        "project_info": {
            "message_id": project_mid,
            "from": project_mail.get("from", ""),
            "subject": project_mail.get("subject", ""),
        },
        "resource_info": {
            "message_id": resource_mid,
            "from": resource_mail.get("from", ""),
            "subject": resource_mail.get("subject", ""),
        },
    }


def build_compare_key(diff_record: dict) -> Tuple[str, str, str, str]:
    return comparison_key_from_diff_record(diff_record)


def main() -> None:
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    try:
        if OUTPUT_DIFF_FILE.exists():
            shutil.move(str(OUTPUT_DIFF_FILE), str(OUTPUT_BK_DIFF_FILE))
            logger.info("前回 diff_file を bk_diff_file に退避（監査用途のみ）")
        else:
            OUTPUT_BK_DIFF_FILE.parent.mkdir(parents=True, exist_ok=True)
            OUTPUT_BK_DIFF_FILE.write_text("", encoding="utf-8")
            logger.info("初回実行: 空の bk_diff_file を作成")

        pairs = read_jsonl_as_list(str(INPUT_PAIRS))
        mail_master = read_jsonl_as_dict(str(INPUT_MAIL_MASTER), key="message_id")
        logger.info(f"入力ペア数={len(pairs)} メールマスタ件数={len(mail_master)}")

        diff_records = [build_compare_key_record(pair, mail_master) for pair in pairs]
        write_jsonl(str(OUTPUT_DIFF_FILE), diff_records)
        logger.info(f"今回 diff_file 出力={len(diff_records)}件")

        # 重複判定の正本: Success Cache（read-only）
        success_cache = load_success_cache(str(INPUT_SUCCESS_CACHE))
        if not success_cache:
            logger.warn(
                f"Success Cacheが空または未存在のため全件をCache MISSとして扱う: {INPUT_SUCCESS_CACHE}"
            )
        else:
            logger.info(f"Success Cache件数={len(success_cache)}")

        # 監査用途: 前回diffとの重なり件数（判定には使用しない）
        previous_diff_records = read_jsonl_as_list(str(OUTPUT_BK_DIFF_FILE))
        previous_key_set = {build_compare_key(record) for record in previous_diff_records}
        logger.info(f"前回 bk_diff_file 件数={len(previous_diff_records)}（監査用途）")

        new_records: List[dict] = []
        duplicate_records: List[dict] = []
        incomplete_key_count = 0
        previous_diff_overlap_count = 0

        for pair, diff_record in zip(pairs, diff_records):
            record = dict(pair)
            comparison_key = build_compare_key(diff_record)

            if not is_complete_comparison_key(comparison_key):
                # 空値を含むキーはcacheに存在し得ないため必ずCache MISS。件数のみ記録して新規扱い。
                incomplete_key_count += 1

            if comparison_key in previous_key_set:
                previous_diff_overlap_count += 1

            is_duplicate = comparison_key in success_cache
            record["duplicate_proposal_check"] = is_duplicate

            if is_duplicate:
                duplicate_records.append(record)
            else:
                new_records.append(record)

        write_jsonl(str(OUTPUT_NEW), new_records)
        write_jsonl(str(OUTPUT_DUPLICATE), duplicate_records)

        logger.info(
            "判定内訳: "
            f"Cache HIT(重複)={len(duplicate_records)} Cache MISS(新規)={len(new_records)} "
            f"comparison_key空値あり={incomplete_key_count} "
            f"前回diffにも存在={previous_diff_overlap_count}（監査用途 / 判定未使用）"
        )

        elapsed = time.time() - start_time
        write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, len(pairs))
        logger.ok(
            "処理完了: "
            f"入力={len(pairs)} 新規={len(new_records)} 重複={len(duplicate_records)}"
        )

    except Exception as e:
        write_error_log(str(dirs["result"]), e, context=STEP_NAME)
        logger.error(f"処理失敗: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

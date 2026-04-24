"""
06-80_duplicate_proposal_check
06-30 通過ペアを前回比較キーと照合し、新規/重複に仕分けする。
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

STEP_NAME = "06-80_duplicate_proposal_check"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS = (
    project_root
    / "06-30_match_contract_type/01_result/matched_pairs_contract_type.jsonl"
)
INPUT_MAIL_MASTER = (
    project_root / "01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl"
)

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
    return (
        diff_record.get("project_info", {}).get("from", ""),
        diff_record.get("project_info", {}).get("subject", ""),
        diff_record.get("resource_info", {}).get("from", ""),
        diff_record.get("resource_info", {}).get("subject", ""),
    )


def main() -> None:
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    try:
        if OUTPUT_DIFF_FILE.exists():
            shutil.move(str(OUTPUT_DIFF_FILE), str(OUTPUT_BK_DIFF_FILE))
            logger.info("前回 diff_file を bk_diff_file に退避")
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

        previous_diff_records = read_jsonl_as_list(str(OUTPUT_BK_DIFF_FILE))
        previous_key_set = {build_compare_key(record) for record in previous_diff_records}
        logger.info(f"前回 bk_diff_file 件数={len(previous_diff_records)}")

        new_records: List[dict] = []
        duplicate_records: List[dict] = []

        for pair, diff_record in zip(pairs, diff_records):
            record = dict(pair)
            is_duplicate = build_compare_key(diff_record) in previous_key_set
            record["duplicate_proposal_check"] = is_duplicate

            if is_duplicate:
                duplicate_records.append(record)
            else:
                new_records.append(record)

        write_jsonl(str(OUTPUT_NEW), new_records)
        write_jsonl(str(OUTPUT_DUPLICATE), duplicate_records)

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

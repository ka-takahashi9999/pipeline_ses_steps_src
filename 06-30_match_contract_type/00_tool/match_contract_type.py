"""
06-30_match_contract_type
06-12 通過ペアから、案件の契約形態が dispatch 以外のペアのみを通過させる。
LLM使用禁止。
"""

import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import merge_match_info, read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "06-30_match_contract_type"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS = (
    project_root
    / "06-12_filter_required_skills_noise/01_result/matched_pairs_required_skills_noise_filtered.jsonl"
)
INPUT_CONTRACT_TYPE = (
    project_root / "03-30_extract_project_contract_type/01_result/contract_type.jsonl"
)

OUTPUT_MATCHED = STEP_DIR / "01_result/matched_pairs_contract_type.jsonl"
OUTPUT_NO_MATCHED = STEP_DIR / "01_result/99_not_matched_pairs_contract_type.jsonl"

PASS_CONTRACT_TYPES = {"quasi_mandate", "outsourcing"}
FAIL_CONTRACT_TYPE = "dispatch"


def judge_contract_type_match(contract_type: str) -> bool:
    if contract_type == FAIL_CONTRACT_TYPE:
        return False
    return contract_type in PASS_CONTRACT_TYPES


def main() -> None:
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    logger.info("処理開始")

    for path in [INPUT_PAIRS, INPUT_CONTRACT_TYPE]:
        if not path.exists():
            msg = f"入力ファイルが存在しません: {path}"
            logger.error(msg)
            write_error_log(str(dirs["result"]), FileNotFoundError(msg), STEP_NAME)
            sys.exit(1)

    try:
        pairs = read_jsonl_as_list(str(INPUT_PAIRS))
        contract_type_map = read_jsonl_as_dict(str(INPUT_CONTRACT_TYPE), key="message_id")

        logger.info(f"入力ペア数: {len(pairs)}")
        logger.info(f"案件契約形態レコード数: {len(contract_type_map)}")

        matched = []
        no_matched = []

        for pair in pairs:
            project_message_id = pair["project_info"]["message_id"]
            resource_message_id = pair["resource_info"]["message_id"]

            contract_type_rec = contract_type_map.get(project_message_id)
            if contract_type_rec is None:
                raise KeyError(
                    "案件契約形態レコードが存在しません: "
                    f"project_message_id={project_message_id} "
                    f"resource_message_id={resource_message_id}"
                )

            contract_type = contract_type_rec.get("contract_type")
            is_match = judge_contract_type_match(contract_type)
            record = merge_match_info(pair, {"match_contract_type": is_match})

            if is_match:
                matched.append(record)
            else:
                no_matched.append(record)
                logger.info(
                    "NO_MATCH: "
                    f"project={project_message_id} "
                    f"resource={resource_message_id} "
                    f"contract_type={contract_type}"
                )

        write_jsonl(str(OUTPUT_MATCHED), matched)
        write_jsonl(str(OUTPUT_NO_MATCHED), no_matched)

        elapsed = time.time() - start_time
        write_execution_time(
            str(dirs["execution_time"]),
            STEP_NAME,
            elapsed,
            record_count=len(pairs),
        )

        logger.info(f"処理完了 合計={len(pairs)} マッチ={len(matched)} 除外={len(no_matched)}")

    except Exception as e:
        write_error_log(str(dirs["result"]), e, context=STEP_NAME)
        logger.error(f"処理失敗: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

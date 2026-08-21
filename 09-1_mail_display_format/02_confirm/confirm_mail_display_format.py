"""
09-1_mail_display_format confirm スクリプト
① 当日出力ディレクトリの存在確認
② 出力ファイル数 = 入力7ファイルの総ペア件数（一致確認）
③ zip/S3 を使わないことの確認
"""

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.json_utils import read_jsonl_as_list
from common.logger import get_logger

STEP_NAME = "09-1_mail_display_format_confirm"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_DIR = project_root / "08-4_match_score_sort/01_result"
OUTPUT_RESULT_DIR = STEP_DIR / "01_result"
CONFIRM_RESULT = (
    Path(__file__).resolve().parent / "confirm_result_mail_display_format.txt"
)

INPUT_FILES = [
    "match_score_sort_100percent.jsonl",
    "match_score_sort_80to99percent.jsonl",
    "match_score_sort_60to79percent.jsonl",
    "match_score_sort_40to59percent.jsonl",
    "match_score_sort_20to39percent.jsonl",
    "match_score_sort_1to19percent.jsonl",
    "match_score_sort_0percent.jsonl",
]


def is_no_match_file(records: list) -> bool:
    return len(records) == 1 and records[0].get("status") == "no_match"


def count_input_pairs() -> tuple[int, list[str]]:
    total = 0
    missing_files = []
    for filename in INPUT_FILES:
        path = INPUT_DIR / filename
        if not path.exists():
            missing_files.append(filename)
            continue
        records = read_jsonl_as_list(str(path))
        if not is_no_match_file(records):
            total += len(records)
    return total, missing_files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-date", help="確認対象日付 YYYYMMDD")
    return parser.parse_args()


def resolve_target_date(target_date: str) -> str:
    if target_date:
        if not re.fullmatch(r"\d{8}", target_date):
            raise ValueError(f"--target-date は YYYYMMDD 形式で指定してください: {target_date}")
        return target_date
    return datetime.now().strftime("%Y%m%d")


def get_output_dir(target_date: str) -> tuple[Path, str]:
    dirname = f"mail_display_format_{target_date}"
    return OUTPUT_RESULT_DIR / dirname, dirname


def main():
    logger = get_logger(STEP_NAME)
    logger.info("confirm 開始")
    args = parse_args()

    errors = []
    lines = ["=== 09-1_mail_display_format confirm結果 ===", ""]

    target_date = resolve_target_date(args.target_date)
    input_pair_count, missing_input_files = count_input_pairs()
    output_dir, dirname = get_output_dir(target_date)
    zip_path = OUTPUT_RESULT_DIR / f"{dirname}.zip"

    input_ready = len(missing_input_files) == 0
    output_ready = output_dir.exists()

    if input_ready:
        lines.append(f"[OK] 入力ファイル存在: {len(INPUT_FILES)}ファイル")
    else:
        msg = f"[NG] 入力ファイル不足: {', '.join(missing_input_files)}"
        lines.append(msg)
        errors.append(msg)

    if output_ready:
        lines.append(f"[OK] 当日出力ディレクトリ存在: {dirname}")
    else:
        msg = f"[NG] 当日出力ディレクトリが存在しない: {output_dir}"
        lines.append(msg)
        errors.append(msg)

    output_file_count = len(list(output_dir.glob("*.txt"))) if output_ready else 0
    lines.extend([
        f"入力総ペア件数 : {input_pair_count}",
        f"出力ファイル数 : {output_file_count}",
        "",
    ])

    if not input_ready or not output_ready:
        lines.append("[SKIP] 件数一致確認は未判定（前提条件不足）")
    elif input_pair_count != output_file_count:
        msg = f"[NG] 件数不一致: 入力={input_pair_count} 出力={output_file_count}"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] 入力総ペア件数 = 出力ファイル数")

    lines.append("")

    if zip_path.exists():
        msg = f"[NG] zip不要だが存在する: {zip_path}"
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append(f"[OK] zip未作成（仕様どおり）: {zip_path.name}")

    if input_pair_count == 0:
        lines.append("[INFO] 入力総ペア件数は0件")

    output_files = list(output_dir.glob("*.txt")) if output_ready else []
    previous_badge_count = 0
    legacy_label_count = 0
    for path in output_files:
        text = path.read_text(encoding="utf-8")
        previous_badge_count += "[前回も候補" in text
        legacy_label_count += "前回提案済" in text
    legacy_suffix_count = sum("_前回出力済" in path.name for path in output_files)
    lines.extend(
        [
            f"previous candidate badge件数: {previous_badge_count}",
            f"前回提案済 表示件数: {legacy_label_count}",
            f"legacy Success Cache suffix件数: {legacy_suffix_count}",
        ]
    )
    if legacy_label_count or legacy_suffix_count:
        msg = (
            "[NG] 営業表示にSuccess Cache由来の旧表現が残存: "
            f"label={legacy_label_count} suffix={legacy_suffix_count}"
        )
        lines.append(msg)
        errors.append(msg)
    else:
        lines.append("[OK] 『前回提案済』表示とSuccess Cache由来suffixを営業表示から廃止")
    lines.append("[INFO] S3保存確認は実施しない（09-1の責務外）")

    lines.append("")
    lines.append("【結果】NG" if errors else "【結果】OK")
    result_text = "\n".join(lines)

    for line in lines:
        if "[NG]" in line or line.strip() == "【結果】NG":
            logger.error(line)
        else:
            logger.info(line)

    CONFIRM_RESULT.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIRM_RESULT, "w", encoding="utf-8") as f:
        f.write(result_text + "\n")

    logger.info(f"confirm結果ファイル: {CONFIRM_RESULT}")

    if errors:
        logger.error("confirm NG — Pipeline停止")
        sys.exit(1)

    logger.ok("confirm OK")


if __name__ == "__main__":
    main()

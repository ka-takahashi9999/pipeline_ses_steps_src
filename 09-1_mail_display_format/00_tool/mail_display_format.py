"""
09-1_mail_display_format
マッチペアを人間可読形式で1ペア1ファイル出力する。
"""

import re
import shutil
import sys
import time
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_list, read_jsonl_as_dict
from common.logger import get_logger

STEP_NAME = "09-1_mail_display_format"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_DIR = project_root / "08-4_match_score_sort/01_result"
INPUT_MAIL_MASTER = project_root / "01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl"

# 入力ファイル定義: (ファイル名, 出力ラベル)
INPUT_FILES = [
    ("match_score_sort_100percent.jsonl",  "100percent"),
    ("match_score_sort_80to99percent.jsonl", "80to99percent"),
    ("match_score_sort_60to79percent.jsonl", "60to79percent"),
    ("match_score_sort_40to59percent.jsonl", "40to59percent"),
    ("match_score_sort_20to39percent.jsonl", "20to39percent"),
    ("match_score_sort_1to19percent.jsonl",  "1to19percent"),
    ("match_score_sort_0percent.jsonl",      "0percent"),
]


def is_no_match_file(records: list) -> bool:
    return len(records) == 1 and records[0].get("status") == "no_match"


def normalize_body(body: str) -> str:
    """メール本文を改行整形する。連続する空行を1行に圧縮。"""
    if not body:
        return ""
    lines = body.splitlines()
    normalized = []
    prev_blank = False
    for line in lines:
        stripped = line.rstrip()
        if stripped == "":
            if not prev_blank:
                normalized.append("")
            prev_blank = True
        else:
            normalized.append(stripped)
            prev_blank = False
    return "\n".join(normalized).strip()


def format_pair(pair: dict, mail_master: dict) -> str:
    """1ペアのテキスト出力を生成する。"""
    is_duplicate = pair.get("duplicate_proposal_check", False)
    duplicate_flag = "済" if is_duplicate else "未"

    project_mid = pair.get("project_info", {}).get("message_id", "")
    resource_mid = pair.get("resource_info", {}).get("message_id", "")
    required_skills = pair.get("project_info", {}).get("required_skills", [])
    optional_skills = pair.get("project_info", {}).get("optional_skills", [])

    project_mail = mail_master.get(project_mid, {})
    resource_mail = mail_master.get(resource_mid, {})

    lines = []

    # 前回提案済フラグ
    lines.append(f"■■■前回提案済フラグ：{duplicate_flag}")
    lines.append("")

    # 案件メール
    lines.append("■■■案件メール")
    lines.append(f"受信日付:{project_mail.get('date', '')}")
    lines.append(f"メールタイトル:{project_mail.get('subject', '')}")
    lines.append(f"From:{project_mail.get('from', '')}")
    lines.append("メール本文:")
    lines.append(normalize_body(project_mail.get("body_text", "")))
    lines.append("")

    # 要員メール
    lines.append("■■■要員メール")
    lines.append(f"受信日付:{resource_mail.get('date', '')}")
    lines.append(f"メールタイトル:{resource_mail.get('subject', '')}")
    lines.append(f"From:{resource_mail.get('from', '')}")
    lines.append("メール本文:")
    lines.append(normalize_body(resource_mail.get("body_text", "")))
    lines.append("")

    # スキルチェック
    lines.append("■■■スキルチェック")
    lines.append("■必須スキル")
    if required_skills:
        for skill in required_skills:
            mark = "○" if skill.get("match") else "×"
            lines.append(f"[{mark}]:{skill.get('skill', '')}:{skill.get('note', '')}")
    else:
        lines.append("（なし）")
    lines.append("")

    lines.append("■尚可スキル")
    if optional_skills:
        for skill in optional_skills:
            mark = "○" if skill.get("match") else "×"
            lines.append(f"[{mark}]:{skill.get('skill', '')}:{skill.get('note', '')}")
    else:
        lines.append("（なし）")

    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-date", help="出力対象日付 YYYYMMDD")
    return parser.parse_args()


def resolve_target_date(target_date: Optional[str]) -> str:
    if target_date:
        if not re.fullmatch(r"\d{8}", target_date):
            raise ValueError(f"--target-date は YYYYMMDD 形式で指定してください: {target_date}")
        return target_date
    return datetime.now().strftime("%Y%m%d")


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()
    args = parse_args()

    try:
        today = resolve_target_date(args.target_date)
        today_dirname = f"mail_display_format_{today}"
        output_dir = STEP_DIR / "01_result" / today_dirname
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"target date: {today}")

        # メールマスタ読み込み
        mail_master = read_jsonl_as_dict(str(INPUT_MAIL_MASTER), key="message_id")
        logger.info(f"メールマスタ件数={len(mail_master)}")

        total_pairs = 0

        for in_filename, label in INPUT_FILES:
            in_path = INPUT_DIR / in_filename
            if not in_path.exists():
                logger.warn(f"入力ファイルが存在しない: {in_filename}")
                continue

            records = read_jsonl_as_list(str(in_path))
            if is_no_match_file(records):
                logger.info(f"{in_filename}: 0件 (no_match) スキップ")
                continue

            for seq, pair in enumerate(records, 1):
                text = format_pair(pair, mail_master)
                out_filename = f"mail_display_format_{label}_pair_{seq:04d}.txt"
                out_path = output_dir / out_filename
                out_path.write_text(text, encoding="utf-8")
                total_pairs += 1

            logger.info(f"{label}: {len(records)}件出力")

        logger.info(f"テキストファイル出力完了: {total_pairs}件 → {output_dir}")
        logger.info("別日付の mail_display_format 出力は削除しません")

        elapsed = time.time() - start_time
        write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total_pairs)
        logger.ok(f"処理完了: 合計={total_pairs}件")

    except Exception as e:
        write_error_log(str(dirs["result"]), e, context=STEP_NAME)
        logger.error(f"処理失敗: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

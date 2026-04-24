"""
09-2_extract_high_score_mail_display
09-1 の高確度メール表示ファイルのみを抽出し、整理・圧縮して S3 に保存する。
"""

import re
import shutil
import sys
import time
import zipfile
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import boto3

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.logger import get_logger

STEP_NAME = "09-2_extract_high_score_mail_display"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_STEP_DIR = project_root / "09-1_mail_display_format/01_result"

S3_BUCKET = "technoverse"
S3_PREFIX = "pipeline_ses_steps"
AWS_REGION = "ap-northeast-1"

TARGET_GROUPS = [
    (
        "mail_display_format_100percent_pair_*.txt",
        "00_mail_display_format_100percent",
        "100percent",
    ),
    (
        "mail_display_format_80to99percent_pair_*.txt",
        "01_mail_display_format_80to99percent",
        "80to99percent",
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-date", help="処理対象日付 YYYYMMDD")
    return parser.parse_args()


def resolve_target_date(target_date: Optional[str]) -> Optional[str]:
    if target_date is None:
        return None
    if not re.fullmatch(r"\d{8}", target_date):
        raise ValueError(f"--target-date は YYYYMMDD 形式で指定してください: {target_date}")
    return target_date


def get_input_dir(target_date: Optional[str]) -> Tuple[Path, str, str]:
    if target_date:
        input_dirname = f"mail_display_format_{target_date}"
        input_dir = INPUT_STEP_DIR / input_dirname
        if not input_dir.exists():
            raise FileNotFoundError(f"対象日付の09-1入力ディレクトリが存在しません: {input_dir}")
        return input_dir, target_date, "target-date"

    candidates = sorted(path for path in INPUT_STEP_DIR.glob("mail_display_format_*") if path.is_dir())
    if not candidates:
        raise FileNotFoundError(f"09-1出力ディレクトリが存在しません: {INPUT_STEP_DIR}")
    latest = candidates[-1]
    date_part = latest.name.replace("mail_display_format_", "", 1)
    if not re.fullmatch(r"\d{8}", date_part):
        raise ValueError(f"09-1出力日付を解釈できません: {latest.name}")
    return latest, date_part, "latest"


def prepare_output_dirs(today: str) -> tuple[Path, dict[str, Path]]:
    output_dirname = f"mail_display_extract_{today}"
    output_dir = STEP_DIR / "01_result" / output_dirname
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    subdirs: dict[str, Path] = {}
    for _, subdir_name, label in TARGET_GROUPS:
        subdir = output_dir / subdir_name
        subdir.mkdir(parents=True, exist_ok=True)
        subdirs[label] = subdir
    return output_dir, subdirs


def copy_target_files(input_dir: Path, output_subdirs: dict[str, Path], logger) -> dict[str, int]:
    copied_counts: dict[str, int] = {}
    total_count = 0

    for pattern, _, label in TARGET_GROUPS:
        src_files = sorted(input_dir.glob(pattern))
        copied_counts[label] = len(src_files)
        dst_dir = output_subdirs[label]

        for src_file in src_files:
            dst_file = dst_dir / src_file.name
            shutil.copy2(src_file, dst_file)

        total_count += len(src_files)
        logger.info(f"{label}: {len(src_files)}件コピー")

    if total_count == 0:
        logger.warn("対象ファイルが0件です。confirm結果で確認してください。")

    return copied_counts


def create_zip(output_dir: Path, today: str) -> Path:
    zip_path = STEP_DIR / "01_result" / f"mail_display_extract_{today}.zip"
    if zip_path.exists():
        zip_path.unlink()

    with zipfile.ZipFile(str(zip_path), "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(output_dir.rglob("*")):
            arcname = path.relative_to(output_dir.parent)
            zf.write(path, arcname=str(arcname))

    return zip_path


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()
    args = parse_args()

    try:
        target_date = resolve_target_date(args.target_date)
        input_dir, today, source_mode = get_input_dir(target_date)
        logger.info(f"input resolve mode: {source_mode}")
        logger.info(f"target date: {today}")
        logger.info(f"09-1 input dir: {input_dir}")
        output_dir, output_subdirs = prepare_output_dirs(today)
        copied_counts = copy_target_files(input_dir, output_subdirs, logger)

        zip_path = create_zip(output_dir, today)
        logger.info(f"圧縮完了: {zip_path}")

        s3_client = boto3.client("s3", region_name=AWS_REGION)
        s3_key = f"{S3_PREFIX}/{zip_path.name}"
        s3_client.upload_file(str(zip_path), S3_BUCKET, s3_key)
        logger.ok(f"S3アップロード完了: s3://{S3_BUCKET}/{s3_key}")
        logger.info("別日付の抽出結果・zip・S3オブジェクトは削除しません")

        total_count = sum(copied_counts.values())
        elapsed = time.time() - start_time
        write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total_count)
        logger.ok(f"処理完了: 合計={total_count}件")

    except Exception as e:
        write_error_log(str(dirs["result"]), e, context=STEP_NAME)
        logger.error(f"処理失敗: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

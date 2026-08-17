"""
80-9_portal_s3_sync confirm スクリプト

確認項目:
① sync成功（sync_status=SUCCEEDED / mode=apply）
② wait実施（wait_performed=true / verify_wait_sec が非負整数）
③ verify成功（verified=true）
④ missing=0 / extra=0 / size mismatch=0
⑤ expected/actual の file count 一致
⑥ expected/actual の total bytes 一致
⑦ 80-8 summary の file_count / total_bytes と expected 値が一致

AWS APIは再実行せず、80-9のverify summaryのみで確認する。
"""

import json
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.logger import get_logger  # noqa: E402

STEP_NAME = "80-9_portal_s3_sync_confirm"
STEP_DIR = Path(__file__).resolve().parents[1]
SYNC_SUMMARY_PATH = STEP_DIR / "01_result" / "portal_s3_sync_summary.json"
PREPARE_SUMMARY_PATH = project_root / "80-8_portal_s3_prepare" / "01_result" / "portal_s3_prepare_summary.json"
CONFIRM_RESULT = Path(__file__).resolve().parent / "confirm_result_portal_s3_sync.txt"


def main() -> None:
    logger = get_logger(STEP_NAME)
    logger.info("confirm 開始")

    errors = []
    lines = ["=== 80-9_portal_s3_sync confirm結果 ===", ""]

    if not SYNC_SUMMARY_PATH.is_file():
        lines.append(f"[NG] sync summaryが存在しない: {SYNC_SUMMARY_PATH}")
        errors.append("summary missing")
        _write_and_exit(logger, lines, errors)
        return

    with open(SYNC_SUMMARY_PATH, "r", encoding="utf-8") as f:
        summary = json.load(f)

    lines.append(f"[INFO] destination={summary.get('s3_destination')}")
    lines.append(f"[INFO] sync_method={summary.get('sync_method')}")

    # ①
    if summary.get("sync_status") != "SUCCEEDED":
        lines.append(f"[NG] sync_status={summary.get('sync_status')}")
        errors.append("sync status")
    elif summary.get("mode") != "apply":
        lines.append(f"[NG] mode={summary.get('mode')}（本番runはapplyであること）")
        errors.append("mode")
    else:
        lines.append("[OK] sync成功 (mode=apply)")

    # ②
    wait_sec = summary.get("verify_wait_sec")
    if not isinstance(wait_sec, int) or isinstance(wait_sec, bool) or wait_sec < 0:
        lines.append(f"[NG] verify_wait_secが非負整数でない: {wait_sec!r}")
        errors.append("wait sec")
    elif summary.get("wait_performed") is not True:
        lines.append("[NG] sync後のwaitが実施されていない")
        errors.append("wait performed")
    else:
        lines.append(f"[OK] wait実施 ({wait_sec}秒)")

    verify = summary.get("verify") or {}

    # ③
    if verify.get("verified") is not True:
        lines.append(f"[NG] verify未成功: {verify.get('skipped_reason', '')}")
        errors.append("verified")
    else:
        lines.append("[OK] verify成功")

    # ④
    for label, key in (
        ("missing", "missing_count"),
        ("extra", "extra_count"),
        ("size mismatch", "size_mismatch_count"),
    ):
        count = verify.get(key)
        if count != 0:
            samples = verify.get(key.replace("_count", "_samples")) or []
            lines.append(f"[NG] {label}={count} 例={samples[:3]}")
            errors.append(label)
        else:
            lines.append(f"[OK] {label}=0")

    # ⑤⑥
    expected_files = verify.get("expected_file_count")
    actual_files = verify.get("actual_file_count")
    expected_bytes = verify.get("expected_total_bytes")
    actual_bytes = verify.get("actual_total_bytes")

    def _is_count(value):
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0

    if not _is_count(expected_files) or not _is_count(actual_files):
        lines.append(f"[NG] file countが記録されていない: expected={expected_files} actual={actual_files}")
        errors.append("file count")
    elif expected_files != actual_files:
        lines.append(f"[NG] file count不一致: expected={expected_files} actual={actual_files}")
        errors.append("file count")
    else:
        lines.append(f"[OK] file count一致 ({expected_files}件)")

    if not _is_count(expected_bytes) or not _is_count(actual_bytes):
        lines.append(f"[NG] total bytesが記録されていない: expected={expected_bytes} actual={actual_bytes}")
        errors.append("total bytes")
    elif expected_bytes != actual_bytes:
        lines.append(f"[NG] total bytes不一致: expected={expected_bytes} actual={actual_bytes}")
        errors.append("total bytes")
    else:
        lines.append(f"[OK] total bytes一致 ({expected_bytes} bytes)")

    # ⑦
    if PREPARE_SUMMARY_PATH.is_file():
        with open(PREPARE_SUMMARY_PATH, "r", encoding="utf-8") as f:
            prepare_summary = json.load(f)
        if prepare_summary.get("file_count") != expected_files:
            lines.append(
                f"[NG] 80-8 file_countとexpected不一致: "
                f"{prepare_summary.get('file_count')} != {expected_files}"
            )
            errors.append("prepare file count")
        elif prepare_summary.get("total_bytes") != expected_bytes:
            lines.append(
                f"[NG] 80-8 total_bytesとexpected不一致: "
                f"{prepare_summary.get('total_bytes')} != {expected_bytes}"
            )
            errors.append("prepare bytes")
        else:
            lines.append("[OK] 80-8 manifestサマリと一致")
    else:
        lines.append(f"[NG] 80-8 summaryが存在しない: {PREPARE_SUMMARY_PATH}")
        errors.append("prepare summary missing")

    _write_and_exit(logger, lines, errors)


def _write_and_exit(logger, lines, errors) -> None:
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
        logger.error("confirm NG")
        sys.exit(1)
    logger.ok("confirm OK")


if __name__ == "__main__":
    main()

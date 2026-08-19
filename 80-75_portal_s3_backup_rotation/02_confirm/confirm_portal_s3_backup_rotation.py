"""
80-75_portal_s3_backup_rotation confirm スクリプト

確認項目:
① backup成功（backup_status=SUCCEEDED / mode=apply）
② source / destination が canonical CURRENT / bk1 で lock済み
③ wait実施（wait_performed=true / verify_wait_sec が非負整数）
④ bk1 verify成功（verified=true）
⑤ missing=0 / extra=0 / size mismatch=0
⑥ rotation時previous CURRENT snapshot / bk1 の file count 一致
⑦ rotation時previous CURRENT snapshot / bk1 の total bytes 一致
⑧ previous CURRENT snapshotのprovenance / destination / verifiedが記録されている

AWS APIと最新80-9 summaryは読み直さず、80-75 summaryに固定保存された
rotation時previous CURRENT snapshotを正本とする。
"""

import json
import re
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.logger import get_logger  # noqa: E402

STEP_NAME = "80-75_portal_s3_backup_rotation_confirm"
STEP_DIR = Path(__file__).resolve().parents[1]
BACKUP_SUMMARY_PATH = STEP_DIR / "01_result" / "portal_s3_backup_rotation_summary.json"
CONFIRM_RESULT = Path(__file__).resolve().parent / "confirm_result_portal_s3_backup_rotation.txt"

EXPECTED_SOURCE_URI = "s3://technoverse/pipeline_ses_steps/pipeline_ses_steps/"
EXPECTED_DESTINATION_URI = "s3://technoverse/pipeline_ses_steps/pipeline_ses_steps_bk1/"

RUN_DATE_RE = re.compile(r"^\d{8}$")
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def _is_count(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def main() -> None:
    logger = get_logger(STEP_NAME)
    logger.info("confirm 開始")

    errors = []
    lines = ["=== 80-75_portal_s3_backup_rotation confirm結果 ===", ""]

    if not BACKUP_SUMMARY_PATH.is_file():
        lines.append(f"[NG] backup summaryが存在しない: {BACKUP_SUMMARY_PATH}")
        errors.append("summary missing")
        _write_and_exit(logger, lines, errors)
        return

    with open(BACKUP_SUMMARY_PATH, "r", encoding="utf-8") as f:
        summary = json.load(f)

    lines.append(f"[INFO] operation={summary.get('operation')}")
    lines.append(f"[INFO] source={summary.get('s3_source')}")
    lines.append(f"[INFO] destination={summary.get('s3_destination')}")
    lines.append(f"[INFO] backup_method={summary.get('backup_method')}")

    # ①
    if summary.get("backup_status") != "SUCCEEDED":
        lines.append(f"[NG] backup_status={summary.get('backup_status')}")
        errors.append("backup status")
    elif summary.get("mode") != "apply":
        lines.append(f"[NG] mode={summary.get('mode')}（本番runはapplyであること）")
        errors.append("mode")
    else:
        lines.append("[OK] backup成功 (mode=apply)")

    # ②
    if summary.get("s3_source") != EXPECTED_SOURCE_URI:
        lines.append(f"[NG] sourceがcanonical CURRENTでない: {summary.get('s3_source')}")
        errors.append("source uri")
    elif summary.get("s3_destination") != EXPECTED_DESTINATION_URI:
        lines.append(f"[NG] destinationがcanonical bk1でない: {summary.get('s3_destination')}")
        errors.append("destination uri")
    elif summary.get("s3_destination_locked") is not True:
        lines.append("[NG] destination lockが記録されていない")
        errors.append("destination lock")
    else:
        lines.append("[OK] CURRENT -> bk1 のdestination lock済み")

    # ③
    wait_sec = summary.get("verify_wait_sec")
    if not _is_count(wait_sec):
        lines.append(f"[NG] verify_wait_secが非負整数でない: {wait_sec!r}")
        errors.append("wait sec")
    elif summary.get("wait_performed") is not True:
        lines.append("[NG] backup後のwaitが実施されていない")
        errors.append("wait performed")
    else:
        lines.append(f"[OK] wait実施 ({wait_sec}秒)")

    verify = summary.get("verify") or {}

    # ④
    if verify.get("verified") is not True:
        lines.append(f"[NG] bk1 verify未成功: {verify.get('skipped_reason', '')}")
        errors.append("verified")
    else:
        lines.append("[OK] bk1 verify成功")

    # ⑤
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

    # ⑥⑦: latest 80-9 summaryではなく、80-75実行時snapshotが正本。
    previous = summary.get("previous_current") or {}
    snapshot_files = previous.get("file_count")
    backup_files = verify.get("actual_file_count")
    verify_expected_files = verify.get("expected_file_count")
    snapshot_bytes = previous.get("total_bytes")
    backup_bytes = verify.get("actual_total_bytes")
    verify_expected_bytes = verify.get("expected_total_bytes")

    if not all(_is_count(v) for v in (snapshot_files, verify_expected_files, backup_files)):
        lines.append(
            "[NG] file countが記録されていない: "
            f"snapshot={snapshot_files} expected={verify_expected_files} bk1={backup_files}"
        )
        errors.append("file count")
    elif snapshot_files != verify_expected_files or snapshot_files != backup_files:
        lines.append(
            "[NG] file count不一致: "
            f"snapshot={snapshot_files} expected={verify_expected_files} bk1={backup_files}"
        )
        errors.append("file count")
    else:
        lines.append(f"[OK] snapshot / bk1 file count一致 ({snapshot_files}件)")

    if not all(_is_count(v) for v in (snapshot_bytes, verify_expected_bytes, backup_bytes)):
        lines.append(
            "[NG] total bytesが記録されていない: "
            f"snapshot={snapshot_bytes} expected={verify_expected_bytes} bk1={backup_bytes}"
        )
        errors.append("total bytes")
    elif snapshot_bytes != verify_expected_bytes or snapshot_bytes != backup_bytes:
        lines.append(
            "[NG] total bytes不一致: "
            f"snapshot={snapshot_bytes} expected={verify_expected_bytes} bk1={backup_bytes}"
        )
        errors.append("total bytes")
    else:
        lines.append(f"[OK] snapshot / bk1 total bytes一致 ({snapshot_bytes} bytes)")

    # ⑧
    run_date = previous.get("run_date")
    run_id = previous.get("run_id")
    if not isinstance(run_date, str) or not RUN_DATE_RE.match(run_date or ""):
        lines.append(f"[NG] previous CURRENTのrun_dateが不正: {run_date!r}")
        errors.append("run_date")
    elif not isinstance(run_id, str) or not RUN_ID_RE.match(run_id or ""):
        lines.append(f"[NG] previous CURRENTのrun_idが不正: {run_id!r}")
        errors.append("run_id")
    elif previous.get("run_date_source") != "env":
        lines.append(f"[NG] previous CURRENTのrun_date_sourceが不正: {previous.get('run_date_source')!r}")
        errors.append("run_date source")
    elif previous.get("run_id_source") != "env":
        lines.append(f"[NG] previous CURRENTのrun_id_sourceが不正: {previous.get('run_id_source')!r}")
        errors.append("run_id source")
    elif previous.get("destination") != EXPECTED_SOURCE_URI:
        lines.append(f"[NG] previous CURRENT destinationが不正: {previous.get('destination')!r}")
        errors.append("previous destination")
    elif previous.get("verified") is not True:
        lines.append(f"[NG] previous CURRENT verifiedがtrueでない: {previous.get('verified')!r}")
        errors.append("previous verified")
    elif previous.get("sync_step") != "80-9_portal_s3_sync":
        lines.append(f"[NG] previous CURRENT sync_stepが不正: {previous.get('sync_step')!r}")
        errors.append("previous sync step")
    elif not previous.get("status_key"):
        lines.append("[NG] pipeline-status照合keyが記録されていない")
        errors.append("status key")
    else:
        lines.append(f"[OK] previous CURRENT snapshot provenance ({run_date}/{run_id})")

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

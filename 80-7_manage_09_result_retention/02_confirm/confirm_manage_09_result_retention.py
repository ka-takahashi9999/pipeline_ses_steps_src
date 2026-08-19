"""
80-7_manage_09_result_retention confirm スクリプト

確認項目:
① summaryの保持RUN_DATE = 今回RUN_DATE + 直前の正常終了RUN_DATE（S3 status由来）
② apply後: ローカル09系に残るRUN_DATEが保持RUN_DATEのみ（それより古い認識済み成果物=0）
③ 保持RUN_DATEの成果物が実際に残っている
④ HOLD対象（error_*.log 等の運用ログ）が維持されている
⑤ summaryの件数整合（breakdown合計 = planned / apply時は deleted = planned）
⑥ root ZIPのcurrent / previous successfulが残存し、apply後のold canonicalが0件

AWS APIは再実行せず、80-7 summaryとローカル状態のみで確認する。
"""

import json
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.logger import get_logger  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "00_tool"))

from manage_09_result_retention import (  # noqa: E402
    RETENTION_TARGETS,
    RetentionError,
    scan_artifacts,
)

STEP_NAME = "80-7_manage_09_result_retention_confirm"
STEP_DIR = Path(__file__).resolve().parents[1]
SUMMARY_PATH = STEP_DIR / "01_result" / "manage_09_result_retention_summary.json"
CONFIRM_RESULT = Path(__file__).resolve().parent / "confirm_result_manage_09_result_retention.txt"


def main() -> None:
    logger = get_logger(STEP_NAME)
    logger.info("confirm 開始")

    errors = []
    lines = ["=== 80-7_manage_09_result_retention confirm結果 ===", ""]

    if not SUMMARY_PATH.is_file():
        lines.append(f"[NG] summaryが存在しない: {SUMMARY_PATH}")
        errors.append("summary missing")
        _write_and_exit(logger, lines, errors)
        return

    with open(SUMMARY_PATH, "r", encoding="utf-8") as f:
        summary = json.load(f)

    mode = summary.get("mode")
    current = summary.get("current_run_date")
    previous = summary.get("previous_successful_run_date")
    keep = summary.get("keep_run_dates") or []
    root_zip_only = summary.get("root_zip_only") is True
    lines.append(f"[INFO] mode={mode}")
    lines.append(f"[INFO] current_run_date={current}")
    lines.append(f"[INFO] previous_successful_run_date={previous}")
    lines.append(f"[INFO] keep_run_dates={keep}")

    # ① 保持RUN_DATE
    if summary.get("previous_successful_run_date_source") != "s3_pipeline_status":
        lines.append("[NG] previous_successful_run_date_source が s3_pipeline_status ではない")
        errors.append("previous source")
    expected_keep = sorted({current} | ({previous} if previous else set()))
    if sorted(keep) != expected_keep:
        lines.append(f"[NG] keep_run_dates不整合: {sorted(keep)} != {expected_keep}")
        errors.append("keep run dates")
    else:
        lines.append("[OK] 保持RUN_DATE = 今回RUN_DATE + 直前の正常終了RUN_DATE")

    # ⑥ root ZIP
    root_zip = summary.get("root_zip") or {}
    expected_root_keys = [
        f"pipeline_ses_steps/mail_display_extract_{run_date}.zip" for run_date in keep
    ]
    missing_root_keep = [key for key in expected_root_keys if key not in root_zip.get("keep_keys", [])]
    if missing_root_keep:
        lines.append(f"[NG] root ZIPのKEEP対象が不足: {missing_root_keep}")
        errors.append("root zip keep")
    elif mode == "apply" and root_zip.get("verified") is not True:
        lines.append("[NG] root ZIP apply後verifyが成功していない")
        errors.append("root zip verify")
    elif mode == "apply" and len(root_zip.get("deleted_keys") or []) != len(
        root_zip.get("delete_candidate_keys") or []
    ):
        lines.append("[NG] root ZIPのDELETE候補と削除件数が不一致")
        errors.append("root zip delete count")
    else:
        lines.append(
            f"[OK] root ZIP保持/rotation整合 "
            f"(keep={len(root_zip.get('keep_keys') or [])} / "
            f"deleted={len(root_zip.get('deleted_keys') or [])})"
        )

    # ⑤ summary件数整合
    breakdown = summary.get("delete_breakdown") or []
    breakdown_files = sum(row.get("file_count", 0) for row in breakdown)
    breakdown_bytes = sum(row.get("total_bytes", 0) for row in breakdown)
    if breakdown_files != summary.get("planned_delete_files"):
        lines.append(
            f"[NG] breakdown合計とplanned_delete_files不一致: "
            f"{breakdown_files} != {summary.get('planned_delete_files')}"
        )
        errors.append("breakdown files")
    elif breakdown_bytes != summary.get("planned_delete_bytes"):
        lines.append(
            f"[NG] breakdown合計とplanned_delete_bytes不一致: "
            f"{breakdown_bytes} != {summary.get('planned_delete_bytes')}"
        )
        errors.append("breakdown bytes")
    else:
        lines.append(
            f"[OK] summary件数整合 (files={breakdown_files} / bytes={breakdown_bytes})"
        )

    if mode == "apply":
        if summary.get("deleted_files") != summary.get("planned_delete_files"):
            lines.append(
                f"[NG] deleted_files != planned_delete_files: "
                f"{summary.get('deleted_files')} != {summary.get('planned_delete_files')}"
            )
            errors.append("deleted files")
        else:
            lines.append(f"[OK] deleted_files = planned_delete_files ({summary.get('deleted_files')})")
    else:
        if summary.get("deleted_files") != 0 or summary.get("deleted_bytes") != 0:
            lines.append("[NG] dry-runなのに削除が発生している")
            errors.append("dry-run deleted")
        else:
            lines.append("[OK] dry-runでの削除は0件")

    # ②③ ローカル再走査（root ZIP個別検証時は変更対象外）
    if root_zip_only:
        lines.append("[OK] root-zip-onlyのためローカル09成果物確認は対象外")
        _write_and_exit(logger, lines, errors)
        return

    try:
        artifacts, holds = scan_artifacts(project_root, current, logger)
    except RetentionError as exc:
        lines.append(f"[NG] ローカル再走査に失敗: {exc}")
        errors.append("rescan")
        artifacts, holds = [], []

    present_run_dates = sorted({a["run_date"] for a in artifacts})
    lines.append(f"[INFO] ローカル残存RUN_DATE={present_run_dates}")

    if mode == "apply":
        older = [d for d in present_run_dates if d not in keep]
        if older:
            lines.append(f"[NG] 保持対象外のRUN_DATEが残存: {older[:3]}")
            errors.append("older remains")
        else:
            lines.append("[OK] 保持RUN_DATEより古い認識済み09成果物 = 0件")

        scanned_before = summary.get("artifact_run_dates") or []
        keep_missing = [
            run_date
            for run_date in keep
            if run_date in scanned_before and run_date not in present_run_dates
        ]
        if keep_missing:
            lines.append(f"[NG] 保持すべきRUN_DATEの成果物が消えている: {keep_missing}")
            errors.append("keep missing")
        else:
            lines.append("[OK] 保持RUN_DATEの成果物は残存している")
    else:
        expected_delete_dates = sorted(
            {d for d in (summary.get("artifact_run_dates") or []) if d not in keep}
        )
        lines.append(f"[INFO] 削除候補RUN_DATE={expected_delete_dates}")
        if present_run_dates != sorted(summary.get("artifact_run_dates") or []):
            lines.append("[NG] dry-run後にRUN_DATE構成が変化している")
            errors.append("dry-run changed")
        else:
            lines.append("[OK] dry-runでローカル状態は未変更")

    # ④ HOLD対象
    hold_entries = summary.get("hold_entries") or []
    missing_holds = [rel for rel in hold_entries if not (project_root / rel).is_file()]
    if missing_holds:
        lines.append(f"[NG] HOLD対象が消えている: {missing_holds[:3]}")
        errors.append("hold missing")
    else:
        lines.append(f"[OK] HOLD対象維持 ({len(hold_entries)}件)")
    if sorted(holds) != sorted(hold_entries):
        lines.append("[NG] HOLD対象の集合がsummaryと不一致")
        errors.append("hold set")

    lines.append(f"[INFO] 認識対象パターン数={len(RETENTION_TARGETS)}")

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

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
⑨ 新contract summaryのcurrent execution immutable history guardが完全
⑩ pre-publication recovery時は全FAILED executionのpublication未到達 / Redriveなし /
   authority / CURRENT / BK1監査が完全

AWS APIと最新80-9 summaryは読み直さず、80-75 summaryに固定保存された
rotation時previous CURRENT snapshotを正本とする。
"""

import json
import re
import sys
from datetime import datetime
from fractions import Fraction
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
EXPECTED_STATE_MACHINE_ARN = (
    "arn:aws:states:ap-northeast-1:166714029268:stateMachine:"
    "auto-match-llm-classifier-pipeline-orchestration"
)
EXPECTED_EXECUTION_ARN_PREFIX = EXPECTED_STATE_MACHINE_ARN.replace(
    ":stateMachine:", ":execution:"
) + ":"
IMMUTABLE_EXECUTION_GUARD_CONTRACT_VERSION = 1
PREPUBLICATION_RECOVERY_MODE = "pre_publication_failed_runs"
PUBLICATION_BOUNDARY_STEP_NAME = "80-75_portal_s3_backup_rotation"
PUBLICATION_BOUNDARY_STATE = "SendPhaseBLauncherCommand"
PIPELINE_STEP_RE = re.compile(r"^(\d{2})-(\d+)(?:_|\(|$)")

RUN_DATE_RE = re.compile(r"^\d{8}$")
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def _is_count(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _parse_timestamp(value):
    if not isinstance(value, str) or not value:
        return None
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _before_publication_boundary(step_name):
    if not isinstance(step_name, str):
        return False
    match = PIPELINE_STEP_RE.match(step_name)
    boundary = PIPELINE_STEP_RE.match(PUBLICATION_BOUNDARY_STEP_NAME)
    if match is None or boundary is None:
        return False
    major, minor = match.groups()
    boundary_major, boundary_minor = boundary.groups()
    actual_order = (int(major), Fraction(int(minor), 10 ** len(minor)))
    boundary_order = (
        int(boundary_major),
        Fraction(int(boundary_minor), 10 ** len(boundary_minor)),
    )
    return actual_order < boundary_order


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

    # ⑨: markerありの新summaryはimmutable execution evidenceを必須監査する。
    # markerなしのhistorical summaryだけをlegacyとして互換読取りする。
    execution_guard = summary.get("current_execution_guard")
    contract_key = "immutable_execution_guard_contract_version"
    contract_present = contract_key in summary
    contract_version = summary.get(contract_key)
    if not contract_present and execution_guard is None:
        lines.append("[INFO] immutable execution guard未記録（legacy summary）")
    elif contract_present and contract_version != IMMUTABLE_EXECUTION_GUARD_CONTRACT_VERSION:
        lines.append(
            "[NG] immutable execution guard contract versionが不正: "
            f"{contract_version!r}"
        )
        errors.append("immutable execution guard contract version")
    elif contract_present:
        immutable_ok = (
            isinstance(execution_guard, dict)
            and execution_guard.get("validation_result") == "PASS"
            and execution_guard.get("immutable_execution_guard_result") == "PASS"
            and execution_guard.get("evidence_source") == "stepfunctions_execution_history"
            and execution_guard.get("state_machine_arn") == EXPECTED_STATE_MACHINE_ARN
            and isinstance(execution_guard.get("current_execution_arn"), str)
            and execution_guard.get("current_execution_arn", "").startswith(
                EXPECTED_EXECUTION_ARN_PREFIX
            )
            and execution_guard.get("execution_status") == "RUNNING"
            and isinstance(execution_guard.get("run_date"), str)
            and RUN_DATE_RE.fullmatch(execution_guard.get("run_date", "")) is not None
            and isinstance(execution_guard.get("run_id"), str)
            and RUN_ID_RE.fullmatch(execution_guard.get("run_id", "")) is not None
            and execution_guard.get("run_identity_match") is True
            and execution_guard.get("prepare_run_context_matches") == 1
            and _is_count(execution_guard.get("redrive_count"))
            and execution_guard.get("redrive_count") == 0
            and execution_guard.get("redrive_date_present") is False
            and execution_guard.get("execution_redriven_event_present") is False
            and execution_guard.get("prior_terminal_event_present") is False
            and execution_guard.get("execution_redriven_event_count") == 0
            and execution_guard.get("prior_terminal_event_count") == 0
            and _is_count(execution_guard.get("list_pages_checked"))
            and execution_guard.get("list_pages_checked", 0) > 0
            and _is_count(execution_guard.get("history_pages_checked"))
            and execution_guard.get("history_pages_checked", 0) > 0
            and _is_count(execution_guard.get("history_event_count"))
            and execution_guard.get("history_event_count", 0) > 0
        )
        if immutable_ok:
            lines.append(
                "[OK] immutable execution history guard "
                f"({execution_guard.get('run_date')}/{execution_guard.get('run_id')})"
            )
        else:
            lines.append("[NG] immutable execution history guard監査が不完全")
            errors.append("immutable execution guard")
    else:
        # pre-contract summaryでguardが記録済みの場合は、従来の監査条件を維持する。
        legacy_immutable_ok = (
            isinstance(execution_guard, dict)
            and execution_guard.get("validation_result") == "PASS"
            and execution_guard.get("evidence_source") == "stepfunctions_execution_history"
            and execution_guard.get("state_machine_arn") == EXPECTED_STATE_MACHINE_ARN
            and isinstance(execution_guard.get("execution_arn"), str)
            and execution_guard.get("execution_arn", "").startswith(EXPECTED_EXECUTION_ARN_PREFIX)
            and isinstance(execution_guard.get("run_date"), str)
            and RUN_DATE_RE.fullmatch(execution_guard.get("run_date", "")) is not None
            and isinstance(execution_guard.get("run_id"), str)
            and RUN_ID_RE.fullmatch(execution_guard.get("run_id", "")) is not None
            and execution_guard.get("prepare_run_context_matches") == 1
            and _is_count(execution_guard.get("redrive_count"))
            and execution_guard.get("redrive_count") == 0
            and execution_guard.get("redrive_date_present") is False
            and execution_guard.get("execution_redriven_event_count") == 0
            and execution_guard.get("prior_terminal_event_count") == 0
            and _is_count(execution_guard.get("list_pages_checked"))
            and execution_guard.get("list_pages_checked", 0) > 0
            and _is_count(execution_guard.get("history_pages_checked"))
            and execution_guard.get("history_pages_checked", 0) > 0
            and _is_count(execution_guard.get("history_event_count"))
            and execution_guard.get("history_event_count", 0) > 0
        )
        if legacy_immutable_ok:
            lines.append("[OK] immutable execution history guard（legacy contract）")
        else:
            lines.append("[NG] legacy immutable execution history guard監査が不完全")
            errors.append("legacy immutable execution guard")

    # ⑩: recoveryが記録された場合だけ追加監査する。
    recovery = summary.get("recovery")
    if recovery is not None and not isinstance(recovery, dict):
        lines.append("[NG] recoveryがJSON objectではありません")
        errors.append("unknown recovery mode")
    elif recovery is not None and recovery.get("recovery_mode") == PREPUBLICATION_RECOVERY_MODE:
        intervening = recovery.get("intervening_runs")
        execution_window = recovery.get("execution_window") or {}
        current_unchanged = recovery.get("current_unchanged") or {}
        bk1_unchanged = recovery.get("bk1_unchanged") or {}
        publication_guard = recovery.get("publication_guard") or {}
        runs_ok = (
            isinstance(intervening, list)
            and len(intervening) > 0
            and all(
                isinstance(item, dict)
                and item.get("status") == "FAILED"
                and item.get("validation_result") == "PASS"
                and item.get("step_order_verified") is True
                and item.get("before_publication_boundary") is True
                and _before_publication_boundary(item.get("current_step"))
                and item.get("publication_boundary_reached") is False
                and isinstance(item.get("execution_evidence"), dict)
                and item["execution_evidence"].get("validation_result") == "PASS"
                and item["execution_evidence"].get("evidence_source")
                == "stepfunctions_execution_history"
                and isinstance(item["execution_evidence"].get("execution_arn"), str)
                and item["execution_evidence"].get("execution_arn", "").startswith(
                    EXPECTED_EXECUTION_ARN_PREFIX
                )
                and item["execution_evidence"].get("execution_status") == "FAILED"
                and item["execution_evidence"].get("run_identity_match") is True
                and item["execution_evidence"].get("redrive_count") == 0
                and item["execution_evidence"].get("redrive_date_present") is False
                and item["execution_evidence"].get(
                    "execution_redriven_event_present"
                ) is False
                and _is_count(
                    item["execution_evidence"].get("history_pages_checked")
                )
                and item["execution_evidence"].get("history_pages_checked", 0) > 0
                and _is_count(
                    item["execution_evidence"].get("history_event_count")
                )
                and item["execution_evidence"].get("history_event_count", 0) > 0
                and item["execution_evidence"].get("publication_boundary_state")
                == PUBLICATION_BOUNDARY_STATE
                and item["execution_evidence"].get(
                    "publication_boundary_reached"
                ) is False
                for item in intervening
            )
        )
        authority_stop = _parse_timestamp(
            execution_window.get("authority_stop_date")
        )
        current_start = _parse_timestamp(
            execution_window.get("current_start_date")
        )
        outside = execution_window.get("outside_recovery_window")
        outside_ok = (
            isinstance(outside, list)
            and all(
                isinstance(item, dict)
                and item.get("classification") == "OUTSIDE_RECOVERY_WINDOW"
                and item.get("reason")
                in ("COMPLETED_BEFORE_AUTHORITY", "STARTED_AFTER_CURRENT")
                and isinstance(item.get("execution_arn"), str)
                and item.get("execution_arn", "").startswith(
                    EXPECTED_EXECUTION_ARN_PREFIX
                )
                and _parse_timestamp(item.get("execution_start_date")) is not None
                and _parse_timestamp(item.get("execution_stop_date")) is not None
                and (
                    (
                        item.get("reason") == "COMPLETED_BEFORE_AUTHORITY"
                        and authority_stop is not None
                        and _parse_timestamp(item.get("execution_stop_date"))
                        < authority_stop
                    )
                    or (
                        item.get("reason") == "STARTED_AFTER_CURRENT"
                        and current_start is not None
                        and _parse_timestamp(item.get("execution_start_date"))
                        > current_start
                    )
                )
                for item in outside
            )
        )
        candidate_times_ok = (
            authority_stop is not None
            and current_start is not None
            and isinstance(intervening, list)
            and all(
                isinstance(item, dict)
                and isinstance(item.get("execution_evidence"), dict)
                and _parse_timestamp(
                    item.get("execution_evidence", {}).get(
                        "execution_start_date"
                    )
                )
                is not None
                and _parse_timestamp(
                    item.get("execution_evidence", {}).get(
                        "execution_stop_date"
                    )
                )
                is not None
                and authority_stop
                < _parse_timestamp(
                    item["execution_evidence"]["execution_start_date"]
                )
                < _parse_timestamp(
                    item["execution_evidence"]["execution_stop_date"]
                )
                < current_start
                and item["execution_evidence"].get(
                    "recovery_window_candidate"
                )
                is True
                for item in intervening
            )
        )
        window_ok = (
            execution_window.get("ordering_source")
            == "stepfunctions_execution_metadata"
            and isinstance(execution_window.get("authority_execution_arn"), str)
            and execution_window.get("authority_execution_arn", "").startswith(
                EXPECTED_EXECUTION_ARN_PREFIX
            )
            and isinstance(execution_window.get("current_execution_arn"), str)
            and execution_window.get("current_execution_arn", "").startswith(
                EXPECTED_EXECUTION_ARN_PREFIX
            )
            and authority_stop is not None
            and current_start is not None
            and authority_stop < current_start
            and execution_window.get("candidate_execution_count")
            == len(intervening or [])
            and execution_window.get("outside_recovery_window_count")
            == len(outside or [])
            and outside_ok
            and candidate_times_ok
            and isinstance(execution_guard, dict)
            and execution_guard.get("execution_start_date")
            == execution_window.get("current_start_date")
        )
        recovery_ok = (
            recovery.get("enabled") is True
            and recovery.get("eligible") is True
            and recovery.get("rotation_authority_run_date") == run_date
            and recovery.get("rotation_authority_run_id") == run_id
            and recovery.get("all_intervening_runs_checked") is True
            and _is_count(recovery.get("failed_execution_list_pages_checked"))
            and recovery.get("failed_execution_list_pages_checked", 0) > 0
            and runs_ok
            and window_ok
            and publication_guard.get("terminal_status") == "FAILED"
            and publication_guard.get("publication_boundary_step")
            == PUBLICATION_BOUNDARY_STEP_NAME
            and publication_guard.get("publication_boundary_state")
            == PUBLICATION_BOUNDARY_STATE
            and publication_guard.get("publication_boundary_reached") is False
            and publication_guard.get("failure_reason_allowlist_used") is False
            and recovery.get("failure_contract") is None
            and current_unchanged.get("verified") is True
            and current_unchanged.get("manifest_inventory_match") is True
            and current_unchanged.get("unchanged_since_rotation_authority") is True
            and bk1_unchanged.get("verified") is True
            and bk1_unchanged.get("previous_80_75_summary_match") is True
            and bk1_unchanged.get("unchanged_since_rotation_authority") is True
            and isinstance(execution_guard, dict)
            and execution_guard.get("validation_result") == "PASS"
            and execution_guard.get("run_date") == recovery.get("current_run_date")
            and execution_guard.get("run_id") == recovery.get("current_run_id")
        )
        if recovery_ok:
            lines.append(
                f"[OK] pre-publication recovery監査 ({len(intervening)} intervening runs)"
            )
        else:
            lines.append("[NG] pre-publication recovery監査が不完全")
            errors.append("pre-publication recovery audit")
    elif recovery is not None and recovery.get("recovery_mode") is None:
        # commit済みの既存80-7限定recovery contractは互換維持する。
        legacy_keys = {
            "target_run_date",
            "target_run_id",
            "failed_status_key",
            "failed_step",
            "previous_verified_run_date",
            "previous_verified_run_id",
            "previous_verified_finished_at",
        }
        if legacy_keys.issubset(recovery):
            lines.append("[OK] existing 80-7 recovery contract")
        else:
            lines.append("[NG] recovery modeが不明です")
            errors.append("unknown recovery mode")
    elif recovery is not None:
        lines.append(f"[NG] recovery modeが不明です: {recovery.get('recovery_mode')!r}")
        errors.append("unknown recovery mode")

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

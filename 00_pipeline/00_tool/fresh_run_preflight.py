#!/usr/bin/env python3
"""Read-only Fresh Run preflight for managed pipeline executions.

Local RUNNING/Batch-wait files are discovery hints, not execution authority.
They are never rewritten.  A candidate blocks only when current remote or
process evidence proves that work can still progress.  A local-only residue is
ignored only after every terminal condition is verified.
"""

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import boto3


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from common.logger import get_logger  # noqa: E402


STEP_NAME = "00_pipeline_fresh_run_preflight"
MANAGED_ROOT = PROJECT_ROOT / "00_pipeline" / "01_result" / "managed"
BATCH_RUNTIME_ROOT = (
    PROJECT_ROOT
    / "08-5_high_score_required_skill_recheck"
    / "01_result"
    / "_batch_runtime"
)
STATE_MACHINE_ARN = (
    "arn:aws:states:ap-northeast-1:166714029268:stateMachine:"
    "auto-match-llm-classifier-pipeline-orchestration"
)
BUCKET = "technoverse"
BASE_PREFIX = "pipeline_ses_steps"
REGION = "ap-northeast-1"
PIPELINE_STATUS_PREFIX = f"{BASE_PREFIX}/pipeline-status"
BATCH_STATE_PREFIX = f"{BASE_PREFIX}/batch-state/08-5"

CLASS_ACTIVE = "ACTIVE"
CLASS_STALE = "STALE_TERMINAL_LOCAL_RESIDUE"

SFN_TERMINAL_STATUSES = frozenset(
    ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED", "PENDING_REDRIVE")
)
PIPELINE_TERMINAL_STATUSES = frozenset(("SUCCEEDED", "FAILED"))
BATCH_TERMINAL_STATUSES = frozenset(("completed", "failed", "expired", "cancelled"))
ENGINE_TERMINAL_STATES = frozenset(("COMMITTED", "SAFE_STOPPED"))
RECOVERY_ACTIVE_STATES = frozenset(
    (
        "RECOVERY_REQUIRED",
        "RECOVERY_CLAIMED",
        "RECOVERY_FILE_UPLOADED",
        "RECOVERY_PENDING_RECONCILIATION",
        "RECOVERY_SUBMITTED",
        "WAITING",
    )
)
RECOVERY_TERMINAL_STATES = frozenset(
    (
        "RECOVERY_COMPLETED",
        "COMMITTED",
        "SAFE_STOPPED",
        "FAILED",
        "EXPIRED",
        "CANCELLED",
    )
)
TARGET_PROCESS_MARKERS = (
    "run_full_pipeline_managed.sh",
    "run_full_pipeline.sh",
    "run_full_pipeline_phase_b.sh",
    "batch_aws_orchestration.py phase-b",
    "batch_aws_orchestration.py phase-recovery",
)
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
RUN_DATE_RE = re.compile(r"^\d{8}$")
UNIT_RE = re.compile(
    r"^pipeline-ses-(?P<run_date>\d{8})-(?P<run_id>.+)-phase-[ab]\.service$"
)


class PreflightError(RuntimeError):
    """Required evidence is missing, malformed, or contradictory."""


def _required_identity(run_id: Any, run_date: Any, source: str) -> Tuple[str, str]:
    if not isinstance(run_id, str) or not RUN_ID_RE.fullmatch(run_id):
        raise PreflightError(f"{source} run_idが不正です: {run_id!r}")
    if not isinstance(run_date, str) or not RUN_DATE_RE.fullmatch(run_date):
        raise PreflightError(f"{source} run_dateが不正です: {run_date!r}")
    try:
        datetime.strptime(run_date, "%Y%m%d")
    except ValueError as error:
        raise PreflightError(f"{source} run_dateが実在日ではありません: {run_date}") from error
    return run_id, run_date


def _read_json_object(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreflightError(f"JSONを読めません: {path}: {error}") from error
    if not isinstance(value, dict):
        raise PreflightError(f"JSON objectではありません: {path}")
    return value


def _parse_timestamp(value: Any, source: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise PreflightError(f"{source} timestampがありません")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as error:
        raise PreflightError(f"{source} timestampが不正です: {value!r}") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PreflightError(f"{source} timestampにtimezoneがありません")
    return parsed.astimezone(timezone.utc)


def _batch_activity_reasons(state: Optional[Dict[str, Any]]) -> List[str]:
    if state is None:
        return []
    reasons: List[str] = []
    engine_state = str(state.get("state") or "")
    recovery_state = str(state.get("recovery_state") or "")
    if engine_state not in ENGINE_TERMINAL_STATES:
        reasons.append(f"s3_batch_state:{engine_state or 'MISSING'}")
    if state.get("recovery_eligible") is True:
        reasons.append("recovery_eligible:true")
    if recovery_state in RECOVERY_ACTIVE_STATES:
        reasons.append(f"recovery_state:{recovery_state}")
    return reasons


def _batch_terminal_evidence(state: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if state is None:
        return {
            "batch_state_terminal": False,
            "batch_status_terminal": False,
            "recovery_terminal": False,
            "recovery_eligible": None,
            "recovery_eligible_source": "missing_batch_state",
        }

    engine_state = str(state.get("state") or "")
    batch_status = str(state.get("batch_status") or "").lower()
    recovery_state = str(state.get("recovery_state") or "")
    recovery_batch_id = str(state.get("recovery_batch_id") or "")
    eligible = state.get("recovery_eligible")
    eligible_source = "s3_batch_state"
    if eligible is None and engine_state in ENGINE_TERMINAL_STATES:
        # Older SAFE_STOPPED/COMMITTED documents predate this explicit field.
        # Their terminal engine state and lack of an active recovery state prove
        # that recovery is not executable; preserve the source of this default.
        eligible = False
        eligible_source = "terminal_engine_state_default"
    elif not isinstance(eligible, bool):
        eligible_source = "invalid_or_missing"

    recovery_terminal = False
    if eligible is False:
        if not recovery_batch_id and not recovery_state:
            recovery_terminal = True
        elif recovery_state in RECOVERY_TERMINAL_STATES:
            recovery_terminal = batch_status in BATCH_TERMINAL_STATUSES

    return {
        "batch_state_terminal": engine_state in ENGINE_TERMINAL_STATES,
        "batch_status_terminal": batch_status in BATCH_TERMINAL_STATUSES,
        "recovery_terminal": recovery_terminal,
        "recovery_eligible": eligible,
        "recovery_eligible_source": eligible_source,
    }


def classify_run(evidence: Dict[str, Any]) -> Dict[str, Any]:
    """Classify one candidate without mutating any supplied document."""
    run_id, run_date = _required_identity(
        evidence.get("run_id"), evidence.get("run_date"), "evidence"
    )
    local_residue = bool(evidence.get("local_nonterminal_residue"))
    sfn_status = str(evidence.get("step_functions_status") or "")
    pipeline_status = str(evidence.get("s3_pipeline_status") or "")
    process_active = evidence.get("process_active") is True
    batch_state = evidence.get("s3_batch_state")
    if batch_state is not None and not isinstance(batch_state, dict):
        raise PreflightError("s3_batch_stateがJSON objectではありません")

    active_reasons: List[str] = []
    if sfn_status == "RUNNING":
        active_reasons.append("step_functions:RUNNING")
    if process_active:
        active_reasons.append("systemd_or_process:ACTIVE")
    if pipeline_status == "RUNNING":
        active_reasons.append("s3_pipeline_status:RUNNING")
    active_reasons.extend(_batch_activity_reasons(batch_state))

    common = {
        "run_id": run_id,
        "run_date": run_date,
        "local_nonterminal_residue": local_residue,
        "step_functions_status": sfn_status,
        "step_functions_redrive_status": evidence.get("step_functions_redrive_status"),
        "s3_pipeline_status": pipeline_status,
        "process_active": process_active,
    }
    if active_reasons:
        return dict(
            common,
            classification=CLASS_ACTIVE,
            blocks_fresh_run=True,
            reasons=active_reasons,
        )

    terminal = _batch_terminal_evidence(batch_state)
    checks = {
        "step_functions_terminal": sfn_status in SFN_TERMINAL_STATUSES,
        "s3_pipeline_status_terminal": pipeline_status in PIPELINE_TERMINAL_STATUSES,
        "s3_batch_state_terminal": terminal["batch_state_terminal"],
        "batch_terminal": terminal["batch_status_terminal"],
        "recovery_terminal": terminal["recovery_terminal"],
        "recovery_eligible_false": terminal["recovery_eligible"] is False,
        "systemd_process_absent": evidence.get("process_active") is False,
        "local_nonterminal_residue": local_residue,
    }
    if not all(checks.values()):
        failed = sorted(name for name, passed in checks.items() if not passed)
        raise PreflightError(
            f"{run_date}/{run_id} はACTIVEの証明もstale terminalの完全証明もありません: "
            + ",".join(failed)
        )

    return dict(
        common,
        classification=CLASS_STALE,
        blocks_fresh_run=False,
        reasons=["local_only_nonterminal_residue"],
        terminal_checks=checks,
        recovery_eligible=terminal["recovery_eligible"],
        recovery_eligible_source=terminal["recovery_eligible_source"],
    )


def _discover_local_candidates() -> Dict[Tuple[str, str], Dict[str, Any]]:
    candidates: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for path in sorted(MANAGED_ROOT.glob("*/*/status.json")):
        document = _read_json_object(path)
        if document.get("status") != "RUNNING":
            continue
        identity = _required_identity(document.get("run_id"), document.get("run_date"), str(path))
        candidates.setdefault(identity, {})["local_pipeline_status"] = document
        candidates[identity]["local_pipeline_path"] = str(path)

    for path in sorted(BATCH_RUNTIME_ROOT.glob("*/batch_state.json")):
        document = _read_json_object(path)
        if not _batch_activity_reasons(document):
            continue
        identity = _required_identity(
            document.get("pipeline_run_id"), document.get("run_date"), str(path)
        )
        candidates.setdefault(identity, {})["local_batch_state"] = document
        candidates[identity]["local_batch_path"] = str(path)
    return candidates


def _s3_json(s3: Any, key: str) -> Optional[Dict[str, Any]]:
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key)
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404", "NotFound"):
            return None
        raise
    body = response.get("Body")
    if body is None:
        raise PreflightError(f"S3 object bodyがありません: {key}")
    try:
        value = json.loads(body.read().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreflightError(f"S3 JSONが不正です: {key}") from error
    if not isinstance(value, dict):
        raise PreflightError(f"S3 JSON objectではありません: {key}")
    return value


def _list_s3_json(s3: Any, prefix: str, suffix: str) -> Iterable[Tuple[str, Dict[str, Any]]]:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix.rstrip("/") + "/"):
        for item in page.get("Contents", []):
            key = str(item.get("Key") or "")
            if not key.endswith(suffix):
                continue
            document = _s3_json(s3, key)
            if document is not None:
                yield key, document


def _prepare_run_identity(stepfunctions: Any, execution_arn: str) -> Optional[Tuple[str, str]]:
    response = stepfunctions.get_execution_history(
        executionArn=execution_arn, maxResults=25, reverseOrder=False
    )
    for event in response.get("events", []):
        details = event.get("stateExitedEventDetails")
        if not isinstance(details, dict) or details.get("name") != "PrepareRunContext":
            continue
        if details.get("outputDetails", {}).get("truncated") is True:
            raise PreflightError("PrepareRunContext outputがtruncatedです")
        try:
            output = json.loads(details.get("output", ""))
        except json.JSONDecodeError as error:
            raise PreflightError("PrepareRunContext outputが不正です") from error
        if not isinstance(output, dict):
            raise PreflightError("PrepareRunContext outputがobjectではありません")
        return _required_identity(output.get("run_id"), output.get("run_date"), execution_arn)
    return None


def _candidate_times(candidates: Dict[Tuple[str, str], Dict[str, Any]]) -> List[datetime]:
    values: List[datetime] = []
    for item in candidates.values():
        status = item.get("local_pipeline_status")
        if isinstance(status, dict) and status.get("started_at"):
            values.append(_parse_timestamp(status["started_at"], "local started_at"))
            continue
        batch = item.get("local_batch_state")
        if isinstance(batch, dict) and batch.get("prepared_at"):
            values.append(_parse_timestamp(batch["prepared_at"], "local prepared_at"))
    return values


def _execution_index(
    stepfunctions: Any,
    candidates: Dict[Tuple[str, str], Dict[str, Any]],
) -> Tuple[Dict[Tuple[str, str], Dict[str, Any]], List[Dict[str, Any]]]:
    executions: List[Dict[str, Any]] = []
    paginator = stepfunctions.get_paginator("list_executions")
    for page in paginator.paginate(stateMachineArn=STATE_MACHINE_ARN):
        executions.extend(page.get("executions", []))

    candidate_times = _candidate_times(candidates)
    index: Dict[Tuple[str, str], Dict[str, Any]] = {}
    running: List[Dict[str, Any]] = []
    for execution in executions:
        status = str(execution.get("status") or "")
        arn = str(execution.get("executionArn") or "")
        if status == "RUNNING":
            running.append(execution)
        start = execution.get("startDate")
        if not isinstance(start, datetime) or not arn:
            continue
        start_utc = start.astimezone(timezone.utc)
        close_to_candidate = any(
            moment - timedelta(minutes=30) <= start_utc <= moment + timedelta(minutes=5)
            for moment in candidate_times
        )
        if not close_to_candidate and status != "RUNNING":
            continue
        identity = _prepare_run_identity(stepfunctions, arn)
        if identity is None:
            if status == "RUNNING":
                continue
            continue
        if identity in index:
            raise PreflightError(f"Step Functions execution identityが重複しています: {identity!r}")
        description = stepfunctions.describe_execution(executionArn=arn)
        index[identity] = {
            "execution_arn": arn,
            "status": str(description.get("status") or status),
            "redrive_status": description.get("redriveStatus"),
        }
    return index, running


def _active_process_snapshot() -> Tuple[Set[Tuple[str, str]], List[str]]:
    identities: Set[Tuple[str, str]] = set()
    unattributed: List[str] = []
    systemctl = subprocess.run(
        [
            "/usr/bin/systemctl",
            "list-units",
            "pipeline-ses-*",
            "--state=active,activating,reloading",
            "--all",
            "--plain",
            "--no-legend",
            "--no-pager",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if systemctl.returncode != 0:
        raise PreflightError(f"systemd active unit確認に失敗しました: {systemctl.stderr.strip()}")
    for line in systemctl.stdout.splitlines():
        unit = line.split(maxsplit=1)[0] if line.strip() else ""
        match = UNIT_RE.fullmatch(unit)
        if match is None:
            if unit:
                unattributed.append(f"systemd:{unit}")
            continue
        identities.add((match.group("run_id"), match.group("run_date")))

    processes = subprocess.run(
        ["/usr/bin/ps", "-eo", "args="],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if processes.returncode != 0:
        raise PreflightError(f"pipeline process確認に失敗しました: {processes.stderr.strip()}")
    for command in processes.stdout.splitlines():
        if not any(marker in command for marker in TARGET_PROCESS_MARKERS):
            continue
        run_id_match = re.search(r"(?:RUN_ID=|--pipeline-run-id\s+)([A-Za-z0-9._-]+)", command)
        run_date_match = re.search(r"(?:RUN_DATE=|--run-date\s+)(\d{8})", command)
        if run_id_match and run_date_match:
            identities.add((run_id_match.group(1), run_date_match.group(1)))
        else:
            unattributed.append(f"process:{command[:200]}")
    return identities, unattributed


def collect_live_report() -> Dict[str, Any]:
    """Collect current read-only evidence and return the Fresh Run decision."""
    candidates = _discover_local_candidates()
    s3 = boto3.client("s3", region_name=REGION)
    stepfunctions = boto3.client("stepfunctions", region_name=REGION)

    # Remote-only RUNNING/nonterminal records must also block Fresh Run.
    for _, document in _list_s3_json(s3, PIPELINE_STATUS_PREFIX, "/status.json"):
        if document.get("status") != "RUNNING":
            continue
        identity = _required_identity(
            document.get("run_id"), document.get("run_date"), "S3 pipeline-status"
        )
        candidates.setdefault(identity, {})["remote_pipeline_status"] = document
    for _, document in _list_s3_json(s3, BATCH_STATE_PREFIX, "/state.json"):
        if not _batch_activity_reasons(document):
            continue
        identity = _required_identity(
            document.get("pipeline_run_id"), document.get("run_date"), "S3 Batch state"
        )
        candidates.setdefault(identity, {})["remote_batch_state"] = document

    execution_index, running_executions = _execution_index(stepfunctions, candidates)
    process_identities, unattributed_processes = _active_process_snapshot()
    results: List[Dict[str, Any]] = []
    matched_running_arns: Set[str] = set()

    for (run_id, run_date), candidate in sorted(candidates.items(), key=lambda item: item[0][::-1]):
        pipeline_key = f"{PIPELINE_STATUS_PREFIX}/{run_date}/{run_id}/status.json"
        batch_key = f"{BATCH_STATE_PREFIX}/{run_date}/{run_id}/state.json"
        remote_pipeline = candidate.get("remote_pipeline_status") or _s3_json(s3, pipeline_key)
        remote_batch = candidate.get("remote_batch_state") or _s3_json(s3, batch_key)
        execution = execution_index.get((run_id, run_date), {})
        if execution.get("status") == "RUNNING":
            matched_running_arns.add(str(execution.get("execution_arn") or ""))
        local_residue = bool(
            candidate.get("local_pipeline_status") or candidate.get("local_batch_state")
        )
        evidence = {
            "run_id": run_id,
            "run_date": run_date,
            "local_nonterminal_residue": local_residue,
            "step_functions_status": execution.get("status"),
            "step_functions_redrive_status": execution.get("redrive_status"),
            "s3_pipeline_status": (
                remote_pipeline.get("status") if isinstance(remote_pipeline, dict) else None
            ),
            "s3_batch_state": remote_batch,
            "process_active": (run_id, run_date) in process_identities,
        }
        result = classify_run(evidence)
        result["local_pipeline_path"] = candidate.get("local_pipeline_path")
        result["local_batch_path"] = candidate.get("local_batch_path")
        result["execution_arn"] = execution.get("execution_arn")
        results.append(result)

    global_active = list(unattributed_processes)
    for execution in running_executions:
        arn = str(execution.get("executionArn") or "")
        if arn not in matched_running_arns:
            global_active.append(f"step_functions:{arn}")
    active_count = sum(item["classification"] == CLASS_ACTIVE for item in results)
    stale_count = sum(item["classification"] == CLASS_STALE for item in results)
    return {
        "preflight": "BLOCKED" if active_count or global_active else "READY",
        "fresh_run_allowed": not active_count and not global_active,
        "candidate_count": len(results),
        "active_count": active_count,
        "stale_terminal_local_residue_count": stale_count,
        "global_active_evidence": global_active,
        "runs": results,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--json",
        action="store_true",
        help="判定reportをJSONで表示する（常にread-only）。",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    logger = get_logger(STEP_NAME)
    try:
        report = collect_live_report()
    except Exception as error:  # noqa: BLE001 - preflight must fail closed
        logger.error(f"Fresh Run preflight evidence確認失敗: {type(error).__name__}: {error}")
        return 2

    for item in report["runs"]:
        identity = f"{item['run_date']}/{item['run_id']}"
        if item["classification"] == CLASS_STALE:
            logger.warn(
                f"{identity} はstale terminal local residueです。"
                "過去local JSONを変更せずFresh Run blocking対象から除外します"
            )
        else:
            logger.error(f"{identity} はACTIVEです: {','.join(item['reasons'])}")
    for reason in report["global_active_evidence"]:
        logger.error(f"identity未解決のactive evidence: {reason}")
    if args.json:
        logger.info(json.dumps(report, ensure_ascii=False, sort_keys=True))
    if report["fresh_run_allowed"]:
        logger.ok(
            "Fresh Run preflight READY "
            f"(candidates={report['candidate_count']} stale={report['stale_terminal_local_residue_count']})"
        )
        return 0
    logger.error(
        "Fresh Run preflight BLOCKED "
        f"(active={report['active_count']} global_active={len(report['global_active_evidence'])})"
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())

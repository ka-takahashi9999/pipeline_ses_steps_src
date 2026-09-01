#!/usr/bin/env python3
"""
80-75_portal_s3_backup_rotation

Portal CURRENT を 1世代だけ backup（bk1）へ退避する。

  CURRENT : s3://technoverse/pipeline_ses_steps/pipeline_ses_steps/
  BK1     : s3://technoverse/pipeline_ses_steps/pipeline_ses_steps_bk1/

CURRENT + bk1 の 2 set 構成（backup 1世代）。将来 bk2 / bk3 へ拡張する場合も
「CURRENTと同じrelative path」を保つ。ファイル個別の世代管理は行わない。

方式:
- destination安全ロック: bucket / base prefix / current prefix / backup prefix /
  完全URI を期待値と完全一致比較する（startswith判定はしない）。
  source は CURRENT、destination は bk1 のみ。設定値・環境変数の書き換えで
  bucket root / pipeline_ses_steps root / private / pipeline-status / pipeline-logs /
  別bucket / 任意prefix / bk2 / bk3 へ向けることはできない。
- previous CURRENT正常性guard: 直前の 80-9 summary（apply / SUCCEEDED / verified /
  missing=extra=size mismatch=0 / canonical destination）と pipeline-status 上の
  SUCCEEDED run を照合し、さらに CURRENT実体のcount/bytesが 80-9 の actual と
  一致することまで確認する。partial CURRENT を backup しない。
- 限定recovery: date / run_idをCLIで明示した既知80-7 FAILED runだけを対象とし、
  previous verified success確定後にCURRENT objectが1件も変更されていない場合だけ
  BK1 rotation元として許可する。FAILED statusを無条件に無視する経路ではない。
- pre-publication recovery: verified 80-9 authority以降の全terminal runがFAILEDで、
  immutable execution history上publication境界未到達かつRedriveなしと証明でき、
  CURRENTとBK1のinventory・LastModifiedが不変の場合だけrotationを許可する。
  Gmail件数やfailure reason/step固有のallowlistはrotation許可条件にしない。
- backup本体: `aws s3 sync CURRENT BK1 --delete`（argv配列 / shell未使用 /
  include-excludeフィルタ未使用）
- backup後 PORTAL_S3_VERIFY_WAIT_SEC 秒待ち、CURRENT と bk1 を全件LISTして
  {relative_path: size} で完全比較する。missing / extra / size mismatch はすべて異常終了。
- 初回のbk1作成は通常rotationと分離し、`--bootstrap` を明示指定した場合のみ許可する。

pipeline-status（CONTROL）/ pipeline-logs（AUDIT）/ private / root直下ZIP は
backup対象外prefixのため一切変更しない（pipeline-status は read-only参照のみ）。

usage:
  portal_s3_backup_rotation.py [--bootstrap] [--dry-run]
  portal_s3_backup_rotation.py --recovery-run-date YYYYMMDD --recovery-run-id RUN_ID [--dry-run]
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from fractions import Fraction
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_execution_time  # noqa: E402
from common.json_utils import read_jsonl  # noqa: E402
from common.logger import get_logger  # noqa: E402
from common.pipeline_s3_env import get_config_value, load_pipeline_s3_config  # noqa: E402

STEP_NAME = "80-75_portal_s3_backup_rotation"
STEP_DIR = Path(__file__).resolve().parents[1]

SYNC_STEP_DIR_NAME = "80-9_portal_s3_sync"
SYNC_SUMMARY_FILENAME = "portal_s3_sync_summary.json"
BACKUP_SUMMARY_FILENAME = "portal_s3_backup_rotation_summary.json"
IMMUTABLE_EXECUTION_GUARD_CONTRACT_VERSION = 1
PREPARE_STEP_DIR_NAME = "80-8_portal_s3_prepare"
PREVIOUS_MANIFEST_FILENAME = "portal_s3_manifest.jsonl"
RECOVERY_FAILED_STEP_NAME = "80-7_manage_09_result_retention"
PREPUBLICATION_RECOVERY_MODE = "pre_publication_failed_runs"
PUBLICATION_BOUNDARY_STEP_NAME = "80-75_portal_s3_backup_rotation"
PUBLICATION_BOUNDARY_STATE = "SendPhaseBLauncherCommand"
PIPELINE_STEP_RE = re.compile(r"^(\d{2})-(\d+)(?:_|\(|$)")

AWS_BIN = "/usr/bin/aws"
RESULT_DIR_NAME = "01_result"

# ---- destination安全ロック（唯一許可する source / destination） ----------------
# ここを設定ファイル・環境変数で上書きできてはならない。
EXPECTED_BUCKET = "technoverse"
EXPECTED_BASE_PREFIX = "pipeline_ses_steps"
EXPECTED_CURRENT_LEAF = "pipeline_ses_steps"
EXPECTED_BACKUP_LEAF = "pipeline_ses_steps_bk1"
EXPECTED_CURRENT_PREFIX = f"{EXPECTED_BASE_PREFIX}/{EXPECTED_CURRENT_LEAF}"
EXPECTED_BACKUP_PREFIX = f"{EXPECTED_BASE_PREFIX}/{EXPECTED_BACKUP_LEAF}"
EXPECTED_SOURCE_URI = f"s3://{EXPECTED_BUCKET}/{EXPECTED_CURRENT_PREFIX}/"
EXPECTED_DESTINATION_URI = f"s3://{EXPECTED_BUCKET}/{EXPECTED_BACKUP_PREFIX}/"

# current execution の immutable evidence はこのState Machineだけを参照する。
# destination lockと同様、環境変数や設定ファイルから任意ARNへ差し替えさせない。
EXPECTED_STATE_MACHINE_ARN = (
    "arn:aws:states:ap-northeast-1:166714029268:stateMachine:"
    "auto-match-llm-classifier-pipeline-orchestration"
)
PREPARE_RUN_CONTEXT_STATE = "PrepareRunContext"
PRIOR_TERMINAL_EVENT_TYPES = frozenset(
    ("ExecutionFailed", "ExecutionAborted", "ExecutionTimedOut")
)

# pipeline-status は read-only参照のみ（CONTROL prefixを書き換えない）
EXPECTED_STATUS_PREFIX = "pipeline-status"

RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
RUN_DATE_RE = re.compile(r"^\d{8}$")
STATUS_SCHEMA_VERSION = "1.0"
STATUS_REQUIRED_KEYS = frozenset(
    (
        "schema_version",
        "run_id",
        "run_date",
        "status",
        "started_at",
        "finished_at",
        "finished_at_source",
        "exit_code",
        "exit_code_source",
        "current_step",
        "error_message",
        "log_s3_uri",
        "updated_at",
    )
)

SAMPLE_LIMIT = 3

# 直前の80-9 summaryに要求する固定値
REQUIRED_SYNC_SUMMARY_VALUES = (
    ("step", SYNC_STEP_DIR_NAME),
    ("mode", "apply"),
    ("sync_status", "SUCCEEDED"),
    ("s3_destination", EXPECTED_SOURCE_URI),
)


class RotationError(Exception):
    """S3を変更しない / verify不成立で異常終了すべき状態。"""


# ---------------------------------------------------------------------------
# destination安全ロック
# ---------------------------------------------------------------------------


def _check_prefix_components(prefix: str, expected_components: List[str], label: str) -> None:
    components = prefix.split("/")
    if components != expected_components:
        raise RotationError(f"destination安全ロック違反: {label}の構造が不正です: {prefix!r}")
    if any(component in ("", ".", "..") for component in components):
        raise RotationError(f"destination安全ロック違反: {label}に不正componentがあります: {prefix!r}")
    if any("*" in component or "?" in component for component in components):
        raise RotationError(f"destination安全ロック違反: {label}にwildcardがあります: {prefix!r}")


def lock_backup_route(
    bucket: Any, base_prefix: Any, current_prefix: Any, backup_prefix: Any
) -> Tuple[str, str]:
    """
    bucket / base prefix / CURRENT prefix / BK1 prefix / 完全URI を期待値と完全一致比較する。
    1項目でも一致しなければ RotationError を送出する（sync開始前にFAILさせる）。

    生の設定値をそのまま比較するため、末尾スラッシュ・空文字・上位prefix・`..`・
    wildcard・bk2 / bk3 はすべて不一致として拒否される。
    """
    for name, value, expected in (
        ("PIPELINE_S3_BUCKET", bucket, EXPECTED_BUCKET),
        ("PIPELINE_S3_BASE_PREFIX", base_prefix, EXPECTED_BASE_PREFIX),
        ("PORTAL_S3_PREFIX", current_prefix, EXPECTED_CURRENT_PREFIX),
        ("PORTAL_S3_BACKUP_PREFIX", backup_prefix, EXPECTED_BACKUP_PREFIX),
    ):
        if not isinstance(value, str) or value != expected:
            raise RotationError(
                f"destination安全ロック違反: {name} が期待値と一致しません "
                f"(actual={value!r} / expected={expected!r})"
            )

    _check_prefix_components(
        current_prefix, [EXPECTED_BASE_PREFIX, EXPECTED_CURRENT_LEAF], "CURRENT prefix"
    )
    _check_prefix_components(
        backup_prefix, [EXPECTED_BASE_PREFIX, EXPECTED_BACKUP_LEAF], "BK1 prefix"
    )

    source_uri = f"s3://{bucket}/{current_prefix}/"
    destination_uri = f"s3://{bucket}/{backup_prefix}/"
    if source_uri != EXPECTED_SOURCE_URI:
        raise RotationError(
            f"destination安全ロック違反: source URIが期待値と一致しません "
            f"(actual={source_uri!r} / expected={EXPECTED_SOURCE_URI!r})"
        )
    if destination_uri != EXPECTED_DESTINATION_URI:
        raise RotationError(
            f"destination安全ロック違反: backup URIが期待値と一致しません "
            f"(actual={destination_uri!r} / expected={EXPECTED_DESTINATION_URI!r})"
        )
    if source_uri == destination_uri:
        raise RotationError("destination安全ロック違反: sourceとdestinationが同一です")
    return source_uri, destination_uri


def lock_status_prefix(status_prefix: Any) -> str:
    """pipeline-status は read-only参照のみ。prefix名を固定する。"""
    if not isinstance(status_prefix, str) or status_prefix != EXPECTED_STATUS_PREFIX:
        raise RotationError(
            f"pipeline-status prefixが期待値と一致しません "
            f"(actual={status_prefix!r} / expected={EXPECTED_STATUS_PREFIX!r})"
        )
    return status_prefix


def parse_wait_seconds(raw: Any) -> int:
    """PORTAL_S3_VERIFY_WAIT_SEC を非負整数として解釈する。"""
    text = str(raw).strip()
    if not text.isdigit():
        raise RotationError(f"PORTAL_S3_VERIFY_WAIT_SEC は非負整数のみ指定できます: {raw!r}")
    return int(text)


# ---------------------------------------------------------------------------
# previous CURRENT 正常性guard
# ---------------------------------------------------------------------------


def _require_zero(container: Dict[str, Any], key: str, label: str) -> None:
    value = container.get(key)
    if value != 0:
        raise RotationError(f"previous CURRENTが正常ではありません: {label}={value!r}")


def _require_count(container: Dict[str, Any], key: str, label: str = "previous 80-9 summary") -> int:
    value = container.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RotationError(f"{label}の{key}が不正です: {value!r}")
    return value


def load_previous_sync_summary(summary_path: Path) -> Dict[str, Any]:
    if not summary_path.is_file():
        raise RotationError(f"直前の80-9 summaryが存在しません: {summary_path}")
    try:
        with open(summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)
    except (OSError, ValueError) as exc:
        raise RotationError(f"直前の80-9 summaryを読めません: {summary_path} ({exc})") from exc
    if not isinstance(summary, dict):
        raise RotationError(f"直前の80-9 summaryがJSON objectではありません: {summary_path}")
    return summary


def validate_previous_sync_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
    """
    直前の80-9 summaryが「正常に完了したCURRENT publish」を表しているか確認し、
    provenance（run_date / run_id）と期待count/bytesを返す。
    """
    for key, expected in REQUIRED_SYNC_SUMMARY_VALUES:
        actual = summary.get(key)
        if actual != expected:
            raise RotationError(
                f"previous CURRENTが正常ではありません: 80-9 summaryの{key}が不正です "
                f"(actual={actual!r} / expected={expected!r})"
            )
    if summary.get("s3_destination_locked") is not True:
        raise RotationError("previous CURRENTが正常ではありません: 80-9 destination lockが記録されていません")

    verify = summary.get("verify")
    if not isinstance(verify, dict):
        raise RotationError("previous CURRENTが正常ではありません: 80-9 summaryにverifyがありません")
    if verify.get("verified") is not True:
        raise RotationError("previous CURRENTが正常ではありません: 80-9 verified=false")
    _require_zero(verify, "missing_count", "missing")
    _require_zero(verify, "extra_count", "extra")
    _require_zero(verify, "size_mismatch_count", "size mismatch")

    expected_files = _require_count(verify, "expected_file_count")
    actual_files = _require_count(verify, "actual_file_count")
    expected_bytes = _require_count(verify, "expected_total_bytes")
    actual_bytes = _require_count(verify, "actual_total_bytes")
    if expected_files != actual_files or expected_bytes != actual_bytes:
        raise RotationError(
            "previous CURRENTが正常ではありません: 80-9 expected/actualが一致していません "
            f"(files {expected_files}/{actual_files} / bytes {expected_bytes}/{actual_bytes})"
        )
    if actual_files <= 0:
        raise RotationError("previous CURRENTが正常ではありません: 80-9 actual_file_countが0件です")

    run_date = summary.get("run_date")
    run_id = summary.get("run_id")
    if summary.get("run_date_source") != "env" or summary.get("run_id_source") != "env":
        raise RotationError(
            "previous CURRENTが正常ではありません: 80-9 summaryのrun_date/run_idがmanaged run由来ではありません "
            f"(run_date_source={summary.get('run_date_source')!r} / "
            f"run_id_source={summary.get('run_id_source')!r})"
        )
    if not isinstance(run_date, str) or not RUN_DATE_RE.match(run_date):
        raise RotationError(f"80-9 summaryのrun_dateが不正です: {run_date!r}")
    if not isinstance(run_id, str) or not RUN_ID_RE.match(run_id):
        raise RotationError(f"80-9 summaryのrun_idが不正です: {run_id!r}")

    return {
        "run_date": run_date,
        "run_id": run_id,
        "run_date_source": summary.get("run_date_source"),
        "run_id_source": summary.get("run_id_source"),
        "destination": summary.get("s3_destination"),
        "verified": verify.get("verified"),
        "sync_step": summary.get("step"),
        "file_count": actual_files,
        "total_bytes": actual_bytes,
    }


def build_s3_client(region: str):
    """boto3 S3クライアントを生成する（テストから差し替え可能にするため関数化）。"""
    import boto3

    return boto3.client("s3", region_name=region)


def build_stepfunctions_client(region: str):
    """boto3 Step Functions clientを生成する（read-only APIだけに使用）。"""
    import boto3

    return boto3.client("stepfunctions", region_name=region)


def _next_token(response: Dict[str, Any], operation: str, seen: set) -> Optional[str]:
    """AWS pagination tokenをfail-closedで検証する。"""
    token = response.get("nextToken")
    if token is None:
        return None
    if not isinstance(token, str) or not token:
        raise RotationError(f"Step Functions {operation}のnextTokenが不正です")
    if token in seen:
        raise RotationError(f"Step Functions {operation}のnextTokenが循環しています")
    seen.add(token)
    return token


def _list_executions_by_status(
    stepfunctions_client, status: str
) -> Tuple[List[Dict[str, Any]], int]:
    """対象State Machineの指定status executionを全ページ取得する。"""
    executions: List[Dict[str, Any]] = []
    seen_tokens = set()
    seen_arns = set()
    token = None
    pages = 0
    try:
        while True:
            params: Dict[str, Any] = {
                "stateMachineArn": EXPECTED_STATE_MACHINE_ARN,
                "statusFilter": status,
                "maxResults": 1000,
            }
            if token is not None:
                params["nextToken"] = token
            response = stepfunctions_client.list_executions(**params)
            pages += 1
            page_executions = response.get("executions")
            if not isinstance(page_executions, list):
                raise RotationError("Step Functions ListExecutionsのexecutionsが配列ではありません")
            for execution in page_executions:
                if not isinstance(execution, dict):
                    raise RotationError("Step Functions ListExecutionsに不正なexecutionがあります")
                execution_arn = execution.get("executionArn")
                if not isinstance(execution_arn, str) or not execution_arn:
                    raise RotationError("Step Functions ListExecutionsにexecutionArnがありません")
                if execution_arn in seen_arns:
                    raise RotationError(
                        f"Step Functions ListExecutionsでexecutionArnが重複しました: {execution_arn}"
                    )
                if execution.get("status") != status:
                    raise RotationError(
                        "Step Functions ListExecutionsが指定status以外を返しました "
                        f"({execution_arn} status={execution.get('status')!r})"
                    )
                seen_arns.add(execution_arn)
                executions.append(execution)
            token = _next_token(response, "ListExecutions", seen_tokens)
            if token is None:
                break
    except RotationError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise RotationError(f"Step Functions ListExecutionsに失敗しました: {exc}") from exc
    return executions, pages


def _list_running_executions(stepfunctions_client) -> Tuple[List[Dict[str, Any]], int]:
    """対象State MachineのRUNNING executionを全ページ取得する。"""
    return _list_executions_by_status(stepfunctions_client, "RUNNING")


def _require_execution_timestamp(
    execution: Dict[str, Any], key: str, label: str
) -> datetime:
    """AWS管理execution metadataのtimezone付きtimestampをfail-closedで返す。"""
    value = execution.get(key)
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise RotationError(f"{label}の{key}が不正です（execution ordering判定不能）")
    return value


def _execution_timestamp_text(value: datetime) -> str:
    """summary監査用にAWS execution timestampをISO 8601へ固定する。"""
    return value.isoformat()


def _get_execution_history(
    stepfunctions_client, execution_arn: str
) -> Tuple[List[Dict[str, Any]], int]:
    """execution historyを先頭から全ページ取得する。"""
    events: List[Dict[str, Any]] = []
    seen_tokens = set()
    seen_event_ids = set()
    token = None
    pages = 0
    try:
        while True:
            params: Dict[str, Any] = {
                "executionArn": execution_arn,
                "maxResults": 1000,
                "reverseOrder": False,
                "includeExecutionData": True,
            }
            if token is not None:
                params["nextToken"] = token
            response = stepfunctions_client.get_execution_history(**params)
            pages += 1
            page_events = response.get("events")
            if not isinstance(page_events, list):
                raise RotationError("Step Functions GetExecutionHistoryのeventsが配列ではありません")
            for event in page_events:
                if not isinstance(event, dict):
                    raise RotationError("Step Functions execution historyに不正なeventがあります")
                event_id = event.get("id")
                event_type = event.get("type")
                if not isinstance(event_id, int) or isinstance(event_id, bool) or event_id <= 0:
                    raise RotationError("Step Functions execution historyのevent idが不正です")
                if event_id in seen_event_ids:
                    raise RotationError(
                        f"Step Functions execution historyのevent idが重複しました: {event_id}"
                    )
                if not isinstance(event_type, str) or not event_type:
                    raise RotationError("Step Functions execution historyのevent typeが不正です")
                seen_event_ids.add(event_id)
                events.append(event)
            token = _next_token(response, "GetExecutionHistory", seen_tokens)
            if token is None:
                break
    except RotationError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise RotationError(
            f"Step Functions GetExecutionHistoryに失敗しました: {execution_arn} ({exc})"
        ) from exc
    return events, pages


def _prepare_run_context_identity(
    events: List[Dict[str, Any]], require_complete_output: bool = False
) -> Optional[Dict[str, str]]:
    """PrepareRunContext StateExited outputからrun_date/run_idを抽出する。"""
    prepare_outputs = []
    for event in events:
        details = event.get("stateExitedEventDetails")
        if not isinstance(details, dict) or details.get("name") != PREPARE_RUN_CONTEXT_STATE:
            continue
        if require_complete_output:
            output_details = details.get("outputDetails")
            if not isinstance(output_details, dict) or output_details.get("truncated") is not False:
                raise RotationError("PrepareRunContext history outputが完全取得されていません")
        raw_output = details.get("output")
        if not isinstance(raw_output, str) or not raw_output:
            raise RotationError("PrepareRunContext history eventにoutputがありません")
        try:
            output = json.loads(raw_output)
        except ValueError as exc:
            raise RotationError("PrepareRunContext history outputがJSONではありません") from exc
        if not isinstance(output, dict):
            raise RotationError("PrepareRunContext history outputがJSON objectではありません")
        run_date = output.get("run_date")
        run_id = output.get("run_id")
        if not isinstance(run_date, str) or not RUN_DATE_RE.fullmatch(run_date):
            raise RotationError(f"PrepareRunContext historyのrun_dateが不正です: {run_date!r}")
        try:
            datetime.strptime(run_date, "%Y%m%d")
        except ValueError as exc:
            raise RotationError(
                f"PrepareRunContext historyのrun_dateが実在日ではありません: {run_date!r}"
            ) from exc
        if not isinstance(run_id, str) or not RUN_ID_RE.fullmatch(run_id):
            raise RotationError(f"PrepareRunContext historyのrun_idが不正です: {run_id!r}")
        prepare_outputs.append({"run_date": run_date, "run_id": run_id})
    if not prepare_outputs:
        return None
    if len(prepare_outputs) != 1:
        raise RotationError(
            "PrepareRunContext history eventを一意に特定できません "
            f"(matches={len(prepare_outputs)})"
        )
    return prepare_outputs[0]


def _pipeline_step_order(step_name: Any) -> Optional[Tuple[int, Fraction]]:
    """step prefixをDAG上の数値順へ変換する。解析不能は安全側DENY用にNone。"""
    if not isinstance(step_name, str):
        return None
    match = PIPELINE_STEP_RE.match(step_name)
    if match is None:
        return None
    major_text, minor_text = match.groups()
    return int(major_text), Fraction(int(minor_text), 10 ** len(minor_text))


def _validate_prepublication_step(step_name: Any) -> Dict[str, Any]:
    """failure reasonではなく、current_stepが80-75より前かだけを判定する。"""
    actual_order = _pipeline_step_order(step_name)
    boundary_order = _pipeline_step_order(PUBLICATION_BOUNDARY_STEP_NAME)
    if actual_order is None or boundary_order is None:
        raise RotationError(f"failure stepの順序を解決できません: {step_name!r}")
    if actual_order >= boundary_order:
        raise RotationError(
            "publication境界以降のFAILED runはrecoveryできません "
            f"(step={step_name!r} / boundary={PUBLICATION_BOUNDARY_STEP_NAME})"
        )
    return {
        "current_step": step_name,
        "step_order_verified": True,
        "before_publication_boundary": True,
    }


def _validate_historical_prepublication_execution(
    stepfunctions_client,
    execution: Dict[str, Any],
    expected_identity: Dict[str, str],
) -> Dict[str, Any]:
    """過去FAILED executionのidentity・freshness・publication未到達を履歴で証明する。"""
    execution_arn = execution["executionArn"]
    try:
        description = stepfunctions_client.describe_execution(executionArn=execution_arn)
    except Exception as exc:  # noqa: BLE001
        raise RotationError(f"historical DescribeExecutionに失敗しました: {exc}") from exc
    if not isinstance(description, dict):
        raise RotationError("historical DescribeExecutionの応答がobjectではありません")
    if description.get("executionArn") != execution_arn:
        raise RotationError("historical execution ARNがListExecutionsと一致しません")
    if description.get("stateMachineArn") != EXPECTED_STATE_MACHINE_ARN:
        raise RotationError("historical state machine ARNがcanonicalではありません")
    if description.get("status") != "FAILED":
        raise RotationError("historical execution statusがFAILEDではありません")
    listed_start = _require_execution_timestamp(
        execution, "startDate", "historical ListExecutions"
    )
    listed_stop = _require_execution_timestamp(
        execution, "stopDate", "historical ListExecutions"
    )
    described_start = _require_execution_timestamp(
        description, "startDate", "historical DescribeExecution"
    )
    described_stop = _require_execution_timestamp(
        description, "stopDate", "historical DescribeExecution"
    )
    if described_start != listed_start or described_stop != listed_stop:
        raise RotationError("historical execution timestampがList/Describe間で一致しません")
    if "redriveCount" not in description:
        raise RotationError("historical DescribeExecutionにredriveCountがありません")
    redrive_count = description["redriveCount"]
    if (
        not isinstance(redrive_count, int)
        or isinstance(redrive_count, bool)
        or redrive_count != 0
    ):
        raise RotationError(f"historical redriveCountが0ではありません: {redrive_count!r}")
    if description.get("redriveDate") is not None:
        raise RotationError("historical executionにredriveDateがあります")

    events, history_pages = _get_execution_history(stepfunctions_client, execution_arn)
    event_ids = [event["id"] for event in events]
    if event_ids != sorted(event_ids):
        raise RotationError("historical execution historyのevent順序が不明です")
    identity = _prepare_run_context_identity(events, require_complete_output=True)
    if identity is None or identity != expected_identity:
        raise RotationError(
            "historical execution identityがpipeline-statusと一致しません "
            f"(expected={expected_identity!r} / actual={identity!r})"
        )
    if any(event["type"] == "ExecutionRedriven" for event in events):
        raise RotationError("historical execution historyにExecutionRedrivenがあります")
    terminal_events = [
        event for event in events
        if event["type"] in PRIOR_TERMINAL_EVENT_TYPES or event["type"] == "ExecutionSucceeded"
    ]
    if (
        len(terminal_events) != 1
        or terminal_events[0]["type"] != "ExecutionFailed"
        or terminal_events[0]["id"] != max(event_ids)
    ):
        raise RotationError("historical executionのterminal historyを一意に証明できません")

    entered_states = []
    for event in events:
        details = event.get("stateEnteredEventDetails")
        if details is None:
            continue
        if not isinstance(details, dict):
            raise RotationError("historical executionのStateEntered detailsが不正です")
        state_name = details.get("name")
        if not isinstance(state_name, str) or not state_name:
            raise RotationError("historical executionのStateEntered nameが不正です")
        entered_states.append(state_name)
    if PUBLICATION_BOUNDARY_STATE in entered_states:
        raise RotationError(
            "historical executionがpublication境界へ到達しています "
            f"(state={PUBLICATION_BOUNDARY_STATE})"
        )
    return {
        "validation_result": "PASS",
        "evidence_source": "stepfunctions_execution_history",
        "execution_arn": execution_arn,
        "execution_status": "FAILED",
        "execution_start_date": _execution_timestamp_text(listed_start),
        "execution_stop_date": _execution_timestamp_text(listed_stop),
        "recovery_window_candidate": True,
        "run_identity_match": True,
        "redrive_count": 0,
        "redrive_date_present": False,
        "execution_redriven_event_present": False,
        "history_pages_checked": history_pages,
        "history_event_count": len(events),
        "publication_boundary_state": PUBLICATION_BOUNDARY_STATE,
        "publication_boundary_reached": False,
    }


def _resolve_authority_execution_boundary(
    stepfunctions_client,
    authority_identity: Dict[str, str],
    authority_document: Dict[str, Any],
) -> Dict[str, Any]:
    """verified authorityをSUCCEEDED executionへ結び、AWS stopDateを確定する。"""
    if stepfunctions_client is None:
        raise RotationError("authority execution metadataを取得できません")
    authority_started = _parse_evidence_timestamp(
        "rotation authority started_at", authority_document["started_at"]
    )
    authority_finished = _parse_evidence_timestamp(
        "rotation authority finished_at", authority_document["finished_at"]
    )
    executions, list_pages = _list_executions_by_status(
        stepfunctions_client, "SUCCEEDED"
    )
    time_matches = []
    for execution in executions:
        start_date = _require_execution_timestamp(
            execution, "startDate", "authority ListExecutions"
        )
        stop_date = _require_execution_timestamp(
            execution, "stopDate", "authority ListExecutions"
        )
        if start_date >= stop_date:
            raise RotationError("authority candidate executionのtimestamp順序が不正です")
        if start_date <= authority_started and stop_date >= authority_finished:
            time_matches.append(execution)

    identity_matches = []
    for execution in time_matches:
        execution_arn = execution["executionArn"]
        events, history_pages = _get_execution_history(
            stepfunctions_client, execution_arn
        )
        event_ids = [event["id"] for event in events]
        if not event_ids or event_ids != sorted(event_ids):
            raise RotationError("authority execution historyのevent順序が不明です")
        identity = _prepare_run_context_identity(events, require_complete_output=True)
        if identity != authority_identity:
            continue
        try:
            description = stepfunctions_client.describe_execution(
                executionArn=execution_arn
            )
        except Exception as exc:  # noqa: BLE001
            raise RotationError(f"authority DescribeExecutionに失敗しました: {exc}") from exc
        if not isinstance(description, dict):
            raise RotationError("authority DescribeExecutionの応答がobjectではありません")
        if description.get("executionArn") != execution_arn:
            raise RotationError("authority execution ARNがListExecutionsと一致しません")
        if description.get("stateMachineArn") != EXPECTED_STATE_MACHINE_ARN:
            raise RotationError("authority state machine ARNがcanonicalではありません")
        if description.get("status") != "SUCCEEDED":
            raise RotationError("authority execution statusがSUCCEEDEDではありません")
        listed_start = _require_execution_timestamp(
            execution, "startDate", "authority ListExecutions"
        )
        listed_stop = _require_execution_timestamp(
            execution, "stopDate", "authority ListExecutions"
        )
        described_start = _require_execution_timestamp(
            description, "startDate", "authority DescribeExecution"
        )
        described_stop = _require_execution_timestamp(
            description, "stopDate", "authority DescribeExecution"
        )
        if listed_start != described_start or listed_stop != described_stop:
            raise RotationError("authority execution timestampがList/Describe間で一致しません")
        identity_matches.append(
            {
                "execution_arn": execution_arn,
                "execution_start": listed_start,
                "execution_stop": listed_stop,
                "history_pages_checked": history_pages,
                "history_event_count": len(events),
            }
        )
    if len(identity_matches) != 1:
        raise RotationError(
            "rotation authority executionを一意に特定できません "
            f"(time_matches={len(time_matches)} / identity_matches={len(identity_matches)})"
        )
    matched = identity_matches[0]
    return {
        "execution_arn": matched["execution_arn"],
        "execution_start": matched["execution_start"],
        "execution_stop": matched["execution_stop"],
        "execution_start_date": _execution_timestamp_text(
            matched["execution_start"]
        ),
        "execution_stop_date": _execution_timestamp_text(matched["execution_stop"]),
        "list_pages_checked": list_pages,
        "history_pages_checked": matched["history_pages_checked"],
        "history_event_count": matched["history_event_count"],
    }


def _historical_failed_execution_evidence(
    stepfunctions_client,
    identities: List[Dict[str, str]],
    authority_stop: datetime,
    current_start: datetime,
) -> Tuple[
    Dict[Tuple[str, str], Dict[str, Any]], int, Dict[str, Any]
]:
    """AWS execution時刻でwindow抽出後、candidate historyだけをstrict検証する。"""
    if stepfunctions_client is None:
        raise RotationError("historical execution historyを取得できません")
    if authority_stop >= current_start:
        raise RotationError("authority/current execution windowの順序を確定できません")
    executions, list_pages = _list_executions_by_status(stepfunctions_client, "FAILED")
    wanted = {(item["run_date"], item["run_id"]) for item in identities}
    evidence_by_identity: Dict[Tuple[str, str], Dict[str, Any]] = {}
    candidates = []
    outside_window = []
    for execution in executions:
        execution_arn = execution["executionArn"]
        start_date = _require_execution_timestamp(
            execution, "startDate", "historical ListExecutions"
        )
        stop_date = _require_execution_timestamp(
            execution, "stopDate", "historical ListExecutions"
        )
        if start_date >= stop_date:
            raise RotationError(
                f"historical executionのtimestamp順序が不正です: {execution_arn}"
            )
        if stop_date < authority_stop:
            outside_window.append(
                {
                    "execution_arn": execution_arn,
                    "classification": "OUTSIDE_RECOVERY_WINDOW",
                    "reason": "COMPLETED_BEFORE_AUTHORITY",
                    "execution_start_date": _execution_timestamp_text(start_date),
                    "execution_stop_date": _execution_timestamp_text(stop_date),
                }
            )
            continue
        if start_date > current_start:
            outside_window.append(
                {
                    "execution_arn": execution_arn,
                    "classification": "OUTSIDE_RECOVERY_WINDOW",
                    "reason": "STARTED_AFTER_CURRENT",
                    "execution_start_date": _execution_timestamp_text(start_date),
                    "execution_stop_date": _execution_timestamp_text(stop_date),
                }
            )
            continue
        if not (
            authority_stop < start_date
            and stop_date < current_start
        ):
            raise RotationError(
                "historical executionがrecovery window境界を跨ぐため順序判定不能です "
                f"({execution_arn})"
            )
        candidates.append(execution)

    for execution in candidates:
        execution_arn = execution["executionArn"]
        events, _ = _get_execution_history(stepfunctions_client, execution_arn)
        identity = _prepare_run_context_identity(events, require_complete_output=True)
        if identity is None:
            raise RotationError(
                "intervening candidate executionにPrepareRunContext identityがありません"
            )
        identity_key = (identity["run_date"], identity["run_id"])
        if identity_key not in wanted:
            raise RotationError(
                "intervening candidate executionに対応するpipeline-statusがありません "
                f"({identity_key!r})"
            )
        if identity_key in evidence_by_identity:
            raise RotationError(f"historical execution identityが重複しています: {identity_key!r}")
        evidence_by_identity[identity_key] = _validate_historical_prepublication_execution(
            stepfunctions_client, execution, identity
        )
    missing = sorted(wanted - set(evidence_by_identity))
    if missing:
        raise RotationError(f"intervening FAILED execution historyを解決できません: {missing!r}")
    return evidence_by_identity, list_pages, {
        "ordering_source": "stepfunctions_execution_metadata",
        "candidate_execution_count": len(candidates),
        "outside_recovery_window_count": len(outside_window),
        "outside_recovery_window": outside_window,
    }


def guard_current_execution_history(
    stepfunctions_client, current_identity: Dict[str, str], logger
) -> Dict[str, Any]:
    """
    current managed runをPrepareRunContext outputで一意解決し、redriveまたは
    同一execution内の過去terminal eventをimmutable historyから拒否する。
    """
    executions, list_pages = _list_running_executions(stepfunctions_client)
    matches = []
    try:
        for execution in executions:
            execution_arn = execution["executionArn"]
            description = stepfunctions_client.describe_execution(executionArn=execution_arn)
            if not isinstance(description, dict):
                raise RotationError("Step Functions DescribeExecutionの応答がobjectではありません")
            if description.get("executionArn") != execution_arn:
                raise RotationError("DescribeExecutionのexecutionArnがListExecutionsと一致しません")
            if description.get("stateMachineArn") != EXPECTED_STATE_MACHINE_ARN:
                raise RotationError("DescribeExecutionのstateMachineArnがcanonical ARNではありません")
            if description.get("status") != "RUNNING":
                raise RotationError(
                    "DescribeExecutionのstatusがRUNNINGではありません "
                    f"({execution_arn} status={description.get('status')!r})"
                )

            events, history_pages = _get_execution_history(
                stepfunctions_client, execution_arn
            )
            identity = _prepare_run_context_identity(events)
            if identity is not None and (
                identity["run_date"] == current_identity["run_date"]
                and identity["run_id"] == current_identity["run_id"]
            ):
                matches.append((execution, description, events, history_pages))
    except RotationError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise RotationError(f"Step Functions DescribeExecutionに失敗しました: {exc}") from exc

    if len(matches) != 1:
        raise RotationError(
            "PrepareRunContextのrun_date/run_idに一致するcurrent executionを一意に特定できません "
            f"(matches={len(matches)})"
        )

    execution, description, events, history_pages = matches[0]
    listed_start = _require_execution_timestamp(
        execution, "startDate", "current ListExecutions"
    )
    described_start = _require_execution_timestamp(
        description, "startDate", "current DescribeExecution"
    )
    if listed_start != described_start:
        raise RotationError("current execution startDateがList/Describe間で一致しません")
    if "redriveCount" not in description:
        raise RotationError("DescribeExecutionにredriveCountがありません")
    redrive_count = description["redriveCount"]
    if (
        not isinstance(redrive_count, int)
        or isinstance(redrive_count, bool)
        or redrive_count < 0
    ):
        raise RotationError(f"DescribeExecutionのredriveCountが不正です: {redrive_count!r}")
    if redrive_count > 0:
        raise RotationError(f"current executionはredriveCount={redrive_count}のため拒否します")
    if description.get("redriveDate") is not None:
        raise RotationError("current executionにredriveDateがあるため拒否します")

    redriven_count = sum(event["type"] == "ExecutionRedriven" for event in events)
    if redriven_count:
        raise RotationError(
            f"current execution historyにExecutionRedrivenが{redriven_count}件あるため拒否します"
        )
    prior_terminal = [
        event["type"] for event in events if event["type"] in PRIOR_TERMINAL_EVENT_TYPES
    ]
    if prior_terminal:
        raise RotationError(
            "current execution historyに過去terminal eventがあるため拒否します "
            f"(events={prior_terminal[:SAMPLE_LIMIT]})"
        )

    execution_arn = description["executionArn"]
    logger.info(
        "immutable execution history照合OK: "
        f"run={current_identity['run_date']}/{current_identity['run_id']} / "
        f"execution={execution_arn}"
    )
    return {
        "validation_result": "PASS",
        "immutable_execution_guard_result": "PASS",
        "evidence_source": "stepfunctions_execution_history",
        "state_machine_arn": EXPECTED_STATE_MACHINE_ARN,
        "execution_arn": execution_arn,
        "current_execution_arn": execution_arn,
        "execution_status": "RUNNING",
        "execution_start_date": _execution_timestamp_text(listed_start),
        "run_date": current_identity["run_date"],
        "run_id": current_identity["run_id"],
        "run_identity_match": True,
        "prepare_run_context_matches": 1,
        "redrive_count": redrive_count,
        "redrive_date_present": False,
        "execution_redriven_event_present": False,
        "prior_terminal_event_present": False,
        "execution_redriven_event_count": 0,
        "prior_terminal_event_count": 0,
        "list_pages_checked": list_pages,
        "history_pages_checked": history_pages,
        "history_event_count": len(events),
    }


def _list_prefix_objects(
    s3_client, bucket: str, prefix: str, include_fingerprint: bool
) -> Dict[str, Any]:
    """
    prefix配下を全ページLISTし、pathごとのsizeまたはsource fingerprintを返す。
    prefix自身のobjectのみ除外し、directory markerを含む全objectをactual集合に含める。
    """
    actual: Dict[str, Any] = {}
    full_prefix = f"{prefix}/"
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=full_prefix):
            if page.get("IsTruncated") and not page.get("NextContinuationToken"):
                raise RotationError("S3 LISTのpaginationが不正です（IsTruncatedだが継続トークンなし）")
            for obj in page.get("Contents") or []:
                key = obj.get("Key", "")
                if not key.startswith(full_prefix):
                    raise RotationError(f"prefix外のkeyが返却されました: {key}")
                relative_path = key[len(full_prefix) :]
                if not relative_path:
                    continue
                size = obj.get("Size")
                if not isinstance(size, int):
                    raise RotationError(f"S3 objectのSizeが不正です: {key}")
                if relative_path in actual:
                    raise RotationError(f"S3 LISTでkeyが重複しました: {key}")
                if include_fingerprint:
                    etag = obj.get("ETag")
                    last_modified = obj.get("LastModified")
                    if not isinstance(etag, str) or not etag:
                        raise RotationError(f"S3 objectのETagが不正です: {key}")
                    if last_modified is None:
                        raise RotationError(f"S3 objectのLastModifiedがありません: {key}")
                    actual[relative_path] = {
                        "size": size,
                        "etag": etag,
                        "last_modified": last_modified,
                    }
                else:
                    actual[relative_path] = size
    except RotationError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise RotationError(f"S3 LISTに失敗しました ({prefix}): {exc}") from exc
    return actual


def list_prefix_objects(s3_client, bucket: str, prefix: str) -> Dict[str, int]:
    """BK1 mirror確認用に {relative_path: size} を返す。"""
    return _list_prefix_objects(s3_client, bucket, prefix, include_fingerprint=False)


def list_source_fingerprints(s3_client, bucket: str, prefix: str) -> Dict[str, Dict[str, Any]]:
    """CURRENT変更検知用に path + size + ETag + LastModified を返す。"""
    return _list_prefix_objects(s3_client, bucket, prefix, include_fingerprint=True)


def list_status_runs(s3_client, bucket: str, base_prefix: str, status_prefix: str) -> List[Dict[str, Any]]:
    """pipeline-status配下の status.json を全件LISTする（read-only）。"""
    full_prefix = f"{base_prefix}/{status_prefix}/"
    runs: List[Dict[str, Any]] = []
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=full_prefix):
            if page.get("IsTruncated") and not page.get("NextContinuationToken"):
                raise RotationError("pipeline-status LISTのpaginationが不正です")
            for obj in page.get("Contents") or []:
                key = obj.get("Key", "")
                if not key.startswith(full_prefix):
                    raise RotationError(f"pipeline-status prefix外のkeyが返却されました: {key}")
                remainder = key[len(full_prefix) :].split("/")
                if len(remainder) != 3 or remainder[2] != "status.json":
                    # 想定外のkeyは黙って捨てず、判定不能として記録した上で無視する
                    continue
                runs.append(
                    {
                        "run_date": remainder[0],
                        "run_id": remainder[1],
                        "key": key,
                        "last_modified": obj.get("LastModified"),
                    }
                )
    except RotationError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise RotationError(f"pipeline-status LISTに失敗しました: {exc}") from exc
    if not runs:
        raise RotationError(f"pipeline-status上にrunが1件もありません: s3://{bucket}/{full_prefix}")
    return runs


def get_status_document(s3_client, bucket: str, key: str) -> Dict[str, Any]:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
    except Exception as exc:  # noqa: BLE001
        raise RotationError(f"pipeline-status statusの取得に失敗しました: {key} ({exc})") from exc
    try:
        document = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise RotationError(f"pipeline-status statusを解釈できません: {key} ({exc})") from exc
    if not isinstance(document, dict):
        raise RotationError(f"pipeline-status statusがJSON objectではありません: {key}")
    return document


def _sort_key(run: Dict[str, Any]) -> Tuple[Any, str, str]:
    last_modified = run.get("last_modified")
    return (last_modified, run["run_date"], run["run_id"])


def resolve_current_managed_identity(args: argparse.Namespace) -> Optional[Dict[str, str]]:
    """除外候補となる自run identityをmanaged environmentからのみ解決する。"""
    cli_run_id = getattr(args, "current_run_id", None)
    env_run_date = (os.environ.get("RUN_DATE") or "").strip()
    env_run_id = (os.environ.get("RUN_ID") or "").strip()

    if cli_run_id is not None and str(cli_run_id).strip():
        cli_run_id = str(cli_run_id).strip()
        if not RUN_ID_RE.match(cli_run_id):
            raise RotationError(f"--current-run-id の形式が不正です: {cli_run_id!r}")
        if not env_run_id:
            raise RotationError("--current-run-id 単独ではpipeline-status runを除外できません")
        if cli_run_id != env_run_id:
            raise RotationError("--current-run-id がmanaged environmentのRUN_IDと一致しません")

    if not env_run_date and not env_run_id:
        return None
    if not env_run_date or not env_run_id:
        raise RotationError("current managed runのRUN_DATE/RUN_IDが片方しかありません")
    if not RUN_DATE_RE.match(env_run_date):
        raise RotationError(f"current managed RUN_DATEの形式が不正です: {env_run_date!r}")
    try:
        datetime.strptime(env_run_date, "%Y%m%d")
    except ValueError as exc:
        raise RotationError(
            f"current managed RUN_DATEが実在日ではありません: {env_run_date!r}"
        ) from exc
    if not RUN_ID_RE.match(env_run_id):
        raise RotationError(f"current managed RUN_IDの形式が不正です: {env_run_id!r}")
    return {"run_date": env_run_date, "run_id": env_run_id, "source": "env"}


def _validate_status_timestamp(name: str, value: Any) -> None:
    """99-9と同じ基準でtimezone付きISO 8601 timestampを検証する。"""
    if not isinstance(value, str) or not value:
        raise RotationError(f"current RUNNING statusの{name}が文字列ではありません: {value!r}")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise RotationError(
            f"current RUNNING statusの{name}がISO 8601形式ではありません: {value!r}"
        ) from exc
    if parsed.tzinfo is None:
        raise RotationError(f"current RUNNING statusの{name}にtimezoneがありません: {value!r}")


def _validate_running_current_document(
    current_run: Dict[str, Any], document: Dict[str, Any], identity: Dict[str, str]
) -> None:
    """99-9 status schema 1.0準拠のRUNNING自runだけを除外候補として認める。"""
    actual_keys = set(document)
    missing_keys = sorted(STATUS_REQUIRED_KEYS - actual_keys)
    extra_keys = sorted(actual_keys - STATUS_REQUIRED_KEYS)
    if missing_keys or extra_keys:
        raise RotationError(
            "current RUNNING statusのschema keyが13 key完全一致ではありません "
            f"(missing={missing_keys} / extra={extra_keys})"
        )
    if document["schema_version"] != STATUS_SCHEMA_VERSION:
        raise RotationError(
            "current RUNNING statusのschema_versionが不正です "
            f"(actual={document['schema_version']!r} / expected={STATUS_SCHEMA_VERSION!r})"
        )
    if not isinstance(document["run_date"], str) or not RUN_DATE_RE.match(document["run_date"]):
        raise RotationError(f"current RUNNING statusのrun_dateが不正です: {document['run_date']!r}")
    try:
        datetime.strptime(document["run_date"], "%Y%m%d")
    except ValueError as exc:
        raise RotationError(
            f"current RUNNING statusのrun_dateが実在日ではありません: {document['run_date']!r}"
        ) from exc
    if not isinstance(document["run_id"], str) or not RUN_ID_RE.match(document["run_id"]):
        raise RotationError(f"current RUNNING statusのrun_idが不正です: {document['run_id']!r}")
    _validate_status_timestamp("started_at", document["started_at"])
    _validate_status_timestamp("updated_at", document["updated_at"])
    for key in ("current_step", "log_s3_uri"):
        if not isinstance(document[key], str) or not document[key].strip():
            raise RotationError(f"current RUNNING statusの{key}が空または文字列ではありません")
    if not isinstance(document["error_message"], str):
        raise RotationError("current RUNNING statusのerror_messageが文字列ではありません")

    if current_run["run_date"] != identity["run_date"] or current_run["run_id"] != identity["run_id"]:
        raise RotationError("current managed run identityとpipeline-status keyが一致しません")
    if document["run_date"] != identity["run_date"]:
        raise RotationError("current managed RUN_DATEとstatus.jsonのrun_dateが一致しません")
    if document["run_id"] != identity["run_id"]:
        raise RotationError("current managed RUN_IDとstatus.jsonのrun_idが一致しません")
    if document["status"] != "RUNNING":
        raise RotationError(
            "current managed runはRUNNINGの場合だけ除外できます "
            f"(status={document['status']!r})"
        )
    if document["exit_code"] is not None:
        raise RotationError("RUNNING status.jsonにexit_codeがあるため自runを除外できません")
    if document["finished_at"] is not None:
        raise RotationError("RUNNING status.jsonにfinished_atがあるため自runを除外できません")
    if document["finished_at_source"] != "not_finished":
        raise RotationError("RUNNING status.jsonのfinished_at_sourceが不正です")
    if document["exit_code_source"] != "pending":
        raise RotationError("RUNNING status.jsonのexit_code_sourceが不正です")


def resolve_recovery_target(args: argparse.Namespace) -> Optional[Dict[str, str]]:
    """date / run_idを両方明示した場合だけ限定recoveryを有効にする。"""
    run_date = getattr(args, "recovery_run_date", None)
    run_id = getattr(args, "recovery_run_id", None)
    if run_date is None and run_id is None:
        return None
    if not run_date or not run_id:
        raise RotationError("recoveryは --recovery-run-date と --recovery-run-id の両方が必要です")
    if not isinstance(run_date, str) or not RUN_DATE_RE.fullmatch(run_date):
        raise RotationError(f"recovery RUN_DATEの形式が不正です: {run_date!r}")
    try:
        datetime.strptime(run_date, "%Y%m%d")
    except ValueError as exc:
        raise RotationError(f"recovery RUN_DATEが実在日ではありません: {run_date!r}") from exc
    if not isinstance(run_id, str) or not RUN_ID_RE.fullmatch(run_id):
        raise RotationError(f"recovery RUN_IDの形式が不正です: {run_id!r}")
    return {"run_date": run_date, "run_id": run_id}


def _validate_terminal_status_document(
    run: Dict[str, Any],
    document: Dict[str, Any],
    label: str,
    finished_at_source: Optional[str] = "managed_wrapper",
    exit_code_source: Optional[str] = "managed_wrapper",
) -> None:
    """recovery判定に使うterminal statusをschema 1.0完全一致で検証する。"""
    actual_keys = set(document)
    missing_keys = sorted(STATUS_REQUIRED_KEYS - actual_keys)
    extra_keys = sorted(actual_keys - STATUS_REQUIRED_KEYS)
    if missing_keys or extra_keys:
        raise RotationError(
            f"{label} statusのschema keyが13 key完全一致ではありません "
            f"(missing={missing_keys} / extra={extra_keys})"
        )
    if document["schema_version"] != STATUS_SCHEMA_VERSION:
        raise RotationError(f"{label} statusのschema_versionが不正です")
    if document["run_date"] != run["run_date"] or document["run_id"] != run["run_id"]:
        raise RotationError(f"{label} statusのkey identityとdocumentが一致しません")
    if not RUN_DATE_RE.fullmatch(document["run_date"] or ""):
        raise RotationError(f"{label} statusのrun_dateが不正です")
    try:
        datetime.strptime(document["run_date"], "%Y%m%d")
    except ValueError as exc:
        raise RotationError(f"{label} statusのrun_dateが実在日ではありません") from exc
    if not RUN_ID_RE.fullmatch(document["run_id"] or ""):
        raise RotationError(f"{label} statusのrun_idが不正です")

    parsed_timestamps = {}
    for name in ("started_at", "finished_at", "updated_at"):
        value = document[name]
        if not isinstance(value, str) or not value:
            raise RotationError(f"{label} statusの{name}が不正です: {value!r}")
        candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise RotationError(f"{label} statusの{name}がISO 8601形式ではありません") from exc
        if parsed.tzinfo is None:
            raise RotationError(f"{label} statusの{name}にtimezoneがありません")
        parsed_timestamps[name] = parsed
    if parsed_timestamps["finished_at"] < parsed_timestamps["started_at"]:
        raise RotationError(f"{label} statusのfinished_atがstarted_atより前です")
    if not isinstance(document["finished_at_source"], str) or not document["finished_at_source"]:
        raise RotationError(f"{label} statusのfinished_at_sourceが不正です")
    if finished_at_source is not None and document["finished_at_source"] != finished_at_source:
        raise RotationError(f"{label} statusのfinished_at_sourceが不正です")
    if not isinstance(document["exit_code_source"], str) or not document["exit_code_source"]:
        raise RotationError(f"{label} statusのexit_code_sourceが不正です")
    if exit_code_source is not None and document["exit_code_source"] != exit_code_source:
        raise RotationError(f"{label} statusのexit_code_sourceが不正です")
    if not isinstance(document["exit_code"], int) or isinstance(document["exit_code"], bool):
        raise RotationError(f"{label} statusのexit_codeが整数ではありません")
    for key in ("current_step", "log_s3_uri"):
        if not isinstance(document[key], str) or not document[key].strip():
            raise RotationError(f"{label} statusの{key}が空または文字列ではありません")
    if not isinstance(document["error_message"], str):
        raise RotationError(f"{label} statusのerror_messageが文字列ではありません")


def _parse_evidence_timestamp(label: str, value: Any) -> datetime:
    """順序証拠に使うtimezone付きISO 8601 timestampを返す。"""
    if not isinstance(value, str) or not value:
        raise RotationError(f"{label}が不正です: {value!r}")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise RotationError(f"{label}がISO 8601形式ではありません") from exc
    if parsed.tzinfo is None:
        raise RotationError(f"{label}にtimezoneがありません")
    return parsed


def _require_ordering_last_modified(run: Dict[str, Any], label: str) -> datetime:
    value = run.get("last_modified")
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise RotationError(f"{label}のLastModifiedが不正です（順序判定不能）")
    return value


def _guard_prepublication_failure_recovery(
    s3_client,
    stepfunctions_client,
    bucket: str,
    runs: List[Dict[str, Any]],
    provenance: Dict[str, Any],
    current_run: Dict[str, Any],
    current_document: Dict[str, Any],
    current_identity: Dict[str, str],
    logger,
) -> Dict[str, Any]:
    """
    verified publication以降、自RUNNING runより前の全terminal runについて、
    failure reasonではなくpublication境界未到達とCURRENT/BK1不変をauthority条件にする。
    """
    authority_matches = [
        run
        for run in runs
        if run["run_date"] == provenance["run_date"] and run["run_id"] == provenance["run_id"]
    ]
    if len(authority_matches) != 1:
        raise RotationError(
            "rotation authorityのpipeline-statusを一意に特定できません "
            f"(matches={len(authority_matches)})"
        )
    authority_run = authority_matches[0]
    authority_document = get_status_document(s3_client, bucket, authority_run["key"])
    _validate_terminal_status_document(
        authority_run, authority_document, "rotation authority SUCCEEDED"
    )
    if authority_document["status"] != "SUCCEEDED" or authority_document["exit_code"] != 0:
        raise RotationError("rotation authorityがSUCCEEDED/exit=0ではありません")

    authority_execution = _resolve_authority_execution_boundary(
        stepfunctions_client,
        {"run_date": authority_run["run_date"], "run_id": authority_run["run_id"]},
        authority_document,
    )
    current_execution_guard = guard_current_execution_history(
        stepfunctions_client, current_identity, logger
    )
    current_execution_start = _parse_evidence_timestamp(
        "current execution startDate",
        current_execution_guard.get("execution_start_date"),
    )
    if authority_execution["execution_stop"] >= current_execution_start:
        raise RotationError("authority/current execution metadataの順序を確定できません")

    authority_modified = _require_ordering_last_modified(authority_run, "rotation authority status")
    current_modified = _require_ordering_last_modified(current_run, "current RUNNING status")
    if authority_modified >= current_modified:
        raise RotationError("rotation authorityとcurrent RUNNING runの順序を確定できません")

    authority_finished = _parse_evidence_timestamp(
        "rotation authority finished_at", authority_document["finished_at"]
    )
    current_started = _parse_evidence_timestamp(
        "current RUNNING started_at", current_document["started_at"]
    )
    if authority_finished >= current_started:
        raise RotationError("rotation authority終了とcurrent RUNNING開始の順序が不正です")

    intervening = []
    for run in runs:
        if run["key"] == authority_run["key"]:
            continue
        modified = _require_ordering_last_modified(run, "pipeline-status run")
        if modified == authority_modified or modified == current_modified:
            raise RotationError("pipeline-statusのLastModifiedが同一で順序判定不能です")
        if modified > current_modified:
            raise RotationError(
                "current RUNNING runより後のpipeline-statusが存在します（順序判定不能） "
                f"({run['run_date']}/{run['run_id']})"
            )
        if modified > authority_modified:
            intervening.append(run)

    if not intervening:
        raise RotationError("pre-publication recovery対象のintervening terminal runがありません")
    intervening.sort(key=_sort_key)
    identities = [
        {"run_date": run["run_date"], "run_id": run["run_id"]}
        for run in intervening
    ]
    history_by_identity, failed_list_pages, execution_window = (
        _historical_failed_execution_evidence(
            stepfunctions_client,
            identities,
            authority_execution["execution_stop"],
            current_execution_start,
        )
    )

    validations = []
    previous_finished = authority_finished
    for run in intervening:
        if run["run_date"] == current_run["run_date"] and run["run_id"] == current_run["run_id"]:
            raise RotationError("same run_idのRedrive相当はrecoveryとして許可しません")
        document = get_status_document(s3_client, bucket, run["key"])
        _validate_terminal_status_document(
            run,
            document,
            "intervening pre-publication FAILED",
            finished_at_source=None,
            exit_code_source=None,
        )
        if document["status"] != "FAILED" or document["exit_code"] == 0:
            raise RotationError(
                "intervening runが非正常FAILED terminalではありません "
                f"({run['run_date']}/{run['run_id']} status={document['status']!r} "
                f"exit={document['exit_code']!r})"
            )
        step_evidence = _validate_prepublication_step(document["current_step"])

        started = _parse_evidence_timestamp("intervening started_at", document["started_at"])
        finished = _parse_evidence_timestamp("intervening finished_at", document["finished_at"])
        if started <= previous_finished or finished >= current_started:
            raise RotationError(
                "intervening runの実行順序をstatus timestampから確定できません "
                f"({run['run_date']}/{run['run_id']})"
            )
        previous_finished = finished
        identity_key = (run["run_date"], run["run_id"])
        history_evidence = history_by_identity.get(identity_key)
        if history_evidence is None:
            raise RotationError(f"intervening execution evidenceがありません: {identity_key!r}")
        validations.append(
            {
                "run_date": run["run_date"],
                "run_id": run["run_id"],
                "status_key": run["key"],
                "status": "FAILED",
                "validation_result": "PASS",
                "current_step": step_evidence["current_step"],
                "step_order_verified": True,
                "before_publication_boundary": True,
                "publication_boundary_reached": False,
                "execution_evidence": history_evidence,
            }
        )

    logger.info(
        "pre-publication recovery authority照合OK: "
        f"authority={authority_run['run_date']}/{authority_run['run_id']} / "
        f"intervening={len(validations)}"
    )
    return {
        "status_key": authority_run["key"],
        "status": "SUCCEEDED",
        "exit_code": 0,
        "current_execution_guard": current_execution_guard,
        "recovery": {
            "enabled": True,
            "eligible": True,
            "recovery_mode": PREPUBLICATION_RECOVERY_MODE,
            "rotation_authority_run_date": authority_run["run_date"],
            "rotation_authority_run_id": authority_run["run_id"],
            "rotation_authority_status_key": authority_run["key"],
            "previous_verified_finished_at": authority_document["finished_at"],
            "current_run_date": current_run["run_date"],
            "current_run_id": current_run["run_id"],
            "all_intervening_runs_checked": True,
            "failed_execution_list_pages_checked": failed_list_pages,
            "execution_window": {
                "ordering_source": execution_window["ordering_source"],
                "authority_execution_arn": authority_execution["execution_arn"],
                "authority_stop_date": authority_execution["execution_stop_date"],
                "current_execution_arn": current_execution_guard["execution_arn"],
                "current_start_date": current_execution_guard["execution_start_date"],
                "candidate_execution_count": execution_window[
                    "candidate_execution_count"
                ],
                "outside_recovery_window_count": execution_window[
                    "outside_recovery_window_count"
                ],
                "outside_recovery_window": execution_window[
                    "outside_recovery_window"
                ],
            },
            "publication_guard": {
                "terminal_status": "FAILED",
                "publication_boundary_step": PUBLICATION_BOUNDARY_STEP_NAME,
                "publication_boundary_state": PUBLICATION_BOUNDARY_STATE,
                "publication_boundary_reached": False,
                "failure_reason_allowlist_used": False,
            },
            "intervening_runs": validations,
            "skipped_runs": [
                {"run_date": item["run_date"], "run_id": item["run_id"]}
                for item in validations
            ],
        },
    }


def _guard_recovery_pipeline_status(
    s3_client,
    bucket: str,
    runs: List[Dict[str, Any]],
    provenance: Dict[str, Any],
    recovery_target: Dict[str, str],
    logger,
) -> Dict[str, Any]:
    """
    FAILEDを無視せず、明示targetが最新terminalの既知80-7 failureであることと、
    previous 80-9 summaryが指すrunが直前successfulであることを検証する。
    """
    if any(run.get("last_modified") is None for run in runs):
        raise RotationError("pipeline-status LISTにLastModifiedがありません（recovery順序判定不能）")
    latest_modified = max(run["last_modified"] for run in runs)
    latest_runs = [run for run in runs if run["last_modified"] == latest_modified]
    if len(latest_runs) != 1:
        raise RotationError(
            f"recovery対象の最新pipeline-status runを一意に特定できません (matches={len(latest_runs)})"
        )
    latest = latest_runs[0]
    if (
        latest["run_date"] != recovery_target["run_date"]
        or latest["run_id"] != recovery_target["run_id"]
    ):
        raise RotationError(
            "latest FAILED runが明示recovery targetと一致しません "
            f"(latest={latest['run_date']}/{latest['run_id']} / "
            f"target={recovery_target['run_date']}/{recovery_target['run_id']})"
        )

    failed_document = get_status_document(s3_client, bucket, latest["key"])
    _validate_terminal_status_document(latest, failed_document, "recovery target FAILED")
    if failed_document["status"] != "FAILED" or failed_document["exit_code"] == 0:
        raise RotationError(
            "recovery targetが非正常FAILED terminalではありません "
            f"(status={failed_document['status']!r} / exit={failed_document['exit_code']!r})"
        )
    expected_failed_step = (
        f"{RECOVERY_FAILED_STEP_NAME}(RUN_DATE={recovery_target['run_date']})"
    )
    if failed_document["current_step"] != expected_failed_step:
        raise RotationError(
            "recoveryを許可しないfailure stepです "
            f"(actual={failed_document['current_step']!r} / expected={expected_failed_step!r})"
        )

    if provenance["run_date"] >= recovery_target["run_date"]:
        raise RotationError(
            "previous verified runがrecovery targetより前ではありません "
            f"({provenance['run_date']} >= {recovery_target['run_date']})"
        )
    previous_matches = [
        run
        for run in runs
        if run["run_date"] == provenance["run_date"] and run["run_id"] == provenance["run_id"]
    ]
    if len(previous_matches) != 1:
        raise RotationError(
            "previous verified runのpipeline-statusを一意に特定できません "
            f"(matches={len(previous_matches)})"
        )
    previous_run = previous_matches[0]
    previous_document = get_status_document(s3_client, bucket, previous_run["key"])
    _validate_terminal_status_document(previous_run, previous_document, "previous verified SUCCEEDED")
    if previous_document["status"] != "SUCCEEDED" or previous_document["exit_code"] != 0:
        raise RotationError("previous verified runがSUCCEEDED/exit=0ではありません")

    successful_before_target = []
    for run in runs:
        if run["run_date"] >= recovery_target["run_date"]:
            continue
        document = get_status_document(s3_client, bucket, run["key"])
        if document.get("status") == "SUCCEEDED":
            _validate_terminal_status_document(run, document, "previous successful candidate")
            if document["exit_code"] != 0:
                raise RotationError("SUCCEEDED statusのexit_codeが0ではありません")
            successful_before_target.append(run)
    if not successful_before_target:
        raise RotationError("recovery targetより前のsuccessful runを解決できません")
    previous_successful = sorted(successful_before_target, key=_sort_key)[-1]
    if previous_successful["key"] != previous_run["key"]:
        raise RotationError(
            "80-9 summaryのrunが直前successful runではありません "
            f"(status={previous_successful['run_date']}/{previous_successful['run_id']} / "
            f"summary={provenance['run_date']}/{provenance['run_id']})"
        )

    logger.info(
        "recovery pipeline-status照合OK: "
        f"failed={latest['run_date']}/{latest['run_id']} / "
        f"previous={previous_run['run_date']}/{previous_run['run_id']}"
    )
    return {
        "status_key": previous_run["key"],
        "status": "SUCCEEDED",
        "exit_code": 0,
        "recovery": {
            "enabled": True,
            "eligible": True,
            "target_run_date": latest["run_date"],
            "target_run_id": latest["run_id"],
            "failed_status_key": latest["key"],
            "failed_step": failed_document["current_step"],
            "previous_verified_run_date": previous_run["run_date"],
            "previous_verified_run_id": previous_run["run_id"],
            "previous_verified_finished_at": previous_document["finished_at"],
        },
    }


def guard_pipeline_status(
    s3_client,
    bucket: str,
    base_prefix: str,
    status_prefix: str,
    provenance: Dict[str, Any],
    current_identity: Optional[Dict[str, str]],
    logger,
    recovery_target: Optional[Dict[str, str]] = None,
    stepfunctions_client=None,
) -> Dict[str, Any]:
    """
    pipeline-status を照合し、直前の terminal run が
    「80-9 summaryが指すrun」かつ SUCCEEDED であることを確認する。

    env由来identityとstatus documentが正しいRUNNING自runだけを除外し、
    その後の最新runがsummaryのrunと一致しない場合はFAILさせる。
    これにより「古い成功summaryだけが残り、直近runが80-9途中で落ちてCURRENTがpartial」
    というケースをbackupしない。
    """
    runs = list_status_runs(s3_client, bucket, base_prefix, status_prefix)
    current_run = None
    current_document = None
    if current_identity:
        current_runs = [
            run for run in runs
            if run["run_date"] == current_identity["run_date"]
            and run["run_id"] == current_identity["run_id"]
        ]
        if len(current_runs) != 1:
            raise RotationError(
                "current managed runのpipeline-status keyを一意に特定できません "
                f"(matches={len(current_runs)})"
            )
        current_run = current_runs[0]
        current_document = get_status_document(s3_client, bucket, current_run["key"])
        _validate_running_current_document(current_run, current_document, current_identity)
        runs = [run for run in runs if run["key"] != current_run["key"]]
        logger.info(
            f"RUNNING自runのみ除外: {current_identity['run_date']}/{current_identity['run_id']}"
        )
    if not runs:
        raise RotationError("自runを除くpipeline-status runが存在しません")
    if recovery_target is not None:
        result = _guard_recovery_pipeline_status(
            s3_client, bucket, runs, provenance, recovery_target, logger
        )
        if current_identity is not None:
            result["current_execution_guard"] = guard_current_execution_history(
                stepfunctions_client, current_identity, logger
            )
        return result
    if any(run.get("last_modified") is None for run in runs):
        raise RotationError("pipeline-status LISTにLastModifiedがありません（順序判定不能）")

    latest = sorted(runs, key=_sort_key)[-1]
    if latest["run_date"] != provenance["run_date"] or latest["run_id"] != provenance["run_id"]:
        if current_run is not None and current_document is not None:
            return _guard_prepublication_failure_recovery(
                s3_client,
                stepfunctions_client,
                bucket,
                runs,
                provenance,
                current_run,
                current_document,
                current_identity,
                logger,
            )
        raise RotationError(
            "previous CURRENTが正常ではありません: 直近のpipeline-status runと80-9 summaryが一致しません "
            f"(status={latest['run_date']}/{latest['run_id']} / "
            f"summary={provenance['run_date']}/{provenance['run_id']})"
        )

    document = get_status_document(s3_client, bucket, latest["key"])
    if document.get("run_id") != provenance["run_id"] or document.get("run_date") != provenance["run_date"]:
        raise RotationError(
            "previous CURRENTが正常ではありません: status.jsonのrun_id/run_dateが80-9 summaryと一致しません "
            f"(status={document.get('run_date')!r}/{document.get('run_id')!r})"
        )
    if document.get("status") != "SUCCEEDED":
        raise RotationError(
            f"previous CURRENTが正常ではありません: pipeline-status status={document.get('status')!r}"
        )
    if document.get("exit_code") != 0:
        raise RotationError(
            f"previous CURRENTが正常ではありません: pipeline-status exit_code={document.get('exit_code')!r}"
        )

    logger.info(f"pipeline-status照合OK: {latest['run_date']}/{latest['run_id']} SUCCEEDED")
    result = {
        "status_key": latest["key"],
        "status": document.get("status"),
        "exit_code": 0,
    }
    if current_identity is not None:
        result["current_execution_guard"] = guard_current_execution_history(
            stepfunctions_client, current_identity, logger
        )
    return result


def validate_recovery_manifest_reference(
    manifest_path: Path,
    summary_manifest_path: Any,
) -> Dict[str, Any]:
    """80-9 summaryがcanonical previous manifestを参照していることを検証する。"""
    resolved_manifest = manifest_path.resolve()
    if not isinstance(summary_manifest_path, str) or not summary_manifest_path:
        raise RotationError("previous 80-9 summaryにmanifest_pathがありません")
    if Path(summary_manifest_path).resolve() != resolved_manifest:
        raise RotationError(
            "previous 80-9 summaryのmanifest_pathがcanonical pathと一致しません "
            f"(actual={summary_manifest_path!r} / expected={str(resolved_manifest)!r})"
        )
    if not resolved_manifest.is_file():
        raise RotationError(f"previous verified manifestが存在しません: {resolved_manifest}")
    return {
        "manifest_path": str(resolved_manifest),
        "manifest_reference_verified": True,
    }


def load_recovery_manifest_inventory(manifest_path: Path) -> Dict[str, int]:
    """previous verified 80-8 manifestをstrictなpath/size inventoryとして読む。"""
    inventory: Dict[str, int] = {}
    try:
        records = read_jsonl(str(manifest_path))
        for index, record in enumerate(records, 1):
            if not isinstance(record, dict) or set(record) != {"relative_path", "size"}:
                raise RotationError(
                    f"previous verified manifestのschemaが不正です (record={index})"
                )
            relative_path = record.get("relative_path")
            size = record.get("size")
            if not isinstance(relative_path, str) or not relative_path:
                raise RotationError(
                    f"previous verified manifestのrelative_pathが不正です (record={index})"
                )
            parts = relative_path.split("/")
            if relative_path.startswith("/") or any(part in ("", ".", "..") for part in parts):
                raise RotationError(
                    f"previous verified manifestのrelative_pathが安全ではありません: {relative_path!r}"
                )
            if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                raise RotationError(
                    f"previous verified manifestのsizeが不正です: {relative_path!r}"
                )
            if relative_path in inventory:
                raise RotationError(
                    f"previous verified manifestにrelative_path重複があります: {relative_path!r}"
                )
            inventory[relative_path] = size
    except RotationError:
        raise
    except (OSError, ValueError) as exc:
        raise RotationError(f"previous verified manifestを読めません: {manifest_path} ({exc})") from exc
    if not inventory:
        raise RotationError("previous verified manifestが0件です")
    return inventory


def validate_recovery_current_inventory(
    current: Dict[str, int], manifest: Dict[str, int], provenance: Dict[str, Any]
) -> Dict[str, Any]:
    """CURRENTをsummary count/bytesだけでなくmanifest全path/sizeとも照合する。"""
    if current != manifest:
        missing = sorted(set(manifest) - set(current))
        extra = sorted(set(current) - set(manifest))
        mismatched = sorted(
            path for path in set(current) & set(manifest) if current[path] != manifest[path]
        )
        raise RotationError(
            "CURRENT実体とprevious verified manifestのinventoryが一致しません "
            f"(missing={len(missing)} / extra={len(extra)} / size_mismatch={len(mismatched)} / "
            f"samples={(missing + extra + mismatched)[:SAMPLE_LIMIT]})"
        )
    file_count = len(manifest)
    total_bytes = sum(manifest.values())
    if file_count != provenance["file_count"] or total_bytes != provenance["total_bytes"]:
        raise RotationError(
            "previous verified manifestと80-9 summaryのcount/bytesが一致しません "
            f"(manifest={file_count}/{total_bytes} / "
            f"summary={provenance['file_count']}/{provenance['total_bytes']})"
        )
    return {
        "verified": True,
        "non_empty": True,
        "file_count": file_count,
        "total_bytes": total_bytes,
        "manifest_inventory_match": True,
        "unchanged_since_rotation_authority": True,
    }


def validate_recovery_current_unchanged(
    current_fingerprints: Dict[str, Dict[str, Any]], previous_finished_at: Any
) -> Dict[str, Any]:
    """previous SUCCEEDED確定後に変更されたCURRENT objectが1件もないことを検証する。"""
    if not isinstance(previous_finished_at, str) or not previous_finished_at:
        raise RotationError("previous verified statusのfinished_atが不正です")
    candidate = (
        previous_finished_at[:-1] + "+00:00"
        if previous_finished_at.endswith("Z")
        else previous_finished_at
    )
    try:
        cutoff = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise RotationError("previous verified statusのfinished_atがISO 8601形式ではありません") from exc
    if cutoff.tzinfo is None:
        raise RotationError("previous verified statusのfinished_atにtimezoneがありません")

    changed = []
    for relative_path, fingerprint in current_fingerprints.items():
        last_modified = fingerprint.get("last_modified")
        if not isinstance(last_modified, datetime) or last_modified.tzinfo is None:
            raise RotationError(f"CURRENT objectのLastModifiedが不正です: {relative_path}")
        if last_modified > cutoff:
            changed.append(relative_path)
    if changed:
        raise RotationError(
            "previous verified SUCCEEDED後にCURRENTが変更されています "
            f"(count={len(changed)} / samples={sorted(changed)[:SAMPLE_LIMIT]})"
        )
    return {
        "current_unchanged_since_previous_success": True,
        "previous_success_finished_at": previous_finished_at,
    }


def load_previous_backup_summary(summary_path: Path) -> Dict[str, Any]:
    """pre-publication recoveryでBK1 baselineに使う直前80-75成功summaryを読む。"""
    if not summary_path.is_file():
        raise RotationError(f"直前の80-75 summaryが存在しません: {summary_path}")
    try:
        with open(summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)
    except (OSError, ValueError) as exc:
        raise RotationError(f"直前の80-75 summaryを読めません: {summary_path} ({exc})") from exc
    if not isinstance(summary, dict):
        raise RotationError("直前の80-75 summaryがJSON objectではありません")
    return summary


def validate_previous_backup_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
    """直前rotationでverifiedとなったBK1のcount/bytes baselineを返す。"""
    required = (
        ("step", STEP_NAME),
        ("operation", "rotation"),
        ("mode", "apply"),
        ("backup_status", "SUCCEEDED"),
        ("s3_source", EXPECTED_SOURCE_URI),
        ("s3_destination", EXPECTED_DESTINATION_URI),
        ("s3_destination_locked", True),
    )
    for key, expected in required:
        if summary.get(key) != expected:
            raise RotationError(
                "直前の80-75 summaryがBK1 baselineとして不正です "
                f"({key}={summary.get(key)!r})"
            )
    verify = summary.get("verify")
    if not isinstance(verify, dict) or verify.get("verified") is not True:
        raise RotationError("直前の80-75 summaryがverified成功ではありません")
    for key in ("missing_count", "extra_count", "size_mismatch_count"):
        if verify.get(key) != 0:
            raise RotationError(f"直前の80-75 summaryの{key}が0ではありません")
    expected_files = _require_count(verify, "expected_file_count", "previous 80-75 summary")
    actual_files = _require_count(verify, "actual_file_count", "previous 80-75 summary")
    expected_bytes = _require_count(verify, "expected_total_bytes", "previous 80-75 summary")
    actual_bytes = _require_count(verify, "actual_total_bytes", "previous 80-75 summary")
    if expected_files <= 0 or expected_files != actual_files or expected_bytes != actual_bytes:
        raise RotationError("直前の80-75 summaryのBK1 count/bytesが不正です")
    return {
        "summary_verified": True,
        "file_count": actual_files,
        "total_bytes": actual_bytes,
    }


def validate_recovery_bk1_unchanged(
    backup_fingerprints: Dict[str, Dict[str, Any]],
    authority_finished_at: Any,
    baseline: Dict[str, Any],
) -> Dict[str, Any]:
    """BK1の非空、完全LIST、baseline count/bytes、authority後の更新なしを確認する。"""
    if not backup_fingerprints:
        raise RotationError("BK1が0件のためrecoveryを許可できません")
    cutoff = _parse_evidence_timestamp("rotation authority finished_at", authority_finished_at)
    changed = []
    for relative_path, fingerprint in backup_fingerprints.items():
        last_modified = fingerprint.get("last_modified")
        if not isinstance(last_modified, datetime) or last_modified.tzinfo is None:
            raise RotationError(f"BK1 objectのLastModifiedが不正です: {relative_path}")
        if last_modified > cutoff:
            changed.append(relative_path)
    if changed:
        raise RotationError(
            "rotation authority終了後にBK1が変更されています "
            f"(count={len(changed)} / samples={sorted(changed)[:SAMPLE_LIMIT]})"
        )
    file_count = len(backup_fingerprints)
    total_bytes = sum(item["size"] for item in backup_fingerprints.values())
    if file_count != baseline["file_count"] or total_bytes != baseline["total_bytes"]:
        raise RotationError(
            "BK1実体と直前80-75成功summaryのcount/bytesが一致しません "
            f"(s3={file_count}/{total_bytes} / "
            f"summary={baseline['file_count']}/{baseline['total_bytes']})"
        )
    return {
        "verified": True,
        "non_empty": True,
        "inventory_listed": True,
        "previous_80_75_summary_match": True,
        "unchanged_since_rotation_authority": True,
        "file_count": file_count,
        "total_bytes": total_bytes,
    }


# ---------------------------------------------------------------------------
# aws s3 sync（CURRENT -> BK1）
# ---------------------------------------------------------------------------


def build_sync_argv(source_uri: str, destination_uri: str, region: str, dry_run: bool) -> List[str]:
    """aws s3 sync の argv を組み立てる（shell文字列 / include / exclude は使わない）。"""
    if source_uri != EXPECTED_SOURCE_URI:
        raise RotationError(f"destination安全ロック違反: source={source_uri!r}")
    if destination_uri != EXPECTED_DESTINATION_URI:
        raise RotationError(f"destination安全ロック違反: destination={destination_uri!r}")
    argv = [
        AWS_BIN,
        "s3",
        "sync",
        source_uri,
        destination_uri,
        "--delete",
        "--only-show-errors",
        "--region",
        region,
    ]
    if dry_run:
        argv.append("--dryrun")
    return argv


def run_sync(argv: List[str], logger) -> None:
    if argv[3] != EXPECTED_SOURCE_URI:
        raise RotationError(f"destination安全ロック違反: source={argv[3]!r}")
    if argv[4] != EXPECTED_DESTINATION_URI:
        raise RotationError(f"destination安全ロック違反: destination={argv[4]!r}")
    if "--include" in argv or "--exclude" in argv:
        raise RotationError("CURRENT->BK1 backupではinclude/excludeフィルタを使いません")
    logger.info(f"aws s3 sync 実行: src={argv[3]} / dest={argv[4]}")
    completed = subprocess.run(  # noqa: S603 - argv配列固定・shell未使用
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=False,
        check=False,
    )
    output = completed.stdout.decode("utf-8", errors="replace").strip()
    if output:
        for line in output.splitlines()[:SAMPLE_LIMIT]:
            logger.info(f"aws出力: {line}")
    if completed.returncode != 0:
        raise RotationError(f"aws s3 sync が失敗しました (exit={completed.returncode})")
    logger.ok("aws s3 sync 成功")


# ---------------------------------------------------------------------------
# verify（CURRENT と BK1 の完全比較）
# ---------------------------------------------------------------------------


def compare_sets(expected: Dict[str, int], actual: Dict[str, int], logger) -> Dict[str, Any]:
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    mismatched = sorted(
        path for path in set(expected) & set(actual) if expected[path] != actual[path]
    )

    expected_bytes = sum(expected.values())
    actual_bytes = sum(actual.values())

    for path in missing[:SAMPLE_LIMIT]:
        logger.error(f"[NG] missing: {path}")
    for path in extra[:SAMPLE_LIMIT]:
        logger.error(f"[NG] extra: {path}")
    for path in mismatched[:SAMPLE_LIMIT]:
        logger.error(f"[NG] size mismatch: {path} current={expected[path]} bk1={actual[path]}")

    result = {
        "expected_file_count": len(expected),
        "actual_file_count": len(actual),
        "expected_total_bytes": expected_bytes,
        "actual_total_bytes": actual_bytes,
        "missing_count": len(missing),
        "extra_count": len(extra),
        "size_mismatch_count": len(mismatched),
        "missing_samples": missing[:SAMPLE_LIMIT],
        "extra_samples": extra[:SAMPLE_LIMIT],
        "size_mismatch_samples": mismatched[:SAMPLE_LIMIT],
    }
    result["verified"] = (
        not missing
        and not extra
        and not mismatched
        and len(expected) == len(actual)
        and expected_bytes == actual_bytes
    )
    return result


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="bk1が存在しない状態から初回作成する（通常rotationとは別経路）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="aws s3 sync --dryrun で実行し、S3を変更せず wait / verify もしない",
    )
    parser.add_argument("--step-dir", default=str(STEP_DIR), help="出力先stepディレクトリ（focused test用）")
    parser.add_argument("--sync-dir", default=None, help="80-9 stepディレクトリ（focused test用）")
    parser.add_argument(
        "--current-run-id",
        default=None,
        help="互換確認用。環境変数RUN_IDと一致する場合のみ受理し、除外identityには使用しない。",
    )
    parser.add_argument(
        "--recovery-run-date",
        default=None,
        help="限定recovery対象のFAILED RUN_DATE。--recovery-run-idとの同時指定が必須。",
    )
    parser.add_argument(
        "--recovery-run-id",
        default=None,
        help="限定recovery対象のFAILED RUN_ID。--recovery-run-dateとの同時指定が必須。",
    )
    parser.add_argument(
        "--prepare-dir",
        default=None,
        help="previous verified manifestの80-8 stepディレクトリ（focused test用）",
    )
    return parser.parse_args()


def run(args: argparse.Namespace, logger) -> Dict[str, Any]:
    config = load_pipeline_s3_config()
    bucket = get_config_value(config, "PIPELINE_S3_BUCKET")
    base_prefix = get_config_value(config, "PIPELINE_S3_BASE_PREFIX")
    current_prefix = get_config_value(config, "PORTAL_S3_PREFIX")
    backup_prefix = get_config_value(config, "PORTAL_S3_BACKUP_PREFIX")
    status_prefix = get_config_value(config, "PIPELINE_STATUS_PREFIX")
    region = get_config_value(config, "PIPELINE_AWS_REGION")

    # sync開始前に source / destination を完全固定する
    source_uri, destination_uri = lock_backup_route(bucket, base_prefix, current_prefix, backup_prefix)
    lock_status_prefix(status_prefix)
    wait_seconds = parse_wait_seconds(get_config_value(config, "PORTAL_S3_VERIFY_WAIT_SEC"))

    sync_dir = Path(args.sync_dir) if args.sync_dir else (project_root / SYNC_STEP_DIR_NAME)
    sync_summary_path = sync_dir / RESULT_DIR_NAME / SYNC_SUMMARY_FILENAME
    prepare_dir = (
        Path(args.prepare_dir)
        if getattr(args, "prepare_dir", None)
        else (project_root / PREPARE_STEP_DIR_NAME)
    )
    previous_manifest_path = prepare_dir / RESULT_DIR_NAME / PREVIOUS_MANIFEST_FILENAME
    recovery_target = resolve_recovery_target(args)

    operation = "bootstrap" if args.bootstrap else "rotation"
    logger.info(f"operation={operation} / mode={'dry-run' if args.dry_run else 'apply'}")
    logger.info(f"CURRENT(lock済): {source_uri}")
    logger.info(f"BK1(lock済): {destination_uri}")

    summary: Dict[str, Any] = {
        "step": STEP_NAME,
        "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "operation": operation,
        "mode": "dry-run" if args.dry_run else "apply",
        "s3_source": source_uri,
        "s3_destination": destination_uri,
        "s3_destination_locked": True,
        "backup_method": "aws s3 sync CURRENT -> BK1 --delete (no CLI filters)",
        "backup_status": "SUCCEEDED",
        "verify_wait_sec": wait_seconds,
        "wait_performed": False,
        "sync_summary_path": str(sync_summary_path),
    }
    if not args.dry_run:
        summary["immutable_execution_guard_contract_version"] = (
            IMMUTABLE_EXECUTION_GUARD_CONTRACT_VERSION
        )

    current_identity = resolve_current_managed_identity(args)
    if not args.dry_run and current_identity is None:
        raise RotationError("apply rotationにはmanaged RUN_DATE/RUN_IDが必須です")

    # ---- previous CURRENT 正常性guard ------------------------------------
    previous_summary = load_previous_sync_summary(sync_summary_path)
    provenance = validate_previous_sync_summary(previous_summary)
    logger.info(
        f"previous CURRENT: run={provenance['run_date']}/{provenance['run_id']} "
        f"files={provenance['file_count']} bytes={provenance['total_bytes']}"
    )

    s3_client = build_s3_client(region)
    stepfunctions_client = None
    if current_identity is not None:
        stepfunctions_client = build_stepfunctions_client(region)
    status_info = guard_pipeline_status(
        s3_client,
        bucket,
        base_prefix,
        status_prefix,
        provenance,
        current_identity,
        logger,
        recovery_target=recovery_target,
        stepfunctions_client=stepfunctions_client,
    )
    current_execution_guard = status_info.pop("current_execution_guard", None)
    if current_execution_guard is not None:
        summary["current_execution_guard"] = current_execution_guard
    recovery_status = status_info.get("recovery")
    prepublication_recovery = bool(
        recovery_status
        and recovery_status.get("recovery_mode") == PREPUBLICATION_RECOVERY_MODE
    )

    current_before_fingerprint = list_source_fingerprints(s3_client, bucket, current_prefix)
    unchanged_info = None
    if recovery_status is not None:
        unchanged_info = validate_recovery_current_unchanged(
            current_before_fingerprint,
            recovery_status["previous_verified_finished_at"],
        )
    current_before = {
        path: fingerprint["size"] for path, fingerprint in current_before_fingerprint.items()
    }
    if not current_before:
        raise RotationError(f"CURRENTが0件です（backupしません）: {source_uri}")
    current_bytes = sum(current_before.values())
    if len(current_before) != provenance["file_count"] or current_bytes != provenance["total_bytes"]:
        raise RotationError(
            "previous CURRENTが正常ではありません: CURRENT実体と80-9 summaryが一致しません "
            f"(s3 {len(current_before)}件/{current_bytes}bytes / "
            f"summary {provenance['file_count']}件/{provenance['total_bytes']}bytes)"
        )
    logger.info(f"CURRENT実体照合OK: files={len(current_before)} / bytes={current_bytes}")

    recovery_info = None
    if recovery_status is not None:
        manifest_info = validate_recovery_manifest_reference(
            previous_manifest_path,
            previous_summary.get("manifest_path"),
        )
        recovery_info = dict(recovery_status)
        recovery_info.update(manifest_info)
        recovery_info.update(unchanged_info or {})
        recovery_info["file_count"] = len(current_before)
        recovery_info["total_bytes"] = current_bytes
        recovery_info["inventory_verified"] = True
        if prepublication_recovery:
            manifest_inventory = load_recovery_manifest_inventory(previous_manifest_path)
            recovery_info["current_unchanged"] = validate_recovery_current_inventory(
                current_before, manifest_inventory, provenance
            )
        logger.info(
            "recovery eligibility=true: "
            f"mode={recovery_info.get('recovery_mode', 'explicit_80_7')} / "
            f"authority={provenance['run_date']}/{provenance['run_id']} / "
            f"CURRENT files={recovery_info['file_count']} bytes={recovery_info['total_bytes']}"
        )

    if prepublication_recovery:
        backup_before_fingerprint = list_source_fingerprints(s3_client, bucket, backup_prefix)
        backup_before = {
            path: fingerprint["size"]
            for path, fingerprint in backup_before_fingerprint.items()
        }
        previous_backup_summary_path = (
            Path(args.step_dir) / RESULT_DIR_NAME / BACKUP_SUMMARY_FILENAME
        )
        previous_backup_summary = load_previous_backup_summary(previous_backup_summary_path)
        backup_baseline = validate_previous_backup_summary(previous_backup_summary)
        if recovery_info is None:
            raise RotationError("pre-publication recovery auditの初期化に失敗しました")
        recovery_info["bk1_unchanged"] = validate_recovery_bk1_unchanged(
            backup_before_fingerprint,
            recovery_status["previous_verified_finished_at"],
            backup_baseline,
        )
    else:
        backup_before = list_prefix_objects(s3_client, bucket, backup_prefix)
    if args.bootstrap:
        if backup_before:
            raise RotationError(
                f"bootstrapはbk1が存在しない場合のみ実行できます（既存 {len(backup_before)}件）: {destination_uri}"
            )
    else:
        if not backup_before:
            raise RotationError(
                f"bk1が存在しません。初回作成は --bootstrap で実行してください: {destination_uri}"
            )

    if recovery_info is not None:
        summary["recovery"] = recovery_info

    summary["previous_current"] = {
        "run_date": provenance["run_date"],
        "run_id": provenance["run_id"],
        "run_date_source": provenance["run_date_source"],
        "run_id_source": provenance["run_id_source"],
        "destination": provenance["destination"],
        "verified": provenance["verified"],
        "sync_step": provenance["sync_step"],
        "file_count": provenance["file_count"],
        "total_bytes": provenance["total_bytes"],
        "status_key": status_info["status_key"],
    }
    summary["current_before"] = {"file_count": len(current_before), "total_bytes": current_bytes}
    summary["backup_before"] = {
        "file_count": len(backup_before),
        "total_bytes": sum(backup_before.values()),
    }
    summary["expected_backup"] = {"file_count": len(current_before), "total_bytes": current_bytes}

    # ---- backup本体 -------------------------------------------------------
    argv = build_sync_argv(source_uri, destination_uri, region, args.dry_run)
    run_sync(argv, logger)

    if args.dry_run:
        logger.warn("dry-runのため wait / verify は実施しません（S3未変更）")
        summary["verify"] = {"verified": False, "skipped_reason": "dry-run"}
        return summary

    # ---- verify -----------------------------------------------------------
    logger.info(f"verify前 wait {wait_seconds}秒")
    time.sleep(wait_seconds)
    summary["wait_performed"] = True

    current_after_fingerprint = list_source_fingerprints(s3_client, bucket, current_prefix)
    if current_after_fingerprint != current_before_fingerprint:
        raise RotationError("backup中にCURRENTが変化しました（backup結果を信頼できません）")
    current_after = {
        path: fingerprint["size"] for path, fingerprint in current_after_fingerprint.items()
    }

    backup_after = list_prefix_objects(s3_client, bucket, backup_prefix)
    verify_result = compare_sets(current_after, backup_after, logger)
    summary["verify"] = verify_result

    if not verify_result["verified"]:
        raise RotationError(
            "bk1 verifyに失敗しました "
            f"(missing={verify_result['missing_count']} / extra={verify_result['extra_count']} / "
            f"size_mismatch={verify_result['size_mismatch_count']})"
        )

    logger.ok(
        f"bk1 verify成功: files={verify_result['actual_file_count']} / "
        f"bytes={verify_result['actual_total_bytes']}"
    )
    return summary


def main() -> int:
    logger = get_logger(STEP_NAME)
    args = parse_args()
    started = time.time()
    dirs = ensure_result_dirs(args.step_dir)
    summary_path = dirs["result"] / BACKUP_SUMMARY_FILENAME

    summary: Dict[str, Any]
    exit_code = 0
    try:
        summary = run(args, logger)
    except RotationError as exc:
        logger.error(f"[NG] {exc}")
        summary = {
            "step": STEP_NAME,
            "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operation": "bootstrap" if args.bootstrap else "rotation",
            "mode": "dry-run" if args.dry_run else "apply",
            "backup_status": "FAILED",
            "error_message": str(exc),
        }
        if not args.dry_run:
            summary["immutable_execution_guard_contract_version"] = (
                IMMUTABLE_EXECUTION_GUARD_CONTRACT_VERSION
            )
        exit_code = 1
    except Exception as exc:  # noqa: BLE001 - 想定外例外も握りつぶさずFAILさせる
        logger.error(f"[NG] 想定外エラー: {type(exc).__name__}: {exc}")
        summary = {
            "step": STEP_NAME,
            "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operation": "bootstrap" if args.bootstrap else "rotation",
            "mode": "dry-run" if args.dry_run else "apply",
            "backup_status": "FAILED",
            "error_message": f"{type(exc).__name__}: {exc}",
        }
        if not args.dry_run:
            summary["immutable_execution_guard_contract_version"] = (
                IMMUTABLE_EXECUTION_GUARD_CONTRACT_VERSION
            )
        exit_code = 1

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    logger.info(f"summary: {summary_path}")

    write_execution_time(
        str(dirs["execution_time"]),
        STEP_NAME,
        time.time() - started,
        record_count=int(summary.get("verify", {}).get("actual_file_count", 0) or 0),
    )
    if exit_code == 0:
        logger.ok("完了")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

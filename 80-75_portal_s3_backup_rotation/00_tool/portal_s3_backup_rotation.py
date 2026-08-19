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
- backup本体: `aws s3 sync CURRENT BK1 --delete`（argv配列 / shell未使用 /
  include-excludeフィルタ未使用）
- backup後 PORTAL_S3_VERIFY_WAIT_SEC 秒待ち、CURRENT と bk1 を全件LISTして
  {relative_path: size} で完全比較する。missing / extra / size mismatch はすべて異常終了。
- 初回のbk1作成は通常rotationと分離し、`--bootstrap` を明示指定した場合のみ許可する。

pipeline-status（CONTROL）/ pipeline-logs（AUDIT）/ private / root直下ZIP は
backup対象外prefixのため一切変更しない（pipeline-status は read-only参照のみ）。

usage:
  portal_s3_backup_rotation.py [--bootstrap] [--dry-run]
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_execution_time  # noqa: E402
from common.logger import get_logger  # noqa: E402
from common.pipeline_s3_env import get_config_value, load_pipeline_s3_config  # noqa: E402

STEP_NAME = "80-75_portal_s3_backup_rotation"
STEP_DIR = Path(__file__).resolve().parents[1]

SYNC_STEP_DIR_NAME = "80-9_portal_s3_sync"
SYNC_SUMMARY_FILENAME = "portal_s3_sync_summary.json"
BACKUP_SUMMARY_FILENAME = "portal_s3_backup_rotation_summary.json"

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


def _require_count(container: Dict[str, Any], key: str) -> int:
    value = container.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RotationError(f"previous 80-9 summaryの{key}が不正です: {value!r}")
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
        "file_count": actual_files,
        "total_bytes": actual_bytes,
    }


def build_s3_client(region: str):
    """boto3 S3クライアントを生成する（テストから差し替え可能にするため関数化）。"""
    import boto3

    return boto3.client("s3", region_name=region)


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
    missing_keys = sorted(STATUS_REQUIRED_KEYS - set(document))
    if missing_keys:
        raise RotationError(f"current RUNNING statusに必須keyがありません: {missing_keys}")
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


def guard_pipeline_status(
    s3_client,
    bucket: str,
    base_prefix: str,
    status_prefix: str,
    provenance: Dict[str, Any],
    current_identity: Optional[Dict[str, str]],
    logger,
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
    if any(run.get("last_modified") is None for run in runs):
        raise RotationError("pipeline-status LISTにLastModifiedがありません（順序判定不能）")

    latest = sorted(runs, key=_sort_key)[-1]
    if latest["run_date"] != provenance["run_date"] or latest["run_id"] != provenance["run_id"]:
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
    return {"status_key": latest["key"], "status": document.get("status"), "exit_code": 0}


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

    # ---- previous CURRENT 正常性guard ------------------------------------
    previous_summary = load_previous_sync_summary(sync_summary_path)
    provenance = validate_previous_sync_summary(previous_summary)
    logger.info(
        f"previous CURRENT: run={provenance['run_date']}/{provenance['run_id']} "
        f"files={provenance['file_count']} bytes={provenance['total_bytes']}"
    )

    s3_client = build_s3_client(region)
    current_identity = resolve_current_managed_identity(args)
    status_info = guard_pipeline_status(
        s3_client, bucket, base_prefix, status_prefix, provenance, current_identity, logger
    )

    current_before_fingerprint = list_source_fingerprints(s3_client, bucket, current_prefix)
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

    summary["previous_current"] = {
        "run_date": provenance["run_date"],
        "run_id": provenance["run_id"],
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

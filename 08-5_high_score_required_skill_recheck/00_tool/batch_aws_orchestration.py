#!/usr/bin/env python3
"""Connect the Issue 1 Batch engine to the two-phase AWS pipeline runtime.

This module only owns the EC2-side interface: deterministic Batch run identity,
S3 persistence/restore, Phase A submit/resume selection, and the Phase B commit
gate.  Batch evaluation, collection, safety checks, and publication remain in
``high_score_required_skill_recheck_batch``.
"""

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import boto3


TOOL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOL_DIR))

import high_score_required_skill_recheck_batch as ENGINE  # noqa: E402


DEFAULT_BUCKET = "technoverse"
DEFAULT_BASE_PREFIX = "pipeline_ses_steps"
DEFAULT_REGION = "ap-northeast-1"
STATE_PREFIX = "batch-state/08-5"
PIPELINE_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
RUN_DATE_RE = re.compile(r"^[0-9]{8}$")
REMOTE_ARTIFACTS = ("input.jsonl", "manifest.jsonl", "submit.claim")


class OrchestrationError(RuntimeError):
    """Fail-closed AWS integration error."""


def _required_identity(pipeline_run_id: str, run_date: str) -> None:
    if not PIPELINE_RUN_ID_RE.fullmatch(pipeline_run_id or ""):
        raise ValueError("pipeline run_id contract不正")
    if not RUN_DATE_RE.fullmatch(run_date or ""):
        raise ValueError("run_dateはYYYYMMDD必須")


def batch_run_id_for(pipeline_run_id: str) -> str:
    """Return a stable Issue 1-compatible ID without changing pipeline run_id."""
    if not PIPELINE_RUN_ID_RE.fullmatch(pipeline_run_id or ""):
        raise ValueError("pipeline run_id contract不正")
    return "p" + hashlib.sha256(pipeline_run_id.encode("utf-8")).hexdigest()[:23]


def _settings() -> Tuple[str, str, str]:
    bucket = os.environ.get("PIPELINE_S3_BUCKET", DEFAULT_BUCKET)
    base_prefix = os.environ.get("PIPELINE_S3_BASE_PREFIX", DEFAULT_BASE_PREFIX).strip("/")
    region = os.environ.get("PIPELINE_AWS_REGION", DEFAULT_REGION)
    if not bucket or not base_prefix or any(
        part in ("", ".", "..") for part in base_prefix.split("/")
    ):
        raise OrchestrationError("S3 bucket/base prefix contract不正")
    return bucket, base_prefix, region


def state_prefix(pipeline_run_id: str, run_date: str) -> str:
    _required_identity(pipeline_run_id, run_date)
    _, base_prefix, _ = _settings()
    return f"{base_prefix}/{STATE_PREFIX}/{run_date}/{pipeline_run_id}"


def _json_bytes(value: Dict[str, Any]) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )


def _read_s3_object(s3: Any, bucket: str, key: str) -> bytes:
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response.get("Body")
    if body is None:
        raise OrchestrationError(f"S3 object body欠落: {key}")
    payload = body.read()
    if not isinstance(payload, bytes):
        raise OrchestrationError(f"S3 object body型不正: {key}")
    return payload


def _remote_state(
    s3: Any, bucket: str, prefix: str
) -> Optional[Dict[str, Any]]:
    try:
        payload = _read_s3_object(s3, bucket, f"{prefix}/state.json")
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404", "NotFound"):
            return None
        raise
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise OrchestrationError("S3 state.jsonがvalid UTF-8 JSONではありません") from error
    if not isinstance(parsed, dict):
        raise OrchestrationError("S3 state.jsonがJSON objectではありません")
    return parsed


def _validate_state_identity(
    state: Dict[str, Any], pipeline_run_id: str, run_date: str, batch_run_id: str
) -> None:
    if state.get("pipeline_run_id") != pipeline_run_id:
        raise OrchestrationError("state pipeline_run_id不一致")
    if state.get("run_date") != run_date:
        raise OrchestrationError("state run_date不一致")
    if state.get("run_id") != batch_run_id:
        raise OrchestrationError("state batch run_id不一致")
    manifest_sha256 = state.get("manifest_sha256")
    if not isinstance(manifest_sha256, str) or not re.fullmatch(
        r"[0-9a-f]{64}", manifest_sha256
    ):
        raise OrchestrationError("state manifest_sha256不正")


def _attach_pipeline_identity(
    run_dir: Path, pipeline_run_id: str, run_date: str, prefix: str
) -> Dict[str, Any]:
    store = ENGINE.FileStateStore(run_dir)
    state, etag = store.load()
    expected_batch_run_id = batch_run_id_for(pipeline_run_id)
    existing_pipeline_id = state.get("pipeline_run_id")
    existing_run_date = state.get("run_date")
    if existing_pipeline_id not in (None, pipeline_run_id):
        raise OrchestrationError("local state pipeline_run_id不一致")
    if existing_run_date not in (None, run_date):
        raise OrchestrationError("local state run_date不一致")
    if state.get("run_id") != expected_batch_run_id:
        raise OrchestrationError("local state batch run_id不一致")
    updates = {
        "pipeline_run_id": pipeline_run_id,
        "run_date": run_date,
        "state_s3_prefix": prefix,
    }
    if any(state.get(key) != value for key, value in updates.items()):
        state.update(updates)
        store.cas(etag, state)
        state, _ = store.load()
    _validate_state_identity(state, pipeline_run_id, run_date, expected_batch_run_id)
    return state


def persist_run(
    s3: Any, bucket: str, prefix: str, run_dir: Path
) -> Dict[str, Any]:
    """Upload immutable artifacts first and state.json last."""
    state, _ = ENGINE.FileStateStore(run_dir).load()
    for filename in REMOTE_ARTIFACTS:
        path = run_dir / filename
        if not path.exists():
            if filename == "submit.claim":
                continue
            raise OrchestrationError(f"Batch artifact欠落: {filename}")
        s3.put_object(
            Bucket=bucket,
            Key=f"{prefix}/{filename}",
            Body=path.read_bytes(),
            ContentType="application/jsonl" if filename.endswith(".jsonl") else "application/json",
        )
    s3.put_object(
        Bucket=bucket,
        Key=f"{prefix}/state.json",
        Body=_json_bytes(state),
        ContentType="application/json; charset=utf-8",
    )
    return state


def restore_run(
    s3: Any,
    bucket: str,
    prefix: str,
    pipeline_run_id: str,
    run_date: str,
    runtime_root: Path = ENGINE.RUNTIME_ROOT,
) -> Optional[Path]:
    remote = _remote_state(s3, bucket, prefix)
    if remote is None:
        return None
    batch_run_id = batch_run_id_for(pipeline_run_id)
    _validate_state_identity(remote, pipeline_run_id, run_date, batch_run_id)
    run_dir = ENGINE._run_dir(batch_run_id, runtime_root)

    if (run_dir / "batch_state.json").exists():
        local, _ = ENGINE.FileStateStore(run_dir).load()
        _validate_state_identity(local, pipeline_run_id, run_date, batch_run_id)
        local_revision = int(local.get("state_revision", -1))
        remote_revision = int(remote.get("state_revision", -1))
        if local_revision > remote_revision:
            return run_dir

    run_dir.mkdir(parents=True, exist_ok=True)
    for filename in ("input.jsonl", "manifest.jsonl"):
        payload = _read_s3_object(s3, bucket, f"{prefix}/{filename}")
        ENGINE._atomic_write_bytes(run_dir / filename, payload)
    try:
        claim = _read_s3_object(s3, bucket, f"{prefix}/submit.claim")
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if code not in ("NoSuchKey", "404", "NotFound"):
            raise
    else:
        ENGINE._atomic_write_bytes(run_dir / "submit.claim", claim)
    ENGINE._atomic_write_bytes(run_dir / "batch_state.json", _json_bytes(remote))
    ENGINE.validate_prepared(run_dir)
    return run_dir


def phase_a(
    pipeline_run_id: str,
    run_date: str,
    s3: Optional[Any] = None,
    runtime_root: Path = ENGINE.RUNTIME_ROOT,
    client: Optional[Any] = None,
) -> Dict[str, Any]:
    """Prepare/submit once, persist state, and return the Batch-wait contract."""
    _required_identity(pipeline_run_id, run_date)
    bucket, _, region = _settings()
    prefix = state_prefix(pipeline_run_id, run_date)
    s3_client = s3 or boto3.client("s3", region_name=region)
    batch_run_id = batch_run_id_for(pipeline_run_id)
    run_dir = restore_run(
        s3_client, bucket, prefix, pipeline_run_id, run_date, runtime_root
    )
    if run_dir is None:
        run_dir = ENGINE._run_dir(batch_run_id, runtime_root)
        if not (run_dir / "batch_state.json").exists():
            ENGINE.prepare_run(batch_run_id, runtime_root=runtime_root)
        _attach_pipeline_identity(run_dir, pipeline_run_id, run_date, prefix)
        persist_run(s3_client, bucket, prefix, run_dir)
    else:
        _attach_pipeline_identity(run_dir, pipeline_run_id, run_date, prefix)

    store = ENGINE.FileStateStore(run_dir)
    state, _ = store.load()
    engine_state = str(state.get("state") or "")
    result: Dict[str, Any]
    if state.get("batch_id") or engine_state in {
        ENGINE.STATE_PENDING_RECONCILIATION,
        ENGINE.STATE_COMPLETED,
        ENGINE.STATE_COLLECTED,
        ENGINE.STATE_COMMITTED,
    }:
        result = {
            "resumed": True,
            "state": engine_state,
            "batch_id": state.get("batch_id"),
            "batch_status": state.get("batch_status"),
        }
    elif engine_state == ENGINE.STATE_PREPARED:
        batch_client = client or ENGINE.OpenAIHttpBatchClient()
        try:
            result = ENGINE.submit_run(
                batch_run_id, batch_client, runtime_root=runtime_root
            )
        except ENGINE.PendingReconciliation:
            state, _ = store.load()
            if state.get("state") != ENGINE.STATE_PENDING_RECONCILIATION:
                persist_run(s3_client, bucket, prefix, run_dir)
                raise
            result = {
                "resumed": True,
                "state": state.get("state"),
                "batch_id": state.get("batch_id"),
                "batch_status": state.get("batch_status"),
            }
    else:
        persist_run(s3_client, bucket, prefix, run_dir)
        raise OrchestrationError(
            f"自動submit/resubmit不可のBatch stateです: {engine_state}"
        )

    state = persist_run(s3_client, bucket, prefix, run_dir)
    return {
        "contract": "SUSPENDED",
        "reason": "BATCH_WAIT",
        "current_step": "08-5_BATCH_WAIT",
        "pipeline_run_id": pipeline_run_id,
        "run_date": run_date,
        "batch_run_id": batch_run_id,
        "batch_id": state.get("batch_id"),
        "batch_status": state.get("batch_status"),
        "state": state.get("state"),
        "manifest_sha256": state.get("manifest_sha256"),
        "resumed": bool(result.get("resumed")),
        "state_s3_prefix": prefix,
    }


def phase_b(
    pipeline_run_id: str,
    run_date: str,
    s3: Optional[Any] = None,
    runtime_root: Path = ENGINE.RUNTIME_ROOT,
    client: Optional[Any] = None,
) -> Dict[str, Any]:
    """Restore completed state, collect/publish, and enforce the marker gate."""
    _required_identity(pipeline_run_id, run_date)
    bucket, _, region = _settings()
    prefix = state_prefix(pipeline_run_id, run_date)
    s3_client = s3 or boto3.client("s3", region_name=region)
    run_dir = restore_run(
        s3_client, bucket, prefix, pipeline_run_id, run_date, runtime_root
    )
    if run_dir is None:
        raise OrchestrationError("Phase B対象のS3 Batch stateがありません")
    batch_run_id = batch_run_id_for(pipeline_run_id)
    state = _attach_pipeline_identity(run_dir, pipeline_run_id, run_date, prefix)
    if state.get("batch_status") != "completed":
        raise OrchestrationError(
            f"Phase B開始時Batchがcompletedではありません: {state.get('batch_status')!r}"
        )
    manifest_sha256 = str(state.get("manifest_sha256") or "")
    batch_client = client or ENGINE.OpenAIHttpBatchClient()
    result = ENGINE.collect_run(
        batch_run_id, batch_client, runtime_root=runtime_root, publish=True
    )
    marker = ENGINE.validate_commit_marker(
        expected_run_id=batch_run_id,
        expected_manifest_sha256=manifest_sha256,
    )
    committed_state = persist_run(s3_client, bucket, prefix, run_dir)
    if committed_state.get("state") != ENGINE.STATE_COMMITTED:
        raise OrchestrationError("collector後stateがCOMMITTEDではありません")
    return {
        "gate": "PRODUCTION_COMMIT_VALID",
        "pipeline_run_id": pipeline_run_id,
        "run_date": run_date,
        "batch_run_id": batch_run_id,
        "batch_id": committed_state.get("batch_id"),
        "manifest_sha256": manifest_sha256,
        "marker_run_id": marker.get("run_id"),
        "marker_manifest_sha256": marker.get("manifest_sha256"),
        "collector_retry": bool(result.get("collector_retry")),
        "state_s3_prefix": prefix,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("phase-a", "phase-b"):
        child = subparsers.add_parser(command)
        child.add_argument("--pipeline-run-id", required=True)
        child.add_argument("--run-date", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "phase-a":
            result = phase_a(args.pipeline_run_id, args.run_date)
        else:
            result = phase_b(args.pipeline_run_id, args.run_date)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except Exception as error:
        print(f"ERROR: {type(error).__name__}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

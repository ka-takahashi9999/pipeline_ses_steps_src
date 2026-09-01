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
REMOTE_ARTIFACTS = (
    "input.jsonl",
    "manifest.jsonl",
    "submit.claim",
    "recovery.claim",
)


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
    state, _ = _remote_state_version(s3, bucket, prefix)
    return state


def _remote_state_version(
    s3: Any, bucket: str, prefix: str
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        response = s3.get_object(Bucket=bucket, Key=f"{prefix}/state.json")
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404", "NotFound"):
            return None, None
        raise
    body = response.get("Body")
    if body is None:
        raise OrchestrationError("S3 state.json body欠落")
    payload = body.read()
    if not isinstance(payload, bytes):
        raise OrchestrationError("S3 state.json body型不正")
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise OrchestrationError("S3 state.jsonがvalid UTF-8 JSONではありません") from error
    if not isinstance(parsed, dict):
        raise OrchestrationError("S3 state.jsonがJSON objectではありません")
    etag = response.get("ETag")
    return parsed, str(etag) if etag else None


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
            if filename in ("submit.claim", "recovery.claim"):
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


def _persist_recovery_checkpoint(
    s3: Any,
    bucket: str,
    prefix: str,
    state: Dict[str, Any],
    expected_remote_revision: int,
    allow_revision_jump: bool = False,
) -> int:
    """CAS one Recovery state transition to S3 before its next side effect."""
    remote, remote_etag = _remote_state_version(s3, bucket, prefix)
    if remote is None:
        raise OrchestrationError("Recovery checkpoint対象のS3 stateがありません")
    remote_revision = int(remote.get("state_revision", -1))
    next_revision = int(state.get("state_revision", -1))
    if remote_revision != expected_remote_revision:
        raise OrchestrationError(
            "Recovery checkpoint S3 revision競合: "
            f"expected={expected_remote_revision} actual={remote_revision}"
        )
    valid_next_revision = (
        next_revision > expected_remote_revision
        if allow_revision_jump
        else next_revision == expected_remote_revision + 1
    )
    if not valid_next_revision:
        raise OrchestrationError(
            "Recovery checkpoint local revision不正: "
            f"expected_after={expected_remote_revision} actual={next_revision}"
        )
    if any(
        remote.get(field) != state.get(field)
        for field in ("run_id", "pipeline_run_id", "run_date", "manifest_sha256")
    ):
        raise OrchestrationError("Recovery checkpoint identity不一致")
    kwargs: Dict[str, Any] = {}
    if remote_etag:
        kwargs["IfMatch"] = remote_etag
    try:
        s3.put_object(
            Bucket=bucket,
            Key=f"{prefix}/state.json",
            Body=_json_bytes(state),
            ContentType="application/json; charset=utf-8",
            **kwargs,
        )
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if code in ("PreconditionFailed", "ConditionalRequestConflict", "412"):
            raise OrchestrationError("Recovery checkpoint S3 CAS競合") from error
        raise
    return next_revision


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
    for claim_name in ("submit.claim", "recovery.claim"):
        try:
            claim = _read_s3_object(s3, bucket, f"{prefix}/{claim_name}")
        except Exception as error:
            code = getattr(error, "response", {}).get("Error", {}).get("Code")
            if code not in ("NoSuchKey", "404", "NotFound"):
                raise
        else:
            ENGINE._atomic_write_bytes(run_dir / claim_name, claim)
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
        except ENGINE.FileReadinessError:
            persist_run(s3_client, bucket, prefix, run_dir)
            raise
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


def _acquire_remote_recovery_claim(
    s3: Any,
    bucket: str,
    prefix: str,
    claim_payload: bytes,
    state: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        s3.put_object(
            Bucket=bucket,
            Key=f"{prefix}/recovery.claim",
            Body=claim_payload,
            ContentType="application/json; charset=utf-8",
            IfNoneMatch="*",
        )
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        if code in ("PreconditionFailed", "ConditionalRequestConflict", "412"):
            if state is None:
                raise ENGINE.SubmissionBlocked(
                    "persistent S3 recovery claim取得済み: 二重submitを拒否"
                ) from error
            try:
                existing = json.loads(
                    _read_s3_object(s3, bucket, f"{prefix}/recovery.claim").decode(
                        "utf-8"
                    )
                )
            except Exception as read_error:
                raise ENGINE.SubmissionBlocked(
                    "persistent S3 recovery claim証拠不正"
                ) from read_error
            if not isinstance(existing, dict):
                raise ENGINE.SubmissionBlocked(
                    "persistent S3 recovery claim証拠不正"
                ) from error
            ENGINE._validate_owned_recovery_claim(existing, state)
            remote = _remote_state(s3, bucket, prefix)
            if (
                remote is None
                or remote.get("recovery_attempt_count") != 1
                or remote.get("recovery_state") != state.get("recovery_state")
            ):
                raise ENGINE.SubmissionBlocked(
                    "persistent S3 recovery claim/state不一致"
                ) from error
            ENGINE._validate_owned_recovery_claim(existing, remote)
            return
        raise


def phase_recovery(
    pipeline_run_id: str,
    run_date: str,
    s3: Optional[Any] = None,
    runtime_root: Path = ENGINE.RUNTIME_ROOT,
    client: Optional[Any] = None,
) -> Dict[str, Any]:
    """Run only the claimed one-time 08-5 file visibility recovery on EC2."""
    _required_identity(pipeline_run_id, run_date)
    bucket, _, region = _settings()
    prefix = state_prefix(pipeline_run_id, run_date)
    s3_client = s3 or boto3.client("s3", region_name=region)
    run_dir = restore_run(
        s3_client, bucket, prefix, pipeline_run_id, run_date, runtime_root
    )
    if run_dir is None:
        raise OrchestrationError("Recovery対象のS3 Batch stateがありません")
    batch_run_id = batch_run_id_for(pipeline_run_id)
    state = _attach_pipeline_identity(run_dir, pipeline_run_id, run_date, prefix)
    store = ENGINE.FileStateStore(run_dir)
    recovery_nonce = str(state.get("recovery_nonce") or "")
    if not recovery_nonce:
        raise OrchestrationError("recovery_nonce欠落")
    remote_state = _remote_state(s3_client, bucket, prefix)
    if remote_state is None:
        raise OrchestrationError("Recovery対象のS3 Batch stateがありません")
    remote_revision = int(remote_state.get("state_revision", -1))

    def checkpoint(checkpoint_state: Dict[str, Any]) -> None:
        nonlocal remote_revision
        remote_revision = _persist_recovery_checkpoint(
            s3_client,
            bucket,
            prefix,
            checkpoint_state,
            remote_revision,
        )

    local_revision = int(state.get("state_revision", -1))
    if local_revision > remote_revision:
        if (
            state.get("recovery_attempt_count") != 1
            or state.get("recovery_state")
            not in {
                ENGINE.RECOVERY_CLAIMED,
                ENGINE.RECOVERY_FILE_UPLOADED,
                ENGINE.RECOVERY_PENDING_RECONCILIATION,
                ENGINE.RECOVERY_SUBMITTED,
            }
        ):
            raise OrchestrationError("Recovery local checkpoint state不正")
        remote_revision = _persist_recovery_checkpoint(
            s3_client,
            bucket,
            prefix,
            state,
            remote_revision,
            allow_revision_jump=True,
        )
    elif local_revision < remote_revision:
        raise OrchestrationError("Recovery local state revisionがS3より古いです")

    if (
        state.get("recovery_attempt_count") == 1
        and state.get("recovery_state")
        in (ENGINE.RECOVERY_SUBMITTED, ENGINE.RECOVERY_PENDING_RECONCILIATION)
    ):
        return {
            "contract": "SUSPENDED",
            "reason": "BATCH_WAIT",
            "current_step": "08-5_BATCH_WAIT",
            "pipeline_run_id": pipeline_run_id,
            "run_date": run_date,
            "batch_run_id": batch_run_id,
            "batch_id": state.get("batch_id"),
            "state": state.get("state"),
            "recovery_attempt_count": 1,
            "resumed": True,
            "state_s3_prefix": prefix,
        }

    def acquire_claim(
        claim_payload: bytes, checkpoint_state: Dict[str, Any]
    ) -> None:
        _acquire_remote_recovery_claim(
            s3_client,
            bucket,
            prefix,
            claim_payload,
            state=checkpoint_state,
        )

    batch_client = client or ENGINE.OpenAIHttpBatchClient()
    try:
        result = ENGINE.recover_file_visibility_failure(
            batch_run_id,
            batch_client,
            runtime_root=runtime_root,
            checkpoint_callback=checkpoint,
            claim_callback=acquire_claim,
        )
    except ENGINE.PendingReconciliation:
        state, _ = store.load()
        if state.get("state") != ENGINE.RECOVERY_PENDING_RECONCILIATION:
            raise
        result = {
            "state": state.get("state"),
            "batch_id": None,
            "resumed": True,
        }
    except Exception:
        state, _ = store.load()
        if (
            state.get("state") == ENGINE.STATE_SAFE_STOPPED
            and int(state.get("state_revision", -1)) == remote_revision + 1
        ):
            checkpoint(state)
        raise

    state, _ = store.load()
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
        "recovery_attempt_count": state.get("recovery_attempt_count"),
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
    for command in ("phase-a", "phase-b", "phase-recovery"):
        child = subparsers.add_parser(command)
        child.add_argument("--pipeline-run-id", required=True)
        child.add_argument("--run-date", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "phase-a":
            result = phase_a(args.pipeline_run_id, args.run_date)
        elif args.command == "phase-recovery":
            result = phase_recovery(args.pipeline_run_id, args.run_date)
        else:
            result = phase_b(args.pipeline_run_id, args.run_date)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except Exception as error:
        print(f"ERROR: {type(error).__name__}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

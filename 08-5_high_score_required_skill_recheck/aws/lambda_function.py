"""Status-only Lambda for the 08-5 OpenAI Batch wait loop."""

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3


API_BASE_URL = "https://api.openai.com/v1"
API_KEY_PARAMETER = "/openai/api_key"
DEFAULT_BUCKET = "technoverse"
DEFAULT_BASE_PREFIX = "pipeline_ses_steps"
DEFAULT_REGION = "ap-northeast-1"
STATE_PREFIX = "batch-state/08-5"
WAIT_STATUSES = {"validating", "in_progress", "finalizing", "cancelling"}
FAILURE_STATUSES = {"failed", "expired", "cancelled"}
TIMESTAMP_FIELDS = (
    "created_at",
    "in_progress_at",
    "expires_at",
    "finalizing_at",
    "completed_at",
    "failed_at",
    "expired_at",
    "cancelling_at",
    "cancelled_at",
)
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
RUN_DATE_RE = re.compile(r"^[0-9]{8}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class StatusError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _settings() -> Dict[str, str]:
    values = {
        "bucket": os.environ.get("PIPELINE_S3_BUCKET", DEFAULT_BUCKET),
        "base_prefix": os.environ.get(
            "PIPELINE_S3_BASE_PREFIX", DEFAULT_BASE_PREFIX
        ).strip("/"),
        "region": os.environ.get("PIPELINE_AWS_REGION", DEFAULT_REGION),
    }
    if not values["bucket"] or not values["base_prefix"]:
        raise StatusError("Lambda S3 settings不正")
    return values


def _state_key(base_prefix: str, run_date: str, run_id: str) -> str:
    return f"{base_prefix}/{STATE_PREFIX}/{run_date}/{run_id}/state.json"


def _pipeline_status_key(base_prefix: str, run_date: str, run_id: str) -> str:
    return f"{base_prefix}/pipeline-status/{run_date}/{run_id}/status.json"


def _read_json_s3(s3: Any, bucket: str, key: str) -> Dict[str, Any]:
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response.get("Body")
    if body is None:
        raise StatusError(f"S3 body欠落: {key}")
    try:
        parsed = json.loads(body.read().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise StatusError(f"S3 JSON不正: {key}") from error
    if not isinstance(parsed, dict):
        raise StatusError(f"S3 JSON object不正: {key}")
    return parsed


def _put_json_s3(s3: Any, bucket: str, key: str, value: Dict[str, Any]) -> None:
    body = (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )


def _validate_identity(state: Dict[str, Any], run_date: str, run_id: str) -> None:
    if state.get("pipeline_run_id") != run_id or state.get("run_date") != run_date:
        raise StatusError("Batch state pipeline identity不一致")
    batch_run_id = state.get("run_id")
    if not isinstance(batch_run_id, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_-]{0,23}", batch_run_id
    ):
        raise StatusError("Batch state internal run_id不正")
    if not SHA256_RE.fullmatch(str(state.get("manifest_sha256") or "")):
        raise StatusError("Batch state manifest_sha256不正")


def _api_key(ssm: Any) -> str:
    response = ssm.get_parameter(Name=API_KEY_PARAMETER, WithDecryption=True)
    value = str(response.get("Parameter", {}).get("Value") or "")
    if not value:
        raise StatusError("OpenAI API keyが空です")
    return value


def _openai_get(path: str, api_key: str, query: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    url = API_BASE_URL + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {api_key}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = response.read()
    except urllib.error.HTTPError as error:
        raise StatusError(f"OpenAI status HTTP {error.code}") from error
    except urllib.error.URLError as error:
        raise StatusError("OpenAI status network error") from error
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise StatusError("OpenAI status JSON不正") from error
    if not isinstance(parsed, dict):
        raise StatusError("OpenAI status responseがobjectではありません")
    return parsed


def _metadata_matches(batch: Dict[str, Any], state: Dict[str, Any]) -> bool:
    metadata = batch.get("metadata")
    if not isinstance(metadata, dict):
        return False
    expected = {
        "run_id": str(state.get("run_id") or ""),
        "submission_nonce": str(state.get("submission_nonce") or ""),
        "manifest_sha256": str(state.get("manifest_sha256") or ""),
    }
    recovery_pending = state.get("state") == "RECOVERY_PENDING_RECONCILIATION"
    if recovery_pending:
        expected.update(
            {
                "recovery_nonce": str(state.get("recovery_nonce") or ""),
                "recovery_attempt_count": "1",
            }
        )
    if not all(str(metadata.get(key) or "") == value for key, value in expected.items()):
        return False
    input_file_id = str(
        (
            state.get("recovery_file_id")
            if recovery_pending
            else state.get("input_file_id")
        )
        or ""
    )
    return not input_file_id or str(batch.get("input_file_id") or "") == input_file_id


def _sanitize_batch_terminal_errors(batch: Dict[str, Any]) -> List[Dict[str, Any]]:
    errors = batch.get("errors")
    data = errors.get("data") if isinstance(errors, dict) else None
    if not isinstance(data, list):
        return []

    def safe_text(value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        sanitized = re.sub(
            r"(?i)\bBearer\s+\S+", "Bearer [REDACTED]", value
        )
        sanitized = re.sub(
            r"\bsk-[A-Za-z0-9_-]{8,}\b", "[REDACTED]", sanitized
        )
        return sanitized[:2000]

    sanitized_errors: List[Dict[str, Any]] = []
    for item in data[:100]:
        if not isinstance(item, dict):
            continue
        line = item.get("line")
        sanitized_errors.append(
            {
                "code": safe_text(item.get("code")),
                "message": safe_text(item.get("message")),
                "param": safe_text(item.get("param")),
                "line": (
                    line
                    if isinstance(line, int) and not isinstance(line, bool)
                    else None
                ),
            }
        )
    return sanitized_errors


def _reconcile_pending(state: Dict[str, Any], api_key: str) -> Optional[Dict[str, Any]]:
    recovery_pending = state.get("state") == "RECOVERY_PENDING_RECONCILIATION"
    expected_file_id = state.get("recovery_file_id") if recovery_pending else state.get("input_file_id")
    if not expected_file_id:
        raise StatusError("PENDING_RECONCILIATION input_file_id欠落")
    listed = _openai_get("/batches", api_key, {"limit": "100"})
    data = listed.get("data")
    if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
        raise StatusError("OpenAI batch list data不正")
    matches: List[Dict[str, Any]] = [item for item in data if _metadata_matches(item, state)]
    state["reconciliation_checks"] = int(state.get("reconciliation_checks", 0)) + 1
    if len(matches) == 1:
        batch_id = str(matches[0].get("id") or "")
        if not batch_id:
            raise StatusError("reconciliation batch_id欠落")
        state["batch_id"] = batch_id
        state["state"] = "SUBMITTED"
        state["reconciled_at"] = utc_now()
        if recovery_pending:
            if batch_id == state.get("original_batch_id"):
                raise StatusError("reconciliation Recovery batch_idがoriginalと同一です")
            state["input_file_id"] = str(expected_file_id)
            state["recovery_batch_id"] = batch_id
            state["recovery_state"] = "RECOVERY_SUBMITTED"
            state["recovery_final_outcome"] = "WAITING"
        return _openai_get(f"/batches/{batch_id}", api_key)
    if len(matches) > 1:
        state["state"] = "SAFE_STOPPED"
        state["safe_stop_reason"] = "reconciliation_duplicate_batches"
        return None
    if int(state["reconciliation_checks"]) >= 3:
        state["state"] = "SAFE_STOPPED"
        state["safe_stop_reason"] = "reconciliation_no_match_after_bounded_checks"
    else:
        state["batch_status"] = "pending_reconciliation"
    return None


def _recovery_eligibility(
    state: Dict[str, Any], batch: Dict[str, Any]
) -> Dict[str, Any]:
    """Classify only the known zero-request file visibility validation failure."""
    if str(batch.get("status") or "") != "failed":
        return {"eligible": False, "reason": "terminal_status_not_failed"}
    attempt = state.get("recovery_attempt_count")
    if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt != 0:
        return {"eligible": False, "reason": "recovery_already_attempted"}
    counts = batch.get("request_counts")
    if not isinstance(counts, dict):
        return {"eligible": False, "reason": "request_counts_missing"}
    for field in ("total", "completed", "failed"):
        value = counts.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value != 0:
            return {"eligible": False, "reason": "request_counts_not_all_zero"}
    if "in_progress_at" not in batch or batch.get("in_progress_at") is not None:
        return {"eligible": False, "reason": "in_progress_at_not_null"}
    if "output_file_id" not in batch or batch.get("output_file_id") is not None:
        return {"eligible": False, "reason": "output_file_id_not_null"}
    if "error_file_id" not in batch or batch.get("error_file_id") is not None:
        return {"eligible": False, "reason": "error_file_id_not_null"}
    errors = batch.get("errors")
    data = errors.get("data") if isinstance(errors, dict) else None
    if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
        return {"eligible": False, "reason": "terminal_error_contract_unknown"}
    error = data[0]
    code = str(error.get("code") or "").lower()
    param = str(error.get("param") or "").lower()
    message = str(error.get("message") or "")
    visibility_message = bool(
        re.search(r"(?i)cannot\s+find\s+(?:the\s+)?file\b", message)
        or re.search(r"(?i)\bfile\b.{0,160}\bnot\s+found\b", message)
        or re.search(r"(?i)\borganization\b.{0,160}\bdoes\s+not\s+have\s+access\b", message)
    )
    if code != "invalid_request" or param != "file_id" or not visibility_message:
        return {"eligible": False, "reason": "not_known_file_visibility_error"}
    if not str(state.get("input_file_id") or "") or not str(
        state.get("batch_id") or ""
    ):
        return {"eligible": False, "reason": "original_identity_missing"}
    return {"eligible": True, "reason": "file_visibility_validation_failure"}


def _observe(state: Dict[str, Any], batch: Dict[str, Any]) -> str:
    batch_id = str(state.get("batch_id") or "")
    if str(batch.get("id") or "") != batch_id:
        raise StatusError("OpenAI retrieve batch_id不一致")
    status = str(batch.get("status") or "")
    if not status:
        raise StatusError("OpenAI Batch status欠落")
    state["batch_status"] = status
    for field in ("request_counts", "output_file_id", "error_file_id"):
        if field in batch:
            state[field] = batch[field]
    if status == "completed" or status in FAILURE_STATUSES:
        state["batch_errors"] = _sanitize_batch_terminal_errors(batch)
    timestamps: Dict[str, Any] = {}
    for field in TIMESTAMP_FIELDS:
        if field in batch:
            state[field] = batch[field]
            timestamps[field] = batch[field]
    state["batch_timestamps"] = timestamps
    state.setdefault("status_history", []).append(
        {"observed_at": utc_now(), "status": status}
    )
    if status == "completed" and state.get("state") not in {"COLLECTED", "COMMITTED"}:
        state["state"] = "COMPLETED"
        if state.get("recovery_attempt_count") == 1:
            state["recovery_state"] = "RECOVERY_COMPLETED"
            state["recovery_final_outcome"] = "COMPLETED"
    elif status in FAILURE_STATUSES:
        classification = _recovery_eligibility(state, batch)
        state["recovery_eligible"] = bool(classification["eligible"])
        state["recovery_reason"] = str(classification["reason"])
        if classification["eligible"]:
            state.update(
                {
                    "state": "RECOVERY_REQUIRED",
                    "recovery_state": "RECOVERY_REQUIRED",
                    "recovery_nonce": uuid.uuid4().hex,
                    "original_file_id": str(state.get("input_file_id") or ""),
                    "original_batch_id": batch_id,
                    "original_terminal_error": list(state.get("batch_errors") or []),
                    "original_request_counts": dict(batch.get("request_counts") or {}),
                    "recovery_final_outcome": "RECOVERY_REQUIRED",
                    "safe_stop_reason": None,
                }
            )
        else:
            state["state"] = "SAFE_STOPPED"
            state["safe_stop_reason"] = f"batch_{status}"
            if state.get("recovery_attempt_count") == 1:
                state["recovery_state"] = "SAFE_STOPPED"
                state["recovery_final_outcome"] = "SAFE_STOPPED"
    elif status not in WAIT_STATUSES:
        state["state"] = "SAFE_STOPPED"
        state["safe_stop_reason"] = f"unknown_batch_status:{status}"
    return status


def _write_pipeline_failed(
    s3: Any,
    bucket: str,
    base_prefix: str,
    run_date: str,
    run_id: str,
    reason: str,
) -> None:
    key = _pipeline_status_key(base_prefix, run_date, run_id)
    document = _read_json_s3(s3, bucket, key)
    if document.get("run_id") != run_id or document.get("run_date") != run_date:
        raise StatusError("pipeline status identity不一致")
    if document.get("status") == "FAILED":
        return
    if document.get("status") != "RUNNING" or document.get("current_step") != "08-5_BATCH_WAIT":
        raise StatusError("pipeline statusがBatch waitではありません")
    now = utc_now()
    document.update(
        {
            "status": "FAILED",
            "finished_at": now,
            "finished_at_source": "batch_status_lambda",
            "exit_code": 86,
            "exit_code_source": "batch_status_lambda",
            "error_message": reason,
            "updated_at": now,
        }
    )
    _put_json_s3(s3, bucket, key, document)


def check_status(
    event: Dict[str, Any],
    s3: Optional[Any] = None,
    ssm: Optional[Any] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    run_id = str(event.get("run_id") or "")
    run_date = str(event.get("run_date") or "")
    if not RUN_ID_RE.fullmatch(run_id) or not RUN_DATE_RE.fullmatch(run_date):
        raise StatusError("Lambda event run identity不正")
    settings = _settings()
    s3_client = s3 or boto3.client("s3", region_name=settings["region"])
    ssm_client = ssm or boto3.client("ssm", region_name=settings["region"])
    key = _state_key(settings["base_prefix"], run_date, run_id)
    state = _read_json_s3(s3_client, settings["bucket"], key)
    _validate_identity(state, run_date, run_id)

    key_value = api_key or _api_key(ssm_client)
    batch: Optional[Dict[str, Any]] = None
    if state.get("state") == "COMMITTED":
        status = "completed"
    elif state.get("state") == "RECOVERY_REQUIRED":
        status = "failed"
    elif state.get("state") in {
        "PENDING_RECONCILIATION",
        "RECOVERY_PENDING_RECONCILIATION",
    } and not state.get("batch_id"):
        batch = _reconcile_pending(state, key_value)
        status = str(state.get("batch_status") or "pending_reconciliation")
    else:
        batch_id = str(state.get("batch_id") or "")
        if not batch_id:
            raise StatusError("Batch state batch_id欠落")
        batch = _openai_get(f"/batches/{batch_id}", key_value)
        status = str(batch.get("status") or "")

    if batch is not None:
        status = _observe(state, batch)

    state["state_revision"] = int(state.get("state_revision", 0)) + 1
    state["state_updated_at"] = utc_now()
    _put_json_s3(s3_client, settings["bucket"], key, state)

    if state.get("state") == "RECOVERY_REQUIRED":
        return {
            "outcome": "RECOVERY_REQUIRED",
            "batch_status": status,
            "reason": str(state.get("recovery_reason") or ""),
            "run_id": run_id,
            "run_date": run_date,
        }
    if state.get("state") == "SAFE_STOPPED":
        reason = str(state.get("safe_stop_reason") or f"batch_{status}")
        _write_pipeline_failed(
            s3_client,
            settings["bucket"],
            settings["base_prefix"],
            run_date,
            run_id,
            reason,
        )
        return {
            "outcome": "FAILED",
            "batch_status": status,
            "reason": reason,
            "run_id": run_id,
            "run_date": run_date,
        }
    if status == "completed":
        return {
            "outcome": "COMPLETED",
            "batch_status": status,
            "run_id": run_id,
            "run_date": run_date,
        }
    if status not in WAIT_STATUSES and status != "pending_reconciliation":
        raise StatusError(f"nonterminal Batch status contract外: {status}")
    return {
        "outcome": "WAIT",
        "batch_status": status,
        "run_id": run_id,
        "run_date": run_date,
    }


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    return check_status(event)

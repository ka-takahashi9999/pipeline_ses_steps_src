"""08-5 production Batch engine (Issue 1 only).

OpenAI Batchのprepare/submit/resume/reconciliation/collectorと、commit markerを
伴うtransactional publishを提供する。AWS orchestrationやresource操作は扱わない。
network処理はCLIの``--allow-network``明示時だけ実行できる。
"""

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[2]
STEP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(TOOL_DIR))

from common.json_utils import read_jsonl, write_jsonl  # noqa: E402
from common.skillsheet_ai_context import build_skillsheet_ai_context  # noqa: E402

import batch_minimal_safety_guard as SAFETY_GUARD  # noqa: E402
import high_score_required_skill_recheck as DIRECT  # noqa: E402
import high_score_required_skill_recheck_core as SHARED_CORE  # noqa: E402


ENGINE_VERSION = "08-5-production-batch-v1"
RUNTIME_ROOT = STEP_DIR / "01_result/_batch_runtime"
RESULT_DIR = STEP_DIR / "01_result"
COMMIT_MARKER = RESULT_DIR / "production_commit.json"
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,23}$")
API_BASE_URL = "https://api.openai.com/v1"

STATE_PREPARED = "PREPARED"
STATE_CLAIMED = "CLAIMED"
STATE_PENDING_RECONCILIATION = "PENDING_RECONCILIATION"
STATE_SUBMITTED = "SUBMITTED"
STATE_COMPLETED = "COMPLETED"
STATE_COLLECTED = "COLLECTED"
STATE_COMMITTED = "COMMITTED"
STATE_SAFE_STOPPED = "SAFE_STOPPED"
TERMINAL_BATCH_STATUSES = {"completed", "failed", "expired", "cancelled"}

ARTIFACT_PATHS = {
    "all": DIRECT.OUTPUT_ALL,
    "confirmed": DIRECT.OUTPUT_CONFIRMED,
    "human_review": DIRECT.OUTPUT_HUMAN_REVIEW,
    "not_confirmed": DIRECT.OUTPUT_NOT_CONFIRMED,
    "error": DIRECT.OUTPUT_ERROR,
}
ARTIFACT_FILENAMES = {name: path.name for name, path in ARTIFACT_PATHS.items()}


class BatchEngineError(RuntimeError):
    pass


class CASConflict(BatchEngineError):
    pass


class SubmissionBlocked(BatchEngineError):
    pass


class PendingReconciliation(BatchEngineError):
    pass


class ReconciliationFailed(BatchEngineError):
    pass


class CollectorIntegrityError(BatchEngineError):
    pass


class PublishError(BatchEngineError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _validate_run_id(run_id: str) -> str:
    if not RUN_ID_RE.fullmatch(run_id or ""):
        raise ValueError("run_idは先頭英数字、1-24文字の英数字/_/-に限定します")
    return run_id


def _run_dir(run_id: str, runtime_root: Path = RUNTIME_ROOT) -> Path:
    _validate_run_id(run_id)
    root = runtime_root.resolve()
    result = (runtime_root / run_id).resolve()
    if result == root or root not in result.parents:
        raise ValueError("Batch runtime root外のpathです")
    return result


def _json_bytes(value: Any, pretty: bool = False) -> bytes:
    if pretty:
        text = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    else:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return text.encode("utf-8")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("wb") as target:
            target.write(payload)
            target.flush()
            os.fsync(target.fileno())
        os.replace(str(temporary), str(path))
    finally:
        if temporary.exists():
            temporary.unlink()


def _atomic_write_json(path: Path, value: Dict[str, Any]) -> None:
    _atomic_write_bytes(path, _json_bytes(value, pretty=True))


def _read_json_object(path: Path) -> Dict[str, Any]:
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError(f"JSON objectではありません: {path}")
    return parsed


class FileStateStore:
    """同一persistent filesystem上でETag CASを行うstate store。

    Issue 2はこのinterfaceと同じload/create/cas contractを持つpersistent adapterを
    接続できる。ここではAWSへの依存やwriteを持たない。
    """

    def __init__(self, run_dir: Path):
        self.run_dir = run_dir
        self.state_path = run_dir / "batch_state.json"
        self.lock_path = run_dir / ".batch_state.lock"
        self.claim_path = run_dir / "submit.claim"

    @staticmethod
    def _etag(payload: bytes) -> str:
        return hashlib.sha256(payload).hexdigest()

    def create(self, state: Dict[str, Any]) -> str:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        payload = _json_bytes(state, pretty=True)
        try:
            descriptor = os.open(
                str(self.state_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
            )
        except FileExistsError as error:
            raise CASConflict("batch stateは既に存在します") from error
        with os.fdopen(descriptor, "wb") as target:
            target.write(payload)
            target.flush()
            os.fsync(target.fileno())
        return self._etag(payload)

    def load(self) -> Tuple[Dict[str, Any], str]:
        payload = self.state_path.read_bytes()
        parsed = json.loads(payload.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("batch stateがJSON objectではありません")
        return parsed, self._etag(payload)

    def cas(self, expected_etag: str, state: Dict[str, Any]) -> str:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+b") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            current_payload = self.state_path.read_bytes()
            current_etag = self._etag(current_payload)
            if current_etag != expected_etag:
                raise CASConflict(
                    f"batch state ETag競合: expected={expected_etag} actual={current_etag}"
                )
            next_state = dict(state)
            next_state["state_revision"] = int(next_state.get("state_revision", 0)) + 1
            next_state["state_updated_at"] = utc_now()
            payload = _json_bytes(next_state, pretty=True)
            _atomic_write_bytes(self.state_path, payload)
            return self._etag(payload)

    def acquire_submit_claim(self, submission_nonce: str) -> None:
        claim = {
            "claimed_at": utc_now(),
            "submission_nonce": submission_nonce,
        }
        try:
            descriptor = os.open(
                str(self.claim_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
            )
        except FileExistsError as error:
            raise SubmissionBlocked(
                "persistent submit claim取得済み: 二重submitを拒否"
            ) from error
        with os.fdopen(descriptor, "wb") as target:
            target.write(_json_bytes(claim, pretty=True))
            target.flush()
            os.fsync(target.fileno())


def _custom_id(run_id: str, ordinal: int, record: Dict[str, Any]) -> str:
    project_id = str(record.get("project_info", {}).get("message_id", ""))
    resource_id = str(record.get("resource_info", {}).get("message_id", ""))
    identity = f"{project_id}|{resource_id}|{SHARED_CORE.sha256_value(record)}"
    suffix = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12]
    return f"b-{run_id}-{ordinal:06d}-{suffix}"


def _build_request_context(
    record: Dict[str, Any],
    score_band: str,
    source_ordinal: int,
    skillsheet_map: Dict[str, Dict[str, Any]],
    cleaned_email_map: Dict[str, str],
) -> Dict[str, Any]:
    required_skills = DIRECT._required_skills_from_record(record)
    project_id = str(record.get("project_info", {}).get("message_id", ""))
    resource_id = str(record.get("resource_info", {}).get("message_id", ""))
    skillsheet_record = skillsheet_map.get(resource_id)
    base = {
        "source_ordinal": source_ordinal,
        "score_band": score_band,
        "project_message_id": project_id,
        "resource_message_id": resource_id,
        "required_skills": required_skills,
        "source_record": record,
        "source_record_sha256": SHARED_CORE.sha256_value(record),
    }
    if (
        not skillsheet_record
        or not skillsheet_record.get("success", False)
        or not str(skillsheet_record.get("skillsheet") or "").strip()
    ):
        return {
            **base,
            "dispatch": "local_fallback",
            "fallback_reason": "スキルシート欠落のため人間確認",
            "skillsheet_text": "",
            "project_body_text": "",
        }
    normalized = str(skillsheet_record.get("skillsheet") or "").strip()
    skillsheet_text = DIRECT._truncate_skillsheet(
        build_skillsheet_ai_context(normalized)
    )
    project_body = cleaned_email_map.get(project_id, "")
    project_body_text = DIRECT._truncate_project_body(project_body) if project_body else ""
    response_schema = DIRECT._build_schema(required_skills)
    user_prompt = DIRECT._build_user_prompt(
        required_skills, skillsheet_text, project_body_text
    )
    body = SHARED_CORE.build_batch_request_body(
        DIRECT.SYSTEM_PROMPT,
        DIRECT.RECHECK_LLM_MODEL,
        response_schema,
        user_prompt,
    )
    return {
        **base,
        "dispatch": "batch",
        "skillsheet_text": skillsheet_text,
        "project_body_text": project_body_text,
        "response_schema": response_schema,
        "body": body,
    }


def load_production_contexts() -> List[Dict[str, Any]]:
    skillsheet_map = DIRECT._load_skillsheet_map()
    cleaned_email_map = DIRECT._load_cleaned_email_map()
    contexts: List[Dict[str, Any]] = []
    source_ordinal = 0
    for score_band, input_path in DIRECT.INPUT_SCORE_FILES:
        for record in read_jsonl(str(input_path)):
            if DIRECT._is_no_match_record(record):
                continue
            source_ordinal += 1
            contexts.append(
                _build_request_context(
                    record,
                    score_band,
                    source_ordinal,
                    skillsheet_map,
                    cleaned_email_map,
                )
            )
    return contexts


def _manifest_entry(
    run_id: str, ordinal: int, context: Dict[str, Any]
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    entry = {
        "ordinal": ordinal,
        "source_ordinal": context["source_ordinal"],
        "score_band": context["score_band"],
        "project_message_id": context["project_message_id"],
        "resource_message_id": context["resource_message_id"],
        "dispatch": context["dispatch"],
        "required_skills": context["required_skills"],
        "required_skill_count": len(context["required_skills"]),
        "skillsheet_text": context["skillsheet_text"],
        "skillsheet_chars_used": len(context["skillsheet_text"]),
        "project_body_text": context["project_body_text"],
        "source_record": context["source_record"],
        "source_record_sha256": context["source_record_sha256"],
    }
    if context["dispatch"] != "batch":
        entry["custom_id"] = None
        entry["fallback_reason"] = context["fallback_reason"]
        return entry, None
    custom_id = _custom_id(run_id, ordinal, context["source_record"])
    request = {
        "custom_id": custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": context["body"],
    }
    entry.update(
        {
            "custom_id": custom_id,
            "request_sha256": SHARED_CORE.sha256_value(request),
            "request_body_sha256": SHARED_CORE.sha256_value(context["body"]),
            "response_schema_sha256": SHARED_CORE.sha256_value(
                context["response_schema"]
            ),
        }
    )
    return entry, request


def validate_prepared(run_dir: Path) -> Dict[str, Any]:
    manifest = list(read_jsonl(str(run_dir / "manifest.jsonl")))
    input_records = list(read_jsonl(str(run_dir / "input.jsonl")))
    if not manifest:
        raise ValueError("manifestが空です")
    ordinals = [entry.get("ordinal") for entry in manifest]
    if ordinals != list(range(1, len(manifest) + 1)):
        raise ValueError("manifest ordinalが連番ではありません")
    input_by_id: Dict[str, Dict[str, Any]] = {}
    for request in input_records:
        custom_id = str(request.get("custom_id") or "")
        if not custom_id or custom_id in input_by_id:
            raise ValueError(f"input custom_idが空または重複: {custom_id!r}")
        input_by_id[custom_id] = request
    manifest_batch = [entry for entry in manifest if entry.get("dispatch") == "batch"]
    manifest_ids = [str(entry.get("custom_id") or "") for entry in manifest_batch]
    if len(set(manifest_ids)) != len(manifest_ids) or set(manifest_ids) != set(input_by_id):
        raise ValueError("input/manifest custom_id集合不一致")
    for entry in manifest:
        if SHARED_CORE.sha256_value(entry.get("source_record")) != entry.get(
            "source_record_sha256"
        ):
            raise ValueError(f"source record hash不一致 ordinal={entry.get('ordinal')}")
        required_skills = entry.get("required_skills")
        if not isinstance(required_skills, list) or len(required_skills) != int(
            entry.get("required_skill_count", -1)
        ):
            raise ValueError(f"required skill contract不正 ordinal={entry.get('ordinal')}")
        if entry.get("dispatch") != "batch":
            continue
        custom_id = str(entry["custom_id"])
        request = input_by_id[custom_id]
        if request.get("method") != "POST" or request.get("url") != "/v1/chat/completions":
            raise ValueError(f"Batch request envelope不正: {custom_id}")
        if SHARED_CORE.sha256_value(request) != entry.get("request_sha256"):
            raise ValueError(f"request hash不一致: {custom_id}")
        if SHARED_CORE.sha256_value(request.get("body")) != entry.get(
            "request_body_sha256"
        ):
            raise ValueError(f"request body hash不一致: {custom_id}")
        schema = DIRECT._build_schema(required_skills)
        if SHARED_CORE.sha256_value(schema) != entry.get("response_schema_sha256"):
            raise ValueError(f"response schema hash不一致: {custom_id}")
    manifest_sha256 = SHARED_CORE.sha256_value(manifest)
    state, state_etag = FileStateStore(run_dir).load()
    if state.get("manifest_sha256") != manifest_sha256:
        raise ValueError("state/manifest SHA-256不一致")
    return {
        "manifest_count": len(manifest),
        "request_count": len(input_records),
        "custom_id_unique": len(input_by_id) == len(input_records),
        "manifest_sha256": manifest_sha256,
        "state_etag": state_etag,
    }


def prepare_run(
    run_id: str,
    runtime_root: Path = RUNTIME_ROOT,
    contexts: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    run_dir = _run_dir(run_id, runtime_root)
    if run_dir.exists():
        raise FileExistsError(f"既存Batch runを上書きしません: {run_dir}")
    selected = list(contexts) if contexts is not None else load_production_contexts()
    if not selected:
        raise ValueError("Batch prepare対象が0件です")
    run_dir.mkdir(parents=True, exist_ok=False)
    manifest: List[Dict[str, Any]] = []
    requests_to_send: List[Dict[str, Any]] = []
    for ordinal, context in enumerate(selected, 1):
        entry, request = _manifest_entry(run_id, ordinal, context)
        manifest.append(entry)
        if request is not None:
            requests_to_send.append(request)
    if len({row["custom_id"] for row in requests_to_send}) != len(requests_to_send):
        raise ValueError("custom_id重複")
    write_jsonl(str(run_dir / "input.jsonl"), requests_to_send)
    write_jsonl(str(run_dir / "manifest.jsonl"), manifest)
    submission_nonce = uuid.uuid4().hex
    state = {
        "engine_version": ENGINE_VERSION,
        "run_id": run_id,
        "state_revision": 0,
        "state": STATE_PREPARED,
        "prepared_at": utc_now(),
        "state_updated_at": utc_now(),
        "submission_nonce": submission_nonce,
        "manifest_sha256": SHARED_CORE.sha256_value(manifest),
        "manifest_count": len(manifest),
        "request_count": len(requests_to_send),
        "input_file_id": None,
        "batch_id": None,
        "batch_status": None,
        "request_counts": None,
        "output_file_id": None,
        "error_file_id": None,
        "reconciliation_checks": 0,
        "safe_stop_reason": None,
        "production_commit_marker": None,
    }
    FileStateStore(run_dir).create(state)
    validation = validate_prepared(run_dir)
    return {"run_id": run_id, "run_dir": str(run_dir), **validation}


class OpenAIHttpBatchClient:
    """Issue 1 runtime用の薄いOpenAI HTTP adapter。"""

    @staticmethod
    def _headers() -> Dict[str, str]:
        from common.llm_client import _get_api_key

        return {"Authorization": f"Bearer {_get_api_key()}"}

    @staticmethod
    def _json(response: requests.Response, operation: str) -> Dict[str, Any]:
        try:
            response.raise_for_status()
        except requests.RequestException as error:
            raise BatchEngineError(
                f"OpenAI {operation}失敗: status={response.status_code}"
            ) from error
        parsed = response.json()
        if not isinstance(parsed, dict):
            raise BatchEngineError(f"OpenAI {operation}応答がJSON objectではありません")
        return parsed

    def upload_input(self, input_path: Path) -> str:
        with input_path.open("rb") as source:
            response = requests.post(
                API_BASE_URL + "/files",
                headers=self._headers(),
                data={"purpose": "batch"},
                files={"file": ("input.jsonl", source, "application/jsonl")},
                timeout=120,
            )
        uploaded = self._json(response, "file upload")
        file_id = str(uploaded.get("id") or "")
        if not file_id:
            raise BatchEngineError("file upload応答にidがありません")
        return file_id

    def create_batch(
        self, input_file_id: str, metadata: Dict[str, str]
    ) -> Dict[str, Any]:
        response = requests.post(
            API_BASE_URL + "/batches",
            headers={**self._headers(), "Content-Type": "application/json"},
            json={
                "input_file_id": input_file_id,
                "endpoint": "/v1/chat/completions",
                "completion_window": "24h",
                "metadata": metadata,
            },
            timeout=120,
        )
        return self._json(response, "batch create")

    def retrieve_batch(self, batch_id: str) -> Dict[str, Any]:
        response = requests.get(
            API_BASE_URL + f"/batches/{batch_id}",
            headers=self._headers(),
            timeout=60,
        )
        return self._json(response, "batch retrieve")

    def list_batches(self) -> List[Dict[str, Any]]:
        response = requests.get(
            API_BASE_URL + "/batches",
            headers=self._headers(),
            params={"limit": 100},
            timeout=60,
        )
        parsed = self._json(response, "batch list")
        data = parsed.get("data")
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            raise BatchEngineError("batch list応答のdataが不正です")
        return data

    def download_file(self, file_id: str) -> bytes:
        response = requests.get(
            API_BASE_URL + f"/files/{file_id}/content",
            headers=self._headers(),
            timeout=120,
        )
        try:
            response.raise_for_status()
        except requests.RequestException as error:
            raise BatchEngineError(
                f"OpenAI file content取得失敗: status={response.status_code}"
            ) from error
        return response.content


def _require_network(allow_network: bool) -> None:
    if not allow_network:
        raise PermissionError("network処理は--allow-network明示時だけ実行できます")


def _batch_metadata(state: Dict[str, Any]) -> Dict[str, str]:
    return {
        "run_id": str(state["run_id"]),
        "step": DIRECT.STEP_NAME,
        "submission_nonce": str(state["submission_nonce"]),
        "manifest_sha256": str(state["manifest_sha256"]),
    }


def _update_observed_batch(state: Dict[str, Any], batch: Dict[str, Any]) -> None:
    status = str(batch.get("status") or "")
    if status:
        state["batch_status"] = status
        state.setdefault("status_history", []).append(
            {"observed_at": utc_now(), "status": status}
        )
    for field in ("output_file_id", "error_file_id", "request_counts"):
        if batch.get(field) is not None:
            state[field] = batch[field]
    if status == "completed" and state.get("state") not in {
        STATE_COLLECTED,
        STATE_COMMITTED,
    }:
        state["state"] = STATE_COMPLETED


def _assert_new_submit_allowed(state: Dict[str, Any]) -> None:
    if state.get("batch_id"):
        raise SubmissionBlocked("既存batch_idがあるため新規submitせずresumeします")
    if state.get("state") == STATE_PENDING_RECONCILIATION:
        raise PendingReconciliation("create応答不明: reconciliation前の再submitを拒否")
    if state.get("state") != STATE_PREPARED:
        raise SubmissionBlocked(f"state={state.get('state')}: 新規submit不可")
    if state.get("input_file_id"):
        raise SubmissionBlocked("既存input_file_idがある曖昧stateの再submitを拒否")


def resume_run(
    run_id: str,
    client: Any,
    runtime_root: Path = RUNTIME_ROOT,
) -> Dict[str, Any]:
    run_dir = _run_dir(run_id, runtime_root)
    validation = validate_prepared(run_dir)
    store = FileStateStore(run_dir)
    state, etag = store.load()
    if state.get("state") == STATE_COMMITTED:
        marker = validate_commit_marker(
            run_id, str(validation["manifest_sha256"])
        )
        return {
            "run_id": run_id,
            "batch_id": state.get("batch_id"),
            "batch_status": state.get("batch_status"),
            "state": STATE_COMMITTED,
            "state_etag": etag,
            "production_commit": marker,
            "resumed": True,
        }
    batch_id = str(state.get("batch_id") or "")
    if not batch_id:
        raise SubmissionBlocked("resume対象batch_idがありません")
    observed = client.retrieve_batch(batch_id)
    if str(observed.get("id") or batch_id) != batch_id:
        raise BatchEngineError("retrieveしたbatch idがstateと一致しません")
    _update_observed_batch(state, observed)
    next_etag = store.cas(etag, state)
    return {
        "run_id": run_id,
        "batch_id": batch_id,
        "batch_status": state.get("batch_status"),
        "state": state.get("state"),
        "state_etag": next_etag,
        "resumed": True,
    }


def submit_run(
    run_id: str,
    client: Any,
    runtime_root: Path = RUNTIME_ROOT,
) -> Dict[str, Any]:
    run_dir = _run_dir(run_id, runtime_root)
    validation = validate_prepared(run_dir)
    store = FileStateStore(run_dir)
    state, etag = store.load()
    if state.get("batch_id"):
        return resume_run(run_id, client, runtime_root)
    _assert_new_submit_allowed(state)
    if state.get("manifest_sha256") != validation["manifest_sha256"]:
        raise SubmissionBlocked("submit直前manifest hash不一致")

    nonce = str(state.get("submission_nonce") or "")
    if not nonce:
        raise SubmissionBlocked("submission_nonceがありません")
    store.acquire_submit_claim(nonce)
    state, etag = store.load()
    _assert_new_submit_allowed(state)
    state["state"] = STATE_CLAIMED
    state["claim_acquired_at"] = utc_now()
    etag = store.cas(etag, state)

    input_file_id = client.upload_input(run_dir / "input.jsonl")
    if not input_file_id:
        raise BatchEngineError("input_file_idが空です")
    state, etag = store.load()
    if state.get("state") != STATE_CLAIMED:
        raise CASConflict("upload後のstateがCLAIMEDではありません")
    state["input_file_id"] = str(input_file_id)
    state["file_uploaded_at"] = utc_now()
    state["state"] = STATE_PENDING_RECONCILIATION
    state["batch_create_started_at"] = utc_now()
    etag = store.cas(etag, state)

    # create request送信前にPENDING_RECONCILIATIONを永続化する。timeoutやprocess
    # crash時も、このclaimから自動再submitする経路は存在しない。
    try:
        batch = client.create_batch(str(input_file_id), _batch_metadata(state))
    except Exception as error:
        raise PendingReconciliation(
            "Batch create応答不明。自動再submitせずreconciliationが必要です"
        ) from error
    batch_id = str(batch.get("id") or "")
    if not batch_id:
        raise PendingReconciliation(
            "Batch create応答にidがないため自動再submitせずreconciliationが必要です"
        )
    state, etag = store.load()
    if state.get("state") != STATE_PENDING_RECONCILIATION:
        raise CASConflict("create応答後のstateがPENDING_RECONCILIATIONではありません")
    state["batch_id"] = batch_id
    state["batch_submitted_at"] = utc_now()
    state["state"] = STATE_SUBMITTED
    _update_observed_batch(state, batch)
    next_etag = store.cas(etag, state)
    return {
        "run_id": run_id,
        "batch_id": batch_id,
        "batch_status": state.get("batch_status"),
        "state": state.get("state"),
        "state_etag": next_etag,
        "resumed": False,
    }


def _reconciliation_matches(
    batches: Iterable[Dict[str, Any]], state: Dict[str, Any]
) -> List[Dict[str, Any]]:
    expected = _batch_metadata(state)
    input_file_id = str(state.get("input_file_id") or "")
    matches: List[Dict[str, Any]] = []
    for batch in batches:
        metadata = batch.get("metadata")
        if not isinstance(metadata, dict):
            continue
        if not all(str(metadata.get(key) or "") == value for key, value in expected.items()):
            continue
        if input_file_id and str(batch.get("input_file_id") or "") != input_file_id:
            continue
        if str(batch.get("id") or ""):
            matches.append(batch)
    return matches


def reconcile_pending(
    run_id: str,
    client: Any,
    runtime_root: Path = RUNTIME_ROOT,
    max_checks: int = 3,
) -> Dict[str, Any]:
    if max_checks < 1 or max_checks > 10:
        raise ValueError("max_checksは1-10に限定します")
    run_dir = _run_dir(run_id, runtime_root)
    store = FileStateStore(run_dir)
    pending_state, pending_etag = store.load()
    if pending_state.get("batch_id"):
        return resume_run(run_id, client, runtime_root)
    if pending_state.get("state") != STATE_PENDING_RECONCILIATION:
        raise SubmissionBlocked("PENDING_RECONCILIATION stateではありません")
    pending_identity = tuple(
        pending_state.get(field)
        for field in (
            "run_id",
            "submission_nonce",
            "manifest_sha256",
            "input_file_id",
        )
    )

    matches_by_id: Dict[str, Dict[str, Any]] = {}
    checks_completed = 0
    for _ in range(max_checks):
        for match in _reconciliation_matches(client.list_batches(), pending_state):
            matches_by_id[str(match["id"])] = match
        checks_completed += 1
        if len(matches_by_id) >= 1:
            break

    # list結果を得た時点のpending ETagからだけ遷移する。競合時は現stateを
    # 再確認し、別workerが採用したbatch_id/stateを古い結果で上書きしない。
    state = pending_state
    etag = pending_etag
    for _ in range(3):
        if state.get("batch_id"):
            return resume_run(run_id, client, runtime_root)
        if state.get("state") != STATE_PENDING_RECONCILIATION:
            return {
                "run_id": run_id,
                "match_count": len(matches_by_id),
                "batch_id": state.get("batch_id"),
                "state": state.get("state"),
                "state_etag": etag,
                "reconciliation_skipped": True,
            }
        current_identity = tuple(
            state.get(field)
            for field in (
                "run_id",
                "submission_nonce",
                "manifest_sha256",
                "input_file_id",
            )
        )
        if current_identity != pending_identity:
            return {
                "run_id": run_id,
                "match_count": len(matches_by_id),
                "batch_id": state.get("batch_id"),
                "state": state.get("state"),
                "state_etag": etag,
                "reconciliation_skipped": True,
            }
        next_state = dict(state)
        next_state["reconciliation_checks"] = int(
            next_state.get("reconciliation_checks", 0)
        ) + checks_completed
        if len(matches_by_id) == 1:
            adopted = next(iter(matches_by_id.values()))
            next_state["batch_id"] = str(adopted["id"])
            next_state["state"] = STATE_SUBMITTED
            next_state["reconciled_at"] = utc_now()
            _update_observed_batch(next_state, adopted)
        else:
            next_state["state"] = STATE_SAFE_STOPPED
            next_state["safe_stop_reason"] = (
                "reconciliation_no_match_after_bounded_checks"
                if not matches_by_id
                else "reconciliation_duplicate_batches"
            )
        try:
            next_etag = store.cas(etag, next_state)
            state = next_state
            break
        except CASConflict:
            state, etag = store.load()
    else:
        raise CASConflict("reconciliation pending transitionのCAS retry上限超過")

    if len(matches_by_id) == 1:
        return {
            "run_id": run_id,
            "match_count": 1,
            "batch_id": state["batch_id"],
            "state": state["state"],
            "state_etag": next_etag,
        }
    if not matches_by_id:
        raise ReconciliationFailed(
            f"bounded reconciliation {checks_completed}回で一致0件: 安全停止"
        )
    raise ReconciliationFailed(
        f"reconciliation一致{len(matches_by_id)}件: duplicateとして安全停止"
    )


def _read_jsonl_bytes_strict(payload: bytes, label: str) -> List[Dict[str, Any]]:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise CollectorIntegrityError(f"{label}: UTF-8 decode error") from error
    if not text:
        return []
    records: List[Dict[str, Any]] = []
    for line_number, raw in enumerate(text.splitlines(), 1):
        if not raw.strip():
            raise CollectorIntegrityError(f"{label}:{line_number}: 空行")
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as error:
            raise CollectorIntegrityError(
                f"{label}:{line_number}: malformed JSONL: {error}"
            ) from error
        if not isinstance(parsed, dict):
            raise CollectorIntegrityError(f"{label}:{line_number}: JSON objectではない")
        records.append(parsed)
    return records


def _parse_success_response(
    record: Dict[str, Any]
) -> Tuple[Optional[Dict[str, Any]], str]:
    response = record.get("response")
    if not isinstance(response, dict):
        return None, "response object欠落"
    status_code = response.get("status_code")
    if not isinstance(status_code, int) or not 200 <= status_code < 300:
        return None, f"HTTP status不正: {status_code!r}"
    body = response.get("body")
    if not isinstance(body, dict):
        return None, "response.body欠落"
    try:
        choice = body["choices"][0]
        content = choice["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return None, "choices[0].message.content欠落"
    if choice.get("finish_reason") == "length":
        return None, "Batch output truncated: finish_reason=length"
    if not isinstance(content, str):
        return None, "response contentが文字列でない"
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as error:
        return None, f"response content parse error: {error}"
    if not isinstance(parsed, dict):
        return None, "response contentがJSON objectでない"
    return parsed, ""


def _validate_response(
    parsed: Dict[str, Any], required_skills: List[Dict[str, Any]]
) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    expected_top = {"required_skill_checks", "category_match", "category_note"}
    if set(parsed) != expected_top:
        return None, (
            "top-level schema keys不一致: "
            f"expected={sorted(expected_top)} actual={sorted(parsed)}"
        )
    raw_checks = parsed.get("required_skill_checks")
    if isinstance(raw_checks, list):
        expected_check = {"skill", "confidence", "reason", "evidence"}
        for index, check in enumerate(raw_checks):
            if not isinstance(check, dict) or set(check) != expected_check:
                return None, f"required_skill_checks[{index}] schema keys不一致"
    checks, validation_error = DIRECT._validate_required_skill_checks(
        required_skills, raw_checks
    )
    if validation_error:
        return None, validation_error
    if parsed.get("category_match") not in DIRECT.VALID_CATEGORY_MATCHES:
        return None, "category_match不正"
    category_note = parsed.get("category_note")
    if not isinstance(category_note, str) or not category_note.strip():
        return None, "category_noteが空または文字列でない"
    return checks, ""


def _fallback_result(
    entry: Dict[str, Any], reason: str, error_type: str, error_message: str,
    category_match: str = "unclear", category_note: str = "判定不明",
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    required_skills = entry["required_skills"]
    checks = DIRECT._fallback_checks(required_skills, reason)
    result = DIRECT._add_recheck_result(
        entry["source_record"],
        entry["score_band"],
        checks,
        int(entry.get("skillsheet_chars_used", 0)),
        category_match,
        category_note,
    )
    error = DIRECT._make_error(
        entry["source_record"], entry["score_band"], error_type, error_message
    )
    return result, error


def _apply_guard_to_result(
    result: Dict[str, Any], entry: Dict[str, Any], schema_valid: bool
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    checks = result.get("required_skill_checks")
    if not isinstance(checks, list):
        raise CollectorIntegrityError("production record builderのchecks contract不正")
    guarded_checks, guarded_category, metadata = SAFETY_GUARD.apply_minimal_safety_guard(
        checks,
        str(result.get("category_match") or "unclear"),
        schema_valid,
        str(entry.get("skillsheet_text") or ""),
    )
    result["required_skill_checks"] = guarded_checks
    result["category_match"] = guarded_category
    SHARED_CORE.refresh_result_counts(
        result,
        DIRECT.STATUS_CONFIRMED,
        DIRECT.STATUS_HUMAN_REVIEW,
        DIRECT.STATUS_NOT_CONFIRMED,
    )
    return result, metadata


def _short_status(record: Dict[str, Any]) -> str:
    status = record.get("recheck_info", {}).get("recheck_status")
    return {
        DIRECT.STATUS_CONFIRMED: "confirmed",
        DIRECT.STATUS_HUMAN_REVIEW: "human_review",
        DIRECT.STATUS_NOT_CONFIRMED: "not_confirmed",
    }.get(status, "invalid")


def collect_records(
    manifest: Sequence[Dict[str, Any]],
    output_payload: bytes,
    error_payload: bytes,
) -> Dict[str, Any]:
    """download済みpayloadをproduction recordへ変換する純粋collector。"""
    output_records = _read_jsonl_bytes_strict(output_payload, "output")
    error_records = _read_jsonl_bytes_strict(error_payload, "error")
    batch_entries = [entry for entry in manifest if entry.get("dispatch") == "batch"]
    expected_by_id = {str(entry.get("custom_id") or ""): entry for entry in batch_entries}
    if "" in expected_by_id or len(expected_by_id) != len(batch_entries):
        raise CollectorIntegrityError("manifest custom_idが空または重複")

    seen: Counter = Counter()
    response_by_id: Dict[str, Tuple[str, Dict[str, Any]]] = {}
    for source, records in (("output", output_records), ("error", error_records)):
        for record in records:
            custom_id = str(record.get("custom_id") or "")
            seen[custom_id] += 1
            response_by_id.setdefault(custom_id, (source, record))
    duplicates = sorted(custom_id for custom_id, count in seen.items() if count > 1)
    unknown = sorted(custom_id for custom_id in seen if custom_id not in expected_by_id)
    missing = sorted(custom_id for custom_id in expected_by_id if custom_id not in seen)
    if duplicates or unknown or missing:
        raise CollectorIntegrityError(
            "custom_id union不整合: "
            f"duplicate={duplicates[:3]} missing={missing[:3]} unknown={unknown[:3]}"
        )

    results: List[Dict[str, Any]] = []
    production_errors: List[Dict[str, Any]] = []
    audit_pairs: List[Dict[str, Any]] = []
    for entry in sorted(manifest, key=lambda item: int(item["ordinal"])):
        schema_valid = False
        schema_error = ""
        request_source = "local_fallback"
        if entry.get("dispatch") != "batch":
            result, error_record = _fallback_result(
                entry,
                str(entry.get("fallback_reason") or "スキルシート欠落のため人間確認"),
                "missing_resource_skillsheet",
                "Batch dispatch前のlocal fallback",
            )
            production_errors.append(error_record)
        else:
            custom_id = str(entry["custom_id"])
            request_source, response_record = response_by_id[custom_id]
            if request_source == "error" or response_record.get("error"):
                result, error_record = _fallback_result(
                    entry,
                    "Batch per-request errorのため人間確認",
                    "batch_request_error",
                    json.dumps(response_record.get("error") or response_record, ensure_ascii=False),
                )
                production_errors.append(error_record)
            else:
                parsed, parse_error = _parse_success_response(response_record)
                if parsed is None:
                    truncated = "finish_reason=length" in parse_error
                    result, error_record = _fallback_result(
                        entry,
                        (
                            "Batch出力truncationのため人間確認"
                            if truncated
                            else "Batch response parse errorのため人間確認"
                        ),
                        (
                            "batch_output_truncated"
                            if truncated
                            else "batch_response_parse_error"
                        ),
                        parse_error,
                    )
                    schema_error = parse_error
                    production_errors.append(error_record)
                else:
                    normalized_checks, validation_error = _validate_response(
                        parsed, entry["required_skills"]
                    )
                    category_match, category_note = DIRECT._extract_category_fields(parsed)
                    if validation_error or normalized_checks is None:
                        result, error_record = _fallback_result(
                            entry,
                            "Batch出力検証エラーのため人間確認",
                            "invalid_output_schema",
                            validation_error,
                            category_match,
                            category_note,
                        )
                        schema_error = validation_error
                        production_errors.append(error_record)
                    else:
                        schema_valid = True
                        result = DIRECT._add_recheck_result(
                            entry["source_record"],
                            entry["score_band"],
                            normalized_checks,
                            int(entry.get("skillsheet_chars_used", 0)),
                            category_match,
                            category_note,
                            apply_auto_true_override=True,
                        )

        before_record = json.loads(json.dumps(result, ensure_ascii=False))
        result, guard_metadata = _apply_guard_to_result(result, entry, schema_valid)
        results.append(result)
        audit_pairs.append(
            {
                "ordinal": int(entry["ordinal"]),
                "custom_id": entry.get("custom_id"),
                "project_message_id": entry["project_message_id"],
                "resource_message_id": entry["resource_message_id"],
                "schema_valid": schema_valid,
                "schema_error": schema_error,
                "request_source": request_source,
                "before": {
                    "status": _short_status(before_record),
                    "category_match": before_record.get("category_match"),
                    "required_skill_checks": before_record.get("required_skill_checks"),
                },
                "after": {
                    "status": _short_status(result),
                    "category_match": result.get("category_match"),
                    "required_skill_checks": result.get("required_skill_checks"),
                },
                "guard": guard_metadata,
            }
        )

    if [pair["ordinal"] for pair in audit_pairs] != list(range(1, len(manifest) + 1)):
        raise CollectorIntegrityError("ordinal順復元に失敗")
    return {
        "records": results,
        "errors": production_errors,
        "audit_pairs": audit_pairs,
        "output_count": len(output_records),
        "error_response_count": len(error_records),
        "schema_invalid_count": sum(
            pair["request_source"] == "output" and not pair["schema_valid"]
            for pair in audit_pairs
        ),
        "per_request_fallback_count": sum(
            pair["request_source"] == "error" for pair in audit_pairs
        ),
    }


def _partition_records(records: Sequence[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    partitions: Dict[str, List[Dict[str, Any]]] = {
        "confirmed": [],
        "human_review": [],
        "not_confirmed": [],
    }
    for record in records:
        status = _short_status(record)
        if status not in partitions:
            raise CollectorIntegrityError(f"recheck_status不正: {status}")
        partitions[status].append(record)
    return partitions


def write_stage(
    stage_dir: Path,
    records: Sequence[Dict[str, Any]],
    errors: Sequence[Dict[str, Any]],
) -> Dict[str, Path]:
    stage_dir.mkdir(parents=True, exist_ok=True)
    partitions = _partition_records(records)
    staged = {
        "all": stage_dir / ARTIFACT_FILENAMES["all"],
        "confirmed": stage_dir / ARTIFACT_FILENAMES["confirmed"],
        "human_review": stage_dir / ARTIFACT_FILENAMES["human_review"],
        "not_confirmed": stage_dir / ARTIFACT_FILENAMES["not_confirmed"],
        "error": stage_dir / ARTIFACT_FILENAMES["error"],
    }
    write_jsonl(str(staged["all"]), list(records))
    write_jsonl(str(staged["confirmed"]), partitions["confirmed"])
    write_jsonl(str(staged["human_review"]), partitions["human_review"])
    write_jsonl(str(staged["not_confirmed"]), partitions["not_confirmed"])
    write_jsonl(str(staged["error"]), list(errors))
    confirm_stage(staged)
    return staged


def _read_path_strict(path: Path) -> List[Dict[str, Any]]:
    return _read_jsonl_bytes_strict(path.read_bytes(), path.name)


def confirm_stage(staged: Dict[str, Path]) -> Dict[str, int]:
    if set(staged) != set(ARTIFACT_PATHS):
        raise CollectorIntegrityError("stage artifact集合不一致")
    rows = {name: _read_path_strict(path) for name, path in staged.items()}
    expected = _partition_records(rows["all"])
    for name in ("confirmed", "human_review", "not_confirmed"):
        if rows[name] != expected[name]:
            raise CollectorIntegrityError(f"stage分類内容不一致: {name}")
    for index, record in enumerate(rows["all"], 1):
        expected_count = len(record.get("project_info", {}).get("required_skills") or [])
        checks = record.get("required_skill_checks")
        if not isinstance(checks, list) or len(checks) != expected_count:
            raise CollectorIntegrityError(
                f"stage required_skill_checks件数不一致: ordinal={index}"
            )
    return {name: len(value) for name, value in rows.items()}


def _restore_bytes(path: Path, previous: Optional[bytes]) -> None:
    if previous is None:
        if path.exists():
            path.unlink()
    else:
        _atomic_write_bytes(path, previous)


def validate_commit_marker(
    expected_run_id: str,
    expected_manifest_sha256: str,
    marker_path: Path = COMMIT_MARKER,
    artifact_paths: Dict[str, Path] = ARTIFACT_PATHS,
) -> Dict[str, Any]:
    if not expected_run_id or not expected_manifest_sha256:
        raise PublishError("commit marker検証にはexpected run_id/manifest_sha256が必須です")
    marker = _read_json_object(marker_path)
    if marker.get("engine_version") != ENGINE_VERSION:
        raise PublishError("production commit marker engine_version不一致")
    if marker.get("run_id") != expected_run_id:
        raise PublishError("production commit marker run_id不一致")
    if marker.get("manifest_sha256") != expected_manifest_sha256:
        raise PublishError("production commit marker manifest_sha256不一致")
    artifacts = marker.get("artifacts")
    if not isinstance(artifacts, dict) or set(artifacts) != set(artifact_paths):
        raise PublishError("production commit marker artifact集合不一致")
    for name, path in artifact_paths.items():
        metadata = artifacts.get(name)
        if not isinstance(metadata, dict) or metadata.get("filename") != path.name:
            raise PublishError(f"commit marker artifact metadata不正: {name}")
        if not path.exists() or _file_sha256(path) != metadata.get("sha256"):
            raise PublishError(f"commit marker hash不一致: {name}")
    return marker


def transactional_publish(
    staged: Dict[str, Path],
    run_id: str,
    manifest_sha256: str,
    state_etag: str,
    artifact_paths: Optional[Dict[str, Path]] = None,
    marker_path: Optional[Path] = None,
    fail_after_artifacts: Optional[int] = None,
) -> Dict[str, Any]:
    """全artifactをrollback可能にreplaceし、markerを最後に生成する。"""
    targets = dict(artifact_paths or ARTIFACT_PATHS)
    marker_target = marker_path or COMMIT_MARKER
    if set(staged) != set(targets):
        raise PublishError("stage/production artifact集合不一致")
    counts = confirm_stage(staged)
    for path in staged.values():
        if not path.exists():
            raise PublishError(f"stage artifact欠落: {path}")
    previous = {
        name: path.read_bytes() if path.exists() else None for name, path in targets.items()
    }
    previous_marker = marker_target.read_bytes() if marker_target.exists() else None
    replaced = 0
    marker_written = False
    try:
        # 古いmarkerが新artifactをcommit済みと誤認させないよう最初に外す。
        if marker_target.exists():
            marker_target.unlink()
        for name in ("all", "confirmed", "human_review", "not_confirmed", "error"):
            target = targets[name]
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(f".{target.name}.{run_id}.publish.tmp")
            shutil.copyfile(staged[name], temporary)
            with temporary.open("rb") as source:
                os.fsync(source.fileno())
            os.replace(str(temporary), str(target))
            replaced += 1
            if fail_after_artifacts is not None and replaced >= fail_after_artifacts:
                raise PublishError("injected transactional publish failure")

        artifact_metadata = {
            name: {
                "filename": path.name,
                "sha256": _file_sha256(path),
                "bytes": path.stat().st_size,
                "records": counts[name],
            }
            for name, path in targets.items()
        }
        marker = {
            "engine_version": ENGINE_VERSION,
            "run_id": run_id,
            "manifest_sha256": manifest_sha256,
            "state_etag_at_publish": state_etag,
            "committed_at": utc_now(),
            "artifacts": artifact_metadata,
        }
        _atomic_write_json(marker_target, marker)
        marker_written = True
        validate_commit_marker(run_id, manifest_sha256, marker_target, targets)
        return marker
    except Exception as error:
        rollback_errors: List[str] = []
        for name, path in targets.items():
            try:
                _restore_bytes(path, previous[name])
            except Exception as rollback_error:
                rollback_errors.append(f"{name}:{rollback_error}")
        try:
            _restore_bytes(marker_target, previous_marker)
        except Exception as rollback_error:
            rollback_errors.append(f"marker:{rollback_error}")
        suffix = f" rollback_errors={rollback_errors}" if rollback_errors else ""
        raise PublishError(f"transactional publish失敗（rollback実施）: {error}{suffix}") from error
    finally:
        if not marker_written:
            for path in targets.values():
                temporary = path.with_name(f".{path.name}.{run_id}.publish.tmp")
                if temporary.exists():
                    temporary.unlink()


def _verify_completed_counts(state: Dict[str, Any], batch: Dict[str, Any]) -> None:
    if str(batch.get("status") or "") != "completed":
        raise CollectorIntegrityError(
            f"collector開始時Batchがcompletedではありません: {batch.get('status')!r}"
        )
    counts = batch.get("request_counts")
    if not isinstance(counts, dict):
        raise CollectorIntegrityError("Batch request_countsがありません")
    expected = int(state.get("request_count", -1))
    total = int(counts.get("total", -1))
    completed = int(counts.get("completed", 0))
    failed = int(counts.get("failed", 0))
    if total != expected or completed + failed != expected:
        raise CollectorIntegrityError(
            "Batch request count不一致: "
            f"expected={expected} total={total} completed={completed} failed={failed}"
        )


def _verify_result_file_contract(
    batch: Dict[str, Any],
    output_file_id: str,
    error_file_id: str,
    output_payload: bytes,
    error_payload: bytes,
) -> None:
    counts = batch.get("request_counts")
    if not isinstance(counts, dict):
        raise CollectorIntegrityError("Batch request_countsがありません")
    completed = int(counts.get("completed", 0))
    failed = int(counts.get("failed", 0))
    if completed > 0 and not output_file_id:
        raise CollectorIntegrityError("completed>0ですがoutput_file_idがありません")
    if failed > 0 and not error_file_id:
        raise CollectorIntegrityError("failed>0ですがerror_file_idがありません")
    output_count = len(_read_jsonl_bytes_strict(output_payload, "output"))
    error_count = len(_read_jsonl_bytes_strict(error_payload, "error"))
    if output_count != completed or error_count != failed:
        raise CollectorIntegrityError(
            "Batch request_counts/result file件数不一致: "
            f"completed={completed} output={output_count} "
            f"failed={failed} error={error_count}"
        )


def collect_run(
    run_id: str,
    client: Any,
    runtime_root: Path = RUNTIME_ROOT,
    publish: bool = False,
) -> Dict[str, Any]:
    run_dir = _run_dir(run_id, runtime_root)
    validation = validate_prepared(run_dir)
    store = FileStateStore(run_dir)
    state, etag = store.load()
    if state.get("state") == STATE_COMMITTED:
        marker = validate_commit_marker(
            run_id, str(validation["manifest_sha256"])
        )
        return {
            "run_id": run_id,
            "state": STATE_COMMITTED,
            "state_etag": etag,
            "record_count": int(marker["artifacts"]["all"]["records"]),
            "production_error_count": int(marker["artifacts"]["error"]["records"]),
            "published": True,
            "production_commit": marker,
            "collector_retry": True,
            "audit_pairs": [],
        }
    batch_id = str(state.get("batch_id") or "")
    if not batch_id:
        raise CollectorIntegrityError("collector対象batch_idがありません")

    # completedの保存済みstateを信用せず、collector開始時に必ず再確認する。
    batch = client.retrieve_batch(batch_id)
    if str(batch.get("id") or batch_id) != batch_id:
        raise CollectorIntegrityError("collector retrieve batch_id不一致")
    _verify_completed_counts(state, batch)
    _update_observed_batch(state, batch)
    etag = store.cas(etag, state)

    output_file_id = str(batch.get("output_file_id") or "")
    error_file_id = str(batch.get("error_file_id") or "")
    counts = batch.get("request_counts")
    if not isinstance(counts, dict):
        raise CollectorIntegrityError("Batch request_countsがありません")
    if int(counts.get("completed", 0)) > 0 and not output_file_id:
        raise CollectorIntegrityError("completed>0ですがoutput_file_idがありません")
    if int(counts.get("failed", 0)) > 0 and not error_file_id:
        raise CollectorIntegrityError("failed>0ですがerror_file_idがありません")
    output_payload = client.download_file(output_file_id) if output_file_id else b""
    error_payload = client.download_file(error_file_id) if error_file_id else b""
    _verify_result_file_contract(
        batch, output_file_id, error_file_id, output_payload, error_payload
    )
    manifest = list(read_jsonl(str(run_dir / "manifest.jsonl")))
    collected = collect_records(manifest, output_payload, error_payload)

    stage_dir = run_dir / "stage"
    staged = write_stage(stage_dir, collected["records"], collected["errors"])
    state, etag = store.load()
    state["state"] = STATE_COLLECTED
    state["collected_at"] = utc_now()
    state["stage_hashes"] = {name: _file_sha256(path) for name, path in staged.items()}
    etag = store.cas(etag, state)

    marker = None
    if publish:
        marker = transactional_publish(
            staged,
            run_id,
            str(validation["manifest_sha256"]),
            etag,
        )
        state, etag = store.load()
        state["state"] = STATE_COMMITTED
        state["production_commit_marker"] = str(COMMIT_MARKER)
        state["committed_at"] = marker["committed_at"]
        etag = store.cas(etag, state)
    return {
        "run_id": run_id,
        "state": STATE_COMMITTED if publish else STATE_COLLECTED,
        "state_etag": etag,
        "record_count": len(collected["records"]),
        "production_error_count": len(collected["errors"]),
        "schema_invalid_count": collected["schema_invalid_count"],
        "per_request_fallback_count": collected["per_request_fallback_count"],
        "stage_dir": str(stage_dir),
        "published": publish,
        "production_commit": marker,
        "audit_pairs": collected["audit_pairs"],
    }


def _extract_skillsheet_from_saved_request(request: Dict[str, Any]) -> str:
    body = request.get("body")
    messages = body.get("messages") if isinstance(body, dict) else None
    if not isinstance(messages, list):
        raise CollectorIntegrityError("保存済みinput messages不正")
    users = [
        message.get("content")
        for message in messages
        if isinstance(message, dict) and message.get("role") == "user"
    ]
    if len(users) != 1 or not isinstance(users[0], str):
        raise CollectorIntegrityError("保存済みinput user prompt不正")
    marker = "【要員スキルシート本文】"
    if marker not in users[0]:
        raise CollectorIntegrityError("保存済みinput skillsheet marker欠落")
    skillsheet = users[0].split(marker, 1)[1]
    trailer = "\n\n上記の案件本文とスキルシートを根拠に"
    if trailer in skillsheet:
        skillsheet = skillsheet.split(trailer, 1)[0]
    return skillsheet.strip()


def _source_record_map() -> Dict[Tuple[str, str], Tuple[str, int, Dict[str, Any]]]:
    result: Dict[Tuple[str, str], Tuple[str, int, Dict[str, Any]]] = {}
    source_ordinal = 0
    for score_band, path in DIRECT.INPUT_SCORE_FILES:
        for record in read_jsonl(str(path)):
            if DIRECT._is_no_match_record(record):
                continue
            source_ordinal += 1
            key = (
                str(record.get("project_info", {}).get("message_id", "")),
                str(record.get("resource_info", {}).get("message_id", "")),
            )
            if key in result:
                raise CollectorIntegrityError(f"production source pair重複: {key}")
            result[key] = (score_band, source_ordinal, record)
    return result


def _direct_candidate_map() -> Dict[Tuple[str, str], bool]:
    result: Dict[Tuple[str, str], bool] = {}
    for record in read_jsonl(str(DIRECT.OUTPUT_ALL)):
        key = (
            str(record.get("project_info", {}).get("message_id", "")),
            str(record.get("resource_info", {}).get("message_id", "")),
        )
        result[key] = (
            record.get("recheck_info", {}).get("recheck_status")
            != DIRECT.STATUS_NOT_CONFIRMED
            and record.get("category_match") != "mismatch"
        )
    return result


def _aggregate_audit(
    pairs: Sequence[Dict[str, Any]], side: str,
    direct_candidates: Dict[Tuple[str, str], bool],
) -> Dict[str, int]:
    statuses = Counter(pair[side]["status"] for pair in pairs)
    def candidate(pair: Dict[str, Any]) -> bool:
        return (
            pair[side]["status"] != "not_confirmed"
            and pair[side]["category_match"] != "mismatch"
        )
    return {
        "confirmed": statuses["confirmed"],
        "human_review": statuses["human_review"],
        "not_confirmed": statuses["not_confirmed"],
        "category_mismatch": sum(
            pair[side]["category_match"] == "mismatch" for pair in pairs
        ),
        "candidate_set": sum(candidate(pair) for pair in pairs),
        "candidate_loss": sum(
            direct_candidates.get(
                (pair["project_message_id"], pair["resource_message_id"]), False
            )
            and not candidate(pair)
            for pair in pairs
        ),
    }


def offline_replay_saved_canary(canary_run_dir: Path) -> Dict[str, Any]:
    """保存済みcanary artifactだけをproduction collectorへ通す（write/APIなし）。"""
    saved_manifest = list(read_jsonl(str(canary_run_dir / "manifest.jsonl")))
    saved_inputs = list(read_jsonl(str(canary_run_dir / "input.jsonl")))
    if len(saved_manifest) != len(saved_inputs):
        raise CollectorIntegrityError("保存済みmanifest/input件数不一致")
    input_by_id = {str(row.get("custom_id") or ""): row for row in saved_inputs}
    if len(input_by_id) != len(saved_inputs) or "" in input_by_id:
        raise CollectorIntegrityError("保存済みinput custom_id重複/空")
    sources = _source_record_map()
    production_manifest: List[Dict[str, Any]] = []
    for saved in sorted(saved_manifest, key=lambda item: int(item["ordinal"])):
        custom_id = str(saved.get("custom_id") or "")
        request = input_by_id.get(custom_id)
        if request is None:
            raise CollectorIntegrityError(f"保存済みinput接続不整合: {custom_id}")
        if SHARED_CORE.sha256_value(request) != saved.get("request_sha256"):
            raise CollectorIntegrityError(f"保存済みrequest hash不一致: {custom_id}")
        key = (
            str(saved.get("project_message_id") or ""),
            str(saved.get("resource_message_id") or ""),
        )
        source = sources.get(key)
        if source is None:
            raise CollectorIntegrityError(f"production source record欠落: {key}")
        score_band, source_ordinal, source_record = source
        if SHARED_CORE.sha256_value(source_record) != saved.get("source_record_sha256"):
            raise CollectorIntegrityError(f"production source hash不一致: {custom_id}")
        required_skills = DIRECT._required_skills_from_record(source_record)
        skillsheet = _extract_skillsheet_from_saved_request(request)
        production_manifest.append(
            {
                "ordinal": int(saved["ordinal"]),
                "source_ordinal": source_ordinal,
                "custom_id": custom_id,
                "dispatch": "batch",
                "score_band": score_band,
                "project_message_id": key[0],
                "resource_message_id": key[1],
                "required_skills": required_skills,
                "required_skill_count": len(required_skills),
                "skillsheet_text": skillsheet,
                "skillsheet_chars_used": len(skillsheet),
                "source_record": source_record,
                "source_record_sha256": saved["source_record_sha256"],
            }
        )
    output_payload = (canary_run_dir / "output_raw.jsonl").read_bytes()
    error_path = canary_run_dir / "error_raw.jsonl"
    error_payload = error_path.read_bytes() if error_path.exists() else b""
    collected = collect_records(production_manifest, output_payload, error_payload)
    direct_candidates = _direct_candidate_map()
    before = _aggregate_audit(collected["audit_pairs"], "before", direct_candidates)
    after = _aggregate_audit(collected["audit_pairs"], "after", direct_candidates)
    return {
        **collected,
        "sample_count": len(production_manifest),
        "before": before,
        "after": after,
        "new_human_review_rescue": sum(
            pair["before"]["status"] == "not_confirmed"
            and pair["after"]["status"] == "human_review"
            for pair in collected["audit_pairs"]
        ),
        "confirmed_to_human_review": sum(
            pair["before"]["status"] == "confirmed"
            and pair["after"]["status"] == "human_review"
            for pair in collected["audit_pairs"]
        ),
        "production_write": 0,
        "new_batch_submit": 0,
        "new_llm_call": 0,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="08-5 production Batch engine")
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--run-id", required=True)
    for command in ("submit", "resume", "reconcile", "collect"):
        child = subparsers.add_parser(command)
        child.add_argument("--run-id", required=True)
        child.add_argument("--allow-network", action="store_true")
        if command == "reconcile":
            child.add_argument("--max-checks", type=int, default=3)
        if command == "collect":
            child.add_argument("--publish", action="store_true")
    replay = subparsers.add_parser("offline-replay")
    replay.add_argument("--canary-run-dir", type=Path, required=True)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    try:
        if args.command == "prepare":
            result = prepare_run(args.run_id)
        elif args.command == "offline-replay":
            replay = offline_replay_saved_canary(args.canary_run_dir)
            result = {
                key: replay[key]
                for key in (
                    "sample_count",
                    "before",
                    "after",
                    "new_human_review_rescue",
                    "confirmed_to_human_review",
                    "production_write",
                    "new_batch_submit",
                    "new_llm_call",
                )
            }
        else:
            _require_network(args.allow_network)
            client = OpenAIHttpBatchClient()
            if args.command == "submit":
                result = submit_run(args.run_id, client)
            elif args.command == "resume":
                result = resume_run(args.run_id, client)
            elif args.command == "reconcile":
                result = reconcile_pending(
                    args.run_id, client, max_checks=args.max_checks
                )
            elif args.command == "collect":
                result = collect_run(
                    args.run_id, client, publish=args.publish
                )
            else:
                raise ValueError(f"unknown command: {args.command}")
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    except Exception as error:
        print(f"ERROR: {type(error).__name__}: {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

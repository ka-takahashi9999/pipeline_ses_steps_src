"""08-5 OpenAI Batch API production-noninterference canary tool.

prepare/report are offline. submit/status/fetch require --allow-network and are
never called by the test suite. Every artifact is constrained below
08-5_high_score_required_skill_recheck/_test_batch_api_canary/<run_id>/.
"""

import argparse
import hashlib
import importlib.util
import json
import math
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STEP_DIR = Path(__file__).resolve().parents[1]
CANARY_ROOT = STEP_DIR / "_test_batch_api_canary"
PRODUCTION_RESULT_DIR = STEP_DIR / "01_result"
CURRENT_TOOL = Path(__file__).resolve()
PRODUCTION_TOOL = CURRENT_TOOL.with_name("high_score_required_skill_recheck.py")
MAX_SAMPLE_SIZE = 678
SUBMIT_CLAIM_FILENAME = "submit.claim"
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,23}$")
API_BASE_URL = "https://api.openai.com/v1"
TERMINAL_STATUSES = {"completed", "failed", "expired", "cancelled"}
TERMINAL_TIME_FIELDS = {
    "completed": "completed_at",
    "failed": "failed_at",
    "expired": "expired_at",
    "cancelled": "cancelled_at",
}
BATCH_TIME_FIELDS = (
    "created_at",
    "in_progress_at",
    "finalizing_at",
    "completed_at",
    "failed_at",
    "expired_at",
    "cancelling_at",
    "cancelled_at",
    "expires_at",
)
SECRET_PATTERNS = (
    re.compile(r"Bearer\s+[A-Za-z0-9._-]+", re.IGNORECASE),
    re.compile(r"sk-[A-Za-z0-9_-]{12,}"),
    re.compile(r'"Authorization"\s*:', re.IGNORECASE),
    re.compile(r'"api_key"\s*:', re.IGNORECASE),
)

sys.path.insert(0, str(PROJECT_ROOT))
from common.json_utils import read_jsonl, write_jsonl  # noqa: E402
from common.skillsheet_ai_context import build_skillsheet_ai_context  # noqa: E402


def _load_production_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "high_score_required_skill_recheck_canary_readonly", PRODUCTION_TOOL
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("08-5 production moduleをread-only importできません")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PRODUCTION = _load_production_module()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _write_json(
    path: Path, value: Dict[str, Any], root: Path = CANARY_ROOT
) -> None:
    _assert_canary_path(path, root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except Exception:
        if temporary.exists():
            temporary.unlink()
        raise


def _read_json(path: Path) -> Dict[str, Any]:
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError(f"JSON objectではありません: {path}")
    return parsed


def _validate_run_id(run_id: str) -> str:
    if not RUN_ID_RE.fullmatch(run_id or ""):
        raise ValueError(
            "不正run ID: 先頭英数字、全体1-24文字の英数字/_/-のみ使用可能"
        )
    return run_id


def _assert_canary_path(path: Path, root: Path = CANARY_ROOT) -> None:
    resolved_root = root.resolve()
    resolved_path = path.resolve()
    if resolved_root.name != "_test_batch_api_canary":
        raise ValueError(f"canary root名が不正です: {resolved_root}")
    if resolved_path == resolved_root or resolved_root not in resolved_path.parents:
        raise ValueError(f"production/non-canary pathへの書込み拒否: {resolved_path}")
    if PRODUCTION_RESULT_DIR.resolve() == resolved_path or PRODUCTION_RESULT_DIR.resolve() in resolved_path.parents:
        raise ValueError(f"production output pathへの書込み拒否: {resolved_path}")


def _run_dir(run_id: str, root: Path = CANARY_ROOT) -> Path:
    _validate_run_id(run_id)
    result = root / run_id
    _assert_canary_path(result, root=root)
    return result


def _build_full_system_prompt(response_schema: Dict[str, Any]) -> str:
    schema_text = json.dumps(response_schema, ensure_ascii=False, indent=2)
    return (
        f"{PRODUCTION.SYSTEM_PROMPT}\n\n"
        "必ず以下のJSONスキーマに従ってJSONのみを返すこと。"
        "キー名は変更禁止。値のみ更新可。\n"
        f"```json\n{schema_text}\n```"
    )


def _build_request_body(
    required_skills: List[Dict[str, Any]],
    skillsheet_text: str,
    project_body_text: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    response_schema = PRODUCTION._build_schema(required_skills)
    user_prompt = PRODUCTION._build_user_prompt(
        required_skills, skillsheet_text, project_body_text
    )
    body = {
        "model": PRODUCTION.RECHECK_LLM_MODEL,
        "messages": [
            {"role": "system", "content": _build_full_system_prompt(response_schema)},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.0,
        "max_tokens": 4096,
        "response_format": {"type": "json_object"},
    }
    return body, response_schema


def _load_candidates() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    skillsheet_map = PRODUCTION._load_skillsheet_map()
    cleaned_email_map = PRODUCTION._load_cleaned_email_map()
    candidates: List[Dict[str, Any]] = []
    excluded: List[Dict[str, Any]] = []
    source_ordinal = 0

    for score_band, input_path in PRODUCTION.INPUT_SCORE_FILES:
        for record in read_jsonl(str(input_path)):
            if PRODUCTION._is_no_match_record(record):
                continue
            source_ordinal += 1
            project_id = str(record.get("project_info", {}).get("message_id", ""))
            resource_id = str(record.get("resource_info", {}).get("message_id", ""))
            skillsheet_record = skillsheet_map.get(resource_id)
            if (
                not skillsheet_record
                or not skillsheet_record.get("success", False)
                or not str(skillsheet_record.get("skillsheet") or "").strip()
            ):
                excluded.append(
                    {
                        "source_ordinal": source_ordinal,
                        "project_message_id": project_id,
                        "resource_message_id": resource_id,
                        "reason": "missing_resource_skillsheet",
                    }
                )
                continue

            required_skills = PRODUCTION._required_skills_from_record(record)
            normalized = str(skillsheet_record.get("skillsheet") or "").strip()
            skillsheet_text = PRODUCTION._truncate_skillsheet(
                build_skillsheet_ai_context(normalized)
            )
            project_body = cleaned_email_map.get(project_id, "")
            project_body_text = (
                PRODUCTION._truncate_project_body(project_body) if project_body else ""
            )
            body, response_schema = _build_request_body(
                required_skills, skillsheet_text, project_body_text
            )
            source_hash = _sha256(record)
            candidates.append(
                {
                    "source_ordinal": source_ordinal,
                    "score_band": score_band,
                    "project_message_id": project_id,
                    "resource_message_id": resource_id,
                    "required_skill_count": len(required_skills),
                    "required_skill_texts": [
                        PRODUCTION._skill_text(skill) for skill in required_skills
                    ],
                    "request_size": len(_canonical_bytes(body)),
                    "source_record_sha256": source_hash,
                    "selection_hash": hashlib.sha256(
                        (source_hash + "|08-5-batch-canary-v1").encode("utf-8")
                    ).hexdigest(),
                    "body": body,
                    "response_schema": response_schema,
                }
            )

    if len(candidates) != MAX_SAMPLE_SIZE:
        raise ValueError(
            "API送信可能件数が想定と不一致: "
            f"expected={MAX_SAMPLE_SIZE} actual={len(candidates)} excluded={len(excluded)}"
        )
    return candidates, excluded


def _quantile_cutpoints(values: Sequence[int]) -> Tuple[int, int, int]:
    ordered = sorted(values)
    if not ordered:
        return 0, 0, 0
    indexes = [int((len(ordered) - 1) * fraction) for fraction in (0.25, 0.5, 0.75)]
    return tuple(ordered[index] for index in indexes)  # type: ignore[return-value]


def _skill_bucket(count: int) -> str:
    if count <= 2:
        return "1-2"
    if count <= 5:
        return "3-5"
    return "6+"


def _size_bucket(size: int, cutpoints: Tuple[int, int, int]) -> str:
    if size <= cutpoints[0]:
        return "q1"
    if size <= cutpoints[1]:
        return "q2"
    if size <= cutpoints[2]:
        return "q3"
    return "q4"


def _sample_candidates(
    candidates: Sequence[Dict[str, Any]], sample_size: int
) -> List[Dict[str, Any]]:
    if sample_size < 1 or sample_size > MAX_SAMPLE_SIZE:
        raise ValueError(f"sample-sizeは1-{MAX_SAMPLE_SIZE}で指定してください")
    if sample_size > len(candidates):
        raise ValueError("sample-sizeがeligible件数を超えています")
    if sample_size == len(candidates):
        return sorted(candidates, key=lambda item: item["source_ordinal"])

    cutpoints = _quantile_cutpoints([int(item["request_size"]) for item in candidates])
    strata: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for item in candidates:
        key = (
            str(item["score_band"]),
            _skill_bucket(int(item["required_skill_count"])),
            _size_bucket(int(item["request_size"]), cutpoints),
        )
        strata[key].append(item)

    quotas: Dict[Tuple[str, str, str], int] = {}
    remainders: List[Tuple[float, Tuple[str, str, str]]] = []
    assigned = 0
    for key in sorted(strata):
        exact = len(strata[key]) * sample_size / len(candidates)
        base = int(math.floor(exact))
        quotas[key] = base
        assigned += base
        remainders.append((exact - base, key))
    for _, key in sorted(remainders, key=lambda item: (-item[0], item[1]))[
        : sample_size - assigned
    ]:
        quotas[key] += 1

    selected: List[Dict[str, Any]] = []
    for key in sorted(strata):
        ordered = sorted(
            strata[key], key=lambda item: (item["selection_hash"], item["source_ordinal"])
        )
        selected.extend(ordered[: quotas[key]])
    if len(selected) != sample_size:
        raise AssertionError(
            f"sampling件数不一致: expected={sample_size} actual={len(selected)}"
        )
    return sorted(selected, key=lambda item: item["source_ordinal"])


def _custom_id(run_id: str, ordinal: int, item: Dict[str, Any]) -> str:
    identity = (
        f"{item['project_message_id']}|{item['resource_message_id']}|"
        f"{item['source_record_sha256']}"
    )
    identity_hash = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12]
    return f"c-{run_id}-{ordinal:04d}-{identity_hash}"


def _ensure_no_secrets(paths: Iterable[Path]) -> None:
    violations: List[str] = []
    for path in paths:
        text = path.read_text(encoding="utf-8", errors="replace")
        for pattern in SECRET_PATTERNS:
            if pattern.search(text):
                violations.append(f"{path.name}:{pattern.pattern}")
    if violations:
        raise ValueError("秘密情報らしき値を成果物で検出: " + ", ".join(violations))


def _source_hash_map(
    candidates: Sequence[Dict[str, Any]],
) -> Dict[Tuple[int, str, str], str]:
    return {
        (
            int(item["source_ordinal"]),
            str(item["project_message_id"]),
            str(item["resource_message_id"]),
        ): str(item["source_record_sha256"])
        for item in candidates
    }


def _validate_prepared(
    run_dir: Path,
    source_hashes: Optional[Dict[Tuple[int, str, str], str]] = None,
) -> Dict[str, Any]:
    input_records = list(read_jsonl(str(run_dir / "input.jsonl")))
    manifest = list(read_jsonl(str(run_dir / "manifest.jsonl")))
    if len(input_records) != len(manifest):
        raise ValueError(
            f"input/manifest件数不一致: input={len(input_records)} manifest={len(manifest)}"
        )
    input_by_id: Dict[str, Dict[str, Any]] = {}
    manifest_by_id: Dict[str, Dict[str, Any]] = {}
    for record in input_records:
        custom_id = str(record.get("custom_id") or "")
        if not custom_id or custom_id in input_by_id:
            raise ValueError(f"duplicate/empty custom_id in input: {custom_id!r}")
        input_by_id[custom_id] = record
    for entry in manifest:
        custom_id = str(entry.get("custom_id") or "")
        if not custom_id or custom_id in manifest_by_id:
            raise ValueError(f"duplicate/empty custom_id in manifest: {custom_id!r}")
        manifest_by_id[custom_id] = entry
    if set(input_by_id) != set(manifest_by_id):
        raise ValueError("manifest mismatch: custom_id集合がinputと不一致")
    for custom_id, request in input_by_id.items():
        entry = manifest_by_id[custom_id]
        if _sha256(request) != entry.get("request_sha256"):
            raise ValueError(f"manifest mismatch: request SHA-256不一致 {custom_id}")
        if _sha256(request.get("body")) != entry.get("request_body_sha256"):
            raise ValueError(f"manifest mismatch: request body SHA-256不一致 {custom_id}")
        if request.get("method") != "POST" or request.get("url") != "/v1/chat/completions":
            raise ValueError(f"Batch request envelope不正: {custom_id}")
        required_skill_texts = entry.get("required_skill_texts")
        if not isinstance(required_skill_texts, list) or not all(
            isinstance(skill, str) for skill in required_skill_texts
        ):
            raise ValueError(f"manifest mismatch: required skill一覧不正 {custom_id}")
        if len(required_skill_texts) != int(entry.get("required_skill_count", -1)):
            raise ValueError(f"manifest mismatch: required skill件数不一致 {custom_id}")
        expected_schema = PRODUCTION._build_schema(
            [{"skill": skill} for skill in required_skill_texts]
        )
        if _sha256(expected_schema) != entry.get("response_schema_sha256"):
            raise ValueError(f"manifest mismatch: response schema SHA-256不一致 {custom_id}")
        if source_hashes is not None:
            source_key = (
                int(entry.get("source_ordinal", -1)),
                str(entry.get("project_message_id") or ""),
                str(entry.get("resource_message_id") or ""),
            )
            expected_source_hash = source_hashes.get(source_key)
            if (
                expected_source_hash is None
                or expected_source_hash != entry.get("source_record_sha256")
            ):
                raise ValueError(
                    f"manifest mismatch: source record SHA-256不一致 {custom_id}"
                )
    _ensure_no_secrets([run_dir / "input.jsonl", run_dir / "manifest.jsonl"])
    manifest_sha256 = _sha256(manifest)
    state_path = run_dir / "batch_state.json"
    if state_path.exists():
        state = _read_json(state_path)
        stored_manifest_sha256 = state.get("manifest_sha256")
        if stored_manifest_sha256 and stored_manifest_sha256 != manifest_sha256:
            raise ValueError("manifest mismatch: batch state SHA-256不一致")
    return {
        "input_count": len(input_records),
        "manifest_count": len(manifest),
        "custom_id_unique": len(input_by_id) == len(input_records),
        "request_body_generated": all(isinstance(row.get("body"), dict) for row in input_records),
        "production_write": 0,
        "api_calls": 0,
        "manifest_sha256": manifest_sha256,
    }


def prepare_run(
    run_id: str,
    sample_size: int,
    root: Path = CANARY_ROOT,
    candidates: Optional[Sequence[Dict[str, Any]]] = None,
    excluded: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    run_dir = _run_dir(run_id, root=root)
    if run_dir.exists():
        raise FileExistsError(f"既存canary runのoverwrite拒否: {run_dir}")
    if sample_size < 1 or sample_size > MAX_SAMPLE_SIZE:
        raise ValueError(f"sample-sizeは1-{MAX_SAMPLE_SIZE}で指定してください")

    if candidates is None:
        loaded_candidates, loaded_excluded = _load_candidates()
        candidates = loaded_candidates
        excluded = loaded_excluded
    selected = _sample_candidates(candidates, sample_size)
    run_dir.mkdir(parents=True, exist_ok=False)
    _assert_canary_path(run_dir, root=root)

    input_records: List[Dict[str, Any]] = []
    manifest: List[Dict[str, Any]] = []
    custom_ids = set()
    for ordinal, item in enumerate(selected, 1):
        custom_id = _custom_id(run_id, ordinal, item)
        if custom_id in custom_ids:
            raise ValueError(f"duplicate custom_id生成拒否: {custom_id}")
        custom_ids.add(custom_id)
        request = {
            "custom_id": custom_id,
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": item["body"],
        }
        manifest_entry = {
            "ordinal": ordinal,
            "source_ordinal": item["source_ordinal"],
            "custom_id": custom_id,
            "project_message_id": item["project_message_id"],
            "resource_message_id": item["resource_message_id"],
            "score_band": item["score_band"],
            "required_skill_count": item["required_skill_count"],
            "required_skill_texts": item["required_skill_texts"],
            "request_size": item["request_size"],
            "request_sha256": _sha256(request),
            "request_body_sha256": _sha256(item["body"]),
            "response_schema_sha256": _sha256(item["response_schema"]),
            "source_record_sha256": item["source_record_sha256"],
        }
        input_records.append(request)
        manifest.append(manifest_entry)

    input_path = run_dir / "input.jsonl"
    manifest_path = run_dir / "manifest.jsonl"
    _assert_canary_path(input_path, root=root)
    _assert_canary_path(manifest_path, root=root)
    write_jsonl(str(input_path), input_records)
    write_jsonl(str(manifest_path), manifest)
    prepared_at = _utc_now()
    state = {
        "canary_run_id": run_id,
        "sample_size": sample_size,
        "eligible_input_count": len(candidates),
        "excluded_input_count": len(excluded or []),
        "excluded_reasons": dict(
            Counter(str(item.get("reason", "unknown")) for item in (excluded or []))
        ),
        "prepared_at": prepared_at,
        "file_uploaded_at": None,
        "batch_submitted_at": None,
        "submit_started_at": None,
        "batch_create_started_at": None,
        "batch_create_response_received_at": None,
        "submission_state": "prepared",
        "submission_attempt": 0,
        "manifest_sha256": _sha256(manifest),
        "input_file_id": None,
        "batch_id": None,
        "last_status": None,
        "status_history": [],
        "batch_timestamps": {field: None for field in BATCH_TIME_FIELDS},
        "request_counts": None,
        "batch_usage": None,
        "output_file_id": None,
        "error_file_id": None,
        "production_write": 0,
    }
    _write_json(run_dir / "batch_state.json", state, root=root)
    validation = _validate_prepared(run_dir, _source_hash_map(candidates))
    return {
        "run_id": run_id,
        "run_dir": str(run_dir),
        "prepared_at": prepared_at,
        **validation,
    }


def _require_network(allow_network: bool) -> None:
    if not allow_network:
        raise PermissionError(
            "API modeは--allow-network明示時のみ実行可能です。今回の検証では指定禁止です"
        )


def _authorization_headers() -> Dict[str, str]:
    from common.llm_client import _get_api_key

    return {"Authorization": f"Bearer {_get_api_key()}"}


def _response_json(response: requests.Response, operation: str) -> Dict[str, Any]:
    try:
        response.raise_for_status()
    except requests.RequestException as error:
        raise RuntimeError(
            f"OpenAI {operation}失敗: status={response.status_code}"
        ) from error
    parsed = response.json()
    if not isinstance(parsed, dict):
        raise RuntimeError(f"OpenAI {operation} responseがJSON objectではありません")
    return parsed


def _update_batch_state(state: Dict[str, Any], batch: Dict[str, Any]) -> None:
    observed_at = _utc_now()
    status = batch.get("status")
    if status is not None:
        state["last_status"] = str(status)
        state.setdefault("status_history", []).append(
            {"observed_at": observed_at, "status": str(status)}
        )
    timestamps = state.setdefault("batch_timestamps", {})
    for field in BATCH_TIME_FIELDS:
        value = batch.get(field)
        if value is not None:
            timestamps[field] = value
    for field in ("output_file_id", "error_file_id", "request_counts", "usage"):
        if field in batch and batch.get(field) is not None:
            state["batch_usage" if field == "usage" else field] = batch[field]


def _assert_submit_allowed(state: Dict[str, Any]) -> None:
    if state.get("batch_id"):
        raise ValueError("既存batch_idあり: 二重submitを拒否")
    submission_state = str(state.get("submission_state") or "")
    if submission_state in {"submitting", "pending_reconciliation", "submitted"}:
        raise ValueError(
            f"submission_state={submission_state}: 安全確認前の再submitを拒否"
        )
    if state.get("input_file_id"):
        raise ValueError("既存input_file_idあり: 旧stateの曖昧な再submitを拒否")


def _acquire_submit_claim(run_dir: Path, root: Path = CANARY_ROOT) -> Path:
    claim_path = run_dir / SUBMIT_CLAIM_FILENAME
    _assert_canary_path(claim_path, root=root)
    try:
        descriptor = os.open(
            str(claim_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
    except FileExistsError as error:
        raise ValueError(
            "submit claim取得済み: 同一canary runの同時/再submitを拒否"
        ) from error
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as claim_file:
            claim_file.write(
                json.dumps(
                    {"claimed_at": _utc_now()},
                    ensure_ascii=False,
                    sort_keys=True,
                )
                + "\n"
            )
            claim_file.flush()
            os.fsync(claim_file.fileno())
    except Exception:
        # claimは意図的に残し、取得後のcrash/書込み失敗でも再submitを拒否する。
        raise
    return claim_path


def submit_run(run_id: str, allow_network: bool, root: Path = CANARY_ROOT) -> Dict[str, Any]:
    _require_network(allow_network)
    run_dir = _run_dir(run_id, root=root)
    candidates, _ = _load_candidates()
    validation = _validate_prepared(run_dir, _source_hash_map(candidates))
    state_path = run_dir / "batch_state.json"
    state = _read_json(state_path)
    _assert_submit_allowed(state)

    _acquire_submit_claim(run_dir, root=root)
    state = _read_json(state_path)
    _assert_submit_allowed(state)

    manifest_sha256 = str(validation["manifest_sha256"])
    stored_manifest_sha256 = str(state.get("manifest_sha256") or manifest_sha256)
    if stored_manifest_sha256 != manifest_sha256:
        raise ValueError("manifest mismatch: submit対象SHA-256不一致")
    try:
        submission_attempt = int(state.get("submission_attempt", 0)) + 1
    except (TypeError, ValueError):
        raise ValueError("submission_attemptが整数ではありません")
    state.update(
        {
            "submission_state": "submitting",
            "submission_attempt": submission_attempt,
            "manifest_sha256": manifest_sha256,
            "submit_started_at": _utc_now(),
        }
    )
    _write_json(state_path, state, root=root)

    headers = _authorization_headers()
    with open(run_dir / "input.jsonl", "rb") as input_file:
        response = requests.post(
            API_BASE_URL + "/files",
            headers=headers,
            data={"purpose": "batch"},
            files={"file": ("input.jsonl", input_file, "application/jsonl")},
            timeout=120,
        )
    uploaded = _response_json(response, "file upload")
    input_file_id = str(uploaded.get("id") or "")
    if not input_file_id:
        raise RuntimeError("OpenAI file upload responseにidがありません")
    state["input_file_id"] = input_file_id
    state["file_uploaded_at"] = _utc_now()
    _write_json(state_path, state, root=root)

    state["submission_state"] = "pending_reconciliation"
    state["batch_create_started_at"] = _utc_now()
    _write_json(state_path, state, root=root)
    response = requests.post(
        API_BASE_URL + "/batches",
        headers={**headers, "Content-Type": "application/json"},
        json={
            "input_file_id": input_file_id,
            "endpoint": "/v1/chat/completions",
            "completion_window": "24h",
            "metadata": {
                "canary_run_id": run_id,
                "step": PRODUCTION.STEP_NAME,
                "canary_attempt": str(submission_attempt),
                "manifest_sha256": manifest_sha256,
            },
        },
        timeout=120,
    )
    batch = _response_json(response, "batch create")
    batch_id = str(batch.get("id") or "")
    if not batch_id:
        raise RuntimeError("OpenAI batch create responseにidがありません")
    response_received_at = _utc_now()
    state["batch_id"] = batch_id
    state["batch_submitted_at"] = response_received_at
    state["batch_create_response_received_at"] = response_received_at
    state["submission_state"] = "submitted"
    _update_batch_state(state, batch)
    _write_json(state_path, state, root=root)
    return {"batch_id": batch_id, "status": state.get("last_status"), **validation}


def status_run(run_id: str, allow_network: bool, root: Path = CANARY_ROOT) -> Dict[str, Any]:
    _require_network(allow_network)
    run_dir = _run_dir(run_id, root=root)
    state_path = run_dir / "batch_state.json"
    state = _read_json(state_path)
    batch_id = str(state.get("batch_id") or "")
    if not batch_id:
        raise ValueError("batch_state.jsonにbatch_idがありません")
    response = requests.get(
        API_BASE_URL + f"/batches/{batch_id}",
        headers=_authorization_headers(),
        timeout=60,
    )
    batch = _response_json(response, "batch retrieve")
    _update_batch_state(state, batch)
    _write_json(state_path, state, root=root)
    return {
        "batch_id": batch_id,
        "status": state.get("last_status"),
        "request_counts": state.get("request_counts"),
    }


def _download_file(file_id: str, target: Path, root: Path = CANARY_ROOT) -> None:
    response = requests.get(
        API_BASE_URL + f"/files/{file_id}/content",
        headers=_authorization_headers(),
        timeout=120,
    )
    try:
        response.raise_for_status()
    except requests.RequestException as error:
        raise RuntimeError(
            f"OpenAI file content取得失敗: status={response.status_code}"
        ) from error
    _assert_canary_path(target, root=root)
    if target.exists():
        raise FileExistsError(f"既存fetch成果物のoverwrite拒否: {target}")
    target.write_bytes(response.content)


def fetch_run(run_id: str, allow_network: bool, root: Path = CANARY_ROOT) -> Dict[str, Any]:
    _require_network(allow_network)
    run_dir = _run_dir(run_id, root=root)
    state = _read_json(run_dir / "batch_state.json")
    status = str(state.get("last_status") or "")
    if status not in TERMINAL_STATUSES:
        raise ValueError(f"Batchがterminalではありません: {status!r}")
    downloaded: List[str] = []
    output_file_id = str(state.get("output_file_id") or "")
    error_file_id = str(state.get("error_file_id") or "")
    if output_file_id:
        target = run_dir / "output_raw.jsonl"
        _download_file(output_file_id, target, root=root)
        downloaded.append(target.name)
    if error_file_id:
        target = run_dir / "error_raw.jsonl"
        _download_file(error_file_id, target, root=root)
        downloaded.append(target.name)
    if not downloaded:
        raise ValueError("取得可能なoutput_file_id/error_file_idがありません")
    _ensure_no_secrets([run_dir / name for name in downloaded])
    return {"status": status, "downloaded": downloaded, "production_write": 0}


def _read_jsonl_strict(path: Path) -> Tuple[List[Dict[str, Any]], List[str]]:
    records: List[Dict[str, Any]] = []
    errors: List[str] = []
    if not path.exists():
        return records, errors
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            errors.append(f"{path.name}:{line_number}: empty line")
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as error:
            errors.append(f"{path.name}:{line_number}: malformed JSON: {error}")
            continue
        if not isinstance(parsed, dict):
            errors.append(f"{path.name}:{line_number}: JSON objectではない")
            continue
        records.append(parsed)
    return records, errors


def _usage_from_response_body(body: Dict[str, Any]) -> Dict[str, int]:
    usage = body.get("usage") if isinstance(body.get("usage"), dict) else {}
    input_tokens = int(usage.get("input_tokens", usage.get("prompt_tokens", 0)) or 0)
    output_tokens = int(usage.get("output_tokens", usage.get("completion_tokens", 0)) or 0)
    total_tokens = int(usage.get("total_tokens", input_tokens + output_tokens) or 0)
    input_details = usage.get("input_tokens_details")
    if not isinstance(input_details, dict):
        input_details = usage.get("prompt_tokens_details")
    if not isinstance(input_details, dict):
        input_details = {}
    cached_tokens = int(input_details.get("cached_tokens", 0) or 0)
    return {
        "input_tokens": max(0, input_tokens),
        "cached_input_tokens": max(0, cached_tokens),
        "output_tokens": max(0, output_tokens),
        "total_tokens": max(0, total_tokens),
    }


def _parse_success(record: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Dict[str, int], str]:
    response = record.get("response")
    if not isinstance(response, dict):
        return None, _usage_from_response_body({}), "response object欠落"
    status_code = response.get("status_code")
    if not isinstance(status_code, int) or not 200 <= status_code < 300:
        return None, _usage_from_response_body({}), f"HTTP status不正: {status_code!r}"
    body = response.get("body")
    if not isinstance(body, dict):
        return None, _usage_from_response_body({}), "response.body欠落"
    usage = _usage_from_response_body(body)
    try:
        content = body["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return None, usage, "choices[0].message.content欠落"
    if not isinstance(content, str):
        return None, usage, "response contentが文字列でない"
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as error:
        return None, usage, f"response content parse error: {error}"
    if not isinstance(parsed, dict):
        return None, usage, "response contentがJSON objectでない"
    return parsed, usage, ""


def _validate_response_schema(
    parsed: Dict[str, Any], manifest_entry: Dict[str, Any]
) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    expected_top = {"required_skill_checks", "category_match", "category_note"}
    if set(parsed) != expected_top:
        errors.append(
            f"top-level schema keys不一致 expected={sorted(expected_top)} actual={sorted(parsed)}"
        )
    checks = parsed.get("required_skill_checks")
    if not isinstance(checks, list):
        errors.append("required_skill_checksがlistでない")
        checks = []
    if len(checks) != int(manifest_entry.get("required_skill_count", -1)):
        errors.append("required_skill_checks件数不一致")
    expected_check = {"skill", "confidence", "reason", "evidence"}
    for index, check in enumerate(checks):
        if not isinstance(check, dict) or set(check) != expected_check:
            errors.append(f"required_skill_checks[{index}] schema keys不一致")
            continue
        if check.get("confidence") not in PRODUCTION.VALID_CONFIDENCES:
            errors.append(f"required_skill_checks[{index}] confidence不正")
    required_skill_texts = manifest_entry.get("required_skill_texts")
    if not isinstance(required_skill_texts, list) or not all(
        isinstance(skill, str) for skill in required_skill_texts
    ):
        errors.append("manifest required_skill_texts不正")
    else:
        production_required_skills = [
            {"skill": skill} for skill in required_skill_texts
        ]
        _, production_error = PRODUCTION._validate_required_skill_checks(
            production_required_skills, checks
        )
        if production_error:
            errors.append(f"production validation不一致: {production_error}")
    if parsed.get("category_match") not in PRODUCTION.VALID_CATEGORY_MATCHES:
        errors.append("category_match不正")
    if not isinstance(parsed.get("category_note"), str) or not parsed.get("category_note"):
        errors.append("category_noteが空または文字列でない")
    return not errors, errors


def _validate_responses(
    manifest: Sequence[Dict[str, Any]],
    output_records: Sequence[Dict[str, Any]],
    error_records: Sequence[Dict[str, Any]],
    parse_errors: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    manifest_by_id = {str(entry["custom_id"]): entry for entry in manifest}
    seen: Counter = Counter()
    by_id: Dict[str, Tuple[str, Dict[str, Any]]] = {}
    for source, records in (("output", output_records), ("error", error_records)):
        for record in records:
            custom_id = str(record.get("custom_id") or "")
            seen[custom_id] += 1
            if custom_id not in by_id:
                by_id[custom_id] = (source, record)

    duplicates = sorted(custom_id for custom_id, count in seen.items() if count > 1)
    unknown = sorted(custom_id for custom_id in seen if custom_id not in manifest_by_id)
    missing = sorted(custom_id for custom_id in manifest_by_id if custom_id not in seen)
    shadow_results: List[Dict[str, Any]] = []
    usage_totals = {
        "input_tokens": 0,
        "cached_input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
    }
    completed = 0
    failed = 0
    response_parse_errors: List[str] = list(parse_errors or [])
    response_schema_errors: List[str] = []
    schema_invalid_custom_ids: List[str] = []
    schema_invalid_custom_id_set = set()

    for entry in sorted(manifest, key=lambda item: int(item["ordinal"])):
        custom_id = str(entry["custom_id"])
        found = by_id.get(custom_id)
        if found is None:
            continue
        source, record = found
        shadow = {
            "ordinal": entry["ordinal"],
            "custom_id": custom_id,
            "project_message_id": entry["project_message_id"],
            "resource_message_id": entry["resource_message_id"],
            "score_band": entry["score_band"],
            "source": source,
            "schema_valid": False,
            "schema_errors": [],
            "required_skill_checks": None,
            "category_match": None,
            "category_note": None,
        }
        if source == "error" or record.get("error"):
            failed += 1
            shadow["schema_errors"] = ["Batch error response"]
        else:
            parsed, usage, parse_error = _parse_success(record)
            for key in usage_totals:
                usage_totals[key] += usage[key]
            if parse_error or parsed is None:
                failed += 1
                response_parse_errors.append(f"{custom_id}: {parse_error}")
                shadow["schema_errors"] = [parse_error]
            else:
                schema_valid, schema_errors = _validate_response_schema(parsed, entry)
                shadow["schema_valid"] = schema_valid
                shadow["schema_errors"] = schema_errors
                shadow["required_skill_checks"] = parsed.get("required_skill_checks")
                shadow["category_match"] = parsed.get("category_match")
                shadow["category_note"] = parsed.get("category_note")
                if schema_valid:
                    completed += 1
                else:
                    failed += 1
                    if custom_id not in schema_invalid_custom_id_set:
                        schema_invalid_custom_id_set.add(custom_id)
                        schema_invalid_custom_ids.append(custom_id)
                    response_schema_errors.extend(
                        f"{custom_id}: {error}" for error in schema_errors
                    )
        shadow_results.append(shadow)

    integrity_ok = not (
        duplicates
        or unknown
        or missing
        or response_parse_errors
        or response_schema_errors
    ) and completed + failed == len(manifest)
    return {
        "expected_request_count": len(manifest),
        "success_count": completed,
        "failed_count": failed,
        "duplicate_custom_ids": duplicates,
        "duplicate_count": len(duplicates),
        "missing_custom_ids": missing,
        "missing_count": len(missing),
        "unknown_custom_ids": unknown,
        "unknown_count": len(unknown),
        "parse_errors": response_parse_errors,
        "parse_error_count": len(response_parse_errors),
        "schema_errors": response_schema_errors,
        "schema_error_count": len(response_schema_errors),
        "schema_invalid_custom_ids": schema_invalid_custom_ids,
        "schema_invalid_pair_count": len(schema_invalid_custom_ids),
        "schema_violation_message_count": len(response_schema_errors),
        "integrity_ok": integrity_ok,
        "usage": usage_totals,
        "shadow_results": shadow_results,
    }


def _epoch_iso(value: Any) -> Optional[str]:
    if not isinstance(value, (int, float)):
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _latency_report(state: Dict[str, Any]) -> Dict[str, Any]:
    timestamps = state.get("batch_timestamps")
    if not isinstance(timestamps, dict):
        timestamps = {}
    last_status = str(state.get("last_status") or "")
    terminal_field = TERMINAL_TIME_FIELDS.get(last_status)
    created_epoch = timestamps.get("created_at")
    terminal_epoch = timestamps.get(terminal_field) if terminal_field else None
    created = (
        datetime.fromtimestamp(created_epoch, tz=timezone.utc)
        if isinstance(created_epoch, (int, float))
        else None
    )
    terminal = (
        datetime.fromtimestamp(terminal_epoch, tz=timezone.utc)
        if isinstance(terminal_epoch, (int, float))
        else None
    )
    in_progress_epoch = timestamps.get("in_progress_at")
    in_progress = (
        datetime.fromtimestamp(in_progress_epoch, tz=timezone.utc)
        if isinstance(in_progress_epoch, (int, float))
        else None
    )
    local_submit_started = _parse_iso(state.get("submit_started_at"))
    local_batch_create_started = _parse_iso(state.get("batch_create_started_at"))

    def elapsed_minutes(start: Optional[datetime], end: Optional[datetime]) -> Optional[float]:
        if start is None or end is None:
            return None
        return round((end - start).total_seconds() / 60.0, 3)

    return {
        "local_prepared_at": state.get("prepared_at"),
        "file_uploaded_at": state.get("file_uploaded_at"),
        "batch_submitted_at": state.get("batch_submitted_at"),
        "local_submit_started_at": state.get("submit_started_at"),
        "local_batch_create_started_at": state.get("batch_create_started_at"),
        "local_batch_create_response_received_at": state.get(
            "batch_create_response_received_at"
        ),
        "batch_timestamps_epoch": {field: timestamps.get(field) for field in BATCH_TIME_FIELDS},
        "batch_timestamps_iso": {field: _epoch_iso(timestamps.get(field)) for field in BATCH_TIME_FIELDS},
        "official_created_at": _epoch_iso(created_epoch),
        "official_terminal_field": terminal_field,
        "official_terminal_at": _epoch_iso(terminal_epoch),
        "official_batch_elapsed_minutes": elapsed_minutes(created, terminal),
        "local_submit_start_to_terminal_minutes": elapsed_minutes(
            local_submit_started, terminal
        ),
        "local_batch_create_start_to_terminal_minutes": elapsed_minutes(
            local_batch_create_started, terminal
        ),
        "official_in_progress_to_terminal_minutes": elapsed_minutes(
            in_progress, terminal
        ),
    }


def _cost_report(usage: Dict[str, int], pricing: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "pricing_source": None,
        "direct_equivalent_usd": None,
        "batch_cost_usd": None,
        "cost_reduction_usd": None,
        "cost_reduction_percent": None,
    }
    if not pricing:
        return result

    def calculate(rate: Dict[str, Any]) -> float:
        input_rate = float(rate["input_per_million"])
        cached_rate = float(rate.get("cached_input_per_million", input_rate))
        output_rate = float(rate["output_per_million"])
        cached = min(usage["cached_input_tokens"], usage["input_tokens"])
        uncached = usage["input_tokens"] - cached
        return (
            uncached * input_rate + cached * cached_rate + usage["output_tokens"] * output_rate
        ) / 1_000_000

    direct = calculate(pricing["direct"])
    batch = calculate(pricing["batch"])
    reduction = direct - batch
    result.update(
        {
            "pricing_source": pricing.get("source", "user_supplied_pricing_file"),
            "direct_equivalent_usd": round(direct, 8),
            "batch_cost_usd": round(batch, 8),
            "cost_reduction_usd": round(reduction, 8),
            "cost_reduction_percent": round(reduction / direct * 100, 3) if direct else None,
        }
    )
    return result


def _direct_result_map() -> Dict[Tuple[str, str], Dict[str, Any]]:
    path = PRODUCTION.OUTPUT_ALL
    if not path.exists():
        return {}
    result: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for record in read_jsonl(str(path)):
        project_id = str(record.get("project_info", {}).get("message_id", ""))
        resource_id = str(record.get("resource_info", {}).get("message_id", ""))
        result[(project_id, resource_id)] = {
            "required_skill_checks": record.get("required_skill_checks"),
            "category_match": record.get("category_match"),
            "category_note": record.get("category_note"),
            "recheck_status": record.get("recheck_info", {}).get("recheck_status"),
        }
    return result


def _report_text(report: Dict[str, Any]) -> str:
    latency = report["latency"]
    usage = report["usage"]
    cost = report["cost"]
    lines = [
        "【Batch Canary Result】",
        "",
        f"sample size: {report['sample_size']}",
        f"submitted: {report['submitted']}",
        f"completed: {report['completed']}",
        f"failed: {report['failed']}",
        f"expired: {report['expired']}",
        f"duplicate: {report['duplicate']}",
        f"missing: {report['missing']}",
        f"unknown: {report['unknown']}",
        f"parse error: {report['parse_error']}",
        f"schema invalid pairs: {report['schema_invalid_pairs']}",
        f"schema violation messages: {report['schema_violation_messages']}",
        "schema invalid custom ids: "
        f"{json.dumps(report['schema_invalid_custom_ids'], ensure_ascii=False)}",
        "schema error (legacy alias; validation message count): "
        f"{report['schema_error']}",
        f"official created time: {latency['official_created_at']}",
        f"official terminal time: {latency['official_terminal_at']}",
        f"official batch elapsed: {latency['official_batch_elapsed_minutes']} min",
        f"local submit start: {latency['local_submit_started_at']}",
        "local submit start to terminal: "
        f"{latency['local_submit_start_to_terminal_minutes']} min",
        f"input tokens: {usage['input_tokens']}",
        f"cached input tokens: {usage['cached_input_tokens']}",
        f"output tokens: {usage['output_tokens']}",
        f"total tokens: {usage['total_tokens']}",
        f"estimated/direct comparison: {cost['direct_equivalent_usd']}",
        f"Batch cost: {cost['batch_cost_usd']}",
        f"cost reduction: {cost['cost_reduction_percent']}%",
        "production write: 0",
        f"integrity: {'OK' if report['integrity_ok'] else 'NG'}",
    ]
    return "\n".join(lines) + "\n"


def report_run(
    run_id: str,
    pricing_file: Optional[Path] = None,
    root: Path = CANARY_ROOT,
) -> Dict[str, Any]:
    run_dir = _run_dir(run_id, root=root)
    candidates, _ = _load_candidates()
    prepared_validation = _validate_prepared(run_dir, _source_hash_map(candidates))
    state = _read_json(run_dir / "batch_state.json")
    manifest = list(read_jsonl(str(run_dir / "manifest.jsonl")))
    output_records, output_parse_errors = _read_jsonl_strict(run_dir / "output_raw.jsonl")
    error_records, error_parse_errors = _read_jsonl_strict(run_dir / "error_raw.jsonl")
    validation = _validate_responses(
        manifest,
        output_records,
        error_records,
        output_parse_errors + error_parse_errors,
    )
    direct_map = _direct_result_map()
    for shadow in validation["shadow_results"]:
        shadow["saved_direct_result"] = direct_map.get(
            (shadow["project_message_id"], shadow["resource_message_id"])
        )
    pricing = _read_json(pricing_file) if pricing_file else None
    latency = _latency_report(state)
    cost = _cost_report(validation["usage"], pricing)
    last_status = str(state.get("last_status") or "")
    request_counts = state.get("request_counts")
    report = {
        "title": "Batch Canary Result",
        "canary_run_id": run_id,
        "sample_size": len(manifest),
        "submitted": (
            int(request_counts.get("total", 0))
            if isinstance(request_counts, dict)
            else (len(manifest) if state.get("batch_id") else 0)
        ),
        "completed": validation["success_count"],
        "failed": validation["failed_count"],
        "expired": len(manifest) if last_status == "expired" else 0,
        "cancelled": len(manifest) if last_status == "cancelled" else 0,
        "duplicate": validation["duplicate_count"],
        "missing": validation["missing_count"],
        "unknown": validation["unknown_count"],
        "parse_error": validation["parse_error_count"],
        "schema_invalid_pairs": validation["schema_invalid_pair_count"],
        "schema_violation_messages": validation["schema_violation_message_count"],
        "schema_invalid_custom_ids": validation["schema_invalid_custom_ids"],
        "schema_error": validation["schema_violation_message_count"],
        "schema_error_unit": "validation_messages (legacy alias)",
        "integrity_ok": validation["integrity_ok"],
        "duplicate_custom_ids": validation["duplicate_custom_ids"],
        "missing_custom_ids": validation["missing_custom_ids"],
        "unknown_custom_ids": validation["unknown_custom_ids"],
        "parse_errors": validation["parse_errors"],
        "schema_errors": validation["schema_errors"],
        "usage": validation["usage"],
        "batch_level_usage": state.get("batch_usage"),
        "cost": cost,
        "latency": latency,
        "status": last_status or None,
        "status_history": state.get("status_history", []),
        "request_counts": request_counts,
        "shadow_results": validation["shadow_results"],
        "prepared_validation": prepared_validation,
        "production_write": 0,
    }
    _write_json(run_dir / "report.json", report, root=root)
    report_txt = run_dir / "report.txt"
    _assert_canary_path(report_txt, root=root)
    report_txt.write_text(_report_text(report), encoding="utf-8")
    artifact_paths = [path for path in run_dir.iterdir() if path.is_file()]
    _ensure_no_secrets(artifact_paths)
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="08-5 OpenAI Batch API production-noninterference canary"
    )
    subparsers = parser.add_subparsers(dest="mode", required=True)

    prepare = subparsers.add_parser("prepare", help="offline sample/input/manifest生成")
    prepare.add_argument("--run-id", required=True)
    prepare.add_argument("--sample-size", required=True, type=int)
    prepare.add_argument(
        "--dry-run",
        action="store_true",
        help="API通信0のoffline validation（prepare自体は常にAPI通信0）",
    )

    for mode in ("submit", "status", "fetch"):
        api_parser = subparsers.add_parser(mode)
        api_parser.add_argument("--run-id", required=True)
        api_parser.add_argument(
            "--allow-network",
            action="store_true",
            help="OpenAI API通信を明示許可（今回の検証では使用禁止）",
        )

    report = subparsers.add_parser("report", help="取得済みfixture/responseをoffline検証")
    report.add_argument("--run-id", required=True)
    report.add_argument("--pricing-file", type=Path)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    try:
        if args.mode == "prepare":
            result = prepare_run(args.run_id, args.sample_size)
            result["dry_run"] = bool(args.dry_run)
        elif args.mode == "submit":
            result = submit_run(args.run_id, args.allow_network)
        elif args.mode == "status":
            result = status_run(args.run_id, args.allow_network)
        elif args.mode == "fetch":
            result = fetch_run(args.run_id, args.allow_network)
        else:
            result = report_run(args.run_id, args.pricing_file)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        if args.mode == "report" and not result.get("integrity_ok", False):
            sys.exit(1)
    except Exception as error:
        print(f"ERROR: {type(error).__name__}: {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

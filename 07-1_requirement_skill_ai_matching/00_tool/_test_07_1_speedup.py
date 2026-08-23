"""07-1 Direct concurrent speed test (test only).

Production 07-1 is imported as the source of truth for request construction and
response validation.  This file never writes production outputs.  Network use
requires both an explicit sample size and --allow-network; at most 500 logical
requests can be made in one run.
"""

import argparse
import copy
import hashlib
import importlib.util
import json
import math
import re
import statistics
import sys
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Set, Tuple

import requests


TOOL_DIR = Path(__file__).resolve().parent
STEP_DIR = TOOL_DIR.parent
PROJECT_ROOT = STEP_DIR.parent
PRODUCTION_PATH = TOOL_DIR / "normalized/requirement_skill_ai_matching.py"
TEST_OUTPUT_ROOT = STEP_DIR / "_test_07_1_speedup"
MAX_LIVE_SAMPLE_SIZE = 500
MAX_CONCURRENCY_HARD_LIMIT = 4
PRESERVED_SAMPLE_PREFIX_SIZE = 300
DEFAULT_SAMPLE_SEED = "07-1-speedup-v1"
DEFAULT_INITIAL_CONCURRENCY = 2
DEFAULT_MAX_CONCURRENCY = 4
LAUNCH_INTERVAL_SECONDS = 0.5
PRICE_PER_MILLION = {
    "uncached_input": 0.15,
    "cached_input": 0.075,
    "output": 0.60,
}
PRICE_SOURCE = "https://developers.openai.com/api/docs/models/gpt-4o-mini"
BASELINE_TIMING = STEP_DIR / "99_execution_time/07-1_requirement_skill_ai_matching.txt"
BASELINE_USAGE_DIR = STEP_DIR / "99_execution_time"

sys.path.insert(0, str(PROJECT_ROOT))

from common.json_utils import append_jsonl, read_jsonl, write_jsonl
from common.llm_client import (
    LLMOutputTruncatedError,
    _extract_usage,
    _get_api_key,
    _validate_schema_keys,
)
from common.logger import get_logger


def _load_production_module() -> Any:
    spec = importlib.util.spec_from_file_location("production_07_1_for_speed_test", PRODUCTION_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("production 07-1 moduleをloadできません")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


production = _load_production_module()
logger = get_logger("_test_07_1_speedup")


def _json_hash(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _request_identity(original_ordinal: int, project_mid: str, resource_mid: str) -> str:
    raw = f"{original_ordinal}\0{project_mid}\0{resource_mid}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _valid_response_from_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
    response = copy.deepcopy(schema)
    for field in ("required_skills", "optional_skills"):
        for item in response.get(field, []):
            item["match"] = False
            item["note"] = "test request capture"
    return response


def capture_production_request(
    pair: Dict[str, Any],
    project_skills_map: Dict[str, Any],
    skillsheet_map: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Capture the exact call_llm kwargs emitted by production process_pair."""
    captured: Dict[str, Any] = {}

    def fake_call_llm(**kwargs: Any) -> Dict[str, Any]:
        captured.update(kwargs)
        return _valid_response_from_schema(kwargs["response_schema"])

    original = production.call_llm
    production.call_llm = fake_call_llm
    try:
        production.process_pair(pair, project_skills_map, skillsheet_map, logger)
    finally:
        production.call_llm = original
    return captured or None


def build_request_payload(call_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Build the Chat Completions body exactly as common.llm_client.call_llm."""
    schema_str = json.dumps(
        call_kwargs["response_schema"], ensure_ascii=False, indent=2
    )
    full_system_prompt = (
        f"{call_kwargs['system_prompt']}\n\n"
        "必ず以下のJSONスキーマに従ってJSONのみを返すこと。キー名は変更禁止。値のみ更新可。\n"
        f"```json\n{schema_str}\n```"
    )
    return {
        "model": call_kwargs.get("model", "gpt-4o-mini"),
        "messages": [
            {"role": "system", "content": full_system_prompt},
            {"role": "user", "content": call_kwargs["user_prompt"]},
        ],
        "temperature": call_kwargs.get("temperature", 0.0),
        "max_tokens": call_kwargs.get("max_tokens", 1024),
        "response_format": {"type": "json_object"},
    }


def load_inputs() -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    for path in (
        production.INPUT_PAIRS,
        production.INPUT_PROJECT_SKILLS,
        production.INPUT_SKILLSHEETS,
    ):
        if not Path(path).exists():
            raise FileNotFoundError(f"inputがありません: {path}")
    pairs = list(read_jsonl(str(production.INPUT_PAIRS)))
    projects = {
        str(rec["message_id"]): rec
        for rec in read_jsonl(str(production.INPUT_PROJECT_SKILLS))
        if rec.get("message_id")
    }
    skillsheets = {
        str(rec["message_id"]): rec
        for rec in read_jsonl(str(production.INPUT_SKILLSHEETS))
        if rec.get("message_id")
    }
    return pairs, projects, skillsheets


def deterministic_sample(
    pairs: List[Dict[str, Any]],
    project_skills_map: Dict[str, Any],
    skillsheet_map: Dict[str, Any],
    sample_size: int,
    seed: str = DEFAULT_SAMPLE_SEED,
) -> List[Dict[str, Any]]:
    """Select repeatable valid API calls, spread across repeated projects."""
    if sample_size < 1 or sample_size > MAX_LIVE_SAMPLE_SIZE:
        raise ValueError(f"sample_sizeは1..{MAX_LIVE_SAMPLE_SIZE}のみ")

    grouped: Dict[str, List[Tuple[int, Dict[str, Any]]]] = defaultdict(list)
    for original_ordinal, pair in enumerate(pairs):
        project_mid = str(pair.get("project_info", {}).get("message_id", ""))
        if project_mid:
            grouped[project_mid].append((original_ordinal, pair))

    ranked_projects = sorted(
        (pid for pid, rows in grouped.items() if len(rows) >= 2),
        key=lambda pid: hashlib.sha256(f"{seed}\0{pid}".encode("utf-8")).hexdigest(),
    )
    if not ranked_projects:
        raise ValueError("同一project内に複数pairを持つsample候補がありません")

    initial_project_count = min(4, max(1, sample_size // 2), len(ranked_projects))
    selected_projects = ranked_projects[:initial_project_count]
    next_project_index = initial_project_count
    positions = {pid: 0 for pid in selected_projects}
    selected: List[Dict[str, Any]] = []
    preserved_prefix_max_ordinal: Optional[int] = None

    while len(selected) < sample_size:
        progressed = False
        for project_mid in list(selected_projects):
            rows = grouped[project_mid]
            while positions[project_mid] < len(rows):
                original_ordinal, pair = rows[positions[project_mid]]
                positions[project_mid] += 1
                call_kwargs = capture_production_request(
                    pair, project_skills_map, skillsheet_map
                )
                if call_kwargs is None:
                    continue
                if (
                    sample_size > PRESERVED_SAMPLE_PREFIX_SIZE
                    and len(selected) >= PRESERVED_SAMPLE_PREFIX_SIZE
                    and preserved_prefix_max_ordinal is not None
                    and original_ordinal <= preserved_prefix_max_ordinal
                ):
                    continue
                resource_mid = str(
                    pair.get("resource_info", {}).get("message_id", "")
                )
                payload = build_request_payload(call_kwargs)
                selected.append(
                    {
                        "original_ordinal": original_ordinal,
                        "project_message_id": project_mid,
                        "resource_message_id": resource_mid,
                        "request_identity": _request_identity(
                            original_ordinal, project_mid, resource_mid
                        ),
                        "request_body_sha256": _json_hash(payload),
                        "pair": pair,
                    }
                )
                if len(selected) == PRESERVED_SAMPLE_PREFIX_SIZE:
                    preserved_prefix_max_ordinal = max(
                        row["original_ordinal"] for row in selected
                    )
                progressed = True
                break
            if len(selected) >= sample_size:
                break
        if not progressed:
            if next_project_index < len(ranked_projects):
                next_project = ranked_projects[next_project_index]
                next_project_index += 1
                selected_projects.append(next_project)
                positions[next_project] = 0
                continue
            break

    if len(selected) != sample_size:
        raise ValueError(
            f"valid sample不足: requested={sample_size} selected={len(selected)}"
        )

    selected.sort(key=lambda item: item["original_ordinal"])
    first_by_project: Dict[str, int] = {}
    for item in selected:
        pid = item["project_message_id"]
        first_by_project.setdefault(pid, item["original_ordinal"])
    for item in selected:
        item["is_project_warm_one"] = (
            item["original_ordinal"] == first_by_project[item["project_message_id"]]
        )
    return selected


def manifest_record(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ordinal": item["original_ordinal"],
        "project_message_id": item["project_message_id"],
        "resource_message_id": item["resource_message_id"],
        "request_identity": item["request_identity"],
        "request_body_sha256": item["request_body_sha256"],
        "is_project_warm_one": item["is_project_warm_one"],
    }


class LaunchRateLimiter:
    def __init__(self, min_interval_seconds: float = LAUNCH_INTERVAL_SECONDS):
        self.min_interval_seconds = min_interval_seconds
        self._lock = threading.Lock()
        self._last_start = 0.0

    def wait(self) -> None:
        with self._lock:
            remaining = self.min_interval_seconds - (time.monotonic() - self._last_start)
            if remaining > 0:
                time.sleep(remaining)
            self._last_start = time.monotonic()


def _retry_after_seconds(headers: Dict[str, str], attempt: int) -> float:
    value = headers.get("retry-after", "").strip()
    try:
        return min(30.0, max(0.0, float(value)))
    except (TypeError, ValueError):
        return min(8.0, float(2 ** (attempt - 1)))


def _rate_headers(headers: Any) -> Dict[str, str]:
    wanted = {
        "retry-after",
        "x-ratelimit-limit-requests",
        "x-ratelimit-remaining-requests",
        "x-ratelimit-reset-requests",
        "x-ratelimit-limit-tokens",
        "x-ratelimit-remaining-tokens",
        "x-ratelimit-reset-tokens",
        "x-request-id",
    }
    return {
        str(key).lower(): str(value)
        for key, value in dict(headers or {}).items()
        if str(key).lower() in wanted
    }


class DirectTestClient:
    """Direct Chat Completions client with test-local telemetry only."""

    def __init__(
        self,
        api_key: str,
        post: Callable[..., Any] = requests.post,
        rate_limiter: Optional[LaunchRateLimiter] = None,
    ):
        self.api_key = api_key
        self.post = post
        self.rate_limiter = rate_limiter or LaunchRateLimiter()
        self.local = threading.local()

    def set_expected(self, request_body_sha256: str) -> None:
        self.local.expected_hash = request_body_sha256
        self.local.metadata = {}

    def pop_metadata(self) -> Dict[str, Any]:
        metadata = getattr(self.local, "metadata", {})
        self.local.metadata = {}
        return metadata

    def __call__(
        self,
        system_prompt: str,
        user_prompt: str,
        response_schema: Dict[str, Any],
        model: str = "gpt-4o-mini",
        temperature: float = 0.0,
        max_tokens: int = 1024,
        max_retries: int = 3,
        retry_wait_seconds: float = 5.0,
        telemetry_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        del retry_wait_seconds, telemetry_context
        kwargs = {
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "response_schema": response_schema,
            "model": model,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        payload = build_request_payload(kwargs)
        actual_hash = _json_hash(payload)
        expected_hash = getattr(self.local, "expected_hash", "")
        if not expected_hash or actual_hash != expected_hash:
            raise RuntimeError(
                "production request bodyとの不一致を検出: "
                f"expected={expected_hash} actual={actual_hash}"
            )

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        logical_start = time.monotonic()
        attempts: List[Dict[str, Any]] = []
        final_usage = {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "usage_available": False,
        }
        final_headers: Dict[str, str] = {}
        last_error: Optional[Exception] = None

        for attempt in range(1, max_retries + 1):
            self.rate_limiter.wait()
            attempt_start = time.monotonic()
            response = None
            response_json: Any = None
            status_code: Optional[int] = None
            error_type = ""
            try:
                response = self.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers=headers,
                    json=payload,
                    timeout=60,
                )
                status_code = response.status_code
                final_headers = _rate_headers(response.headers)
                try:
                    response_json = response.json()
                except ValueError:
                    response_json = None
                response.raise_for_status()
                if not isinstance(response_json, dict):
                    raise ValueError("OpenAI APIレスポンスがJSON objectでない")
                choice = response_json["choices"][0]
                content = choice["message"]["content"]
                if choice.get("finish_reason") == "length":
                    raise LLMOutputTruncatedError(
                        "LLM output truncated: "
                        f"finish_reason=length model={model} max_tokens={max_tokens} "
                        f"content_chars={len(content or '')}"
                    )
                parsed = json.loads(content)
                _validate_schema_keys(parsed, response_schema)
                final_usage = _extract_usage(response_json)
                attempts.append(
                    {
                        "attempt": attempt,
                        "status_code": status_code,
                        "latency_seconds": time.monotonic() - attempt_start,
                        "error_type": "",
                        "rate_limit_headers": final_headers,
                        "usage": final_usage,
                    }
                )
                self.local.metadata = {
                    "latency_seconds": time.monotonic() - logical_start,
                    "attempts": attempts,
                    "retry_count": attempt - 1,
                    "rate_limit_429_count": sum(
                        row.get("status_code") == 429 for row in attempts
                    ),
                    "api_failure": False,
                    "usage": final_usage,
                    "rate_limit_headers": final_headers,
                    "request_body_sha256": actual_hash,
                }
                return parsed
            except LLMOutputTruncatedError as error:
                error_type = type(error).__name__
                last_error = error
                attempts.append(
                    {
                        "attempt": attempt,
                        "status_code": status_code,
                        "latency_seconds": time.monotonic() - attempt_start,
                        "error_type": error_type,
                        "rate_limit_headers": final_headers,
                        "usage": _extract_usage(response_json),
                    }
                )
                break
            except (requests.exceptions.RequestException, ValueError, KeyError, json.JSONDecodeError) as error:
                error_type = type(error).__name__
                last_error = error
                attempts.append(
                    {
                        "attempt": attempt,
                        "status_code": status_code,
                        "latency_seconds": time.monotonic() - attempt_start,
                        "error_type": error_type,
                        "rate_limit_headers": final_headers,
                        "usage": _extract_usage(response_json),
                    }
                )
                non_retryable = status_code in (400, 401, 403)
                if non_retryable or attempt >= max_retries:
                    break
                time.sleep(_retry_after_seconds(final_headers, attempt))

        self.local.metadata = {
            "latency_seconds": time.monotonic() - logical_start,
            "attempts": attempts,
            "retry_count": max(0, len(attempts) - 1),
            "rate_limit_429_count": sum(
                row.get("status_code") == 429 for row in attempts
            ),
            "api_failure": True,
            "usage": final_usage,
            "rate_limit_headers": final_headers,
            "request_body_sha256": actual_hash,
        }
        if isinstance(last_error, LLMOutputTruncatedError):
            raise last_error
        if isinstance(last_error, (ValueError, json.JSONDecodeError)):
            raise ValueError(f"OpenAI APIレスポンスJSON不正: {last_error}") from last_error
        raise RuntimeError(
            f"OpenAI API呼び出しが{max_retries}回以内に成功しませんでした: {last_error}"
        ) from last_error


class AdaptiveConcurrency:
    def __init__(self, initial: int, maximum: int):
        if initial < 1 or maximum < initial or maximum > MAX_CONCURRENCY_HARD_LIMIT:
            raise ValueError(
                f"concurrencyは1 <= initial <= max <= {MAX_CONCURRENCY_HARD_LIMIT}"
            )
        self.current = initial
        self.maximum = maximum
        self.success_streak = 0
        self.history = [
            {"elapsed_seconds": 0.0, "concurrency": initial, "reason": "initial"}
        ]
        self.started = time.monotonic()

    @staticmethod
    def _positive_number(value: Any) -> Optional[float]:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if number >= 0 else None

    def _change(self, new_value: int, reason: str) -> None:
        new_value = max(1, min(self.maximum, new_value))
        if new_value != self.current:
            self.current = new_value
            self.history.append(
                {
                    "elapsed_seconds": time.monotonic() - self.started,
                    "concurrency": self.current,
                    "reason": reason,
                }
            )

    def observe(self, checkpoint: Dict[str, Any]) -> None:
        telemetry = checkpoint.get("telemetry", {})
        if telemetry.get("rate_limit_429_count", 0):
            self.success_streak = 0
            self._change(self.current - 1, "429")
            return
        if telemetry.get("api_failure") or checkpoint.get("status") != "success":
            self.success_streak = 0
            self._change(self.current - 1, "failure")
            return
        if float(telemetry.get("latency_seconds", 0.0)) > 45.0:
            self.success_streak = 0
            self._change(self.current - 1, "latency>45s")
            return

        headers = telemetry.get("rate_limit_headers") or {}
        remaining_requests = self._positive_number(
            headers.get("x-ratelimit-remaining-requests")
        )
        remaining_tokens = self._positive_number(
            headers.get("x-ratelimit-remaining-tokens")
        )
        if remaining_requests is None or remaining_tokens is None:
            self.success_streak = 0
            return
        if remaining_requests <= self.current * 2 or remaining_tokens <= self.current * 12000:
            self.success_streak = 0
            self._change(self.current - 1, "rate-limit headroom low")
            return
        self.success_streak += 1
        if self.success_streak >= 2 and self.current < self.maximum:
            self.success_streak = 0
            self._change(self.current + 1, "rate-limit headroom confirmed")


def _worker(
    item: Dict[str, Any],
    project_skills_map: Dict[str, Any],
    skillsheet_map: Dict[str, Any],
    client: Any,
    concurrency_at_submit: int,
) -> Dict[str, Any]:
    started = datetime.now().astimezone().isoformat()
    client.set_expected(item["request_body_sha256"])
    worker_exception = ""
    try:
        result, error = production.process_pair(
            item["pair"], project_skills_map, skillsheet_map, logger
        )
    except Exception as exc:
        result = None
        error = production._make_error(
            item["project_message_id"],
            item["resource_message_id"],
            "llm_call_error",
            str(exc)[:1000],
        )
        worker_exception = f"{type(exc).__name__}: {exc}"
    telemetry = client.pop_metadata()
    return {
        "ordinal": item["original_ordinal"],
        "project_message_id": item["project_message_id"],
        "resource_message_id": item["resource_message_id"],
        "request_identity": item["request_identity"],
        "request_body_sha256": item["request_body_sha256"],
        "status": "success" if result is not None and error is None else "error",
        "result": result,
        "error": error,
        "worker_exception": worker_exception,
        "started_at": started,
        "completed_at": datetime.now().astimezone().isoformat(),
        "concurrency_at_submit": concurrency_at_submit,
        "telemetry": telemetry,
    }


def collect_checkpoints(
    manifest: List[Dict[str, Any]],
    checkpoints: List[Dict[str, Any]],
    allow_missing: bool = False,
) -> Dict[str, Any]:
    expected = {row["request_identity"]: row for row in manifest}
    seen: Dict[str, Dict[str, Any]] = {}
    duplicates: List[str] = []
    unknown: List[str] = []
    malformed: List[str] = []
    for index, row in enumerate(checkpoints, 1):
        if not isinstance(row, dict):
            malformed.append(f"line={index}:not_object")
            continue
        identity = row.get("request_identity")
        if not isinstance(identity, str) or not identity:
            malformed.append(f"line={index}:identity")
            continue
        if identity not in expected:
            unknown.append(identity)
            continue
        if identity in seen:
            duplicates.append(identity)
            continue
        expected_row = expected[identity]
        required_match = (
            row.get("ordinal") == expected_row.get("ordinal")
            and row.get("project_message_id") == expected_row.get("project_message_id")
            and row.get("resource_message_id") == expected_row.get("resource_message_id")
            and row.get("request_body_sha256") == expected_row.get("request_body_sha256")
            and row.get("status") in ("success", "error")
        )
        if not required_match:
            malformed.append(f"line={index}:{identity}")
            continue
        seen[identity] = row
    missing = sorted(set(expected) - set(seen))
    if duplicates or unknown or malformed or (missing and not allow_missing):
        raise ValueError(
            "checkpoint不整合: "
            f"duplicate={len(duplicates)} unknown={len(unknown)} "
            f"malformed={len(malformed)} missing={len(missing)}"
        )
    ordered = [
        seen[row["request_identity"]]
        for row in sorted(manifest, key=lambda rec: rec["ordinal"])
        if row["request_identity"] in seen
    ]
    return {
        "ordered": ordered,
        "seen": seen,
        "duplicate": duplicates,
        "unknown": unknown,
        "malformed": malformed,
        "missing": missing,
    }


def run_scheduler(
    sample: List[Dict[str, Any]],
    project_skills_map: Dict[str, Any],
    skillsheet_map: Dict[str, Any],
    client: Any,
    checkpoint_path: Path,
    initial_concurrency: int,
    max_concurrency: int,
    existing_checkpoints: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[List[Dict[str, Any]], AdaptiveConcurrency, bool]:
    manifest = [manifest_record(item) for item in sample]
    existing = existing_checkpoints or []
    resume_state = collect_checkpoints(manifest, existing, allow_missing=True)
    completed: Dict[str, Dict[str, Any]] = dict(resume_state["seen"])
    if any(row.get("status") != "success" for row in completed.values()):
        raise ValueError("error checkpointを含むrunは安全のためresumeしません")

    by_project: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in sample:
        by_project[item["project_message_id"]].append(item)
    for rows in by_project.values():
        rows.sort(key=lambda item: item["original_ordinal"])

    warmed: Set[str] = set()
    ready: Deque[Dict[str, Any]] = deque()
    followers: Dict[str, Deque[Dict[str, Any]]] = {}
    for pid, rows in sorted(
        by_project.items(), key=lambda entry: entry[1][0]["original_ordinal"]
    ):
        leader = rows[0]
        leader_done = leader["request_identity"] in completed
        if leader_done:
            warmed.add(pid)
        elif leader["request_identity"] not in completed:
            ready.append(leader)
        followers[pid] = deque(
            row for row in rows[1:] if row["request_identity"] not in completed
        )
        if pid in warmed:
            ready.extend(followers[pid])
            followers[pid].clear()

    controller = AdaptiveConcurrency(initial_concurrency, max_concurrency)
    stop_scheduling = False
    original_call_llm = production.call_llm
    production.call_llm = client
    in_flight: Dict[Any, Dict[str, Any]] = {}
    try:
        with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            while ready or in_flight:
                while ready and not stop_scheduling and len(in_flight) < controller.current:
                    item = ready.popleft()
                    future = executor.submit(
                        _worker,
                        item,
                        project_skills_map,
                        skillsheet_map,
                        client,
                        len(in_flight) + 1,
                    )
                    in_flight[future] = item
                if not in_flight:
                    break
                done, _ = wait(set(in_flight), return_when=FIRST_COMPLETED)
                for future in done:
                    item = in_flight.pop(future)
                    try:
                        checkpoint = future.result()
                    except Exception as exc:
                        checkpoint = {
                            "ordinal": item["original_ordinal"],
                            "project_message_id": item["project_message_id"],
                            "resource_message_id": item["resource_message_id"],
                            "request_identity": item["request_identity"],
                            "request_body_sha256": item["request_body_sha256"],
                            "status": "error",
                            "result": None,
                            "error": production._make_error(
                                item["project_message_id"],
                                item["resource_message_id"],
                                "llm_call_error",
                                str(exc)[:1000],
                            ),
                            "worker_exception": f"{type(exc).__name__}: {exc}",
                            "started_at": "",
                            "completed_at": datetime.now().astimezone().isoformat(),
                            "concurrency_at_submit": 0,
                            "telemetry": {"api_failure": True},
                        }
                    append_jsonl(str(checkpoint_path), checkpoint)
                    completed[checkpoint["request_identity"]] = checkpoint
                    controller.observe(checkpoint)
                    pid = item["project_message_id"]
                    if item["is_project_warm_one"] and checkpoint["status"] == "success":
                        warmed.add(pid)
                        ready.extend(followers[pid])
                        followers[pid].clear()
                    telemetry = checkpoint.get("telemetry", {})
                    total_retries = sum(
                        row.get("telemetry", {}).get("retry_count", 0)
                        for row in completed.values()
                    )
                    retry_stop_limit = max(3, int(math.ceil(len(sample) * 0.10)))
                    if telemetry.get("api_failure") or checkpoint["status"] != "success":
                        stop_scheduling = True
                    elif total_retries >= retry_stop_limit:
                        stop_scheduling = True
    finally:
        production.call_llm = original_call_llm

    all_rows = list(existing) + [
        row for identity, row in completed.items()
        if identity not in {old.get("request_identity") for old in existing}
    ]
    return all_rows, controller, stop_scheduling


def _percentile(values: List[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _aggregate_usage(checkpoints: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    totals = {
        "input_tokens": 0,
        "cached_input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
    }
    for checkpoint in checkpoints:
        for attempt in checkpoint.get("telemetry", {}).get("attempts", []):
            usage = attempt.get("usage", {})
            for key in totals:
                totals[key] += int(usage.get(key, 0) or 0)
    totals["cache_rate"] = (
        totals["cached_input_tokens"] / totals["input_tokens"]
        if totals["input_tokens"]
        else 0.0
    )
    uncached = max(0, totals["input_tokens"] - totals["cached_input_tokens"])
    totals["estimated_cost_usd"] = (
        uncached * PRICE_PER_MILLION["uncached_input"]
        + totals["cached_input_tokens"] * PRICE_PER_MILLION["cached_input"]
        + totals["output_tokens"] * PRICE_PER_MILLION["output"]
    ) / 1_000_000
    return totals


def _load_baseline(sample_size: int) -> Dict[str, Any]:
    elapsed = 0.0
    processed = 0
    if BASELINE_TIMING.exists():
        text = BASELINE_TIMING.read_text(encoding="utf-8")
        elapsed_match = re.search(r"\((\d+(?:\.\d+)?)秒\)", text)
        count_match = re.search(r"処理件数:\s*(\d+)件", text)
        elapsed = float(elapsed_match.group(1)) if elapsed_match else 0.0
        processed = int(count_match.group(1)) if count_match else 0

    usage_files = sorted(BASELINE_USAGE_DIR.glob("llm_usage_*.jsonl"), key=lambda p: p.stat().st_mtime)
    usage_rows = list(read_jsonl(str(usage_files[-1]))) if usage_files else []
    calls = len({row.get("call_number") for row in usage_rows})
    usage = {
        key: sum(int(row.get(key, 0) or 0) for row in usage_rows)
        for key in ("input_tokens", "cached_input_tokens", "output_tokens", "total_tokens")
    }
    factor = sample_size / calls if calls else 0.0
    equivalent_usage = {key: value * factor for key, value in usage.items()}
    equivalent_usage["cache_rate"] = (
        usage["cached_input_tokens"] / usage["input_tokens"]
        if usage["input_tokens"]
        else 0.0
    )
    uncached = max(0.0, equivalent_usage["input_tokens"] - equivalent_usage["cached_input_tokens"])
    equivalent_usage["estimated_cost_usd"] = (
        uncached * PRICE_PER_MILLION["uncached_input"]
        + equivalent_usage["cached_input_tokens"] * PRICE_PER_MILLION["cached_input"]
        + equivalent_usage["output_tokens"] * PRICE_PER_MILLION["output"]
    ) / 1_000_000
    return {
        "source_elapsed_seconds": elapsed,
        "source_processed_count": processed,
        "source_api_calls": calls,
        "equivalent_elapsed_seconds": elapsed * factor if calls else 0.0,
        "equivalent_usage": equivalent_usage,
    }


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def snapshot_production_outputs() -> Dict[str, Any]:
    paths: List[Path] = []
    for directory in (STEP_DIR / "01_result", STEP_DIR / "99_execution_time"):
        if directory.exists():
            paths.extend(path for path in directory.rglob("*") if path.is_file())
    return {
        str(path.relative_to(STEP_DIR)): {
            "size": path.stat().st_size,
            "sha256": _file_digest(path),
        }
        for path in sorted(paths)
    }


def _safe_run_dir(run_id: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}", run_id):
        raise ValueError("run_idは英数字開始の80文字以内（英数字_.-のみ）")
    root = TEST_OUTPUT_ROOT.resolve()
    run_dir = (TEST_OUTPUT_ROOT / run_id).resolve()
    if root not in run_dir.parents:
        raise ValueError("test output root外のrun_idです")
    return run_dir


def _report_text(report: Dict[str, Any]) -> str:
    usage = report["usage"]
    timing = report["timing"]
    return "\n".join(
        [
            "【07-1 Speedup Test】",
            f"IMPLEMENTATION: {report['implementation']}",
            f"production changes: {report['production_changes']}",
            f"focused tests: {report.get('focused_tests', 'reported separately')}",
            f"live sample: {report['request_count']} requests",
            f"projects: {report['project_count']}",
            f"elapsed: {timing['elapsed_seconds']:.3f}s",
            f"baseline equivalent: {report['baseline']['equivalent_elapsed_seconds']:.3f}s",
            f"speedup: {timing['speedup']:.3f}x",
            f"peak concurrency: {timing['peak_concurrency']}",
            f"429: {report['rate_limit_429_count']}",
            f"retries: {report['retry_count']}",
            f"API failures: {report['api_failure_count']}",
            f"input tokens: {usage['input_tokens']}",
            f"cached input tokens: {usage['cached_input_tokens']}",
            f"cache rate: {usage['cache_rate'] * 100:.2f}%",
            f"output tokens: {usage['output_tokens']}",
            f"estimated cost: ${usage['estimated_cost_usd']:.6f}",
            f"duplicate: {report['integrity']['duplicate_count']}",
            f"missing: {report['integrity']['missing_count']}",
            f"production write: {report['production_write_count']}",
            f"NEXT SCALE: {report['next_scale']}",
            "",
        ]
    )


def build_report(
    manifest: List[Dict[str, Any]],
    ordered: List[Dict[str, Any]],
    elapsed: float,
    controller: AdaptiveConcurrency,
    production_changes: List[str],
    stopped_early: bool,
) -> Dict[str, Any]:
    usage = _aggregate_usage(ordered)
    baseline = _load_baseline(len(manifest))
    latencies = [
        float(row.get("telemetry", {}).get("latency_seconds", 0.0))
        for row in ordered
    ]
    retries = sum(row.get("telemetry", {}).get("retry_count", 0) for row in ordered)
    rate_429 = sum(
        row.get("telemetry", {}).get("rate_limit_429_count", 0) for row in ordered
    )
    api_failures = sum(bool(row.get("telemetry", {}).get("api_failure")) for row in ordered)
    errors = sum(row.get("status") != "success" for row in ordered)
    baseline_elapsed = baseline["equivalent_elapsed_seconds"]
    speedup = baseline_elapsed / elapsed if elapsed else 0.0
    cache_delta = usage["cache_rate"] - baseline["equivalent_usage"]["cache_rate"]
    cost_delta = usage["estimated_cost_usd"] - baseline["equivalent_usage"]["estimated_cost_usd"]
    stop_reasons = []
    if stopped_early:
        stop_reasons.append("scheduler stop condition")
    if errors:
        stop_reasons.append("result/API failure")
    if rate_429 >= 2:
        stop_reasons.append("continued 429")
    if retries >= max(3, int(math.ceil(len(manifest) * 0.10))):
        stop_reasons.append("many retries")
    if cache_delta < -0.10:
        stop_reasons.append("cache rate degraded >10pt")
    if speedup < 1.5:
        stop_reasons.append("speedup <1.5x")
    if production_changes:
        stop_reasons.append("production write")

    true_count = 0
    false_count = 0
    for row in ordered:
        result = row.get("result") or {}
        for field in ("required_skills", "optional_skills"):
            for skill in result.get(field, []):
                if skill.get("match") is True:
                    true_count += 1
                elif skill.get("match") is False:
                    false_count += 1

    completed_all = len(ordered) == len(manifest)
    return {
        "implementation": (
            "PASS" if not errors and not production_changes and completed_all else "FAIL"
        ),
        "production_changes": len(production_changes),
        "production_write_count": len(production_changes),
        "production_changed_paths": production_changes,
        "request_count": len(manifest),
        "completed_count": len(ordered),
        "project_count": len({row["project_message_id"] for row in manifest}),
        "success_count": len(ordered) - errors,
        "error_count": errors,
        "timing": {
            "elapsed_seconds": elapsed,
            "requests_per_second": len(ordered) / elapsed if elapsed else 0.0,
            "average_latency_seconds": statistics.mean(latencies) if latencies else 0.0,
            "p50_latency_seconds": _percentile(latencies, 0.50),
            "p95_latency_seconds": _percentile(latencies, 0.95),
            "peak_concurrency": max(
                [row.get("concurrency_at_submit", 0) for row in ordered] + [0]
            ),
            "concurrency_history": controller.history,
            "speedup": speedup,
        },
        "rate_limit_429_count": rate_429,
        "retry_count": retries,
        "api_failure_count": api_failures,
        "usage": usage,
        "pricing": {
            "model": production.LLM_MODEL,
            "usd_per_million_tokens": PRICE_PER_MILLION,
            "source": PRICE_SOURCE,
        },
        "baseline": baseline,
        "comparison": {
            "cache_rate_delta": cache_delta,
            "estimated_cost_delta_usd": cost_delta,
            "input_token_delta": usage["input_tokens"] - baseline["equivalent_usage"]["input_tokens"],
            "output_token_delta": usage["output_tokens"] - baseline["equivalent_usage"]["output_tokens"],
        },
        "quality": {
            "request_body_mismatch_count": 0,
            "production_validator_pass_count": len(ordered) - errors,
            "schema_error_count": errors,
            "match_true_count": true_count,
            "match_false_count": false_count,
            "match_true_rate": (
                true_count / (true_count + false_count)
                if true_count + false_count
                else 0.0
            ),
        },
        "integrity": {
            "duplicate_count": 0,
            "missing_count": len(manifest) - len(ordered),
            "unknown_count": 0,
            "malformed_count": 0,
        },
        "stop_reasons": stop_reasons,
        "next_scale": "STOP" if stop_reasons else "DISCUSS",
    }


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="07-1 test-only concurrent speed test")
    parser.add_argument("--sample-size", type=int, required=True)
    parser.add_argument("--allow-network", action="store_true")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--seed", default=DEFAULT_SAMPLE_SEED)
    parser.add_argument("--initial-concurrency", type=int, default=DEFAULT_INITIAL_CONCURRENCY)
    parser.add_argument("--max-concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY)
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    if not args.allow_network:
        raise SystemExit("OpenAI APIは--allow-network明示時のみ呼び出せます")
    if args.sample_size < 1 or args.sample_size > MAX_LIVE_SAMPLE_SIZE:
        raise SystemExit(f"--sample-sizeは1..{MAX_LIVE_SAMPLE_SIZE}のみ")
    AdaptiveConcurrency(args.initial_concurrency, args.max_concurrency)
    if args.resume and not args.run_id:
        raise SystemExit("--resumeには--run-idが必要です")

    run_id = args.run_id or datetime.now().astimezone().strftime("test_%Y%m%d_%H%M%S")
    run_dir = _safe_run_dir(run_id)
    if run_dir.exists() and not args.resume:
        raise SystemExit(f"run directoryが既に存在します（--resumeを使用）: {run_dir}")
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "manifest.jsonl"
    checkpoint_path = run_dir / "checkpoint.jsonl"

    before = snapshot_production_outputs()
    pairs, project_map, skillsheet_map = load_inputs()
    sample = deterministic_sample(
        pairs, project_map, skillsheet_map, args.sample_size, args.seed
    )
    manifest = [manifest_record(item) for item in sample]
    if manifest_path.exists():
        existing_manifest = list(read_jsonl(str(manifest_path)))
        if existing_manifest != manifest:
            raise SystemExit("resume manifestと現在input/request bodyが一致しません")
    else:
        write_jsonl(str(manifest_path), manifest)

    existing_checkpoints = (
        list(read_jsonl(str(checkpoint_path))) if checkpoint_path.exists() else []
    )
    logger.info(
        f"live test開始 sample={len(sample)} projects={len(set(row['project_message_id'] for row in sample))} "
        f"initial={args.initial_concurrency} max={args.max_concurrency} resume={len(existing_checkpoints)}"
    )
    client = DirectTestClient(_get_api_key())
    started = time.monotonic()
    checkpoints, controller, stopped_early = run_scheduler(
        sample,
        project_map,
        skillsheet_map,
        client,
        checkpoint_path,
        args.initial_concurrency,
        args.max_concurrency,
        existing_checkpoints,
    )
    elapsed = time.monotonic() - started
    collected = collect_checkpoints(manifest, checkpoints, allow_missing=stopped_early)
    ordered = collected["ordered"]
    write_jsonl(
        str(run_dir / "results.jsonl"),
        [row["result"] for row in ordered if row.get("status") == "success"],
    )
    write_jsonl(
        str(run_dir / "errors.jsonl"),
        [row["error"] for row in ordered if row.get("status") == "error"],
    )
    usage = _aggregate_usage(ordered)
    (run_dir / "usage.json").write_text(
        json.dumps(usage, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    after = snapshot_production_outputs()
    changed = sorted(
        path for path in set(before) | set(after) if before.get(path) != after.get(path)
    )
    report = build_report(
        manifest, ordered, elapsed, controller, changed, stopped_early
    )
    report["run_id"] = run_id
    report["sample_seed"] = args.seed
    report["created_at"] = datetime.now().astimezone().isoformat()
    timing = report["timing"]
    (run_dir / "timing.json").write_text(
        json.dumps(timing, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (run_dir / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (run_dir / "report.txt").write_text(_report_text(report), encoding="utf-8")
    logger.info(_report_text(report).strip())

    if collected["missing"] and not stopped_early:
        return 1
    return 0 if report["implementation"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())

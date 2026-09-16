#!/usr/bin/env python3
"""Canonical 08-5 entrypoint for legacy/Batch execution and publication."""

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Sequence


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[2]
STEP_DIR = Path(__file__).resolve().parents[1]
RESULT_DIR = STEP_DIR / "01_result"
EXECUTION_TIME_DIR = STEP_DIR / "99_execution_time"
CONTEXT_ROOT = RESULT_DIR / "_execution_context"
OWNER_PATH = CONTEXT_ROOT / "current_owner.json"
LOCK_PATH = CONTEXT_ROOT / ".context.lock"
COMMIT_MARKER = RESULT_DIR / "production_commit.json"
ENTRY_VERSION = "08-5-mode-switch-v1"
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
RUN_DATE_RE = re.compile(r"^[0-9]{8}$")

sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(TOOL_DIR))

import config as MODE_CONFIG  # noqa: E402
import high_score_required_skill_recheck as LEGACY  # noqa: E402
import high_score_required_skill_recheck_core as SHARED_CORE  # noqa: E402


class ExecutionContextError(RuntimeError):
    """The run no longer owns the immutable 08-5 execution context."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_write_json(path: Path, value: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as target:
            json.dump(value, target, ensure_ascii=False, indent=2, sort_keys=True)
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        os.replace(str(temporary), str(path))
    finally:
        if temporary.exists():
            temporary.unlink()


def _read_json_object(path: Path) -> Dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ExecutionContextError(f"JSON objectではありません: {path}")
    return value


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _validate_identity(run_id: str, run_date: str) -> None:
    if not RUN_ID_RE.fullmatch(run_id or ""):
        raise ExecutionContextError("RUN_ID contract不正")
    if not RUN_DATE_RE.fullmatch(run_date or ""):
        raise ExecutionContextError("RUN_DATEはYYYYMMDD必須")
    try:
        parsed = datetime.strptime(run_date, "%Y%m%d")
    except ValueError as error:
        raise ExecutionContextError("RUN_DATEが実在日ではありません") from error
    if parsed.strftime("%Y%m%d") != run_date:
        raise ExecutionContextError("RUN_DATE contract不正")


def _context_path(run_id: str, run_date: str) -> Path:
    identity_hash = hashlib.sha256(f"{run_date}\0{run_id}".encode("utf-8")).hexdigest()
    return CONTEXT_ROOT / run_date / f"{identity_hash}.json"


def _input_paths() -> Sequence[Path]:
    paths = [path for _, path in LEGACY.configured_input_score_files()]
    paths.extend(
        [LEGACY.INPUT_SKILLSHEETS, LEGACY.INPUT_CLEANED_EMAILS, LEGACY.CONCURRENT_CONFIG]
    )
    return paths


def input_fingerprint(limit: Optional[int] = None) -> str:
    entries = []
    for path in _input_paths():
        if not path.is_file():
            raise ExecutionContextError(f"08-5入力が見つかりません: {path}")
        try:
            fingerprint_path = str(path.relative_to(PROJECT_ROOT))
        except ValueError:
            # Focused/isolated execution may intentionally bind input outside
            # the workspace; the resolved path remains part of its identity.
            fingerprint_path = str(path.resolve())
        entries.append(
            {
                "path": fingerprint_path,
                "bytes": path.stat().st_size,
                "sha256": _file_sha256(path),
            }
        )
    return _canonical_sha256({"files": entries, "limit": limit})


def implementation_identity(mode: str) -> Dict[str, Any]:
    names = [
        "config.py",
        "run_high_score_required_skill_recheck.py",
        "high_score_required_skill_recheck.py",
        "high_score_required_skill_recheck_core.py",
    ]
    if mode == "batch":
        names.extend(
            [
                "high_score_required_skill_recheck_batch.py",
                "batch_aws_orchestration.py",
                "batch_minimal_safety_guard.py",
            ]
        )
    files = {name: _file_sha256(TOOL_DIR / name) for name in names}
    return {"version": ENTRY_VERSION, "sha256": _canonical_sha256(files)}


def _identity_fields(context: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: context.get(key)
        for key in (
            "run_id",
            "run_date",
            "execution_mode",
            "input_fingerprint",
            "implementation",
            "owner_token",
        )
    }


def prepare_execution_context(
    run_id: str,
    run_date: str,
    mode: str,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Create one immutable run context, or validate an owned same-run retry."""
    _validate_identity(run_id, run_date)
    fingerprint = input_fingerprint(limit)
    implementation = implementation_identity(mode)
    path = _context_path(run_id, run_date)
    CONTEXT_ROOT.mkdir(parents=True, exist_ok=True)
    with LOCK_PATH.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if path.exists():
            context = _read_json_object(path)
            expected = {
                "run_id": run_id,
                "run_date": run_date,
                "execution_mode": mode,
                "input_fingerprint": fingerprint,
                "implementation": implementation,
            }
            for key, value in expected.items():
                if context.get(key) != value:
                    raise ExecutionContextError(
                        f"同一runの08-5 context変更を拒否: field={key}"
                    )
            owner = _read_json_object(OWNER_PATH) if OWNER_PATH.exists() else {}
            if owner.get("owner_token") != context.get("owner_token"):
                raise ExecutionContextError(
                    "08-5 run所有権が後続runへ移動済みのため再開できません"
                )
            return context

        context = {
            "schema_version": 1,
            "run_id": run_id,
            "run_date": run_date,
            "execution_mode": mode,
            "input_fingerprint": fingerprint,
            "implementation": implementation,
            "owner_token": uuid.uuid4().hex,
            "artifacts_committed": False,
            "started_at": _utc_now(),
            "updated_at": _utc_now(),
        }
        _atomic_write_json(path, context)
        _atomic_write_json(OWNER_PATH, _identity_fields(context))
        return context


def require_execution_context(
    run_id: str,
    run_date: str,
    mode: str,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Validate a resume without creating/reclaiming run ownership."""
    _validate_identity(run_id, run_date)
    path = _context_path(run_id, run_date)
    if not path.exists():
        raise ExecutionContextError("08-5 run contextがないため再開できません")
    fingerprint = input_fingerprint(limit)
    implementation = implementation_identity(mode)
    with LOCK_PATH.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        context = _read_json_object(path)
        expected = {
            "run_id": run_id,
            "run_date": run_date,
            "execution_mode": mode,
            "input_fingerprint": fingerprint,
            "implementation": implementation,
        }
        for key, value in expected.items():
            if context.get(key) != value:
                raise ExecutionContextError(
                    f"08-5再開context不一致: field={key}"
                )
        owner = _read_json_object(OWNER_PATH) if OWNER_PATH.exists() else {}
        if owner.get("owner_token") != context.get("owner_token"):
            raise ExecutionContextError("08-5 publish所有者が現在runと一致しません")
        return context


def assert_publish_owner(context: Dict[str, Any]) -> None:
    """Fail closed immediately before a production publication."""
    path = _context_path(str(context.get("run_id") or ""), str(context.get("run_date") or ""))
    with LOCK_PATH.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        current = _read_json_object(path)
        owner = _read_json_object(OWNER_PATH)
        if _identity_fields(current) != _identity_fields(context):
            raise ExecutionContextError("保存済み08-5 run contextが変更されています")
        if owner.get("owner_token") != context.get("owner_token"):
            raise ExecutionContextError("08-5 publish所有者照合に失敗しました")


def mark_context_committed(context: Dict[str, Any]) -> Dict[str, Any]:
    path = _context_path(str(context["run_id"]), str(context["run_date"]))
    with LOCK_PATH.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        current = _read_json_object(path)
        owner = _read_json_object(OWNER_PATH)
        if owner.get("owner_token") != context.get("owner_token"):
            raise ExecutionContextError("成果物確定時の08-5 run所有者不一致")
        current["artifacts_committed"] = True
        current["committed_at"] = _utc_now()
        current["updated_at"] = current["committed_at"]
        _atomic_write_json(path, current)
        return current


def _artifact_paths() -> Dict[str, Path]:
    return {
        "all": LEGACY.OUTPUT_ALL,
        "confirmed": LEGACY.OUTPUT_CONFIRMED,
        "human_review": LEGACY.OUTPUT_HUMAN_REVIEW,
        "not_confirmed": LEGACY.OUTPUT_NOT_CONFIRMED,
        "error": LEGACY.OUTPUT_ERROR,
    }


def _validate_legacy_commit_marker(context: Dict[str, Any]) -> Dict[str, Any]:
    marker = _read_json_object(COMMIT_MARKER)
    expected = {
        "engine_version": ENTRY_VERSION,
        "execution_mode": "legacy",
        "run_id": context.get("run_id"),
        "run_date": context.get("run_date"),
        "input_fingerprint": context.get("input_fingerprint"),
        "owner_token": context.get("owner_token"),
    }
    for field, value in expected.items():
        if marker.get(field) != value:
            raise ExecutionContextError(f"legacy commit marker不一致: field={field}")
    artifacts = marker.get("artifacts")
    targets = _artifact_paths()
    if not isinstance(artifacts, dict) or set(artifacts) != set(targets):
        raise ExecutionContextError("legacy commit marker成果物集合不一致")
    for name, path in targets.items():
        metadata = artifacts.get(name)
        if not isinstance(metadata, dict) or metadata.get("filename") != path.name:
            raise ExecutionContextError(f"legacy commit marker metadata不正: {name}")
        if not path.is_file() or metadata.get("sha256") != _file_sha256(path):
            raise ExecutionContextError(f"legacy正式成果物hash不一致: {name}")
    return marker


def _publish_legacy(staged: Dict[str, Path], context: Dict[str, Any]) -> Dict[str, Any]:
    targets = _artifact_paths()
    rows = {
        name: list(LEGACY.read_jsonl(str(path))) for name, path in staged.items()
    }
    counts = SHARED_CORE.validate_output_contract(rows, LEGACY._skill_text)
    previous = {name: path.read_bytes() if path.exists() else None for name, path in targets.items()}
    previous_marker = COMMIT_MARKER.read_bytes() if COMMIT_MARKER.exists() else None
    replaced = []
    try:
        assert_publish_owner(context)
        if COMMIT_MARKER.exists():
            COMMIT_MARKER.unlink()
        for name in SHARED_CORE.OUTPUT_ARTIFACT_ORDER:
            target = targets[name]
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(
                f".{target.name}.{context['owner_token']}.publish.tmp"
            )
            shutil.copyfile(staged[name], temporary)
            with temporary.open("rb") as source:
                os.fsync(source.fileno())
            os.replace(str(temporary), str(target))
            replaced.append(name)
        marker = {
            "engine_version": ENTRY_VERSION,
            "execution_mode": "legacy",
            "run_id": context["run_id"],
            "run_date": context["run_date"],
            "input_fingerprint": context["input_fingerprint"],
            "owner_token": context["owner_token"],
            "committed_at": _utc_now(),
            "artifacts": {
                name: {
                    "filename": path.name,
                    "sha256": _file_sha256(path),
                    "bytes": path.stat().st_size,
                    "records": counts[name],
                }
                for name, path in targets.items()
            },
        }
        _atomic_write_json(COMMIT_MARKER, marker)
        _validate_legacy_commit_marker(context)
        return marker
    except Exception:
        for name, target in targets.items():
            previous_bytes = previous[name]
            if previous_bytes is None:
                if target.exists():
                    target.unlink()
            else:
                temporary = target.with_name(f".{target.name}.rollback.tmp")
                temporary.write_bytes(previous_bytes)
                os.replace(str(temporary), str(target))
        if previous_marker is None:
            if COMMIT_MARKER.exists():
                COMMIT_MARKER.unlink()
        else:
            temporary = COMMIT_MARKER.with_name(".production_commit.rollback.tmp")
            temporary.write_bytes(previous_marker)
            os.replace(str(temporary), str(COMMIT_MARKER))
        raise
    finally:
        for name in replaced:
            temporary = targets[name].with_name(
                f".{targets[name].name}.{context['owner_token']}.publish.tmp"
            )
            if temporary.exists():
                temporary.unlink()


def _suspend_for_batch_wait() -> int:
    contract_text = "SUSPENDED:BATCH_WAIT:08-5_BATCH_WAIT\n"
    contract_value = os.environ.get("PIPELINE_SUSPEND_CONTRACT_FILE")
    current_step_value = os.environ.get("PIPELINE_CURRENT_STEP_FILE")
    if not contract_value or not current_step_value:
        raise ExecutionContextError(
            "Batch Phase Aにはmanaged suspension contract pathsが必須です"
        )
    contract = Path(contract_value)
    current_step = Path(current_step_value)
    for path, payload in (
        (current_step, "08-5_BATCH_WAIT\n"),
        (contract, contract_text),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        temporary.write_text(payload, encoding="utf-8")
        os.replace(str(temporary), str(path))
    raw_exit_code = os.environ.get("PIPELINE_SUSPEND_EXIT_CODE", "85")
    try:
        exit_code = int(raw_exit_code)
    except ValueError as error:
        raise ExecutionContextError("PIPELINE_SUSPEND_EXIT_CODEが整数ではありません") from error
    if exit_code <= 0 or exit_code > 255:
        raise ExecutionContextError("PIPELINE_SUSPEND_EXIT_CODE範囲不正")
    return exit_code


def run_phase_a(run_id: str, run_date: str, mode: str, limit: Optional[int]) -> int:
    if mode == "batch" and limit is not None:
        raise ExecutionContextError("batch modeでは--limitを使用できません")
    if mode == "batch":
        # Validate the ability to suspend before any Batch/File API call.
        if not os.environ.get("PIPELINE_SUSPEND_CONTRACT_FILE") or not os.environ.get(
            "PIPELINE_CURRENT_STEP_FILE"
        ):
            raise ExecutionContextError(
                "Batch Phase Aにはmanaged suspension contract pathsが必須です"
            )
    context = prepare_execution_context(run_id, run_date, mode, limit)
    if context.get("artifacts_committed") is True:
        assert_publish_owner(context)
        if mode == "legacy":
            _validate_legacy_commit_marker(context)
        return 0

    if mode == "legacy":
        runtime_id = hashlib.sha256(
            f"{run_date}\0{run_id}".encode("utf-8")
        ).hexdigest()
        stage_dir = RESULT_DIR / "_legacy_runtime" / runtime_id / "stage"
        if stage_dir.exists():
            shutil.rmtree(stage_dir)
        started = time.time()
        result = LEGACY.run_legacy_to_stage(stage_dir, limit=limit)
        marker = _publish_legacy(result["staged"], context)
        mark_context_committed(context)
        from common.file_utils import write_execution_time

        write_execution_time(
            str(EXECUTION_TIME_DIR),
            "high_score_required_skill_recheck",
            time.time() - started,
            int(result["processed_count"]),
        )
        print(
            json.dumps(
                {
                    "execution_mode": mode,
                    "run_id": run_id,
                    "run_date": run_date,
                    "processed_count": result["processed_count"],
                    "error_count": result["error_count"],
                    "skipped_no_match_count": result["skipped_no_match_count"],
                    "artifacts": marker["artifacts"],
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0

    import batch_aws_orchestration as batch_orchestration

    result = batch_orchestration.phase_a(run_id, run_date)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return _suspend_for_batch_wait()


def run_phase_b(run_id: str, run_date: str, mode: str) -> int:
    if mode != "batch":
        raise ExecutionContextError("legacy modeにBatch Phase Bは存在しません")
    context = require_execution_context(run_id, run_date, mode)
    import batch_aws_orchestration as batch_orchestration

    result = batch_orchestration.phase_b(
        run_id,
        run_date,
        publish_owner_check=lambda: assert_publish_owner(context),
    )
    mark_context_committed(context)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("phase-a", "phase-b"), default="phase-a")
    parser.add_argument("--pipeline-run-id")
    parser.add_argument("--run-date")
    parser.add_argument("--limit", type=int)
    return parser


def main() -> int:
    args = _parser().parse_args()
    run_date = args.run_date or os.environ.get("RUN_DATE") or datetime.now().strftime("%Y%m%d")
    run_id = args.pipeline_run_id or os.environ.get("RUN_ID") or f"standalone-{run_date}"
    try:
        mode = MODE_CONFIG.resolve_execution_mode()
        if args.phase == "phase-b":
            if args.limit is not None:
                raise ExecutionContextError("Phase Bでは--limitを使用できません")
            return run_phase_b(run_id, run_date, mode)
        return run_phase_a(run_id, run_date, mode, args.limit)
    except Exception as error:
        print(f"ERROR: {type(error).__name__}: {error}", file=sys.stderr)
        return 2 if isinstance(error, (MODE_CONFIG.ExecutionModeError, ExecutionContextError)) else 1


if __name__ == "__main__":
    sys.exit(main())

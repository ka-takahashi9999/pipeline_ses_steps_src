#!/usr/bin/env python3
"""Bounded cleanup for explicitly managed local Pipeline run artifacts.

Dry-run is the default.  Only explicit generated-artifact categories are
eligible; source, dictionaries, tests, current formal outputs and active runs
are always outside this tool's deletion scope.
"""

import argparse
import datetime
import gzip
import hashlib
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANAGED_ROOT = ROOT / "00_pipeline/01_result/managed"
DEFAULT_S3_DAILY_ROOT = ROOT / "01-1_fetch_gmail/02_confirm/s3_daily"
DEFAULT_CHECKPOINT_ROOT = (
    ROOT / "07-1_requirement_skill_ai_matching/01_result/concurrent_checkpoints"
)
DEFAULT_USAGE_DIRS = (
    ROOT / "07-1_requirement_skill_ai_matching/99_execution_time",
    ROOT / "08-5_high_score_required_skill_recheck/99_execution_time",
)
DEFAULT_RUNTIME_ROOT = ROOT / "08-5_high_score_required_skill_recheck/01_result"
DEFAULT_SCRIPT_LOG = ROOT / "00_pipeline/01_result/pipeline_script_exec.log"
DEFAULT_STATUS_HISTORY = ROOT.parent / "pipeline-improvement-guide/STATUS.md"
DEFAULT_STALE_GENERATION_ROOTS = (
    ROOT / "00_pipeline/01_result/manual_recovery",
    ROOT / "80-9_portal_s3_sync/01_result/manual_recovery",
    ROOT / "00_pipeline/10_assistance_tool/01_result/oneoff_resume",
    ROOT / "00_pipeline/10_assistance_tool/01_result/pair_quality_analysis",
    ROOT / "00_pipeline/10_assistance_tool/01_result/quality_dictionary_analysis",
    ROOT / "00_pipeline/10_assistance_tool/02_confirm/pair_quality_analysis",
    ROOT / "00_pipeline/10_assistance_tool/02_confirm/quality_dictionary_analysis",
    ROOT / "00_pipeline/10_assistance_tool/suggest_result",
)
RUN_DATE_RE = re.compile(r"^[0-9]{8}$")
DAILY_DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
USAGE_RE = re.compile(r"^llm_usage_([A-Za-z0-9][A-Za-z0-9_.-]{0,127})\.jsonl$")
TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "ABORTED", "TIMED_OUT", "CANCELLED"}
RUNTIME_TERMINAL_STATUSES = TERMINAL_STATUSES | {"COMMITTED", "SAFE_STOPPED"}
EXPECTED_LOG_BUCKET = "technoverse"
EXPECTED_LOG_PREFIX = "pipeline_ses_steps/pipeline-logs/"
EXPECTED_SOURCE_BUCKET = "technoverse-ses-mail"
EXPECTED_SOURCE_PREFIX = "raw/"
STALE_FILE_PATTERNS = ("error_*.log", "nohup*.log")
PIPELINE_LEGACY_LOG_PATTERNS = ("pipeline_script_exec_*.log", "nohup*.log")
DEFAULT_STALE_DAYS = 30
DEFAULT_SCRIPT_LOG_MAX_BYTES = 256 * 1024 * 1024
DEFAULT_SCRIPT_LOG_KEEP = 3
DEFAULT_ACTIVE_LOG_GRACE_SECONDS = 600


def _tree_size(path):
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _tree_mtime_ns(path):
    timestamps = [path.stat().st_mtime_ns]
    timestamps.extend(item.stat().st_mtime_ns for item in path.rglob("*"))
    return max(timestamps)


def _safe_regular_tree(path):
    if path.is_symlink() or not path.is_dir():
        return False
    for item in path.rglob("*"):
        if item.is_symlink():
            return False
        if item.is_file() and item.stat().st_nlink != 1:
            return False
        if not item.is_file() and not item.is_dir():
            return False
    return True


def _remove_tree_files_first(path):
    if not _safe_regular_tree(path):
        raise ValueError("unsafe tree shape: {}".format(path))
    items = sorted(path.rglob("*"), key=lambda item: len(item.parts), reverse=True)
    for item in items:
        if item.is_file():
            item.unlink()
        else:
            item.rmdir()
    path.rmdir()


def _safe_regular_file(path):
    return (
        not path.is_symlink()
        and path.is_file()
        and path.stat().st_nlink == 1
    )


def _read_json_object(path):
    if not _safe_regular_file(path):
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _today(value=None):
    if value is None:
        return datetime.date.today()
    if isinstance(value, datetime.datetime):
        return value.date()
    return value


def _parse_run_date(value):
    for pattern in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(value, pattern).date()
        except (TypeError, ValueError):
            continue
    return None


def _is_expired_date(value, stale_days, today=None):
    parsed = _parse_run_date(value)
    return parsed is not None and (_today(today) - parsed).days >= stale_days


def _is_expired_mtime(path, stale_days, now=None):
    now_timestamp = time.time() if now is None else now
    return now_timestamp - (path.stat().st_mtime_ns / 1_000_000_000) >= stale_days * 86400


def _contains_active_name(path, active_names):
    active_names = set(active_names or ())
    if not active_names:
        return False
    if path.name in active_names:
        return True
    if path.is_dir():
        return any(item.name in active_names for item in path.rglob("*"))
    return False


def _append_history(path, category, identity, local_state, final_state):
    if path is None:
        return
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = (
        "- {date} / {identity} / category={category} / local_state={local_state} "
        "/ final_state={final_state}\n"
    ).format(
        date=datetime.date.today().isoformat(),
        identity=identity,
        category=category,
        local_state=local_state,
        final_state=final_state,
    )
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)
        handle.flush()
        os.fsync(handle.fileno())


def _generation_split(records, keep, active_names=None):
    active_names = set(active_names or ())
    ordered = sorted(records, key=lambda item: (item[0], str(item[1])), reverse=True)
    protected = list(ordered[:keep])
    candidates = []
    for item in ordered[keep:]:
        if item[1].name in active_names:
            protected.append(item)
        else:
            candidates.append(item)
    return protected, candidates


def _load_status_document(run_dir):
    status_path = run_dir / "status.json"
    if status_path.is_symlink() or not status_path.is_file() or status_path.stat().st_nlink != 1:
        return None
    try:
        document = json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if document.get("run_date") != run_dir.parent.name or document.get("run_id") != run_dir.name:
        return None
    return document


def _load_terminal_status(run_dir):
    document = _load_status_document(run_dir)
    if document is None or document.get("status") not in TERMINAL_STATUSES:
        return None
    return document


def _parse_log_uri(uri):
    parsed = urlparse(uri or "")
    key = parsed.path.lstrip("/")
    if (
        parsed.scheme != "s3"
        or parsed.netloc != EXPECTED_LOG_BUCKET
        or not key.startswith(EXPECTED_LOG_PREFIX)
        or not key.endswith("/pipeline.log")
    ):
        return None
    return parsed.netloc, key


def managed_candidates(
    managed_root,
    keep,
    terminal_confirmed_run_ids=None,
    stale_days=DEFAULT_STALE_DAYS,
    active_run_ids=None,
    today=None,
):
    terminal_confirmed_run_ids = set(terminal_confirmed_run_ids or ())
    active_run_ids = set(active_run_ids or ())
    terminal_runs = []
    stale_candidates = []
    nonterminal_protected = []
    if not managed_root.is_dir():
        return [], [], []
    for date_dir in managed_root.iterdir():
        if not date_dir.is_dir() or date_dir.is_symlink() or not RUN_DATE_RE.match(date_dir.name):
            continue
        for run_dir in date_dir.iterdir():
            log_path = run_dir / "pipeline.log"
            if run_dir.is_symlink() or not run_dir.is_dir() or not log_path.is_file():
                continue
            status = _load_status_document(run_dir)
            if status is None or log_path.is_symlink() or log_path.stat().st_nlink != 1:
                continue
            item = {
                "mtime": log_path.stat().st_mtime,
                "run_dir": run_dir,
                "log_path": log_path,
                "status": status,
                "externally_confirmed": run_dir.name in terminal_confirmed_run_ids,
            }
            if run_dir.name in active_run_ids:
                nonterminal_protected.append(item)
            elif status.get("status") in TERMINAL_STATUSES or item["externally_confirmed"]:
                terminal_runs.append(item)
            elif _is_expired_date(date_dir.name, stale_days, today):
                stale_candidates.append(item)
            else:
                nonterminal_protected.append(item)
    terminal_runs.sort(key=lambda item: (item["mtime"], str(item["run_dir"])), reverse=True)
    protected = terminal_runs[:keep] + nonterminal_protected
    candidates = [(item, "terminal_generation_limit") for item in terminal_runs[keep:]]
    candidates.extend((item, "stale_nonterminal_expired") for item in stale_candidates)
    return protected, candidates, []


def _remote_log_matches(s3_client, log_path, status):
    location = _parse_log_uri(status.get("log_s3_uri"))
    if location is None:
        return False, "invalid_or_missing_log_s3_uri"
    bucket, key = location
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
    except (BotoCoreError, ClientError) as error:
        return False, "s3_head_failed:{}".format(type(error).__name__)
    if response.get("ContentLength") != log_path.stat().st_size:
        return False, "s3_size_mismatch"
    return True, "s3_exact_size_confirmed"


def cleanup_managed_logs(
    managed_root,
    keep,
    apply,
    s3_client,
    terminal_confirmed_run_ids=None,
    stale_days=DEFAULT_STALE_DAYS,
    active_run_ids=None,
    today=None,
    history_path=None,
):
    protected, candidates, skipped = managed_candidates(
        managed_root,
        keep,
        terminal_confirmed_run_ids,
        stale_days,
        active_run_ids,
        today,
    )
    deleted = []
    for item, candidate_reason in candidates:
        run_dir = item["run_dir"]
        log_path = item["log_path"]
        status = item["status"]
        size = log_path.stat().st_size
        if candidate_reason == "terminal_generation_limit":
            matched, remote_reason = _remote_log_matches(s3_client, log_path, status)
            if not matched:
                if _is_expired_date(run_dir.parent.name, stale_days, today):
                    candidate_reason = "stale_terminal_unconfirmed_expired"
                    reason = "{}:{}".format(candidate_reason, remote_reason)
                else:
                    skipped.append((run_dir, size, remote_reason))
                    continue
            else:
                reason = remote_reason
                if item["externally_confirmed"]:
                    reason += ":terminal_confirmed"
        else:
            reason = candidate_reason
        if apply:
            if candidate_reason in {"stale_nonterminal_expired", "stale_terminal_unconfirmed_expired"}:
                _append_history(
                    history_path,
                    "managed_log",
                    "RUN_ID={}".format(run_dir.name),
                    str(status.get("status") or "UNKNOWN"),
                    "AWS未確認・{}日expire確定（削除開始）".format(stale_days),
                )
            log_path.unlink()
            if candidate_reason in {"stale_nonterminal_expired", "stale_terminal_unconfirmed_expired"}:
                _append_history(
                    history_path,
                    "managed_log",
                    "RUN_ID={}".format(run_dir.name),
                    str(status.get("status") or "UNKNOWN"),
                    "AWS未確認・{}日expire・retentionでローカル本体削除済み".format(stale_days),
                )
        deleted.append((run_dir, size, reason))
    return protected, deleted, skipped


def cleanup_managed_metadata(
    managed_root,
    apply,
    terminal_confirmed_run_ids=None,
    active_run_ids=None,
    stale_days=DEFAULT_STALE_DAYS,
    today=None,
    history_path=None,
):
    terminal_confirmed_run_ids = set(terminal_confirmed_run_ids or ())
    active_run_ids = set(active_run_ids or ())
    protected = []
    deleted = []
    skipped = []
    candidates = []
    allowed_names = {"status.json", "current_step", "suspend_contract"}
    if not managed_root.is_dir():
        return protected, deleted, skipped
    for status_path in sorted(managed_root.glob("*/*/status.json")):
        run_dir = status_path.parent
        if (run_dir / "pipeline.log").exists():
            protected.append((run_dir.stat().st_mtime_ns, run_dir))
            continue
        document = _load_status_document(run_dir)
        size = _tree_size(run_dir)
        if document is None:
            skipped.append((run_dir, size, "invalid_status_metadata"))
            continue
        if run_dir.name in active_run_ids:
            protected.append((run_dir.stat().st_mtime_ns, run_dir))
            continue
        unknown_names = {path.name for path in run_dir.iterdir()} - allowed_names
        if unknown_names or not _safe_regular_tree(run_dir):
            skipped.append((run_dir, size, "unexpected_or_unsafe_metadata_files"))
            continue
        status = document.get("status")
        confirmed = run_dir.name in terminal_confirmed_run_ids
        if status not in TERMINAL_STATUSES and not confirmed:
            if not _is_expired_date(run_dir.parent.name, stale_days, today):
                protected.append((run_dir.stat().st_mtime_ns, run_dir))
                continue
            reason = "stale_nonterminal_metadata_expired"
        else:
            reason = "managed_metadata_after_log_retention"
        candidates.append((run_dir, size, status or "UNKNOWN", reason))
    if apply:
        for run_dir, _, status, reason in candidates:
            _append_history(
                history_path,
                "managed_run_metadata",
                "RUN_DATE={} RUN_ID={}".format(run_dir.parent.name, run_dir.name),
                str(status),
                "{}・削除開始".format(reason),
            )
    for run_dir, size, status, reason in candidates:
        if apply:
            parent = run_dir.parent
            _remove_tree_files_first(run_dir)
            try:
                parent.rmdir()
            except OSError:
                pass
        deleted.append((run_dir, size, "{}:{}".format(reason, status)))
    if apply and candidates:
        _append_history(
            history_path,
            "managed_run_metadata",
            "count={}".format(len(candidates)),
            "mixed",
            "retentionでローカルrun metadata削除済み",
        )
    return protected, deleted, skipped


def _manifest_is_regenerable(pipeline_dir):
    manifest = pipeline_dir / "selection_manifest.jsonl"
    if manifest.is_symlink() or not manifest.is_file() or manifest.stat().st_nlink != 1:
        return False, "missing_or_unsafe_manifest"
    count = 0
    try:
        with manifest.open(encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                record = json.loads(line)
                if record.get("bucket") != EXPECTED_SOURCE_BUCKET:
                    return False, "unexpected_source_bucket"
                if not str(record.get("key", "")).startswith(EXPECTED_SOURCE_PREFIX):
                    return False, "unexpected_source_prefix"
                if record.get("status") not in {"success", "needs_review"}:
                    return False, "nonterminal_manifest_record"
                count += 1
    except (OSError, ValueError):
        return False, "invalid_manifest"
    if count == 0:
        return False, "empty_manifest"
    if not _safe_regular_tree(pipeline_dir):
        return False, "unsafe_tree_shape"
    return True, "canonical_s3_source_manifest"


def daily_candidates(s3_daily_root, keep):
    runs = []
    if not s3_daily_root.is_dir():
        return [], []
    for date_dir in s3_daily_root.iterdir():
        if not date_dir.is_dir() or date_dir.is_symlink() or not DAILY_DATE_RE.match(date_dir.name):
            continue
        pipeline_dir = date_dir / "pipeline"
        if pipeline_dir.is_dir() and not pipeline_dir.is_symlink():
            runs.append((date_dir.name, pipeline_dir))
    runs.sort(key=lambda item: item[0], reverse=True)
    return runs[:keep], runs[keep:]


def cleanup_s3_daily(s3_daily_root, keep, apply):
    protected, candidates = daily_candidates(s3_daily_root, keep)
    deleted = []
    skipped = []
    for run_date, pipeline_dir in candidates:
        valid, reason = _manifest_is_regenerable(pipeline_dir)
        size = _tree_size(pipeline_dir)
        if not valid:
            skipped.append((pipeline_dir, size, reason))
            continue
        if apply:
            _remove_tree_files_first(pipeline_dir)
        deleted.append((pipeline_dir, size, reason))
    return protected, deleted, skipped


def cleanup_nonterminal_s3_daily(
    s3_daily_root,
    stale_days,
    apply,
    active_run_dates=None,
    today=None,
    history_path=None,
):
    active_run_dates = set(active_run_dates or ())
    protected = []
    deleted = []
    skipped = []
    if not s3_daily_root.is_dir():
        return protected, deleted, skipped
    for date_dir in sorted(s3_daily_root.iterdir()):
        if not date_dir.is_dir() or date_dir.is_symlink() or not DAILY_DATE_RE.match(date_dir.name):
            continue
        pipeline_dir = date_dir / "pipeline"
        if not pipeline_dir.is_dir() or pipeline_dir.is_symlink():
            continue
        valid, invalid_reason = _manifest_is_regenerable(pipeline_dir)
        if valid:
            continue
        size = _tree_size(pipeline_dir)
        compact_date = date_dir.name.replace("-", "")
        if date_dir.name in active_run_dates or compact_date in active_run_dates:
            protected.append((date_dir.name, pipeline_dir))
            continue
        if not _is_expired_date(date_dir.name, stale_days, today):
            protected.append((date_dir.name, pipeline_dir))
            continue
        if not _safe_regular_tree(pipeline_dir):
            skipped.append((pipeline_dir, size, "unsafe_tree_shape"))
            continue
        if apply:
            _append_history(
                history_path,
                "nonterminal_s3_daily",
                "RUN_DATE={}".format(date_dir.name),
                invalid_reason,
                "{}日expire確定（削除開始）".format(stale_days),
            )
            _remove_tree_files_first(pipeline_dir)
            _append_history(
                history_path,
                "nonterminal_s3_daily",
                "RUN_DATE={}".format(date_dir.name),
                invalid_reason,
                "{}日expire・retentionでローカル本体削除済み".format(stale_days),
            )
        deleted.append((pipeline_dir, size, "nonterminal_s3_daily_expired:{}".format(invalid_reason)))
    return protected, deleted, skipped


def named_validation_candidates(s3_daily_root, keep, active_names=None):
    records = []
    if not s3_daily_root.is_dir():
        return [], []
    for date_dir in s3_daily_root.iterdir():
        if not date_dir.is_dir() or date_dir.is_symlink() or not DAILY_DATE_RE.match(date_dir.name):
            continue
        for candidate in date_dir.iterdir():
            if candidate.name == "pipeline" or candidate.is_symlink() or not candidate.is_dir():
                continue
            records.append((_tree_mtime_ns(candidate), candidate))
    return _generation_split(records, keep, active_names)


def cleanup_named_validations(s3_daily_root, keep, apply, active_names=None):
    protected, candidates = named_validation_candidates(s3_daily_root, keep, active_names)
    deleted = []
    skipped = []
    for _, path in candidates:
        size = _tree_size(path)
        if not _safe_regular_tree(path):
            skipped.append((path, size, "unsafe_tree_shape"))
            continue
        if apply:
            _remove_tree_files_first(path)
        deleted.append((path, size, "named_validation_generation_limit"))
    return protected, deleted, skipped


def checkpoint_candidates(checkpoint_root, keep, active_run_ids=None):
    records = []
    if not checkpoint_root.is_dir():
        return [], []
    for run_dir in checkpoint_root.iterdir():
        if run_dir.is_symlink() or not run_dir.is_dir():
            continue
        records.append((_tree_mtime_ns(run_dir), run_dir))
    return _generation_split(records, keep, active_run_ids)


def cleanup_checkpoints(checkpoint_root, keep, apply, active_run_ids=None):
    protected, candidates = checkpoint_candidates(checkpoint_root, keep, active_run_ids)
    deleted = []
    skipped = []
    for _, path in candidates:
        size = _tree_size(path)
        if not _safe_regular_tree(path):
            skipped.append((path, size, "unsafe_tree_shape"))
            continue
        if apply:
            _remove_tree_files_first(path)
        deleted.append((path, size, "checkpoint_run_limit"))
    return protected, deleted, skipped


def usage_candidates(usage_dir, keep, active_run_ids=None):
    active_run_ids = set(active_run_ids or ())
    records = []
    if not usage_dir.is_dir():
        return [], []
    for path in usage_dir.iterdir():
        match = USAGE_RE.match(path.name)
        if match and _safe_regular_file(path):
            records.append((path.stat().st_mtime_ns, path, match.group(1)))
    records.sort(key=lambda item: (item[0], str(item[1])), reverse=True)
    protected = list(records[:keep])
    candidates = []
    for item in records[keep:]:
        if item[2] in active_run_ids:
            protected.append(item)
        else:
            candidates.append(item)
    return protected, candidates


def cleanup_usage_logs(usage_dir, keep, apply, active_run_ids=None):
    protected, candidates = usage_candidates(usage_dir, keep, active_run_ids)
    deleted = []
    skipped = []
    for _, path, run_id in candidates:
        companion = path.with_suffix(".counter")
        paths = [path]
        if companion.exists():
            paths.append(companion)
        if not all(_safe_regular_file(item) for item in paths):
            skipped.append((path, 0, "unsafe_usage_file_or_counter"))
            continue
        size = sum(item.stat().st_size for item in paths)
        if apply:
            for item in paths:
                item.unlink()
        deleted.append((path, size, "llm_usage_run_limit:{}".format(run_id)))
    return protected, deleted, skipped


def _load_pipeline_statuses(managed_root):
    statuses = {}
    if not managed_root.is_dir():
        return statuses
    for status_path in managed_root.glob("*/*/status.json"):
        document = _read_json_object(status_path)
        if document is None:
            continue
        run_date = document.get("run_date")
        run_id = document.get("run_id")
        if (
            not isinstance(run_date, str)
            or not RUN_DATE_RE.match(run_date)
            or not isinstance(run_id, str)
            or status_path.parent.name != run_id
            or status_path.parent.parent.name != run_date
        ):
            continue
        statuses[(run_date, run_id)] = (document, status_path.stat().st_mtime_ns)
    return statuses


def _legacy_runtime_name(run_date, run_id):
    return hashlib.sha256("{}\0{}".format(run_date, run_id).encode("utf-8")).hexdigest()


def _batch_runtime_name(run_id):
    return "p" + hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:23]


def _add_runtime_artifact(
    groups, run_date, run_id, path, mtime_ns, runtime_terminal_status=None
):
    key = (run_date, run_id)
    group = groups.setdefault(
        key,
        {"run_date": run_date, "run_id": run_id, "paths": [], "mtime_ns": 0},
    )
    if path not in group["paths"]:
        group["paths"].append(path)
    group["mtime_ns"] = max(group["mtime_ns"], mtime_ns)
    if runtime_terminal_status in RUNTIME_TERMINAL_STATUSES:
        group["runtime_terminal_status"] = runtime_terminal_status


def _runtime_groups(runtime_root):
    groups = {}
    context_root = runtime_root / "_execution_context"
    legacy_root = runtime_root / "_legacy_runtime"
    batch_root = runtime_root / "_batch_runtime"

    if context_root.is_dir():
        for context_path in context_root.glob("*/*.json"):
            document = _read_json_object(context_path)
            if document is None:
                continue
            run_date = document.get("run_date")
            run_id = document.get("run_id")
            if (
                not isinstance(run_date, str)
                or not RUN_DATE_RE.match(run_date)
                or context_path.parent.name != run_date
                or not isinstance(run_id, str)
            ):
                continue
            expected_name = _legacy_runtime_name(run_date, run_id)
            if context_path.stem != expected_name or not HASH_RE.match(context_path.stem):
                continue
            _add_runtime_artifact(
                groups,
                run_date,
                run_id,
                context_path,
                context_path.stat().st_mtime_ns,
                "COMMITTED" if document.get("artifacts_committed") is True else None,
            )
            legacy_dir = legacy_root / expected_name
            if legacy_dir.is_dir() and not legacy_dir.is_symlink():
                _add_runtime_artifact(
                    groups, run_date, run_id, legacy_dir, _tree_mtime_ns(legacy_dir)
                )

    if batch_root.is_dir():
        for run_dir in batch_root.iterdir():
            if run_dir.is_symlink() or not run_dir.is_dir():
                continue
            document = _read_json_object(run_dir / "batch_state.json")
            if document is None:
                continue
            run_date = document.get("run_date")
            run_id = document.get("pipeline_run_id")
            if (
                not isinstance(run_date, str)
                or not RUN_DATE_RE.match(run_date)
                or not isinstance(run_id, str)
                or run_dir.name != _batch_runtime_name(run_id)
            ):
                continue
            _add_runtime_artifact(
                groups,
                run_date,
                run_id,
                run_dir,
                _tree_mtime_ns(run_dir),
                (
                    str(document.get("state")).upper()
                    if str(document.get("state")).upper() in RUNTIME_TERMINAL_STATUSES
                    else None
                ),
            )
    return groups


def runtime_candidates(
    runtime_root,
    managed_root,
    keep,
    active_run_ids=None,
    terminal_confirmed_run_ids=None,
    stale_days=DEFAULT_STALE_DAYS,
    today=None,
):
    active_run_ids = set(active_run_ids or ())
    terminal_confirmed_run_ids = set(terminal_confirmed_run_ids or ())
    statuses = _load_pipeline_statuses(managed_root)
    groups = _runtime_groups(runtime_root)
    current_owner = _read_json_object(runtime_root / "_execution_context/current_owner.json")
    owner_identity = None
    if current_owner is not None:
        owner_identity = (current_owner.get("run_date"), current_owner.get("run_id"))

    terminal = []
    skipped = []
    for identity, group in groups.items():
        status_entry = statuses.get(identity)
        status = status_entry[0].get("status") if status_entry else group.get("runtime_terminal_status")
        size = sum(
            _tree_size(path) if path.is_dir() else path.stat().st_size
            for path in group["paths"]
        )
        group["size"] = size
        externally_confirmed = group["run_id"] in terminal_confirmed_run_ids
        if status not in RUNTIME_TERMINAL_STATUSES and not externally_confirmed:
            if group["run_id"] in active_run_ids or identity == owner_identity:
                skipped.append((group["paths"][0], size, "active_nonterminal_status:{}".format(status)))
            elif _is_expired_date(group["run_date"], stale_days, today):
                group["status"] = status or "UNKNOWN"
                group["candidate_reason"] = "stale_runtime_expired"
                terminal.append((group["mtime_ns"], group))
            else:
                skipped.append((group["paths"][0], size, "nonterminal_or_unknown_status:{}".format(status)))
            continue
        group["status"] = status if status in RUNTIME_TERMINAL_STATUSES else "TERMINAL_CONFIRMED"
        group["candidate_reason"] = "terminal_runtime_limit"
        status_mtime = status_entry[1] if status_entry else group["mtime_ns"]
        terminal.append((max(group["mtime_ns"], status_mtime), group))

    terminal.sort(key=lambda item: (item[0], item[1]["run_date"], item[1]["run_id"]), reverse=True)
    normal_terminal = [item for item in terminal if item[1]["candidate_reason"] == "terminal_runtime_limit"]
    stale_runtime = [item for item in terminal if item[1]["candidate_reason"] == "stale_runtime_expired"]
    protected = list(normal_terminal[:keep])
    candidates = []
    for item in normal_terminal[keep:] + stale_runtime:
        identity = (item[1]["run_date"], item[1]["run_id"])
        if item[1]["run_id"] in active_run_ids or identity == owner_identity:
            protected.append(item)
        else:
            candidates.append(item)
    return protected, candidates, skipped


def _runtime_group_is_safe(group):
    for path in group["paths"]:
        if path.is_dir():
            if not _safe_regular_tree(path):
                return False
        elif not _safe_regular_file(path):
            return False
    return True


def _remove_runtime_group(group, context_root):
    for path in sorted(group["paths"], key=lambda item: len(item.parts), reverse=True):
        parent = path.parent
        if path.is_dir():
            _remove_tree_files_first(path)
        else:
            path.unlink()
        if parent.parent == context_root and parent.is_dir():
            try:
                parent.rmdir()
            except OSError:
                pass


def cleanup_runtime_context(
    runtime_root,
    managed_root,
    keep,
    apply,
    active_run_ids=None,
    terminal_confirmed_run_ids=None,
    stale_days=DEFAULT_STALE_DAYS,
    today=None,
    history_path=None,
):
    protected, candidates, skipped = runtime_candidates(
        runtime_root,
        managed_root,
        keep,
        active_run_ids,
        terminal_confirmed_run_ids,
        stale_days,
        today,
    )
    deleted = []
    context_root = runtime_root / "_execution_context"
    for _, group in candidates:
        path = group["paths"][0]
        if not _runtime_group_is_safe(group):
            skipped.append((path, group["size"], "unsafe_runtime_group"))
            continue
        if apply:
            if group["candidate_reason"] == "stale_runtime_expired":
                _append_history(
                    history_path,
                    "08_5_runtime_context",
                    "RUN_ID={}".format(group["run_id"]),
                    group["status"],
                    "{}日expire確定（削除開始）".format(stale_days),
                )
            _remove_runtime_group(group, context_root)
            if group["candidate_reason"] == "stale_runtime_expired":
                _append_history(
                    history_path,
                    "08_5_runtime_context",
                    "RUN_ID={}".format(group["run_id"]),
                    group["status"],
                    "{}日expire・retentionでローカル本体削除済み".format(stale_days),
                )
        deleted.append(
            (
                path,
                group["size"],
                "{}:{}:{}".format(
                    group["candidate_reason"], group["status"], group["run_id"]
                ),
            )
        )
    return protected, deleted, skipped


def cleanup_stale_generations(
    roots,
    stale_days,
    apply,
    active_names=None,
    now=None,
    history_path=None,
):
    active_names = set(active_names or ())
    protected = []
    deleted = []
    skipped = []
    candidates = []
    for root in roots:
        if not root.is_dir() or root.is_symlink():
            continue
        for generation in sorted(root.iterdir()):
            if generation.is_symlink() or not generation.is_dir():
                continue
            size = _tree_size(generation)
            if _contains_active_name(generation, active_names):
                protected.append((_tree_mtime_ns(generation), generation))
                continue
            generation_mtime = _tree_mtime_ns(generation) / 1_000_000_000
            now_timestamp = time.time() if now is None else now
            if now_timestamp - generation_mtime < stale_days * 86400:
                protected.append((_tree_mtime_ns(generation), generation))
                continue
            if not _safe_regular_tree(generation):
                skipped.append((generation, size, "unsafe_tree_shape"))
                continue
            candidates.append((generation, size))
    if apply and candidates:
        identities = ",".join(path.name for path, _ in candidates[:20])
        if len(candidates) > 20:
            identities += ",..."
        history_identity = "count={} ids={}".format(len(candidates), identities)
        _append_history(
            history_path,
            "stale_generated_history",
            history_identity,
            "UNKNOWN",
            "{}日expire確定（削除開始）".format(stale_days),
        )
    for generation, size in candidates:
        if apply:
            _remove_tree_files_first(generation)
        deleted.append((generation, size, "stale_generation_expired"))
    if apply and candidates:
        _append_history(
            history_path,
            "stale_generated_history",
            history_identity,
            "UNKNOWN",
            "{}日expire・retentionでローカル本体削除済み".format(stale_days),
        )
    return protected, deleted, skipped


def _stale_generated_files(root):
    paths = set()
    pipeline_result = root / "00_pipeline/01_result"
    if pipeline_result.is_dir():
        for pattern in PIPELINE_LEGACY_LOG_PATTERNS:
            paths.update(pipeline_result.glob(pattern))
    for step_dir in root.iterdir() if root.is_dir() else ():
        result_dir = step_dir / "01_result"
        if not result_dir.is_dir():
            continue
        for pattern in STALE_FILE_PATTERNS:
            paths.update(result_dir.glob(pattern))
    return sorted(paths)


def cleanup_stale_generated_files(
    root,
    stale_days,
    apply,
    now=None,
    history_path=None,
):
    protected = []
    deleted = []
    skipped = []
    candidates = []
    for path in _stale_generated_files(root):
        if not _safe_regular_file(path):
            skipped.append((path, 0, "unsafe_file_shape"))
            continue
        size = path.stat().st_size
        if not _is_expired_mtime(path, stale_days, now):
            protected.append((path.stat().st_mtime_ns, path))
            continue
        candidates.append((path, size))
    if apply and candidates:
        history_identity = "count={} files={}".format(
            len(candidates), ",".join(path.name for path, _ in candidates[:20])
        )
        if len(candidates) > 20:
            history_identity += ",..."
        _append_history(
            history_path,
            "stale_generated_log",
            history_identity,
            "UNKNOWN",
            "{}日expire確定（削除開始）".format(stale_days),
        )
    for path, size in candidates:
        if apply:
            path.unlink()
        deleted.append((path, size, "stale_generated_log_expired"))
    if apply and candidates:
        _append_history(
            history_path,
            "stale_generated_log",
            history_identity,
            "UNKNOWN",
            "{}日expire・retentionでローカル本体削除済み".format(stale_days),
        )
    return protected, deleted, skipped


def rotate_script_log(
    log_path,
    max_bytes,
    keep,
    apply,
    active_grace_seconds=DEFAULT_ACTIVE_LOG_GRACE_SECONDS,
    now=None,
):
    protected = []
    rotated = []
    skipped = []
    if not log_path.exists():
        return protected, rotated, skipped
    if not _safe_regular_file(log_path):
        return protected, rotated, [(log_path, 0, "unsafe_log_file")]
    stat = log_path.stat()
    archive_pattern = re.compile(r"^{}\.(\d+)\.gz$".format(re.escape(log_path.name)))
    for candidate in sorted(log_path.parent.glob(log_path.name + ".*.gz")):
        match = archive_pattern.match(candidate.name)
        if not match or int(match.group(1)) <= keep:
            continue
        if not _safe_regular_file(candidate):
            skipped.append((candidate, 0, "unsafe_rotation_archive"))
            continue
        size = candidate.stat().st_size
        if apply:
            candidate.unlink()
        rotated.append((candidate, size, "rotation_generation_limit"))
    if stat.st_size <= max_bytes:
        return [(stat.st_mtime_ns, log_path)], rotated, skipped
    now_timestamp = time.time() if now is None else now
    if now_timestamp - stat.st_mtime < active_grace_seconds:
        return protected, rotated, [(log_path, stat.st_size, "recently_modified_active_log")]
    archive_paths = [Path("{}.{}.gz".format(log_path, index)) for index in range(1, keep + 1)]
    if apply:
        temp_archive = Path("{}.1.gz.tmp".format(log_path))
        if temp_archive.exists():
            if not _safe_regular_file(temp_archive):
                raise ValueError("unsafe rotation temp file: {}".format(temp_archive))
            temp_archive.unlink()
        try:
            with log_path.open("rb") as source, gzip.open(str(temp_archive), "wb") as target:
                shutil.copyfileobj(source, target, length=1024 * 1024)
            for index in range(keep, 1, -1):
                older = archive_paths[index - 1]
                newer = archive_paths[index - 2]
                if older.exists():
                    if not _safe_regular_file(older):
                        raise ValueError("unsafe rotation archive: {}".format(older))
                    older.unlink()
                if newer.exists():
                    if not _safe_regular_file(newer):
                        raise ValueError("unsafe rotation archive: {}".format(newer))
                    os.replace(str(newer), str(older))
            os.replace(str(temp_archive), str(archive_paths[0]))
            with log_path.open("r+b") as handle:
                handle.truncate(0)
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            if temp_archive.exists() and _safe_regular_file(temp_archive):
                temp_archive.unlink()
    rotated.append((log_path, stat.st_size, "size_rotation_keep_{}_gzip".format(keep)))
    return protected, rotated, skipped


def _print_summary(label, protected, deleted, skipped, apply):
    action = "deleted" if apply else "would_delete"
    detail_limit = 10
    total = sum(item[1] for item in deleted)
    print(
        "{} protected={} {}={} bytes={} skipped={}".format(
            label, len(protected), action, len(deleted), total, len(skipped)
        )
    )
    for path, size, reason in deleted[:detail_limit]:
        print("{} {} bytes={} reason={}".format(action, path, size, reason))
    if len(deleted) > detail_limit:
        print("{} ... omitted_generations={}".format(action, len(deleted) - detail_limit))
    for path, size, reason in skipped[:detail_limit]:
        print("skipped {} bytes={} reason={}".format(path, size, reason))
    if len(skipped) > detail_limit:
        print("skipped ... omitted_generations={}".format(len(skipped) - detail_limit))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--managed-root", type=Path, default=DEFAULT_MANAGED_ROOT)
    parser.add_argument("--s3-daily-root", type=Path, default=DEFAULT_S3_DAILY_ROOT)
    parser.add_argument("--checkpoint-root", type=Path, default=DEFAULT_CHECKPOINT_ROOT)
    parser.add_argument("--usage-dir", action="append", type=Path)
    parser.add_argument("--runtime-root", type=Path, default=DEFAULT_RUNTIME_ROOT)
    parser.add_argument("--script-log", type=Path, default=DEFAULT_SCRIPT_LOG)
    parser.add_argument("--status-history", type=Path, default=DEFAULT_STATUS_HISTORY)
    parser.add_argument("--stale-generation-root", action="append", type=Path)
    parser.add_argument("--managed-keep", type=int, default=3)
    parser.add_argument("--s3-daily-keep", type=int, default=4)
    parser.add_argument("--named-validation-keep", type=int, default=4)
    parser.add_argument("--checkpoint-keep", type=int, default=3)
    parser.add_argument("--llm-usage-keep", type=int, default=30)
    parser.add_argument("--runtime-keep", type=int, default=3)
    parser.add_argument("--active-run-id", action="append", default=[])
    parser.add_argument("--active-run-date", action="append", default=[])
    parser.add_argument("--terminal-confirmed-run-id", action="append", default=[])
    parser.add_argument("--stale-days", type=int, default=DEFAULT_STALE_DAYS)
    parser.add_argument("--script-log-max-bytes", type=int, default=DEFAULT_SCRIPT_LOG_MAX_BYTES)
    parser.add_argument("--script-log-keep", type=int, default=DEFAULT_SCRIPT_LOG_KEEP)
    parser.add_argument(
        "--active-log-grace-seconds",
        type=int,
        default=DEFAULT_ACTIVE_LOG_GRACE_SECONDS,
    )
    parser.add_argument("--rotate-log-only", action="store_true")
    parser.add_argument("--region", default="ap-northeast-1")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    keep_values = (
        args.managed_keep,
        args.s3_daily_keep,
        args.named_validation_keep,
        args.checkpoint_keep,
        args.llm_usage_keep,
        args.runtime_keep,
        args.script_log_keep,
    )
    if any(value < 1 for value in keep_values):
        parser.error("keep counts must be at least 1")
    if args.stale_days < 1 or args.script_log_max_bytes < 1 or args.active_log_grace_seconds < 0:
        parser.error("stale days/rotation values are invalid")
    rotation = rotate_script_log(
        args.script_log,
        args.script_log_max_bytes,
        args.script_log_keep,
        args.apply,
        args.active_log_grace_seconds,
    )
    _print_summary("pipeline_script_exec_rotation", *rotation, args.apply)
    if args.rotate_log_only:
        return 0
    usage_dirs = tuple(args.usage_dir or DEFAULT_USAGE_DIRS)
    active_run_ids = set(args.active_run_id)
    active_run_dates = set(args.active_run_date)
    terminal_confirmed_run_ids = set(args.terminal_confirmed_run_id)
    stale_generation_roots = tuple(args.stale_generation_root or DEFAULT_STALE_GENERATION_ROOTS)

    s3_client = boto3.client(
        "s3",
        region_name=args.region,
        config=Config(retries={"max_attempts": 2, "mode": "standard"}, connect_timeout=5, read_timeout=15),
    )
    managed = cleanup_managed_logs(
        args.managed_root,
        args.managed_keep,
        args.apply,
        s3_client,
        terminal_confirmed_run_ids,
        args.stale_days,
        active_run_ids,
        history_path=args.status_history,
    )
    daily = cleanup_s3_daily(args.s3_daily_root, args.s3_daily_keep, args.apply)
    nonterminal_daily = cleanup_nonterminal_s3_daily(
        args.s3_daily_root,
        args.stale_days,
        args.apply,
        active_run_dates,
        history_path=args.status_history,
    )
    named = cleanup_named_validations(
        args.s3_daily_root,
        args.named_validation_keep,
        args.apply,
        active_run_ids | active_run_dates,
    )
    checkpoints = cleanup_checkpoints(
        args.checkpoint_root, args.checkpoint_keep, args.apply, active_run_ids
    )
    usage_results = [
        (
            str(usage_dir),
            cleanup_usage_logs(
                usage_dir, args.llm_usage_keep, args.apply, active_run_ids
            ),
        )
        for usage_dir in usage_dirs
    ]
    runtime = cleanup_runtime_context(
        args.runtime_root,
        args.managed_root,
        args.runtime_keep,
        args.apply,
        active_run_ids,
        terminal_confirmed_run_ids,
        args.stale_days,
        history_path=args.status_history,
    )
    managed_metadata = cleanup_managed_metadata(
        args.managed_root,
        args.apply,
        terminal_confirmed_run_ids,
        active_run_ids,
        args.stale_days,
        history_path=args.status_history,
    )
    stale_generations = cleanup_stale_generations(
        stale_generation_roots,
        args.stale_days,
        args.apply,
        active_run_ids | active_run_dates,
        history_path=args.status_history,
    )
    stale_files = cleanup_stale_generated_files(
        ROOT,
        args.stale_days,
        args.apply,
        history_path=args.status_history,
    )
    _print_summary("managed_logs", *managed, args.apply)
    _print_summary("s3_daily_pipeline", *daily, args.apply)
    _print_summary("s3_daily_nonterminal_expiry", *nonterminal_daily, args.apply)
    _print_summary("s3_daily_named_validation", *named, args.apply)
    _print_summary("07_1_concurrent_checkpoints", *checkpoints, args.apply)
    for usage_dir, result in usage_results:
        _print_summary("llm_usage:{}".format(usage_dir), *result, args.apply)
    _print_summary("08_5_runtime_context", *runtime, args.apply)
    _print_summary("managed_run_metadata", *managed_metadata, args.apply)
    _print_summary("stale_generated_histories", *stale_generations, args.apply)
    _print_summary("stale_generated_logs", *stale_files, args.apply)
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="/home/ec2-user/pipeline_ses_steps"
SCRIPT_DIR="$ROOT/00_pipeline/00_tool"
CONFIG_FILE="${PIPELINE_S3_CONFIG_FILE:-$SCRIPT_DIR/pipeline_s3_config.env}"
PIPELINE_SCRIPT="${PIPELINE_SCRIPT:-$SCRIPT_DIR/run_full_pipeline.sh}"
STATUS_WRITER="${PIPELINE_STATUS_WRITER:-$ROOT/99-9_publish_pipeline_status/00_tool/publish_pipeline_status.py}"
PYTHON_BIN="${PIPELINE_PYTHON_BIN:-/usr/bin/python3}"
AWS_BIN="${PIPELINE_AWS_BIN:-/usr/bin/aws}"
FLOCK_BIN="${PIPELINE_FLOCK_BIN:-/usr/bin/flock}"

if [[ ! -r "$CONFIG_FILE" ]]; then
  echo "pipeline S3 config is not readable: $CONFIG_FILE" >&2
  exit 2
fi
# shellcheck source=pipeline_s3_config.env
source "$CONFIG_FILE"

: "${RUN_ID:?RUN_ID is required}"
: "${RUN_DATE:?RUN_DATE is required}"

if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
  echo "RUN_ID contains unsupported characters: $RUN_ID" >&2
  exit 2
fi
if [[ ! "$RUN_DATE" =~ ^[0-9]{8}$ ]] || ! date -d "$RUN_DATE" '+%Y%m%d' >/dev/null 2>&1; then
  echo "RUN_DATE must be a valid YYYYMMDD date: $RUN_DATE" >&2
  exit 2
fi
if [[ ! -x "$PYTHON_BIN" || ! -r "$STATUS_WRITER" ]]; then
  echo "python3 or status writer is unavailable; FAILED status cannot be published" >&2
  exit 2
fi

STARTED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
STATE_DIR="$ROOT/00_pipeline/01_result/managed/$RUN_DATE/$RUN_ID"
LOCAL_LOG="$STATE_DIR/pipeline.log"
LOCAL_STATUS_FILE="$STATE_DIR/status.json"
CURRENT_STEP_FILE="$STATE_DIR/current_step"
LOCK_FILE="${PIPELINE_LOCK_FILE:-$ROOT/00_pipeline/01_result/run_full_pipeline.lock}"
LOG_S3_URI="s3://$PIPELINE_S3_BUCKET/$PIPELINE_S3_BASE_PREFIX/$PIPELINE_LOG_PREFIX/$RUN_DATE/$RUN_ID/pipeline.log"

LOCK_STATE="NOT_ACQUIRED"
LOCK_FD_OPEN=0
LOCAL_IO_ENABLED=0
FINALIZED=0
SIGNAL_ERROR=""
FAILURE_MESSAGE=""

managed_log() {
  local message
  message="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
  if [[ "$LOCAL_IO_ENABLED" -eq 1 ]]; then
    printf '%s\n' "$message" | tee -a "$LOCAL_LOG"
  else
    printf '%s\n' "$message" >&2
  fi
}

publish_status() {
  local status="$1"
  local current_step="$2"
  local finished_at="${3:-}"
  local exit_code="${4:-}"
  local error_message="${5:-}"
  local -a args=(
    "$STATUS_WRITER"
    --run-id "$RUN_ID"
    --run-date "$RUN_DATE"
    --status "$status"
    --started-at "$STARTED_AT"
    --current-step "$current_step"
    --error-message "$error_message"
    --log-s3-uri "$LOG_S3_URI"
    --bucket "$PIPELINE_S3_BUCKET"
    --base-prefix "$PIPELINE_S3_BASE_PREFIX"
    --status-prefix "$PIPELINE_STATUS_PREFIX"
    --log-prefix "$PIPELINE_LOG_PREFIX"
    --region "$PIPELINE_AWS_REGION"
  )

  if [[ "$LOCAL_IO_ENABLED" -eq 1 ]]; then
    args+=(--local-output "$LOCAL_STATUS_FILE")
  fi
  if [[ "$status" != "RUNNING" ]]; then
    args+=(--finished-at "$finished_at" --exit-code "$exit_code")
  fi
  "$PYTHON_BIN" "${args[@]}"
}

publish_terminal_with_retry() {
  local status="$1"
  local current_step="$2"
  local finished_at="$3"
  local exit_code="$4"
  local error_message="$5"
  local attempt

  for attempt in 1 2 3; do
    if publish_status "$status" "$current_step" "$finished_at" "$exit_code" "$error_message"; then
      return 0
    fi
    managed_log "terminal status publish failed (attempt=$attempt)"
    if [[ "$attempt" -lt 3 ]]; then
      sleep 2
    fi
  done
  return 1
}

upload_log() {
  if [[ "$LOCAL_IO_ENABLED" -ne 1 || ! -f "$LOCAL_LOG" || ! -x "$AWS_BIN" ]]; then
    return 1
  fi
  "$AWS_BIN" s3 cp "$LOCAL_LOG" "$LOG_S3_URI" \
    --region "$PIPELINE_AWS_REGION" \
    --only-show-errors
}

read_current_step() {
  if [[ "$LOCAL_IO_ENABLED" -eq 1 && -s "$CURRENT_STEP_FILE" ]]; then
    head -n 1 "$CURRENT_STEP_FILE"
  else
    printf '%s\n' "INITIALIZING"
  fi
}

capture_error() {
  local exit_code="$1"
  local failed_command="$2"
  if [[ -z "$FAILURE_MESSAGE" && "$FINALIZED" -eq 0 ]]; then
    FAILURE_MESSAGE="managed wrapper command failed (exit=$exit_code): $failed_command"
  fi
}

fail_managed() {
  local message="$1"
  local exit_code="${2:-1}"
  FAILURE_MESSAGE="$message"
  managed_log "$message"
  exit "$exit_code"
}

finalize() {
  local original_exit_code="$1"
  local final_exit_code="$original_exit_code"
  local final_status="FAILED"
  local error_message
  local current_step
  local finished_at

  if [[ "$FINALIZED" -eq 1 ]]; then
    return
  fi
  FINALIZED=1
  trap - EXIT TERM INT HUP ERR
  set +e

  current_step="$(read_current_step)"
  if [[ "$LOCK_STATE" == "BLOCKED_BY_OTHER" ]]; then
    current_step="ALREADY_RUNNING"
  fi
  finished_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  if [[ "$original_exit_code" -eq 0 ]]; then
    final_status="SUCCEEDED"
    error_message=""
    managed_log "pipeline completed successfully"
  elif [[ -n "$SIGNAL_ERROR" ]]; then
    error_message="$SIGNAL_ERROR"
    managed_log "pipeline interrupted: $SIGNAL_ERROR (exit=$original_exit_code)"
  elif [[ -n "$FAILURE_MESSAGE" ]]; then
    error_message="$FAILURE_MESSAGE"
    managed_log "$error_message"
  else
    error_message="pipeline exited with code $original_exit_code at $current_step"
    managed_log "$error_message"
  fi

  if ! upload_log; then
    error_message="${error_message:+$error_message; }pipeline log upload failed"
    final_status="FAILED"
    if [[ "$final_exit_code" -eq 0 ]]; then
      final_exit_code=1
    fi
    managed_log "pipeline log upload failed: $LOG_S3_URI"
  fi

  if ! publish_terminal_with_retry \
    "$final_status" "$current_step" "$finished_at" "$final_exit_code" "$error_message"; then
    managed_log "terminal status could not be published"
    if [[ "$final_exit_code" -eq 0 ]]; then
      final_exit_code=1
    fi
  fi

  if [[ "$LOCK_STATE" == "ACQUIRED" && "$LOCK_FD_OPEN" -eq 1 ]]; then
    : > "$LOCK_FILE"
    "$FLOCK_BIN" -u 9
    LOCK_STATE="RELEASED"
  fi
  exit "$final_exit_code"
}

handle_signal() {
  local signal_name="$1"
  local signal_exit_code="$2"
  SIGNAL_ERROR="received $signal_name signal"
  exit "$signal_exit_code"
}

trap 'finalize "$?"' EXIT
trap 'handle_signal TERM 143' TERM
trap 'handle_signal INT 130' INT
trap 'handle_signal HUP 129' HUP
trap 'capture_error "$?" "$BASH_COMMAND"' ERR

if ! mkdir -p "$STATE_DIR"; then
  fail_managed "failed to create managed state directory: $STATE_DIR"
fi
if [[ ! -x "$AWS_BIN" || ! -x "$FLOCK_BIN" ]]; then
  fail_managed "required executable is missing (aws/flock)" 2
fi
if [[ ! -r "$PIPELINE_SCRIPT" ]]; then
  fail_managed "pipeline script is not readable: $PIPELINE_SCRIPT" 2
fi
if ! mkdir -p "$(dirname "$LOCK_FILE")"; then
  fail_managed "failed to create lock directory: $(dirname "$LOCK_FILE")"
fi

# Open without truncating: a lock loser must not destroy the active RUN_ID metadata.
exec 9>>"$LOCK_FILE"
LOCK_FD_OPEN=1
if "$FLOCK_BIN" -n 9; then
  lock_exit_code=0
else
  lock_exit_code=$?
fi

if [[ "$lock_exit_code" -ne 0 ]]; then
  if [[ "$lock_exit_code" -ne 1 ]]; then
    fail_managed "failed to acquire pipeline lock (exit=$lock_exit_code)" "$lock_exit_code"
  fi

  LOCK_STATE="BLOCKED_BY_OTHER"
  active_run_id="$(head -n 1 "$LOCK_FILE" 2>/dev/null || true)"
  if [[ "$active_run_id" == "$RUN_ID" ]]; then
    LOCK_STATE="DUPLICATE_SAME_RUN"
    FINALIZED=1
    trap - EXIT TERM INT HUP ERR
    echo "pipeline run is already active for RUN_ID=$RUN_ID; leaving its status unchanged"
    exit 0
  fi

  LOCAL_IO_ENABLED=1
  : > "$LOCAL_LOG"
  FAILURE_MESSAGE="another pipeline run is active (active_run_id=${active_run_id:-unknown})"
  managed_log "$FAILURE_MESSAGE"
  exit 75
fi

LOCK_STATE="ACQUIRED"
LOCAL_IO_ENABLED=1
: > "$LOCK_FILE"
printf '%s\n' "$RUN_ID" >&9
: > "$LOCAL_LOG"
printf '%s\n' "INITIALIZING" > "$CURRENT_STEP_FILE"

export RUN_ID RUN_DATE
export PIPELINE_STARTED_AT="$STARTED_AT"
export PIPELINE_LOG="$LOCAL_LOG"
export PIPELINE_LOG_S3_URI="$LOG_S3_URI"
export PIPELINE_LOCAL_STATUS_FILE="$LOCAL_STATUS_FILE"
export PIPELINE_CURRENT_STEP_FILE="$CURRENT_STEP_FILE"
export PIPELINE_STATUS_WRITER="$STATUS_WRITER"
export PIPELINE_S3_BUCKET PIPELINE_S3_BASE_PREFIX PIPELINE_STATUS_PREFIX
export PIPELINE_LOG_PREFIX PIPELINE_AWS_REGION
export AWS_DEFAULT_REGION="$PIPELINE_AWS_REGION"

managed_log "managed pipeline start (RUN_DATE=$RUN_DATE, RUN_ID=$RUN_ID)"
publish_status "RUNNING" "INITIALIZING"

set +e
/usr/bin/bash "$PIPELINE_SCRIPT"
pipeline_exit_code=$?
set -e
if [[ "$pipeline_exit_code" -ne 0 ]]; then
  FAILURE_MESSAGE="pipeline exited with code $pipeline_exit_code at $(read_current_step)"
fi
exit "$pipeline_exit_code"

#!/usr/bin/env bash
set -euo pipefail

# 既知の80-7 FAILED runだけを復旧する薄いtail entrypoint。
# run_full_pipeline_managed.sh の PIPELINE_SCRIPT として使用し、managed lock/statusを再利用する。

ROOT="/home/ec2-user/pipeline_ses_steps"
LOG="${PIPELINE_LOG:-$ROOT/00_pipeline/01_result/pipeline_script_exec.log}"

: "${RUN_DATE:?RUN_DATE is required}"
: "${RUN_ID:?RUN_ID is required}"
: "${RECOVERY_FAILED_RUN_DATE:?RECOVERY_FAILED_RUN_DATE is required}"
: "${RECOVERY_FAILED_RUN_ID:?RECOVERY_FAILED_RUN_ID is required}"

if [[ ! "$RUN_DATE" =~ ^[0-9]{8}$ ]] || ! date -d "$RUN_DATE" '+%Y%m%d' >/dev/null 2>&1; then
  echo "RUN_DATE must be a valid YYYYMMDD date: $RUN_DATE" >&2
  exit 2
fi
if [[ "$RUN_DATE" != "$RECOVERY_FAILED_RUN_DATE" ]]; then
  echo "RUN_DATE must match RECOVERY_FAILED_RUN_DATE" >&2
  exit 2
fi
if [[ ! "$RECOVERY_FAILED_RUN_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
  echo "RECOVERY_FAILED_RUN_ID contains unsupported characters" >&2
  exit 2
fi
if [[ "$RUN_ID" == "$RECOVERY_FAILED_RUN_ID" ]]; then
  echo "recovery managed RUN_ID must differ from the historical FAILED RUN_ID" >&2
  exit 2
fi

mkdir -p "$ROOT/00_pipeline/01_result"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"
}

publish_current_step() {
  local step="$1"
  if [[ -n "${PIPELINE_CURRENT_STEP_FILE:-}" ]]; then
    printf '%s\n' "$step" > "${PIPELINE_CURRENT_STEP_FILE}.tmp.$$"
    mv "${PIPELINE_CURRENT_STEP_FILE}.tmp.$$" "$PIPELINE_CURRENT_STEP_FILE"
  fi
  if [[ -n "${PIPELINE_STATUS_WRITER:-}" ]]; then
    python3 "$PIPELINE_STATUS_WRITER" --status RUNNING --current-step "$step"
  fi
}

run_step() {
  local step="$1"
  shift
  publish_current_step "$step"
  log "=== START $step ==="
  local start_ts
  local exit_code
  local -a pipeline_status
  start_ts=$(date +%s)
  set +e
  python3 "$@" 2>&1 | tee -a "$LOG"
  pipeline_status=("${PIPESTATUS[@]}")
  set -e
  exit_code="${pipeline_status[0]}"
  if [[ "$exit_code" -eq 0 && "${pipeline_status[1]}" -ne 0 ]]; then
    exit_code="${pipeline_status[1]}"
  fi
  if [[ "$exit_code" -ne 0 ]]; then
    log "=== FAILED $step (exit=$exit_code, elapsed=$(( $(date +%s) - start_ts ))s) ==="
    exit "$exit_code"
  fi
  log "=== DONE $step (elapsed=$(( $(date +%s) - start_ts ))s) ==="
}

log "########## tail recovery start ##########"
log "RUN_DATE=$RUN_DATE / failed_run=$RECOVERY_FAILED_RUN_DATE/$RECOVERY_FAILED_RUN_ID"

run_step \
  "80-75_portal_s3_backup_rotation_preflight(recovery=$RECOVERY_FAILED_RUN_DATE/$RECOVERY_FAILED_RUN_ID)" \
  "$ROOT/80-75_portal_s3_backup_rotation/00_tool/portal_s3_backup_rotation.py" \
  --dry-run \
  --recovery-run-date "$RECOVERY_FAILED_RUN_DATE" \
  --recovery-run-id "$RECOVERY_FAILED_RUN_ID"

run_step \
  "80-7_manage_09_result_retention(RUN_DATE=$RUN_DATE)" \
  "$ROOT/80-7_manage_09_result_retention/00_tool/manage_09_result_retention.py" \
  --apply --run-date "$RUN_DATE"

run_step \
  "80-75_portal_s3_backup_rotation(recovery=$RECOVERY_FAILED_RUN_DATE/$RECOVERY_FAILED_RUN_ID)" \
  "$ROOT/80-75_portal_s3_backup_rotation/00_tool/portal_s3_backup_rotation.py" \
  --recovery-run-date "$RECOVERY_FAILED_RUN_DATE" \
  --recovery-run-id "$RECOVERY_FAILED_RUN_ID"

run_step \
  "80-8_portal_s3_prepare" \
  "$ROOT/80-8_portal_s3_prepare/00_tool/portal_s3_prepare.py"

run_step \
  "80-9_portal_s3_sync" \
  "$ROOT/80-9_portal_s3_sync/00_tool/portal_s3_sync.py"

log "########## tail recovery end ##########"

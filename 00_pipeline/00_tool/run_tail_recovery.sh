#!/usr/bin/env bash
set -euo pipefail

# 20260914成果物専用。managed wrapperでは起動しない。FAILED statusはread-only。
ROOT="/home/ec2-user/pipeline_ses_steps"
: "${RECOVERY_ID:?new RECOVERY_ID is required}"
: "${RECOVERY_SOURCE_EXECUTION_ARN:?source execution ARN is required}"
: "${PREVIOUS_MANUAL_RECOVERY_RECEIPT:?September 11 publication receipt is required}"

SOURCE_RUN_ID="sfn-6e454f73-4f5c-46b4-bfa1-e3c633b52b3f"
if [[ ! "$RECOVERY_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ || "$RECOVERY_ID" == "$SOURCE_RUN_ID" ]]; then
  echo "invalid or reused source RECOVERY_ID" >&2
  exit 2
fi
if [[ -n "${PIPELINE_STATUS_WRITER:-}" || -n "${PIPELINE_CURRENT_STEP_FILE:-}" ]]; then
  echo "tail recovery must run independently of the managed status writer" >&2
  exit 2
fi
if [[ "${1:-}" != "--apply" || "$#" != 1 ]]; then
  echo "Explicit --apply is required. Steps: 80-75 -> 80-8 -> 80-9; RUN_DATE=20260914" >&2
  exit 2
fi
export RUN_DATE=20260914
export RUN_ID="$RECOVERY_ID"
STATE_DIR="$ROOT/00_pipeline/01_result/manual_recovery/$RECOVERY_ID"
LOCK_FILE="$ROOT/00_pipeline/01_result/run_full_pipeline.lock"
exec 9<>"$LOCK_FILE"
/usr/bin/flock -n 9 || { echo "another pipeline run is active" >&2; exit 1; }
mkdir -p "$ROOT/00_pipeline/01_result/manual_recovery"
mkdir "$STATE_DIR" || { echo "recovery ID already used" >&2; exit 1; }
# lock file is already locked; truncate through the inherited descriptor.
: > "$LOCK_FILE"
printf '%s\n' "$RECOVERY_ID" >&9
# fd9は継承。各stepのread-only preflightでもowner/active runを再照合する。
MANUAL_ARGS=(--manual-recovery-id "$RECOVERY_ID"
  --manual-source-run-date "$RUN_DATE" --manual-source-run-id "$SOURCE_RUN_ID"
  --manual-source-execution-arn "$RECOVERY_SOURCE_EXECUTION_ARN")
log="$STATE_DIR/tail.log"
run_step() {
  python3 "$@" 2>&1 | tee -a "$log"
}
run_step "$ROOT/80-75_portal_s3_backup_rotation/00_tool/portal_s3_backup_rotation.py" \
  --step-dir "$STATE_DIR/80-75" \
  --manual-recovery-receipt "$PREVIOUS_MANUAL_RECOVERY_RECEIPT" "${MANUAL_ARGS[@]}"
run_step "$ROOT/80-8_portal_s3_prepare/00_tool/portal_s3_prepare.py" \
  --step-dir "$STATE_DIR/80-8" --run-date "$RUN_DATE" --run-id "$RECOVERY_ID"
run_step "$ROOT/80-9_portal_s3_sync/00_tool/portal_s3_sync.py" \
  --step-dir "$STATE_DIR/80-9" --prepare-dir "$STATE_DIR/80-8" \
  --run-date "$RUN_DATE" --run-id "$RECOVERY_ID" "${MANUAL_ARGS[@]}"

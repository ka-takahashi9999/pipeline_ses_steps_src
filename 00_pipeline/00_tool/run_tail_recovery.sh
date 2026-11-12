#!/usr/bin/env bash
set -euo pipefail

# pre-publication FAILED runの既存成果物専用。
# managed wrapperでは起動せず、元FAILED status/executionはread-onlyで保つ。
ROOT="/home/ec2-user/pipeline_ses_steps"
: "${RECOVERY_ID:?new RECOVERY_ID is required}"
: "${RECOVERY_SOURCE_RUN_DATE:?source RUN_DATE is required}"
: "${RECOVERY_SOURCE_RUN_ID:?source RUN_ID is required}"
: "${RECOVERY_SOURCE_EXECUTION_ARN:?source execution ARN is required}"

SOURCE_RUN_ID="$RECOVERY_SOURCE_RUN_ID"
if [[ ! "$RECOVERY_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ || "$RECOVERY_ID" == "$SOURCE_RUN_ID" ]]; then
  echo "invalid or reused source RECOVERY_ID" >&2
  exit 2
fi
if [[ ! "$SOURCE_RUN_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ || ! "$RECOVERY_SOURCE_RUN_DATE" =~ ^[0-9]{8}$ ]]; then
  echo "invalid source RUN_DATE/RUN_ID" >&2
  exit 2
fi
if [[ -n "${PIPELINE_STATUS_WRITER:-}" || -n "${PIPELINE_CURRENT_STEP_FILE:-}" ]]; then
  echo "tail recovery must run independently of the managed status writer" >&2
  exit 2
fi
if [[ "${1:-}" != "--apply" || "$#" != 1 ]]; then
  echo "Explicit --apply is required. Steps: 80-7 -> 80-75 -> 80-8 -> 80-9" >&2
  exit 2
fi
export RUN_DATE="$RECOVERY_SOURCE_RUN_DATE"
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
  --manual-source-execution-arn "$RECOVERY_SOURCE_EXECUTION_ARN"
  --manual-prepublication-source)
log="$STATE_DIR/tail.log"
run_step() {
  python3 "$@" 2>&1 | tee -a "$log"
}
PREVIOUS_ARGS=(--manual-previous-managed)
if [[ -n "${PREVIOUS_MANUAL_RECOVERY_RECEIPT:-}" ]]; then
  PREVIOUS_ARGS=(--manual-recovery-receipt "$PREVIOUS_MANUAL_RECOVERY_RECEIPT")
fi
AUTHORITY_FILE="$STATE_DIR/publication_authority.json"
run_step "$ROOT/80-75_portal_s3_backup_rotation/00_tool/portal_s3_backup_rotation.py" \
  --resolve-publication-authority --authority-output "$AUTHORITY_FILE"
# いかなる削除・公開より前にsource FAILED / redrive / previous CURRENTを読取検証する。
run_step "$ROOT/80-75_portal_s3_backup_rotation/00_tool/portal_s3_backup_rotation.py" \
  --dry-run --step-dir "$STATE_DIR/80-75-preflight" \
  --publication-authority-file "$AUTHORITY_FILE" \
  "${PREVIOUS_ARGS[@]}" "${MANUAL_ARGS[@]}"
RETENTION_PLAN="$(python3 "$ROOT/80-75_portal_s3_backup_rotation/00_tool/tail_recovery_plan.py" \
  "$STATE_DIR/80-75-preflight/01_result/portal_s3_backup_rotation_summary.json")"
if [[ "$RETENTION_PLAN" == "RUN_80_7" ]]; then
  run_step "$ROOT/80-7_manage_09_result_retention/00_tool/manage_09_result_retention.py" \
    --apply --run-date "$RUN_DATE"
elif [[ "$RETENTION_PLAN" != "SKIP_80_7" ]]; then
  echo "invalid retention recovery plan: $RETENTION_PLAN" >&2
  exit 1
fi
run_step "$ROOT/80-75_portal_s3_backup_rotation/00_tool/portal_s3_backup_rotation.py" \
  --step-dir "$STATE_DIR/80-75" \
  --publication-authority-file "$AUTHORITY_FILE" \
  "${PREVIOUS_ARGS[@]}" "${MANUAL_ARGS[@]}"
run_step "$ROOT/80-8_portal_s3_prepare/00_tool/portal_s3_prepare.py" \
  --step-dir "$STATE_DIR/80-8" --run-date "$RUN_DATE" --run-id "$RECOVERY_ID"
run_step "$ROOT/80-9_portal_s3_sync/00_tool/portal_s3_sync.py" \
  --step-dir "$STATE_DIR/80-9" --prepare-dir "$STATE_DIR/80-8" \
  --run-date "$RUN_DATE" --run-id "$RECOVERY_ID" "${MANUAL_ARGS[@]}"

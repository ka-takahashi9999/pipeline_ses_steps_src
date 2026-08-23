#!/usr/bin/env bash
set -euo pipefail

# 08-5 Batch completed後だけ実行するtail entrypoint。01系～08-4は実行しない。

ROOT="/home/ec2-user/pipeline_ses_steps"
LOG="${PIPELINE_LOG:-$ROOT/00_pipeline/01_result/pipeline_script_exec.log}"

: "${RUN_DATE:?RUN_DATE is required}"
: "${RUN_ID:?RUN_ID is required}"

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

log "########## pipeline Phase B start ##########"
log "RUN_DATE=$RUN_DATE / RUN_ID=$RUN_ID"

# このcommand内部でcompleted再確認、collector、integrity/fallback/safety guard、
# transactional publish、expected run/manifest commit marker検証まで完了する。
run_step \
  "08-5_batch_collect_commit_gate" \
  "$ROOT/08-5_high_score_required_skill_recheck/00_tool/batch_aws_orchestration.py" \
  phase-b --pipeline-run-id "$RUN_ID" --run-date "$RUN_DATE"

# marker gate成功後だけ09系と既存CURRENT/BK1 publication contractへ進む。
run_step "09-1_mail_display_format(RUN_DATE=$RUN_DATE)" "$ROOT/09-1_mail_display_format/00_tool/mail_display_format.py" --target-date "$RUN_DATE"
run_step "09-2_extract_high_score_mail_display(RUN_DATE=$RUN_DATE)" "$ROOT/09-2_extract_high_score_mail_display/00_tool/extract_high_score_mail_display.py" --target-date "$RUN_DATE"
run_step "09-3_prepare_sales_proposal_input(RUN_DATE=$RUN_DATE)" "$ROOT/09-3_prepare_sales_proposal_input/00_tool/prepare_sales_proposal_input.py" --target-date "$RUN_DATE"
run_step "09-3_prepare_sales_mail_context(RUN_DATE=$RUN_DATE)" "$ROOT/09-3_prepare_sales_mail_context/00_tool/prepare_sales_mail_context.py" --target-date "$RUN_DATE"
run_step "09-4_remove_category_mismatch_sales_candidates(RUN_DATE=$RUN_DATE)" "$ROOT/09-4_remove_category_mismatch_sales_candidates/00_tool/remove_category_mismatch_sales_candidates.py" --target-date "$RUN_DATE"
run_step "09-5_generate_sales_reply_draft(RUN_DATE=$RUN_DATE)" "$ROOT/09-5_generate_sales_reply_draft/00_tool/generate_sales_reply_draft.py" --target-date "$RUN_DATE"

run_step "80-7_manage_09_result_retention(RUN_DATE=$RUN_DATE)" "$ROOT/80-7_manage_09_result_retention/00_tool/manage_09_result_retention.py" --apply --run-date "$RUN_DATE"
run_step "80-75_portal_s3_backup_rotation" "$ROOT/80-75_portal_s3_backup_rotation/00_tool/portal_s3_backup_rotation.py"
run_step "80-8_portal_s3_prepare" "$ROOT/80-8_portal_s3_prepare/00_tool/portal_s3_prepare.py"
run_step "80-9_portal_s3_sync" "$ROOT/80-9_portal_s3_sync/00_tool/portal_s3_sync.py"

log "########## pipeline Phase B end ##########"

#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/ec2-user/pipeline_ses_steps"
LOG="$ROOT/00_pipeline/01_result/pipeline_script_exec.log"
RUN_DATE="${RUN_DATE:-$(date '+%Y%m%d')}"

mkdir -p "$ROOT/00_pipeline/01_result"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"
}

run_step() {
  local step="$1"
  shift
  log "=== START $step ==="
  local start_ts
  start_ts=$(date +%s)
  python3 "$@" 2>&1 | tee -a "$LOG"
  local exit_code=${PIPESTATUS[0]}
  local elapsed=$(( $(date +%s) - start_ts ))
  if [ "$exit_code" -ne 0 ]; then
    log "=== FAILED $step (exit=$exit_code, elapsed=${elapsed}s) ==="
    exit "$exit_code"
  fi
  log "=== DONE $step (elapsed=${elapsed}s) ==="
}

log "########## pipeline start ##########"
log "RUN_DATE=$RUN_DATE"

run_step "01-1_fetch_gmail" "$ROOT/01-1_fetch_gmail/00_tool/fetch_gmail.py" --after 2026/04/14 --before 2026/04/17 --max 3000
# 当日分のメールを自動取得（20時定時実行想定: after=当日 before=翌日 で当日送信分3000件を取得）
#run_step "01-1_fetch_gmail" "$ROOT/01-1_fetch_gmail/00_tool/fetch_gmail.py" --after "$(date '+%Y/%m/%d')" --before "$(date -d '+1 day' '+%Y/%m/%d')" --max 3000

run_step "01-2_remove_duplicate_emails" "$ROOT/01-2_remove_duplicate_emails/00_tool/remove_duplicate_emails.py"

run_step "01-3_remove_individual_email" "$ROOT/01-3_remove_individual_email/00_tool/remove_individual_email.py"

run_step "01-4_cleanup_email_text" "$ROOT/01-4_cleanup_email_text/00_tool/cleanup_email_text.py"

run_step "02-1_classify_type_project_resource" "$ROOT/02-1_classify_type_project_resource/00_tool/classify_type_project_resource.py"

run_step "02-2_classify_output_file_project_resource" "$ROOT/02-2_classify_output_file_project_resource/00_tool/classify_output_file_project_resource.py"

run_step "03-1_extract_project_budget" "$ROOT/03-1_extract_project_budget/00_tool/extract_project_budget.py"

run_step "03-2_extract_project_age" "$ROOT/03-2_extract_project_age/00_tool/extract_project_age.py"

run_step "03-3_extract_project_remote" "$ROOT/03-3_extract_project_remote/00_tool/extract_project_remote.py"

run_step "03-4_extract_project_foreign" "$ROOT/03-4_extract_project_foreign/00_tool/extract_project_foreign.py"

run_step "03-5_extract_project_freelance" "$ROOT/03-5_extract_project_freelance/00_tool/extract_project_freelance.py"

run_step "03-6_extract_project_workload" "$ROOT/03-6_extract_project_workload/00_tool/extract_project_workload.py"

run_step "03-7_extract_project_vendor_tiers" "$ROOT/03-7_extract_project_vendor_tiers/00_tool/extract_project_vendor_tiers.py"

run_step "03-8_extract_project_skill_category" "$ROOT/03-8_extract_project_skill_category/00_tool/extract_project_skill_category.py"

run_step "03-9_extract_project_phase_category" "$ROOT/03-9_extract_project_phase_category/00_tool/extract_project_phase_category.py"

run_step "03-10_extract_project_location" "$ROOT/03-10_extract_project_location/00_tool/extract_project_location.py"

run_step "03-30_extract_project_contract_type" "$ROOT/03-30_extract_project_contract_type/00_tool/extract_project_contract_type.py"

run_step "03-50_extract_project_required_skills" "$ROOT/03-50_extract_project_required_skills/00_tool/extract_project_required_skills.py"

run_step "03-51_extract_project_required_skills_list" "$ROOT/03-51_extract_project_required_skills_list/00_tool/extract_project_required_skills_list.py"

run_step "04-1_fetch_skillsheets_text" "$ROOT/04-1_fetch_skillsheets_text/00_tool/fetch_skillsheets_text.py"

run_step "05-1_extract_resource_budget" "$ROOT/05-1_extract_resource_budget/00_tool/extract_resource_budget.py"

run_step "05-2_extract_resource_age" "$ROOT/05-2_extract_resource_age/00_tool/extract_resource_age.py"

run_step "05-3_extract_resource_remote" "$ROOT/05-3_extract_resource_remote/00_tool/extract_resource_remote.py"

run_step "05-4_extract_resource_foreign" "$ROOT/05-4_extract_resource_foreign/00_tool/extract_resource_foreign.py"

run_step "05-5_extract_resource_freelance" "$ROOT/05-5_extract_resource_freelance/00_tool/extract_resource_freelance.py"

run_step "05-6_extract_resource_workload" "$ROOT/05-6_extract_resource_workload/00_tool/extract_resource_workload.py"

run_step "05-7_extract_resource_vendor_tiers" "$ROOT/05-7_extract_resource_vendor_tiers/00_tool/extract_resource_vendor_tiers.py"

run_step "05-8_extract_resource_skill_category" "$ROOT/05-8_extract_resource_skill_category/00_tool/extract_resource_skill_category.py"

run_step "05-9_extract_resource_phase_category" "$ROOT/05-9_extract_resource_phase_category/00_tool/extract_resource_phase_category.py"

run_step "05-10_extract_resource_location" "$ROOT/05-10_extract_resource_location/00_tool/extract_resource_location.py"

run_step "06-0_match_all_message_id" "$ROOT/06-0_match_all_message_id/00_tool/match_all_message_id.py"

run_step "06-1_match_budget" "$ROOT/06-1_match_budget/00_tool/match_budget.py"

run_step "06-2_match_age" "$ROOT/06-2_match_age/00_tool/match_age.py"

run_step "06-3_match_remote" "$ROOT/06-3_match_remote/00_tool/match_remote.py"

run_step "06-4_match_foreign" "$ROOT/06-4_match_foreign/00_tool/match_foreign.py"

run_step "06-5_match_freelance" "$ROOT/06-5_match_freelance/00_tool/match_freelance.py"

run_step "06-6_match_workload" "$ROOT/06-6_match_workload/00_tool/match_workload.py"

run_step "06-7_match_vendor_tiers" "$ROOT/06-7_match_vendor_tiers/00_tool/match_vendor_tiers.py"

run_step "06-8_match_skill_category" "$ROOT/06-8_match_skill_category/00_tool/match_skill_category.py"

run_step "06-9_match_phase_category" "$ROOT/06-9_match_phase_category/00_tool/match_phase_category.py"

run_step "06-10_match_location" "$ROOT/06-10_match_location/00_tool/match_location.py"

run_step "06-11_match_required_skills_list" "$ROOT/06-11_match_required_skills_list/00_tool/match_required_skills_list.py"

run_step "06-12_filter_required_skills_noise" "$ROOT/06-12_filter_required_skills_noise/00_tool/filter_required_skills_noise.py"

run_step "06-30_match_contract_type" "$ROOT/06-30_match_contract_type/00_tool/match_contract_type.py"

# ──────────────────────────────────────────────────────────────────────
# 【同じメール集合で 06-80 を再実行する場合】（例: コード修正後の reprocess）
# 06-80 は前回の diff_file を bk_diff にローテートして重複判定に使う。
# そのため同一メール集合で再実行すると全ペアが duplicate 扱いになり、
# 08-1 が bk_merged から旧結果を復元するため、コード変更が反映されない。
# 下記 2 ファイルを退避してから 06-80 を動かすと、全ペアを新規ルートで流せる。
#
# DIFF_FILE="$ROOT/06-80_duplicate_proposal_check/01_result/duplicate_proposal_check_diff_file.jsonl"
# [ -f "$DIFF_FILE" ] && mv "$DIFF_FILE" "${DIFF_FILE}.bak_$(date +%Y%m%d)"
# # bk_merged は 08-1 側が最後に上書きするので通常は退避不要。必要なら以下も退避:
# # BK_MERGED="$ROOT/08-1_restore_and_merge_requirement_skill_ai_matching/01_result/bk_merged_requirement_skill_ai_matching.jsonl"
# # [ -f "$BK_MERGED" ] && mv "$BK_MERGED" "${BK_MERGED}.bak_$(date +%Y%m%d)"
# ──────────────────────────────────────────────────────────────────────
run_step "06-80_duplicate_proposal_check" "$ROOT/06-80_duplicate_proposal_check/00_tool/duplicate_proposal_check.py"

# 07-1 は LLM使用step。06-80で仕分けた新規ペアのみを処理する。
run_step "07-1_requirement_skill_ai_matching" "$ROOT/07-1_requirement_skill_ai_matching/00_tool/requirement_skill_ai_matching.py" #新規のみ全件
#run_step "07-1_requirement_skill_ai_matching" "$ROOT/07-1_requirement_skill_ai_matching/00_tool/requirement_skill_ai_matching.py" --limit 2000 #件数指定(100件を例)

run_step "08-1_restore_and_merge_requirement_skill_ai_matching" "$ROOT/08-1_restore_and_merge_requirement_skill_ai_matching/00_tool/restore_and_merge_requirement_skill_ai_matching.py"

run_step "08-2_match_score_aggregation" "$ROOT/08-2_match_score_aggregation/00_tool/match_score_aggregation.py"

run_step "08-3_match_score_partition" "$ROOT/08-3_match_score_partition/00_tool/match_score_partition.py"

run_step "08-4_match_score_sort" "$ROOT/08-4_match_score_sort/00_tool/match_score_sort.py"

run_step "08-5_high_score_required_skill_recheck" "$ROOT/08-5_high_score_required_skill_recheck/00_tool/high_score_required_skill_recheck.py"

run_step "09-1_mail_display_format(RUN_DATE=$RUN_DATE)" "$ROOT/09-1_mail_display_format/00_tool/mail_display_format.py" --target-date "$RUN_DATE"

run_step "09-2_extract_high_score_mail_display(RUN_DATE=$RUN_DATE)" "$ROOT/09-2_extract_high_score_mail_display/00_tool/extract_high_score_mail_display.py" --target-date "$RUN_DATE"

run_step "09-3_prepare_sales_proposal_input(RUN_DATE=$RUN_DATE)" "$ROOT/09-3_prepare_sales_proposal_input/00_tool/prepare_sales_proposal_input.py" --target-date "$RUN_DATE"

# 既存の 09-3_prepare_sales_proposal_input は現状維持で残す。
# 新しい営業メール文脈生成/ドラフト生成は同一 RUN_DATE で固定して後続追加する。
run_step "09-3_prepare_sales_mail_context(RUN_DATE=$RUN_DATE)" "$ROOT/09-3_prepare_sales_mail_context/00_tool/prepare_sales_mail_context.py" --target-date "$RUN_DATE"

run_step "09-4_generate_sales_reply_draft(RUN_DATE=$RUN_DATE)" "$ROOT/09-4_generate_sales_reply_draft/00_tool/generate_sales_reply_draft.py" --target-date "$RUN_DATE"

##アシスタントツール（shスクリプトのためrun_stepではなくbashで直接実行）
log "=== START run_suggest_and_cleanup ==="
bash "$ROOT/00_pipeline/10_assistance_tool/run_suggest_and_cleanup.sh" 2>&1 | tee -a "$LOG"
log "=== DONE run_suggest_and_cleanup ==="

log "########## pipeline end ##########"

####
# nohupで実行する場合（ログはpipeline_script_exec.logに追記）
# nohup bash /home/ec2-user/pipeline_ses_steps/00_pipeline/00_tool/run_full_pipeline.sh > /home/ec2-user/pipeline_ses_steps/00_pipeline/01_result/pipeline_script_exec.log 2>&1 &
####

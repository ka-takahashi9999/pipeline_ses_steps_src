#!/usr/bin/env bash
set -euo pipefail

ROOT="${PIPELINE_ROOT:-/home/ec2-user/pipeline_ses_steps}"
SCRIPT_DIR="$ROOT/00_pipeline/10_assistance_tool"
PYTHON_BIN="${PIPELINE_PYTHON_BIN:-python3}"
RUN_DATE_VALUE="${RUN_DATE:-$(date '+%Y%m%d')}"
OUTPUT_ROOT="${DICTIONARY_SUGGEST_OUTPUT_ROOT:-$SCRIPT_DIR/suggest_result}"
TMP_ROOT="$OUTPUT_ROOT/.tmp"
STAGE="$TMP_ROOT/${RUN_DATE_VALUE}.$$"
LOCK_FILE="$OUTPUT_ROOT/.run_suggest_and_cleanup.lock"
LOG="${DICTIONARY_SUGGEST_LOG:-$SCRIPT_DIR/run_suggest_and_cleanup.log}"
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-date)
      RUN_DATE_VALUE="${2:?--run-date requires a value}"
      STAGE="$TMP_ROOT/${RUN_DATE_VALUE}.$$"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ ! "$RUN_DATE_VALUE" =~ ^[0-9]{8}$ ]]; then
  echo "RUN_DATE must be YYYYMMDD" >&2
  exit 2
fi

SUGGEST_SCRIPTS=(
  "03-8_extract_project_skill_category/10_assistance_tool/suggest_skill_dictionary.py"
  "03-9_extract_project_phase_category/10_assistance_tool/suggest_phase_dictionary.py"
  "03-10_extract_project_location/10_assistance_tool/suggest_location_dictionary.py"
  "05-8_extract_resource_skill_category/10_assistance_tool/suggest_skill_dictionary.py"
  "05-9_extract_resource_phase_category/10_assistance_tool/suggest_phase_dictionary.py"
  "05-10_extract_resource_location/10_assistance_tool/suggest_location_dictionary.py"
)
CLEANUP_SCRIPTS=(
  "03-8_extract_project_skill_category/10_assistance_tool/cleanup_skill_candidates.py"
  "03-9_extract_project_phase_category/10_assistance_tool/cleanup_phase_candidates.py"
  "03-10_extract_project_location/10_assistance_tool/cleanup_location_candidates.py"
  "05-8_extract_resource_skill_category/10_assistance_tool/cleanup_skill_candidates.py"
  "05-9_extract_resource_phase_category/10_assistance_tool/cleanup_phase_candidates.py"
  "05-10_extract_resource_location/10_assistance_tool/cleanup_location_candidates.py"
)
CANDIDATE_NAMES=(
  "03-8_skill_dictionary_candidates.txt"
  "03-9_phase_dictionary_candidates.txt"
  "03-10_location_dictionary_candidates.txt"
  "05-8_skill_dictionary_candidates.txt"
  "05-9_phase_dictionary_candidates.txt"
  "05-10_location_dictionary_candidates.txt"
)
FIXED_CANDIDATES=(
  "03-8_extract_project_skill_category/10_assistance_tool/skill_dictionary_candidates.txt"
  "03-9_extract_project_phase_category/10_assistance_tool/phase_dictionary_candidates.txt"
  "03-10_extract_project_location/10_assistance_tool/location_dictionary_candidates.txt"
  "05-8_extract_resource_skill_category/10_assistance_tool/skill_dictionary_candidates.txt"
  "05-9_extract_resource_phase_category/10_assistance_tool/phase_dictionary_candidates.txt"
  "05-10_extract_resource_location/10_assistance_tool/location_dictionary_candidates.txt"
)
HISTORY_PREFIXES=(
  "03-8_skill_dictionary_candidates"
  "03-9_phase_dictionary_candidates"
  "03-10_location_dictionary_candidates"
  "05-8_skill_dictionary_candidates"
  "05-9_phase_dictionary_candidates"
  "05-10_location_dictionary_candidates"
)
DICTIONARY_FILES=(
  "03-8_extract_project_skill_category/00_tool/skill_dictionary.txt"
  "common/phase_dictionary.txt"
  "03-10_extract_project_location/00_tool/location_dictionary.txt"
  "05-8_extract_resource_skill_category/00_tool/skill_dictionary.txt"
  "05-10_extract_resource_location/00_tool/location_dictionary.txt"
)

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "suggest_and_cleanup dry-run: RUN_DATE=$RUN_DATE_VALUE"
  echo "temporary stage: $STAGE"
  for index in "${!SUGGEST_SCRIPTS[@]}"; do
    echo "suggest/cleanup: ${SUGGEST_SCRIPTS[$index]} -> .tmp/${CANDIDATE_NAMES[$index]}"
    echo "publish fixed: ${FIXED_CANDIDATES[$index]}"
    echo "publish history: ${HISTORY_PREFIXES[$index]}_${RUN_DATE_VALUE}.txt"
  done
  echo "dry-run completed: no candidate, dictionary, pair-analysis, or pipeline command executed"
  exit 0
fi

mkdir -p "$TMP_ROOT" "$(dirname "$LOG")"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "another suggest_and_cleanup run is active: $LOCK_FILE" >&2
  rmdir "$TMP_ROOT" 2>/dev/null || true
  exit 1
fi
if [[ -e "$STAGE" ]]; then
  echo "staging path already exists: $STAGE" >&2
  exit 1
fi
mkdir "$STAGE"

COMMITTED=0
declare -a DESTINATIONS=()
declare -a PUBLISH_TEMPS=()
declare -a BACKUPS=()
declare -a DEST_EXISTED=()
declare -a PUBLISHED_INDEXES=()
declare -a PRUNED_ORIGINALS=()
declare -a PRUNED_STAGED=()

cleanup_all() {
  local status=$?
  local index
  trap - EXIT INT TERM
  set +e

  if [[ "$COMMITTED" -eq 0 ]]; then
    for ((index=${#PUBLISHED_INDEXES[@]} - 1; index >= 0; index--)); do
      local published_index="${PUBLISHED_INDEXES[$index]}"
      if [[ "${DEST_EXISTED[$published_index]}" -eq 1 ]]; then
        mv -f -- "${BACKUPS[$published_index]}" "${DESTINATIONS[$published_index]}"
      else
        rm -f -- "${DESTINATIONS[$published_index]}"
      fi
    done
    for ((index=${#PRUNED_ORIGINALS[@]} - 1; index >= 0; index--)); do
      if [[ -e "${PRUNED_STAGED[$index]}" ]]; then
        mv -f -- "${PRUNED_STAGED[$index]}" "${PRUNED_ORIGINALS[$index]}"
      fi
    done
  fi

  for index in "${!PUBLISH_TEMPS[@]}"; do
    rm -f -- "${PUBLISH_TEMPS[$index]}"
    if [[ -n "${BACKUPS[$index]}" ]]; then
      rm -f -- "${BACKUPS[$index]}"
    fi
  done
  if [[ -d "$STAGE" && "$STAGE" == "$TMP_ROOT/"* ]]; then
    rm -rf -- "$STAGE"
  fi
  rmdir "$TMP_ROOT" 2>/dev/null || true
  exit "$status"
}
trap cleanup_all EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"
}

run_step() {
  local step="$1"
  shift
  log "=== START $step ==="
  local start_ts exit_code
  local -a pipeline_status
  start_ts=$(date +%s)
  set +e
  "$@" 2>&1 | tee -a "$LOG"
  pipeline_status=("${PIPESTATUS[@]}")
  set -e
  exit_code="${pipeline_status[0]}"
  if [[ "$exit_code" -eq 0 && "${pipeline_status[1]}" -ne 0 ]]; then
    exit_code="${pipeline_status[1]}"
  fi
  if [[ "$exit_code" -ne 0 ]]; then
    log "=== FAILED $step (exit=$exit_code, elapsed=$(( $(date +%s) - start_ts ))s) ==="
    return "$exit_code"
  fi
  log "=== DONE $step (elapsed=$(( $(date +%s) - start_ts ))s) ==="
}

prepare_publish_file() {
  local source="$1"
  local destination="$2"
  local destination_dir publish_temp backup index

  destination_dir="$(dirname "$destination")"
  if [[ ! -d "$destination_dir" ]]; then
    log "公開先ディレクトリがありません: $destination_dir"
    return 1
  fi

  publish_temp=$(mktemp "$destination_dir/.dictionary_candidate_publish.XXXXXX")
  DESTINATIONS+=("$destination")
  PUBLISH_TEMPS+=("$publish_temp")
  BACKUPS+=("")
  DEST_EXISTED+=(0)
  index=$((${#DESTINATIONS[@]} - 1))
  cp -- "$source" "$publish_temp"
  chmod 0644 "$publish_temp"

  if [[ -e "$destination" ]]; then
    chmod --reference="$destination" "$publish_temp"
    backup=$(mktemp "$destination_dir/.dictionary_candidate_backup.XXXXXX")
    BACKUPS[$index]="$backup"
    DEST_EXISTED[$index]=1
    cp -p -- "$destination" "$backup"
  fi
}

publish_prepared_files() {
  local index
  for index in "${!DESTINATIONS[@]}"; do
    PUBLISHED_INDEXES+=("$index")
    mv -f -- "${PUBLISH_TEMPS[$index]}" "${DESTINATIONS[$index]}"
  done
}

prune_history_transactionally() {
  local prefix excess index original staged
  local -a history_files
  mkdir "$STAGE/pruned_history"
  for prefix in "${HISTORY_PREFIXES[@]}"; do
    mapfile -t history_files < <(
      find "$OUTPUT_ROOT" -maxdepth 1 -type f -name \
        "${prefix}_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].txt" -print | LC_ALL=C sort
    )
    excess=$((${#history_files[@]} - 14))
    if [[ "$excess" -le 0 ]]; then
      continue
    fi
    for ((index=0; index < excess; index++)); do
      original="${history_files[$index]}"
      staged="$STAGE/pruned_history/${prefix}_$index.txt"
      PRUNED_ORIGINALS+=("$original")
      PRUNED_STAGED+=("$staged")
      mv -- "$original" "$staged"
    done
  done
}

for relative in "${DICTIONARY_FILES[@]}"; do
  if [[ ! -f "$ROOT/$relative" ]]; then
    log "辞書ファイルがありません: $ROOT/$relative"
    exit 1
  fi
  sha256sum "$ROOT/$relative" >> "$STAGE/dictionary_sha256_before.txt"
done

log "########## suggest_and_cleanup start (RUN_DATE=$RUN_DATE_VALUE) ##########"
for index in "${!SUGGEST_SCRIPTS[@]}"; do
  candidate="$STAGE/${CANDIDATE_NAMES[$index]}"
  run_step "suggest_${CANDIDATE_NAMES[$index]}" env \
    DICTIONARY_CANDIDATES_FILE="$candidate" \
    RUN_DATE="$RUN_DATE_VALUE" \
    "$PYTHON_BIN" "$ROOT/${SUGGEST_SCRIPTS[$index]}"
done

for index in "${!CLEANUP_SCRIPTS[@]}"; do
  candidate="$STAGE/${CANDIDATE_NAMES[$index]}"
  run_step "cleanup_${CANDIDATE_NAMES[$index]}" env \
    DICTIONARY_CANDIDATES_FILE="$candidate" \
    "$PYTHON_BIN" "$ROOT/${CLEANUP_SCRIPTS[$index]}"
  if [[ ! -f "$candidate" ]]; then
    log "cleanup後の候補ファイルがありません: $candidate"
    exit 1
  fi
done

log "=== START dictionary exact-match checks ==="
diff -q "$ROOT/03-8_extract_project_skill_category/00_tool/skill_dictionary.txt" \
        "$ROOT/05-8_extract_resource_skill_category/00_tool/skill_dictionary.txt" >/dev/null
log "skill dictionary exact-match / phase dictionary SSOT: OK (location remains separately managed)"

for relative in "${DICTIONARY_FILES[@]}"; do
  sha256sum "$ROOT/$relative" >> "$STAGE/dictionary_sha256_after.txt"
done
if ! diff -q "$STAGE/dictionary_sha256_before.txt" "$STAGE/dictionary_sha256_after.txt" >/dev/null; then
  log "dictionary SHA-256 changed; refusing publish"
  exit 1
fi
log "dictionary SHA-256 unchanged: OK"

mkdir -p "$OUTPUT_ROOT"
for index in "${!CANDIDATE_NAMES[@]}"; do
  candidate="$STAGE/${CANDIDATE_NAMES[$index]}"
  prepare_publish_file "$candidate" "$ROOT/${FIXED_CANDIDATES[$index]}"
  prepare_publish_file "$candidate" "$OUTPUT_ROOT/${HISTORY_PREFIXES[$index]}_${RUN_DATE_VALUE}.txt"
done

publish_prepared_files
for index in "${!CANDIDATE_NAMES[@]}"; do
  candidate="$STAGE/${CANDIDATE_NAMES[$index]}"
  cmp -s "$candidate" "$ROOT/${FIXED_CANDIDATES[$index]}"
  cmp -s "$candidate" "$OUTPUT_ROOT/${HISTORY_PREFIXES[$index]}_${RUN_DATE_VALUE}.txt"
done
prune_history_transactionally
log "fixed candidates and dated histories published from the same cleaned stage: OK"
log "########## suggest_and_cleanup end (RUN_DATE=$RUN_DATE_VALUE) ##########"
COMMITTED=1

#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="/home/ec2-user/pipeline_ses_steps"
SCRIPT_DIR="$ROOT/00_pipeline/00_tool"
CONFIG_FILE="${PIPELINE_S3_CONFIG_FILE:-$SCRIPT_DIR/pipeline_s3_config.env}"
MANAGED_WRAPPER="${PIPELINE_MANAGED_WRAPPER:-$SCRIPT_DIR/run_full_pipeline_managed.sh}"
SYSTEMD_RUN_BIN="${PIPELINE_SYSTEMD_RUN_BIN:-/usr/bin/systemd-run}"
SYSTEMCTL_BIN="${PIPELINE_SYSTEMCTL_BIN:-/usr/bin/systemctl}"

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
if [[ ! -x "$SYSTEMD_RUN_BIN" || ! -x "$SYSTEMCTL_BIN" ]]; then
  echo "systemd-run or systemctl is not executable" >&2
  exit 2
fi
if [[ ! -r "$MANAGED_WRAPPER" ]]; then
  echo "managed wrapper is not readable: $MANAGED_WRAPPER" >&2
  exit 2
fi

UNIT_NAME="pipeline-ses-${RUN_DATE}-${RUN_ID}.service"

# A repeated SSM launcher call for the same execution is treated as already accepted.
if "$SYSTEMCTL_BIN" is-active --quiet "$UNIT_NAME"; then
  echo "managed pipeline unit is already active: $UNIT_NAME"
  exit 0
fi

systemd_args=(
  --unit="$UNIT_NAME"
  --description="SES pipeline $RUN_DATE $RUN_ID"
  --property=Type=exec
  --property=KillMode=control-group
  --property=TimeoutStopSec=120
  --collect
  --working-directory="$ROOT"
  --setenv="RUN_ID=$RUN_ID"
  --setenv="RUN_DATE=$RUN_DATE"
  --setenv="PIPELINE_S3_CONFIG_FILE=$CONFIG_FILE"
  --setenv="PIPELINE_S3_BUCKET=$PIPELINE_S3_BUCKET"
  --setenv="PIPELINE_S3_BASE_PREFIX=$PIPELINE_S3_BASE_PREFIX"
  --setenv="PIPELINE_STATUS_PREFIX=$PIPELINE_STATUS_PREFIX"
  --setenv="PIPELINE_LOG_PREFIX=$PIPELINE_LOG_PREFIX"
  --setenv="PIPELINE_AWS_REGION=$PIPELINE_AWS_REGION"
)

if [[ -n "$PIPELINE_SYSTEMD_USER" ]]; then
  systemd_args+=(--uid="$PIPELINE_SYSTEMD_USER")
fi

set +e
systemd_output="$($SYSTEMD_RUN_BIN "${systemd_args[@]}" /usr/bin/bash "$MANAGED_WRAPPER" 2>&1)"
systemd_exit_code=$?
set -e
printf '%s\n' "$systemd_output"

if [[ "$systemd_exit_code" -ne 0 ]]; then
  # Close the small race between the is-active check and systemd-run.
  if "$SYSTEMCTL_BIN" is-active --quiet "$UNIT_NAME"; then
    echo "managed pipeline unit became active concurrently: $UNIT_NAME"
    exit 0
  fi
  exit "$systemd_exit_code"
fi

echo "managed pipeline accepted: unit=$UNIT_NAME run_id=$RUN_ID run_date=$RUN_DATE"

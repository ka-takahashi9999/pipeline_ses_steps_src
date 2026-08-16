#!/usr/bin/env bash
#
# pipeline_aws_readonly.sh
#   SES Pipeline 調査用の AWS read-only ラッパー。
#   allow list 方式。未知のsubcommandはすべて拒否する（fail-closed）。
#
#   設計:
#   - 実行できるのは以下に列挙した read-only 操作のみ
#   - AWSリソース変更 / 秘密情報取得 / 書込み・削除は一切実装しない
#   - ユーザー引数を eval / bash -c / sh -c へ渡さない
#   - S3 は s3://technoverse/pipeline_ses_steps/ 配下のみ、s3-cat は stdout のみ
#   - region は ap-northeast-1 固定
#   - 正本と実行用コピーの sha256 不一致時は fail-closed（実行禁止）
#
#   正本   : /home/ec2-user/pipeline_ses_steps/tools/pipeline_aws_readonly.sh （Git管理対象 / sandbox外allowしない）
#   実行用 : /home/ec2-user/bin/pipeline_aws_readonly.sh （正規実行経路 / Codex rulesでallow）
#
# usage:
#   pipeline_aws_readonly.sh <subcommand> [args...]
#   pipeline_aws_readonly.sh --self-check
#   pipeline_aws_readonly.sh --help
#
set -euo pipefail

AWS_BIN="/usr/bin/aws"
REGION="ap-northeast-1"

S3_ROOT="s3://technoverse/pipeline_ses_steps/"
SFN_ACCOUNT="166714029268"
SFN_NAME="auto-match-llm-classifier-pipeline-orchestration"
SFN_STATE_MACHINE_ARN="arn:aws:states:${REGION}:${SFN_ACCOUNT}:stateMachine:${SFN_NAME}"
SFN_EXECUTION_PREFIX="arn:aws:states:${REGION}:${SFN_ACCOUNT}:execution:${SFN_NAME}:"

CANONICAL="/home/ec2-user/pipeline_ses_steps/tools/pipeline_aws_readonly.sh"
INSTALLED="/home/ec2-user/bin/pipeline_aws_readonly.sh"

die() { echo "[ERROR] $*" >&2; exit 1; }
info() { echo "[INFO] $*"; }
warn() { echo "[WARN] $*" >&2; }

usage() {
  cat <<'EOF'
usage: pipeline_aws_readonly.sh <subcommand> [args...]

SES Pipeline 調査用の AWS read-only ラッパー（allow list方式 / 変更操作は未実装）。
region は ap-northeast-1 固定。

S3（s3://technoverse/pipeline_ses_steps/ 配下のみ）:
  s3-cat <s3-uri>                       S3オブジェクトを stdout へ出力（ファイル出力・アップロード不可）
  s3-ls  <s3-uri> [--recursive] [--human-readable] [--summarize]

Step Functions（SES Pipeline の State Machine のみ / read-only）:
  sfn-list-executions [--max-items N] [--status-filter STATUS]
  sfn-describe-execution <execution-arn>
  sfn-get-execution-history <execution-arn> [--max-items N] [--reverse-order]

EC2（read-only）:
  ec2-describe-instances [i-xxxx ...]
  ec2-describe-instance-status [i-xxxx ...]

CloudTrail（read-only）:
  cloudtrail-lookup-events [--start-time T] [--end-time T] [--max-items N] [--event-name NAME]

その他:
  --self-check    正本と実行用コピーの sha256 一致を確認
  --help          このヘルプ

例:
  pipeline_aws_readonly.sh s3-ls s3://technoverse/pipeline_ses_steps/pipeline-logs/20260814/
  pipeline_aws_readonly.sh s3-cat s3://technoverse/pipeline_ses_steps/pipeline-logs/20260814/xxx/pipeline.log

大量ログはモデルへ直接流さず、ローカルで rg / awk / python3 により集計してから扱うこと。
EOF
}

# ---- 正本と実行用コピーの checksum 照合 ----
self_check() {
  local quiet="${1:-}" a b
  if [ ! -f "$CANONICAL" ]; then
    [ "$quiet" = quiet ] || warn "正本が見つかりません: $CANONICAL"; return 2
  fi
  if [ ! -f "$INSTALLED" ]; then
    [ "$quiet" = quiet ] || warn "実行用コピーが見つかりません: $INSTALLED"; return 2
  fi
  a="$(sha256sum "$CANONICAL" | awk '{print $1}')"
  b="$(sha256sum "$INSTALLED" | awk '{print $1}')"
  if [ "$a" = "$b" ]; then
    [ "$quiet" = quiet ] || info "self-check OK: 正本と実行用コピーは一致 ($a)"
    return 0
  fi
  if [ "$quiet" != quiet ]; then
    warn "self-check NG: 正本と実行用コピーが不一致"
    warn "  正本   $CANONICAL  $a"
    warn "  実行用 $INSTALLED  $b"
    warn "  反映する場合: cp -p $CANONICAL $INSTALLED"
  fi
  return 1
}

# ---- 入力検証 ----
require_num() {
  [[ "$1" =~ ^[0-9]+$ ]] || die "数値以外は指定できません: $1"
}
require_instance_id() {
  [[ "$1" =~ ^i-[0-9a-f]{8,17}$ ]] || die "instance-id の形式が不正です: $1"
}
require_time() {
  [[ "$1" =~ ^[0-9TZ:.+-]{4,40}$ ]] || die "時刻の形式が不正です: $1"
}
require_name() {
  [[ "$1" =~ ^[A-Za-z0-9_-]{1,128}$ ]] || die "名称の形式が不正です: $1"
}
require_s3_uri() {
  local uri="$1"
  case "$uri" in
    *..*) die "'..' を含むS3 URIは指定できません: $uri" ;;
  esac
  case "$uri" in
    "$S3_ROOT"*) : ;;
    *) die "許可されていないS3パスです（$S3_ROOT 配下のみ）: $uri" ;;
  esac
}
require_execution_arn() {
  local arn="$1"
  case "$arn" in
    "$SFN_EXECUTION_PREFIX"*) : ;;
    *) die "SES Pipeline の execution ARN ではありません: $arn" ;;
  esac
  [[ "$arn" =~ ^[A-Za-z0-9:_/.-]+$ ]] || die "execution ARN の形式が不正です: $arn"
}

[ $# -ge 1 ] || { usage; die "subcommand が指定されていません"; }

CMD="$1"; shift

case "$CMD" in
  --self-check) self_check; exit $? ;;
  -h|--help) usage; exit 0 ;;
esac

# ---- checksum は fail-closed ----
if ! self_check quiet; then
  self_check || true
  die "正本と実行用コピーの整合が取れないため実行を中止しました"
fi

[ -x "$AWS_BIN" ] || die "aws CLI が見つかりません: $AWS_BIN"

case "$CMD" in
  s3-cat)
    [ $# -eq 1 ] || die "usage: s3-cat <s3-uri>"
    require_s3_uri "$1"
    case "$1" in
      */) die "ディレクトリ相当のURIは s3-cat できません: $1" ;;
    esac
    # 出力先は stdout 固定（ローカルファイル出力・アップロードは不可）
    exec "$AWS_BIN" s3 cp "$1" - --region "$REGION"
    ;;

  s3-ls)
    [ $# -ge 1 ] || die "usage: s3-ls <s3-uri> [--recursive] [--human-readable] [--summarize]"
    require_s3_uri "$1"
    args=("s3" "ls" "$1" "--region" "$REGION")
    shift
    while [ $# -gt 0 ]; do
      case "$1" in
        --recursive|--human-readable|--summarize) args+=("$1"); shift ;;
        *) die "s3-ls で許可されていないオプションです: $1" ;;
      esac
    done
    exec "$AWS_BIN" "${args[@]}"
    ;;

  sfn-list-executions)
    args=("stepfunctions" "list-executions" "--state-machine-arn" "$SFN_STATE_MACHINE_ARN" "--region" "$REGION")
    while [ $# -gt 0 ]; do
      case "$1" in
        --max-items) [ $# -ge 2 ] || die "--max-items に値がありません"; require_num "$2"; args+=("--max-items" "$2"); shift 2 ;;
        --status-filter) [ $# -ge 2 ] || die "--status-filter に値がありません"; require_name "$2"; args+=("--status-filter" "$2"); shift 2 ;;
        *) die "sfn-list-executions で許可されていないオプションです: $1" ;;
      esac
    done
    exec "$AWS_BIN" "${args[@]}"
    ;;

  sfn-describe-execution)
    [ $# -eq 1 ] || die "usage: sfn-describe-execution <execution-arn>"
    require_execution_arn "$1"
    exec "$AWS_BIN" stepfunctions describe-execution --execution-arn "$1" --region "$REGION"
    ;;

  sfn-get-execution-history)
    [ $# -ge 1 ] || die "usage: sfn-get-execution-history <execution-arn> [--max-items N] [--reverse-order]"
    require_execution_arn "$1"
    args=("stepfunctions" "get-execution-history" "--execution-arn" "$1" "--region" "$REGION")
    shift
    while [ $# -gt 0 ]; do
      case "$1" in
        --max-items) [ $# -ge 2 ] || die "--max-items に値がありません"; require_num "$2"; args+=("--max-items" "$2"); shift 2 ;;
        --reverse-order) args+=("$1"); shift ;;
        *) die "sfn-get-execution-history で許可されていないオプションです: $1" ;;
      esac
    done
    exec "$AWS_BIN" "${args[@]}"
    ;;

  ec2-describe-instances|ec2-describe-instance-status)
    sub="describe-instances"
    [ "$CMD" = "ec2-describe-instance-status" ] && sub="describe-instance-status"
    args=("ec2" "$sub" "--region" "$REGION")
    if [ $# -gt 0 ]; then
      args+=("--instance-ids")
      while [ $# -gt 0 ]; do
        require_instance_id "$1"
        args+=("$1"); shift
      done
    fi
    exec "$AWS_BIN" "${args[@]}"
    ;;

  cloudtrail-lookup-events)
    args=("cloudtrail" "lookup-events" "--region" "$REGION")
    while [ $# -gt 0 ]; do
      case "$1" in
        --start-time) [ $# -ge 2 ] || die "--start-time に値がありません"; require_time "$2"; args+=("--start-time" "$2"); shift 2 ;;
        --end-time) [ $# -ge 2 ] || die "--end-time に値がありません"; require_time "$2"; args+=("--end-time" "$2"); shift 2 ;;
        --max-items) [ $# -ge 2 ] || die "--max-items に値がありません"; require_num "$2"; args+=("--max-items" "$2"); shift 2 ;;
        --event-name) [ $# -ge 2 ] || die "--event-name に値がありません"; require_name "$2"
          args+=("--lookup-attributes" "AttributeKey=EventName,AttributeValue=$2"); shift 2 ;;
        *) die "cloudtrail-lookup-events で許可されていないオプションです: $1" ;;
      esac
    done
    exec "$AWS_BIN" "${args[@]}"
    ;;

  *)
    usage >&2
    die "許可されていないsubcommandです: $CMD"
    ;;
esac

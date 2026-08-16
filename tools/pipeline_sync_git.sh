#!/usr/bin/env bash
#
# pipeline_sync_git.sh
#   pipeline_ses_steps の「明示した対象パスだけ」を pipeline_ses_steps_src へ同期し、
#   diff確認 → 対象限定add → commit → 通常push まで行う正規経路。
#
#   - 全同期はしない（対象パスの明示が必須）
#   - 生成物は除外する
#   - `..` / 絶対パス / SRC外への脱出は拒否する
#   - 同期対象外の想定外Git変更があれば停止する
#   - force push / reset --hard / clean の機能は持たない
#
#   正本   : /home/ec2-user/pipeline_ses_steps/tools/pipeline_sync_git.sh （Git管理対象）
#   実行用 : /home/ec2-user/bin/pipeline_sync_git.sh （正本のコピー）
#   両者の一致は `--self-check` で確認する。
#
# usage:
#   pipeline_sync_git.sh [--dry-run] [--no-push] [--prune] [-m "message"] <相対パス>...
#   pipeline_sync_git.sh --self-check
#
set -euo pipefail

SRC="/home/ec2-user/pipeline_ses_steps"
DST="/home/ec2-user/pipeline_ses_steps_src"

CANONICAL="$SRC/tools/pipeline_sync_git.sh"
INSTALLED="/home/ec2-user/bin/pipeline_sync_git.sh"

DRY_RUN=0
NO_PUSH=0
PRUNE=0
COMMIT_MSG=""
TARGETS=()

# 生成物・同期対象外
EXCLUDE_DIRS=(01_result 02_confirm 99_execution_time __pycache__ .git .venv node_modules)
EXCLUDE_GLOBS=('*.jsonl' '*.json' '*.log' '*.pyc' '*.zip' '*.gz' 'nohup.out' 'settings.local.json')

# 除外パターンに該当しても同期する例外（相対パス完全一致）
INCLUDE_ALWAYS=('.claude/settings.json')

usage() {
  cat <<'EOF'
usage: pipeline_sync_git.sh [--dry-run] [--no-push] [-m "message"] <相対パス>...

  <相対パス>  /home/ec2-user/pipeline_ses_steps からの相対パス（ファイル / ディレクトリ）
  --dry-run   同期・git操作を行わず、対象と除外だけ表示する
  --no-push   commit まで行い push しない
  --prune     指定した「ディレクトリ」配下に限り、SRC に存在しないファイルを DST から削除する
              （ディレクトリ配下限定。それ以外は削除しない）
  --self-check   正本と実行用コピーの sha256 一致だけを確認して終了する
  -m, --message  commit message（省略時は自動生成）

例:
  pipeline_sync_git.sh --dry-run AGENTS.md CLAUDE.md .agents/skills
  pipeline_sync_git.sh -m "Update agent design" AGENTS.md docs
  pipeline_sync_git.sh --self-check
EOF
}

die() { echo "[ERROR] $*" >&2; exit 1; }
info() { echo "[INFO] $*"; }
warn() { echo "[WARN] $*" >&2; }

# 正本と実行用コピーの checksum 照合
self_check() {
  local a b
  [ -f "$CANONICAL" ] || { warn "正本が見つかりません: $CANONICAL"; return 2; }
  [ -f "$INSTALLED" ] || { warn "実行用コピーが見つかりません: $INSTALLED"; return 2; }
  a="$(sha256sum "$CANONICAL" | awk '{print $1}')"
  b="$(sha256sum "$INSTALLED" | awk '{print $1}')"
  if [ "$a" = "$b" ]; then
    info "self-check OK: 正本と実行用コピーは一致 ($a)"
    return 0
  fi
  warn "self-check NG: 正本と実行用コピーが不一致"
  warn "  正本   $CANONICAL  $a"
  warn "  実行用 $INSTALLED  $b"
  warn "  同期する場合: cp -p $CANONICAL $INSTALLED"
  return 1
}

# ---- 引数解析 ----
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --no-push) NO_PUSH=1; shift ;;
    --prune) PRUNE=1; shift ;;
    --self-check) self_check; exit $? ;;
    -m|--message) [ $# -ge 2 ] || die "-m にメッセージがありません"; COMMIT_MSG="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    --) shift; while [ $# -gt 0 ]; do TARGETS+=("$1"); shift; done ;;
    -*) die "不明なオプション: $1" ;;
    *) TARGETS+=("$1"); shift ;;
  esac
done

[ "${#TARGETS[@]}" -gt 0 ] || { usage; die "対象パスが指定されていません（全同期は行いません）"; }

# 正本と実行用コピーのズレは警告のみ（実行は止めない）
self_check >/dev/null 2>&1 || warn "正本と実行用コピーが不一致、または一方が欠落しています（--self-check で確認）"

[ -d "$SRC" ] || die "SRC が存在しません: $SRC"
[ -d "$DST/.git" ] || die "DST がGitリポジトリではありません: $DST"

# ---- 除外判定 ----
is_excluded() {
  local rel="$1" seg name inc
  for inc in "${INCLUDE_ALWAYS[@]}"; do
    [ "$rel" = "$inc" ] && return 1
  done
  name="$(basename "$rel")"
  local IFS='/'
  # shellcheck disable=SC2206
  local parts=($rel)
  unset IFS
  for seg in "${parts[@]}"; do
    for d in "${EXCLUDE_DIRS[@]}"; do
      [ "$seg" = "$d" ] && return 0
    done
  done
  for g in "${EXCLUDE_GLOBS[@]}"; do
    # shellcheck disable=SC2053
    [[ "$name" == $g ]] && return 0
  done
  return 1
}

# ---- 対象パス検証・正規化 ----
NORM_TARGETS=()
for t in "${TARGETS[@]}"; do
  [ -n "$t" ] || die "空のパスが指定されました"
  case "$t" in
    /*) die "絶対パスは指定できません: $t" ;;
    *..*) die "'..' を含むパスは指定できません: $t" ;;
  esac
  rel="${t#./}"
  rel="${rel%/}"
  [ -n "$rel" ] || die "リポジトリルート全体は同期対象にできません"
  [ -e "$SRC/$rel" ] || die "存在しません: $SRC/$rel"

  real="$(readlink -f "$SRC/$rel")" || die "パス解決に失敗: $rel"
  case "$real" in
    "$SRC"/*) : ;;
    *) die "SRC 外を指しています: $rel -> $real" ;;
  esac

  if is_excluded "$rel"; then
    die "同期対象外（生成物 / 除外パターン）: $rel"
  fi
  NORM_TARGETS+=("$rel")
done

# ---- 同期ファイル一覧の作成 ----
FILES=()
SKIPPED=()
for rel in "${NORM_TARGETS[@]}"; do
  if [ -d "$SRC/$rel" ] && [ ! -L "$SRC/$rel" ]; then
    while IFS= read -r f; do
      sub="${f#"$SRC"/}"
      if is_excluded "$sub"; then
        SKIPPED+=("$sub")
      else
        FILES+=("$sub")
      fi
    done < <(find "$SRC/$rel" \( -type f -o -type l \) | sort)
  else
    FILES+=("$rel")
  fi
done

[ "${#FILES[@]}" -gt 0 ] || die "同期対象ファイルが0件です"

# ---- prune 対象（ディレクトリ指定分のみ）----
PRUNE_FILES=()
if [ "$PRUNE" -eq 1 ]; then
  for rel in "${NORM_TARGETS[@]}"; do
    [ -d "$SRC/$rel" ] && [ ! -L "$SRC/$rel" ] || continue
    [ -d "$DST/$rel" ] || continue
    while IFS= read -r f; do
      sub="${f#"$DST"/}"
      is_excluded "$sub" && continue
      [ -e "$SRC/$sub" ] || [ -L "$SRC/$sub" ] || PRUNE_FILES+=("$sub")
    done < <(find "$DST/$rel" \( -type f -o -type l \) | sort)
  done
fi

echo "--- 同期対象 (${#FILES[@]}件) ---"
printf '  %s\n' "${FILES[@]}"
if [ "${#PRUNE_FILES[@]}" -gt 0 ]; then
  echo "--- 削除対象 / prune (${#PRUNE_FILES[@]}件) ---"
  printf '  %s\n' "${PRUNE_FILES[@]}"
fi
if [ "${#SKIPPED[@]}" -gt 0 ]; then
  echo "--- 除外 (${#SKIPPED[@]}件) ---"
  printf '  %s\n' "${SKIPPED[@]}"
fi

# ---- 想定外のGit変更チェック ----
echo "--- 事前 git status チェック ---"
unexpected=0
while IFS= read -r line; do
  [ -n "$line" ] || continue
  st="${line:0:2}"
  p="${line:3}"
  case "$st" in
    R*|C*) echo "  [想定外] rename/copy 検出: $line"; unexpected=1; continue ;;
  esac
  ok=0
  for rel in "${NORM_TARGETS[@]}"; do
    if [ "$p" = "$rel" ] || [ "${p#"$rel"/}" != "$p" ]; then ok=1; break; fi
  done
  if [ "$ok" -eq 0 ]; then
    echo "  [想定外] $line"
    unexpected=1
  fi
done < <(git -C "$DST" -c core.quotepath=false status --porcelain -uall)

if [ "$unexpected" -eq 1 ]; then
  die "同期対象外の変更が _src に存在します。人間が確認してください（本スクリプトは何も変更していません）"
fi
info "想定外のGit変更なし"

if [ "$DRY_RUN" -eq 1 ]; then
  info "dry-run のため、ここで終了します（同期・git操作は行っていません）"
  exit 0
fi

# ---- 選択同期 ----
for f in "${FILES[@]}"; do
  mkdir -p "$DST/$(dirname "$f")"
  cp -Pf "$SRC/$f" "$DST/$f"
done
info "同期完了: ${#FILES[@]}件"

# ---- prune 実行 ----
if [ "${#PRUNE_FILES[@]}" -gt 0 ]; then
  for f in "${PRUNE_FILES[@]}"; do
    if git -C "$DST" ls-files --error-unmatch -- "$f" >/dev/null 2>&1; then
      git -C "$DST" rm -q -f -- "$f"
    else
      rm -f "$DST/$f"
    fi
  done
  info "prune完了: ${#PRUNE_FILES[@]}件"
  # 空になったディレクトリの掃除は prune 指定した対象ディレクトリ配下に限定する
  for rel in "${NORM_TARGETS[@]}"; do
    [ -d "$DST/$rel" ] || continue
    find "$DST/$rel" -mindepth 1 -type d -empty -delete 2>/dev/null || true
  done
fi

# ---- diff 確認 ----
echo "--- git status ---"
git -C "$DST" -c core.quotepath=false status --short
echo "--- git add (対象限定) ---"
git -C "$DST" add -- "${NORM_TARGETS[@]}"
echo "--- git diff --cached --stat ---"
git -C "$DST" diff --cached --stat
echo "--- git diff --check ---"
git -C "$DST" diff --cached --check

if git -C "$DST" diff --cached --quiet; then
  info "コミット対象の差分がありません。終了します。"
  exit 0
fi

# ---- commit ----
if [ -z "$COMMIT_MSG" ]; then
  COMMIT_MSG="Sync pipeline_ses_steps $(date '+%Y%m%d_%H%M%S')"
fi
git -C "$DST" commit -m "$COMMIT_MSG"
info "commit: $(git -C "$DST" rev-parse --short HEAD)"

# ---- push（通常pushのみ / force不可） ----
if [ "$NO_PUSH" -eq 1 ]; then
  info "--no-push 指定のため push しません"
  exit 0
fi
git -C "$DST" push
info "push 完了"

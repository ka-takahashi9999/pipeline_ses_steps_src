#!/usr/bin/env bash
#
# pipeline_sync_git.sh
#   pipeline_ses_steps の「明示した対象パスだけ」を pipeline_ses_steps_src へ同期し、
#   diff確認 → 対象ファイル限定add → commit → 通常push まで行う正規経路。
#
#   安全仕様:
#   - 全同期はしない（対象パスの明示が必須）
#   - ディレクトリ入力は内部で具体的ファイルへ展開し、以降はすべてファイル単位で処理する
#   - 生成物は除外する
#   - `..` / 絶対パス / SRC外・DST外へ抜けるsymlink経路は拒否する
#   - 同期対象ファイル以外の変更・untrackedが _src に1件でもあれば停止する
#   - git add は具体的ファイルのみ（`git add .` / `git add -A` / ディレクトリ指定はしない）
#   - 書き込み前に全対象をvalidationし、1件でも違反があれば何も変更せず停止する
#   - 正本と実行用コピーの checksum 不一致時は fail-closed（何もせず異常終了）
#   - force push / reset --hard / clean の機能は持たない
#
#   正本   : /home/ec2-user/pipeline_ses_steps/tools/pipeline_sync_git.sh （Git管理対象）
#   実行用 : /home/ec2-user/bin/pipeline_sync_git.sh （正規実行経路 / Codex rulesでallow）
#
#   標準フロー（2段階）:
#     1. pipeline_sync_git.sh --dry-run <対象パス>...
#     2. pipeline_sync_git.sh -m "message" <対象パス>...   # 同期→stage→diff→commit→push
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
usage: pipeline_sync_git.sh [--dry-run] [--no-push] [--prune] [-m "message"] <相対パス>...
       pipeline_sync_git.sh --self-check

  <相対パス>  /home/ec2-user/pipeline_ses_steps からの相対パス（ファイル / ディレクトリ）
              ディレクトリは内部で具体的ファイルへ展開される
  --dry-run   同期・git操作を行わず、対象 / 除外 / 削除対象だけ表示する
  --no-push   commit まで行い push しない（標準フローでは使わない）
  --prune     指定したディレクトリ配下に限り、SRC に存在しないファイルを DST から削除する
  --self-check  正本と実行用コピーの sha256 一致だけを確認して終了する
  -m, --message  commit message（省略時は自動生成）

標準フロー:
  pipeline_sync_git.sh --dry-run AGENTS.md .agents/skills     # 1. 内容確認
  pipeline_sync_git.sh -m "Update skills" AGENTS.md .agents/skills  # 2. commit+pushまで
EOF
}

die() { echo "[ERROR] $*" >&2; exit 1; }
info() { echo "[INFO] $*"; }
warn() { echo "[WARN] $*" >&2; }

# ---- 正本と実行用コピーの checksum 照合 ----
# $1: quiet で出力抑制
self_check() {
  local quiet="${1:-}" a b
  if [ ! -f "$CANONICAL" ]; then
    [ "$quiet" = quiet ] || warn "正本が見つかりません: $CANONICAL"
    return 2
  fi
  if [ ! -f "$INSTALLED" ]; then
    [ "$quiet" = quiet ] || warn "実行用コピーが見つかりません: $INSTALLED"
    return 2
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

# ---- checksum は fail-closed（不一致なら何もせず異常終了）----
if ! self_check quiet; then
  self_check || true
  die "正本と実行用コピーの整合が取れないため実行を中止しました（同期 / commit / push は行っていません）"
fi

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

# ---- 途中の親ディレクトリがsymlinkでないことを確認 ----
# $1: base, $2: 相対パス  -> 中間componentにsymlinkがあれば 1
check_parent_chain() {
  local base="$1" rel="$2" i n cur
  cur="$base"
  local IFS='/'
  # shellcheck disable=SC2206
  local parts=($rel)
  unset IFS
  n=${#parts[@]}
  for (( i=0; i<n-1; i++ )); do
    cur="$cur/${parts[$i]}"
    [ -L "$cur" ] && return 1
  done
  return 0
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

# ---- ディレクトリを具体的ファイルへ展開（以降はすべてファイル単位）----
declare -A FILE_SET=()
FILES=()
SKIPPED=()
add_file() {
  local rel="$1"
  [ -n "${FILE_SET[$rel]:-}" ] && return 0
  FILE_SET["$rel"]=1
  FILES+=("$rel")
}
for rel in "${NORM_TARGETS[@]}"; do
  if [ -d "$SRC/$rel" ] && [ ! -L "$SRC/$rel" ]; then
    while IFS= read -r f; do
      sub="${f#"$SRC"/}"
      if is_excluded "$sub"; then
        SKIPPED+=("$sub")
      else
        add_file "$sub"
      fi
    done < <(find "$SRC/$rel" \( -type f -o -type l \) | sort)
  else
    add_file "$rel"
  fi
done

[ "${#FILES[@]}" -gt 0 ] || die "同期対象ファイルが0件です"

# ---- prune 対象（ディレクトリ指定分のみ / ファイル単位）----
declare -A PRUNE_SET=()
PRUNE_FILES=()
if [ "$PRUNE" -eq 1 ]; then
  for rel in "${NORM_TARGETS[@]}"; do
    [ -d "$SRC/$rel" ] && [ ! -L "$SRC/$rel" ] || continue
    [ -d "$DST/$rel" ] && [ ! -L "$DST/$rel" ] || continue
    while IFS= read -r f; do
      sub="${f#"$DST"/}"
      is_excluded "$sub" && continue
      if [ ! -e "$SRC/$sub" ] && [ ! -L "$SRC/$sub" ]; then
        [ -n "${PRUNE_SET[$sub]:-}" ] && continue
        PRUNE_SET["$sub"]=1
        PRUNE_FILES+=("$sub")
      fi
    done < <(find "$DST/$rel" \( -type f -o -type l \) | sort)
  done
fi

# ---- 許可ファイル集合（この完全一致リスト以外の _src 変更は許容しない）----
declare -A ALLOWED=()
for f in "${FILES[@]}"; do ALLOWED["$f"]=1; done
for f in "${PRUNE_FILES[@]}"; do ALLOWED["$f"]=1; done

# ---- 書き込み前 validation（1件でも違反があれば何も変更せず停止）----
violations=0
for f in "${FILES[@]}"; do
  # SRC: 中間親がsymlinkでないこと
  if ! check_parent_chain "$SRC" "$f"; then
    echo "  [違反] SRC の親ディレクトリがsymlink: $f" >&2; violations=1; continue
  fi
  # SRC: symlink自体のコピーは許可するが、解決先がSRC外なら拒否
  if [ -L "$SRC/$f" ]; then
    if ! sreal="$(readlink -f "$SRC/$f")"; then
      echo "  [違反] symlinkを解決できません: $f" >&2; violations=1; continue
    fi
    case "$sreal" in
      "$SRC"/*) : ;;
      *) echo "  [違反] SRC外を指すsymlink: $f -> $sreal" >&2; violations=1; continue ;;
    esac
  fi
  # DST: 中間親がsymlinkでDST外へ抜けないこと
  if ! check_parent_chain "$DST" "$f"; then
    echo "  [違反] DST の親ディレクトリがsymlink: $f" >&2; violations=1
  fi
done
for f in "${PRUNE_FILES[@]}"; do
  if ! check_parent_chain "$DST" "$f"; then
    echo "  [違反] DST の親ディレクトリがsymlink（prune対象）: $f" >&2; violations=1
  fi
done
[ "$violations" -eq 0 ] || die "パス検証に失敗しました。何も変更していません"

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

# ---- 想定外のGit変更チェック（ファイル単位の完全一致）----
echo "--- 事前 git status チェック ---"
unexpected=0
while IFS= read -r line; do
  [ -n "$line" ] || continue
  st="${line:0:2}"
  p="${line:3}"
  case "$st" in
    R*|C*) echo "  [想定外] rename/copy 検出: $line" >&2; unexpected=1; continue ;;
  esac
  if [ -z "${ALLOWED[$p]:-}" ]; then
    echo "  [想定外] $line" >&2
    unexpected=1
  fi
done < <(git -C "$DST" -c core.quotepath=false status --porcelain -uall)

if [ "$unexpected" -eq 1 ]; then
  die "同期対象ファイル以外の変更が _src に存在します。人間が確認してください（本スクリプトは何も変更していません）"
fi
info "想定外のGit変更なし"

if [ "$DRY_RUN" -eq 1 ]; then
  info "dry-run のため、ここで終了します（同期・git操作は行っていません）"
  exit 0
fi

# ---- 選択同期 ----
for f in "${FILES[@]}"; do
  mkdir -p "$DST/$(dirname "$f")"
  # 宛先が既存symlink（特にディレクトリへのsymlink）だと cp がその中へ書き込むため、先に外す
  [ -L "$DST/$f" ] && rm -f "$DST/$f"
  cp -PfT "$SRC/$f" "$DST/$f"
done
info "同期完了: ${#FILES[@]}件"

# ---- prune 実行（ファイル単位）----
if [ "${#PRUNE_FILES[@]}" -gt 0 ]; then
  for f in "${PRUNE_FILES[@]}"; do
    if git -C "$DST" ls-files --error-unmatch -- "$f" >/dev/null 2>&1; then
      git -C "$DST" rm -q -f -- "$f"
    else
      rm -f "$DST/$f"
    fi
  done
  info "prune完了: ${#PRUNE_FILES[@]}件"
  for rel in "${NORM_TARGETS[@]}"; do
    [ -d "$DST/$rel" ] && [ ! -L "$DST/$rel" ] || continue
    find "$DST/$rel" -mindepth 1 -type d -empty -delete 2>/dev/null || true
  done
fi

# ---- stage（具体的ファイルのみ。ディレクトリ / . / -A は使わない）----
echo "--- git add (ファイル単位: ${#ALLOWED[@]}件) ---"
STAGE_FILES=("${FILES[@]}")
[ "${#PRUNE_FILES[@]}" -gt 0 ] && STAGE_FILES+=("${PRUNE_FILES[@]}")
git -C "$DST" add -- "${STAGE_FILES[@]}"

# ---- 内容確認 ----
echo "--- git status ---"
git -C "$DST" -c core.quotepath=false status --short
echo "--- git diff --cached --stat ---"
git -C "$DST" diff --cached --stat
echo "--- git diff --cached (先頭200行) ---"
# head で打ち切ると SIGPIPE になるため pipefail の影響を打ち消す
{ git -C "$DST" diff --cached || true; } | head -200 || true
echo "--- git diff --check ---"
git -C "$DST" diff --cached --check

# ---- commit ----
if git -C "$DST" diff --cached --quiet; then
  info "コミット対象の差分はありません"
else
  if [ -z "$COMMIT_MSG" ]; then
    COMMIT_MSG="Sync pipeline_ses_steps $(date '+%Y%m%d_%H%M%S')"
  fi
  git -C "$DST" commit -m "$COMMIT_MSG"
  info "commit: $(git -C "$DST" rev-parse --short HEAD)"
fi

# ---- push（通常pushのみ / force不可）----
if [ "$NO_PUSH" -eq 1 ]; then
  info "--no-push 指定のため push しません"
  exit 0
fi

ahead="$(git -C "$DST" rev-list --count '@{u}..HEAD' 2>/dev/null || echo 0)"
if [ "$ahead" -eq 0 ]; then
  info "push対象のcommitはありません"
  exit 0
fi
info "未push commit: ${ahead}件"
git -C "$DST" push
info "push 完了"

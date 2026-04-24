#!/usr/bin/env bash
set -euo pipefail

SRC="/home/ec2-user/pipeline_ses_steps"
DST="/home/ec2-user/pipeline_ses_steps_src"

echo "[INFO] sync start"

mkdir -p "$DST"

# 共通ファイル
cp "$SRC/AGENTS.md" "$DST/"

if [ -d "$SRC/common" ]; then
  rm -rf "$DST/common"
  cp -r "$SRC/common" "$DST/"
fi

# 各 step の 00_tool を同期
for d in "$SRC"/*/; do
  [ -d "$d" ] || continue
  step_name="$(basename "$d")"

  if [ -d "${d}00_tool" ]; then
    mkdir -p "$DST/$step_name"
    rm -rf "$DST/$step_name/00_tool"
    cp -r "${d}00_tool" "$DST/$step_name/"
  fi
done

# 任意: .agents/skills を同期したい場合
if [ -d "$SRC/.agents/skills" ]; then
  mkdir -p "$DST/.agents"
  rm -rf "$DST/.agents/skills"
  cp -r "$SRC/.agents/skills" "$DST/.agents/"
fi

# 任意: .codex/config.toml を同期したい場合
if [ -f "$SRC/.codex/config.toml" ]; then
  mkdir -p "$DST/.codex"
  cp "$SRC/.codex/config.toml" "$DST/.codex/"
fi

echo "[INFO] sync done"

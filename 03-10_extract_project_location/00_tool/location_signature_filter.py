#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
署名ブロック除去モジュール

SES案件メールの末尾にある署名ブロック（TEL/FAX/Email/会社住所等）を
本文から切り落とすことで、署名由来の地名誤抽出を防ぐ。

方針:
  ・本文中の最後の「案件情報セクションヘッダー」を特定し、
    それより後ろにある署名マーカーのみを対象にする
  ・案件情報セクション内の E-mail / TEL は除去しない
"""

import re
from typing import List, Optional

# ── 強い署名マーカー ──────────────────────────────────────
_STRONG_SIG_RE = re.compile(
    r"TEL\s*[:：]|FAX\s*[:：]|"           # TEL: / FAX: (コロン必須)
    r"E-?[Mm]ail\s*[:：]|"               # Email: / E-mail:
    r"メールアドレス\s*[:：]|"
    r"電話番号\s*[:：]|携帯番号\s*[:：]|"
    r"配信停止|unsubscribe|"
    r"派遣事業許可|労働者派遣事業|"
    r"共通アドレス|代表番号",
    re.IGNORECASE,
)

# ── 弱い署名マーカー ──────────────────────────────────────
_WEAK_SIG_RE = re.compile(
    r"^(株式会社|合同会社|有限会社|一般社団法人|NPO法人)|"
    r"^(本社|営業所|支社|〒\d)",
)

# ── セパレータ ─────────────────────────────────────────────
_SEP_RE = re.compile(r"^[-=_]{2,}\s*$")

# ── 案件情報セクションヘッダー ────────────────────────────
# 【...】 ◆ ■ □ 等のフォーマットを持つ行が案件本文の目印
_CASE_HDR_RE = re.compile(
    r"^\s*(?:[【◆■□※▼▶★]|\[.+?\]|^[（(]\d+[）)]\s*\S)"
)


def _find_last_case_header(lines: List[str]) -> int:
    """
    案件情報セクションヘッダーが現れる最後の行インデックスを返す。
    見つからない場合は 0 を返す。
    """
    last_idx = 0
    for i, line in enumerate(lines):
        if _CASE_HDR_RE.match(line):
            last_idx = i
    return last_idx


def remove_signature(body: str) -> str:
    """
    本文末尾の署名ブロックを除去して返す。

    アルゴリズム:
      1. 最後の「案件ヘッダー行」を特定（以降を署名検索の対象外に）
      2. 案件ヘッダーより5行以上後ろで強い署名マーカーを検索
      3. 見つかれば末尾側のブロックを切り落とす
      4. 案件ヘッダーと署名ブロック開始が2行以内なら除去しない（安全策）

    Args:
        body: メール本文（改行含む）

    Returns:
        署名を除去した本文。署名が見つからない or 過剰除去になる場合は原文を返す。
    """
    if not body:
        return body

    lines = body.splitlines()
    n = len(lines)
    if n < 5:
        return body

    # ── Step 1: 最後の案件ヘッダー位置を特定 ────────────────
    last_case_idx = _find_last_case_header(lines)

    # 署名を探す範囲は「案件ヘッダーより 5行後」から末尾まで
    search_from = last_case_idx + 5

    if search_from >= n:
        return body  # 本文が短すぎて署名を探す余地なし

    # ── Step 2: 強い署名マーカーを末尾から検索 ───────────────
    last_strong_idx: Optional[int] = None
    for i in range(n - 1, search_from - 1, -1):
        if _STRONG_SIG_RE.search(lines[i]):
            last_strong_idx = i
            break

    if last_strong_idx is None:
        return body

    # ── Step 3: 署名ブロックの開始位置を特定 ─────────────────
    # 強いマーカーから前方へ遡り、署名ブロックの先頭を見つける
    block_start = last_strong_idx
    lookback_limit = max(search_from, last_strong_idx - 20)
    for i in range(last_strong_idx - 1, lookback_limit - 1, -1):
        line = lines[i].strip()

        if not line:
            block_start = i
            continue
        if _SEP_RE.match(line):
            block_start = i
            continue
        if _STRONG_SIG_RE.search(line):
            block_start = i
            continue
        if _WEAK_SIG_RE.match(line):
            block_start = i
            continue
        if re.match(r"^\s*(https?://|www\.)\S+\s*$", line, re.IGNORECASE):
            block_start = i
            continue

        # 案件本文らしい行に達した → ここで停止
        break

    # ── Step 4: 安全チェック ──────────────────────────────────
    # block_start が案件ヘッダーに近すぎる場合は除去しない
    if block_start <= last_case_idx + 2:
        return body

    trimmed = "\n".join(lines[:block_start])
    return trimmed if trimmed.strip() else body

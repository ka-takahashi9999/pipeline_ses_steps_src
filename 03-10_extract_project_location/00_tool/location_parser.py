#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ロケーション抽出パーサーモジュール

抽出フロー:
  1. 場所ラベル行 + 次行 からキャンディデート取得（最優先）
  2. キャンディデートを正規化（リモート/出社頻度ノイズを除去）
  3. 辞書マッチ（token に対して）
  4. ラベルで取れない場合は本文前半への辞書マッチ（既存動作維持）

出力:
  (location, location_raw, location_source)
  location_source: "label" | "body"
"""

import re
import unicodedata
from typing import Dict, List, Optional, Tuple

# ── ラベル検出 ────────────────────────────────────────────
_LABEL_RE = re.compile(r"(場\s*所|勤務地|作業場所|勤務場所|就業場所|最寄|最寄駅|拠点)")

# ── ノイズ除去パターン ────────────────────────────────────
# 場所以外の情報（リモート条件・出社頻度・補足）を除去
_NOISE_RE = re.compile(
    r"フル\s*リモート?|"
    r"基本\s*リモート|"
    r"原則\s*リモート|"
    r"テレワーク|"
    r"在宅\s*(?:勤務)?|"
    r"常駐|"
    r"出社|"
    r"地方可|地方不可|地方NG|"
    r"相談可|"
    r"必要時|"
    r"初日|"
    r"顧客先|"
    r"都内近郊?|"
    r"週\s*\d+(?:[〜~\-]\d+)?\s*[日回]?\s*(?:程度|以上)?\s*(?:出社|リモート|在宅|テレワーク)|"
    r"リモート(?!\s*[可相談])|"  # 「リモート可」「リモート相談」は残す（辞書に関係なし）
    r"リモート",  # 残ったリモートも除去
)

# ── 区切り文字（場所候補の分割） ─────────────────────────
_SPLIT_RE = re.compile(r"[、,，/]|\bor\b|または")

# ── リモート系フォールバック判定 ────────────────────────────
# 物理場所が取れなかった場合の最終救済。地方不可/地方NG は含めない。
_REMOTE_RE = re.compile(
    r"フルリモ|リモート|在宅|テレワーク|地方可|地方OK|地方在住可"
)

# ── 地名でない説明 token の判定 ──────────────────────────
# 先頭語が明らかに非地名のトークンを除外する
_JUNK_TOKEN_RE = re.compile(
    r"^フェーズ|"           # フェーズによって あり
    r"^確認中|"             # 確認中(1週間程度...)
    r"^週\s*\d|"            # 週0～週1 等の頻度表現
    r"^フルリモ[^ー]"       # フルリモ！ (フルリモート/フルリモー は NOISE_RE 除去済み)
)

# ── セクションヘッダー検出（次行読み取り停止用） ────────────
_HEADER_RE = re.compile(r"^[【◆■□※▼▶★]|^\[")

# ── 〒 を含む行（署名住所） ──────────────────────────────
_POSTAL_RE = re.compile(r"〒")

# ── 本文辞書検索でスキップする行パターン ────────────────────────────
# 会社フッター由来の誤マッチを防ぐ（住所フィールド・営業電話・拠点リスト等）
_BODY_SIG_LINE_RE = re.compile(
    r"住所\s*[:：]|所在地\s*[:：]|"           # 住所/所在地フィールド（コロン付き）
    r"営業電話|"                               # 営業電話
    r"本社\s*[/｜|・]|[/｜|・]\s*本社|"      # 「本社 / 支社 /」式拠点リスト
    r"支社\s*[/｜|・]|[/｜|・]\s*支社|"
    r"支社営業部|支店営業部",                  # 支社内営業部署（差出人署名）
    re.IGNORECASE,
)

# 地方候補が競合した場合だけ優先できる、明示的な地域表現。
# 曖昧地名の網羅DBは持たず、既存辞書語そのもの、またはその直後の行政区分で判定する。
_EXPLICIT_REGION_KEYWORDS = {
    "東京", "大阪", "都内", "23区内", "首都圏",
    "北海道", "東北", "関東", "中部", "北陸", "東海",
    "関西", "関西圏", "近畿", "中国地方", "四国", "九州", "沖縄",
}
_ADMINISTRATIVE_SUFFIXES = {"都", "道", "府", "県", "市", "区"}
# 既存辞書に曖昧性metadataがないため、今回原文で地域競合を確認できた語だけを
# 最小限に列挙する。非曖昧な勤務地や複数拠点を明示地域で上書きしないために使う。
_KNOWN_AMBIGUOUS_LOCATION_KEYWORDS = {"京橋", "日本橋"}


def _n(s: str) -> str:
    """NFKC正規化"""
    return unicodedata.normalize("NFKC", s or "")


def _get_label_inline_content(line: str) -> Optional[str]:
    """
    ラベル行から同行のコンテンツを取得する。

    対応パターン:
      ・場所：神谷町          → 神谷町
      【作業場所】　浅草橋     → 浅草橋
      ◆場所                  → None（次行参照が必要）
      場所：（空）            → None
    """
    # パターン1: ： または : セパレータ
    sep_m = re.search(r"[：:]", line)
    if sep_m:
        content = line[sep_m.end():].strip()
        return content if content else None

    # パターン2: 【...】の後ろにコンテンツ
    bracket_m = re.search(r"】\s*(.+)", line)
    if bracket_m:
        content = bracket_m.group(1).strip()
        return content if content else None

    return None  # ラベルのみの行 → 次行参照が必要


def _extract_label_candidates(lines: List[str]) -> List[str]:
    """
    ラベル行と次行からロケーション候補文字列リストを返す。

    ラベル行に同行コンテンツがあればそれを使用。
    ラベルのみ行（【就業場所】等）は次の非空行を取得。
    """
    candidates: List[str] = []

    for i, raw_line in enumerate(lines):
        line = raw_line.strip()

        # 〒を含む行は署名住所 → スキップ
        if _POSTAL_RE.search(line):
            continue

        if not _LABEL_RE.search(line):
            continue

        inline = _get_label_inline_content(line)
        if inline:
            candidates.append(inline)
            continue

        # ラベルのみ行 → 次の非空行を取得
        for j in range(i + 1, min(i + 5, len(lines))):
            next_line = lines[j].strip()
            if not next_line:
                continue
            # 次の行が新しいセクションヘッダーなら停止
            if _HEADER_RE.match(next_line):
                break
            # 〒 行はスキップして続ける
            if _POSTAL_RE.search(next_line):
                break
            candidates.append(next_line)
            break

    return candidates


def _normalize_and_split(candidate: str) -> List[str]:
    """
    候補文字列を正規化して分割する。

    ノイズ語を除去し、区切り文字で分割する。
    括弧内コンテンツは残す（括弧内に地名が入る場合があるため）。
    例:
      「リモート、日本橋、小伝馬町」 → [「日本橋」, 「小伝馬町」]
      「フルリモート（出社時は白金高輪）」 → [「（出社時は白金高輪）」] → dict が 白金高輪 を検出
      「神田（リモートも設ける予定）」 → [「神田（リモートも設ける予定）」] → dict が 神田 を検出
    """
    text = _n(candidate)
    text = _NOISE_RE.sub(" ", text)
    text = re.sub(r"\s{2,}", " ", text).strip()

    parts = _SPLIT_RE.split(text)
    tokens = []
    for p in parts:
        t = p.strip()
        # 先頭の記号/装飾を除去
        t = re.sub(r"^[\s　・★■□▼※\-\.※]+", "", t).strip()
        # 括弧のみのトークンはアンラップ（中身が地名の場合に辞書検索できるようにする）
        # 例: （有明） → 有明 / （コアタイム10:00）→ コアタイム10:00 → 辞書未ヒットでnull
        t = re.sub(r"^[(（]\s*(.*?)\s*[)）]$", r"\1", t)
        t = t.strip()
        if not t or len(t) < 2:
            continue
        # 地名でない説明トークンを除外
        if _JUNK_TOKEN_RE.match(t):
            continue
        tokens.append(t)

    return tokens


def _match_from_tokens(
    tokens: List[str],
    entries: List[Tuple[str, str, "re.Pattern"]],
) -> Optional[Tuple[str, str]]:
    """
    トークンリストから地方を返す。

    通常は従来どおり最初のマッチを採用する。異なる地方の候補が競合し、
    都内・広域名・都道府県・市区に相当する明示候補が一地方だけに存在し、
    他地方の候補が確認済みの曖昧地名だけの場合に限り、その地方を優先する。
    明示候補同士や非曖昧な複数勤務地が競合する場合は従来の先着結果を維持する。

    Returns:
        (region, matched_keyword) or None
    """
    first_match: Optional[Tuple[str, str]] = None
    matched_regions = set()
    matches: List[Tuple[int, int, int, int, str, str, bool]] = []

    # 各トークンに対して全辞書パターンを確認する。first_match の選び方は
    # 既存動作（token順、同一token内は最初の出現位置）をそのまま維持する。
    for token_index, token in enumerate(tokens):
        text = _n(token)
        best_pos = None
        best_region = None
        best_kw = None
        for entry_index, (region, keyword, pattern) in enumerate(entries):
            m = pattern.search(text)
            if not m:
                continue

            matched_regions.add(region)
            is_explicit = _is_explicit_location_match(text, keyword, m.end())
            matches.append(
                (token_index, m.start(), m.end(), entry_index, region, keyword, is_explicit)
            )

            if best_pos is None or m.start() < best_pos:
                best_pos = m.start()
                best_region = region
                best_kw = keyword
        if best_region and first_match is None:
            first_match = (best_region, best_kw)

    if first_match is None:
        return None

    # 単一地方しか検出されない通常ケースは一切overrideしない。
    if len(matched_regions) <= 1:
        return first_match

    explicit_matches = [m for m in matches if m[6]]
    explicit_regions = {m[4] for m in explicit_matches}
    if len(explicit_regions) == 1:
        explicit_region = next(iter(explicit_regions))

        # 「東京都内」中の「京都」のように、明示表現と文字範囲が重なる
        # 部分一致は独立した地方候補として扱わない。
        opposing_matches = []
        for match in matches:
            token_index, start, end, _, region, keyword, _ = match
            if region == explicit_region:
                continue
            overlaps_explicit = any(
                token_index == explicit[0]
                and start < explicit[2]
                and explicit[1] < end
                for explicit in explicit_matches
                if explicit[4] == explicit_region
            )
            if not overlaps_explicit:
                opposing_matches.append(match)

        # overrideは、競合する他地方の候補が確認済みの曖昧地名だけの場合に限定。
        # 西葛西+大阪出張、品川or大阪など、実在する複数拠点は上書きしない。
        if not opposing_matches or all(
            match[5] in _KNOWN_AMBIGUOUS_LOCATION_KEYWORDS
            for match in opposing_matches
        ):
            explicit_matches.sort(key=lambda m: (m[0], m[1], m[3]))
            _, _, _, _, region, keyword, _ = explicit_matches[0]
            return (region, keyword)

    # 明示候補なし、または明示候補同士が競合する場合は既存挙動を維持。
    return first_match


def _is_explicit_location_match(text: str, keyword: str, match_end: int) -> bool:
    """辞書matchが行政区分・広域名を明示する強いlocation evidenceか判定する。"""
    normalized_keyword = _n(keyword)
    if normalized_keyword in _EXPLICIT_REGION_KEYWORDS:
        return True

    # 「京都」の末尾「都」のような地名構成文字は行政区分suffixとみなさない。
    # 辞書語自体のsuffixで強いとするのは、明確な都道府県名と市区名に限定する。
    if normalized_keyword in {"東京都", "北海道"}:
        return True
    if normalized_keyword.endswith(("府", "県", "市", "区")):
        return True

    # 辞書が「東京」「神奈川」「大阪」の形式でも、原文が
    # 「東京都」「神奈川県」「大阪府/大阪市」なら明示表現として扱う。
    if match_end < len(text) and text[match_end] in _ADMINISTRATIVE_SUFFIXES:
        return True

    return False


def _body_dict_search(
    body: str,
    entries: List[Tuple[str, str, "re.Pattern"]],
) -> Optional[Tuple[str, str, str]]:
    """
    本文全体への辞書マッチ（既存ロジック維持）。

    Returns:
        (region, location_raw, "body") or None
    """
    text = _n(body)
    best_pos = None
    best_region = None
    best_kw = None

    # 〒 を含む行・会社フッター由来の行を除外してから検索
    lines = text.splitlines()
    filtered = "\n".join(
        l for l in lines
        if "〒" not in l and not _BODY_SIG_LINE_RE.search(l)
    )
    text = filtered

    for region, keyword, pattern in entries:
        m = pattern.search(text)
        if m and (best_pos is None or m.start() < best_pos):
            best_pos = m.start()
            best_region = region
            best_kw = keyword

    if best_region is None:
        return None

    start = max(0, best_pos - 20)
    end = min(len(text), best_pos + len(best_kw) + 30)
    raw = text[start:end].strip()[:100]
    return (best_region, raw, "body")


def _remote_fallback_search(body: str) -> Optional[Tuple[str, str, str]]:
    """
    物理場所が取れなかった場合の最終フォールバック。
    本文にリモート系キーワードがあれば location="remote" を返す。
    地方不可 / 地方NG はマッチ対象に含めない。

    Returns:
        ("remote", raw_context, "remote_fallback") or None
    """
    text = _n(body)
    for line in text.splitlines():
        if "〒" in line:
            continue
        m = _REMOTE_RE.search(line)
        if m:
            start = max(0, m.start() - 15)
            end = min(len(line), m.end() + 20)
            raw = line[start:end].strip()[:80]
            return ("remote", raw, "remote_fallback")
    return None


def parse_location(
    body: str,
    entries: List[Tuple[str, str, "re.Pattern"]],
) -> Tuple[Optional[str], Optional[str], str]:
    """
    メール本文からロケーション（地方）を抽出する。

    Args:
        body: 署名除去済みのメール本文
        entries: ロケーション辞書エントリー [(region, keyword, pattern), ...]

    Returns:
        (location, location_raw, location_source)
          location: 地方名 or None
          location_raw: マッチしたコンテキスト文字列 or None
          location_source: "label" | "body" | ""
    """
    if not body:
        return None, None, ""

    lines = body.splitlines()

    # ── フェーズ1: ラベル行 + 次行からの抽出（最優先） ──────
    label_candidates = _extract_label_candidates(lines)
    for candidate in label_candidates:
        tokens = _normalize_and_split(candidate)
        if not tokens:
            continue
        result = _match_from_tokens(tokens, entries)
        if result:
            region, kw = result
            raw = _n(candidate)[:100]
            return region, raw, "label"

    # ── フェーズ2: 本文全体への辞書マッチ（フォールバック） ──
    result2 = _body_dict_search(body, entries)
    if result2:
        return result2

    # ── フェーズ3: リモート系フォールバック（最終救済） ─────
    result3 = _remote_fallback_search(body)
    if result3:
        return result3

    return None, None, ""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 03-1: 案件メールから単価（月額）をルールベースで抽出

ルール:
  ① min・max 両方あれば中央値を unit_price に格納
  ② 片方のみならその値を unit_price に格納
  ③ 「スキル見合い」等のみ → unit_price = 1_000_000
  ④ 時給・日給は月給に換算（時給×160 / 日給×20）
  ⑤ 抽出不可 → unit_price=null、99_price_null へ分離

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/projects.jsonl

出力①（抽出結果）:
  01_result/extract_project_budget.jsonl
出力②（null）:
  01_result/99_price_null_extract_project_budget.jsonl
"""

import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple

_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "03-1_extract_project_budget"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_PROJECTS = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl"
)
INPUT_MASTER = str(
    _PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)
OUTPUT_EXTRACTED = "extract_project_budget.jsonl"
OUTPUT_NULL      = "99_price_null_extract_project_budget.jsonl"

HOURLY_TO_MONTHLY = 160    # 時給 × 8h × 20日
DAILY_TO_MONTHLY  = 20     # 日給 × 20日
SKILL_BASED_PRICE = 1_000_000

# ── 正規化 ─────────────────────────────────────────────────
def _n(s: str) -> str:
    """NFKC正規化（全角→半角、波ダッシュ等を統一）"""
    return unicodedata.normalize("NFKC", s or "")


# ── パターン定義（NFKC 正規化済みテキストに適用）─────────────
# NFKC後の範囲区切り: 〜(U+301C はそのまま) / ~(U+FF5E→007E) / -(U+FF0D→002D)
_SEP = r"[〜~\-]"

# 価格キーワード（近傍探索のアンカー）
_PRICE_KW_RE = re.compile(
    r"(単\s*価|単\s*金|月\s*額|人月単価|報\s*酬|金\s*額|フィー|予\s*算|費\s*用"
    r"|【単価】|【単金】|【報酬】|【金額】|【予算】|日給\s*制"
    r"|単価条件|単金条件|スキル見合)"
)

# スキル見合い系
_SKILL_RE = re.compile(
    r"スキル見合|スキルみあ|スキルによ|スキル次第|経験見合|経験によ|能力次第"
    r"|応相談|要相談|詳細応相談|別途相談"
)

# 万-万 閉区間: 80万〜90万 / 80万円~90万円
RX_MAN_MAN = re.compile(
    r"(\d{1,3})\s*万[円]?\s*" + _SEP + r"\s*(\d{1,3})\s*万[円]?"
)
# 数-数万 閉区間: 80〜90万 / 75~85万 / 70-75万
RX_NUM_MAN = re.compile(
    r"(\d{1,3})\s*" + _SEP + r"\s*(\d{1,3})\s*万[円]?"
)
# 上限のみ: 〜80万 / ~80万
RX_OPEN_HIGH = re.compile(r"[〜~]\s*(\d{1,3})\s*万[円]?")
# 下限のみ: 80万〜 / 80万~（後ろに数字なし）
RX_OPEN_LOW = re.compile(r"(\d{1,3})\s*万[円]?\s*[〜~](?!\s*\d)")
# コンマ円 7桁: 1,200,000 or 1,200,000円
RX_COMMA7 = re.compile(r"(\d{1,2}),(\d{3}),(\d{3})\s*円?")
# コンマ円 6桁: 850,000円（円必須で誤検知防止）
RX_COMMA6 = re.compile(r"(\d{3}),(\d{3})\s*円")
# 前後・程: 80万前後 / 80万程 / 80万程度
RX_ZENPO = re.compile(r"(\d{1,3})\s*万[円]?\s*(?:前後|程度?)")
# 単一万: 80万 / 80万円
RX_MAN1 = re.compile(r"(\d{1,3})\s*万[円]?")

# 時給・日給
RX_DAILY_RANGE = re.compile(
    r"日給\s*(\d{1,2})\s*万[円]?\s*[〜~\-]\s*(\d{1,2})\s*万[円]?"
)
RX_DAILY_MAN  = re.compile(r"日給\s*(\d{1,2})\s*万[円]?")
RX_DAILY_YEN  = re.compile(r"日給\s*(\d{3,6})\s*円")
RX_HOURLY_YEN = re.compile(r"時給\s*(\d{3,5})\s*円")
RX_HOURLY_MAN = re.compile(r"時給\s*(\d{1,2})\s*万[円]?")
# 時給記号なし: 1900円/h / 2000円/時
RX_HOURLY_YEN_SLASH = re.compile(r"(\d{3,5})\s*円\s*/\s*[hｈ時]")
RX_HOURLY_YEN_SLASH_FLEX = re.compile(
    r"(?:¥|￥)?\s*([\d,]{3,7})\s*(?:円)?\s*/\s*(?:[hHｈ]|時間)"
)
# 数値のみ範囲（万なし）: 単価:60-65 / 単価:60~65 → 万として解釈
RX_NUM_RANGE_ONLY = re.compile(r"(\d{2,3})\s*[〜~\-]\s*(\d{2,3})(?!\s*万)(?!\s*\d)")

_PRICE_LINE_KW_RE = re.compile(r"(単\s*価|単\s*金|月\s*額|報\s*酬)")
_RANGE_NUM_EXCLUDE_LINE_RE = re.compile(r"(精算|清算|年齢|勤務時間|勤怠|時間幅|稼働時間)")
_SECTION_HEADER_RE = re.compile(
    r"^\s*(?:[■●◇【\[]|案件名\b|案件\b|場所\b|時間\b|精算\b|清算\b|年齢\b|勤務時間\b|勤怠\b|稼働時間\b)"
)
_TIME_RANGE_RE = re.compile(r"\d{1,2}:\d{2}\s*[〜~\-]\s*\d{1,2}:\d{2}")

# 明示的 /月 付き（日給より優先）
RX_MAN_MON_RANGE = re.compile(
    r"(\d{1,3})\s*万[円]?\s*[〜~\-]\s*(\d{1,3})\s*万[円]?\s*/\s*月"
)
RX_MAN_MON = re.compile(r"(\d{1,3})\s*万[円]?\s*/\s*月")

# 明示的 /日 付き（日給プレフィクスなし）
RX_MAN_DAY_RANGE = re.compile(
    r"(\d{1,3})\s*万[円]?\s*[〜~\-]\s*(\d{1,3})\s*万[円]?\s*/\s*日"
)
RX_MAN_DAY = re.compile(r"(\d{1,3})\s*万[円]?\s*/\s*日")

# 日給制コンテキスト: 日給制（メイン：4万～）など、近傍のN万を日給として解釈（非貪欲）
RX_DAILY_SEIDO_MAN = re.compile(r"日給\s*制.{0,20}?(\d{1,2})\s*万[円]?")


# ── セグメント抽出 ────────────────────────────────────────
def _get_segments(text: str) -> List[str]:
    """価格キーワードを含む行と、その直後の最小価格値行だけを返す"""
    segs: List[str] = []
    seen: set = set()
    lines = text.splitlines()

    def _has_price_value(line: str) -> bool:
        return bool(
            _SKILL_RE.search(line)
            or RX_MAN_MAN.search(line)
            or RX_NUM_MAN.search(line)
            or RX_OPEN_HIGH.search(line)
            or RX_OPEN_LOW.search(line)
            or RX_COMMA7.search(line)
            or RX_COMMA6.search(line)
            or RX_ZENPO.search(line)
            or RX_MAN1.search(line)
            or RX_DAILY_RANGE.search(line)
            or RX_DAILY_MAN.search(line)
            or RX_DAILY_YEN.search(line)
            or RX_HOURLY_YEN.search(line)
            or RX_HOURLY_YEN_SLASH.search(line)
            or RX_HOURLY_YEN_SLASH_FLEX.search(line)
            or RX_HOURLY_MAN.search(line)
            or RX_MAN_MON_RANGE.search(line)
            or RX_MAN_MON.search(line)
            or RX_MAN_DAY_RANGE.search(line)
            or RX_MAN_DAY.search(line)
            or RX_NUM_RANGE_ONLY.search(line)
        )

    for idx, line in enumerate(lines):
        if not _PRICE_KW_RE.search(line):
            continue

        seg_lines = [line.strip()]
        if not _has_price_value(line):
            for next_idx in range(idx + 1, len(lines)):
                next_line = lines[next_idx].strip()
                if not next_line:
                    continue
                if _SECTION_HEADER_RE.search(next_line):
                    break
                if _has_price_value(next_line):
                    seg_lines.append(next_line)
                    break
                break

        seg = "\n".join(part for part in seg_lines if part)
        if seg and seg not in seen:
            seen.add(seg)
            segs.append(seg)
    return segs


def _yen_to_int(value: str) -> int:
    return int(value.replace(",", ""))


def _is_range_num_only_allowed(seg: str, match: re.Match) -> bool:
    line = seg.replace("\n", " ")
    if not _PRICE_LINE_KW_RE.search(line):
        return False
    if _RANGE_NUM_EXCLUDE_LINE_RE.search(line):
        return False
    if _TIME_RANGE_RE.search(line):
        return False

    suffix = line[match.end():].lstrip()
    for token in ("h", "H", "時間", "時", "歳", "代", "名", "回"):
        if suffix.startswith(token):
            return False

    return True


def _prefer_skill_based(seg: str) -> bool:
    if not _SKILL_RE.search(seg):
        return False
    if any(
        pattern.search(seg)
        for pattern in (
            RX_MAN_MAN,
            RX_NUM_MAN,
            RX_OPEN_HIGH,
            RX_OPEN_LOW,
            RX_COMMA7,
            RX_COMMA6,
            RX_ZENPO,
            RX_MAN1,
        )
    ):
        return False
    if RX_NUM_RANGE_ONLY.search(seg):
        return True
    return True


# ── 抽出関数 ─────────────────────────────────────────────
def _monthly_explicit(seg: str) -> Optional[Tuple]:
    """/月 明示付きパターン（日給より優先）"""
    m = RX_MAN_MON_RANGE.search(seg)
    if m:
        a, b = int(m.group(1)) * 10000, int(m.group(2)) * 10000
        lo, hi = min(a, b), max(a, b)
        if _PRICE_MIN <= lo and hi <= _PRICE_MAX:
            return (lo + hi) // 2, {"min": lo, "max": hi}, "range-closed-man-mon", 0.95, "monthly", m.group(0)
    m = RX_MAN_MON.search(seg)
    if m:
        v = int(m.group(1)) * 10000
        if _PRICE_MIN <= v <= _PRICE_MAX:
            return v, None, "man-mon", 0.93, "monthly", m.group(0)
    return None


def _hourly_daily(seg: str) -> Optional[Tuple]:
    """時給・日給を検出して月換算で返す"""
    m = RX_DAILY_RANGE.search(seg)
    if m:
        lo = int(m.group(1)) * 10000 * DAILY_TO_MONTHLY
        hi = int(m.group(2)) * 10000 * DAILY_TO_MONTHLY
        return (lo + hi) // 2, {"min": lo, "max": hi}, "daily-man-range", 0.82, "monthly", m.group(0)

    m = RX_DAILY_MAN.search(seg)
    if m:
        v = int(m.group(1)) * 10000 * DAILY_TO_MONTHLY
        return v, None, "daily-man", 0.80, "monthly", m.group(0)

    m = RX_DAILY_YEN.search(seg)
    if m:
        v = int(m.group(1)) * DAILY_TO_MONTHLY
        return v, None, "daily-yen", 0.80, "monthly", m.group(0)

    m = RX_HOURLY_YEN.search(seg)
    if m:
        v = int(m.group(1)) * HOURLY_TO_MONTHLY
        return v, None, "hourly-yen", 0.82, "monthly", m.group(0)

    m = RX_HOURLY_YEN_SLASH.search(seg)
    if m:
        v = int(m.group(1)) * HOURLY_TO_MONTHLY
        return v, None, "hourly-yen-slash", 0.80, "monthly", m.group(0)

    m = RX_HOURLY_YEN_SLASH_FLEX.search(seg)
    if m:
        v = _yen_to_int(m.group(1)) * HOURLY_TO_MONTHLY
        return v, None, "hourly-yen-slash", 0.82, "monthly", m.group(0)

    m = RX_HOURLY_MAN.search(seg)
    if m:
        v = int(m.group(1)) * 10000 * HOURLY_TO_MONTHLY
        return v, None, "hourly-man", 0.80, "monthly", m.group(0)

    # /日 明示付き（日給プレフィクスなし）
    m = RX_MAN_DAY_RANGE.search(seg)
    if m:
        lo = int(m.group(1)) * 10000 * DAILY_TO_MONTHLY
        hi = int(m.group(2)) * 10000 * DAILY_TO_MONTHLY
        return (lo + hi) // 2, {"min": lo, "max": hi}, "man-day-range", 0.82, "monthly", m.group(0)

    m = RX_MAN_DAY.search(seg)
    if m:
        v = int(m.group(1)) * 10000 * DAILY_TO_MONTHLY
        return v, None, "man-day", 0.80, "monthly", m.group(0)

    # 日給制コンテキスト: 日給制（メイン：4万～）など
    m = RX_DAILY_SEIDO_MAN.search(seg)
    if m:
        v = int(m.group(1)) * 10000 * DAILY_TO_MONTHLY
        return v, None, "daily-seido-man", 0.78, "monthly", m.group(0)

    return None


_PRICE_MIN = 100_000   # 10万（下限）
_PRICE_MAX = 5_000_000 # 500万（上限）


def _man_patterns(seg: str) -> Optional[Tuple]:
    """万円パターンで価格を抽出"""
    if _prefer_skill_based(seg):
        return SKILL_BASED_PRICE, None, "skill-based", 0.70, "monthly", "スキル見合い"

    # 万-万 閉区間
    m = RX_MAN_MAN.search(seg)
    if m:
        a, b = int(m.group(1)) * 10000, int(m.group(2)) * 10000
        lo, hi = min(a, b), max(a, b)
        if _PRICE_MIN <= lo and hi <= _PRICE_MAX:
            return (lo + hi) // 2, {"min": lo, "max": hi}, "range-closed-man", 0.93, "monthly", m.group(0)

    # 数-数万 閉区間
    m = RX_NUM_MAN.search(seg)
    if m:
        a, b = int(m.group(1)) * 10000, int(m.group(2)) * 10000
        lo, hi = min(a, b), max(a, b)
        if _PRICE_MIN <= lo and hi <= _PRICE_MAX:
            return (lo + hi) // 2, {"min": lo, "max": hi}, "range-closed-man", 0.93, "monthly", m.group(0)

    # 上限のみ
    m = RX_OPEN_HIGH.search(seg)
    if m:
        v = int(m.group(1)) * 10000
        if _PRICE_MIN <= v <= _PRICE_MAX:
            return v, {"min": None, "max": v}, "range-open-high", 0.88, "monthly", m.group(0)

    # 下限のみ
    m = RX_OPEN_LOW.search(seg)
    if m:
        v = int(m.group(1)) * 10000
        if _PRICE_MIN <= v <= _PRICE_MAX:
            return v, {"min": v, "max": None}, "range-open-low", 0.88, "monthly", m.group(0)

    # コンマ円 7桁
    m = RX_COMMA7.search(seg)
    if m:
        v = int(m.group(1)) * 1_000_000 + int(m.group(2)) * 1_000 + int(m.group(3))
        if _PRICE_MIN <= v <= _PRICE_MAX:
            return v, None, "comma-yen", 0.90, "monthly", m.group(0)

    # コンマ円 6桁
    m = RX_COMMA6.search(seg)
    if m:
        v = int(m.group(1)) * 1_000 + int(m.group(2))
        if _PRICE_MIN <= v <= _PRICE_MAX:
            return v, None, "comma-yen", 0.90, "monthly", m.group(0)

    # 前後
    m = RX_ZENPO.search(seg)
    if m:
        v = int(m.group(1)) * 10000
        if _PRICE_MIN <= v <= _PRICE_MAX:
            return v, None, "man-zenpo", 0.78, "monthly", m.group(0)

    # 単一万（範囲を絞って誤検知防止）
    m = RX_MAN1.search(seg)
    if m:
        v = int(m.group(1)) * 10000
        if 200_000 <= v <= 3_000_000:
            return v, None, "man", 0.85, "monthly", m.group(0)

    # 数値のみ範囲（万なし）: 単価:60-65 → 万として解釈（価格キーワード近傍限定）
    m = RX_NUM_RANGE_ONLY.search(seg)
    if m and _is_range_num_only_allowed(seg, m):
        a, b = int(m.group(1)) * 10000, int(m.group(2)) * 10000
        lo, hi = min(a, b), max(a, b)
        if _PRICE_MIN <= lo and hi <= _PRICE_MAX:
            return (lo + hi) // 2, {"min": lo, "max": hi}, "range-num-only", 0.72, "monthly", m.group(0)

    return None


def rule_extract(body: str) -> Tuple[Optional[int], Optional[Dict], str, float, str, str]:
    """
    ルールベースで単価を抽出。
    Returns: (unit_price, range_dict, reason, confidence, kind, unit_price_raw)
    """
    if not body:
        return None, None, "no-body", 0.0, "unknown", ""

    text = _n(body)
    segments = _get_segments(text)

    if not segments:
        # 件名等の短テキストは全体をセグメントとして試行（keyword不要）
        if len(text) <= 200:
            segments = [text]
        else:
            return None, None, "no-price-keyword", 0.0, "unknown", ""

    skill_found = False

    for seg in segments:
        # /月 明示付き（日給より最優先）
        result = _monthly_explicit(seg)
        if result:
            return result

        # 時給・日給（/日 明示 / 日給制 含む）
        result = _hourly_daily(seg)
        if result:
            return result

        # 万円パターン
        result = _man_patterns(seg)
        if result:
            return result

        # スキル見合いフラグ（数値が見つからない場合のフォールバック）
        if _SKILL_RE.search(seg):
            skill_found = True

    if skill_found:
        return SKILL_BASED_PRICE, None, "skill-based", 0.70, "monthly", "スキル見合い"

    return None, None, "no-match", 0.0, "unknown", ""


# ── レコード構築 ──────────────────────────────────────────
def build_record(mid: str, body: str, subject: str = "") -> Dict:
    """1メール分の出力レコードを構築する。本文抽出不可の場合は件名をフォールバック使用。"""
    unit, rng, reason, conf, kind, raw = rule_extract(body)

    # 本文から抽出できない場合は件名をフォールバック
    if unit is None and subject:
        unit, rng, reason, conf, kind, raw = rule_extract(subject)
        if unit is not None:
            reason = "subject-" + reason

    return {
        "message_id": mid,
        "unit_price": unit,
        "unit_price_sub_infor": {
            "range": rng,
            "currency": "JPY",
            "method": "rule",
            "reason": reason,
            "tax_included": "unknown",
            "confidence": round(conf, 2),
            "kind": kind,
            "unit_price_raw": raw,
        },
    }


# ── メイン ────────────────────────────────────────────────
def main() -> None:
    start = time.time()
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = dirs["result"]

    for path in [INPUT_PROJECTS, INPUT_CLEANED, INPUT_MASTER]:
        if not Path(path).exists():
            logger.error(f"入力ファイルが存在しません: {path}")
            sys.exit(1)

    try:
        project_ids = [r["message_id"] for r in read_jsonl_as_list(INPUT_PROJECTS)]
        cleaned_map = read_jsonl_as_dict(INPUT_CLEANED)
        master_map  = read_jsonl_as_dict(INPUT_MASTER)
    except Exception as e:
        write_error_log(str(result_dir), e, "入力ファイル読み込みエラー")
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    extracted: List[Dict] = []
    null_records: List[Dict] = []

    for mid in project_ids:
        body    = (cleaned_map.get(mid) or {}).get("body_text", "")
        subject = (master_map.get(mid) or {}).get("subject", "")
        rec     = build_record(mid, body, subject)

        if rec["unit_price"] is None:
            null_records.append(rec)
        else:
            extracted.append(rec)

        logger.info(
            f"{mid} → unit_price={rec['unit_price']} "
            f"reason={rec['unit_price_sub_infor']['reason']}",
            message_id=mid,
        )

    # 出力
    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path,  extracted)
    write_jsonl(null_path, null_records)

    elapsed = time.time() - start
    total   = len(project_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    null_rate = len(null_records) / total * 100 if total else 0
    logger.ok(
        f"Step完了: 入力={total}件 / 抽出={len(extracted)}件 / "
        f"null={len(null_records)}件 ({null_rate:.1f}%)"
    )


if __name__ == "__main__":
    main()

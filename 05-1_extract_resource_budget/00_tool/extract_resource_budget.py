#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 05-1: 要員メールから希望単価（月額）をルールベースで抽出

ルール:
  ① min・max 両方あれば中央値を desired_unit_price に格納
  ② 片方のみならその値を desired_unit_price に格納
  ③ 「スキル見合い」等のみ → desired_unit_price = 1_000_000
  ④ 時給・日給は月給に換算（時給×160 / 日給×20）
  ⑤ 抽出不可 → desired_unit_price=null、99_price_null へ分離

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/resources.jsonl

出力①（抽出結果）:
  01_result/extract_resource_budget.jsonl
出力②（null）:
  01_result/99_price_null_extract_resource_budget.jsonl
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

STEP_NAME = "05-1_extract_resource_budget"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_RESOURCES = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "resources.jsonl"
)
INPUT_MASTER = str(
    _PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)
OUTPUT_EXTRACTED = "extract_resource_budget.jsonl"
OUTPUT_NULL      = "99_price_null_extract_resource_budget.jsonl"

HOURLY_TO_MONTHLY = 160    # 時給 × 8h × 20日
DAILY_TO_MONTHLY  = 20     # 日給 × 20日
SKILL_BASED_PRICE = 1_000_000

# ── 正規化 ─────────────────────────────────────────────────
def _n(s: str) -> str:
    """NFKC正規化（全角→半角、波ダッシュ等を統一）"""
    return unicodedata.normalize("NFKC", s or "")


# ── パターン定義（NFKC 正規化済みテキストに適用）─────────────
_SEP = r"[〜~\-]"

# 価格キーワード（近傍探索のアンカー）
# 要員メール向けに「希望」系キーワードを追加
_PRICE_KW_RE = re.compile(
    r"(単\s*価|単\s*金|月\s*額|人月単価|報\s*酬|金\s*額|フィー|予\s*算|費\s*用"
    r"|希望\s*単\s*価|希望\s*月\s*額|希望\s*報\s*酬|希望\s*金\s*額"
    r"|【単価】|【単金】|【報酬】|【金額】|【希望単価】|日給\s*制"
    r"|【稼働】|【希望】)"
)

# スキル見合い系
_SKILL_RE = re.compile(
    r"スキル見合|スキルによ|スキル次第|経験見合|経験によ|能力次第"
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
# 前後: 80万前後
RX_ZENPO = re.compile(r"(\d{1,3})\s*万[円]?\s*前後")
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
RX_HOURLY_YEN_SLASH = re.compile(r"(\d{3,5})\s*円\s*/\s*[hｈ時]")
# 数値のみ範囲（万なし）: 単価:60-65 / 単価:60~65 → 万として解釈
RX_NUM_RANGE_ONLY = re.compile(r"(\d{2,3})\s*[〜~\-]\s*(\d{2,3})(?!\s*万)(?!\s*\d)")


# ── セグメント抽出 ────────────────────────────────────────
def _get_segments(text: str) -> List[str]:
    """価格キーワード周辺のテキストセグメントを返す（重複排除）"""
    segs: List[str] = []
    seen: set = set()
    for m in _PRICE_KW_RE.finditer(text):
        start = max(0, m.start() - 5)
        end   = min(len(text), m.end() + 150)
        seg   = text[start:end]
        if seg not in seen:
            seen.add(seg)
            segs.append(seg)
    return segs


# ── 抽出関数 ─────────────────────────────────────────────
def _hourly_daily(seg: str) -> Optional[Tuple]:
    """時給・日給を検出して月換算で返す"""
    m = RX_DAILY_RANGE.search(seg)
    if m:
        lo = int(m.group(1)) * 10000 * DAILY_TO_MONTHLY
        hi = int(m.group(2)) * 10000 * DAILY_TO_MONTHLY
        return (lo + hi) // 2, lo, hi, "daily-man-range", m.group(0)

    m = RX_DAILY_MAN.search(seg)
    if m:
        v = int(m.group(1)) * 10000 * DAILY_TO_MONTHLY
        return v, None, None, "daily-man", m.group(0)

    m = RX_DAILY_YEN.search(seg)
    if m:
        v = int(m.group(1)) * DAILY_TO_MONTHLY
        return v, None, None, "daily-yen", m.group(0)

    m = RX_HOURLY_YEN.search(seg)
    if m:
        v = int(m.group(1)) * HOURLY_TO_MONTHLY
        return v, None, None, "hourly-yen", m.group(0)

    m = RX_HOURLY_YEN_SLASH.search(seg)
    if m:
        v = int(m.group(1)) * HOURLY_TO_MONTHLY
        return v, None, None, "hourly-yen-slash", m.group(0)

    m = RX_HOURLY_MAN.search(seg)
    if m:
        v = int(m.group(1)) * 10000 * HOURLY_TO_MONTHLY
        return v, None, None, "hourly-man", m.group(0)

    return None


_PRICE_MIN = 100_000   # 10万（下限）
_PRICE_MAX = 5_000_000 # 500万（上限）


def _man_patterns(seg: str) -> Optional[Tuple]:
    """万円パターンで価格を抽出。Returns: (price, min, max, reason, raw)"""
    # 万-万 閉区間
    m = RX_MAN_MAN.search(seg)
    if m:
        a, b = int(m.group(1)) * 10000, int(m.group(2)) * 10000
        lo, hi = min(a, b), max(a, b)
        if _PRICE_MIN <= lo and hi <= _PRICE_MAX:
            return (lo + hi) // 2, lo, hi, "range-closed-man", m.group(0)

    # 数-数万 閉区間
    m = RX_NUM_MAN.search(seg)
    if m:
        a, b = int(m.group(1)) * 10000, int(m.group(2)) * 10000
        lo, hi = min(a, b), max(a, b)
        if _PRICE_MIN <= lo and hi <= _PRICE_MAX:
            return (lo + hi) // 2, lo, hi, "range-closed-man", m.group(0)

    # 上限のみ
    m = RX_OPEN_HIGH.search(seg)
    if m:
        v = int(m.group(1)) * 10000
        if _PRICE_MIN <= v <= _PRICE_MAX:
            return v, None, v, "range-open-high", m.group(0)

    # 下限のみ
    m = RX_OPEN_LOW.search(seg)
    if m:
        v = int(m.group(1)) * 10000
        if _PRICE_MIN <= v <= _PRICE_MAX:
            return v, v, None, "range-open-low", m.group(0)

    # コンマ円 7桁
    m = RX_COMMA7.search(seg)
    if m:
        v = int(m.group(1)) * 1_000_000 + int(m.group(2)) * 1_000 + int(m.group(3))
        if _PRICE_MIN <= v <= _PRICE_MAX:
            return v, None, None, "comma-yen", m.group(0)

    # コンマ円 6桁
    m = RX_COMMA6.search(seg)
    if m:
        v = int(m.group(1)) * 1_000 + int(m.group(2))
        if _PRICE_MIN <= v <= _PRICE_MAX:
            return v, None, None, "comma-yen", m.group(0)

    # 前後
    m = RX_ZENPO.search(seg)
    if m:
        v = int(m.group(1)) * 10000
        if _PRICE_MIN <= v <= _PRICE_MAX:
            return v, None, None, "man-zenpo", m.group(0)

    # 単一万（範囲を絞って誤検知防止）
    m = RX_MAN1.search(seg)
    if m:
        v = int(m.group(1)) * 10000
        if 200_000 <= v <= 3_000_000:
            return v, None, None, "man", m.group(0)

    # 数値のみ範囲（万なし）: 単価:60-65 → 万として解釈（価格キーワード近傍限定）
    m = RX_NUM_RANGE_ONLY.search(seg)
    if m:
        a, b = int(m.group(1)) * 10000, int(m.group(2)) * 10000
        lo, hi = min(a, b), max(a, b)
        if _PRICE_MIN <= lo and hi <= _PRICE_MAX:
            return (lo + hi) // 2, lo, hi, "range-num-only", m.group(0)

    return None


def rule_extract(body: str) -> Tuple[Optional[int], Optional[int], Optional[int], str, str]:
    """
    ルールベースで希望単価を抽出。
    Returns: (desired_unit_price, price_min, price_max, reason, raw)
    """
    if not body:
        return None, None, None, "no-body", ""

    text = _n(body)
    segments = _get_segments(text)

    if not segments:
        if len(text) <= 200:
            segments = [text]
        else:
            return None, None, None, "no-price-keyword", ""

    skill_found = False

    for seg in segments:
        # 時給・日給（最優先）
        result = _hourly_daily(seg)
        if result:
            price, p_min, p_max, reason, raw = result
            return price, p_min, p_max, reason, raw

        # 万円パターン
        result = _man_patterns(seg)
        if result:
            price, p_min, p_max, reason, raw = result
            return price, p_min, p_max, reason, raw

        # スキル見合いフラグ
        if _SKILL_RE.search(seg):
            skill_found = True

    if skill_found:
        return SKILL_BASED_PRICE, None, None, "skill-based", "スキル見合い"

    return None, None, None, "no-match", ""


# ── レコード構築 ──────────────────────────────────────────
def build_record(mid: str, body: str, subject: str = "") -> Dict:
    """1メール分の出力レコードを構築する。本文抽出不可の場合は件名をフォールバック使用。"""
    price, p_min, p_max, reason, raw = rule_extract(body)

    # 本文から抽出できない場合は件名をフォールバック
    if price is None and subject:
        price, p_min, p_max, reason, raw = rule_extract(subject)
        if price is not None:
            reason = "subject-" + reason

    return {
        "message_id": mid,
        "desired_unit_price": price,
        "desired_unit_price_sub_infor": {
            "desired_unit_price_min": p_min,
            "desired_unit_price_max": p_max,
            "desired_unit_price_raw": raw,
        },
    }


# ── メイン ────────────────────────────────────────────────
def main() -> None:
    start = time.time()
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = dirs["result"]

    for path in [INPUT_RESOURCES, INPUT_CLEANED]:
        if not Path(path).exists():
            logger.error(f"入力ファイルが存在しません: {path}")
            sys.exit(1)

    try:
        resource_ids = [r["message_id"] for r in read_jsonl_as_list(INPUT_RESOURCES)]
        cleaned_map  = read_jsonl_as_dict(INPUT_CLEANED)
        master_map   = read_jsonl_as_dict(INPUT_MASTER) if Path(INPUT_MASTER).exists() else {}
    except Exception as e:
        write_error_log(str(result_dir), e, "入力ファイル読み込みエラー")
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    extracted: List[Dict] = []
    null_records: List[Dict] = []

    for mid in resource_ids:
        body    = (cleaned_map.get(mid) or {}).get("body_text", "")
        subject = (master_map.get(mid) or {}).get("subject", "")
        rec     = build_record(mid, body, subject)

        if rec["desired_unit_price"] is None:
            null_records.append(rec)
        else:
            extracted.append(rec)

        logger.info(
            f"{mid} → desired_unit_price={rec['desired_unit_price']} reason={rec['desired_unit_price_sub_infor']['desired_unit_price_raw']}",
            message_id=mid,
        )

    # 出力
    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path,  extracted)
    write_jsonl(null_path, null_records)

    elapsed = time.time() - start
    total   = len(resource_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    null_rate = len(null_records) / total * 100 if total else 0
    logger.ok(
        f"Step完了: 入力={total}件 / 抽出={len(extracted)}件 / "
        f"null={len(null_records)}件 ({null_rate:.1f}%)"
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 05-2: 要員メールから現在年齢をルールベースで抽出

ルール:
  ① 「30歳」「30代」等の記載から current_age を抽出
  ② 記載がない場合は current_age=1、source="default"
  ③ 抽出できた場合は source="extracted"
  ④ null/unknown は出力しない（必ずデフォルト値1を設定）

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/resources.jsonl

出力①（全件）:
  01_result/extract_resource_age.jsonl
出力②（current_age が null/unknown のもの、本来0件）:
  01_result/99_age_null_extract_resource_age.jsonl
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

STEP_NAME = "05-2_extract_resource_age"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_RESOURCES = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "resources.jsonl"
)
OUTPUT_EXTRACTED = "extract_resource_age.jsonl"
OUTPUT_NULL      = "99_age_null_extract_resource_age.jsonl"

AGE_DEFAULT = 1
AGE_MIN = 18    # 有効とみなす年齢の下限
AGE_MAX = 75    # 有効とみなす年齢の上限


# ── 正規化 ────────────────────────────────────────────────
def _n(s: str) -> str:
    """NFKC正規化（全角→半角、波ダッシュ等を統一）"""
    return unicodedata.normalize("NFKC", s or "")


# ── 年齢キーワード（近傍セグメント抽出用）───────────────────
_AGE_KW_RE = re.compile(
    r"年\s*齢|年\s*令|歳|才|氏\s*名|名\s*前"
)


def _get_age_segments(text: str) -> List[str]:
    """年齢キーワード周辺のセグメントを返す（重複排除）"""
    segs: List[str] = []
    seen: set = set()
    for m in _AGE_KW_RE.finditer(text):
        start = max(0, m.start() - 30)
        end   = min(len(text), m.end() + 60)
        seg   = text[start:end]
        if seg not in seen:
            seen.add(seg)
            segs.append(seg)
    return segs


# ── 代（decade）から年齢への変換 ────────────────────────────
def _decade_to_age(decade: int, qualifier: str) -> int:
    """XX代から代表年齢を返す。前半→+3、後半→+7、なし→+5（中央値）"""
    if "前半" in qualifier:
        return decade + 3
    if "後半" in qualifier:
        return decade + 7
    return decade + 5


# ── セグメントから年齢を抽出 ────────────────────────────────
def _extract_from_segment(seg: str) -> Optional[Tuple[int, str]]:
    """
    セグメント内から current_age と current_age_raw を抽出する。
    Returns: (age, raw_text) or None
    """
    # パターン1: 年齢ラベル付き「年齢：30歳」「年齢:35才」
    m = re.search(r"年\s*[齢令]\s*[：:]\s*(\d{1,2})\s*(?:歳|才)", seg)
    if m:
        age = int(m.group(1))
        if AGE_MIN <= age <= AGE_MAX:
            return age, m.group(0)

    # パターン1b: 年齢ラベル + 数字（歳なし）「【年齢】50」「【年齢】 44」「【年齢】\n55」
    m = re.search(
        r"年\s*[齢令]\s*[】\]）)]*\s*[：:]?\s*\n?\s*(\d{1,2})(?!\s*(?:\d|歳|才|代))",
        seg,
    )
    if m:
        age = int(m.group(1))
        if AGE_MIN <= age <= AGE_MAX:
            return age, m.group(0)

    # パターン2: 名前付き括弧「K.Y(32歳/男)」「BK（29歳/男性）」
    m = re.search(r"[A-Za-z.]+\s*[（(]\s*(\d{1,2})\s*(?:歳|才)\s*[/／]", seg)
    if m:
        age = int(m.group(1))
        if AGE_MIN <= age <= AGE_MAX:
            return age, m.group(0)

    # パターン2b: 数字/性別「T.N(37/男性)」
    m = re.search(r"[（(]\s*(\d{1,2})\s*[/／]\s*(?:男性|女性|男|女)", seg)
    if m:
        age = int(m.group(1))
        if AGE_MIN <= age <= AGE_MAX:
            return age, m.group(0)

    # パターン3: 年齢単独「(32歳/男)」「（29歳）」
    m = re.search(r"[（(]\s*(\d{1,2})\s*(?:歳|才)\s*(?:[/／]|[)）])", seg)
    if m:
        age = int(m.group(1))
        if AGE_MIN <= age <= AGE_MAX:
            return age, m.group(0)

    # パターン3b: 括弧数字 + 性別（歳なし）「（32）男性」「(32)男性」
    m = re.search(r"[（(]\s*(\d{1,2})\s*[）)]\s*(?:男性|女性|男|女)", seg)
    if m:
        age = int(m.group(1))
        if AGE_MIN <= age <= AGE_MAX:
            return age, m.group(0)

    # パターン4: 数字 + 歳/才「30歳」「35才」
    m = re.search(r"(\d{1,2})\s*(?:歳|才)", seg)
    if m:
        age = int(m.group(1))
        if AGE_MIN <= age <= AGE_MAX:
            return age, m.group(0)

    # パターン5: XX代「30代」「40代前半」「40代後半」
    m = re.search(r"([2-7]\d)\s*代(前半|後半)?", seg)
    if m:
        decade = int(m.group(1))
        qualifier = m.group(2) or ""
        age = _decade_to_age(decade, qualifier)
        if AGE_MIN <= age <= AGE_MAX:
            return age, m.group(0)

    # パターン6: 名前（英字）+ 括弧数字「K.F (47)」「T.T（32）」（歳/性別なし）
    m = re.search(r"[A-Za-z.]+\s*[（(]\s*(\d{1,2})\s*[）)]", seg)
    if m:
        age = int(m.group(1))
        if AGE_MIN <= age <= AGE_MAX:
            return age, m.group(0)

    return None


# ── メイン抽出関数 ──────────────────────────────────────────
def rule_extract_age(body: str) -> Tuple[int, str, Optional[str]]:
    """
    ルールベースで現在年齢を抽出。
    Returns: (current_age, source, current_age_raw)
      source: "extracted" or "default"
    """
    if not body:
        return AGE_DEFAULT, "default", None

    text = _n(body)
    segments = _get_age_segments(text)

    if not segments:
        return AGE_DEFAULT, "default", None

    for seg in segments:
        result = _extract_from_segment(seg)
        if result is not None:
            age, raw = result
            return age, "extracted", raw

    return AGE_DEFAULT, "default", None


# ── レコード構築 ──────────────────────────────────────────
def build_record(mid: str, body: str) -> Dict:
    age, source, raw = rule_extract_age(body)
    return {
        "message_id": mid,
        "current_age": age,
        "current_age_source": source,
        "current_age_raw": raw,
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
    except Exception as e:
        write_error_log(str(result_dir), e, "入力ファイル読み込みエラー")
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    extracted: List[Dict] = []
    null_records: List[Dict] = []

    for mid in resource_ids:
        body = (cleaned_map.get(mid) or {}).get("body_text", "")
        rec  = build_record(mid, body)

        # current_age が None になることは設計上ないが念のため分離
        if rec["current_age"] is None:
            null_records.append(rec)
        else:
            extracted.append(rec)

        logger.info(
            f"{mid} → current_age={rec['current_age']} source={rec['current_age_source']} raw={rec['current_age_raw']}",
            message_id=mid,
        )

    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path,  extracted)
    write_jsonl(null_path, null_records)

    elapsed = time.time() - start
    total   = len(resource_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    extracted_count = sum(1 for r in extracted if r["current_age_source"] == "extracted")
    default_count   = sum(1 for r in extracted if r["current_age_source"] == "default")
    logger.ok(
        f"Step完了: 入力={total}件 / 抽出={extracted_count}件 / "
        f"デフォルト={default_count}件 / null={len(null_records)}件"
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 03-2: 案件メールから年齢制限をルールベースで抽出

ルール:
  ① 「45歳まで」「40代まで」等の記載から age_max を抽出
  ② 「年齢不問」「制限なし」等は age_max=100、source="extracted"
  ③ 記載がない場合は age_max=100、source="default"（age_raw=null）
  ④ null/unknown は出力しない（必ずデフォルト値100を設定）

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/projects.jsonl

出力①（抽出結果）:
  01_result/extract_project_age.jsonl
出力②（異常レコード、本来0件）:
  01_result/99_age_null_extract_project_age.jsonl
"""

import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Dict, Optional, Tuple

_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "03-2_extract_project_age"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_PROJECTS = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl"
)
OUTPUT_EXTRACTED = "extract_project_age.jsonl"
OUTPUT_NULL      = "99_age_null_extract_project_age.jsonl"

AGE_DEFAULT = 100


# ── 正規化 ────────────────────────────────────────────────
def _n(s: str) -> str:
    """NFKC正規化（全角→半角、波ダッシュ等を統一）"""
    return unicodedata.normalize("NFKC", s or "")


# ── 年齢抽出ロジック ──────────────────────────────────────
# 年齢キーワードアンカー（近傍セグメント抽出用）
_AGE_KW_RE = re.compile(
    r"年\s*齢(?:制限|目安|条件)?|年\s*令|年\s*代"
)

# 代（decade）の前半/後半変換
def _decade_max(decade: int, qualifier: str) -> int:
    """XX代の最大年齢を返す。前半/半ば→+4、後半→+9、なし→+9"""
    if "前半" in qualifier or "半ば" in qualifier:
        return decade + 4
    if "後半" in qualifier:
        return decade + 9
    return decade + 9


def _get_age_segments(text: str) -> list:
    """年齢キーワード周辺のセグメントを返す（重複排除）"""
    segs = []
    seen: set = set()
    for m in _AGE_KW_RE.finditer(text):
        start = max(0, m.start() - 5)
        end   = min(len(text), m.end() + 80)
        seg   = text[start:end]
        if seg not in seen:
            seen.add(seg)
            segs.append(seg)
    return segs


def _extract_from_segment(seg: str) -> Optional[Tuple[int, str]]:
    """
    セグメント内から age_max と age_raw を抽出する。
    Returns: (age_max, age_raw) or None
    """
    # 年齢不問 / 制限なし / 制限はありません
    m = re.search(r"不問|制限\s*な[しい]|制限はありません", seg)
    if m:
        return AGE_DEFAULT, m.group(0)

    # 年齢(制限)?：なし / 特になし / 確認中（括弧付き・「年齢制限:なし」も対応）
    m = re.search(r"年\s*齢(?:制限)?\s*[：:]\s*[（(]?\s*(なし|特になし|確認中|制限なし)\s*[）)]?", seg)
    if m:
        return AGE_DEFAULT, m.group(0)

    # 【年齢】なし / 【年齢】特になし / [年齢]確認中 等（bracket付きラベル）
    m = re.search(r"[【\[]\s*年\s*齢\s*[】\]]\s*(なし|特になし|確認中|制限なし)", seg)
    if m:
        return AGE_DEFAULT, m.group(0)

    # 「以上原則NG」「以上NG」: XX歳以上NG → max = XX - 1
    m = re.search(r"(\d+)\s*(?:歳|才)\s*以上\s*(?:原則\s*)?NG", seg)
    if m:
        return int(m.group(1)) - 1, m.group(0)

    # XX歳未満 / XX才未満（strict < → XX - 1）
    m = re.search(r"(\d+)\s*(?:歳|才)\s*未満", seg)
    if m:
        return int(m.group(1)) - 1, m.group(0)

    # XX代不可 / XX歳代不可 → max = decade - 1（60代不可 → 59）
    m = re.search(r"([2-9]\d)\s*(?:歳)?\s*代\s*不可", seg)
    if m:
        return int(m.group(1)) - 1, m.group(0)

    # 範囲指定（28歳〜45歳、28〜45歳）→ 最大値を採用
    m = re.search(r"(\d+)\s*(?:歳|才)?\s*[〜~]\s*(\d+)\s*(?:歳|才)", seg)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        return max(lo, hi), m.group(0)

    # 下限のみ「30歳〜」（上限なし）→ デフォルト100
    m = re.search(r"(\d+)\s*(?:歳|才)\s*[〜~]\s*(?!\d)", seg)
    if m:
        return AGE_DEFAULT, m.group(0) + "（下限のみ）"

    # 代の範囲（20代〜40代まで）→ 上限の代を採用
    m = re.search(r"([2-9]\d)\s*(?:歳)?\s*代(?:前半|後半|半ば)?\s*[〜~]\s*([2-9]\d)\s*(?:歳)?\s*代(前半|後半|半ば)?\s*(?:まで|迄)?", seg)
    if m:
        decade = int(m.group(2))
        qualifier = m.group(3) or ""
        return _decade_max(decade, qualifier), m.group(0)

    # 数値範囲 + 代（20代後半〜40代まで）→ 上限の代を採用
    m = re.search(r"(\d+)\s*(?:歳|才)?\s*[〜~]\s*([2-9]\d)\s*(?:歳)?\s*代(前半|後半|半ば)?(?:まで|迄)?", seg)
    if m:
        decade = int(m.group(2))
        qualifier = m.group(3) or ""
        return _decade_max(decade, qualifier), m.group(0)

    # 「XX歳以下」「XX歳まで」「〜XX歳」
    m = re.search(r"[〜~]?\s*(\d+)\s*(?:歳|才)\s*(?:まで|迄|以下)", seg)
    if m:
        return int(m.group(1)), m.group(0)

    # 「XX歳くらいまで」「XX歳ぐらいまで」
    m = re.search(r"(\d+)\s*(?:歳|才)\s*[くぐ]?らい(?:まで|迄)?", seg)
    if m:
        return int(m.group(1)), m.group(0)

    # 「XX歳前後希望」「XX歳前後まで」→ 指定年齢をそのまま上限として扱う
    m = re.search(r"(\d{2})\s*(?:歳|才)\s*前後\s*(?:希望|まで|迄|程度)", seg)
    if m:
        return int(m.group(1)), m.group(0)

    # XX歳代(前半/後半/半ば)?(まで等) - 「50歳代前半まで」「40歳代まで」形式
    m = re.search(r"([2-9]\d)\s*歳\s*代(前半|後半|半ば)?\s*(?:まで|迄|以下|希望|程度|望ましい)", seg)
    if m:
        decade = int(m.group(1))
        qualifier = m.group(2) or ""
        return _decade_max(decade, qualifier), m.group(0)
    m = re.search(r"([2-9]\d)\s*歳\s*代(前半|後半|半ば)", seg)
    if m:
        decade = int(m.group(1))
        qualifier = m.group(2)
        return _decade_max(decade, qualifier), m.group(0)

    # 「XX代前後まで」「XX歳代前後まで」（40代前後まで → 49）
    m = re.search(r"([2-9]\d)\s*(?:歳)?\s*代\s*前後\s*(?:まで|迄)", seg)
    if m:
        return _decade_max(int(m.group(1)), ""), m.group(0)

    # 「〜XX代後半」「XX代前半まで」等（修飾語必須、bare な「XX代」は採用しない）
    m = re.search(r"[〜~]?\s*([2-9]\d)\s*代(前半|後半|半ば)?\s*(?:まで|迄|以下|希望|程度)", seg)
    if m:
        decade = int(m.group(1))
        qualifier = m.group(2) or ""
        return _decade_max(decade, qualifier), m.group(0)

    # 「〜XX代前半/後半/半ば」（冒頭の〜が上限表現、qualifier必須、まで等不要）
    # 例: 年齢制限:~50代前半 → age_max=54
    m = re.search(r"[〜~]\s*([2-9]\d)\s*(?:歳)?\s*代(前半|後半|半ば)", seg)
    if m:
        decade = int(m.group(1))
        qualifier = m.group(2)
        return _decade_max(decade, qualifier), m.group(0)

    # 「〜XX代」（冒頭の〜が上限表現、qualifier なし）→ decade末尾を採用
    # 例: 年齢制限:~40代 → age_max=49
    m = re.search(r"[〜~]\s*([2-9]\d)\s*(?:歳)?\s*代(?!\s*(?:前半|後半|半ば))", seg)
    if m:
        decade = int(m.group(1))
        return decade + 9, m.group(0)

    # 単純な年齢指定（年齢: 55歳 等、修飾語なし）
    m = re.search(r"[：:]\s*(\d{2})\s*(?:歳|才)", seg)
    if m:
        return int(m.group(1)), m.group(0)

    # 「〜XX歳」（上限表現、まで/以下なし）- 年齢制限:～50歳 等
    m = re.search(r"[〜~]\s*(\d{2})\s*(?:歳|才)(?!\s*[以]?上)", seg)
    if m:
        return int(m.group(1)), m.group(0)

    return None


def _extract_from_fulltext_fallback(text: str) -> Optional[Tuple[int, str]]:
    """
    キーワードアンカーなしで全行を走査するfallback。
    _get_age_segments が空だった場合（年齢/年令/年代 アンカー不在）に呼ぶ。
    誤検知抑制のため 歳/才 または XX代+修飾語 の組み合わせに限定。
    """
    for line in text.split('\n'):
        stripped = line.strip()
        if not stripped:
            continue

        # 【年齢】なし / 【年齢】特になし 等（bracket付きラベル）
        m = re.search(r"[【\[]\s*年\s*齢\s*[】\]]\s*(なし|特になし|確認中|制限なし)", stripped)
        if m:
            return AGE_DEFAULT, m.group(0)

        # XX歳以上NG
        m = re.search(r"(\d{2})\s*(?:歳|才)\s*以上\s*(?:原則\s*)?NG", stripped)
        if m:
            return int(m.group(1)) - 1, m.group(0)

        # XX歳未満（strict < → XX - 1）
        m = re.search(r"(\d{2})\s*(?:歳|才)\s*未満", stripped)
        if m:
            return int(m.group(1)) - 1, m.group(0)

        # XX代不可 / XX歳代不可 → max = decade - 1
        m = re.search(r"([2-9]\d)\s*(?:歳)?\s*代\s*不可", stripped)
        if m:
            return int(m.group(1)) - 1, m.group(0)

        # 代の範囲（20代〜40代まで）→ 上限の代を採用
        m = re.search(r"([2-9]\d)\s*(?:歳)?\s*代(?:前半|後半|半ば)?\s*[〜~]\s*([2-9]\d)\s*(?:歳)?\s*代(前半|後半|半ば)?\s*(?:まで|迄)?", stripped)
        if m:
            decade = int(m.group(2))
            qualifier = m.group(3) or ""
            return _decade_max(decade, qualifier), m.group(0)

        # XX代前半/後半/半ばまで（例: 50代前半まで）
        m = re.search(r"([2-9]\d)\s*代(前半|後半|半ば)?\s*(?:まで|迄|以下)", stripped)
        if m:
            decade = int(m.group(1))
            return _decade_max(decade, m.group(2) or ""), m.group(0)

        # XX歳まで / XX歳以下
        m = re.search(r"(\d{2})\s*(?:歳|才)\s*(?:まで|迄|以下)", stripped)
        if m:
            return int(m.group(1)), m.group(0)

        # XX歳くらい/ぐらいまで
        m = re.search(r"(\d{2})\s*(?:歳|才)\s*[くぐ]?らい(?:まで|迄)?", stripped)
        if m:
            return int(m.group(1)), m.group(0)

        # 「XX歳前後希望」「XX歳前後まで」
        m = re.search(r"(\d{2})\s*(?:歳|才)\s*前後\s*(?:希望|まで|迄|程度)", stripped)
        if m:
            return int(m.group(1)), m.group(0)

        # XX歳代(前半/後半/半ば)?(まで等) - 「40歳代まで」「50歳代前半まで」形式
        m = re.search(r"([2-9]\d)\s*歳\s*代(前半|後半|半ば)?\s*(?:まで|迄|以下|希望|程度|望ましい)", stripped)
        if m:
            decade = int(m.group(1))
            qualifier = m.group(2) or ""
            return _decade_max(decade, qualifier), m.group(0)
        m = re.search(r"([2-9]\d)\s*歳\s*代(前半|後半|半ば)", stripped)
        if m:
            decade = int(m.group(1))
            qualifier = m.group(2)
            return _decade_max(decade, qualifier), m.group(0)

        # 「XX代前後まで」「XX歳代前後まで」（40代前後まで → 49）
        m = re.search(r"([2-9]\d)\s*(?:歳)?\s*代\s*前後\s*(?:まで|迄)", stripped)
        if m:
            return _decade_max(int(m.group(1)), ""), m.group(0)

        # XX代希望 / XX代可 / XX代尚可
        m = re.search(r"([2-9]\d)\s*代(前半|後半|半ば)?\s*(?:希望|尚可|可)\b", stripped)
        if m:
            decade = int(m.group(1))
            return _decade_max(decade, m.group(2) or ""), m.group(0)

        # 〜XX代前半/後半/半ば（冒頭の〜が上限表現、qualifier必須）
        m = re.search(r"[〜~]\s*([2-9]\d)\s*(?:歳)?\s*代(前半|後半|半ば)", stripped)
        if m:
            decade = int(m.group(1))
            qualifier = m.group(2)
            return _decade_max(decade, qualifier), m.group(0)

        # 〜XX代（冒頭の〜が上限表現、qualifier なし）
        m = re.search(r"[〜~]\s*([2-9]\d)\s*(?:歳)?\s*代(?!\s*(?:前半|後半|半ば))", stripped)
        if m:
            decade = int(m.group(1))
            return decade + 9, m.group(0)

        # 〜XX歳（上限表現）
        m = re.search(r"[〜~]\s*(\d{2})\s*(?:歳|才)(?!\s*[以]?上)", stripped)
        if m:
            return int(m.group(1)), m.group(0)

    return None


def rule_extract_age(body: str) -> Tuple[int, str, Optional[str]]:
    """
    ルールベースで年齢上限を抽出。
    Returns: (age_max, source, age_raw)
      source: "extracted" or "default"
    """
    if not body:
        return AGE_DEFAULT, "default", None

    text = _n(body)
    segments = _get_age_segments(text)

    # ① キーワードアンカー周辺のセグメントを優先スキャン
    for seg in segments:
        result = _extract_from_segment(seg)
        if result is not None:
            age_max, age_raw = result
            return age_max, "extracted", age_raw

    # ② セグメントで取れなかった場合: 全行 fallback
    result = _extract_from_fulltext_fallback(text)
    if result is not None:
        age_max, age_raw = result
        return age_max, "extracted", age_raw

    return AGE_DEFAULT, "default", None


# ── レコード構築 ──────────────────────────────────────────
def build_record(mid: str, body: str) -> Dict:
    age_max, source, age_raw = rule_extract_age(body)
    return {
        "message_id": mid,
        "age_max": age_max,
        "age_max_source": source,
        "age_raw": age_raw,
    }


# ── メイン ────────────────────────────────────────────────
def main() -> None:
    start = time.time()
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = dirs["result"]

    for path in [INPUT_PROJECTS, INPUT_CLEANED]:
        if not Path(path).exists():
            logger.error(f"入力ファイルが存在しません: {path}")
            sys.exit(1)

    try:
        project_ids = [r["message_id"] for r in read_jsonl_as_list(INPUT_PROJECTS)]
        cleaned_map = read_jsonl_as_dict(INPUT_CLEANED)
    except Exception as e:
        write_error_log(str(result_dir), e, "入力ファイル読み込みエラー")
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    extracted: list = []
    null_records: list = []

    for mid in project_ids:
        body = (cleaned_map.get(mid) or {}).get("body_text", "")
        rec  = build_record(mid, body)

        # age_max が None/unknown になることは設計上ないが念のため分離
        if rec["age_max"] is None:
            null_records.append(rec)
        else:
            extracted.append(rec)

        logger.info(
            f"{mid} → age_max={rec['age_max']} source={rec['age_max_source']} raw={rec['age_raw']}",
            message_id=mid,
        )

    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path,  extracted)
    write_jsonl(null_path, null_records)

    elapsed = time.time() - start
    total   = len(project_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    extracted_count = sum(1 for r in extracted if r["age_max_source"] == "extracted")
    default_count   = sum(1 for r in extracted if r["age_max_source"] == "default")
    logger.ok(
        f"Step完了: 入力={total}件 / 抽出={extracted_count}件 / "
        f"デフォルト={default_count}件 / null={len(null_records)}件"
    )


if __name__ == "__main__":
    main()

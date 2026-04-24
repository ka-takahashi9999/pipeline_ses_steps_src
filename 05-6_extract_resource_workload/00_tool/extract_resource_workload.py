#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 05-6: 要員メールから稼働率をルールベースで抽出

ルール:
  ① %範囲「60%〜80%稼働」→ workload_min=60, workload_max=80
  ② %単体「80%稼働」→ workload_min=80, workload_max=80
  ③ 週N〜M日（稼働）→ workload_min=N*20, workload_max=M*20
  ④ 週N日以上/から → workload_min=N*20, workload_max=100
  ⑤ 週N日まで/以下 → workload_min=20, workload_max=N*20
  ⑥ 週N日（単体）→ workload_min=N*20, workload_max=N*20
  ⑦ 副業希望/副業可 → workload_min=20, workload_max=80
  ⑧ フルタイム/常勤/フル稼働 → workload_min=100, workload_max=100（source=extracted）
  ⑨ 記載がない場合 → workload_min=100, workload_max=100（source=default）

注意:
  ・週N日パターンはリモート/在宅の修飾が続く場合はスキップ（稼働日数ではなくリモート日数）
  ・%パターンは稼働コンテキスト外の偶発的マッチを避けるため稼働関連キーワード近傍を優先

値域:
  ・1〜100の整数（%換算）
  ・週1日=20%, 週2日=40%, 週3日=60%, 週4日=80%, 週5日=100%

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/resources.jsonl

出力①（抽出結果）:
  01_result/extract_resource_workload.jsonl
出力②（異常レコード、本来0件）:
  01_result/99_workload_null_extract_resource_workload.jsonl
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

STEP_NAME = "05-6_extract_resource_workload"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_RESOURCES = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "resources.jsonl"
)
OUTPUT_EXTRACTED = "extract_resource_workload.jsonl"
OUTPUT_NULL      = "99_workload_null_extract_resource_workload.jsonl"

DEFAULT_WORKLOAD = 100
_DAYS_TO_PCT = {1: 20, 2: 40, 3: 60, 4: 80, 5: 100}
_KANJI_DAYS  = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5}


# ── 正規化 ─────────────────────────────────────────────────
def _n(s: str) -> str:
    """NFKC正規化（全角→半角、波ダッシュ等を統一）"""
    return unicodedata.normalize("NFKC", s or "")


def _to_days(raw: str) -> Optional[int]:
    """数字または漢数字を日数（int）に変換する。変換不可はNone。"""
    raw = raw.strip()
    if raw in _KANJI_DAYS:
        return _KANJI_DAYS[raw]
    try:
        d = int(raw)
        return d if 1 <= d <= 5 else None
    except ValueError:
        return None


def _days_pct(days: int) -> int:
    """週N日を%に変換する（週5日=100%）。"""
    return _DAYS_TO_PCT.get(days, min(days * 20, 100))


# ── 抽出パターン（優先度順に評価）────────────────────────

# 1. %範囲（稼働コンテキスト）: 60%〜80%稼働 / 稼働率60%〜80%
_PCT_RANGE_RE = re.compile(
    r"(?:稼働\s*率?\s*)?(\d{1,3})\s*%\s*[〜~]\s*(\d{1,3})\s*%\s*(?:稼働|程度|前後|で)?|"
    r"[【\[]?\s*(?:希望)?稼働\s*率?\s*[】\]]?\s*[：:]?\s*(\d{1,3})\s*%\s*[〜~]\s*(\d{1,3})\s*%"
)

# 2. %単体（稼働コンテキスト必須）: 80%稼働 / 稼働率80% / 稼働80%
_PCT_SINGLE_RE = re.compile(
    r"(\d{1,3})\s*%\s*稼働|"
    r"[【\[]?\s*(?:希望)?稼働\s*率?\s*[】\]]?\s*[：:]?\s*(\d{1,3})\s*%|"
    r"稼働\s*(\d{1,3})\s*%"
)

# 3. 週N〜M日（稼働日数）: 週3〜4日、週3日〜4日
# ※「回」は出社回数（リモート文脈）と区別できないため除外し「日」のみ対象とする
_WEEK_RANGE_RE = re.compile(
    r"週\s*([1-5一二三四五])\s*日?\s*[〜~]\s*([1-5一二三四五])\s*日"
    r"(?!\s*(?:リモート|在宅|テレワーク|出社))"
)

# 4. 週N日以上/から（下限のみ）
_WEEK_MIN_RE = re.compile(
    r"週\s*([1-5一二三四五])\s*日\s*(?:以上|から|〜)"
    r"(?!\s*(?:リモート|在宅|テレワーク|出社))"
)

# 5. 週N日まで/以下/程度（上限のみ）
_WEEK_MAX_RE = re.compile(
    r"週\s*([1-5一二三四五])\s*日\s*(?:まで|以下|程度)"
    r"(?!\s*(?:リモート|在宅|テレワーク|出社))"
)

# 6. 週N日（単体）: 週3日
_WEEK_EXACT_RE = re.compile(
    r"週\s*([1-5一二三四五])\s*日"
    r"(?!\s*(?:リモート|在宅|テレワーク|出社|以上|から|まで|以下|程度|[〜~]))"
)

# 7. 副業希望・副業可（要員側の表現を追加）
_SIDE_JOB_RE = re.compile(
    r"副\s*業\s*(?:可|OK|歓迎|不問|での?\s*参加|可能|希望|前提)"
    r"|副\s*業\s*案\s*件(?:\s*希望)?"
    r"|副\s*業\s*と\s*し\s*て"
    r"|複\s*業\s*(?:可|OK|希望)"
)

# 8. フルタイム/常勤/フル稼働
_FULLTIME_RE = re.compile(
    r"フル\s*タイム|常\s*勤|フル\s*稼働|週\s*5\s*日\s*稼働|フル\s*コミット"
)

# ── 抽出関数 ─────────────────────────────────────────────
def rule_extract_workload(body: str) -> Tuple[int, int, str, Optional[str]]:
    """
    ルールベースで稼働率を抽出する。

    Returns:
        (workload_min, workload_max, source, workload_raw)
          source: "extracted" or "default"
          workload_raw: マッチした文字列 or None
    """
    if not body:
        return DEFAULT_WORKLOAD, DEFAULT_WORKLOAD, "default", None

    text = _n(body)

    # 1. %範囲（稼働コンテキスト）
    m = _PCT_RANGE_RE.search(text)
    if m:
        lo_raw = m.group(1) or m.group(3)
        hi_raw = m.group(2) or m.group(4)
        lo, hi = int(lo_raw), int(hi_raw)
        if 1 <= lo <= 100 and 1 <= hi <= 100:
            return min(lo, hi), max(lo, hi), "extracted", m.group(0)

    # 2. %単体（稼働コンテキスト必須）
    m = _PCT_SINGLE_RE.search(text)
    if m:
        pct_raw = m.group(1) or m.group(2) or m.group(3)
        pct = int(pct_raw)
        if 1 <= pct <= 100:
            return pct, pct, "extracted", m.group(0)

    # 3. 週N〜M日（稼働日数）
    m = _WEEK_RANGE_RE.search(text)
    if m:
        lo_days = _to_days(m.group(1))
        hi_days = _to_days(m.group(2))
        if lo_days is not None and hi_days is not None:
            return (
                _days_pct(min(lo_days, hi_days)),
                _days_pct(max(lo_days, hi_days)),
                "extracted",
                m.group(0),
            )

    # 4. 週N日以上（下限のみ）
    m = _WEEK_MIN_RE.search(text)
    if m:
        days = _to_days(m.group(1))
        if days is not None:
            return _days_pct(days), DEFAULT_WORKLOAD, "extracted", m.group(0)

    # 5. 週N日まで（上限のみ）
    m = _WEEK_MAX_RE.search(text)
    if m:
        days = _to_days(m.group(1))
        if days is not None:
            return 20, _days_pct(days), "extracted", m.group(0)

    # 6. 週N日（単体）
    m = _WEEK_EXACT_RE.search(text)
    if m:
        days = _to_days(m.group(1))
        if days is not None:
            pct = _days_pct(days)
            return pct, pct, "extracted", m.group(0)

    # 7. 副業希望/副業可
    m = _SIDE_JOB_RE.search(text)
    if m:
        return 20, 80, "extracted", m.group(0)

    # 8. フルタイム/常勤/フル稼働
    m = _FULLTIME_RE.search(text)
    if m:
        return DEFAULT_WORKLOAD, DEFAULT_WORKLOAD, "extracted", m.group(0)

    return DEFAULT_WORKLOAD, DEFAULT_WORKLOAD, "default", None


# ── レコード構築 ──────────────────────────────────────────
def build_record(mid: str, body: str) -> Dict:
    wmin, wmax, source, raw = rule_extract_workload(body)
    return {
        "message_id": mid,
        "workload_min": wmin,
        "workload_max": wmax,
        "workload_max_source": source,
        "workload_raw": raw,
    }


def _is_valid(rec: Dict) -> bool:
    """workload_min/max が 1〜100 の整数であること。"""
    for key in ("workload_min", "workload_max"):
        v = rec.get(key)
        if not isinstance(v, int) or not (1 <= v <= 100):
            return False
    return True


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

        if not _is_valid(rec):
            null_records.append(rec)
        else:
            extracted.append(rec)

        logger.info(
            f"{mid} → min={rec['workload_min']} max={rec['workload_max']} "
            f"source={rec['workload_max_source']} raw={rec['workload_raw']}",
            message_id=mid,
        )

    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path,  extracted)
    write_jsonl(null_path, null_records)

    elapsed = time.time() - start
    total   = len(resource_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    full_count    = sum(1 for r in extracted if r["workload_max"] == 100 and r["workload_min"] == 100)
    partial_count = sum(1 for r in extracted if r["workload_max"] < 100)
    ext_count     = sum(1 for r in extracted if r["workload_max_source"] == "extracted")
    def_count     = sum(1 for r in extracted if r["workload_max_source"] == "default")

    logger.ok(
        f"Step完了: 入力={total}件 / フルタイム={full_count}件 / "
        f"部分稼働={partial_count}件 "
        f"(extracted={ext_count}件 default={def_count}件) / null={len(null_records)}件"
    )


if __name__ == "__main__":
    main()

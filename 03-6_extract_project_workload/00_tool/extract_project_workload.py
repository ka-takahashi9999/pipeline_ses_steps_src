#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 03-6: 案件メールから稼働率をルールベースで抽出

ルール:
  ① %範囲指定「60%〜80%稼働」→ workload_min=60, workload_max=80
  ② %単体指定「80%稼働」→ workload_min=80, workload_max=80
  ③ 稼働時間H/月（140〜180H, 140-180H等）→ workload_min=100, workload_max=100
  ④ フルタイム/常勤/フル稼働 → workload_min=100, workload_max=100（source=extracted）
  ⑤ 稼働率:週5 / 稼働:週5（160h）→ workload_min=100, workload_max=100
  ⑥ 記載がない場合 → workload_min=100, workload_max=100（source=default）

注意:
  ・週N日/週N回は原則 workload 根拠として使わない
  ・休日/シフト/リモート頻度/出社頻度の週N日表現は抽出対象外
  ・%パターンは稼働コンテキスト外の偶発的マッチを避けるため稼働関連キーワード近傍を優先

値域:
  ・1〜100の整数（%換算）
  ・週1日=20%, 週2日=40%, 週3日=60%, 週4日=80%, 週5日=100%

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/projects.jsonl

出力①（抽出結果）:
  01_result/extract_project_workload.jsonl
出力②（異常レコード、本来0件）:
  01_result/99_workload_null_extract_project_workload.jsonl
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

STEP_NAME = "03-6_extract_project_workload"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_PROJECTS = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl"
)
OUTPUT_EXTRACTED = "extract_project_workload.jsonl"
OUTPUT_NULL      = "99_workload_null_extract_project_workload.jsonl"

DEFAULT_WORKLOAD = 100


# ── 正規化 ────────────────────────────────────────────────
def _n(s: str) -> str:
    """NFKC正規化（全角→半角、波ダッシュ等を統一）"""
    return unicodedata.normalize("NFKC", s or "")


# ── 抽出パターン（優先度順に評価） ────────────────────────

# 1. %範囲（稼働コンテキスト）: 60%〜80%稼働 / 稼働率60%〜80%
_PCT_RANGE_WORKLOAD_RE = re.compile(
    r"(?:稼働\s*率?\s*)?(\d{1,3})\s*%\s*[〜~\-]\s*(\d{1,3})\s*%\s*(?:稼働|程度|前後|で)?|"
    r"[【\[]?\s*(?:想定)?稼働\s*率?\s*[】\]]?\s*[_＿]?\s*[：:]?\s*(\d{1,3})\s*%\s*[〜~\-]\s*(\d{1,3})\s*%"
)

# 2. %単体（稼働コンテキスト必須）: 80%稼働 / 稼働率80% / 稼働80%
_PCT_SINGLE_WORKLOAD_RE = re.compile(
    r"(\d{1,3})\s*%\s*稼働|"
    r"[【\[]?\s*(?:想定)?稼働\s*率?\s*[】\]]?\s*[_＿]?\s*[：:]?\s*(\d{1,3})\s*%|"
    r"稼働\s*(\d{1,3})\s*%"
)

# 参考用: 週N日/週N回は原則 workload 抽出に使わない
_WEEK_RANGE_RE = re.compile(
    r"週\s*([1-5一二三四五])\s*[日回]?\s*[〜~]\s*([1-5一二三四五])\s*[日回]"
    r"(?!\s*(?:リモート|在宅|テレワーク|出社))"
)

# 4. 週N日以上/から（下限のみ）
_WEEK_MIN_RE = re.compile(
    r"週\s*([1-5一二三四五])\s*[日回]\s*(?:以上|から|〜)"
    r"(?!\s*(?:リモート|在宅|テレワーク|出社))"
)

# 5. 週N日まで/以下（上限のみ）
_WEEK_MAX_RE = re.compile(
    r"週\s*([1-5一二三四五])\s*[日回]\s*(?:まで|以下|程度)"
    r"(?!\s*(?:リモート|在宅|テレワーク|出社))"
)

# 6. 週N日（単体）: 週3日
_WEEK_EXACT_RE = re.compile(
    r"週\s*([1-5一二三四五])\s*[日回]"
    r"(?!\s*(?:リモート|在宅|テレワーク|出社|以上|から|まで|以下|程度|[〜~]))"
)

# 週N日/週N回を workload 根拠に使わない安全側補助文脈
_WEEKLY_NON_WORKLOAD_RE = re.compile(
    r"(?:リモート|在宅|テレワーク|出社頻度|出勤|出社|休み|休日|シフト|土日休み)"
)

# 8. フルタイム/常勤/フル稼働
_FULLTIME_RE = re.compile(r"フル\s*タイム|常\s*勤|フル\s*稼働|週\s*5\s*[日回]\s*稼働|フル\s*コミット")

# 9. 稼働率/稼働: 週5系 → 100%
_WEEK5_WORKLOAD_RE = re.compile(
    r"(?:稼働\s*率?|稼働)\s*[：:]?\s*週\s*5\s*(?:日|回)?(?:のみ|\(\s*\d+\s*[hH]\s*\))?"
)

# 10. 稼働時間（H/月 / 精算幅）: 140〜180H, 140-180H, 140-180h → 100%と扱う
_HOURS_RE = re.compile(
    r"(?:精算(?:幅|条件)?\s*[：:]?\s*)\d{2,3}\s*[hH]?\s*[〜~\-]\s*\d{2,3}\s*[hH]?(?:\s*/?\s*月)?|"
    r"\d{2,3}\s*[hH]\s*[〜~\-]\s*\d{2,3}\s*[hH](?:\s*/?\s*月)?|"
    r"\d{2,3}\s*[hH]\s*/?\s*月"
)


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
    m = _PCT_RANGE_WORKLOAD_RE.search(text)
    if m:
        lo_raw = m.group(1) or m.group(3)
        hi_raw = m.group(2) or m.group(4)
        lo, hi = int(lo_raw), int(hi_raw)
        if 1 <= lo <= 100 and 1 <= hi <= 100:
            return min(lo, hi), max(lo, hi), "extracted", m.group(0)

    # 2. %単体（稼働コンテキスト必須）
    m = _PCT_SINGLE_WORKLOAD_RE.search(text)
    if m:
        pct_raw = m.group(1) or m.group(2) or m.group(3)
        pct = int(pct_raw)
        if 1 <= pct <= 100:
            return pct, pct, "extracted", m.group(0)

    # 3. 稼働時間（H/月 / 精算幅）→ 100%
    m = _HOURS_RE.search(text)
    if m:
        return DEFAULT_WORKLOAD, DEFAULT_WORKLOAD, "extracted", m.group(0)

    # 4. フルタイム/常勤/フル稼働
    m = _FULLTIME_RE.search(text)
    if m:
        return DEFAULT_WORKLOAD, DEFAULT_WORKLOAD, "extracted", m.group(0)

    # 5. 稼働率/稼働: 週5系
    m = _WEEK5_WORKLOAD_RE.search(text)
    if m:
        return DEFAULT_WORKLOAD, DEFAULT_WORKLOAD, "extracted", m.group(0)

    # 週N日/週N回は原則 workload 根拠に使わない。
    # 休日・リモート頻度・出社頻度もここで明示的に抽出対象外とする。
    if (
        _WEEK_RANGE_RE.search(text)
        or _WEEK_MIN_RE.search(text)
        or _WEEK_MAX_RE.search(text)
        or _WEEK_EXACT_RE.search(text)
    ):
        if _WEEKLY_NON_WORKLOAD_RE.search(text):
            return DEFAULT_WORKLOAD, DEFAULT_WORKLOAD, "default", None

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
    total   = len(project_ids)
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

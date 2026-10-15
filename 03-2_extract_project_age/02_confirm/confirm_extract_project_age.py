#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 03-2 confirmスクリプト

【チェック①: null率チェック】
  - age_maxのnull/unknown率が0%であること

【チェック②: 異常値チェック】
  - age_maxが18未満または100超の件をmessage_idとともに出力

終了コード:
  0: 全チェック OK
  1: チェック NG（Pipeline停止）
"""

import sys
import unicodedata
import re
from pathlib import Path
from typing import Dict, List

_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import get_result_path
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "confirm_03-2_extract_project_age"
logger = get_logger(STEP_NAME)

INPUT_PROJECTS = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl"
)
INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
OUTPUT_EXTRACTED      = "extract_project_age.jsonl"
OUTPUT_NULL           = "99_age_null_extract_project_age.jsonl"
OUTPUT_DEFAULT_SIGNAL = "99_default_with_age_signal.jsonl"
OUTPUT_CLASSIFIED     = "99_default_with_age_signal_classified.jsonl"

ANOMALY_LOW  = 18
ANOMALY_HIGH = 100

# 年齢シグナル検出用（source=default なのに本文に年齢情報がある可能性の判定）
_SIGNAL_RE = re.compile(
    r"\d{2}\s*(?:歳|才)"         # 数字+歳/才
    r"|[2-9]\d\s*代"             # XX代
    r"|年\s*齢"                   # 年齢
    r"|不問"
    r"|制限\s*な[しい]"
)

# ── WARN レコードの自動分類パターン ──────────────────────────
# profile_or_resource: 要員紹介メール混入（候補者プロフィール行）
_CLASSIFY_PROFILE_RE = re.compile(
    r"【氏名】|【名前】|要員情報|イニシャル"
    r"|氏\s*名\s*[：:]"
    r"|【年\s*齢】\s*\d"          # 【年 齢】47歳 形式
)

# ambiguous_lower_bound: 下限のみ表現（上限確定不可）
_CLASSIFY_LOWER_BOUND_RE = re.compile(
    r"[2-9]\d\s*代(?:前半|後半|半ば)?\s*以上"
    r"|(?:\d{2})\s*(?:歳|才)\s*以上(?!\s*(?:原則)?\s*NG)"
    r"|[2-9]\d\s*代\s*以降"
)

# genuine_missing: 上限年齢条件が本文にあるのに default のまま
_CLASSIFY_GENUINE_RE = re.compile(
    r"[〜~]\s*\d{2}\s*(?:歳|才)"              # ~XX歳
    r"|[〜~]\s*\d+\s*代"                       # ~XX代
    r"|(?:\d{2})\s*(?:歳|才)\s*(?:まで|以下|未満)"  # XX歳まで/以下/未満
    r"|(?:\d{2})\s*(?:歳|才)\s*以上\s*(?:原則)?\s*NG"  # XX歳以上NG
    r"|[2-9]\d\s*(?:歳)?\s*代\s*(?:まで|迄|以下|不可)"  # XX代まで/不可
    r"|[2-9]\d\s*歳\s*代\s*(?:まで|迄|望ましい)"  # XX歳代まで
)

# non_age_signal: 年齢ではない文脈での不問等
_CLASSIFY_NON_AGE_RE = re.compile(
    r"(?:分野|言語|業種|職種|年数|業務内容)\s*不問"
    r"|商流制限?\s*な[しい]"
)


def _classify_signal(body_n: str) -> tuple:
    """
    NFKC正規化済み本文を受け取り、(category, matched_text) を返す。
    カテゴリ優先順: genuine_missing > ambiguous_lower_bound > profile_or_resource
                   > non_age_signal > other
    ※ 年齢条件パターンを profile 判定より優先させることで、
      案件メールに候補者プロフィールが混在するケースを正しく分類する。
    """
    m = _CLASSIFY_GENUINE_RE.search(body_n)
    if m:
        return "genuine_missing", m.group(0)

    m = _CLASSIFY_LOWER_BOUND_RE.search(body_n)
    if m:
        return "ambiguous_lower_bound", m.group(0)

    m = _CLASSIFY_PROFILE_RE.search(body_n)
    if m:
        return "profile_or_resource", m.group(0)

    m = _CLASSIFY_NON_AGE_RE.search(body_n)
    if m:
        return "non_age_signal", m.group(0)

    return "other", ""


def check_null_rate(total: int, null_count: int) -> bool:
    """チェック①: null率チェック（0%であること）"""
    logger.info("=== チェック①: null率チェック ===")
    if total == 0:
        logger.error("レコードが0件です。")
        return False

    null_rate = null_count / total
    logger.info(f"  入力件数   : {total}件")
    logger.info(f"  抽出成功   : {total - null_count}件 ({(1 - null_rate) * 100:.1f}%)")
    logger.info(f"  null 件数  : {null_count}件 ({null_rate * 100:.1f}%)")

    if null_count > 0:
        logger.error(f"null件数が0ではありません: {null_count}件")
        return False

    logger.ok("null率OK: 0%")
    return True


def check_anomalies(extracted: List[Dict]) -> bool:
    """チェック②: 異常値チェック（18未満または100超）"""
    logger.info("=== チェック②: 異常値チェック ===")

    low_anomalies  = [r for r in extracted if r["age_max"] < ANOMALY_LOW]
    high_anomalies = [r for r in extracted if r["age_max"] > ANOMALY_HIGH]

    def _show(label: str, records: List[Dict]) -> None:
        if not records:
            logger.info(f"  [{label}] 該当なし")
            return
        logger.info(f"  [{label}] {len(records)}件")
        logger.info(f"  {'message_id':<24} {'age_max':>8}  source  age_raw")
        logger.info(f"  {'-'*24} {'-'*8}  {'-'*10}  {'-'*30}")
        for r in records:
            logger.info(
                f"  {r['message_id']:<24} {r['age_max']:>8}  "
                f"{r['age_max_source']:<10}  {str(r.get('age_raw', ''))[:40]}"
            )

    _show(f"age_max < {ANOMALY_LOW}（18歳未満）", low_anomalies)
    _show(f"age_max > {ANOMALY_HIGH}（100超）", high_anomalies)

    return True  # 異常値は人間確認用のためNG判定なし


def check_default_source(extracted: List[Dict], cleaned_map: Dict, result_dir: Path) -> None:
    """チェック③: extract_project_age.jsonl 内の source=default / age_raw=null を集計し、
    年齢シグナルがあるのに default になっているレコードをカテゴリ別に警告する"""
    logger.info("=== チェック③: source=default / age_raw=null 集計 ===")

    total         = len(extracted)
    default_recs  = [r for r in extracted if r.get("age_max_source") == "default"]
    null_raw_recs = [r for r in extracted if r.get("age_raw") is None]

    logger.info(f"  total           : {total}件")
    logger.info(f"  source=default  : {len(default_recs)}件 ({len(default_recs)/total*100:.1f}%)")
    logger.info(f"  age_raw=null    : {len(null_raw_recs)}件 ({len(null_raw_recs)/total*100:.1f}%)")

    # 年齢シグナルがあるのに default になっているものを検出・分類
    signal_records = []
    classified_records = []
    for rec in default_recs:
        mid = rec["message_id"]
        body = (cleaned_map.get(mid) or {}).get("body_text", "")
        body_n = unicodedata.normalize("NFKC", body or "")
        if not _SIGNAL_RE.search(body_n):
            continue
        category, matched_text = _classify_signal(body_n)
        signal_records.append({"message_id": mid, "age_max_source": rec["age_max_source"]})
        classified_records.append({
            "message_id": mid,
            "category": category,
            "matched_text": matched_text,
        })

    # 候補0件でも旧confirm結果が残らないよう、判定結果を必ずatomic writeする。
    out_signal = str(result_dir / OUTPUT_DEFAULT_SIGNAL)
    out_classified = str(result_dir / OUTPUT_CLASSIFIED)
    write_jsonl(out_signal, signal_records)
    write_jsonl(out_classified, classified_records)

    if not signal_records:
        logger.ok("  年齢シグナルありのdefaultレコード: 0件")
        return

    logger.warn(
        f"  [警告] 年齢シグナルありだが source=default: {len(signal_records)}件"
    )

    # カテゴリ別集計・サンプル表示
    cat_order = [
        "genuine_missing",
        "ambiguous_lower_bound",
        "profile_or_resource",
        "non_age_signal",
        "other",
    ]
    cat_map: Dict[str, List[Dict]] = {c: [] for c in cat_order}
    for cr in classified_records:
        cat_map[cr["category"]].append(cr)

    logger.info("  ── カテゴリ別件数 ──")
    for cat in cat_order:
        items = cat_map[cat]
        if not items:
            continue
        logger.info(f"  [{cat}] {len(items)}件")
        for item in items[:5]:
            logger.info(f"    {item['message_id']}  matched={item['matched_text']!r}")

    # 補助出力
    logger.info(f"  補助出力: {out_signal}")
    logger.info(f"  分類出力: {out_classified}")


def check_distribution(extracted: List[Dict]) -> None:
    """age_max の分布サマリ（参考出力）"""
    logger.info("=== 参考: age_max 分布 ===")

    buckets = [
        ("〜35歳",  lambda a: a <= 35),
        ("36〜45歳", lambda a: 36 <= a <= 45),
        ("46〜55歳", lambda a: 46 <= a <= 55),
        ("56〜59歳", lambda a: 56 <= a <= 59),
        ("60〜99歳", lambda a: 60 <= a <= 99),
        ("100（不問/デフォルト）", lambda a: a == 100),
    ]

    total = len(extracted)
    source_extracted = sum(1 for r in extracted if r["age_max_source"] == "extracted")
    source_default   = sum(1 for r in extracted if r["age_max_source"] == "default")
    logger.info(f"  source=extracted : {source_extracted}件 ({source_extracted/total*100:.1f}%)")
    logger.info(f"  source=default   : {source_default}件 ({source_default/total*100:.1f}%)")
    logger.info("")

    for label, fn in buckets:
        cnt  = sum(1 for r in extracted if fn(r["age_max"]))
        rate = cnt / total * 100
        bar  = "█" * int(rate / 3)
        logger.info(f"  {label:<18} {cnt:>5}件 {rate:>5.1f}%  {bar}")


def main() -> None:
    ext_path  = get_result_path(str(_STEP_DIR), OUTPUT_EXTRACTED)
    null_path = get_result_path(str(_STEP_DIR), OUTPUT_NULL)
    result_dir = Path(ext_path).parent

    for path in [INPUT_PROJECTS, ext_path, null_path]:
        if not Path(path).exists():
            logger.error(f"ファイルが存在しません: {path}")
            sys.exit(1)

    try:
        project_ids = read_jsonl_as_list(INPUT_PROJECTS)
        extracted   = read_jsonl_as_list(ext_path)
        null_recs   = read_jsonl_as_list(null_path)
    except Exception as e:
        logger.error(f"ファイル読み込みエラー: {e}")
        sys.exit(1)

    # cleaned_map は check_default_source で使う（存在しなければ空dict）
    cleaned_map: Dict = {}
    if Path(INPUT_CLEANED).exists():
        try:
            from common.json_utils import read_jsonl_as_dict
            cleaned_map = read_jsonl_as_dict(INPUT_CLEANED)
        except Exception:
            pass

    total      = len(project_ids)
    null_count = len(null_recs)

    ok1 = check_null_rate(total, null_count)
    check_anomalies(extracted)
    check_default_source(extracted, cleaned_map, result_dir)
    check_distribution(extracted)

    if not ok1:
        logger.error("confirm NG。Pipelineを停止します。")
        sys.exit(1)

    logger.ok(
        f"confirm OK: 入力={total}件 / 抽出={len(extracted)}件 / null={null_count}件"
    )


if __name__ == "__main__":
    main()

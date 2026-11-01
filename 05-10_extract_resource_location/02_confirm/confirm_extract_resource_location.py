#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 05-10 confirmスクリプト

【チェック①: 件数整合チェック】
  - 入力 resources.jsonl の件数 = 出力①件数 + 出力②件数

【チェック②: location値チェック】
  - location の値が「9地方 / overseas / unknown」以外の値がないこと
  - null禁止: locationにnullが含まれていないこと

【チェック③: 署名由来語チェック】
  - location_raw に署名由来語（TEL/FAX/Email/本社/住所等）が含まれていないこと

【チェック④: schema / message_id チェック】
  - 出力が固定4キーと期待型を満たすこと
  - 入力・出力に message_id 重複がないこと
  - 入力と出力の message_id 集合が一致すること

終了コード:
  0: 全チェック OK
  1: チェック NG（Pipeline停止）
"""

import re
import sys
from collections import Counter
from pathlib import Path
from typing import List

_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import get_result_path
from common.json_utils import read_jsonl_as_list
from common.logger import get_logger

STEP_NAME = "confirm_05-10_extract_resource_location"
logger = get_logger(STEP_NAME)

INPUT_RESOURCES  = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "resources.jsonl"
)
OUTPUT_EXTRACTED = "extract_resource_location.jsonl"
OUTPUT_NULL      = "99_location_null_extract_resource_location.jsonl"

VALID_REGIONS = {
    "北海道地方",
    "東北地方",
    "関東地方",
    "中部地方",
    "近畿地方",
    "中国地方",
    "四国地方",
    "九州地方",
    "沖縄地方",
}

# location の全許容値（9地方 + overseas + unknown）
VALID_LOCATIONS = VALID_REGIONS | {"overseas", "unknown"}
EXPECTED_KEYS = {"message_id", "location", "location_raw", "location_source"}
VALID_SOURCES = {"label", "body", ""}

_SIGNATURE_WORD_RE = re.compile(
    r"TEL|FAX|E-?[Mm]ail|MAIL|メールアドレス|電話番号|携帯番号|"
    r"営業電話|"
    r"住所\s*[:：]|所在地\s*[:：]|"
    r"本\s*社\s*[:：]|"                                      # 本社：/ 本　　社：
    r"(?:東京|大阪|名古屋|福岡|札幌|仙台|横浜|神戸|京都)\s*本社|"  # 東京本社 etc.
    r"本社営業|"
    r"オフィス\s*[:：]|"
    r"本社\s*[/｜|]|[/｜|]\s*本社|"
    r"支社\s*[/｜|]|[/｜|]\s*支社|"
    r"支社営業部|支店営業部|"
    r"SES\s*事業部|営業部\s*[:：]|"
    r"共通メール|共通アドレス|受信用|"
    r"代表番号|"
    r"配信停止|配信解除|unsubscribe|派遣事業",
    re.IGNORECASE,
)


def check_count_consistency(total_input: int, extracted_count: int, null_count: int) -> bool:
    """チェック①: 件数整合チェック"""
    logger.info("=== チェック①: 件数整合チェック ===")
    total_output = extracted_count + null_count
    logger.info(f"  入力件数   : {total_input}件")
    logger.info(f"  出力①件数  : {extracted_count}件 (location非null)")
    logger.info(f"  出力②件数  : {null_count}件 (location=null)")
    logger.info(f"  出力合計   : {total_output}件")

    if total_input != total_output:
        logger.error(f"件数不一致: 入力={total_input}件 / 出力合計={total_output}件")
        return False

    logger.ok("件数整合OK")
    return True


def check_location_values(extracted: List[dict], null_records: List[dict]) -> bool:
    """チェック②: location値チェック（9地方 / overseas / unknown のみ、null禁止）"""
    logger.info("=== チェック②: location値チェック ===")

    invalid_records = []

    for rec in extracted:
        loc = rec.get("location")
        if loc is None:
            invalid_records.append((rec.get("message_id"), loc, "出力①にnullが含まれている（null禁止）"))
        elif loc not in VALID_LOCATIONS:
            invalid_records.append((rec.get("message_id"), loc, "想定外のlocation値"))

    for rec in null_records:
        loc = rec.get("location")
        if loc is not None:
            invalid_records.append((rec.get("message_id"), loc, "出力②にnull以外が含まれている"))

    if invalid_records:
        for mid, val, reason in invalid_records:
            logger.error(f"  {mid}: location={val!r} ({reason})")
        logger.error(f"location値異常: {len(invalid_records)}件")
        return False

    logger.ok("location値チェックOK: 全件が9地方 / overseas / unknown（nullなし）")
    return True


def check_signature_contamination(extracted: List[dict]) -> bool:
    """チェック③: 署名由来語チェック"""
    logger.info("=== チェック③: 署名由来語チェック ===")

    suspicious = []
    for rec in extracted:
        raw = rec.get("location_raw") or ""
        if _SIGNATURE_WORD_RE.search(raw):
            suspicious.append((rec.get("message_id"), rec.get("location"), raw[:80]))

    if suspicious:
        logger.warn(f"署名由来疑い: {len(suspicious)}件")
        for mid, loc, raw in suspicious:
            logger.warn(f"  {mid}: location={loc} / raw={raw!r}")
        logger.error("署名由来語が location_raw に含まれています。抽出ロジックを確認してください。")
        return False

    logger.ok("署名由来語チェックOK: 署名由来疑いなし")
    return True


def check_schema_and_message_ids(
    resources: List[dict], extracted: List[dict], null_records: List[dict]
) -> bool:
    """チェック④: 固定schema・型・message_id重複/接続チェック"""
    logger.info("=== チェック④: schema / message_id チェック ===")

    invalid = []
    output_records = extracted + null_records
    for index, rec in enumerate(output_records):
        if not isinstance(rec, dict):
            invalid.append((index, "recordがdictではない"))
            continue
        if set(rec.keys()) != EXPECTED_KEYS:
            invalid.append((index, f"キー不一致: {sorted(rec.keys())}"))
            continue
        if not isinstance(rec.get("message_id"), str) or not rec["message_id"]:
            invalid.append((index, "message_idが非空文字列ではない"))
        if not isinstance(rec.get("location"), str):
            invalid.append((index, "locationが文字列ではない"))
        if rec.get("location_raw") is not None and not isinstance(rec["location_raw"], str):
            invalid.append((index, "location_rawが文字列/nullではない"))
        if rec.get("location_source") not in VALID_SOURCES:
            invalid.append((index, f"location_sourceが想定外: {rec.get('location_source')!r}"))

    input_ids = [rec.get("message_id") for rec in resources]
    output_ids = [rec.get("message_id") for rec in output_records]
    input_duplicates = [mid for mid, count in Counter(input_ids).items() if count > 1]
    output_duplicates = [mid for mid, count in Counter(output_ids).items() if count > 1]

    if invalid:
        for index, reason in invalid[:10]:
            logger.error(f"  index={index}: {reason}")
        logger.error(f"schema/型異常: {len(invalid)}件")
        return False
    if input_duplicates or output_duplicates:
        logger.error(
            f"message_id重複: 入力={len(input_duplicates)}件 / "
            f"出力={len(output_duplicates)}件"
        )
        return False
    if set(input_ids) != set(output_ids):
        logger.error(
            f"message_id接続不一致: "
            f"入力のみ={len(set(input_ids) - set(output_ids))}件 / "
            f"出力のみ={len(set(output_ids) - set(input_ids))}件"
        )
        return False

    logger.ok("schema / message_id チェックOK: 型・重複・接続に異常なし")
    return True


def show_distribution(extracted: List[dict], null_records: List[dict]) -> None:
    """地方別・ソース別件数分布を出力（参考情報）"""
    all_recs = extracted + null_records
    counter: Counter = Counter()
    for rec in all_recs:
        loc = rec.get("location")
        counter[loc if loc is not None else "null"] += 1

    total = len(all_recs)
    if total == 0:
        return

    logger.info("=== 参考: 地方別件数分布 ===")
    for region in [
        "関東地方", "近畿地方", "中部地方", "九州地方",
        "北海道地方", "東北地方", "中国地方", "四国地方", "沖縄地方",
    ]:
        cnt = counter.get(region, 0)
        pct = cnt / total * 100 if total > 0 else 0.0
        logger.info(f"  {region:<12} {cnt:>5}件 ({pct:.1f}%)")

    overseas_cnt = counter.get("overseas", 0)
    unknown_cnt  = counter.get("unknown", 0)
    null_cnt     = counter.get("null", 0)
    pct_o = overseas_cnt / total * 100 if total > 0 else 0.0
    pct_u = unknown_cnt  / total * 100 if total > 0 else 0.0
    pct_n = null_cnt     / total * 100 if total > 0 else 0.0
    logger.info(f"  {'overseas':<12} {overseas_cnt:>5}件 ({pct_o:.1f}%)")
    logger.info(f"  {'unknown':<12} {unknown_cnt:>5}件 ({pct_u:.1f}%)")
    if null_cnt > 0:
        logger.warn(f"  {'null':<12} {null_cnt:>5}件 ({pct_n:.1f}%)  ← null禁止違反")
    else:
        logger.info(f"  {'null':<12} {null_cnt:>5}件 ({pct_n:.1f}%)  ← null 0件OK")

    logger.info("\n=== 参考: overseas 例（最大5件） ===")
    count = 0
    for rec in all_recs:
        if rec.get("location") == "overseas":
            mid = rec.get("message_id", "")
            raw = str(rec.get("location_raw") or "")[:60]
            logger.info(f"  {mid}: raw={raw!r}")
            count += 1
            if count >= 5:
                break

    logger.info("\n=== 参考: unknown 例（最大5件） ===")
    count = 0
    for rec in all_recs:
        if rec.get("location") == "unknown":
            mid = rec.get("message_id", "")
            raw = str(rec.get("location_raw") or "")[:60]
            logger.info(f"  {mid}: raw={raw!r}")
            count += 1
            if count >= 5:
                break

    logger.info("\n=== 参考: location_source 分布 ===")
    src_counter: Counter = Counter()
    for rec in all_recs:
        src = rec.get("location_source") or ""
        src_counter[src] += 1
    for src, cnt in src_counter.most_common():
        pct = cnt / total * 100
        label = src if src else "(空)"
        logger.info(f"  {label:<10} {cnt:>5}件 ({pct:.1f}%)")


def main() -> None:
    ext_path  = get_result_path(str(_STEP_DIR), OUTPUT_EXTRACTED)
    null_path = get_result_path(str(_STEP_DIR), OUTPUT_NULL)

    for path in [INPUT_RESOURCES, ext_path, null_path]:
        if not Path(path).exists():
            logger.error(f"ファイルが存在しません: {path}")
            sys.exit(1)

    try:
        resource_ids = read_jsonl_as_list(INPUT_RESOURCES)
        extracted    = read_jsonl_as_list(ext_path)
        null_recs    = read_jsonl_as_list(null_path)
    except Exception as e:
        logger.error(f"ファイル読み込みエラー: {e}")
        sys.exit(1)

    total = len(resource_ids)

    ok1 = check_count_consistency(total, len(extracted), len(null_recs))
    ok2 = check_location_values(extracted, null_recs)
    ok3 = check_signature_contamination(extracted)
    ok4 = check_schema_and_message_ids(resource_ids, extracted, null_recs)
    show_distribution(extracted, null_recs)

    if not (ok1 and ok2 and ok3 and ok4):
        logger.error("confirm NG。Pipelineを停止します。")
        sys.exit(1)

    overseas_cnt = sum(1 for r in extracted + null_recs if r.get("location") == "overseas")
    unknown_cnt  = sum(1 for r in extracted + null_recs if r.get("location") == "unknown")
    region_cnt   = len(extracted) + len(null_recs) - overseas_cnt - unknown_cnt

    logger.ok(
        f"confirm OK: 入力={total}件 / 地方確定={region_cnt}件 / "
        f"overseas={overseas_cnt}件 / unknown={unknown_cnt}件 / null=0件"
    )


if __name__ == "__main__":
    main()

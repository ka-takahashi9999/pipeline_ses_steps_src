#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 05-7: 要員メールから商流情報（vendor_flow）をルールベースで抽出

【分類ロジック】
10の位（送信元アドレスで判定）：
  ・@technoverse.co.jp → 1
  ・それ以外           → 2

1の位（メール本文から判定）：
  ・弊社所属・自社社員等 → 0
  ・1社先・BP・パートナー等 → 1
  ・2社先等              → 2
  ・読み取れない場合     → 0（デフォルト）

結果6種：10 / 11 / 12 / 20 / 21 / 22

入力①（送信元）:
  01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl  ← from フィールド
入力②（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力③（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/resources.jsonl

出力①（抽出結果）:
  01_result/extract_resource_vendor_tiers.jsonl
出力②（vendor_flow が有効値以外のもの、本来0件）:
  01_result/99_vendor_tiers_null_extract_resource_vendor_tiers.jsonl
"""

import re
import sys
import time
import unicodedata
from email.utils import parseaddr
from pathlib import Path
from typing import Dict, List, Optional, Tuple

_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "05-7_extract_resource_vendor_tiers"
logger = get_logger(STEP_NAME)

INPUT_MASTER    = str(_PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl")
INPUT_CLEANED   = str(_PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl")
INPUT_RESOURCES = str(_PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "resources.jsonl")
OUTPUT_EXTRACTED = "extract_resource_vendor_tiers.jsonl"
OUTPUT_NULL      = "99_vendor_tiers_null_extract_resource_vendor_tiers.jsonl"

TECHNOVERSE_DOMAIN = "technoverse.co.jp"
VALID_VALUES       = {10, 11, 12, 20, 21, 22}


# ── 正規化 ─────────────────────────────────────────────────
def _n(s: str) -> str:
    """NFKC正規化（全角→半角）"""
    return unicodedata.normalize("NFKC", s or "")


# ── From アドレス抽出 ──────────────────────────────────────
def _extract_email(from_field: str) -> str:
    """
    'Name <addr>' / '<addr>' / 'addr' 形式から
    メールアドレス部分のみを返す（小文字化）。
    """
    if not from_field:
        return ""
    # email.utils.parseaddr で安全に分解
    _, addr = parseaddr(from_field)
    if addr:
        return addr.strip().lower()
    # フォールバック: angle bracket 内を抽出
    m = re.search(r"<([^>]+)>", from_field)
    if m:
        return m.group(1).strip().lower()
    return from_field.strip().lower()


def _is_technoverse(from_field: str) -> bool:
    """送信元が @technoverse.co.jp かどうか判定"""
    addr = _extract_email(from_field)
    return addr.endswith("@" + TECHNOVERSE_DOMAIN)


# ── 1の位: 商流深さパターン ────────────────────────────────

# 深さ2（2社先）を先に評価
RX_DEPTH_2 = re.compile(
    r"2\s*社\s*先|２\s*社\s*先|二\s*社\s*先"
    r"|弊社\s*2\s*社\s*先|弊社\s*２\s*社\s*先"
    r"|CB\s*2\s*社\s*先|2\s*次\s*請\s*け?|二\s*次\s*請\s*け?"
)

# 深さ1（1社先・BP・パートナー）
RX_DEPTH_1 = re.compile(
    r"1\s*社\s*先|１\s*社\s*先|一\s*社\s*先"
    r"|弊社\s*1\s*社\s*先|弊社\s*１\s*社\s*先"
    r"|CB\s*1\s*社\s*先|1\s*社\s*下|一\s*社\s*下"
    r"|1\s*次\s*請\s*け?|一\s*次\s*請\s*け?"
    r"|\bBP\b|BPさん|BP案件"
    r"|懇意\s*BP\s*プロパー|BP\s*プロパー"
    r"|パートナー\s*(?:会社|企業|様)"
    r"|協力\s*(?:会社|パートナー|先)"
    r"|外部\s*パートナー"
)

# 深さ0（弊社所属・自社）
RX_DEPTH_0 = re.compile(
    r"弊社\s*(?:所属|社員|スタッフ|エンジニア|メンバー|人材|要員)"
    r"|自社\s*(?:所属|社員|スタッフ|エンジニア|メンバー)"
    r"|当社\s*(?:所属|社員|スタッフ|エンジニア|メンバー)"
    r"|弊社\s*所属\s*個人"
    r"|弊社\s*(?:直)?\s*個人\s*事業\s*主(?:様)?"
    r"|弊社\s*所属\s*個人\s*事業\s*主(?:様)?"
    r"|弊社\s*(?:正社員|契約社員|アルバイト)"
    r"|自社\s*(?:正社員|契約社員)"
)


def _extract_depth_digit(body: str) -> Tuple[int, Optional[str]]:
    """
    本文から1の位（深さ: 0/1/2）を抽出する。
    Returns: (digit, raw_text)
    """
    if not body:
        return 0, None

    text = _n(body)

    # 2社先を最優先
    m = RX_DEPTH_2.search(text)
    if m:
        return 2, m.group(0)

    # 1社先・BP
    m = RX_DEPTH_1.search(text)
    if m:
        return 1, m.group(0)

    # 弊社所属
    m = RX_DEPTH_0.search(text)
    if m:
        return 0, m.group(0)

    return 0, None


# ── レコード構築 ──────────────────────────────────────────
def build_record(mid: str, from_field: str, body: str) -> Dict:
    tens = 1 if _is_technoverse(from_field) else 2
    ones, raw = _extract_depth_digit(body)
    vendor_flow = tens * 10 + ones
    return {
        "message_id": mid,
        "vendor_flow": vendor_flow,
        "vendor_flow_raw": raw,
    }


# ── メイン ────────────────────────────────────────────────
def main() -> None:
    start = time.time()
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = dirs["result"]

    for path in [INPUT_RESOURCES, INPUT_MASTER, INPUT_CLEANED]:
        if not Path(path).exists():
            logger.error(f"入力ファイルが存在しません: {path}")
            sys.exit(1)

    try:
        resource_ids = [r["message_id"] for r in read_jsonl_as_list(INPUT_RESOURCES)]
        master_map   = read_jsonl_as_dict(INPUT_MASTER)
        cleaned_map  = read_jsonl_as_dict(INPUT_CLEANED)
    except Exception as e:
        write_error_log(str(result_dir), e, "入力ファイル読み込みエラー")
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    extracted: List[Dict] = []
    null_records: List[Dict] = []

    for mid in resource_ids:
        from_field = (master_map.get(mid) or {}).get("from", "")
        body       = (cleaned_map.get(mid) or {}).get("body_text", "")
        rec        = build_record(mid, from_field, body)

        if rec["vendor_flow"] not in VALID_VALUES:
            null_records.append(rec)
        else:
            extracted.append(rec)

        logger.info(
            f"{mid} → vendor_flow={rec['vendor_flow']} from={_extract_email(from_field)} raw={rec['vendor_flow_raw']}",
            message_id=mid,
        )

    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path,  extracted)
    write_jsonl(null_path, null_records)

    elapsed = time.time() - start
    total   = len(resource_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    dist = {}
    for r in extracted:
        dist[r["vendor_flow"]] = dist.get(r["vendor_flow"], 0) + 1
    dist_str = "  ".join(f"{k}:{v}件" for k, v in sorted(dist.items()))
    logger.ok(
        f"Step完了: 入力={total}件 / 有効={len(extracted)}件 / null={len(null_records)}件  [{dist_str}]"
    )


if __name__ == "__main__":
    main()

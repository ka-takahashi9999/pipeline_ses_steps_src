#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 05-3: 要員メールからリモート希望をルールベースで抽出

ルール:
  ① 「フルリモート」「完全リモート」等 → fullremote
  ② 「週N日リモート」「ハイブリッド」「リモート希望」等 → hybrid
  ③ 記載がない場合はデフォルト値 "onsite"（source="default"）
  ④ 抽出できた場合は source="extracted"
  ⑤ null/unknown は出力しない（必ずデフォルト値を設定）

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/resources.jsonl

出力①（全件）:
  01_result/extract_resource_remote.jsonl
出力②（remote_preference が null/unknown または3種類以外、本来0件）:
  01_result/99_remote_null_extract_resource_remote.jsonl
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

STEP_NAME = "05-3_extract_resource_remote"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_RESOURCES = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "resources.jsonl"
)
OUTPUT_EXTRACTED = "extract_resource_remote.jsonl"
OUTPUT_NULL      = "99_remote_null_extract_resource_remote.jsonl"

REMOTE_DEFAULT = "onsite"
VALID_VALUES   = {"onsite", "hybrid", "fullremote"}


# ── 正規化 ─────────────────────────────────────────────────
def _n(s: str) -> str:
    """NFKC正規化（全角→半角、波ダッシュ等を統一）"""
    return unicodedata.normalize("NFKC", s or "")


# ── セグメント抽出用キーワード ─────────────────────────────
_REMOTE_KW_RE = re.compile(
    r"リモート|テレワーク|在\s*宅\s*勤\s*務|在\s*宅|勤\s*務\s*形\s*態|稼\s*働\s*形\s*態"
    r"|希\s*望\s*勤\s*務|勤\s*務\s*希\s*望|希\s*望\s*稼\s*働|ワークスタイル"
    r"|【勤務】|【稼働】|【希望】|【リモート】"
)

# ── フルリモートパターン（先に評価） ──────────────────────
RX_FULLREMOTE = re.compile(
    r"フルリモート"
    r"|完全リモート"
    r"|完全在宅"
    r"|フル在宅"
    r"|完全テレワーク"
    r"|フルテレワーク"
    r"|100\s*%\s*(?:リモート|在宅|テレワーク)"
    r"|(?:リモート|在宅|テレワーク)\s*100\s*%"
    r"|常時リモート"
    r"|常時在宅"
    r"|フルリモ(?!\s*ート以外)"
)

# ── ハイブリッドパターン ──────────────────────────────────
RX_HYBRID = re.compile(
    # 週N日/回リモート・在宅
    r"週\s*\d+\s*日\s*(?:リモート|在宅|テレワーク)"
    r"|週\s*\d+\s*回\s*(?:リモート|在宅|テレワーク)"
    r"|月\s*\d+\s*日\s*(?:リモート|在宅)"
    r"|月\s*\d+\s*回\s*(?:リモート|在宅)"
    r"|(?:リモート|在宅|テレワーク)\s*週\s*\d+"
    r"|(?:リモート|在宅|テレワーク)\s*月\s*\d+"
    # ハイブリッド
    r"|ハイブリッド"
    # 一部・部分リモート
    r"|一部\s*(?:リモート|在宅|テレワーク)"
    r"|部分\s*(?:リモート|在宅)"
    # リモート希望（フルなし）
    r"|(?:リモート|テレワーク|在宅)\s*(?:希望|ワーク希望)"
    r"|希望\s*(?:リモート|テレワーク)"
    r"|在宅\s*希望"
    # 週N以上・週N程度
    r"|週\s*\d+\s*(?:以上|程度|以内)\s*(?:リモート|在宅|テレワーク)"
    r"|(?:リモート|在宅|テレワーク)\s*週\s*\d+\s*(?:以上|程度|以内)"
    # 隔週・不定期リモート
    r"|隔週\s*(?:リモート|在宅)"
    r"|不定期\s*(?:リモート|在宅)"
)


# ── セグメント抽出 ────────────────────────────────────────
def _get_segments(text: str) -> List[str]:
    """リモート関連キーワード周辺のセグメントを返す（重複排除）"""
    segs: List[str] = []
    seen: set = set()
    for m in _REMOTE_KW_RE.finditer(text):
        start = max(0, m.start() - 30)
        end   = min(len(text), m.end() + 120)
        seg   = text[start:end]
        if seg not in seen:
            seen.add(seg)
            segs.append(seg)
    return segs


# ── 抽出関数 ─────────────────────────────────────────────
def rule_extract_remote(body: str) -> Tuple[str, str, Optional[str]]:
    """
    ルールベースでリモート希望を抽出。
    Returns: (remote_preference, source, remote_raw)
      remote_preference: "onsite" | "hybrid" | "fullremote"
      source: "extracted" | "default"
      remote_raw: マッチした文字列 or None
    """
    if not body:
        return REMOTE_DEFAULT, "default", None

    text = _n(body)
    segments = _get_segments(text)

    # セグメントが取れない場合は本文全体をそのまま使う（短い本文向け）
    if not segments:
        if len(text) <= 300:
            segments = [text]
        else:
            return REMOTE_DEFAULT, "default", None

    # fullremote を先に評価（hybrid より優先）
    for seg in segments:
        m = RX_FULLREMOTE.search(seg)
        if m:
            return "fullremote", "extracted", m.group(0)

    for seg in segments:
        m = RX_HYBRID.search(seg)
        if m:
            return "hybrid", "extracted", m.group(0)

    return REMOTE_DEFAULT, "default", None


# ── レコード構築 ──────────────────────────────────────────
def build_record(mid: str, body: str) -> Dict:
    preference, source, raw = rule_extract_remote(body)
    return {
        "message_id": mid,
        "remote_preference": preference,
        "remote_source": source,
        "remote_raw": raw,
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

        # remote_preference が VALID_VALUES 以外なら null 扱い（設計上0件のはず）
        if rec["remote_preference"] not in VALID_VALUES:
            null_records.append(rec)
        else:
            extracted.append(rec)

        logger.info(
            f"{mid} → remote_preference={rec['remote_preference']} "
            f"source={rec['remote_source']} raw={rec['remote_raw']}",
            message_id=mid,
        )

    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path,  extracted)
    write_jsonl(null_path, null_records)

    elapsed = time.time() - start
    total   = len(resource_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    ext_count  = sum(1 for r in extracted if r["remote_source"] == "extracted")
    def_count  = sum(1 for r in extracted if r["remote_source"] == "default")
    logger.ok(
        f"Step完了: 入力={total}件 / 抽出={ext_count}件 / "
        f"デフォルト={def_count}件 / null={len(null_records)}件"
    )


if __name__ == "__main__":
    main()

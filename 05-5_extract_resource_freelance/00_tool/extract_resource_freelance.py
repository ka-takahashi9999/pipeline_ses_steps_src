#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 05-5: 要員メールから雇用形態をルールベースで抽出

ルール:
  ① 「フリーランス」「個人事業主」等の記載 → employment_type="freelance"
  ② 記載がない場合はデフォルト値 "employee"（source="default"）
  ③ 抽出できた場合は source="extracted"
  ④ null/unknown は出力しない（必ずデフォルト値を設定）

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/resources.jsonl

出力①（全件）:
  01_result/extract_resource_freelance.jsonl
出力②（employment_type が null/unknown のもの、本来0件）:
  01_result/99_freelance_null_extract_resource_freelance.jsonl
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

STEP_NAME = "05-5_extract_resource_freelance"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_RESOURCES = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "resources.jsonl"
)
OUTPUT_EXTRACTED = "extract_resource_freelance.jsonl"
OUTPUT_NULL      = "99_freelance_null_extract_resource_freelance.jsonl"

EMPLOYMENT_DEFAULT = "employee"
VALID_VALUES       = {"employee", "freelance"}


# ── 正規化 ─────────────────────────────────────────────────
def _n(s: str) -> str:
    """NFKC正規化（全角→半角、波ダッシュ等を統一）"""
    return unicodedata.normalize("NFKC", s or "")


# ── セグメント抽出用キーワード ─────────────────────────────
# 雇用形態・所属情報が記載される文脈キーワード
_EMPLOYMENT_KW_RE = re.compile(
    r"所\s*属|雇\s*用\s*形\s*態|雇\s*用|契\s*約\s*形\s*態|身\s*分|商\s*流"
    r"|【所属】|【雇用形態】|【契約形態】|【身分】|【商流】"
)

# ── Tier1: 全文スキャン（高精度・文脈不要）────────────────
# フリーランス・個人事業主は出現すれば確実に freelance
RX_FREELANCE_GLOBAL = re.compile(
    r"フリーランス|個人事業主|フリーエンジニア|フリーコンサル"
    r"|独立系\s*エンジニア|独立\s*開業"
)

# ── Tier2: セグメント内スキャン（文脈限定）────────────────
# 所属・雇用形態等のラベル近傍でのみ有効なパターン
RX_FREELANCE_DIRECT = re.compile(
    # 個人（所属が「個人」単独 or「個人/自営業」等）
    r"所属\s*[：:＝=【]*\s*個人(?!\s*情報|\s*的|\s*差)"
    r"|【\s*所属\s*】\s*個人(?!\s*情報|\s*的|\s*差)"
    # BP（Business Partner = 個人 or 小規模）
    r"|所属\s*[：:＝=【]*\s*BP\b"
    r"|【\s*所属\s*】\s*BP\b"
    r"|商流\s*[：:＝=【]*.*BP\b"
    # 業務委託（雇用関係なし）
    r"|雇用形態\s*[：:＝=【]*\s*業務委託"
    r"|契約形態\s*[：:＝=【]*\s*業務委託"
    r"|【\s*(?:雇用形態|契約形態)\s*】\s*業務委託"
    # フリー（雇用形態ラベル近傍）
    r"|雇用形態\s*[：:＝=【]*\s*フリー"
    r"|【\s*雇用形態\s*】\s*フリー"
    # 個人事業主の直接明示
    r"|個人事業主\s*[：:＝=]\s*(?:可|OK)?"
)

RX_EMPLOYEE_CONTEXT = re.compile(
    r"(?:所属|雇用形態|契約形態|身分|商流)\s*[：:＝=【]*\s*"
    r"(?:(?:弊社(?:子会社)?|[\w一-龥ァ-ヶー]+)?(?:1社先|一社先|1社下|一社下)?(?:/|に)?"
    r"(?:グループ会社|グループ)?(?:正社員|契約社員|社員)|(?:弊社)?プロパー)"
    r"|【\s*(?:所\s*属|雇\s*用\s*形\s*態|契\s*約\s*形\s*態|身\s*分|商\s*流)\s*】\s*[：:＝=]?\s*"
    r"(?:(?:弊社(?:子会社)?|[\w一-龥ァ-ヶー]+)?(?:1社先|一社先|1社下|一社下)?(?:/|に)?"
    r"(?:グループ会社|グループ)?(?:正社員|契約社員|社員)|(?:弊社)?プロパー)"
)


# ── セグメント抽出 ────────────────────────────────────────
def _get_segments(text: str) -> List[str]:
    """雇用形態関連キーワード周辺のセグメントを返す（重複排除）"""
    segs: List[str] = []
    seen: set = set()
    for m in _EMPLOYMENT_KW_RE.finditer(text):
        start = max(0, m.start() - 10)
        end   = min(len(text), m.end() + 80)
        seg   = text[start:end]
        if seg not in seen:
            seen.add(seg)
            segs.append(seg)
    return segs


# ── 抽出関数 ─────────────────────────────────────────────
def rule_extract_freelance(body: str) -> Tuple[str, str, Optional[str]]:
    """
    ルールベースで雇用形態を抽出。
    Returns: (employment_type, source, employment_type_raw)
      employment_type: "employee" | "freelance"
      source: "extracted" | "default"
      employment_type_raw: マッチした文字列 or None
    """
    if not body:
        return EMPLOYMENT_DEFAULT, "default", None

    text = _n(body)

    # Tier1.5: 全文スキャン（直接明示を優先）
    m = RX_FREELANCE_DIRECT.search(text)
    if m:
        return "freelance", "extracted", m.group(0)

    # Tier2: セグメント内スキャン（文脈限定）
    segments = _get_segments(text)
    for seg in segments:
        m = RX_FREELANCE_DIRECT.search(seg)
        if m:
            return "freelance", "extracted", m.group(0)

    for seg in segments:
        m = RX_EMPLOYEE_CONTEXT.search(seg)
        if m:
            return "employee", "extracted", m.group(0)

    # Tier1: 全文スキャン（高精度・弱め）
    m = RX_FREELANCE_GLOBAL.search(text)
    if m:
        return "freelance", "extracted", m.group(0)

    return EMPLOYMENT_DEFAULT, "default", None


# ── レコード構築 ──────────────────────────────────────────
def build_record(mid: str, body: str) -> Dict:
    emp_type, source, raw = rule_extract_freelance(body)
    return {
        "message_id": mid,
        "employment_type": emp_type,
        "employment_type_source": source,
        "employment_type_raw": raw,
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

        if rec["employment_type"] not in VALID_VALUES:
            null_records.append(rec)
        else:
            extracted.append(rec)

        logger.info(
            f"{mid} → employment_type={rec['employment_type']} "
            f"source={rec['employment_type_source']} raw={rec['employment_type_raw']}",
            message_id=mid,
        )

    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path,  extracted)
    write_jsonl(null_path, null_records)

    elapsed = time.time() - start
    total   = len(resource_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    ext_count = sum(1 for r in extracted if r["employment_type_source"] == "extracted")
    def_count = sum(1 for r in extracted if r["employment_type_source"] == "default")
    logger.ok(
        f"Step完了: 入力={total}件 / 抽出={ext_count}件 / "
        f"デフォルト={def_count}件 / null={len(null_records)}件"
    )


if __name__ == "__main__":
    main()

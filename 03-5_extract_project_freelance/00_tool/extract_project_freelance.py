#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 03-5: 案件メールから個人事業主制限をルールベースで抽出

ルール:
  ① 「個人事業主不可」「フリーランス不可」等の記載 → freelance_ok=False（source="extracted"）
  ② 記載がない場合 → freelance_ok=True（source="default"）
  ③ null/unknown は出力しない（必ずデフォルト値 True を設定）

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/projects.jsonl

出力①（抽出結果）:
  01_result/extract_project_freelance.jsonl
出力②（異常レコード、本来0件）:
  01_result/99_freelance_null_extract_project_freelance.jsonl
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

STEP_NAME = "03-5_extract_project_freelance"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_PROJECTS = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl"
)
OUTPUT_EXTRACTED = "extract_project_freelance.jsonl"
OUTPUT_NULL      = "99_freelance_null_extract_project_freelance.jsonl"

DEFAULT_FREELANCE_OK = True


# ── 正規化 ────────────────────────────────────────────────
def _n(s: str) -> str:
    """NFKC正規化（全角→半角、波ダッシュ等を統一）"""
    return unicodedata.normalize("NFKC", s or "")


# ── フリーランス不可パターン ──────────────────────────────
_KOJIN_LABEL = r"個人\s*事業\s*主"
_FREELANCE_LABEL = r"(?:フリー\s*ランス|フリー\s*ランサー)"
_SEP = r"\s*[：:；;|｜]\s*"

_FREELANCE_NG_PATTERNS = [
    re.compile(r"【\s*個人\s*事業\s*主\s*】\s*[：:；;|｜]?\s*(?:不可|NG)"),
    re.compile(_KOJIN_LABEL + _SEP + r"(?:不可|NG)(?:\s*\([^)\n]{0,80}\))?"),
    re.compile(r"貴社\s*正社員\s*様?\s*まで"),
    re.compile(r"貴社\s*社員\s*様?\s*まで"),
    re.compile(r"貴社\s*正社員[^\n]{0,30}とさせていただきます"),
    re.compile(r"貴社\s*正社員\s*(?:まで|のみ)"),
    re.compile(r"貴社\s*社員\s*のみ"),
    re.compile(r"貴社\s*社員\s*(?:まで|限定)"),
    re.compile(r"貴社\s*正社員\s*、\s*契約\s*社員\s*まで"),
    re.compile(r"貴社\s*社員\s*もしくは\s*契約\s*社員\s*まで"),
    re.compile(r"貴社\s*正社員\s*もしくは\s*契約\s*社員\s*まで"),
    re.compile(r"【\s*フリー\s*ランス\s*】\s*不可"),
    re.compile(r"フリー\s*ランス\s*(?:不可|NG|は不可|の方は不可|お断り|不採用|はご遠慮|不可能)"),
    re.compile(r"個人\s*事業\s*主\s*(?:不可|NG|は不可|の方は不可|お断り|不採用|はご遠慮|不可能)"),
    re.compile(r"フリー\s*ランサー\s*(?:不可|NG|は不可|お断り|不採用)"),
    re.compile(r"法人\s*(?:のみ|限定|必須|契約のみ|との契約のみ)"),
    re.compile(r"法人\s*格\s*(?:必須|が必要|のある方|を?お持ち)"),
    re.compile(r"法人\s*(?:窓口|経由)\s*(?:必須|のみ|限定)"),
    re.compile(r"SES\s*(?:会社|企業)\s*(?:経由|のみ|限定)"),
    re.compile(r"企業\s*(?:所属|在籍)\s*(?:必須|のみ|限定|が必要)"),
    re.compile(r"会社\s*(?:所属|在籍)\s*(?:必須|のみ|限定|が必要)"),
    re.compile(r"直\s*契約\s*不可"),
    re.compile(r"個人\s*との\s*契約\s*(?:不可|はお断り)"),
    re.compile(r"フリー\s*ランス\s*(?:不\s*可)"),
    re.compile(r"個人\s*事業\s*主\s*(?:不\s*可)"),
]

# ── フリーランス可明示パターン ────────────────────────────
_FREELANCE_OK_PATTERNS = [
    re.compile(r"貴社\s*社員\s*もしくは\s*" + _KOJIN_LABEL + r"\s*まで"),
    re.compile(r"貴社\s*所属\s*" + _KOJIN_LABEL + r"\s*様?\s*まで"),
    re.compile(_KOJIN_LABEL + r"\s*の\s*場合[、,]\s*貴社まで"),
    re.compile(_KOJIN_LABEL + r"\s*の\s*場合[^\n]{0,30}貴社まで"),
    re.compile(r"【\s*個人\s*事業\s*主\s*】\s*[：:；;|｜]?\s*(?:可|OK)"),
    re.compile(_KOJIN_LABEL + _SEP + r"(?:可|OK)(?:\s*\([^)\n]{0,80}\))?"),
    re.compile(_KOJIN_LABEL + r"[^\n]{0,60}(?:場合のみ可|可能な場合のみ|直接契約[^\n]{0,20}可能)"),
    re.compile(_KOJIN_LABEL + r"\s*まで"),
    re.compile(r"【\s*フリー\s*ランス\s*】\s*可"),
    re.compile(r"社員[^\n]{0,20}" + _FREELANCE_LABEL + r"[^\n]{0,20}どちらでも可"),
    re.compile(_FREELANCE_LABEL + r"\s*どちらでも可"),
    re.compile(r"フリー\s*ランス\s*(?:可|OK|歓迎|不問|でも可|の方も可|も可能)"),
    re.compile(r"個人\s*事業\s*主\s*(?:可|OK|歓迎|不問|でも可|の方も可|も可能)"),
    re.compile(r"フリー\s*ランサー\s*(?:可|OK|歓迎|不問)"),
    re.compile(r"フリー\s*ランス\s*(?:での)?(?:参画|応募)\s*(?:可|OK|可能|歓迎)"),
    re.compile(r"個人\s*事業\s*主\s*(?:での)?(?:参画|応募)\s*(?:可|OK|可能|歓迎)"),
    re.compile(r"独立\s*(?:直後|間もない方)\s*(?:も)?\s*(?:可|OK|歓迎)"),
]


def rule_extract_freelance(body: str) -> Tuple[bool, str, Optional[str]]:
    """
    ルールベースでフリーランス可否を抽出する。

    Returns:
        (freelance_ok, source, freelance_raw)
          freelance_ok: True（フリーランス可） or False（フリーランス不可）
          source: "extracted" or "default"
          freelance_raw: マッチした文字列 or None
    """
    if not body:
        return DEFAULT_FREELANCE_OK, "default", None

    text = _n(body)

    # フリーランス不可パターン
    for pat in _FREELANCE_NG_PATTERNS:
        m = pat.search(text)
        if m:
            return False, "extracted", m.group(0)

    # フリーランス可明示パターン
    for pat in _FREELANCE_OK_PATTERNS:
        m = pat.search(text)
        if m:
            return True, "extracted", m.group(0)

    return DEFAULT_FREELANCE_OK, "default", None


# ── レコード構築 ──────────────────────────────────────────
def build_record(mid: str, body: str) -> Dict:
    ok, source, raw = rule_extract_freelance(body)
    return {
        "message_id": mid,
        "freelance_ok": ok,
        "freelance_source": source,
        "freelance_raw": raw,
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

        # freelance_ok が bool 以外（None等）は分離
        if not isinstance(rec["freelance_ok"], bool):
            null_records.append(rec)
        else:
            extracted.append(rec)

        logger.info(
            f"{mid} → ok={rec['freelance_ok']} "
            f"source={rec['freelance_source']} raw={rec['freelance_raw']}",
            message_id=mid,
        )

    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path,  extracted)
    write_jsonl(null_path, null_records)

    elapsed = time.time() - start
    total   = len(project_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    ok_count  = sum(1 for r in extracted if r["freelance_ok"] is True)
    ng_count  = sum(1 for r in extracted if r["freelance_ok"] is False)
    ext_count = sum(1 for r in extracted if r["freelance_source"] == "extracted")
    def_count = sum(1 for r in extracted if r["freelance_source"] == "default")

    logger.ok(
        f"Step完了: 入力={total}件 / ok={ok_count}件 / ng={ng_count}件 "
        f"(extracted={ext_count}件 default={def_count}件) / null={len(null_records)}件"
    )


if __name__ == "__main__":
    main()

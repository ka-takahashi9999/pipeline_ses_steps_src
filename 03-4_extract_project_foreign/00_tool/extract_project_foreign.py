#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 03-4: 案件メールから外国籍制限をルールベースで抽出

ルール:
  ① 「外国籍不可」「日本国籍のみ」等の記載 → foreign_nationality_ok=False（source="extracted"）
  ② 記載がない場合 → foreign_nationality_ok=True（source="default"）
  ③ null/unknown は出力しない（必ずデフォルト値 True を設定）

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/projects.jsonl

出力①（抽出結果）:
  01_result/extract_project_foreign.jsonl
出力②（異常レコード、本来0件）:
  01_result/99_foreign_null_extract_project_foreign.jsonl
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

STEP_NAME = "03-4_extract_project_foreign"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_PROJECTS = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl"
)
OUTPUT_EXTRACTED = "extract_project_foreign.jsonl"
OUTPUT_NULL      = "99_foreign_null_extract_project_foreign.jsonl"

DEFAULT_FOREIGN_OK = True


# ── 正規化 ────────────────────────────────────────────────
def _n(s: str) -> str:
    """NFKC正規化（全角→半角、波ダッシュ等を統一）"""
    return unicodedata.normalize("NFKC", s or "")


# ── 外国籍不可パターン ────────────────────────────────────
# 外国籍が不可であることを示す表現
_FOREIGN_LABEL = r"(?:【\s*外\s*国\s*籍\s*】|外国\s*籍)"
_FOREIGN_SEP = r"(?:\s|[:;|_]|】)+"

_FOREIGN_NG_PATTERNS = [
    re.compile(_FOREIGN_LABEL + _FOREIGN_SEP + r"(?:基本)?(?:不可|NG)(?:\s*\([^)\n]{0,60}\))?"),
    re.compile(_FOREIGN_LABEL + _FOREIGN_SEP + r"(?![^\n]{0,80}(?:可|OK|歓迎|不問|要相談|検討可))[^\n]{0,40}日本語[^\n]{0,30}(?:必要|必須)"),
    re.compile(r"外国\s*籍[^\n]{0,20}の\s*方\s*NG(?:とさせていただきます)?"),
    re.compile(r"外国\s*籍(?:\s*[（(][^)）\n]{0,20}[)）])?\s*(?:・\s*個人\s*事業\s*主)?\s*(?:不可|NG)"),
    re.compile(r"外国\s*籍(?:\s*[（(][^)）\n]{0,20}[)）])?\s*(?:・\s*個人\s*事業\s*主)?\s*[：:;；|｜]?\s*NG"),
    re.compile(r"外国\s*籍[^\n]{0,80}ご提案はご遠慮ください"),
    re.compile(r"外国\s*籍\s*(?:不可|NG|不可能|お断り|の方はご遠慮|は不可|不採用)"),
    re.compile(r"外国\s*籍\s*の\s*方\s*は\s*NG"),
    re.compile(r"日本\s*国\s*籍\s*(?:のみ|限定|必須|のかた|の方|のみ可|に限る)"),
    re.compile(r"日本\s*国\s*籍\s*の[^\n]{0,30}のみ"),
    re.compile(r"日本\s*国\s*籍[^\n]{0,30}限定"),
    re.compile(r"国\s*籍\s*[：:]\s*日本"),
    re.compile(r"日本\s*人\s*(?:募集|希望|限定|のみ|のかた|の方|に限る)"),
    re.compile(r"日本\s*国\s*籍\s*(?:を)?(?:有する|お持ち)"),
    re.compile(r"(?:永住|在留)\s*資格\s*(?:必須|が必要|を?お持ち|のある方)"),
    re.compile(r"ビザ\s*(?:スポンサー)?\s*(?:不可|なし|提供\s*不可)"),
    re.compile(r"visa\s*(?:sponsor)?\s*(?:not\s*available|not\s*provided|unsupported)", re.IGNORECASE),
    re.compile(r"外国\s*籍\s*の\s*方\s*はご?\s*応募\s*(?:不可|いただけません|できません)"),
    re.compile(r"日本\s*国\s*籍\s*(?:を)?(?:有する|持つ)\s*方"),
    re.compile(r"国\s*籍\s*要件?\s*[：:]\s*日本\s*国\s*籍"),
    re.compile(r"(?:(?<=\n)|^)[^\n]*制\s*限\s*[：:][^\n]*外国\s*籍[^\n]*"),
    re.compile(r"セキュリティ\s*クリアランス"),
]

# ── 外国籍OK明示パターン ──────────────────────────────────
# 外国籍が明示的にOKであることを示す表現（デフォルトと同じtrue、source=extracted）
_FOREIGN_OK_PATTERNS = [
    re.compile(_FOREIGN_LABEL + _FOREIGN_SEP + r"(?:可|OK|歓迎|不問|要相談|検討可)(?:[^\n]{0,80})?"),
    re.compile(_FOREIGN_LABEL + _FOREIGN_SEP + r"[^\n]{0,40}(?:ネイティブ|ビジネス)レベル[^\n]{0,20}日本語[^\n]{0,20}なら可"),
    re.compile(r"外国\s*籍[^\n]{0,20}(?:応相談|検討可)(?:[^\n]{0,80})?"),
    re.compile(r"外国\s*籍[^\n]{0,40}(?:ネイティブ|ビジネス)レベル[^\n]{0,20}日本語(?:力)?[^\n]{0,20}あればOK"),
    re.compile(r"外国\s*籍\s*(?:可|OK|歓迎|不問|問わず|問いません)"),
    re.compile(r"国\s*籍\s*(?:不問|問わず|問いません|を問わない)"),
    re.compile(r"外国\s*籍\s*の\s*方\s*(?:も)?(?:歓迎|応募可|OK)"),
    re.compile(r"多\s*国\s*籍\s*(?:歓迎|OK|可)"),
]


def rule_extract_foreign(body: str) -> Tuple[bool, str, Optional[str]]:
    """
    ルールベースで外国籍可否を抽出する。

    Returns:
        (foreign_nationality_ok, source, foreign_nationality_raw)
          foreign_nationality_ok: True（外国籍可） or False（外国籍不可）
          source: "extracted" or "default"
          foreign_nationality_raw: マッチした文字列 or None
    """
    if not body:
        return DEFAULT_FOREIGN_OK, "default", None

    text = _n(body)

    # 外国籍不可パターン
    for pat in _FOREIGN_NG_PATTERNS:
        m = pat.search(text)
        if m:
            return False, "extracted", m.group(0)

    # 外国籍OK明示パターン（明示的にOKと書いてある場合もextractedにする）
    for pat in _FOREIGN_OK_PATTERNS:
        m = pat.search(text)
        if m:
            return True, "extracted", m.group(0)

    return DEFAULT_FOREIGN_OK, "default", None


# ── レコード構築 ──────────────────────────────────────────
def build_record(mid: str, body: str) -> Dict:
    ok, source, raw = rule_extract_foreign(body)
    return {
        "message_id": mid,
        "foreign_nationality_ok": ok,
        "foreign_nationality_source": source,
        "foreign_nationality_raw": raw,
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

        # foreign_nationality_ok が bool 以外（None等）は分離
        if not isinstance(rec["foreign_nationality_ok"], bool):
            null_records.append(rec)
        else:
            extracted.append(rec)

        logger.info(
            f"{mid} → ok={rec['foreign_nationality_ok']} "
            f"source={rec['foreign_nationality_source']} raw={rec['foreign_nationality_raw']}",
            message_id=mid,
        )

    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path,  extracted)
    write_jsonl(null_path, null_records)

    elapsed = time.time() - start
    total   = len(project_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    ok_count    = sum(1 for r in extracted if r["foreign_nationality_ok"] is True)
    ng_count    = sum(1 for r in extracted if r["foreign_nationality_ok"] is False)
    ext_count   = sum(1 for r in extracted if r["foreign_nationality_source"] == "extracted")
    def_count   = sum(1 for r in extracted if r["foreign_nationality_source"] == "default")

    logger.ok(
        f"Step完了: 入力={total}件 / ok={ok_count}件 / ng={ng_count}件 "
        f"(extracted={ext_count}件 default={def_count}件) / null={len(null_records)}件"
    )


if __name__ == "__main__":
    main()

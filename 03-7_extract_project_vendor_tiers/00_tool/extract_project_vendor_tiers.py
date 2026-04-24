#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 03-7: 案件メールから商流制限をルールベースで抽出

商流レベル定義:
  0 = 記載なし（デフォルト）または制限明示なし
  1 = 貴社まで / 貴社所属まで / 貴社正社員 等（直接所属エンジニアのみ）
  2 = 貴社1社先まで（1段階のSES委託先まで）
  3 = 貴社2社先まで（2段階のSES委託先まで）

抽出戦略:
  ① 「商流:」「商流制限:」等のラベル行を優先してパース
  ② ラベル行がない場合は本文全体をフォールバック検索
  ③ 複数マッチは最も制限の厳しいもの（数値が大きい = 制限が多い）を優先
  ④ 記載なし → level=0, source="default", delegation_limit=0

注意:
  ・御社 → 貴社、迄 → まで に正規化してから判定
  ・参照実装はレベルがずれているため使用しない

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/projects.jsonl

出力①（抽出結果）:
  01_result/extract_project_vendor_tiers.jsonl
出力②（異常レコード、本来0件）:
  01_result/99_vendor_tiers_null_extract_project_vendor_tiers.jsonl
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

STEP_NAME = "03-7_extract_project_vendor_tiers"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_PROJECTS = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl"
)
OUTPUT_EXTRACTED = "extract_project_vendor_tiers.jsonl"
OUTPUT_NULL      = "99_vendor_tiers_null_extract_project_vendor_tiers.jsonl"

DEFAULT_LEVEL = 0
VALID_LEVELS  = {0, 1, 2, 3}


# ── 正規化 ────────────────────────────────────────────────
def _normalize(s: str, preserve_newlines: bool = False) -> str:
    """NFKC正規化 + 御社→貴社 + 迄→まで + 空白統一

    preserve_newlines=True の場合は改行を保持する（ラベル行解析用）。
    """
    s = unicodedata.normalize("NFKC", s or "")
    s = s.replace("御社", "貴社").replace("迄", "まで").replace("元請け", "元請")
    if preserve_newlines:
        # 行内の連続空白のみ統一（改行は保持）
        s = "\n".join(re.sub(r"[ \t]+", " ", line).strip() for line in s.splitlines())
    else:
        s = re.sub(r"\s+", " ", s).strip()
    return s


# ── 商流ラベル行パターン ──────────────────────────────────
_LABEL_LINE_RE = re.compile(
    r"^(?:[■●・\-＊*]\s*)?(?:商流制限|商流)\s*[_＿]*\s*[：:]\s*(.+)$"
)

# ── 商流制限なし（level=0 明示、source=extracted） ─────────
_NO_RESTRICTION_RE = re.compile(
    r"商流\s*(?:制限)?\s*(?:御座いません|ございません|制限なし|なし|無し|不問|はありません|ありません)|"
    r"商流\s*制限\s*(?:なし|無し|不問)|"
    r"商流\s*ありません"
)

# ── level別パターン（高レベル優先で評価） ────────────────────

# level=3: 貴社2社先まで
_LEVEL3_PATTERNS = [
    re.compile(r"貴社\s*2\s*社\s*先\s*(?:所属)?まで"),
    re.compile(r"貴社\s*二\s*社\s*先\s*(?:所属)?まで"),
    re.compile(r"貴社\s*(?:から)?\s*2\s*社\s*先\s*まで"),
    re.compile(r"貴社\s*2\s*社\s*下\s*まで"),
    re.compile(r"2\s*社\s*(?:先|下)\s*まで"),
]

# level=2: 貴社1社先まで
_LEVEL2_PATTERNS = [
    re.compile(r"貴社\s*1\s*社\s*先\s*(?:所属|要員)?まで"),
    re.compile(r"貴社\s*一\s*社\s*先\s*(?:所属|要員)?まで"),
    re.compile(r"貴社\s*1\s*社\s*下\s*まで"),
    re.compile(r"貴社\s*一\s*社\s*下\s*まで"),
    re.compile(r"(?:弊社|当社)\s*より\s*1\s*社\s*先\s*(?:正\s*社\s*員|社員)?まで"),
    re.compile(r"1\s*社\s*先\s*(?:正\s*社\s*員|社員)?まで"),
    re.compile(r"1\s*社\s*下\s*まで"),
    re.compile(r"貴社\s*(?:から)?\s*1\s*社\s*まで"),
    re.compile(r"貴社\s*(?:から)?\s*一\s*社\s*まで"),
    re.compile(r"一\s*社\s*先\s*まで"),
    re.compile(r"1\s*社\s*先\s*まで"),
    # 「2社下以降は支援費」= 1社下までは通常商流として扱える、という推定で level=2
    re.compile(r"(?:弊社|当社)\s*から\s*2\s*社\s*下\s*以降\s*.*支援費"),
    re.compile(r"(?:弊社|当社)\s*より\s*2\s*社\s*先\s*以降\s*.*支援費"),
]

# level=1: 貴社まで（直接所属のみ）
_LEVEL1_PATTERNS = [
    re.compile(r"貴社\s*まで"),
    re.compile(r"貴社\s*所属\s*(?:要員\s*)?(?:様\s*)?まで"),
    re.compile(r"貴社\s*(?:正\s*)?社\s*員\s*(?:まで|のみ|様\s*まで)"),
    re.compile(r"貴社\s*要員\s*(?:様\s*)?まで"),
    re.compile(r"貴社\s*(?:社\s*員|正\s*社\s*員)\s*(?:または|もしくは)\s*個人\s*事業\s*主\s*まで"),
    re.compile(r"貴社\s*所属\s*個人\s*事業\s*主(?:様)?まで"),
    re.compile(r"貴社\s*直\s*接\s*(?:所属|雇用)"),
    re.compile(r"自社\s*社\s*員\s*のみ"),
    re.compile(r"直\s*(?:接)?\s*雇\s*用\s*のみ"),
]

_LEVEL_PATTERNS: List[Tuple[int, List[re.Pattern]]] = [
    (3, _LEVEL3_PATTERNS),
    (2, _LEVEL2_PATTERNS),
    (1, _LEVEL1_PATTERNS),
]


def _classify(text: str) -> Optional[Tuple[int, str]]:
    """
    テキストから商流レベルとマッチ文字列を返す。
    マッチなし → None
    """
    # 制限なし明示
    m = _NO_RESTRICTION_RE.search(text)
    if m:
        return 0, m.group(0)

    # level 3 → 2 → 1 の順に評価（高レベル優先）
    for level, patterns in _LEVEL_PATTERNS:
        for pat in patterns:
            m = pat.search(text)
            if m:
                return level, m.group(0)

    return None


def _delegation_limit_from_level(level: int) -> int:
    return level


def rule_extract_vendor_tiers(body: str) -> Tuple[int, str, Optional[str], int]:
    """
    ルールベースで商流レベルを抽出する。

    Returns:
        (commercial_flow_level, source, commercial_flow_raw, commercial_flow_delegation_limit)
    """
    if not body:
        return DEFAULT_LEVEL, "default", None, _delegation_limit_from_level(DEFAULT_LEVEL)

    # ラベル行解析用（改行保持）と全文検索用（改行なし）を分けて正規化
    norm_body_lines = _normalize(body, preserve_newlines=True)
    norm_body_flat  = _normalize(body, preserve_newlines=False)

    # ── ①ラベル行から抽出 ───────────────────────────────
    label_candidates: List[Tuple[int, str]] = []
    for line in norm_body_lines.splitlines():
        m = _LABEL_LINE_RE.match(line.strip())
        if m:
            value = m.group(1).strip()
            result = _classify(value)
            if result is not None:
                label_candidates.append(result)

    if label_candidates:
        # 最も制限の厳しいもの（levelが大きい = より多くの商流段数を許容 = より制限あり）を採用
        best = max(label_candidates, key=lambda x: x[0])
        return best[0], "extracted", best[1], _delegation_limit_from_level(best[0])

    # ── ②本文全体をフォールバック検索 ──────────────────────
    result = _classify(norm_body_flat)
    if result is not None:
        level, raw = result
        # level=0 明示（制限なし表現にマッチ）
        return level, "extracted", raw, _delegation_limit_from_level(level)

    return DEFAULT_LEVEL, "default", None, _delegation_limit_from_level(DEFAULT_LEVEL)


# ── レコード構築 ──────────────────────────────────────────
def build_record(mid: str, body: str) -> Dict:
    level, source, raw, delegation_limit = rule_extract_vendor_tiers(body)
    return {
        "message_id": mid,
        "commercial_flow_level": level,
        "commercial_flow_raw": raw,
        "commercial_flow_delegation_limit": delegation_limit,
        "commercial_flow_source": source,
    }


def _is_valid(rec: Dict) -> bool:
    return rec.get("commercial_flow_level") in VALID_LEVELS


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
            f"{mid} → level={rec['commercial_flow_level']} "
            f"source={rec['commercial_flow_source']} raw={rec['commercial_flow_raw']}",
            message_id=mid,
        )

    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path,  extracted)
    write_jsonl(null_path, null_records)

    elapsed = time.time() - start
    total   = len(project_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    for lv in [0, 1, 2, 3]:
        cnt = sum(1 for r in extracted if r["commercial_flow_level"] == lv)
        logger.info(f"  level={lv}: {cnt}件")

    ext_count = sum(1 for r in extracted if r["commercial_flow_source"] == "extracted")
    def_count = sum(1 for r in extracted if r["commercial_flow_source"] == "default")
    logger.ok(
        f"Step完了: 入力={total}件 / extracted={ext_count}件 / "
        f"default={def_count}件 / null={len(null_records)}件"
    )


if __name__ == "__main__":
    main()

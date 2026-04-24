#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 05-4: 要員メールから国籍をルールベースで抽出

ルール:
  ① 「外国籍」「中国籍」「国籍：中国」等の記載 → nationality="foreign"
  ② 記載がない場合はデフォルト値 "japanese"（source="default"）
  ③ 抽出できた場合は source="extracted"
  ④ null/unknown は出力しない（必ずデフォルト値を設定）

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/resources.jsonl

出力①（全件）:
  01_result/extract_resource_foreign.jsonl
出力②（nationality が null/unknown のもの、本来0件）:
  01_result/99_foreign_null_extract_resource_foreign.jsonl
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

STEP_NAME = "05-4_extract_resource_foreign"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_RESOURCES = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "resources.jsonl"
)
OUTPUT_EXTRACTED = "extract_resource_foreign.jsonl"
OUTPUT_NULL      = "99_foreign_null_extract_resource_foreign.jsonl"

NATIONALITY_DEFAULT = "japanese"
VALID_VALUES        = {"japanese", "foreign"}


# ── 正規化 ─────────────────────────────────────────────────
def _n(s: str) -> str:
    """NFKC正規化（全角→半角、波ダッシュ等を統一）"""
    return unicodedata.normalize("NFKC", s or "")


# ── セグメント抽出用キーワード ─────────────────────────────
_FOREIGN_KW_RE = re.compile(
    r"国\s*籍|外\s*国\s*人|外\s*国\s*籍|日\s*本\s*語\s*(?:レベル|能力|スキル|力)"
    r"|JLPT|N[12]\s*保持|N[12]\s*取得|N[12]\s*合格"
    r"|【\s*国\s*籍\s*】|【\s*外\s*国\s*籍\s*】"
)

# ── 外国籍パターン（NFKC正規化済みテキストに適用）─────────

# パターン①: 外国籍・外国人の明示
RX_EXPLICIT_FOREIGN = re.compile(r"外国籍|外国人")

# パターン②: 国名リスト
_FOREIGN_COUNTRIES = (
    "中国|韓国|朝鮮|台湾|ベトナム|フィリピン|インド|バングラデシュ|ミャンマー"
    "|インドネシア|タイ|スリランカ|ネパール|パキスタン|モンゴル|ロシア"
    "|アメリカ|イギリス|フランス|ドイツ|イタリア|スペイン|ブラジル|ペルー"
    "|カナダ|オーストラリア|ニュージーランド|マレーシア|シンガポール"
    "|カンボジア|ラオス|ウズベキスタン|カザフスタン|エジプト|ナイジェリア"
)

_JAPANESE_MARKERS = r"日本|にほん|ニホン|Japan|japan"

# パターン③: 国籍フィールド
RX_NATIONALITY_FIELD = re.compile(
    r"(?:【\s*国\s*籍\s*】|国\s*籍\s*[：:])\s*([^\n]{1,40})"
)

# パターン④: 特定外国の〇〇籍・〇〇国籍・〇〇人・〇〇出身
RX_COUNTRY_SUFFIX = re.compile(
    r"(?:" + _FOREIGN_COUNTRIES + r")\s*(?:国籍|籍|人|出身)"
)

# パターン⑤: 日本明記
RX_JAPANESE_EXPLICIT = re.compile(
    r"(?:【\s*国\s*籍\s*】|国\s*籍\s*[：:])\s*(?:" + _JAPANESE_MARKERS + r")(?:[\s(（/／].*)?"
    r"|日本籍|日本国籍|日本人"
)

# パターン⑥: 日本語レベル記載（外国籍の強いシグナル）
# 「日本語レベル：N1」「日本語能力：ビジネスレベル」等
RX_JAPANESE_LEVEL = re.compile(
    r"日本語\s*(?:レベル|能力|スキル|力)\s*[：:＝=]\s*[^\n]{1,30}"
    r"|JLPT\s*N[12]"
    r"|N[12]\s*(?:保持|取得|合格)"
)

RX_KEYWORD_ONLY_UNRESOLVED = re.compile(
    r"(?:【\s*国\s*籍\s*】|国\s*籍\s*[：:])\s*(?:$|[-‐ーｰ/／()（）※*＊\s]+$)",
    re.MULTILINE
)

RX_NAME_ATTRIBUTE_BLOCK = re.compile(
    r"(?:^|\n)\s*[・●■□◆◇\-\*＊]?\s*(?:名前|氏名)\s*[：:\s]\s*[^\n()（）]{0,80}[（(]([^)）\n]{1,120})[)）]"
)


# ── セグメント抽出 ────────────────────────────────────────
def _get_segments(text: str) -> List[str]:
    """国籍関連キーワード周辺のセグメントを返す（重複排除）"""
    segs: List[str] = []
    seen: set = set()
    for m in _FOREIGN_KW_RE.finditer(text):
        start = max(0, m.start() - 20)
        end   = min(len(text), m.end() + 100)
        seg   = text[start:end]
        if seg not in seen:
            seen.add(seg)
            segs.append(seg)
    return segs


def _classify_nationality_value(value: str) -> Optional[str]:
    """国籍フィールド値から japanese/foreign を判定。曖昧な場合は None。"""
    if not value:
        return None
    cleaned = _n(value).strip()
    cleaned = re.split(r"[※\n]", cleaned)[0].strip()
    if not cleaned:
        return None
    if re.match(r"^(?:" + _JAPANESE_MARKERS + r")(?:[\s(（/／].*)?$", cleaned):
        return "japanese"
    if re.match(r"^(?:" + _FOREIGN_COUNTRIES + r")(?:[\s(（/／].*)?$", cleaned):
        return "foreign"
    return None


def _trim_field_raw(raw: str) -> str:
    return re.split(r"※", _n(raw))[0].strip()


def _find_keyword_only_unresolved(text: str) -> Optional[str]:
    m = RX_KEYWORD_ONLY_UNRESOLVED.search(text)
    return m.group(0) if m else None


def _extract_name_line_nationality(text: str) -> Optional[Tuple[str, str]]:
    """名前/氏名行の括弧内属性列挙から国籍を抽出する。"""
    for m in RX_NAME_ATTRIBUTE_BLOCK.finditer(text):
        attrs = _n(m.group(1)).strip()
        if not attrs:
            continue
        tokens = [tok.strip() for tok in re.split(r"\s*(?:/|／|・|,|，)\s*", attrs) if tok.strip()]
        for tok in tokens:
            nationality = _classify_nationality_value(tok)
            if nationality:
                return nationality, tok

        m_jp = re.search(r"(?:" + _JAPANESE_MARKERS + r")", attrs)
        if m_jp:
            return "japanese", m_jp.group(0)

        m_foreign = re.search(r"(?:" + _FOREIGN_COUNTRIES + r")", attrs)
        if m_foreign:
            return "foreign", m_foreign.group(0)

    return None


# ── 抽出関数 ─────────────────────────────────────────────
def rule_extract_foreign(body: str) -> Tuple[str, str, Optional[str]]:
    """
    ルールベースで国籍を抽出。
    Returns: (nationality, source, nationality_raw)
      nationality: "japanese" | "foreign"
      source: "extracted" | "default"
      nationality_raw: マッチした文字列 or None
    """
    if not body:
        return NATIONALITY_DEFAULT, "default", None

    text = _n(body)

    # ① 明示的な外国籍・外国人（全文スキャン、高精度）
    m = RX_EXPLICIT_FOREIGN.search(text)
    if m:
        return "foreign", "extracted", m.group(0)

    # ② 国籍フィールド値から japanese/foreign を判定
    m = RX_NATIONALITY_FIELD.search(text)
    if m:
        nationality = _classify_nationality_value(m.group(1))
        if nationality:
            return nationality, "extracted", _trim_field_raw(m.group(0))

    # ③ 特定外国の〇〇籍・〇〇国籍・〇〇人・〇〇出身
    m = RX_COUNTRY_SUFFIX.search(text)
    if m:
        return "foreign", "extracted", m.group(0)

    # ④ 日本明記
    m = RX_JAPANESE_EXPLICIT.search(text)
    if m:
        return "japanese", "extracted", m.group(0)

    # ⑤ 名前/氏名行の括弧内属性列挙
    name_line_hit = _extract_name_line_nationality(text)
    if name_line_hit:
        nationality, raw = name_line_hit
        return nationality, "extracted", raw

    # ⑥ 日本語レベル記載（セグメント内のみで評価）
    segments = _get_segments(text)
    for seg in segments:
        m = RX_JAPANESE_LEVEL.search(seg)
        if m:
            return "foreign", "extracted", m.group(0)

    return NATIONALITY_DEFAULT, "default", None


# ── レコード構築 ──────────────────────────────────────────
def build_record(mid: str, body: str) -> Dict:
    nationality, source, raw = rule_extract_foreign(body)
    return {
        "message_id": mid,
        "nationality": nationality,
        "nationality_source": source,
        "nationality_raw": raw,
    }


def _excerpt(text: str, width: int = 120) -> str:
    one_line = re.sub(r"\s+", " ", _n(text)).strip()
    return one_line[:width]


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

        if rec["nationality"] not in VALID_VALUES:
            null_records.append(rec)
        else:
            extracted.append(rec)

        if rec["nationality_source"] == "default":
            unresolved = _find_keyword_only_unresolved(_n(body))
            if unresolved:
                logger.info(
                    f"{mid} → reason=keyword_only_unresolved excerpt={_excerpt(unresolved)}",
                    message_id=mid,
                )

        logger.info(
            f"{mid} → nationality={rec['nationality']} "
            f"source={rec['nationality_source']} raw={rec['nationality_raw']}",
            message_id=mid,
        )

    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path,  extracted)
    write_jsonl(null_path, null_records)

    elapsed = time.time() - start
    total   = len(resource_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    ext_count = sum(1 for r in extracted if r["nationality_source"] == "extracted")
    def_count = sum(1 for r in extracted if r["nationality_source"] == "default")
    logger.ok(
        f"Step完了: 入力={total}件 / 抽出={ext_count}件 / "
        f"デフォルト={def_count}件 / null={len(null_records)}件"
    )


if __name__ == "__main__":
    main()

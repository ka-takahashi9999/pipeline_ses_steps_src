#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 03-3: 案件メールからリモート勤務条件をルールベースで抽出

ルール:
  ① 「フルリモート」「完全リモート」等 → fullremote
  ② 「週2リモート」「ハイブリッド」「リモート可」等 → hybrid
  ③ 記載がない場合 → onsite（source="default"）
  ④ 抽出できた場合は source="extracted"
  ⑤ null/unknown は出力しない（必ずデフォルト値 onsite を設定）

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/projects.jsonl

出力①（抽出結果）:
  01_result/extract_project_remote.jsonl
出力②（異常レコード、本来0件）:
  01_result/99_remote_null_extract_project_remote.jsonl
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

STEP_NAME = "03-3_extract_project_remote"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_PROJECTS = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl"
)
OUTPUT_EXTRACTED = "extract_project_remote.jsonl"
OUTPUT_NULL      = "99_remote_null_extract_project_remote.jsonl"

VALID_REMOTE_TYPES = {"onsite", "hybrid", "fullremote"}
DEFAULT_REMOTE_TYPE = "onsite"


# ── 正規化 ────────────────────────────────────────────────
def _n(s: str) -> str:
    """NFKC正規化（全角→半角、波ダッシュ等を統一）"""
    return unicodedata.normalize("NFKC", s or "")


# ── fullremote パターン ──────────────────────────────────
_FULLREMOTE_PATTERNS = [
    re.compile(r"フル\s*リモート"),
    re.compile(r"完全\s*リモート"),
    re.compile(r"100\s*%\s*リモート"),
    re.compile(r"完全\s*在宅"),
    re.compile(r"フル\s*在宅"),
    re.compile(r"全日\s*リモート"),
    re.compile(r"全\s*日程?\s*在宅"),
    re.compile(r"リモート\s*のみ"),
    re.compile(r"在宅\s*のみ"),
    re.compile(r"週\s*5\s*[日回]?\s*(?:リモート|在宅)"),
    re.compile(r"週\s*[5六七]\s*[日回]?\s*(?:リモート|在宅)"),
    re.compile(r"リモートワーク\s*100"),
    # 「フルリモ」（ート省略形）: フルリモ可 / フルリモ！ / フルリモ相談可 等
    # ※ フルリモ不可 は上記 _ONSITE_PATTERNS で先行評価済み
    re.compile(r"フル\s*リモ"),
    # 「フルテレワーク」「基本フルテレワーク」
    re.compile(r"フル\s*テレワーク"),
]

# ── 週N日リモート抽出（hybrid判定＋日数取得） ────────────
_WEEKLY_REMOTE_RE = re.compile(
    r"週\s*([1-9一二三四])\s*[日回]?\s*(?:程度|以上|くらい|ほど)?\s*の?\s*(?:リモート|在宅|テレワーク)"
)

_WEEKLY_REMOTE_KANJI: Dict[str, int] = {
    "一": 1, "二": 2, "三": 3, "四": 4,
}

# ── hybrid パターン（週Nリモート以外） ──────────────────
_HYBRID_PATTERNS = [
    re.compile(r"ハイブリッド"),
    re.compile(r"一部\s*リモート"),
    re.compile(r"部分\s*リモート"),
    re.compile(r"一部\s*在宅"),
    re.compile(r"部分\s*在宅"),
    re.compile(r"リモート\s*[可可能対応]"),
    re.compile(r"リモートワーク\s*[可可能対応]"),
    re.compile(r"在宅\s*[可可能対応]"),
    re.compile(r"テレワーク\s*[可可能対応]"),
    re.compile(r"リモート\s*相談"),
    re.compile(r"在宅\s*相談"),
    re.compile(r"一部\s*テレワーク"),
    re.compile(r"月\s*[1-9]\s*[日回]\s*(?:リモート|在宅|テレワーク)"),
    re.compile(r"月\s*[一二三四]\s*[日回]\s*(?:リモート|在宅|テレワーク)"),
    re.compile(r"(?:出社|常駐)\s*(?:が|は|も)\s*必要.*(?:リモート|在宅)"),
    re.compile(r"(?:リモート|在宅).*(?:出社|常駐)\s*(?:が|は|も)\s*必要"),
    re.compile(r"リモート\s*(?:あり|有)"),
    re.compile(r"在宅\s*(?:あり|有)"),
    re.compile(r"テレワーク\s*(?:あり|有|可)"),
    # ── 補強パターン ──────────────────────────────────────
    # 「基本リモート」系: 基本リモート / 基本リモート勤務 / ・出社：基本リモート 等
    re.compile(r"基本\s*リモート"),
    # 「原則リモート」: 原則リモート(週1~2時間程度出社の可能性あり) 等
    re.compile(r"原則\s*リモート"),
    # 「リモート(ワーク/勤務)併用」: リモート併用 / リモートワーク併用 / リモート勤務併用 等
    re.compile(r"リモート\s*(?:ワーク|勤務)?\s*併用"),
    # 「テレワーク(勤務)併用」: テレワーク併用 / テレワーク併用可 等
    re.compile(r"テレワーク\s*(?:勤務\s*)?併用"),
    # 「在宅(勤務)併用」
    re.compile(r"在宅\s*(?:勤務\s*)?併用"),
    # 「オンサイト+リモート」: オンサイト＋リモート勤務併用 等
    re.compile(r"オンサイト.*リモート"),
    # 「リモートも設ける」: 神田（リモートも設ける予定だが頻度は不明）等
    re.compile(r"リモート.*設ける"),
    # 「基本(は)テレワーク」「テレワークメイン」「在宅メイン」「リモートメイン」
    re.compile(r"基本\s*(?:は\s*)?テレワーク"),
    re.compile(r"テレワーク\s*メイン"),
    re.compile(r"在宅\s*メイン"),
    re.compile(r"リモート\s*メイン"),
    # 「他リモート」: 週N出社、他リモート 等の文脈
    re.compile(r"他\s*リモート"),
    # 「場所/作業場所/勤務地：...リモート/テレワーク/在宅」（同一行、ラベル付き場所フィールド）
    # ※ \s* ではなく [ \t]* を使い、改行をセパレータとして誤認しないようにする
    re.compile(r"(?:場所|作業場所|勤務地)[ \t]*[：: ][ \t]*[^\n]*(?:リモート|テレワーク|在宅)"),
    # 「場所/勤務地\nテレワーク」形式（場所ラベルの次行に値がある場合）
    re.compile(r"(?:場所|作業場所|勤務地)[^\n]*\n\s*(?:テレワーク|リモート|在宅)"),
    # 「在宅勤務可」「在宅勤務も可能」「在宅勤務あり」: 在宅\s*可 では取れない形式
    re.compile(r"在宅勤務\s*(?:も\s*)?(?:可|可能|あり|有|対応)"),
    # 週N~M / 週N,M / 週N、M リモート（範囲指定、既存の単日パターンと分離）
    # 「の」を挟む形式（週1~2程度のリモートを想定 等）にも対応
    re.compile(r"週\s*[1-9一二三四]\s*[~～、,]\s*[1-9]\s*[日回]?\s*(?:程度|以上|くらい|ほど)?\s*の?\s*(?:リモート|在宅|テレワーク)"),
    # 「X/テレワーク」「X/リモート」（勤務地スラッシュ区切り表記、否定表現を除く）
    re.compile(r"[/／]\s*(?:テレワーク|リモート|在宅)(?!\s*(?:不可|なし|無し|禁止))"),
    # 「リモート主体」
    re.compile(r"リモート\s*主体"),
    # 「リモート週N」系（週N リモート と語順が逆のケース）
    re.compile(r"リモート\s*週\s*[1-9一二三四]"),
    # 「在宅ワーク可能」「在宅ワーク可」: 在宅ワーク + 可否表現
    re.compile(r"在宅\s*ワーク\s*(?:可|可能|あり|有)"),
    # 「在宅見込み」: 定着後は週N回の頻度で在宅見込み 等
    re.compile(r"在宅\s*見込み"),
    # 「現在リモートワーク」「現在はリモートワーク」
    re.compile(r"現在\s*(?:は\s*)?リモートワーク"),
    # 「リモート頻度：週N」（週0を含む、頻度フィールドのhybrid表現）
    # ※ onsite確定パターン（常駐/出社/なし）は _ONSITE_PATTERNS で先行評価済み
    re.compile(r"リモート\s*頻度\s*[：:]\s*週[0-9]"),
    # 「テレワーク週N」（テレワーク+週N の逆語順）
    re.compile(r"テレワーク\s*週\s*[1-9一二三四]"),
    # 「ハイブリット」（ハイブリッドの誤字）
    re.compile(r"ハイブリット"),
    # 「TW併用」「TW使用」（テレワーク略語）
    re.compile(r"TW\s*(?:併用|使用)"),
    # 「基本的にリモートワーク」「基本的にテレワーク」
    re.compile(r"基本的\s*(?:に|は)?\s*(?:リモートワーク|テレワーク)"),
    # 「ほぼリモート」「ほぼ在宅」「ほぼテレワーク」
    re.compile(r"ほぼ\s*(?:リモート|テレワーク|在宅)"),
    # 「週N日出社」系（週1~4出社 ≒ 残りはリモート → hybrid）
    re.compile(r"週\s*[1-4一二三四]\s*[日回]?\s*(?:程度|以上|くらい|ほど)?\s*の?\s*出社"),
    # 「週N-M日出社」「週N~M出社」（範囲表記）
    re.compile(r"週\s*[1-4一二三四]\s*[-~～]\s*[1-4]\s*[日回]?\s*(?:程度)?\s*の?\s*出社"),
    # 「週Nは出社が必要」「週N出社が必要」（Nが1~4の場合 → hybrid）
    re.compile(r"週\s*[1-4一二三四]\s*[日回]?\s*(?:は\s*)?出社\s*(?:が\s*)?必要"),
]

# ── onsite 確定パターン（リモートを否定する表現） ───────
_ONSITE_PATTERNS = [
    re.compile(r"フル\s*出社"),
    re.compile(r"完全\s*出社"),
    re.compile(r"リモート\s*(?:不可|なし|無し|禁止)"),
    re.compile(r"在宅\s*(?:不可|なし|無し|禁止)"),
    re.compile(r"常駐\s*必須"),
    re.compile(r"リモート\s*(?:不\s*可)"),
    # フルリモート否定（フルリモ全般パターンより先に評価される必要あり）
    re.compile(r"フルリモ\s*(?:不可|なし|無し|禁止)"),
    # 「在宅勤務なし」: 在宅\s*なし では取れない「在宅勤務なし」形式
    re.compile(r"在宅勤務\s*(?:なし|無し|不可)"),
    # 「(常駐)」: 豊洲（常駐）/ 駅名(常駐) 等（NFKC正規化後は全角括弧も半角になる）
    re.compile(r"\(常駐\)"),
    # 「基本常駐」「原則常駐」
    re.compile(r"基本\s*常駐"),
    re.compile(r"原則\s*常駐"),
    # 「テレワーク不可/なし」（_HYBRID_PATTERNS のテレワーク系パターンより先に評価）
    re.compile(r"テレワーク\s*(?:不可|なし|無し|禁止)"),
    # 「リモート頻度：常駐/基本出社/出社/なし」: 頻度フィールドに onsite 明示
    re.compile(r"リモート\s*頻度\s*[：:]\s*(?:常駐|基本\s*出社|出社|なし|無し|不可)"),
]


def _parse_weekly_days(m: re.Match) -> int:
    """週N日リモートのNを整数に変換する。"""
    raw = m.group(1)
    return _WEEKLY_REMOTE_KANJI.get(raw, int(raw))


def rule_extract_remote(body: str) -> Tuple[str, str, Optional[int], Optional[str]]:
    """
    ルールベースでリモート種別を抽出する。

    Returns:
        (remote_type, source, remote_days_per_week, remote_raw)
          remote_type: "onsite" | "hybrid" | "fullremote"
          source: "extracted" | "default"
          remote_days_per_week: int or None
          remote_raw: str or None
    """
    if not body:
        return DEFAULT_REMOTE_TYPE, "default", None, None

    text = _n(body)

    # onsite確定パターン（リモート否定）を先に確認
    for pat in _ONSITE_PATTERNS:
        m = pat.search(text)
        if m:
            logger.rule(f"onsite確定パターン一致: {m.group(0)}")
            return "onsite", "extracted", None, m.group(0)

    # fullremoteチェック
    for pat in _FULLREMOTE_PATTERNS:
        m = pat.search(text)
        if m:
            return "fullremote", "extracted", None, m.group(0)

    # 週Nリモート（hybridかfullremoteかを日数で判定）
    m = _WEEKLY_REMOTE_RE.search(text)
    if m:
        days = _parse_weekly_days(m)
        if days >= 5:
            return "fullremote", "extracted", days, m.group(0)
        return "hybrid", "extracted", days, m.group(0)

    # hybridパターン
    for pat in _HYBRID_PATTERNS:
        m = pat.search(text)
        if m:
            return "hybrid", "extracted", None, m.group(0)

    return DEFAULT_REMOTE_TYPE, "default", None, None


# ── レコード構築 ──────────────────────────────────────────
def build_record(mid: str, body: str) -> Dict:
    remote_type, source, days_per_week, remote_raw = rule_extract_remote(body)
    return {
        "message_id": mid,
        "remote_type": remote_type,
        "remote_type_source": source,
        "remote_days_per_week": days_per_week,
        "remote_raw": remote_raw,
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

        # remote_typeがnull/unknownまたは3種類以外は分離
        if rec["remote_type"] not in VALID_REMOTE_TYPES:
            null_records.append(rec)
        else:
            extracted.append(rec)

        logger.info(
            f"{mid} → remote_type={rec['remote_type']} source={rec['remote_type_source']} "
            f"days={rec['remote_days_per_week']} raw={rec['remote_raw']}",
            message_id=mid,
        )

    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path,  extracted)
    write_jsonl(null_path, null_records)

    elapsed = time.time() - start
    total   = len(project_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    fullremote_count = sum(1 for r in extracted if r["remote_type"] == "fullremote")
    hybrid_count     = sum(1 for r in extracted if r["remote_type"] == "hybrid")
    onsite_count     = sum(1 for r in extracted if r["remote_type"] == "onsite")
    extracted_count  = sum(1 for r in extracted if r["remote_type_source"] == "extracted")

    logger.ok(
        f"Step完了: 入力={total}件 / fullremote={fullremote_count}件 / "
        f"hybrid={hybrid_count}件 / onsite={onsite_count}件 "
        f"(extracted={extracted_count}件) / null={len(null_records)}件"
    )


if __name__ == "__main__":
    main()

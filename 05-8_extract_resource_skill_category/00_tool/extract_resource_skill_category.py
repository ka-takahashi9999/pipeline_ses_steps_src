#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 05-8: 要員メールからスキル・カテゴリをルールベースで抽出

Feature flag (config.py の ENABLE_SKILL_CATEGORY):
  False（デフォルト）:
    - 全レコードを skills=[], skills_by_category={}, skills_raw=null でpass-through出力
  True:
    - skill_dictionary.txt を使ってスキルを抽出・カテゴリ分類

マッチングルール:
  ・NFKC正規化後に大文字小文字を区別しないマッチング
  ・複合スキル（"SQL Server"等）は単体スキルより優先してマッチング
  ・単語境界（\b相当）を用いてスキル名の部分マッチを防止

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/resources.jsonl

出力①（抽出結果）:
  01_result/extract_resource_skill_category.jsonl
出力②（nullまたは空スキルの異常レコード）:
  01_result/99_skill_category_null_extract_resource_skill_category.jsonl
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

# feature flag をインポート
sys.path.insert(0, str(_STEP_DIR / "00_tool"))
from config import ENABLE_SKILL_CATEGORY

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "05-8_extract_resource_skill_category"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_RESOURCES = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "resources.jsonl"
)
SKILL_DICT_PATH  = str(_STEP_DIR / "00_tool" / "skill_dictionary.txt")
OUTPUT_EXTRACTED = "extract_resource_skill_category.jsonl"
OUTPUT_NULL      = "99_skill_category_null_extract_resource_skill_category.jsonl"

SKILL_CANONICAL_MAP = {
    "Springboot": "Spring Boot",
    "SpringBoot": "Spring Boot",
    "NodeJS": "Node.js",
}


# ── 正規化 ─────────────────────────────────────────────────
def _n(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "")


def canonicalize_skill_name(skill_name: str) -> str:
    """辞書表記ゆれを canonical 名に寄せる。"""
    return SKILL_CANONICAL_MAP.get(skill_name, skill_name)


# ── 辞書ロード ────────────────────────────────────────────
def load_skill_dictionary(path: str) -> Dict[str, List[Tuple[str, re.Pattern]]]:
    """
    YAML形式のスキル辞書を読み込む。

    Returns:
        {カテゴリ: [(skill_name, compiled_pattern), ...]}
        ・複合スキルが単体スキルより先に来るよう長さ降順でソート済み
    """
    import yaml

    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    result: Dict[str, List[Tuple[str, re.Pattern]]] = {}
    for category, skills in (data or {}).items():
        if not isinstance(skills, list):
            continue
        entries: List[Tuple[str, re.Pattern]] = []
        for skill in skills:
            if not skill:
                continue
            skill_str = str(skill).strip()
            escaped   = re.escape(skill_str)
            # 英数字のみのスキルは単語境界で囲む（日本語混在は不要）
            if re.match(r"^[A-Za-z0-9. #+\-/]+$", skill_str):
                pattern = re.compile(
                    r"(?<![A-Za-z0-9\-\.])" + escaped + r"(?![A-Za-z0-9\-\.])",
                    re.IGNORECASE,
                )
            else:
                pattern = re.compile(escaped, re.IGNORECASE)
            entries.append((skill_str, pattern))

        # 複合スキル優先（長さ降順でソート）
        entries.sort(key=lambda x: len(x[0]), reverse=True)
        result[category] = entries

    return result


# ── スキル抽出 ─────────────────────────────────────────────
def extract_skills(
    body: str,
    skill_dict: Dict[str, List[Tuple[str, re.Pattern]]],
) -> Tuple[List[str], Dict[str, List[str]], Optional[str]]:
    """
    本文からスキルを抽出してカテゴリ分類する。

    Returns:
        (skills, skills_by_category, skills_raw)
    """
    text = _n(body)
    matched_spans: List[Tuple[int, int]] = []
    found_order: List[Tuple[str, str]] = []  # (skill_name, category) 出現順

    def _overlaps(start: int, end: int) -> bool:
        for s, e in matched_spans:
            if start < e and end > s:
                return True
        return False

    for category, entries in skill_dict.items():
        for skill_name, pattern in entries:
            for m in pattern.finditer(text):
                if not _overlaps(m.start(), m.end()):
                    matched_spans.append((m.start(), m.end()))
                    found_order.append((canonicalize_skill_name(skill_name), category))

    if not found_order:
        return [], {}, None

    # 出現位置順にソート
    combined = sorted(
        zip(found_order, matched_spans),
        key=lambda x: x[1][0],
    )

    skills: List[str] = []
    seen_skills: set = set()
    skills_by_category: Dict[str, List[str]] = {}

    for (skill_name, category), _ in combined:
        if skill_name in seen_skills:
            continue
        seen_skills.add(skill_name)
        skills.append(skill_name)
        if category not in skills_by_category:
            skills_by_category[category] = []
        skills_by_category[category].append(skill_name)

    # skills_raw: マッチ位置の前後コンテキスト（最大200文字）
    if matched_spans:
        spans_sorted = sorted(matched_spans)
        first_start  = max(0, spans_sorted[0][0] - 30)
        last_end     = min(len(text), spans_sorted[-1][1] + 50)
        raw_text     = text[first_start:last_end].strip()
        skills_raw: Optional[str] = raw_text[:200] if raw_text else None
    else:
        skills_raw = None

    return skills, skills_by_category, skills_raw


# ── レコード構築 ──────────────────────────────────────────
def build_passthrough_record(mid: str) -> Dict:
    """Feature flag OFF時のpass-throughレコード。"""
    return {
        "message_id": mid,
        "skills": [],
        "skills_by_category": {},
        "skills_raw": None,
    }


def build_extracted_record(
    mid: str,
    body: str,
    skill_dict: Dict[str, List[Tuple[str, re.Pattern]]],
) -> Dict:
    """Feature flag ON時の抽出レコード。"""
    skills, skills_by_category, skills_raw = extract_skills(body, skill_dict)
    return {
        "message_id": mid,
        "skills": skills,
        "skills_by_category": skills_by_category,
        "skills_raw": skills_raw,
    }


def _is_valid(rec: Dict) -> bool:
    """skills/skills_by_category が list/dict 型であること。"""
    return (
        isinstance(rec.get("skills"), list)
        and isinstance(rec.get("skills_by_category"), dict)
    )


# ── メイン ────────────────────────────────────────────────
def main() -> None:
    start = time.time()
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = dirs["result"]

    logger.info(f"ENABLE_SKILL_CATEGORY = {ENABLE_SKILL_CATEGORY}")

    for path in [INPUT_RESOURCES, INPUT_CLEANED]:
        if not Path(path).exists():
            logger.error(f"入力ファイルが存在しません: {path}")
            sys.exit(1)

    try:
        resource_ids = [r["message_id"] for r in read_jsonl_as_list(INPUT_RESOURCES)]
    except Exception as e:
        write_error_log(str(result_dir), e, "入力ファイル読み込みエラー")
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    # feature flag OFF: pass-through
    if not ENABLE_SKILL_CATEGORY:
        logger.info("feature flag OFF: pass-through モードで実行します")
        extracted    = [build_passthrough_record(mid) for mid in resource_ids]
        null_records: list = []
        out_path  = str(result_dir / OUTPUT_EXTRACTED)
        null_path = str(result_dir / OUTPUT_NULL)
        write_jsonl(out_path,  extracted)
        write_jsonl(null_path, null_records)
        elapsed = time.time() - start
        write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, len(resource_ids))
        logger.ok(
            f"Step完了(pass-through): 入力={len(resource_ids)}件 / "
            f"出力={len(extracted)}件 / null=0件"
        )
        return

    # feature flag ON: 辞書ロード → 抽出
    if not Path(SKILL_DICT_PATH).exists():
        logger.error(f"スキル辞書が存在しません: {SKILL_DICT_PATH}")
        sys.exit(1)

    try:
        import yaml  # noqa: F401
    except ImportError:
        logger.error("PyYAMLがインストールされていません: pip install pyyaml")
        sys.exit(1)

    try:
        skill_dict  = load_skill_dictionary(SKILL_DICT_PATH)
        cleaned_map = read_jsonl_as_dict(INPUT_CLEANED)
    except Exception as e:
        write_error_log(str(result_dir), e, "初期化エラー")
        logger.error(f"初期化エラー: {e}")
        sys.exit(1)

    total_skills = sum(len(v) for v in skill_dict.values())
    logger.info(
        f"スキル辞書ロード完了: {len(skill_dict)}カテゴリ / {total_skills}スキル"
    )

    extracted_list: list = []
    null_records = []

    for mid in resource_ids:
        body = (cleaned_map.get(mid) or {}).get("body_text", "")
        rec  = build_extracted_record(mid, body, skill_dict)

        if not _is_valid(rec):
            null_records.append(rec)
        else:
            extracted_list.append(rec)

        n_skills = len(rec.get("skills") or [])
        logger.info(
            f"{mid} → skills={n_skills}件 categories={list(rec.get('skills_by_category', {}).keys())}",
            message_id=mid,
        )

    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path,  extracted_list)
    write_jsonl(null_path, null_records)

    elapsed = time.time() - start
    total   = len(resource_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    has_skills   = sum(1 for r in extracted_list if r.get("skills"))
    empty_skills = sum(1 for r in extracted_list if not r.get("skills"))
    logger.ok(
        f"Step完了: 入力={total}件 / スキルあり={has_skills}件 / "
        f"スキルなし={empty_skills}件 / null={len(null_records)}件"
    )


if __name__ == "__main__":
    main()

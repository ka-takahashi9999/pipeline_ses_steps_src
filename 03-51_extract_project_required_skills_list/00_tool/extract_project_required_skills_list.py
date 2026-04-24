#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 03-51: 案件の必須/尚可スキル文から辞書ベースでスキル語・工程語を抽出する
LLM使用禁止。辞書ベース・文字列処理のみ。

入力:
  03-50_extract_project_required_skills/01_result/extract_project_required_skills.jsonl
  03-8_extract_project_skill_category/00_tool/skill_dictionary.txt
  03-9_extract_project_phase_category/00_tool/phase_dictionary.txt

出力①（抽出結果）:
  01_result/extract_project_required_skills_list.jsonl
出力②（スキル語・工程語が1件も抽出できなかったレコード）:
  01_result/99_required_skills_list_null.jsonl

出力JSONキー（固定）:
  message_id, required_skill_keywords, required_phase_keywords,
  optional_skill_keywords, optional_phase_keywords
"""

import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple

_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "03-51_extract_project_required_skills_list"
logger = get_logger(STEP_NAME)

INPUT_REQUIRED_SKILLS = str(
    _PROJECT_ROOT / "03-50_extract_project_required_skills" / "01_result" / "extract_project_required_skills.jsonl"
)
SKILL_DICT_PATH = str(
    _PROJECT_ROOT / "03-8_extract_project_skill_category" / "00_tool" / "skill_dictionary.txt"
)
PHASE_DICT_PATH = str(
    _PROJECT_ROOT / "03-9_extract_project_phase_category" / "00_tool" / "phase_dictionary.txt"
)

OUTPUT_EXTRACTED = "extract_project_required_skills_list.jsonl"
OUTPUT_NULL      = "99_required_skills_list_null.jsonl"

EXCLUDED_ITEM_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"不可",
        r"未経験者",
        r"優遇",
        r"お願いいたします",
        r"〇×",
        r"○×",
    ]
]

EXCLUDED_ITEM_EXACT_TEXTS = {
    "諸条件",
    "条件",
}

GENERIC_TECH_KEYWORDS = {
    "HTML",
    "CSS",
    "JavaScript",
    "TypeScript",
    "Java",
    "PHP",
    "Python",
    "Ruby",
    "Go",
    "C",
    "C++",
    "C#",
    "SQL",
    "React",
    "Vue",
    "Vue.js",
    "Next.js",
    "Node.js",
    "Spring",
    "Spring Boot",
    "Laravel",
    "AWS",
    "Azure",
    "GCP",
    "Linux",
    "Docker",
}

SUPPLEMENTAL_REQUIRED_PATTERNS = [
    ("Git", re.compile(r"\bGit\b", re.IGNORECASE)),
    ("ブランチ運用", re.compile(r"ブランチ運用")),
    ("チーム開発", re.compile(r"チーム開発")),
    ("レスポンスデザイン", re.compile(r"レスポンシブ?デザイン|レスポンスデザイン")),
    ("表示崩れ防止", re.compile(r"表示崩れ防止|表示崩れ")),
    ("品質", re.compile(r"品質面|品質")),
    ("コーディング", re.compile(r"コーディング")),
    ("仕様書", re.compile(r"仕様書")),
    ("自走", re.compile(r"自走")),
]

YEARS_EXPERIENCE_PATTERN = re.compile(r"(?:(実務経験)\s*)?(\d+)\s*年以上")


# ── 正規化 ────────────────────────────────────────────────────────────────
def _normalize(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "")


def should_skip_skill_item(text: str) -> bool:
    normalized = _normalize(text).strip()
    if not normalized:
        return True
    if normalized in EXCLUDED_ITEM_EXACT_TEXTS:
        return True
    return any(pattern.search(normalized) for pattern in EXCLUDED_ITEM_PATTERNS)


def is_generic_only_keywords(keywords: List[str]) -> bool:
    return bool(keywords) and all(kw in GENERIC_TECH_KEYWORDS for kw in keywords)


def extract_supplemental_required_keywords(text: str) -> List[str]:
    normalized = _normalize(text)
    matched: List[Tuple[int, str]] = []

    for keyword, pattern in SUPPLEMENTAL_REQUIRED_PATTERNS:
        for match in pattern.finditer(normalized):
            matched.append((match.start(), keyword))

    for match in YEARS_EXPERIENCE_PATTERN.finditer(normalized):
        keyword = f"実務経験{match.group(2)}年以上" if match.group(1) else f"{match.group(2)}年以上"
        matched.append((match.start(), keyword))

    seen = set()
    results: List[str] = []
    for _, keyword in sorted(matched, key=lambda x: x[0]):
        if keyword in seen:
            continue
        seen.add(keyword)
        results.append(keyword)
    return results


def is_year_experience_keyword(keyword: str) -> bool:
    return bool(re.fullmatch(r"(?:実務経験)?\d+年以上", keyword))


def augment_required_skill_keywords(required_items: List[dict], base_keywords: List[str]) -> List[str]:
    supplemented = list(base_keywords)
    seen = set(base_keywords)
    generic_only = is_generic_only_keywords(base_keywords)

    priority_keywords: List[str] = []
    extra_keywords: List[str] = []
    priority_set = {"Git"}

    for item in required_items or []:
        text = (item.get("skill") or "").strip()
        if should_skip_skill_item(text):
            continue
        for keyword in extract_supplemental_required_keywords(text):
            if keyword in seen:
                continue
            if generic_only or keyword in priority_set or is_year_experience_keyword(keyword):
                priority_keywords.append(keyword)
            else:
                extra_keywords.append(keyword)
            seen.add(keyword)

    for keyword in priority_keywords + extra_keywords:
        if keyword not in supplemented:
            supplemented.append(keyword)
    return supplemented


# ── 辞書ロード ────────────────────────────────────────────────────────────
def load_skill_list(path: str) -> List[Tuple[str, re.Pattern]]:
    """
    YAML形式のスキル辞書から全スキル名を読み込む。
    複合スキル優先のため長さ降順でソートしてパターンを返す。
    Returns: [(skill_name, compiled_pattern), ...]
    """
    import yaml
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    entries: List[Tuple[str, re.Pattern]] = []
    seen = set()
    for category, skills in (data or {}).items():
        if not isinstance(skills, list):
            continue
        for skill in skills:
            if not skill:
                continue
            skill_str = str(skill).strip()
            if skill_str in seen:
                continue
            seen.add(skill_str)
            escaped = re.escape(_normalize(skill_str))
            if re.match(r"^[A-Za-z0-9. #+\-/]+$", skill_str):
                pattern = re.compile(
                    r"(?<![A-Za-z0-9\-\.])" + escaped + r"(?![A-Za-z0-9\-\.])",
                    re.IGNORECASE,
                )
            else:
                pattern = re.compile(escaped, re.IGNORECASE)
            entries.append((skill_str, pattern))

    # 複合スキル優先（長さ降順）
    entries.sort(key=lambda x: len(x[0]), reverse=True)
    return entries


def load_phase_map(path: str) -> List[Tuple[str, str, re.Pattern]]:
    """
    YAML形式の工程辞書を読み込む。
    Returns: [(phase_name, keyword, compiled_pattern), ...]
      ・長さ降順でソート済み（複合キーワード優先）
    """
    import yaml
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    entries: List[Tuple[str, str, re.Pattern]] = []
    for phase_name, keywords in (data or {}).items():
        if not isinstance(keywords, list):
            continue
        for kw in keywords:
            if not kw:
                continue
            kw_str = str(kw).strip()
            escaped = re.escape(_normalize(kw_str))
            if re.match(r"^[A-Za-z0-9. #+\-/]+$", kw_str):
                pattern = re.compile(
                    r"(?<![A-Za-z0-9\-\.])" + escaped + r"(?![A-Za-z0-9\-\.])",
                    re.IGNORECASE,
                )
            else:
                pattern = re.compile(escaped, re.IGNORECASE)
            entries.append((phase_name, kw_str, pattern))

    # 複合キーワード優先（長さ降順）
    entries.sort(key=lambda x: len(x[1]), reverse=True)
    return entries


# ── 抽出関数 ──────────────────────────────────────────────────────────────
def extract_skill_keywords(text: str, skill_entries: List[Tuple[str, re.Pattern]]) -> List[str]:
    """
    テキストからスキル語を抽出する（重複排除・初出順）。
    """
    normalized = _normalize(text)
    matched: List[Tuple[int, str]] = []
    matched_spans: List[Tuple[int, int]] = []

    def _overlaps(start: int, end: int) -> bool:
        for s, e in matched_spans:
            if start < e and end > s:
                return True
        return False

    for skill_name, pattern in skill_entries:
        for m in pattern.finditer(normalized):
            if not _overlaps(m.start(), m.end()):
                matched_spans.append((m.start(), m.end()))
                matched.append((m.start(), skill_name))

    # 出現順にソートして重複排除
    seen = set()
    result = []
    for _, skill_name in sorted(matched, key=lambda x: x[0]):
        if skill_name not in seen:
            seen.add(skill_name)
            result.append(skill_name)
    return result


def extract_phase_keywords(text: str, phase_entries: List[Tuple[str, str, re.Pattern]]) -> List[str]:
    """
    テキストから工程語を抽出する（phase_nameを返す、重複排除・初出順）。
    """
    normalized = _normalize(text)
    matched: List[Tuple[int, str]] = []
    matched_spans: List[Tuple[int, int]] = []

    def _overlaps(start: int, end: int) -> bool:
        for s, e in matched_spans:
            if start < e and end > s:
                return True
        return False

    for phase_name, _kw, pattern in phase_entries:
        for m in pattern.finditer(normalized):
            if not _overlaps(m.start(), m.end()):
                matched_spans.append((m.start(), m.end()))
                matched.append((m.start(), phase_name))

    # 出現順にソートして重複排除
    seen = set()
    result = []
    for _, phase_name in sorted(matched, key=lambda x: x[0]):
        if phase_name not in seen:
            seen.add(phase_name)
            result.append(phase_name)
    return result


def extract_from_skill_list(
    skill_items: List[dict],
    skill_entries: List[Tuple[str, re.Pattern]],
    phase_entries: List[Tuple[str, str, re.Pattern]],
) -> Tuple[List[str], List[str]]:
    """
    skill_items（required_skills or optional_skills）のリストを処理し、
    スキル語と工程語を抽出してまとめる（重複排除）。
    """
    all_skill_kws: List[str] = []
    all_phase_kws: List[str] = []
    seen_skill: set = set()
    seen_phase: set = set()

    for item in (skill_items or []):
        text = (item.get("skill") or "").strip()
        if should_skip_skill_item(text):
            continue
        for kw in extract_skill_keywords(text, skill_entries):
            if kw not in seen_skill:
                seen_skill.add(kw)
                all_skill_kws.append(kw)
        for kw in extract_phase_keywords(text, phase_entries):
            if kw not in seen_phase:
                seen_phase.add(kw)
                all_phase_kws.append(kw)

    return all_skill_kws, all_phase_kws


def build_record(
    message_id: str,
    required_items: List[dict],
    optional_items: List[dict],
    skill_entries: List[Tuple[str, re.Pattern]],
    phase_entries: List[Tuple[str, str, re.Pattern]],
) -> dict:
    req_skill_kws, req_phase_kws = extract_from_skill_list(required_items, skill_entries, phase_entries)
    req_skill_kws = augment_required_skill_keywords(required_items, req_skill_kws)
    opt_skill_kws, opt_phase_kws = extract_from_skill_list(optional_items, skill_entries, phase_entries)
    return {
        "message_id": message_id,
        "required_skill_keywords": req_skill_kws,
        "required_phase_keywords": req_phase_kws,
        "optional_skill_keywords": opt_skill_kws,
        "optional_phase_keywords": opt_phase_kws,
    }


def is_null_record(rec: dict) -> bool:
    """4つ全て空配列ならTrue（nullレコード扱い）"""
    return (
        not rec["required_skill_keywords"]
        and not rec["required_phase_keywords"]
        and not rec["optional_skill_keywords"]
        and not rec["optional_phase_keywords"]
    )


# ── メイン ────────────────────────────────────────────────────────────────
def main() -> None:
    start = time.time()
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = dirs["result"]

    # 入力ファイル存在確認
    for path in [INPUT_REQUIRED_SKILLS, SKILL_DICT_PATH, PHASE_DICT_PATH]:
        if not Path(path).exists():
            msg = f"入力ファイルが存在しません: {path}"
            logger.error(msg)
            write_error_log(str(result_dir), FileNotFoundError(msg), STEP_NAME)
            sys.exit(1)

    # PyYAML確認
    try:
        import yaml  # noqa: F401
    except ImportError:
        logger.error("PyYAMLがインストールされていません: pip install pyyaml")
        sys.exit(1)

    # 辞書ロード
    try:
        skill_entries = load_skill_list(SKILL_DICT_PATH)
        phase_entries = load_phase_map(PHASE_DICT_PATH)
    except Exception as e:
        write_error_log(str(result_dir), e, "辞書ロードエラー")
        logger.error(f"辞書ロードエラー: {e}")
        sys.exit(1)

    total_skills = len(skill_entries)
    total_phases = sum(1 for _ in {p for p, _, _ in phase_entries})
    logger.info(f"スキル辞書ロード完了: {total_skills}スキル語")
    logger.info(f"工程辞書ロード完了: {len({p for p, _, _ in phase_entries})}工程 / {len(phase_entries)}キーワード")

    # 入力読み込み
    try:
        input_records = read_jsonl_as_list(INPUT_REQUIRED_SKILLS)
    except Exception as e:
        write_error_log(str(result_dir), e, "入力ファイル読み込みエラー")
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    logger.info(f"入力件数: {len(input_records)}")

    extracted_list = []
    null_list = []

    for rec in input_records:
        message_id = rec.get("message_id", "")
        required_items = rec.get("required_skills") or []
        optional_items = rec.get("optional_skills") or []

        out_rec = build_record(message_id, required_items, optional_items, skill_entries, phase_entries)

        if is_null_record(out_rec):
            null_list.append(out_rec)
        else:
            extracted_list.append(out_rec)

        total_kws = (
            len(out_rec["required_skill_keywords"])
            + len(out_rec["required_phase_keywords"])
            + len(out_rec["optional_skill_keywords"])
            + len(out_rec["optional_phase_keywords"])
        )
        logger.info(
            f"{message_id} → req_skill={len(out_rec['required_skill_keywords'])} "
            f"req_phase={len(out_rec['required_phase_keywords'])} "
            f"opt_skill={len(out_rec['optional_skill_keywords'])} "
            f"opt_phase={len(out_rec['optional_phase_keywords'])} "
            f"合計={total_kws}",
            message_id=message_id,
        )

    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path,  extracted_list)
    write_jsonl(null_path, null_list)

    elapsed = time.time() - start
    total = len(input_records)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    logger.ok(
        f"Step完了: 入力={total}件 / 抽出={len(extracted_list)}件 / null={len(null_list)}件"
    )


if __name__ == "__main__":
    main()

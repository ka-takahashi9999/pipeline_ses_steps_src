"""
06-11_match_required_skills_list
03-51 で抽出した required_skill_keywords を主軸とし、
required_phase_keywords のうち差が出やすい工程語だけを補強条件として使う。
LLM使用禁止。

判定ロジック：
  required_skill_keywords / required_phase_keywords が両方空 → false（除外）
  required_skill_keywords が1件も無い → false
  required_skill_keywords がヒットしない → false
  強い工程語が無い案件 → required_skill_keywords ヒットで true
  強い工程語がある案件 → required_skill_keywords と強い工程語の両方ヒットで true
  テストのみある案件 → required_skill_keywords ヒットで true
"""

import sys
import time
import unicodedata
from pathlib import Path
from typing import List, Optional, Set, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import merge_match_info, read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "06-11_match_required_skills_list"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS         = project_root / "06-10_match_location/01_result/matched_pairs_location.jsonl"
INPUT_PROJECT_KWS   = project_root / "03-51_extract_project_required_skills_list/01_result/extract_project_required_skills_list.jsonl"
INPUT_SKILLSHEET    = project_root / "04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl"
INPUT_EMAIL_BODY    = project_root / "01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl"

OUTPUT_MATCHED    = STEP_DIR / "01_result/matched_pairs_required_skills_list.jsonl"
OUTPUT_NO_MATCHED = STEP_DIR / "01_result/99_not_matched_pairs_required_skills_list.jsonl"

REQUIRED_SKILL_KEY = "required_skill_keywords"
REQUIRED_PHASE_KEY = "required_phase_keywords"

STRONG_REQUIRED_PHASE_KEYWORDS = {
    "要件定義",
    "基本設計",
    "詳細設計",
    "設計・構築",
    "運用保守",
    "移行",
    "PMO",
    "要件調査",
    "リリース",
}

WEAK_REQUIRED_PHASE_KEYWORDS = {
    "テスト",
}

SQL_RELATED_DATABASE_KEYWORDS = {
    "Oracle",
    "MySQL",
    "PostgreSQL",
    "SQL Server",
    "SQLServer",
    "PL/SQL",
    "PLSQL",
    "T-SQL",
}


def _normalize(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "")


def build_required_skill_keyword_set(project_rec: dict) -> List[str]:
    """
    required_skill_keywords を重複排除・順序保持で返す。
    ただし SQL が DB製品群と同居している場合は、SQL を独立 required から外す。
    """
    raw_keywords = [kw for kw in (project_rec.get(REQUIRED_SKILL_KEY) or []) if kw]
    weaken_sql = "SQL" in raw_keywords and any(
        kw in SQL_RELATED_DATABASE_KEYWORDS for kw in raw_keywords
    )

    seen: Set[str] = set()
    result: List[str] = []
    for kw in raw_keywords:
        if weaken_sql and kw == "SQL":
            continue
        if kw not in seen:
            seen.add(kw)
            result.append(kw)
    return result


def build_required_phase_keywords(project_rec: dict) -> Tuple[List[str], List[str]]:
    """
    required_phase_keywords から補強対象の工程語だけを返す。
    Returns:
        (strong_phase_keywords, weak_phase_keywords)
    """
    strong_result: List[str] = []
    weak_result: List[str] = []
    seen_strong: Set[str] = set()
    seen_weak: Set[str] = set()
    for kw in (project_rec.get(REQUIRED_PHASE_KEY) or []):
        if kw in STRONG_REQUIRED_PHASE_KEYWORDS and kw not in seen_strong:
            seen_strong.add(kw)
            strong_result.append(kw)
        elif kw in WEAK_REQUIRED_PHASE_KEYWORDS and kw not in seen_weak:
            seen_weak.add(kw)
            weak_result.append(kw)
    return strong_result, weak_result


def keyword_matches_in_text(keyword: str, text: str) -> bool:
    normalized_text = _normalize(text).lower()
    normalized_kw = _normalize(keyword).lower()
    if not normalized_kw:
        return False
    return normalized_kw in normalized_text


def search_keywords_in_text(keywords: List[str], text: str) -> List[str]:
    """
    textにkeywordsのうち含まれるものを返す（1件でもヒットすればよい）。
    NFKC正規化済みテキスト・キーワードで比較。
    英字は小文字化して比較。
    """
    if not text:
        return []
    matched = []
    for kw in keywords:
        if keyword_matches_in_text(kw, text):
            matched.append(kw)
    return matched


def judge_match(
    required_skill_keywords: List[str],
    strong_required_phase_keywords: List[str],
    weak_required_phase_keywords: List[str],
    skillsheet_text: Optional[str],
    email_body_text: Optional[str],
) -> Tuple[bool, List[str], List[str], List[str]]:
    """
    マッチ判定。
    Returns:
        (is_match, matched_skill_keywords, matched_phase_keywords, matched_in)
    """
    if not required_skill_keywords and not strong_required_phase_keywords and not weak_required_phase_keywords:
        return False, [], [], []

    if not required_skill_keywords:
        return False, [], [], []

    texts = [
        ("skillsheet", skillsheet_text or ""),
        ("email_body", email_body_text or ""),
    ]

    best_skill_hits: List[str] = []
    best_phase_hits: List[str] = []

    for source_name, text in texts:
        skill_hits = search_keywords_in_text(required_skill_keywords, text)
        if not skill_hits:
            continue

        if strong_required_phase_keywords:
            strong_phase_hits = search_keywords_in_text(strong_required_phase_keywords, text)
            if strong_phase_hits:
                return True, skill_hits, strong_phase_hits, [source_name]
            if len(skill_hits) > len(best_skill_hits):
                best_skill_hits = skill_hits
                best_phase_hits = []
            continue

        if weak_required_phase_keywords:
            weak_phase_hits = search_keywords_in_text(weak_required_phase_keywords, text)
            return True, skill_hits, weak_phase_hits, [source_name]

        return True, skill_hits, [], [source_name]

    return False, best_skill_hits, best_phase_hits, []


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    logger.info("処理開始")

    # 入力ファイル存在確認
    for path in [INPUT_PAIRS, INPUT_PROJECT_KWS, INPUT_SKILLSHEET, INPUT_EMAIL_BODY]:
        if not path.exists():
            msg = f"入力ファイルが存在しません: {path}"
            logger.error(msg)
            write_error_log(str(dirs["result"]), FileNotFoundError(msg), STEP_NAME)
            sys.exit(1)

    try:
        pairs            = read_jsonl_as_list(str(INPUT_PAIRS))
        project_kws_map  = read_jsonl_as_dict(str(INPUT_PROJECT_KWS),  key="message_id")
        skillsheet_map   = read_jsonl_as_dict(str(INPUT_SKILLSHEET),    key="message_id")
        email_body_map   = read_jsonl_as_dict(str(INPUT_EMAIL_BODY),    key="message_id")
    except Exception as e:
        write_error_log(str(dirs["result"]), e, context=STEP_NAME)
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    logger.info(f"入力ペア数: {len(pairs)}")
    logger.info(f"案件キーワードレコード数: {len(project_kws_map)}")
    logger.info(f"スキルシートレコード数: {len(skillsheet_map)}")
    logger.info(f"メール本文レコード数: {len(email_body_map)}")

    matched = []
    no_matched = []
    passthrough_count = 0

    for pair in pairs:
        project_message_id  = pair["project_info"]["message_id"]
        resource_message_id = pair["resource_info"]["message_id"]

        project_rec       = project_kws_map.get(project_message_id, {})
        skillsheet_rec    = skillsheet_map.get(resource_message_id, {})
        email_body_rec    = email_body_map.get(resource_message_id, {})

        required_skill_keywords = build_required_skill_keyword_set(project_rec)
        strong_required_phase_keywords, weak_required_phase_keywords = build_required_phase_keywords(project_rec)
        skillsheet_text   = skillsheet_rec.get("skillsheet") or ""
        email_body_text   = email_body_rec.get("body_text") or ""

        is_match, matched_skill_kws, matched_phase_kws, matched_in = judge_match(
            required_skill_keywords,
            strong_required_phase_keywords,
            weak_required_phase_keywords,
            skillsheet_text,
            email_body_text,
        )

        match_info_update = {"match_required_skills_list": is_match}
        record = merge_match_info(pair, match_info_update)

        if not required_skill_keywords and not strong_required_phase_keywords and not weak_required_phase_keywords:
            match_reason = "rejected_no_required_conditions"
        elif is_match and matched_phase_kws:
            match_reason = "matched_required_skill_and_phase"
        elif is_match and matched_skill_kws:
            match_reason = "matched_required_skill_only"
        else:
            match_reason = "not_matched"

        record["match_detail"] = {
            "matched_keywords": matched_skill_kws,
            "matched_phase_keywords": matched_phase_kws,
            "matched_in": matched_in,
            "match_reason": match_reason,
        }

        if not required_skill_keywords and not strong_required_phase_keywords and not weak_required_phase_keywords:
            passthrough_count += 1

        if is_match:
            matched.append(record)
        else:
            no_matched.append(record)
            logger.info(
                f"NO_MATCH: project={project_message_id} resource={resource_message_id}"
                f" required_skill_count={len(required_skill_keywords)}"
                f" strong_phase_count={len(strong_required_phase_keywords)}"
                f" weak_phase_count={len(weak_required_phase_keywords)}"
                f" matched_skill_keywords={matched_skill_kws[:5]}"
                f" matched_phase_keywords={matched_phase_kws[:5]} matched_in={matched_in}"
            )

    write_jsonl(str(OUTPUT_MATCHED), matched)
    write_jsonl(str(OUTPUT_NO_MATCHED), no_matched)

    elapsed = time.time() - start_time
    write_execution_time(
        str(dirs["execution_time"]),
        STEP_NAME,
        elapsed,
        record_count=len(pairs),
    )

    logger.info(
        f"処理完了 合計={len(pairs)} マッチ={len(matched)} 除外={len(no_matched)}"
        f" うちキーワード空（pass-through）={passthrough_count}"
    )


if __name__ == "__main__":
    main()

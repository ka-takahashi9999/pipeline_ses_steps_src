"""
06-12_filter_required_skills_noise
06-11 通過ペアに対して、広く一致しやすい語・短語・文脈依存語を追加で除外する。
LLM使用禁止。
"""

import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Set, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import merge_match_info, read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "06-12_filter_required_skills_noise"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_MATCHED       = project_root / "06-11_match_required_skills_list/01_result/matched_pairs_required_skills_list.jsonl"
INPUT_PROJECT_KWS   = project_root / "03-51_extract_project_required_skills_list/01_result/extract_project_required_skills_list.jsonl"
INPUT_SKILLSHEET    = project_root / "04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl"
INPUT_EMAIL_BODY    = project_root / "01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl"

OUTPUT_MATCHED    = STEP_DIR / "01_result/matched_pairs_required_skills_noise_filtered.jsonl"
OUTPUT_NO_MATCHED = STEP_DIR / "01_result/99_not_matched_pairs_required_skills_noise_filtered.jsonl"

UNCONDITIONAL_EXCLUDED_KEYWORDS = {
    "AI",
    "Go",
    "PM",
    "NW",
    "CSS",
    "顧客折衝",
    "問い合わせ対応",
    "テックリード",
}

FW_NETWORK_CONTEXT_HINT_KEYWORDS = {
    "NW",
    "VPN",
    "Cisco",
    "Cisco Catalyst",
    "Cisco ACI",
    "FortiGate",
    "PaloAlto",
    "F5",
    "Firewall",
    "TCP/IP",
    "L2/L3",
    "ネットワーク",
    "ネットワーク設計",
}

SQL_DB_CONTEXT_HINT_KEYWORDS = {
    "DB",
    "Oracle",
    "MySQL",
    "PostgreSQL",
    "SQL Server",
    "SQLServer",
    "データベース",
    "PL/SQL",
    "T-SQL",
}


def _normalize(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "")


def _normalized_text(skillsheet_text: str, email_body_text: str) -> str:
    return (_normalize(skillsheet_text or "") + "\n" + _normalize(email_body_text or "")).lower()


def has_fw_network_context(project_rec: dict) -> bool:
    required_keywords = set(project_rec.get("required_skill_keywords") or [])
    return bool(required_keywords & FW_NETWORK_CONTEXT_HINT_KEYWORDS)


def has_sql_db_context(project_rec: dict) -> bool:
    required_keywords = set(project_rec.get("required_skill_keywords") or [])
    return bool(required_keywords & SQL_DB_CONTEXT_HINT_KEYWORDS)


def _contains_fw_context(text: str) -> bool:
    fw_exclude_patterns = (
        r"fw[・/ ]mw",
        r"言語\s*/\s*fw",
        r"os\s*/\s*fw\s*/\s*pf",
        r"fw\s*/\s*ライブラリ",
        r"ツール\s*/\s*fw",
    )
    if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in fw_exclude_patterns):
        return False

    fw_include_patterns = (
        r"lb/fw",
        r"firewall",
        r"fortigate",
        r"paloalto",
        r"fw装置",
        r"vpn装置[^\n]{0,30}/lb/fw",
        r"(?<![a-z0-9])fw(?![a-z0-9])",
    )
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in fw_include_patterns)


def _contains_sql_context(text: str) -> bool:
    sql_exclude_patterns = (
        r"mysql\s*[:：]?\s*sql",
        r"postgresql\s*[:：]?\s*sql",
        r"postgre\s*sql\s*[:：]?\s*sql",
        r"mssql\s*server\s*\d*(?:\.\d+)?",
        r"sqlserver\s*\d*(?:\.\d+)?",
        r"mysql",
        r"postgresql",
        r"postgre\s*sql",
    )
    if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in sql_exclude_patterns):
        text = re.sub(
            r"mysql\s*[:：]?\s*sql|postgresql\s*[:：]?\s*sql|postgre\s*sql\s*[:：]?\s*sql|"
            r"mssql\s*server\s*\d*(?:\.\d+)?|sqlserver\s*\d*(?:\.\d+)?|"
            r"mysql|postgresql|postgre\s*sql",
            " ",
            text,
            flags=re.IGNORECASE,
        )

    sql_patterns = (
        r"(?<![a-z0-9])sql(?![a-z0-9])",
        r"pl/sql",
        r"plsql",
        r"t-sql",
    )
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in sql_patterns)


def _contains_java_context(text: str) -> bool:
    if re.search(r"javascript", text, flags=re.IGNORECASE):
        text = re.sub(r"javascript", " ", text, flags=re.IGNORECASE)
    java_patterns = (
        r"(?<![a-z0-9])java(?![a-z0-9])",
        r"java\s*\d+(?:\.\d+)?",
        r"openjdk",
        r"jdk\s*\d+(?:\.\d+)?",
    )
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in java_patterns)


def _contains_s3_context(text: str) -> bool:
    s3_patterns = (
        r"aws\s*s3",
        r"amazon\s*s3",
        r"s3\s*bucket",
        r"s3\s*バケット",
    )
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in s3_patterns)


def _contains_react_context(text: str) -> bool:
    react_patterns = (
        r"react\.js",
        r"reactjs",
        r"react\s*native",
        r"(?<![a-z0-9])react(?![a-z0-9])",
    )
    if not any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in react_patterns):
        return False

    react_support_patterns = (
        r"next\.js",
        r"nextjs",
        r"jsx",
        r"tsx",
        r"redux",
        r"material\s*ui",
        r"mui",
        r"emotion",
        r"hooks?",
        r"react\s*router",
        r"react-query",
    )
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in react_support_patterns)


def _contains_pmo_context(text: str) -> bool:
    pmo_exclude_patterns = (
        r"chrome-extension://",
        r"webstore/detail/",
        r"https?://[^\s]*pmo[^\s]*",
        r"[a-z0-9]pmo[a-z0-9]",
    )
    if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in pmo_exclude_patterns):
        return False

    pmo_include_patterns = (
        r"(?<![a-z0-9])pmo(?![a-z0-9])",
        r"pm/pmo",
        r"情報システムpmo",
        r"pmo補佐",
        r"pmo\s*\d+名",
    )
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in pmo_include_patterns)


def should_keep_keyword(keyword: str, project_rec: dict, combined_text: str) -> Tuple[bool, str]:
    if keyword in UNCONDITIONAL_EXCLUDED_KEYWORDS:
        return False, "broad_or_short_keyword"
    if keyword == "FW":
        if not has_fw_network_context(project_rec):
            return False, "fw_without_network_context"
        return _contains_fw_context(combined_text), "fw_context_check"
    if keyword == "SQL":
        if not has_sql_db_context(project_rec):
            return False, "sql_without_db_context"
        return _contains_sql_context(combined_text), "sql_context_check"
    if keyword == "Java":
        return _contains_java_context(combined_text), "java_context_check"
    if keyword == "React":
        return _contains_react_context(combined_text), "react_context_check"
    if keyword == "S3":
        return _contains_s3_context(combined_text), "s3_context_check"
    if keyword == "PMO":
        return _contains_pmo_context(combined_text), "pmo_context_check"
    return True, "kept"


def main():
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()

    logger.info("処理開始")

    for path in [INPUT_MATCHED, INPUT_PROJECT_KWS, INPUT_SKILLSHEET, INPUT_EMAIL_BODY]:
        if not path.exists():
            msg = f"入力ファイルが存在しません: {path}"
            logger.error(msg)
            write_error_log(str(dirs["result"]), FileNotFoundError(msg), STEP_NAME)
            sys.exit(1)

    try:
        matched_pairs    = read_jsonl_as_list(str(INPUT_MATCHED))
        project_kws_map  = read_jsonl_as_dict(str(INPUT_PROJECT_KWS), key="message_id")
        skillsheet_map   = read_jsonl_as_dict(str(INPUT_SKILLSHEET), key="message_id")
        email_body_map   = read_jsonl_as_dict(str(INPUT_EMAIL_BODY), key="message_id")
    except Exception as e:
        write_error_log(str(dirs["result"]), e, context=STEP_NAME)
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    logger.info(f"06-11 入力マッチ数: {len(matched_pairs)}")

    refiltered_matched = []
    refiltered_no_match = []
    filtered_keyword_counter: Dict[str, int] = {}

    for pair in matched_pairs:
        project_message_id = pair["project_info"]["message_id"]
        resource_message_id = pair["resource_info"]["message_id"]

        project_rec = project_kws_map.get(project_message_id, {})
        skillsheet_text = (skillsheet_map.get(resource_message_id) or {}).get("skillsheet") or ""
        email_body_text = (email_body_map.get(resource_message_id) or {}).get("body_text") or ""
        combined_text = _normalized_text(skillsheet_text, email_body_text)

        match_detail = pair.get("match_detail", {})
        matched_keywords = match_detail.get("matched_keywords") or []
        matched_phase_keywords = match_detail.get("matched_phase_keywords") or []
        match_reason = match_detail.get("match_reason") or ""

        # NOTE: 06-11がrequired_conditions空をfalseにするため、
        # このブロックは通常到達しない（後方互換のため残置）
        if match_reason == "pass_through_no_required_conditions":
            record = merge_match_info(pair, {"match_required_skills_noise_filtered": True})
            record["match_detail"] = {
                "matched_keywords": matched_keywords,
                "matched_phase_keywords": matched_phase_keywords,
                "matched_in": match_detail.get("matched_in") or [],
                "match_reason": match_reason,
                "filtered_out_keywords": [],
                "noise_filter_reasons": {},
            }
            refiltered_matched.append(record)
            continue

        remaining_keywords: List[str] = []
        filtered_out_keywords: List[str] = []
        filter_reasons: Dict[str, str] = {}

        for keyword in matched_keywords:
            keep, reason = should_keep_keyword(keyword, project_rec, combined_text)
            if keep:
                remaining_keywords.append(keyword)
            else:
                filtered_out_keywords.append(keyword)
                filter_reasons[keyword] = reason
                filtered_keyword_counter[keyword] = filtered_keyword_counter.get(keyword, 0) + 1

        is_match = bool(remaining_keywords)
        record = merge_match_info(pair, {"match_required_skills_noise_filtered": is_match})
        record["match_detail"] = {
            "matched_keywords": remaining_keywords,
            "matched_phase_keywords": matched_phase_keywords,
            "matched_in": match_detail.get("matched_in") or [],
            "match_reason": match_reason,
            "filtered_out_keywords": filtered_out_keywords,
            "noise_filter_reasons": filter_reasons,
        }

        if is_match:
            refiltered_matched.append(record)
        else:
            refiltered_no_match.append(record)
            logger.info(
                f"NO_MATCH_AFTER_NOISE_FILTER: project={project_message_id} resource={resource_message_id}"
                f" filtered_out_keywords={filtered_out_keywords[:5]}"
            )

    write_jsonl(str(OUTPUT_MATCHED), refiltered_matched)
    write_jsonl(str(OUTPUT_NO_MATCHED), refiltered_no_match)

    elapsed = time.time() - start_time
    write_execution_time(
        str(dirs["execution_time"]),
        STEP_NAME,
        elapsed,
        record_count=len(matched_pairs),
    )

    logger.info(
        f"処理完了 入力={len(matched_pairs)} マッチ={len(refiltered_matched)}"
        f" 除外={len(refiltered_no_match)} filtered_keyword_counter={filtered_keyword_counter}"
    )


if __name__ == "__main__":
    main()

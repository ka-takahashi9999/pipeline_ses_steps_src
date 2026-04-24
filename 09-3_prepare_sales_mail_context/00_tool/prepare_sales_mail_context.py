"""
09-3_prepare_sales_mail_context
高確度案件/要員ペアを営業メール作成用の構造化コンテキスト JSONL に統合する。
"""

import re
import sys
import time
import argparse
import unicodedata
from datetime import datetime
from email.utils import parseaddr
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "09-3_prepare_sales_mail_context"
STEP_DIR = Path(__file__).resolve().parents[1]

HIGH_SCORE_BASE_DIR = project_root / "09-2_extract_high_score_mail_display/01_result"
RECHECK_RESULT_DIR = project_root / "08-5_high_score_required_skill_recheck/01_result"
MATCH_SCORE_ORIGINAL_DIR = project_root / "08-4_match_score_sort/01_result"
MAIL_MASTER_PATH = project_root / "01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl"
CLEANED_BODY_PATH = project_root / "01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl"
PROJECT_BUDGET_PATH = project_root / "03-1_extract_project_budget/01_result/extract_project_budget.jsonl"
PROJECT_AGE_PATH = project_root / "03-2_extract_project_age/01_result/extract_project_age.jsonl"
PROJECT_REMOTE_PATH = project_root / "03-3_extract_project_remote/01_result/extract_project_remote.jsonl"
PROJECT_FOREIGN_PATH = project_root / "03-4_extract_project_foreign/01_result/extract_project_foreign.jsonl"
PROJECT_FREELANCE_PATH = project_root / "03-5_extract_project_freelance/01_result/extract_project_freelance.jsonl"
PROJECT_WORKLOAD_PATH = project_root / "03-6_extract_project_workload/01_result/extract_project_workload.jsonl"
PROJECT_VENDOR_PATH = project_root / "03-7_extract_project_vendor_tiers/01_result/extract_project_vendor_tiers.jsonl"
PROJECT_LOCATION_PATH = project_root / "03-10_extract_project_location/01_result/extract_project_location.jsonl"
PROJECT_CONTRACT_PATH = project_root / "03-30_extract_project_contract_type/01_result/contract_type.jsonl"
PROJECT_REQUIRED_SKILLS_PATH = (
    project_root / "03-50_extract_project_required_skills/01_result/extract_project_required_skills.jsonl"
)
PROJECT_REQUIRED_SKILLS_LIST_PATH = (
    project_root / "03-51_extract_project_required_skills_list/01_result/extract_project_required_skills_list.jsonl"
)
RESOURCE_BUDGET_PATH = project_root / "05-1_extract_resource_budget/01_result/extract_resource_budget.jsonl"
RESOURCE_AGE_PATH = project_root / "05-2_extract_resource_age/01_result/extract_resource_age.jsonl"
RESOURCE_REMOTE_PATH = project_root / "05-3_extract_resource_remote/01_result/extract_resource_remote.jsonl"
RESOURCE_FOREIGN_PATH = project_root / "05-4_extract_resource_foreign/01_result/extract_resource_foreign.jsonl"
RESOURCE_FREELANCE_PATH = project_root / "05-5_extract_resource_freelance/01_result/extract_resource_freelance.jsonl"
RESOURCE_WORKLOAD_PATH = project_root / "05-6_extract_resource_workload/01_result/extract_resource_workload.jsonl"
RESOURCE_VENDOR_PATH = project_root / "05-7_extract_resource_vendor_tiers/01_result/extract_resource_vendor_tiers.jsonl"
RESOURCE_SKILL_CATEGORY_PATH = (
    project_root / "05-8_extract_resource_skill_category/01_result/extract_resource_skill_category.jsonl"
)
RESOURCE_PHASE_PATH = project_root / "05-9_extract_resource_phase_category/01_result/extract_resource_phase_category.jsonl"
RESOURCE_LOCATION_PATH = project_root / "05-10_extract_resource_location/01_result/extract_resource_location.jsonl"

PAIR_CONFIGS = [
    {
        "score_band": "100percent",
        "subdir_name": "00_mail_display_format_100percent",
        "match_file": "match_score_sort_100percent.jsonl",
        "file_glob": "mail_display_format_100percent_pair_*.txt",
    },
    {
        "score_band": "80to99percent",
        "subdir_name": "01_mail_display_format_80to99percent",
        "match_file": "match_score_sort_80to99percent.jsonl",
        "file_glob": "mail_display_format_80to99percent_pair_*.txt",
    },
]
RECHECK_INPUT_FILES = [
    "high_score_required_skill_recheck_confirmed.jsonl",
    "high_score_required_skill_recheck_human_review.jsonl",
]

PAIR_TYPE = "project_resource"
OUTPUT_FILE_TEMPLATE = "prepare_sales_mail_context_{date}.jsonl"
MAX_QUOTE_LENGTH = 1600
MAX_SUMMARY_LENGTH = 240
LABEL_DECORATION_PATTERN = r"[■◇●・\-*◎○〇◆▼□]"
LABEL_OPEN_PATTERN = r"(?:【|\[)?"
LABEL_CLOSE_PATTERN = r"(?:】|\])?"

COMPANY_PATTERN = re.compile(r"(株式会社[^\s]+|有限会社[^\s]+|合同会社[^\s]+|[^\s]+株式会社|[^\s]+有限会社|[^\s]+合同会社)")
LABEL_PATTERNS = {
    "project_start_date": ["作業期間", "期間", "契約開始日", "開始日", "開始時期", "入場時期", "参画時期", "開始", "時期"],
    "resource_available_date": ["稼働開始日", "稼動開始日", "稼働", "稼動", "開始日", "参画可能日"],
    "resource_nearest_station": ["最寄り駅", "最寄駅", "最寄り", "最寄"],
    "project_interview_count": ["Web面談", "WEB面談", "面接回数", "面談回数", "面談"],
    "project_settlement": ["精算幅", "精算"],
    "project_notes": ["備考"],
    "resource_notes": ["備考"],
}
EMAIL_PATTERN = re.compile(r"(?i)\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b")
REGION_LIKE_WORDS = ["北海道地方", "東北地方", "関東地方", "中部地方", "関西地方", "近畿地方", "中国地方", "四国地方", "九州地方", "地方", "remote", "リモート"]
REPLY_DIRECTIVE_PATTERNS = [
    re.compile(r"([^\n。]*返信[^\n。]*してください[^\n。]*)"),
    re.compile(r"([^\n。]*ご連絡[^\n。]*ください[^\n。]*)"),
    re.compile(r"([^\n。]*LINEまたはメール[^\n。]*)"),
]
PARALLEL_STATUS_PATTERNS = [
    re.compile(r"([^\n。]*並行[^\n。]*)"),
    re.compile(r"([^\n。]*提案中[^\n。]*)"),
    re.compile(r"([^\n。]*面談調整中[^\n。]*)"),
    re.compile(r"([^\n。]*面談予定[^\n。]*)"),
]
START_DATE_VALUE_PATTERN = re.compile(
    r"(即日(?:~|〜|-|から)?|"
    r"\d{4}[/-]\d{1,2}[/-]\d{1,2}(?:\s*(?:~|〜|-)\s*\S+)?|"
    r"\d{4}年\d{1,2}月(?:\d{1,2}日)?(?:\s*(?:~|〜|-)\s*\S+)?|"
    r"\d{1,2}/\d{1,2}(?:\s*(?:~|〜|-)\s*\S+)?|"
    r"\d{1,2}月(?:\d{1,2}日)?\s*(?:~|〜|-)\s*[^,\n/]+|"
    r"\d{1,2}月(?:頃|以降|中|末|初旬|中旬|下旬)?(?:開始予定)?)"
)
START_DATE_SIGNAL_PATTERN = re.compile(
    r"(期\s*間|契\s*約\s*開\s*始\s*日|開\s*始\s*日|開\s*始\s*時\s*期|入\s*場\s*時\s*期|参\s*画\s*時\s*期|"
    r"即日(?:~|〜|-|から)?|\d{1,2}月(?:\d{1,2}日)?\s*(?:~|〜)|\d{1,2}/\d{1,2}\s*(?:~|〜))"
)
START_DATE_INVALID_LABEL_PATTERN = re.compile(
    r"^(?:条件|備考|面談|精算|場所|勤務地|作業場所|単価|外国籍|商流|募集人数|再委託|作業期間)\s*$"
)
SCHEDULE_VALUE_PATTERN = re.compile(
    r"(即日(?:~|〜|-|から)?|随時|"
    r"\d{4}[/-]\d{1,2}[/-]\d{1,2}(?:\s*(?:~|〜|-)\s*\S+)?|"
    r"\d{4}年\d{1,2}月(?:\d{1,2}日)?(?:\s*(?:~|〜|-|以降|以後)\s*\S+)?|"
    r"\d{1,2}/\d{1,2}(?:\s*(?:~|〜|-|以降|以後)\s*\S+)?|"
    r"\d{1,2}月(?:\d{1,2}日)?(?:\s*(?:~|〜|-|以降|以後)\s*[^,\n/]*)?|"
    r"\d{1,2}月(?:頃|以降|以後|中|末|初旬|中旬|下旬)?(?:開始予定)?)"
)
SETTLEMENT_VALUE_PATTERN = re.compile(r"(\d+h\s*(?:~|〜|-|－)\s*\d+h|固定|なし|あり|上下割|中間割)")
INTERVIEW_COUNT_VALUE_PATTERN = re.compile(
    r"((?:WEB|Web)?\s*\d+\s*(?:[~〜\-－]\s*\d+)?\s*回|\d+\s*(?:[~〜\-－]\s*\d+)?\s*回)"
)
INTERVIEW_SIGNAL_PATTERN = re.compile(
    r"(面\s*談\s*回\s*数|面\s*接\s*回\s*数|(?:WEB|Web)\s*面\s*談|面\s*談\s*[：:]|面\s*談\s*\d+\s*回)"
)


def normalize_text(text: Optional[str]) -> str:
    return unicodedata.normalize("NFKC", text or "").strip()


def first_non_empty(*values: Any) -> Optional[Any]:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        if isinstance(value, list) and len(value) == 0:
            continue
        return value
    return None


def find_latest_dated_dir(base_dir: Path, prefix: str) -> Tuple[Path, str]:
    pattern = re.compile(rf"^{re.escape(prefix)}(\d{{8}})$")
    candidates: List[Tuple[str, Path]] = []
    if not base_dir.exists():
        raise FileNotFoundError(f"入力ベースディレクトリが存在しません: {base_dir}")
    for path in base_dir.iterdir():
        if not path.is_dir():
            continue
        match = pattern.match(path.name)
        if match:
            candidates.append((match.group(1), path))
    if not candidates:
        raise FileNotFoundError(f"{prefix}YYYYMMDD ディレクトリが存在しません: {base_dir}")
    date_str, latest_dir = max(candidates, key=lambda item: item[0])
    return latest_dir, date_str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-date", help="処理対象日付 YYYYMMDD")
    return parser.parse_args()


def resolve_target_date(target_date: Optional[str]) -> Optional[str]:
    if target_date is None:
        return None
    if not re.fullmatch(r"\d{8}", target_date):
        raise ValueError(f"--target-date は YYYYMMDD 形式で指定してください: {target_date}")
    return target_date


def resolve_input_dir(base_dir: Path, prefix: str, target_date: Optional[str]) -> Tuple[Path, str, str]:
    if target_date:
        target_dir = base_dir / f"{prefix}{target_date}"
        if not target_dir.exists():
            raise FileNotFoundError(f"対象日付の入力ディレクトリが存在しません: {target_dir}")
        return target_dir, target_date, "target-date"
    latest_dir, latest_date = find_latest_dated_dir(base_dir, prefix)
    return latest_dir, latest_date, "latest"


def ensure_required_inputs(paths: List[Path]) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError("必須入力が不足しています: " + ", ".join(missing))


def parse_sequence(file_name: str) -> int:
    match = re.search(r"_pair_(\d+)\.txt$", file_name)
    if not match:
        raise ValueError(f"pair連番を抽出できません: {file_name}")
    return int(match.group(1))


def make_pair_key(record: Dict[str, Any]) -> Tuple[str, str]:
    project_mid = normalize_text(record.get("project_info", {}).get("message_id"))
    resource_mid = normalize_text(record.get("resource_info", {}).get("message_id"))
    return project_mid, resource_mid


def load_rechecked_records(logger) -> Dict[Tuple[str, str], Dict[str, Any]]:
    records_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for filename in RECHECK_INPUT_FILES:
        path = RECHECK_RESULT_DIR / filename
        records = read_jsonl_as_list(str(path))
        loaded_count = 0
        for record in records:
            if record.get("status") == "no_match":
                continue
            key = make_pair_key(record)
            if not all(key):
                logger.warn(f"08-5 record message_id不足のためskip: {filename}")
                continue
            records_by_key[key] = record
            loaded_count += 1
        logger.info(f"08-5採用入力: {filename} {loaded_count}件")
    logger.info(f"08-5 confirmed/human_review 採用候補: {len(records_by_key)}件")
    return records_by_key


def load_pair_records(input_dir: Path, logger) -> List[Dict[str, Any]]:
    pair_records: List[Dict[str, Any]] = []
    rechecked_records = load_rechecked_records(logger)
    skipped_not_rechecked_count = 0
    for config in PAIR_CONFIGS:
        subdir = input_dir / config["subdir_name"]
        if not subdir.exists():
            raise FileNotFoundError(f"高確度pairサブディレクトリが存在しません: {subdir}")
        match_records = read_jsonl_as_list(str(MATCH_SCORE_ORIGINAL_DIR / config["match_file"]))
        if len(match_records) == 1 and match_records[0].get("status") == "no_match":
            match_records = []
        txt_files = sorted(subdir.glob(config["file_glob"]))
        logger.info(f"{config['score_band']}: txt={len(txt_files)}件 / match={len(match_records)}件")
        band_output_count = 0
        band_skip_count = 0
        for txt_file in txt_files:
            seq = parse_sequence(txt_file.name)
            if seq < 1 or seq > len(match_records):
                raise IndexError(
                    f"pairファイルと08-4結果の対応が取れません: {txt_file.name} seq={seq} match_count={len(match_records)}"
                )
            original_pair = match_records[seq - 1]
            key = make_pair_key(original_pair)
            rechecked_pair = rechecked_records.get(key)
            if rechecked_pair is None:
                skipped_not_rechecked_count += 1
                band_skip_count += 1
                continue
            project_mid, resource_mid = key
            score_band = normalize_text(rechecked_pair.get("source_score_band")) or config["score_band"]
            pair_records.append(
                {
                    "pair_file_name": txt_file.name,
                    "score_band": score_band,
                    "pair_type": PAIR_TYPE,
                    "project_message_id": project_mid,
                    "resource_message_id": resource_mid,
                    "match_record": rechecked_pair,
                }
            )
            band_output_count += 1
        logger.info(
            f"{config['score_band']}: 09-3採用={band_output_count}件 / "
            f"08-5未採用skip={band_skip_count}件"
        )
    logger.info(f"08-5 confirmed/human_reviewに存在しないため除外: {skipped_not_rechecked_count}件")
    return pair_records


def parse_addresses(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        items = value
    else:
        items = [value]
    results: List[str] = []
    for item in items:
        text = normalize_text(str(item))
        if not text:
            continue
        for part in re.split(r"[;,]", text):
            candidate = normalize_text(part)
            if candidate:
                results.append(candidate)
    return results


def parse_name_and_email(value: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    text = normalize_text(value)
    if not text:
        return None, None
    name, email = parseaddr(text)
    name = normalize_text(name).strip("\"'") or None
    email = normalize_text(email).lower() or None
    if email is None and "@" in text and "<" not in text:
        email = text.lower()
    return name, email


def infer_company_from_text(*values: Optional[str]) -> Optional[str]:
    for value in values:
        text = normalize_text(value)
        if not text:
            continue
        match = COMPANY_PATTERN.search(text)
        if match:
            return normalize_text(match.group(1))
    return None


def infer_company_from_email(email: Optional[str]) -> Optional[str]:
    if not email or "@" not in email:
        return None
    domain = email.split("@", 1)[1]
    label = domain.split(".")[0]
    if not label:
        return None
    return label


def pick_sender_company(header_name: Optional[str], cleaned_body: str, from_field: Optional[str], email: Optional[str]) -> Optional[str]:
    lines = [line.strip() for line in cleaned_body.splitlines() if line.strip()]
    signature_candidates = list(reversed(lines[-10:]))
    for line in signature_candidates:
        company = infer_company_from_text(line)
        if company:
            return company
    return first_non_empty(
        infer_company_from_text(header_name),
        infer_company_from_text(from_field),
        infer_company_from_email(email),
    )


def clean_sender_name(name: Optional[str], company: Optional[str]) -> Optional[str]:
    value = normalize_text(name)
    if not value:
        return None
    if company:
        value = value.replace(company, "").strip()
    value = re.sub(r"^(SES営業|営業|担当|ご担当者|ご担当者様)", "", value).strip()
    return value or name


def quote_body(body: str) -> str:
    text = normalize_text(body)
    if len(text) <= MAX_QUOTE_LENGTH:
        return text
    return text[:MAX_QUOTE_LENGTH].rstrip() + "..."


def build_flexible_label_pattern(label: str) -> str:
    chars = [re.escape(char) for char in normalize_text(label) if char not in {" ", "　"}]
    return r"[\s　]*".join(chars)


def is_generic_section_header(line: str) -> bool:
    text = normalize_text(line)
    if not text:
        return False
    compact = re.sub(r"[\s　]+", "", text)
    if re.match(r"^[■◇●・\-*]+.+$", compact):
        return True
    if re.match(r"^【.{1,20}】$", compact):
        return True
    return False


def extract_label_value(text: str, labels: List[str]) -> Optional[str]:
    lines = text.splitlines()
    label_patterns = [
        re.compile(
            rf"^(?:{LABEL_DECORATION_PATTERN}+\s*)?{LABEL_OPEN_PATTERN}\s*(?:{build_flexible_label_pattern(label)})\s*{LABEL_CLOSE_PATTERN}(?:\s*{LABEL_DECORATION_PATTERN}+)?\s*(?:[：:]?\s*(.*))?$"
        )
        for label in labels
    ]
    for idx, raw_line in enumerate(lines):
        line = normalize_text(raw_line)
        if not line:
            continue
        for pattern in label_patterns:
            match = pattern.match(line)
            if not match:
                continue
            inline_value = normalize_text(match.group(1))
            if inline_value and re.fullmatch(r"[■◇●・\-*]+", inline_value):
                inline_value = ""
            if inline_value:
                return inline_value
            for next_line in lines[idx + 1 :]:
                candidate = normalize_text(next_line)
                if not candidate:
                    continue
                if is_generic_section_header(candidate):
                    break
                return candidate
    return None


def extract_label_value_anywhere(text: str, labels: List[str]) -> Optional[str]:
    patterns = [
        re.compile(
            rf"{LABEL_OPEN_PATTERN}\s*(?:{build_flexible_label_pattern(label)})\s*{LABEL_CLOSE_PATTERN}(?:\s*{LABEL_DECORATION_PATTERN}+)?\s*(?:[：:]?\s*(.+))$"
        )
        for label in labels
    ]
    for raw_line in text.splitlines():
        line = normalize_text(raw_line)
        if not line:
            continue
        for pattern in patterns:
            match = pattern.search(line)
            if not match:
                continue
            value = normalize_text(match.group(1))
            if value and not re.fullmatch(rf"{LABEL_DECORATION_PATTERN}+", value):
                return value
    return None


def clean_nearest_station_value(value: Optional[str]) -> Optional[str]:
    text = normalize_text(value)
    if not text:
        return None
    text = re.sub(r"^(?:り[：:]|寄[：:]|[：:])\s*", "", text)
    text = re.sub(rf"^(?:{LABEL_DECORATION_PATTERN}+\s*)+", "", text)
    return text or None


def extract_resource_nearest_station(text: str) -> Optional[str]:
    for candidate in [
        extract_label_value(text, LABEL_PATTERNS["resource_nearest_station"]),
    ]:
        normalized = clean_nearest_station_value(candidate)
        if normalized:
            return normalized

    labels = LABEL_PATTERNS["resource_nearest_station"]
    for raw_line in text.splitlines():
        line = normalize_text(raw_line)
        if not line:
            continue
        for label in labels:
            pattern = re.compile(
                rf"{LABEL_OPEN_PATTERN}\s*(?:{build_flexible_label_pattern(label)})\s*{LABEL_CLOSE_PATTERN}"
                rf"(?:\s*{LABEL_DECORATION_PATTERN}+)?(?:\s*[：:]\s*|\s+|\s*{LABEL_DECORATION_PATTERN}+\s+)(.+)$"
            )
            match = pattern.search(line)
            if not match:
                continue
            value = clean_nearest_station_value(match.group(1))
            if value and not re.fullmatch(rf"{LABEL_DECORATION_PATTERN}+", value):
                return value
    return None


def extract_note_block(text: str, label: str) -> Optional[str]:
    lines = text.splitlines()
    collected: List[str] = []
    active = False
    label_pattern = re.compile(
        rf"^(?:{LABEL_DECORATION_PATTERN}+\s*)?{LABEL_OPEN_PATTERN}\s*(?:{build_flexible_label_pattern(label)})\s*{LABEL_CLOSE_PATTERN}(?:\s*{LABEL_DECORATION_PATTERN}+)?\s*(?:[：:]?\s*(.*))?$"
    )
    for raw_line in lines:
        line = normalize_text(raw_line)
        if not line and active:
            break
        if not active:
            match = label_pattern.match(line)
            if match:
                value = normalize_text(match.group(1))
                if value and re.fullmatch(r"[■◇●・\-*]+", value):
                    value = ""
                if value:
                    collected.append(value)
                active = True
            continue
        if is_generic_section_header(line):
            break
        collected.append(line)
    note = "\n".join(part for part in collected if part).strip()
    return note or None


def fallback_text_with_source(primary: Optional[Any], primary_source: str, fallback: Optional[Any], fallback_source: str) -> Tuple[Optional[Any], str]:
    if primary is None or (isinstance(primary, str) and primary.strip() == "") or (isinstance(primary, list) and not primary):
        return fallback, fallback_source
    return primary, primary_source


def clean_start_date_value(value: Optional[str]) -> Optional[str]:
    text = normalize_text(value)
    if not text:
        return None
    text = text.strip(" :：")
    if not text or re.fullmatch(r"[:：\s]+", text):
        return None
    text = re.sub(r"\s*[:：]\s*\d+\s*名.*$", "", text)
    text = text.strip(" :：")
    if not text or re.fullmatch(r"[:：\s]+", text):
        return None
    match = START_DATE_VALUE_PATTERN.search(text)
    if match:
        return normalize_text(match.group(1))
    compact = re.sub(r"[\s:：]", "", text)
    compact = re.sub(LABEL_DECORATION_PATTERN, "", compact)
    if not compact:
        return None
    if START_DATE_INVALID_LABEL_PATTERN.fullmatch(compact):
        return None
    if START_DATE_SIGNAL_PATTERN.search(text):
        return text
    return None


def strip_label_artifacts(text: Optional[str], prefixes: List[str]) -> str:
    value = normalize_text(text)
    if not value:
        return ""
    value = re.sub(rf"^(?:{LABEL_DECORATION_PATTERN}+\s*)+", "", value)
    for prefix in prefixes:
        value = re.sub(rf"^{build_flexible_label_pattern(prefix)}\s*[:：]?\s*", "", value)
    value = re.sub(rf"(?:{LABEL_DECORATION_PATTERN}+\s*)+$", "", value)
    return value.strip(" :：")


def normalize_schedule_like_value(value: Optional[str], prefixes: List[str], invalid_labels: List[str]) -> Optional[str]:
    text = strip_label_artifacts(value, prefixes)
    if not text:
        return None
    match = SCHEDULE_VALUE_PATTERN.search(text)
    if match:
        return normalize_text(match.group(1))
    compact = re.sub(r"[\s:：]", "", text)
    compact = re.sub(LABEL_DECORATION_PATTERN, "", compact)
    if not compact:
        return None
    if compact in invalid_labels:
        return None
    if any(token in text for token in ["即日", "随時", "以降", "以後"]) and any(char.isdigit() for char in text):
        return text
    return None


def normalize_settlement_value(value: Optional[str]) -> Optional[str]:
    text = strip_label_artifacts(value, ["精算条件", "精算幅", "精算", "条件"])
    if not text:
        return None
    match = SETTLEMENT_VALUE_PATTERN.search(text)
    if match:
        return normalize_text(match.group(1))
    compact = re.sub(r"[\s:：]", "", text)
    compact = re.sub(LABEL_DECORATION_PATTERN, "", compact)
    if compact in {"条件", "精算", "精算条件", "精算幅"}:
        return None
    return None


def normalize_foreign_restriction_value(value: Optional[str]) -> Optional[str]:
    text = strip_label_artifacts(value, ["外国籍条件", "外国籍"])
    if not text:
        return None
    compact = re.sub(r"[\s:：]", "", text)
    compact = re.sub(LABEL_DECORATION_PATTERN, "", compact)
    if compact in {"不可", "foreignng"}:
        return "外国籍不可"
    if compact in {"可", "foreignok"}:
        return "外国籍可"
    if compact in {"外国籍不可", "外国籍可"}:
        return compact
    return None


def normalize_parallel_status_value(value: Optional[str]) -> Optional[str]:
    text = strip_label_artifacts(value, ["並行状況", "並行"])
    if not text:
        return None
    compact = re.sub(r"[\s:：]", "", text)
    compact = re.sub(LABEL_DECORATION_PATTERN, "", compact)
    if compact in {"提案中", "面談調整中", "面談予定"}:
        return "並行" + compact
    if compact in {"並行提案中", "並行面談調整中", "並行面談予定"}:
        return compact
    if compact == "並行":
        return None
    return None


def has_resource_available_date_signal(cleaned_body: str) -> bool:
    return any(token in cleaned_body for token in ["稼働開始日", "稼動開始日", "稼働", "稼動", "開始日", "参画可能日"])


def has_project_settlement_signal(cleaned_body: str) -> bool:
    return any(token in cleaned_body for token in ["精算幅", "精算条件", "精算"])


def has_project_foreign_signal(cleaned_body: str) -> bool:
    return "外国籍" in normalize_text(cleaned_body)


def has_parallel_status_signal(*bodies: str) -> bool:
    return any("並行" in normalize_text(body) for body in bodies)


def clean_interview_count_value(value: Optional[str]) -> Optional[str]:
    text = normalize_text(value)
    if not text:
        return None
    match = INTERVIEW_COUNT_VALUE_PATTERN.search(text)
    if match:
        return normalize_text(match.group(1))
    return text


def extract_project_start_date_from_body(cleaned_body: str) -> Optional[str]:
    for candidate in [
        extract_label_value(cleaned_body, LABEL_PATTERNS["project_start_date"]),
        extract_label_value_anywhere(cleaned_body, LABEL_PATTERNS["project_start_date"]),
    ]:
        normalized = clean_start_date_value(candidate)
        if normalized:
            return normalized
    for raw_line in cleaned_body.splitlines():
        line = normalize_text(raw_line)
        if not line:
            continue
        if any(label in line for label in ["期間", "契約開始日", "開始日", "開始時期", "入場時期", "参画時期"]):
            normalized = clean_start_date_value(line)
            if normalized:
                return normalized
    return None


def extract_project_interview_count_from_body(cleaned_body: str) -> Optional[str]:
    for candidate in [
        extract_label_value(cleaned_body, LABEL_PATTERNS["project_interview_count"]),
        extract_label_value_anywhere(cleaned_body, LABEL_PATTERNS["project_interview_count"]),
    ]:
        normalized = clean_interview_count_value(candidate)
        if normalized:
            return normalized
    for raw_line in cleaned_body.splitlines():
        line = normalize_text(raw_line)
        if not line:
            continue
        if INTERVIEW_SIGNAL_PATTERN.search(line):
            normalized = clean_interview_count_value(line)
            if normalized:
                return normalized
    return None


def has_project_start_date_signal(cleaned_body: str) -> bool:
    for raw_line in cleaned_body.splitlines():
        line = normalize_text(raw_line)
        if not line:
            continue
        if START_DATE_SIGNAL_PATTERN.search(line):
            return True
    return False


def has_project_interview_signal(cleaned_body: str) -> bool:
    for raw_line in cleaned_body.splitlines():
        line = normalize_text(raw_line)
        if not line:
            continue
        if INTERVIEW_SIGNAL_PATTERN.search(line):
            return True
    return False


def format_settlement(workload_record: Optional[Dict[str, Any]], cleaned_body: str) -> Tuple[Optional[str], str]:
    if workload_record:
        raw = normalize_settlement_value(workload_record.get("workload_raw"))
        if raw:
            return raw, normalize_text(workload_record.get("workload_max_source")) or "03-6_extract_project_workload"
    fallback = normalize_settlement_value(extract_label_value(cleaned_body, LABEL_PATTERNS["project_settlement"]))
    if fallback:
        return fallback, "01-4_cleanup_email_text"
    return None, "unavailable"


def format_start_date(cleaned_body: str, record: Optional[Dict[str, Any]] = None) -> Tuple[Optional[str], str]:
    structured_value = clean_start_date_value((record or {}).get("project_start_date") or (record or {}).get("start_date"))
    if structured_value:
        return structured_value, normalize_text((record or {}).get("project_start_date_source") or (record or {}).get("start_date_source")) or "structured"
    fallback = extract_project_start_date_from_body(cleaned_body)
    if fallback:
        return fallback, "01-4_cleanup_email_text"
    return None, "unavailable"


def format_available_date(cleaned_body: str) -> Tuple[Optional[str], str]:
    fallback = normalize_schedule_like_value(
        extract_label_value(cleaned_body, LABEL_PATTERNS["resource_available_date"]),
        LABEL_PATTERNS["resource_available_date"],
        ["日", "開始日", "稼働", "稼動", "参画可能日", "開始"],
    )
    if fallback:
        return fallback, "01-4_cleanup_email_text"
    return None, "unavailable"


def format_interview_count(cleaned_body: str, record: Optional[Dict[str, Any]] = None) -> Tuple[Optional[str], str]:
    structured_value = normalize_text((record or {}).get("project_interview_count") or (record or {}).get("interview_count"))
    if structured_value:
        return structured_value, (
            normalize_text((record or {}).get("project_interview_count_source") or (record or {}).get("interview_count_source"))
            or "structured"
        )
    fallback = extract_project_interview_count_from_body(cleaned_body)
    if fallback:
        return fallback, "01-4_cleanup_email_text"
    return None, "unavailable"


def summarize_resource_skills(skill_record: Optional[Dict[str, Any]], phase_record: Optional[Dict[str, Any]]) -> Tuple[Optional[str], str]:
    skills = skill_record.get("skills", []) if skill_record else []
    phases = phase_record.get("phases", []) if phase_record else []
    segments: List[str] = []
    if skills:
        segments.append("スキル: " + ", ".join(skills[:8]))
    if phases:
        segments.append("工程: " + ", ".join(phases[:4]))
    summary = " / ".join(segments).strip()
    if summary:
        if len(summary) > MAX_SUMMARY_LENGTH:
            summary = summary[:MAX_SUMMARY_LENGTH].rstrip() + "..."
        return summary, "05-8_extract_resource_skill_category+05-9_extract_resource_phase_category"
    return None, "unavailable"


def derive_foreign_restriction(project_foreign: Optional[Dict[str, Any]]) -> Tuple[Optional[str], str]:
    if not project_foreign:
        return None, "unavailable"
    raw = normalize_foreign_restriction_value(project_foreign.get("foreign_nationality_raw"))
    if raw:
        return raw, normalize_text(project_foreign.get("foreign_nationality_source")) or "03-4_extract_project_foreign"
    if project_foreign.get("foreign_nationality_ok") is True:
        return "外国籍可", normalize_text(project_foreign.get("foreign_nationality_source")) or "03-4_extract_project_foreign"
    if project_foreign.get("foreign_nationality_ok") is False:
        return "外国籍不可", normalize_text(project_foreign.get("foreign_nationality_source")) or "03-4_extract_project_foreign"
    return None, "unavailable"


def derive_nationality_note(resource_foreign: Optional[Dict[str, Any]]) -> Tuple[Optional[str], str]:
    if not resource_foreign:
        return None, "unavailable"
    raw = normalize_text(resource_foreign.get("nationality_raw"))
    if raw:
        return raw, normalize_text(resource_foreign.get("nationality_source")) or "05-4_extract_resource_foreign"
    nationality = normalize_text(resource_foreign.get("nationality"))
    if nationality == "japanese":
        return "日本国籍想定", normalize_text(resource_foreign.get("nationality_source")) or "05-4_extract_resource_foreign"
    if nationality:
        return nationality, normalize_text(resource_foreign.get("nationality_source")) or "05-4_extract_resource_foreign"
    return None, "unavailable"


def derive_affiliation_label(resource_vendor: Optional[Dict[str, Any]], resource_freelance: Optional[Dict[str, Any]]) -> Tuple[Optional[str], str]:
    if not resource_vendor:
        return None, "unavailable"
    vendor_flow = resource_vendor.get("vendor_flow")
    if vendor_flow is None:
        return None, "unavailable"
    if not isinstance(vendor_flow, int):
        return None, "unavailable"
    external_depth = vendor_flow - 10 if vendor_flow < 20 else vendor_flow - 19
    if external_depth < 0:
        external_depth = 0
    employment = normalize_text((resource_freelance or {}).get("employment_type"))
    if employment == "freelance":
        base_label = "個人事業主"
    else:
        base_label = "社員"
    if vendor_flow < 20:
        if base_label == "個人事業主":
            return "弊社個人事業主", "05-7_extract_resource_vendor_tiers+05-5_extract_resource_freelance"
        return "弊社社員", "05-7_extract_resource_vendor_tiers+05-5_extract_resource_freelance"
    if external_depth <= 1:
        prefix = "1社先"
    elif external_depth == 2:
        prefix = "2社先"
    else:
        prefix = f"{external_depth}社先"
    return prefix + base_label, "05-7_extract_resource_vendor_tiers+05-5_extract_resource_freelance"


def derive_parallel_status(project_body: str, resource_body: str) -> Tuple[str, str]:
    for text in [resource_body, project_body]:
        for pattern in PARALLEL_STATUS_PATTERNS:
            match = pattern.search(text)
            if match:
                normalized = normalize_parallel_status_value(match.group(1))
                if normalized:
                    return normalized, "01-4_cleanup_email_text"
    return "並行提案中", "default"


def extract_reply_directive(text: str) -> Optional[str]:
    for pattern in REPLY_DIRECTIVE_PATTERNS:
        match = pattern.search(text)
        if match:
            return normalize_text(match.group(1))
    return None


def extract_emails(text: str) -> List[str]:
    return [normalize_text(match.group(0)).lower() for match in EMAIL_PATTERN.finditer(text)]


def is_region_like_station(value: Optional[str]) -> bool:
    text = normalize_text(value)
    if not text:
        return False
    compact = text.lower()
    return any(word.lower() in compact for word in REGION_LIKE_WORDS)


def extract_reply_targets(
    body: str,
    header_reply_to: Optional[str],
    header_cc: List[str],
) -> Dict[str, Any]:
    reply_to_candidates: List[str] = []
    cc_candidates: List[str] = []
    directive_lines: List[str] = []
    source = "header"
    ambiguous = False
    signal_detected = False
    body_explicit_detected = False

    for raw_line in body.splitlines():
        line = normalize_text(raw_line)
        if not line:
            continue
        emails = extract_emails(line)

        if re.search(r"(?i)\b(mail\s*to|to)\s*[:：]", line):
            signal_detected = True
            directive_lines.append(line)
            if emails:
                reply_to_candidates.extend(emails)
                body_explicit_detected = True
                source = "body_explicit"
            else:
                ambiguous = True
        if re.search(r"(?i)\b(cc)\s*[:：]", line):
            signal_detected = True
            directive_lines.append(line)
            if emails:
                cc_candidates.extend(emails)
                body_explicit_detected = True
                source = "body_explicit"
            else:
                ambiguous = True

        to_jp = re.search(r"Toを(.+?)宛", line, re.IGNORECASE)
        if to_jp:
            signal_detected = True
            directive_lines.append(line)
            extracted = extract_emails(to_jp.group(1))
            if extracted:
                reply_to_candidates.extend(extracted)
                body_explicit_detected = True
                source = "body_explicit"
            else:
                ambiguous = True

        cc_jp = re.search(r"Ccに(.+?)(?:を入れて|宛|送付|追加)", line, re.IGNORECASE)
        if cc_jp:
            signal_detected = True
            directive_lines.append(line)
            extracted = extract_emails(cc_jp.group(1))
            if extracted:
                cc_candidates.extend(extracted)
                body_explicit_detected = True
                source = "body_explicit"
            else:
                ambiguous = True

        if any(phrase in line for phrase in ["こちらにご連絡", "本メール宛にご返信", "本メール宛に返信", "返信はこの担当まで", "LINEまたはメール"]):
            signal_detected = True
            directive_lines.append(line)
            if emails:
                reply_to_candidates.extend(emails)
                body_explicit_detected = True
                source = "body_explicit"
            else:
                ambiguous = True

    if body_explicit_detected:
        source = "body_explicit"
    elif not signal_detected and header_reply_to:
        reply_to_candidates = [normalize_text(header_reply_to)]
        source = "header"
    if not cc_candidates and header_cc:
        cc_candidates = [normalize_text(value) for value in header_cc if normalize_text(value)]

    deduped_to = []
    deduped_cc = []
    seen = set()
    for value in reply_to_candidates:
        key = normalize_text(value).lower()
        if key and key not in seen:
            seen.add(key)
            deduped_to.append(normalize_text(value))
    seen = set(value.lower() for value in deduped_to)
    for value in cc_candidates:
        key = normalize_text(value).lower()
        if key and key not in seen:
            seen.add(key)
            deduped_cc.append(normalize_text(value))

    reply_target_source = source if body_explicit_detected or deduped_to or deduped_cc else "unavailable"
    if not signal_detected and deduped_to and reply_target_source == "unavailable":
        reply_target_source = "header"

    return {
        "reply_to_candidates": deduped_to,
        "cc_candidates": deduped_cc,
        "reply_target_source": reply_target_source,
        "reply_directive_lines": directive_lines,
        "reply_target_ambiguous": ambiguous,
        "reply_signal_detected": signal_detected,
    }


def build_mail_side_context(mail_record: Dict[str, Any], cleaned_record: Dict[str, Any]) -> Dict[str, Any]:
    from_field = mail_record.get("from")
    reply_to_field = mail_record.get("reply_to")
    header_name, header_email = parse_name_and_email(from_field)
    reply_name, reply_email = parse_name_and_email(reply_to_field)
    cleaned_body = normalize_text(cleaned_record.get("body_text"))
    sender_email = first_non_empty(reply_email, header_email)
    sender_company = pick_sender_company(header_name, cleaned_body, from_field, sender_email)
    sender_name = clean_sender_name(first_non_empty(reply_name, header_name), sender_company)
    cc_addresses = parse_addresses(mail_record.get("cc"))
    reply_directive = extract_reply_directive(cleaned_body)
    reply_targets = extract_reply_targets(cleaned_body, first_non_empty(reply_to_field, sender_email), cc_addresses)
    return {
        "subject": normalize_text(mail_record.get("subject")),
        "sender_name": sender_name,
        "sender_email": sender_email,
        "sender_company": sender_company,
        "reply_to": first_non_empty(reply_to_field, sender_email),
        "cc": cc_addresses,
        "body": cleaned_body,
        "quoted_body": quote_body(cleaned_body),
        "reply_directive": reply_directive,
        "reply_to_candidates": reply_targets["reply_to_candidates"],
        "cc_candidates": reply_targets["cc_candidates"],
        "reply_target_source": reply_targets["reply_target_source"],
        "reply_directive_lines": reply_targets["reply_directive_lines"],
        "reply_target_ambiguous": reply_targets["reply_target_ambiguous"],
        "reply_signal_detected": reply_targets["reply_signal_detected"],
    }


def build_record(
    pair_meta: Dict[str, Any],
    mail_master: Dict[str, Dict[str, Any]],
    cleaned_map: Dict[str, Dict[str, Any]],
    extracted_maps: Dict[str, Dict[str, Dict[str, Any]]],
) -> Dict[str, Any]:
    project_mid = pair_meta["project_message_id"]
    resource_mid = pair_meta["resource_message_id"]
    match_record = pair_meta.get("match_record") or {}
    recheck_info = match_record.get("recheck_info") if isinstance(match_record, dict) else {}
    if not isinstance(recheck_info, dict):
        recheck_info = {}
    required_skill_checks = match_record.get("required_skill_checks") if isinstance(match_record, dict) else []
    if not isinstance(required_skill_checks, list):
        required_skill_checks = []
    category_match = match_record.get("category_match", "unclear") if isinstance(match_record, dict) else "unclear"
    category_note = match_record.get("category_note", "") if isinstance(match_record, dict) else ""
    score_band = normalize_text(match_record.get("source_score_band")) or pair_meta["score_band"]

    project_mail = mail_master.get(project_mid, {})
    resource_mail = mail_master.get(resource_mid, {})
    project_cleaned = cleaned_map.get(project_mid, {})
    resource_cleaned = cleaned_map.get(resource_mid, {})
    project_side = build_mail_side_context(project_mail, project_cleaned)
    resource_side = build_mail_side_context(resource_mail, resource_cleaned)

    project_budget = extracted_maps["project_budget"].get(project_mid)
    project_age = extracted_maps["project_age"].get(project_mid)
    project_remote = extracted_maps["project_remote"].get(project_mid)
    project_foreign = extracted_maps["project_foreign"].get(project_mid)
    project_freelance = extracted_maps["project_freelance"].get(project_mid)
    project_workload = extracted_maps["project_workload"].get(project_mid)
    project_vendor = extracted_maps["project_vendor"].get(project_mid)
    project_location = extracted_maps["project_location"].get(project_mid)
    project_contract = extracted_maps["project_contract"].get(project_mid)
    project_required_skills = extracted_maps["project_required_skills"].get(project_mid)
    project_required_skills_list = extracted_maps["project_required_skills_list"].get(project_mid)

    resource_budget = extracted_maps["resource_budget"].get(resource_mid)
    resource_age = extracted_maps["resource_age"].get(resource_mid)
    resource_remote = extracted_maps["resource_remote"].get(resource_mid)
    resource_foreign = extracted_maps["resource_foreign"].get(resource_mid)
    resource_freelance = extracted_maps["resource_freelance"].get(resource_mid)
    resource_workload = extracted_maps["resource_workload"].get(resource_mid)
    resource_vendor = extracted_maps["resource_vendor"].get(resource_mid)
    resource_skill_category = extracted_maps["resource_skill_category"].get(resource_mid)
    resource_phase = extracted_maps["resource_phase"].get(resource_mid)
    resource_location = extracted_maps["resource_location"].get(resource_mid)

    project_start_date, project_start_date_source = format_start_date(project_side["body"], project_contract)
    resource_available_date, resource_available_date_source = format_available_date(resource_side["body"])
    project_settlement, project_settlement_source = format_settlement(project_workload, project_side["body"])
    project_notes = first_non_empty(
        extract_note_block(project_side["body"], "備考"),
        project_side["reply_directive"],
    )
    project_notes_source = "01-4_cleanup_email_text" if project_notes else "unavailable"
    resource_notes = first_non_empty(
        extract_note_block(resource_side["body"], "備考"),
        resource_side["reply_directive"],
    )
    resource_notes_source = "01-4_cleanup_email_text" if resource_notes else "unavailable"
    project_interview_count, project_interview_count_source = format_interview_count(project_side["body"])
    resource_nearest_station = extract_resource_nearest_station(resource_side["body"])
    if is_region_like_station(resource_nearest_station):
        resource_nearest_station = None
    resource_nearest_station_source = "01-4_cleanup_email_text" if resource_nearest_station else "unavailable"
    project_location_value = normalize_text((project_location or {}).get("location"))
    if project_location_value in {"", "unknown"}:
        project_location_value = extract_label_value(project_side["body"], ["作業場所", "場所", "勤務地"])
        project_location_source = "01-4_cleanup_email_text" if project_location_value else "unavailable"
    else:
        project_location_source = normalize_text((project_location or {}).get("location_source")) or "03-10_extract_project_location"
    project_foreign_restriction, project_foreign_restriction_source = derive_foreign_restriction(project_foreign)
    resource_nationality_note, resource_nationality_note_source = derive_nationality_note(resource_foreign)
    suggested_affiliation_label, suggested_affiliation_label_source = derive_affiliation_label(resource_vendor, resource_freelance)
    suggested_parallel_status, suggested_parallel_status_source = derive_parallel_status(project_side["body"], resource_side["body"])
    resource_skill_summary, resource_skill_summary_source = summarize_resource_skills(resource_skill_category, resource_phase)

    review_notes: List[str] = []
    if not project_mid:
        review_notes.append("project_message_idを特定できません")
    if not resource_mid:
        review_notes.append("resource_message_idを特定できません")
    if not project_side["subject"]:
        review_notes.append("案件subjectが欠落しています")
    if not resource_side["subject"]:
        review_notes.append("要員subjectが欠落しています")
    if not project_side["quoted_body"]:
        review_notes.append("案件引用本文が空です")
    if not resource_side["quoted_body"]:
        review_notes.append("要員引用本文が空です")
    if suggested_affiliation_label is None:
        review_notes.append("所属表現候補を作成できませんでした")
    if project_side["reply_signal_detected"] and not project_side["reply_to_candidates"]:
        review_notes.append("案件側本文に返信先指示シグナルがありますがTO候補を抽出できませんでした")
    if resource_side["reply_signal_detected"] and not resource_side["reply_to_candidates"]:
        review_notes.append("要員側本文に返信先指示シグナルがありますがTO候補を抽出できませんでした")
    if project_side["reply_target_ambiguous"]:
        review_notes.append("案件側の返信先指示が曖昧です")
    if resource_side["reply_target_ambiguous"]:
        review_notes.append("要員側の返信先指示が曖昧です")
    if any("Cc" in line or "CC" in line for line in project_side["reply_directive_lines"]) and not project_side["cc_candidates"]:
        review_notes.append("案件側本文にCC指示がありますがCC候補を抽出できませんでした")
    if any("Cc" in line or "CC" in line for line in resource_side["reply_directive_lines"]) and not resource_side["cc_candidates"]:
        review_notes.append("要員側本文にCC指示がありますがCC候補を抽出できませんでした")
    if resource_nearest_station is None and re.search(r"最寄|最寄駅|最寄り駅", resource_side["body"]):
        review_notes.append("要員本文に最寄駅シグナルがありますが駅名を抽出できませんでした")
    if is_region_like_station(resource_nearest_station):
        review_notes.append("resource_nearest_stationが地域名またはremote表現です")
    if project_start_date is None and has_project_start_date_signal(project_side["body"]):
        review_notes.append("案件本文に開始時期シグナルがありますがproject_start_dateを抽出できませんでした")
    if resource_available_date is None and has_resource_available_date_signal(resource_side["body"]):
        review_notes.append("要員本文に稼働開始シグナルがありますがresource_available_dateを抽出できませんでした")
    if project_interview_count is None and has_project_interview_signal(project_side["body"]):
        review_notes.append("案件本文に面談シグナルがありますがproject_interview_countを抽出できませんでした")
    if project_settlement is None and has_project_settlement_signal(project_side["body"]):
        review_notes.append("案件本文に精算シグナルがありますがproject_settlementを抽出できませんでした")
    if project_foreign_restriction is None and has_project_foreign_signal(project_side["body"]):
        review_notes.append("案件本文に外国籍シグナルがありますがproject_foreign_restrictionを抽出できませんでした")
    if suggested_parallel_status_source != "default" and not suggested_parallel_status:
        review_notes.append("本文に並行シグナルがありますがsuggested_parallel_statusを抽出できませんでした")
    if category_match == "mismatch":
        note_detail = f"（{category_note}）" if category_note else ""
        review_notes.append(f"技術領域の不一致があります{note_detail}")

    return {
        "pair_file_name": pair_meta["pair_file_name"],
        "score_band": score_band,
        "pair_type": pair_meta["pair_type"],
        "project_message_id": project_mid,
        "resource_message_id": resource_mid,
        "required_skill_recheck_status": recheck_info.get("recheck_status"),
        "required_skill_recheck_info": recheck_info,
        "required_skill_checks": required_skill_checks,
        "project_subject": project_side["subject"],
        "resource_subject": resource_side["subject"],
        "project_sender_name": project_side["sender_name"],
        "project_sender_email": project_side["sender_email"],
        "project_sender_company": project_side["sender_company"],
        "resource_sender_name": resource_side["sender_name"],
        "resource_sender_email": resource_side["sender_email"],
        "resource_sender_company": resource_side["sender_company"],
        "project_reply_to": project_side["reply_to"],
        "project_cc": project_side["cc"],
        "project_reply_to_candidates": project_side["reply_to_candidates"],
        "project_cc_candidates": project_side["cc_candidates"],
        "project_reply_target_source": project_side["reply_target_source"],
        "project_reply_directive_lines": project_side["reply_directive_lines"],
        "resource_reply_to": resource_side["reply_to"],
        "resource_cc": resource_side["cc"],
        "resource_reply_to_candidates": resource_side["reply_to_candidates"],
        "resource_cc_candidates": resource_side["cc_candidates"],
        "resource_reply_target_source": resource_side["reply_target_source"],
        "resource_reply_directive_lines": resource_side["reply_directive_lines"],
        "project_budget": (project_budget or {}).get("unit_price"),
        "project_budget_source": (
            normalize_text(((project_budget or {}).get("unit_price_sub_infor") or {}).get("method"))
            or "03-1_extract_project_budget"
            if project_budget else "unavailable"
        ),
        "resource_desired_unit_price": (resource_budget or {}).get("desired_unit_price"),
        "resource_desired_unit_price_source": (
            "05-1_extract_resource_budget" if resource_budget else "unavailable"
        ),
        "resource_vendor_flow": (resource_vendor or {}).get("vendor_flow"),
        "resource_vendor_flow_source": "05-7_extract_resource_vendor_tiers" if resource_vendor else "unavailable",
        "suggested_affiliation_label": suggested_affiliation_label,
        "suggested_affiliation_label_source": suggested_affiliation_label_source,
        "project_start_date": project_start_date,
        "project_start_date_source": project_start_date_source,
        "resource_available_date": resource_available_date,
        "resource_available_date_source": resource_available_date_source,
        "project_location": project_location_value,
        "project_location_source": project_location_source,
        "resource_nearest_station": resource_nearest_station,
        "resource_nearest_station_source": resource_nearest_station_source,
        "project_remote_type": (project_remote or {}).get("remote_type"),
        "project_remote_type_source": (
            normalize_text((project_remote or {}).get("remote_type_source")) or "03-3_extract_project_remote"
            if project_remote else "unavailable"
        ),
        "resource_remote_preference": (resource_remote or {}).get("remote_preference"),
        "resource_remote_preference_source": (
            normalize_text((resource_remote or {}).get("remote_source")) or "05-3_extract_resource_remote"
            if resource_remote else "unavailable"
        ),
        "project_required_skills": [item.get("skill") for item in (project_required_skills or {}).get("required_skills", []) if item.get("skill")],
        "project_required_skills_source": "03-50_extract_project_required_skills" if project_required_skills else "unavailable",
        "project_preferred_skills": [item.get("skill") for item in (project_required_skills or {}).get("optional_skills", []) if item.get("skill")],
        "project_preferred_skills_source": "03-50_extract_project_required_skills" if project_required_skills else "unavailable",
        "project_required_skill_keywords": (project_required_skills_list or {}).get("required_skill_keywords", []),
        "project_optional_skill_keywords": (project_required_skills_list or {}).get("optional_skill_keywords", []),
        "resource_skills": (resource_skill_category or {}).get("skills", []),
        "resource_skills_source": "05-8_extract_resource_skill_category" if resource_skill_category else "unavailable",
        "resource_skill_summary": resource_skill_summary,
        "resource_skill_summary_source": resource_skill_summary_source,
        "project_age_limit": (project_age or {}).get("age_max"),
        "project_age_limit_source": (
            normalize_text((project_age or {}).get("age_max_source")) or "03-2_extract_project_age"
            if project_age else "unavailable"
        ),
        "resource_age": (resource_age or {}).get("current_age"),
        "resource_age_source": (
            normalize_text((resource_age or {}).get("current_age_source")) or "05-2_extract_resource_age"
            if resource_age else "unavailable"
        ),
        "project_foreign_restriction": project_foreign_restriction,
        "project_foreign_restriction_source": project_foreign_restriction_source,
        "resource_nationality_note": resource_nationality_note,
        "resource_nationality_note_source": resource_nationality_note_source,
        "project_interview_count": project_interview_count,
        "project_interview_count_source": project_interview_count_source,
        "project_settlement": project_settlement,
        "project_settlement_source": project_settlement_source,
        "project_notes": project_notes,
        "project_notes_source": project_notes_source,
        "resource_notes": resource_notes,
        "resource_notes_source": resource_notes_source,
        "suggested_parallel_status": suggested_parallel_status,
        "suggested_parallel_status_source": suggested_parallel_status_source,
        "quoted_project_mail_body": project_side["quoted_body"],
        "quoted_project_mail_body_source": "01-4_cleanup_email_text" if project_side["quoted_body"] else "unavailable",
        "quoted_resource_mail_body": resource_side["quoted_body"],
        "quoted_resource_mail_body_source": "01-4_cleanup_email_text" if resource_side["quoted_body"] else "unavailable",
        "project_freelance_ok": (project_freelance or {}).get("freelance_ok"),
        "project_freelance_ok_source": (
            normalize_text((project_freelance or {}).get("freelance_source")) or "03-5_extract_project_freelance"
            if project_freelance else "unavailable"
        ),
        "resource_employment_type": (resource_freelance or {}).get("employment_type"),
        "resource_employment_type_source": (
            normalize_text((resource_freelance or {}).get("employment_type_source")) or "05-5_extract_resource_freelance"
            if resource_freelance else "unavailable"
        ),
        "project_commercial_flow_level": (project_vendor or {}).get("commercial_flow_level"),
        "project_commercial_flow_source": (
            normalize_text((project_vendor or {}).get("commercial_flow_source")) or "03-7_extract_project_vendor_tiers"
            if project_vendor else "unavailable"
        ),
        "project_contract_type": (project_contract or {}).get("contract_type"),
        "project_contract_type_source": (
            normalize_text((project_contract or {}).get("contract_type_source")) or "03-30_extract_project_contract_type"
            if project_contract else "unavailable"
        ),
        "resource_workload_raw": (resource_workload or {}).get("workload_raw"),
        "resource_workload_source": (
            normalize_text((resource_workload or {}).get("workload_max_source")) or "05-6_extract_resource_workload"
            if resource_workload else "unavailable"
        ),
        "category_match": category_match,
        "category_note": category_note,
        "needs_human_review": len(review_notes) > 0,
        "review_notes": review_notes,
    }


def main() -> None:
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()
    args = parse_args()

    required_files = [
        MAIL_MASTER_PATH,
        CLEANED_BODY_PATH,
        PROJECT_BUDGET_PATH,
        PROJECT_AGE_PATH,
        PROJECT_REMOTE_PATH,
        PROJECT_FOREIGN_PATH,
        PROJECT_FREELANCE_PATH,
        PROJECT_WORKLOAD_PATH,
        PROJECT_VENDOR_PATH,
        PROJECT_LOCATION_PATH,
        PROJECT_CONTRACT_PATH,
        PROJECT_REQUIRED_SKILLS_PATH,
        PROJECT_REQUIRED_SKILLS_LIST_PATH,
        RESOURCE_BUDGET_PATH,
        RESOURCE_AGE_PATH,
        RESOURCE_REMOTE_PATH,
        RESOURCE_FOREIGN_PATH,
        RESOURCE_FREELANCE_PATH,
        RESOURCE_WORKLOAD_PATH,
        RESOURCE_VENDOR_PATH,
        RESOURCE_SKILL_CATEGORY_PATH,
        RESOURCE_PHASE_PATH,
        RESOURCE_LOCATION_PATH,
        RECHECK_RESULT_DIR / "high_score_required_skill_recheck_confirmed.jsonl",
        RECHECK_RESULT_DIR / "high_score_required_skill_recheck_human_review.jsonl",
        MATCH_SCORE_ORIGINAL_DIR / "match_score_sort_100percent.jsonl",
        MATCH_SCORE_ORIGINAL_DIR / "match_score_sort_80to99percent.jsonl",
    ]

    try:
        ensure_required_inputs(required_files)
        target_date = resolve_target_date(args.target_date)
        input_dir, input_date, resolve_mode = resolve_input_dir(HIGH_SCORE_BASE_DIR, "mail_display_extract_", target_date)
        logger.info(f"input resolve mode: {resolve_mode}")
        logger.info(f"target date: {input_date}")
        logger.info(f"入力ディレクトリ: {input_dir}")

        pair_records = load_pair_records(input_dir, logger)
        mail_master = read_jsonl_as_dict(str(MAIL_MASTER_PATH))
        cleaned_map = read_jsonl_as_dict(str(CLEANED_BODY_PATH))
        extracted_maps = {
            "project_budget": read_jsonl_as_dict(str(PROJECT_BUDGET_PATH)),
            "project_age": read_jsonl_as_dict(str(PROJECT_AGE_PATH)),
            "project_remote": read_jsonl_as_dict(str(PROJECT_REMOTE_PATH)),
            "project_foreign": read_jsonl_as_dict(str(PROJECT_FOREIGN_PATH)),
            "project_freelance": read_jsonl_as_dict(str(PROJECT_FREELANCE_PATH)),
            "project_workload": read_jsonl_as_dict(str(PROJECT_WORKLOAD_PATH)),
            "project_vendor": read_jsonl_as_dict(str(PROJECT_VENDOR_PATH)),
            "project_location": read_jsonl_as_dict(str(PROJECT_LOCATION_PATH)),
            "project_contract": read_jsonl_as_dict(str(PROJECT_CONTRACT_PATH)),
            "project_required_skills": read_jsonl_as_dict(str(PROJECT_REQUIRED_SKILLS_PATH)),
            "project_required_skills_list": read_jsonl_as_dict(str(PROJECT_REQUIRED_SKILLS_LIST_PATH)),
            "resource_budget": read_jsonl_as_dict(str(RESOURCE_BUDGET_PATH)),
            "resource_age": read_jsonl_as_dict(str(RESOURCE_AGE_PATH)),
            "resource_remote": read_jsonl_as_dict(str(RESOURCE_REMOTE_PATH)),
            "resource_foreign": read_jsonl_as_dict(str(RESOURCE_FOREIGN_PATH)),
            "resource_freelance": read_jsonl_as_dict(str(RESOURCE_FREELANCE_PATH)),
            "resource_workload": read_jsonl_as_dict(str(RESOURCE_WORKLOAD_PATH)),
            "resource_vendor": read_jsonl_as_dict(str(RESOURCE_VENDOR_PATH)),
            "resource_skill_category": read_jsonl_as_dict(str(RESOURCE_SKILL_CATEGORY_PATH)),
            "resource_phase": read_jsonl_as_dict(str(RESOURCE_PHASE_PATH)),
            "resource_location": read_jsonl_as_dict(str(RESOURCE_LOCATION_PATH)),
        }

        output_records: List[Dict[str, Any]] = []
        for pair_meta in pair_records:
            record = build_record(pair_meta, mail_master, cleaned_map, extracted_maps)
            output_records.append(record)

        output_path = STEP_DIR / "01_result" / OUTPUT_FILE_TEMPLATE.format(date=input_date)
        write_jsonl(str(output_path), output_records)
        elapsed = time.time() - start_time
        write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, len(output_records))
        logger.ok(f"JSONL出力完了: {output_path} ({len(output_records)}件)")

    except Exception as error:
        write_error_log(str(dirs["result"]), error, context=STEP_NAME)
        logger.error(f"処理失敗: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()

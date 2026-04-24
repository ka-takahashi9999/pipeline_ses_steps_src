#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 03-30: 案件メール本文から契約形態をルールベースで抽出する

判定方針:
  1. デフォルトは outsourcing
  2. 派遣または準委任のように準委任で契約可能なら quasi_mandate
  3. 明確に派遣必須と読める場合は dispatch
  4. 労働者派遣事業許可番号などの免許情報だけでは dispatch にしない

出力スキーマ:
  {
    "message_id": "...",
    "contract_type": "dispatch" | "quasi_mandate" | "outsourcing",
    "contract_type_source": "extracted" | "default",
    "contract_type_raw": "根拠断片" | null
  }
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

STEP_NAME = "03-30_extract_project_contract_type"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_PROJECTS = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl"
)
OUTPUT_RESULT = "contract_type.jsonl"

VALID_CONTRACT_TYPES = {"dispatch", "quasi_mandate", "outsourcing"}
VALID_SOURCES = {"extracted", "default"}

EXCLUDED_LINE_PATTERNS = [
    re.compile(r"労働者派遣事業許可番号"),
    re.compile(r"派遣事業許可番号"),
    re.compile(r"一般労働者派遣事業"),
    re.compile(r"有料職業紹介事業"),
]

QUASI_MANDATE_PATTERNS = [
    re.compile(r"派遣\s*(?:または|or|/|／)\s*準委任"),
    re.compile(r"準委任\s*(?:または|or|/|／)\s*派遣"),
    re.compile(r"契約(?:形態)?\s*[:：]\s*派遣\s*(?:または|or|/|／)\s*準委任"),
    re.compile(r"契約(?:形態)?\s*[:：]\s*準委任\s*(?:または|or|/|／)\s*派遣"),
]

DISPATCH_PATTERNS = [
    re.compile(r"契約形態\s*[:：]\s*派遣のみ"),
    re.compile(r"契約\s*[:：]\s*派遣(?:契約)?"),
    re.compile(r"※\s*派遣契約"),
    re.compile(r"派遣契約となります"),
    re.compile(r"派遣契約を結んでいただきます"),
    re.compile(r"商流\s*[:：].*派遣契約"),
    re.compile(r"商流\s*[:：].*?\(派遣契約\)"),
    re.compile(r"商流\s*[:：].*?\(派遣\)"),
    re.compile(r"派遣のみ"),
    re.compile(r"派遣契約"),
]

DISPATCH_REQUIRED_CONTEXT_PATTERNS = [
    re.compile(r"契約形態\s*[:：]\s*派遣"),
    re.compile(r"契約\s*[:：]\s*派遣"),
]


def _n(text: str) -> str:
    return unicodedata.normalize("NFKC", text or "")


def _iter_candidate_lines(text: str) -> List[str]:
    return [line.strip() for line in _n(text).splitlines() if line.strip()]


def _is_excluded_line(line: str) -> bool:
    return any(pattern.search(line) for pattern in EXCLUDED_LINE_PATTERNS)


def _find_first_match(lines: List[str], patterns: List[re.Pattern]) -> Optional[str]:
    for line in lines:
        if _is_excluded_line(line):
            continue
        for pattern in patterns:
            match = pattern.search(line)
            if match:
                return line
    return None


def detect_contract_type(body_text: str) -> Tuple[str, str, Optional[str]]:
    lines = _iter_candidate_lines(body_text)

    quasi_line = _find_first_match(lines, QUASI_MANDATE_PATTERNS)
    if quasi_line:
        return "quasi_mandate", "extracted", quasi_line

    dispatch_line = _find_first_match(lines, DISPATCH_PATTERNS)
    if dispatch_line:
        return "dispatch", "extracted", dispatch_line

    dispatch_context_line = _find_first_match(lines, DISPATCH_REQUIRED_CONTEXT_PATTERNS)
    if dispatch_context_line:
        return "dispatch", "extracted", dispatch_context_line

    for line in lines:
        if _is_excluded_line(line):
            continue
        if "準委任契約" in line or re.search(r"契約形態\s*[:：]\s*準委任", line):
            return "quasi_mandate", "extracted", line
        if "準委任" in line and "契約" in line:
            return "quasi_mandate", "extracted", line

    return "outsourcing", "default", None


def build_record(message_id: str, body_text: str) -> dict:
    contract_type, source, raw = detect_contract_type(body_text)
    return {
        "message_id": message_id,
        "contract_type": contract_type,
        "contract_type_source": source,
        "contract_type_raw": raw,
    }


def validate_record(record: dict) -> None:
    if not record.get("message_id"):
        raise ValueError(f"message_id が空です: {record}")
    if record.get("contract_type") not in VALID_CONTRACT_TYPES:
        raise ValueError(f"contract_type が不正です: {record}")
    if record.get("contract_type_source") not in VALID_SOURCES:
        raise ValueError(f"contract_type_source が不正です: {record}")
    raw = record.get("contract_type_raw")
    if raw is not None and not isinstance(raw, str):
        raise ValueError(f"contract_type_raw は文字列またはnullのみ許可です: {record}")


def main() -> None:
    start = time.time()
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = dirs["result"]

    for path in [INPUT_PROJECTS, INPUT_CLEANED]:
        if not Path(path).exists():
            logger.error(f"入力ファイルが存在しません: {path}")
            sys.exit(1)

    try:
        project_records = read_jsonl_as_list(INPUT_PROJECTS)
        cleaned_map = read_jsonl_as_dict(INPUT_CLEANED)
    except Exception as e:
        write_error_log(str(result_dir), e, "入力ファイル読み込みエラー")
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    output_records: List[dict] = []

    try:
        for project in project_records:
            message_id = str(project.get("message_id", ""))
            body_text = (cleaned_map.get(message_id) or {}).get("body_text", "")
            record = build_record(message_id, body_text)
            validate_record(record)
            output_records.append(record)
            logger.info(
                f"{message_id} → contract_type={record['contract_type']} "
                f"source={record['contract_type_source']}",
                message_id=message_id,
            )
    except Exception as e:
        write_error_log(str(result_dir), e, "レコード処理エラー")
        logger.error(f"レコード処理エラー: {e}")
        sys.exit(1)

    if len(output_records) != len(project_records):
        error = ValueError(
            f"出力件数不一致: input={len(project_records)} output={len(output_records)}"
        )
        write_error_log(str(result_dir), error, "件数不一致")
        logger.error(str(error))
        sys.exit(1)

    try:
        write_jsonl(str(result_dir / OUTPUT_RESULT), output_records)
    except Exception as e:
        write_error_log(str(result_dir), e, "出力ファイル書き込みエラー")
        logger.error(f"出力ファイル書き込みエラー: {e}")
        sys.exit(1)

    elapsed = time.time() - start
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, len(output_records))
    logger.ok(f"Step完了: 入力={len(project_records)}件 / 出力={len(output_records)}件")


if __name__ == "__main__":
    main()

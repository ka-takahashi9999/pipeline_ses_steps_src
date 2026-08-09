#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04-2_normalize_skillsheets_text: raw skillsheet text の決定的な軽量化

入力:
  - 04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl

出力:
  - 04-2_normalize_skillsheets_text/01_result/normalize_skillsheets_text.jsonl
  - 04-2_normalize_skillsheets_text/02_confirm/result/normalize_skillsheets_text_summary.txt
  - 04-2_normalize_skillsheets_text/02_confirm/result/normalize_skillsheets_text_detail.tsv
  - 04-2_normalize_skillsheets_text/99_execution_time/normalize_skillsheets_text.txt

LLM は使わない。文字数上限による truncate もしない。
"""

import csv
import re
import statistics
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple

STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "04-2_normalize_skillsheets_text"
SOURCE_STEP = "04-1_fetch_skillsheets_text"

INPUT_JSONL = (
    PROJECT_ROOT
    / "04-1_fetch_skillsheets_text"
    / "01_result"
    / "fetch_skillsheets_text.jsonl"
)
OUTPUT_JSONL = STEP_DIR / "01_result" / "normalize_skillsheets_text.jsonl"
CONFIRM_RESULT_DIR = STEP_DIR / "02_confirm" / "result"
SUMMARY_TXT = CONFIRM_RESULT_DIR / "normalize_skillsheets_text_summary.txt"
DETAIL_TSV = CONFIRM_RESULT_DIR / "normalize_skillsheets_text_detail.tsv"

logger = get_logger(STEP_NAME)

DROP_SHEET_EXACT_NAMES = {
    "データ",
    "マスタ",
    "選択肢",
    "プルダウン",
    "凡例",
    "入力説明",
}
DROP_SHEET_PATTERNS = (
    re.compile(r"^【非表示】マスタ[1-3]$"),
    re.compile(r"^マスタ[1-3]$"),
)
TEMPLATE_KEYWORD = "テンプレート"
SHEET_HEADING_RE = re.compile(r"^===\s*シート:\s*(.*?)\s*===\s*$", re.MULTILINE)

INFO_CHAR_RE = re.compile(r"[A-Za-z0-9Ａ-Ｚａ-ｚ０-９一-龯ぁ-んァ-ヶｦ-ﾟ]")
LOW_INFO_LINE_RE = re.compile(
    r"^[\s●○◎△▲▽▼□■◇◆・･\-_=＝ー―─━│┃|+＋*＊/／\\＼.,，。:：;；()[\]（）【】<>＜＞]+$"
)
EXCEL_ERROR_TOKEN_RE = re.compile(
    r"#(?:VALUE!|NUM!|REF!|DIV/0!|N/A|NAME\?|NULL!)"
)
BIRTH_YEAR_AGE_TABLE_RE = re.compile(
    r"^\s*(\d{4})\s*\|\s*\d{1,3}(?:\s*\|\s*\d{1,3})?\s*$"
)

HEADER_LINES = {
    "No. 期間 企業名 プロジェクト名 OS 言語 ツール",
    "期間 業務内容 開発環境 役割",
    "期間 業務内容 開発環境等 役割等",
    "開始 終了 案件名 工程 言語 DB OS",
}


def _space_key(value: str) -> str:
    """空白差分を無視した完全一致判定用キー。"""
    return re.sub(r"\s+", " ", value.strip())


HEADER_KEYS = {_space_key(line) for line in HEADER_LINES}

BUSINESS_LINE_PREFIX_RE = re.compile(r"^\s*[・\-●■]")
BUSINESS_KEYWORDS = (
    "設計",
    "開発",
    "製造",
    "実装",
    "テスト",
    "試験",
    "保守",
    "運用",
    "構築",
    "要件定義",
    "基本設計",
    "詳細設計",
    "単体テスト",
    "結合テスト",
    "総合テスト",
    "システムテスト",
    "移行",
    "レビュー",
    "管理",
    "調査",
    "分析",
    "改善",
    "問い合わせ",
)
BUSINESS_SECTION_KEYWORDS = (
    "担当フェーズ",
    "担当工程",
    "業務内容",
    "作業内容",
)
TECH_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9.+#/\- ]{0,24}$")


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _should_drop_sheet(sheet_name: str) -> bool:
    normalized = _space_key(sheet_name)
    if normalized in DROP_SHEET_EXACT_NAMES:
        return True
    return any(pattern.fullmatch(normalized) for pattern in DROP_SHEET_PATTERNS)


def _normalize_newlines_and_spaces(text: str) -> Tuple[str, Dict[str, int]]:
    before = text
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.rstrip() for line in text.split("\n")]
    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text, {
        "newline_or_trailing_space_changed": int(text != before),
    }


def _remove_low_value_sheet_blocks(text: str) -> Tuple[str, Dict[str, Any]]:
    matches = list(SHEET_HEADING_RE.finditer(text))
    if not matches:
        return text, {
            "removed_sheet_blocks": 0,
            "removed_sheet_chars": 0,
            "removed_sheet_names": [],
            "template_sheet_blocks": 0,
        }

    parts: List[str] = []
    pos = 0
    removed_blocks = 0
    removed_chars = 0
    removed_names: List[str] = []
    template_blocks = 0

    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        sheet_name = match.group(1).strip()

        parts.append(text[pos:start])
        block = text[start:end]
        if TEMPLATE_KEYWORD in sheet_name:
            template_blocks += 1

        if _should_drop_sheet(sheet_name):
            removed_blocks += 1
            removed_chars += len(block)
            removed_names.append(sheet_name)
        else:
            parts.append(block)
        pos = end

    parts.append(text[pos:])
    return "".join(parts), {
        "removed_sheet_blocks": removed_blocks,
        "removed_sheet_chars": removed_chars,
        "removed_sheet_names": removed_names,
        "template_sheet_blocks": template_blocks,
    }


def _is_low_info_symbol_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if INFO_CHAR_RE.search(stripped):
        return False
    return bool(LOW_INFO_LINE_RE.fullmatch(stripped))


def _remove_low_info_lines(text: str) -> Tuple[str, Dict[str, int]]:
    kept: List[str] = []
    removed = 0
    removed_chars = 0
    for line in text.split("\n"):
        if _is_low_info_symbol_line(line):
            removed += 1
            removed_chars += len(line) + 1
            continue
        kept.append(line)
    return "\n".join(kept), {
        "removed_low_info_lines": removed,
        "removed_low_info_line_chars": removed_chars,
    }


def _remove_excel_error_tokens(text: str) -> Tuple[str, Dict[str, int]]:
    kept: List[str] = []
    removed_tokens = 0
    removed_chars = 0
    removed_empty_lines = 0

    for line in text.split("\n"):
        matches = list(EXCEL_ERROR_TOKEN_RE.finditer(line))
        if not matches:
            kept.append(line)
            continue

        removed_tokens += len(matches)
        removed_chars += sum(len(match.group(0)) for match in matches)
        cleaned_line = EXCEL_ERROR_TOKEN_RE.sub("", line).rstrip()
        if cleaned_line.strip():
            kept.append(cleaned_line)
        else:
            removed_empty_lines += 1

    return "\n".join(kept), {
        "removed_excel_error_tokens": removed_tokens,
        "removed_excel_error_token_chars": removed_chars,
        "removed_excel_error_token_empty_lines": removed_empty_lines,
    }


def _is_birth_year_age_table_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if re.search(r"[A-Za-zＡ-Ｚａ-ｚ一-龯ぁ-んァ-ヶｦ-ﾟ]", stripped):
        return False
    match = BIRTH_YEAR_AGE_TABLE_RE.fullmatch(stripped)
    if not match:
        return False
    birth_year = int(match.group(1))
    return 1900 <= birth_year <= 2029


def _remove_birth_year_age_table_lines(text: str) -> Tuple[str, Dict[str, int]]:
    kept: List[str] = []
    removed = 0
    removed_chars = 0

    for line in text.split("\n"):
        if _is_birth_year_age_table_line(line):
            removed += 1
            removed_chars += len(line) + 1
            continue
        kept.append(line)

    return "\n".join(kept), {
        "removed_birth_year_age_table_lines": removed,
        "removed_birth_year_age_table_line_chars": removed_chars,
    }


def _remove_repeated_headers(text: str) -> Tuple[str, Dict[str, int]]:
    kept: List[str] = []
    seen_headers: set = set()
    removed = 0
    removed_chars = 0

    for line in text.split("\n"):
        key = _space_key(line)
        if key in HEADER_KEYS:
            if key in seen_headers:
                removed += 1
                removed_chars += len(line) + 1
                continue
            seen_headers.add(key)
        kept.append(line)

    return "\n".join(kept), {
        "removed_repeated_header_lines": removed,
        "removed_repeated_header_chars": removed_chars,
    }


def _is_business_content_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if _is_low_info_symbol_line(stripped):
        return False
    if BUSINESS_LINE_PREFIX_RE.match(stripped) and INFO_CHAR_RE.search(stripped):
        return True
    if any(keyword in stripped for keyword in BUSINESS_SECTION_KEYWORDS):
        return True
    if any(keyword in stripped for keyword in BUSINESS_KEYWORDS):
        return True
    if TECH_NAME_RE.fullmatch(stripped) and re.search(r"[A-Za-z]", stripped):
        return True
    return False


def _remove_duplicate_lines(text: str) -> Tuple[str, Dict[str, int]]:
    lines = text.split("\n")
    kept: List[str] = []
    previous_line = None
    removed = 0
    removed_chars = 0

    for line in lines:
        stripped = line.strip()
        if stripped and len(stripped) >= 8:
            if previous_line == line:
                removed += 1
                removed_chars += len(line) + 1
                previous_line = line
                continue
        kept.append(line)
        previous_line = line

    return "\n".join(kept), {
        "removed_duplicate_lines": removed,
        "removed_duplicate_line_chars": removed_chars,
    }


def normalize_skillsheet(raw_text: str) -> Tuple[str, List[str], Dict[str, Any]]:
    cleanup_flags: List[str] = []
    cleanup_stats: Dict[str, Any] = {}

    text, stats = _normalize_newlines_and_spaces(raw_text)
    cleanup_stats.update(stats)
    if stats["newline_or_trailing_space_changed"]:
        cleanup_flags.append("normalized_whitespace")

    text, stats = _remove_low_value_sheet_blocks(text)
    cleanup_stats.update(stats)
    if stats["removed_sheet_blocks"]:
        cleanup_flags.append("removed_low_value_sheet_blocks")
    if stats["template_sheet_blocks"]:
        cleanup_flags.append("template_sheet_present")

    text, stats = _remove_excel_error_tokens(text)
    cleanup_stats.update(stats)
    if stats["removed_excel_error_tokens"]:
        cleanup_flags.append("removed_excel_error_tokens")

    text, stats = _remove_birth_year_age_table_lines(text)
    cleanup_stats.update(stats)
    if stats["removed_birth_year_age_table_lines"]:
        cleanup_flags.append("removed_birth_year_age_table_lines")

    text, stats = _remove_low_info_lines(text)
    cleanup_stats.update(stats)
    if stats["removed_low_info_lines"]:
        cleanup_flags.append("removed_low_info_lines")

    text, stats = _remove_repeated_headers(text)
    cleanup_stats.update(stats)
    if stats["removed_repeated_header_lines"]:
        cleanup_flags.append("removed_repeated_header_lines")

    text, stats = _remove_duplicate_lines(text)
    cleanup_stats.update(stats)
    if stats["removed_duplicate_lines"]:
        cleanup_flags.append("removed_duplicate_lines")

    return text, cleanup_flags, cleanup_stats


def build_record(record: Dict[str, Any]) -> Dict[str, Any]:
    success = record.get("success") is True
    raw_text = _to_text(record.get("skillsheet"))
    cleanup_flags: List[str] = []
    cleanup_stats: Dict[str, Any] = {}

    if not success:
        clean_text = raw_text
        cleanup_flags.append("input_success_false")
        raw_char_count = 0
        clean_char_count = 0
    elif not raw_text.strip():
        clean_text = ""
        cleanup_flags.append("empty_skillsheet")
        raw_char_count = 0
        clean_char_count = 0
    else:
        raw_char_count = len(raw_text)
        clean_text, cleanup_flags, cleanup_stats = normalize_skillsheet(raw_text)
        clean_char_count = len(clean_text)

    reduction_char_count = max(raw_char_count - clean_char_count, 0)
    reduction_rate = (
        round(reduction_char_count / raw_char_count, 6) if raw_char_count > 0 else 0.0
    )

    if success and raw_text.strip() and clean_char_count == 0:
        cleanup_flags.append("cleaned_to_empty")

    return {
        "message_id": record.get("message_id"),
        "success": success,
        "source": record.get("source"),
        "urls": record.get("urls"),
        "skillsheet": clean_text,
        "raw_char_count": raw_char_count,
        "clean_char_count": clean_char_count,
        "reduction_char_count": reduction_char_count,
        "reduction_rate": reduction_rate,
        "cleanup_flags": cleanup_flags,
        "cleanup_stats": cleanup_stats,
        "source_step": SOURCE_STEP,
    }


def _median(values: List[int]) -> float:
    return float(statistics.median(values)) if values else 0.0


def _avg(values: List[int]) -> float:
    return float(sum(values) / len(values)) if values else 0.0


def _summary_lines(records: List[Dict[str, Any]]) -> List[str]:
    raw_counts = [int(r.get("raw_char_count") or 0) for r in records]
    clean_counts = [int(r.get("clean_char_count") or 0) for r in records]
    success_true = sum(1 for r in records if r.get("success") is True)
    success_false = len(records) - success_true

    flag_counts: Counter = Counter()
    for record in records:
        for flag in record.get("cleanup_flags") or []:
            flag_counts[str(flag)] += 1

    lines = [
        f"input_count: {len(records)}",
        f"output_count: {len(records)}",
        f"success_true_count: {success_true}",
        f"success_false_count: {success_false}",
        f"raw_char_count_total: {sum(raw_counts)}",
        f"raw_char_count_average: {_avg(raw_counts):.2f}",
        f"raw_char_count_median: {_median(raw_counts):.2f}",
        f"raw_char_count_max: {max(raw_counts) if raw_counts else 0}",
        f"clean_char_count_total: {sum(clean_counts)}",
        f"clean_char_count_average: {_avg(clean_counts):.2f}",
        f"clean_char_count_median: {_median(clean_counts):.2f}",
        f"clean_char_count_max: {max(clean_counts) if clean_counts else 0}",
        f"raw_20000_or_more_count: {sum(1 for c in raw_counts if c >= 20000)}",
        f"clean_20000_or_more_count: {sum(1 for c in clean_counts if c >= 20000)}",
        f"raw_50000_or_more_count: {sum(1 for c in raw_counts if c >= 50000)}",
        f"clean_50000_or_more_count: {sum(1 for c in clean_counts if c >= 50000)}",
        "cleanup_flags_counts:",
    ]
    if flag_counts:
        for flag, count in sorted(flag_counts.items()):
            lines.append(f"  {flag}: {count}")
    else:
        lines.append("  none: 0")
    return lines


def write_confirm_outputs(records: List[Dict[str, Any]]) -> None:
    CONFIRM_RESULT_DIR.mkdir(parents=True, exist_ok=True)
    SUMMARY_TXT.write_text("\n".join(_summary_lines(records)) + "\n", encoding="utf-8")

    with DETAIL_TSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        writer.writerow(
            [
                "message_id",
                "success",
                "source",
                "raw_char_count",
                "clean_char_count",
                "reduction_char_count",
                "reduction_rate",
                "cleanup_flags",
                "urls",
            ]
        )
        for record in records:
            writer.writerow(
                [
                    record.get("message_id"),
                    record.get("success"),
                    record.get("source"),
                    record.get("raw_char_count"),
                    record.get("clean_char_count"),
                    record.get("reduction_char_count"),
                    record.get("reduction_rate"),
                    ",".join(record.get("cleanup_flags") or []),
                    record.get("urls"),
                ]
            )


def main() -> None:
    start = time.time()
    dirs = ensure_result_dirs(str(STEP_DIR))
    CONFIRM_RESULT_DIR.mkdir(parents=True, exist_ok=True)

    if not INPUT_JSONL.exists():
        logger.error(f"入力ファイルが存在しません: {INPUT_JSONL}")
        sys.exit(1)

    try:
        input_records = read_jsonl_as_list(str(INPUT_JSONL))
        output_records = [build_record(record) for record in input_records]
        write_jsonl(str(OUTPUT_JSONL), output_records)
        write_confirm_outputs(output_records)
    except Exception as e:
        write_error_log(str(dirs["result"]), e, "04-2 normalize 実行エラー")
        logger.error(f"04-2 normalize 実行エラー: {e}")
        sys.exit(1)

    elapsed = time.time() - start
    write_execution_time(
        str(dirs["execution_time"]),
        "normalize_skillsheets_text",
        elapsed,
        len(output_records),
    )
    logger.ok(
        f"Step完了: 入力={len(input_records)}件 / 出力={len(output_records)}件 / "
        f"summary={SUMMARY_TXT}"
    )


if __name__ == "__main__":
    main()

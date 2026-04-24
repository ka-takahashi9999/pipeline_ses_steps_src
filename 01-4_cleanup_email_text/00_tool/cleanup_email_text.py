#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 01-4: メール本文クリーニングスクリプト

LLMトークン削減のため、メール本文から不要文字列を除去する。

除去対象（cleanup_rules.txt で管理）:
  ① 定型挨拶文（部分一致する行を行ごと除去）
  ② 署名（〒マーク等を含む行の先頭から文末まで除去）
  ③ 区切り線（正規表現で一致する行を除去）

入力①（本文参照用）:
  01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl

入力②（処理対象のmessage_id）:
  01-3_remove_individual_email/01_result/remove_individual_emails_raw.jsonl

出力: 01_result/cleanup_email_text_emails_raw.jsonl
  {"message_id": "...", "body_text": "<クリーニング後本文>"}
"""

import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Set, Tuple

# common モジュールのパス解決
_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "01-4_cleanup_email_text"
logger = get_logger(STEP_NAME)

INPUT_MASTER = str(
    _PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)
INPUT_PREV = str(
    _PROJECT_ROOT / "01-3_remove_individual_email" / "01_result" / "remove_individual_emails_raw.jsonl"
)
CLEANUP_RULES_PATH = str(_STEP_DIR / "10_assistance_tool" / "cleanup_rules.txt")

OUTPUT_CLEANED = "cleanup_email_text_emails_raw.jsonl"


# ---------------------------------------------------------------------------
# ルール読み込み
# ---------------------------------------------------------------------------

class CleanupRules:
    """cleanup_rules.txt から読み込んだクリーニングルール一式。"""

    def __init__(
        self,
        signature_starts: List[str],
        greeting_patterns: List[str],
        separator_regexes: List[re.Pattern],
        remove_with_adjacent_url_patterns: List[str],
    ):
        self.signature_starts = signature_starts      # 部分一致で署名開始を検出
        self.greeting_patterns = greeting_patterns    # 部分一致で挨拶行を除去
        self.separator_regexes = separator_regexes    # fullmatch で区切り行を除去
        self.remove_with_adjacent_url_patterns = remove_with_adjacent_url_patterns


def normalize(s: str) -> str:
    """Unicode NFKC 正規化 + 前後空白除去。"""
    return unicodedata.normalize("NFKC", s or "").strip()


def load_cleanup_rules(path: str) -> CleanupRules:
    """cleanup_rules.txt を読み込んでルールオブジェクトを返す。"""
    signature_starts: List[str] = []
    greeting_patterns: List[str] = []
    separator_regexes: List[re.Pattern] = []
    remove_with_adjacent_url_patterns: List[str] = []

    rules_path = Path(path)
    if not rules_path.exists():
        logger.warn(f"クリーニングルールファイルが存在しません: {path}")
        return CleanupRules(
            signature_starts,
            greeting_patterns,
            separator_regexes,
            remove_with_adjacent_url_patterns,
        )

    current_section = None
    with open(rules_path, encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n\r")
            stripped = line.strip()

            if not stripped or stripped.startswith("#"):
                continue

            if stripped.startswith("[") and stripped.endswith("]"):
                current_section = stripped[1:-1].upper()
                continue

            if current_section == "SIGNATURE_START":
                signature_starts.append(normalize(stripped))
            elif current_section == "GREETING_LINE":
                greeting_patterns.append(normalize(stripped))
            elif current_section == "SEPARATOR_REGEX":
                try:
                    separator_regexes.append(re.compile(stripped, re.UNICODE))
                except re.error as e:
                    logger.warn(f"無効な正規表現をスキップ: {stripped!r} ({e})")
            elif current_section == "REMOVE_WITH_ADJACENT_URL":
                remove_with_adjacent_url_patterns.append(normalize(stripped))

    logger.info(
        f"クリーニングルール読み込み完了: "
        f"署名開始={len(signature_starts)}件, "
        f"挨拶={len(greeting_patterns)}件, "
        f"区切り={len(separator_regexes)}件, "
        f"隣接URL除去={len(remove_with_adjacent_url_patterns)}件"
    )
    return CleanupRules(
        signature_starts,
        greeting_patterns,
        separator_regexes,
        remove_with_adjacent_url_patterns,
    )


# ---------------------------------------------------------------------------
# クリーニング処理
# ---------------------------------------------------------------------------

def _is_separator_line(line: str, rules: CleanupRules) -> bool:
    """区切り線パターンに一致するか確認する。"""
    normed = normalize(line)
    for pattern in rules.separator_regexes:
        if pattern.fullmatch(normed):
            return True
    return False


def _is_greeting_line(line: str, rules: CleanupRules) -> bool:
    """挨拶文パターンを含むか確認する。"""
    normed = normalize(line)
    for pat in rules.greeting_patterns:
        if pat in normed:
            return True
    return False


def _find_signature_start(lines: List[str], rules: CleanupRules) -> int:
    """
    署名開始行のインデックスを返す。
    見つからない場合は len(lines) を返す。
    """
    for i, line in enumerate(lines):
        normed = normalize(line)
        for pat in rules.signature_starts:
            if pat in normed:
                # 〒 の場合：その文字より前に本文が続く可能性があるため
                # 行の先頭から削除する（行ごと削除）
                return i
    return len(lines)


def _contains_url(line: str) -> bool:
    """行内にURLが含まれるか確認する。"""
    return "http://" in line or "https://" in line or "<http://" in line or "<https://" in line


def _find_lines_to_remove_with_adjacent_url(lines: List[str], rules: CleanupRules) -> Set[int]:
    """
    特定文言を含む行と、その隣接URL行のインデックス集合を返す。
    対象:
      - 同一行にURLがある場合
      - 次行がURLの場合
      - 1行空けた次行がURLの場合
    """
    remove_indexes: Set[int] = set()

    for i, line in enumerate(lines):
        normed = normalize(line)
        if not normed:
            continue

        matched = False
        for pat in rules.remove_with_adjacent_url_patterns:
            if pat in normed:
                matched = True
                break

        if not matched:
            continue

        remove_indexes.add(i)

        if _contains_url(line):
            continue

        if i + 1 < len(lines) and _contains_url(lines[i + 1]):
            remove_indexes.add(i + 1)
            continue

        if i + 2 < len(lines) and lines[i + 1].strip() == "" and _contains_url(lines[i + 2]):
            remove_indexes.add(i + 2)

    return remove_indexes


def cleanup_body(body_text: str, rules: CleanupRules) -> Tuple[str, int]:
    """
    メール本文をクリーニングして返す。

    Returns:
        (cleaned_text, removed_chars_count)
    """
    original_len = len(body_text)

    # &nbsp; を空白に置換（HTML エンティティ残留対応・行分割前に処理）
    body_text = body_text.replace("&nbsp;", " ")

    # 行分割（改行コード統一）
    lines = body_text.splitlines()

    # ① 署名除去：署名開始行以降を全削除
    sig_idx = _find_signature_start(lines, rules)
    lines = lines[:sig_idx]

    # ①-2 特定文言 + 隣接URL をまとめて除去
    remove_with_url_indexes = _find_lines_to_remove_with_adjacent_url(lines, rules)

    # ② 挨拶文・③ 区切り線 を行ごと除去
    kept_lines = []
    for i, line in enumerate(lines):
        if i in remove_with_url_indexes:
            continue
        if _is_greeting_line(line, rules):
            continue
        if _is_separator_line(line, rules):
            continue
        kept_lines.append(line)

    # 連続する空行を最大2行に圧縮
    cleaned_lines: List[str] = []
    consecutive_blank = 0
    for line in kept_lines:
        if line.strip() == "":
            consecutive_blank += 1
            if consecutive_blank <= 2:
                cleaned_lines.append(line)
        else:
            consecutive_blank = 0
            cleaned_lines.append(line)

    cleaned_text = "\n".join(cleaned_lines).strip()
    removed_chars = original_len - len(cleaned_text)
    return cleaned_text, removed_chars


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = str(dirs["result"])

    start_time = time.time()
    output_records: List[Dict] = []
    input_count = 0

    try:
        # クリーニングルール読み込み
        rules = load_cleanup_rules(CLEANUP_RULES_PATH)

        # メールマスタ読み込み
        logger.info(f"メールマスタ読み込み: {INPUT_MASTER}")
        master = read_jsonl_as_dict(INPUT_MASTER, key="message_id")
        logger.info(f"メールマスタ件数: {len(master)}件")

        # 01-3 の出力（処理対象 message_id）読み込み
        logger.info(f"01-3 出力読み込み: {INPUT_PREV}")
        prev_records = read_jsonl_as_list(INPUT_PREV)
        input_count = len(prev_records)
        logger.info(f"01-3 入力件数: {input_count}件")

        total_removed_chars = 0
        not_found_count = 0

        for rec in prev_records:
            mid = rec.get("message_id", "")
            master_rec = master.get(mid)

            if master_rec is None:
                logger.warn(f"メールマスタに存在しない message_id: {mid}")
                not_found_count += 1
                output_records.append({"message_id": mid, "body_text": ""})
                continue

            body_text = master_rec.get("body_text") or ""
            cleaned_text, removed_chars = cleanup_body(body_text, rules)
            total_removed_chars += removed_chars

            output_records.append({"message_id": mid, "body_text": cleaned_text})

        if not_found_count > 0:
            logger.warn(f"メールマスタに存在しなかった件数: {not_found_count}件")

        avg_removed = total_removed_chars / input_count if input_count > 0 else 0
        logger.info(
            f"クリーニング完了: {input_count}件処理 / "
            f"合計除去文字数={total_removed_chars:,}文字 / "
            f"平均除去文字数={avg_removed:.0f}文字/件"
        )

        # 出力
        out_path = str(dirs["result"] / OUTPUT_CLEANED)
        write_jsonl(out_path, output_records)
        logger.ok(f"出力書き込み完了: {out_path} ({len(output_records)}件)")

    except Exception as e:
        write_error_log(result_dir, e, context=f"input={INPUT_PREV}")
        logger.error(f"処理失敗: {e}")
        sys.exit(1)

    finally:
        elapsed = time.time() - start_time
        write_execution_time(
            str(dirs["execution_time"]),
            STEP_NAME,
            elapsed,
            record_count=len(output_records),
        )

    logger.ok(
        f"Step完了: 入力={input_count}件 / 出力={len(output_records)}件"
    )


if __name__ == "__main__":
    main()

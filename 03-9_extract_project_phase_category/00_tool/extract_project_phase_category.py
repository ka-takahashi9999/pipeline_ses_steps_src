#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 03-9: 案件メールから工程をルールベースで抽出

Feature flag (config.py の ENABLE_PHASE_CATEGORY):
  False（デフォルト）:
    - 全レコードを phases=[], phases_raw=null でpass-through出力
  True:
    - phase_dictionary.txt を使って工程を抽出

辞書形式（YAML）:
  正規工程名（キー）に対して、マッチキーワードリストを持つ。
  キーワードにマッチした場合、正規工程名を phases に追加する。
  同一の正規工程名は重複して追加しない。

マッチングルール:
  ・NFKC正規化後にキーワードマッチング
  ・日本語キーワードは文字境界なし、英数字キーワード（UT/IT/ST/PMO等）はASCII英数字境界を使用

入力①（本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②（処理対象 message_id）:
  02-2_classify_output_file_project_resource/01_result/projects.jsonl

出力①（抽出結果）:
  01_result/extract_project_phase_category.jsonl
出力②（nullまたは空フェーズの異常レコード）:
  01_result/99_phase_null_extract_project_phase_category.jsonl
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

sys.path.insert(0, str(_STEP_DIR / "00_tool"))
from config import ENABLE_PHASE_CATEGORY

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "03-9_extract_project_phase_category"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_PROJECTS = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl"
)
PHASE_DICT_PATH = str(_STEP_DIR / "00_tool" / "phase_dictionary.txt")
OUTPUT_EXTRACTED = "extract_project_phase_category.jsonl"
OUTPUT_NULL      = "99_phase_null_extract_project_phase_category.jsonl"


# ── 正規化 ────────────────────────────────────────────────
def _n(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "")


# ── 辞書ロード ────────────────────────────────────────────
def load_phase_dictionary(path: str) -> Dict[str, List[Tuple[str, re.Pattern]]]:
    """
    YAML形式の工程辞書を読み込む。

    Returns:
        {正規工程名: [(keyword, compiled_pattern), ...]}
        ・長いキーワードが先にマッチするよう長さ降順でソート済み
    """
    import yaml

    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    result: Dict[str, List[Tuple[str, re.Pattern]]] = {}
    for phase_name, keywords in (data or {}).items():
        if not isinstance(keywords, list):
            continue
        entries: List[Tuple[str, re.Pattern]] = []
        for kw in keywords:
            if not kw:
                continue
            kw_str = str(kw).strip()
            escaped = re.escape(kw_str)
            # 英数字のみのキーワード（UT/IT/ST/BD/DD/PG/RD/PMO等）は
            # ASCII英数字境界を使用。\b だと日本語に隣接したケース（例: PMO業務）を
            # 拾えないため、前後にASCII英数字がないことを条件にする。
            if re.match(r"^[A-Za-z0-9]+$", kw_str):
                pattern = re.compile(
                    r"(?<![A-Za-z0-9])" + escaped + r"(?![A-Za-z0-9])",
                    re.IGNORECASE,
                )
            else:
                pattern = re.compile(escaped)
            entries.append((kw_str, pattern))
        # 長いキーワード優先
        entries.sort(key=lambda x: len(x[0]), reverse=True)
        result[phase_name] = entries

    return result


# ── 工程抽出 ──────────────────────────────────────────────
def extract_phases(
    body: str,
    phase_dict: Dict[str, List[Tuple[str, re.Pattern]]],
) -> Tuple[List[str], Optional[str]]:
    """
    本文から工程を抽出する。

    Returns:
        (phases, phases_raw)
          phases: 検出された正規工程名リスト（出現順、重複なし）
          phases_raw: 最初にマッチした前後コンテキスト
    """
    text = _n(body)

    matched_phases: List[str] = []
    seen_phases: set = set()
    first_match_pos = len(text)
    last_match_pos  = 0

    for phase_name, entries in phase_dict.items():
        for kw_str, pattern in entries:
            m = pattern.search(text)
            if m:
                if phase_name not in seen_phases:
                    seen_phases.add(phase_name)
                    matched_phases.append((phase_name, m.start()))
                first_match_pos = min(first_match_pos, m.start())
                last_match_pos  = max(last_match_pos, m.end())
                break  # 同じ工程内で最初にマッチしたキーワードのみ使用

    if not matched_phases:
        return [], None

    # 出現位置順にソート
    matched_phases.sort(key=lambda x: x[1])
    phases = [p for p, _ in matched_phases]

    # phases_raw: 最初〜最後のマッチ位置の前後コンテキスト
    raw_start = max(0, first_match_pos - 20)
    raw_end   = min(len(text), last_match_pos + 30)
    phases_raw: Optional[str] = text[raw_start:raw_end].strip()[:200] or None

    return phases, phases_raw


# ── レコード構築 ──────────────────────────────────────────
def build_passthrough_record(mid: str) -> Dict:
    return {
        "message_id": mid,
        "phases": [],
        "phases_raw": None,
    }


def build_extracted_record(
    mid: str,
    body: str,
    phase_dict: Dict[str, List[Tuple[str, re.Pattern]]],
) -> Dict:
    phases, phases_raw = extract_phases(body, phase_dict)
    return {
        "message_id": mid,
        "phases": phases,
        "phases_raw": phases_raw,
    }


def _is_valid(rec: Dict) -> bool:
    return isinstance(rec.get("phases"), list)


# ── メイン ────────────────────────────────────────────────
def main() -> None:
    start = time.time()
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = dirs["result"]

    logger.info(f"ENABLE_PHASE_CATEGORY = {ENABLE_PHASE_CATEGORY}")

    for path in [INPUT_PROJECTS, INPUT_CLEANED]:
        if not Path(path).exists():
            logger.error(f"入力ファイルが存在しません: {path}")
            sys.exit(1)

    try:
        project_ids = [r["message_id"] for r in read_jsonl_as_list(INPUT_PROJECTS)]
    except Exception as e:
        write_error_log(str(result_dir), e, "入力ファイル読み込みエラー")
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    # feature flag OFF: pass-through
    if not ENABLE_PHASE_CATEGORY:
        logger.info("feature flag OFF: pass-through モードで実行します")
        extracted = [build_passthrough_record(mid) for mid in project_ids]
        write_jsonl(str(result_dir / OUTPUT_EXTRACTED), extracted)
        write_jsonl(str(result_dir / OUTPUT_NULL), [])
        elapsed = time.time() - start
        write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, len(project_ids))
        logger.ok(f"Step完了(pass-through): 入力={len(project_ids)}件 / 出力={len(extracted)}件")
        return

    # feature flag ON: 辞書ロード → 抽出
    if not Path(PHASE_DICT_PATH).exists():
        logger.error(f"工程辞書が存在しません: {PHASE_DICT_PATH}")
        sys.exit(1)

    try:
        import yaml  # noqa: F401
    except ImportError:
        logger.error("PyYAMLがインストールされていません: pip install pyyaml")
        sys.exit(1)

    try:
        phase_dict  = load_phase_dictionary(PHASE_DICT_PATH)
        cleaned_map = read_jsonl_as_dict(INPUT_CLEANED)
    except Exception as e:
        write_error_log(str(result_dir), e, "初期化エラー")
        logger.error(f"初期化エラー: {e}")
        sys.exit(1)

    total_kw = sum(len(v) for v in phase_dict.values())
    logger.info(f"工程辞書ロード完了: {len(phase_dict)}工程 / {total_kw}キーワード")

    extracted_list: list = []
    null_records: list = []

    for mid in project_ids:
        body = (cleaned_map.get(mid) or {}).get("body_text", "")
        rec  = build_extracted_record(mid, body, phase_dict)

        if not _is_valid(rec):
            null_records.append(rec)
        else:
            extracted_list.append(rec)

        logger.info(
            f"{mid} → phases={rec.get('phases')} raw={str(rec.get('phases_raw', ''))[:40]}",
            message_id=mid,
        )

    write_jsonl(str(result_dir / OUTPUT_EXTRACTED), extracted_list)
    write_jsonl(str(result_dir / OUTPUT_NULL), null_records)

    elapsed = time.time() - start
    total   = len(project_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    has_phases   = sum(1 for r in extracted_list if r.get("phases"))
    empty_phases = sum(1 for r in extracted_list if not r.get("phases"))
    logger.ok(
        f"Step完了: 入力={total}件 / 工程あり={has_phases}件 / "
        f"工程なし={empty_phases}件 / null={len(null_records)}件"
    )


if __name__ == "__main__":
    main()

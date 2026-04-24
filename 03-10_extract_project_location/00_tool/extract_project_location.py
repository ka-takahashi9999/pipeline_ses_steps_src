#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 03-10: 案件メールから作業場所（ロケーション）をルールベースで抽出し地方に分類する

Feature flag (config.py の ENABLE_LOCATION):
  False（デフォルト）:
    - 全レコードを location=null, location_raw=null でpass-through出力（出力②へ）
  True:
    - location_dictionary.txt を使ってロケーションを抽出・地方分類

抽出フロー（ENABLE_LOCATION=True）:
  1. 署名ブロック除去（location_signature_filter）
  2. 場所ラベル行 + 次行からキャンディデート抽出（最優先）
  3. キャンディデートの正規化（リモート/出社頻度ノイズ除去）
  4. 辞書マッチ（location_parser）
  5. ラベルで取れない場合は本文全体への辞書マッチ（fallback）

出力スキーマ:
  {
    "message_id": "...",
    "location": "関東地方" | null,
    "location_raw": "...",
    "location_source": "label" | "body" | ""
  }

出力①（location非nullレコード）:
  01_result/extract_project_location.jsonl
出力②（locationがnullのレコード）:
  01_result/99_location_null_extract_project_location.jsonl
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
from config import ENABLE_LOCATION

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "03-10_extract_project_location"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_PROJECTS = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl"
)
DICT_PATH = str(_STEP_DIR / "00_tool" / "location_dictionary.txt")
OUTPUT_EXTRACTED = "extract_project_location.jsonl"
OUTPUT_NULL      = "99_location_null_extract_project_location.jsonl"

VALID_REGIONS = [
    "北海道地方",
    "東北地方",
    "関東地方",
    "中部地方",
    "近畿地方",
    "中国地方",
    "四国地方",
    "九州地方",
    "沖縄地方",
]


def _n(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "")


def load_location_dictionary(path: str) -> List[Tuple[str, str, re.Pattern]]:
    """
    YAML形式のロケーション辞書を読み込む。

    Returns:
        [(region, keyword, compiled_pattern), ...]
    """
    import yaml

    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    entries: List[Tuple[str, str, re.Pattern]] = []

    for region in VALID_REGIONS:
        keywords = data.get(region)
        if not keywords or not isinstance(keywords, list):
            continue

        sorted_kws = sorted(
            [str(k).strip() for k in keywords if k],
            key=lambda x: len(x),
            reverse=True,
        )
        for kw in sorted_kws:
            escaped = re.escape(kw)
            pattern = re.compile(escaped)
            entries.append((region, kw, pattern))

    return entries


def build_passthrough_record(mid: str) -> Dict:
    return {
        "message_id": mid,
        "location": None,
        "location_raw": None,
        "location_source": "",
    }


def build_extracted_record(
    mid: str,
    body: str,
    entries: List[Tuple[str, str, re.Pattern]],
) -> Dict:
    """署名除去 → パーサー呼び出し → レコード構築"""
    from location_signature_filter import remove_signature
    from location_parser import parse_location

    clean_body = remove_signature(body)
    location, location_raw, location_source = parse_location(clean_body, entries)

    return {
        "message_id": mid,
        "location": location,
        "location_raw": location_raw,
        "location_source": location_source,
    }


def main() -> None:
    start = time.time()
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = dirs["result"]

    logger.info(f"ENABLE_LOCATION = {ENABLE_LOCATION}")

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

    # feature flag OFF: pass-through（全件 location=null → 出力②）
    if not ENABLE_LOCATION:
        logger.info("feature flag OFF: pass-through モードで実行します")
        null_records = [build_passthrough_record(mid) for mid in project_ids]
        extracted: list = []
        out_path  = str(result_dir / OUTPUT_EXTRACTED)
        null_path = str(result_dir / OUTPUT_NULL)
        write_jsonl(out_path, extracted)
        write_jsonl(null_path, null_records)
        elapsed = time.time() - start
        write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, len(project_ids))
        logger.ok(
            f"Step完了(pass-through): 入力={len(project_ids)}件 / "
            f"出力①=0件 / 出力②={len(null_records)}件"
        )
        return

    # feature flag ON: 辞書ロード → 抽出
    if not Path(DICT_PATH).exists():
        logger.error(f"ロケーション辞書が存在しません: {DICT_PATH}")
        sys.exit(1)

    try:
        import yaml  # noqa: F401
    except ImportError:
        logger.error("PyYAMLがインストールされていません: pip install pyyaml")
        sys.exit(1)

    try:
        entries = load_location_dictionary(DICT_PATH)
        cleaned_map = read_jsonl_as_dict(INPUT_CLEANED)
    except Exception as e:
        write_error_log(str(result_dir), e, "初期化エラー")
        logger.error(f"初期化エラー: {e}")
        sys.exit(1)

    logger.info(f"ロケーション辞書ロード完了: {len(entries)}キーワード")

    extracted_list: list = []
    null_records = []

    source_counts: Dict[str, int] = {"label": 0, "body": 0, "remote_fallback": 0}

    for mid in project_ids:
        body = (cleaned_map.get(mid) or {}).get("body_text", "")
        rec = build_extracted_record(mid, body, entries)

        src = rec.get("location_source", "")
        if src in source_counts:
            source_counts[src] += 1

        if rec["location"] is None:
            null_records.append(rec)
            logger.info(f"{mid} → location=null", message_id=mid)
        else:
            extracted_list.append(rec)
            logger.info(
                f"{mid} → location={rec['location']} "
                f"source={src} / raw={str(rec['location_raw'])[:40]}",
                message_id=mid,
            )

    out_path  = str(result_dir / OUTPUT_EXTRACTED)
    null_path = str(result_dir / OUTPUT_NULL)
    write_jsonl(out_path, extracted_list)
    write_jsonl(null_path, null_records)

    elapsed = time.time() - start
    total = len(project_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    logger.ok(
        f"Step完了: 入力={total}件 / 出力①(非null)={len(extracted_list)}件 / "
        f"出力②(null)={len(null_records)}件 "
        f"[label={source_counts['label']}件 / body={source_counts['body']}件 / "
        f"remote={source_counts['remote_fallback']}件]"
    )


if __name__ == "__main__":
    main()

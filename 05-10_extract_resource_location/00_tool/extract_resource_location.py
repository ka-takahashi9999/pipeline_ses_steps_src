#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 05-10: 要員メールから居住地/最寄駅（ロケーション）をルールベースで抽出し地方に分類する

抽出フロー:
  1. 署名ブロック除去（03-10の location_signature_filter を流用）
  2. 最寄/最寄駅/居住地/住所ラベル行 + 次行からキャンディデート抽出（最優先）
  3. キャンディデートの正規化（路線名・駅suffix・括弧コンテンツ除去）
  4. 辞書マッチ（05-10独自の location_dictionary.txt を使用）
  5. ラベルで取れない場合は本文全体への辞書マッチ（フォールバック）
  6. 地方抽出失敗時: 海外在住明示表現 → "overseas"、それ以外 → "unknown"
  ※ リモートフォールバックなし（要員は居住地を対象とする）
  ※ locationは常に非null（null禁止）

出力スキーマ:
  {
    "message_id": "...",
    "location": "関東地方" | "overseas" | "unknown",
    "location_raw": "...",
    "location_source": "label" | "body" | ""
  }

出力①（location確定レコード: 9地方 + overseas + unknown）:
  01_result/extract_resource_location.jsonl
出力②（常に空ファイル: null廃止のため）:
  01_result/99_location_null_extract_resource_location.jsonl
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

# 03-10の signature_filter を流用
_TOOL_03_10 = str(_PROJECT_ROOT / "03-10_extract_project_location" / "00_tool")
sys.path.insert(0, _TOOL_03_10)
# 本stepのパーサーを使う
sys.path.insert(0, str(_STEP_DIR / "00_tool"))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "05-10_extract_resource_location"
logger = get_logger(STEP_NAME)

INPUT_RESOURCES = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "resources.jsonl"
)
INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
DICT_PATH = str(_STEP_DIR / "00_tool" / "location_dictionary.txt")
OUTPUT_EXTRACTED = "extract_resource_location.jsonl"
OUTPUT_NULL      = "99_location_null_extract_resource_location.jsonl"

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
    03-10 の load_location_dictionary と同実装。
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
        "location": "unknown",
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
    from resource_location_parser import parse_location

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

    for path in [INPUT_RESOURCES, INPUT_CLEANED]:
        if not Path(path).exists():
            logger.error(f"入力ファイルが存在しません: {path}")
            sys.exit(1)

    if not Path(DICT_PATH).exists():
        logger.error(f"ロケーション辞書が存在しません: {DICT_PATH}")
        sys.exit(1)

    try:
        import yaml  # noqa: F401
    except ImportError:
        logger.error("PyYAMLがインストールされていません: pip install pyyaml")
        sys.exit(1)

    try:
        resource_ids = [r["message_id"] for r in read_jsonl_as_list(INPUT_RESOURCES)]
    except Exception as e:
        write_error_log(str(result_dir), e, "入力ファイル読み込みエラー")
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    try:
        entries = load_location_dictionary(DICT_PATH)
        cleaned_map = read_jsonl_as_dict(INPUT_CLEANED)
    except Exception as e:
        write_error_log(str(result_dir), e, "初期化エラー")
        logger.error(f"初期化エラー: {e}")
        sys.exit(1)

    logger.info(f"ロケーション辞書ロード完了: {len(entries)}キーワード")
    logger.info(f"要員ID数: {len(resource_ids)}件")

    extracted_list: list = []
    null_records: list = []

    source_counts: Dict[str, int] = {"label": 0, "body": 0}

    for mid in resource_ids:
        body = (cleaned_map.get(mid) or {}).get("body_text", "")
        rec = build_extracted_record(mid, body, entries)

        src = rec.get("location_source", "")
        if src in source_counts:
            source_counts[src] += 1

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
    total = len(resource_ids)
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    overseas_count = sum(1 for r in extracted_list if r.get("location") == "overseas")
    unknown_count  = sum(1 for r in extracted_list if r.get("location") == "unknown")
    region_count   = len(extracted_list) - overseas_count - unknown_count

    logger.ok(
        f"Step完了: 入力={total}件 / 地方確定={region_count}件 / "
        f"overseas={overseas_count}件 / unknown={unknown_count}件 "
        f"[label={source_counts['label']}件 / body={source_counts['body']}件]"
    )


if __name__ == "__main__":
    main()

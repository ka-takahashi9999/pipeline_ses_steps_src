#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 02-2: 分類結果ファイル分割出力

02-1 の分類結果を mail_type ごとに別ファイルへ出力する。
出力レコードは message_id のみ。

入力①（分類結果）:
  02-1_classify_type_project_resource/01_result/classify_types_project_resource.jsonl

入力②（unknown）:
  02-1_classify_type_project_resource/01_result/99_no_classify_types_project_resource.jsonl

出力①（案件）:   01_result/projects.jsonl
出力②（要員）:   01_result/resources.jsonl
出力③（あいまい）: 01_result/ambiguous.jsonl
出力④（不明）:   01_result/unknown.jsonl
"""

import sys
import time
from pathlib import Path
from typing import Dict, List

# common モジュールのパス解決
_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "02-2_classify_output_file_project_resource"
logger = get_logger(STEP_NAME)

INPUT_CLASSIFIED = str(
    _PROJECT_ROOT
    / "02-1_classify_type_project_resource"
    / "01_result"
    / "classify_types_project_resource.jsonl"
)
INPUT_UNKNOWN = str(
    _PROJECT_ROOT
    / "02-1_classify_type_project_resource"
    / "01_result"
    / "99_no_classify_types_project_resource.jsonl"
)

OUTPUT_PROJECTS = "projects.jsonl"
OUTPUT_RESOURCES = "resources.jsonl"
OUTPUT_AMBIGUOUS = "ambiguous.jsonl"
OUTPUT_UNKNOWN = "unknown.jsonl"


def main() -> None:
    start = time.time()
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = dirs["result"]

    # 入力確認
    for path in [INPUT_CLASSIFIED, INPUT_UNKNOWN]:
        if not Path(path).exists():
            logger.error(f"入力ファイルが存在しません: {path}")
            sys.exit(1)

    try:
        classified    = read_jsonl_as_list(INPUT_CLASSIFIED)
        unknown_extra = read_jsonl_as_list(INPUT_UNKNOWN)
    except Exception as e:
        write_error_log(str(result_dir), e, "入力ファイル読み込みエラー")
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    # mail_type ごとに仕分け
    buckets: Dict[str, List[Dict]] = {
        "project": [],
        "resource": [],
        "ambiguous": [],
        "unknown": [],
    }

    for rec in classified:
        mail_type = rec.get("mail_type", "unknown")
        mid = rec.get("message_id", "")
        if mail_type not in buckets:
            mail_type = "unknown"
        buckets[mail_type].append({"message_id": mid})

    # 02-1 の 99_no_classify（unknown 確定分）を unknown に追加
    for rec in unknown_extra:
        mid = rec.get("message_id", "")
        buckets["unknown"].append({"message_id": mid})

    # 出力
    output_map = {
        "project":   OUTPUT_PROJECTS,
        "resource":  OUTPUT_RESOURCES,
        "ambiguous": OUTPUT_AMBIGUOUS,
        "unknown":   OUTPUT_UNKNOWN,
    }

    for mail_type, filename in output_map.items():
        out_path = str(result_dir / filename)
        write_jsonl(out_path, buckets[mail_type])
        logger.ok(
            f"出力完了: {filename} ({len(buckets[mail_type])}件)"
        )

    total = len(classified) + len(unknown_extra)
    elapsed = time.time() - start
    write_execution_time(
        str(dirs["execution_time"]),
        STEP_NAME,
        elapsed,
        total,
    )

    logger.ok(
        f"Step完了: 入力={total}件 / "
        f"project={len(buckets['project'])}件 / "
        f"resource={len(buckets['resource'])}件 / "
        f"ambiguous={len(buckets['ambiguous'])}件 / "
        f"unknown={len(buckets['unknown'])}件"
    )


if __name__ == "__main__":
    main()

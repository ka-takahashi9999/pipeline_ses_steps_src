#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工程辞書候補提案ツール（10_assistance_tool）

パイプライン対象外。手動実行専用。

処理:
  1. 現在の phase_dictionary.txt を読み込む
  2. 案件メールを最大 MAX_SAMPLES 件サンプリング
  3. LLMでメール本文を解析し、辞書未登録の工程キーワードを抽出
  4. 候補を 10_assistance_tool/phase_dictionary_candidates.txt に出力

使用方法:
  cd /home/ec2-user/pipeline_ses_steps
  python 03-9_extract_project_phase_category/10_assistance_tool/suggest_phase_dictionary.py

出力:
  03-9_extract_project_phase_category/10_assistance_tool/phase_dictionary_candidates.txt
"""

import sys
import random
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set

_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import ensure_dir
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list
from common.llm_client import call_llm
from common.logger import get_logger

STEP_NAME = "suggest_phase_dictionary"
logger = get_logger(STEP_NAME)

INPUT_CLEANED  = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_PROJECTS = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl"
)
PHASE_DICT_PATH   = str(_PROJECT_ROOT / "common" / "phase_dictionary.txt")
OUTPUT_CANDIDATES = os.environ.get(
    "DICTIONARY_CANDIDATES_FILE",
    str(_STEP_DIR / "10_assistance_tool" / "phase_dictionary_candidates.txt"),
)

MAX_SAMPLES = 100
BATCH_SIZE  = 10

RESPONSE_SCHEMA = {
    "candidates": [
        {
            "keyword": "工程キーワード",
            "phase": "対応する正規工程名",
            "reason": "メール内の出現例",
        }
    ]
}

SYSTEM_PROMPT = """\
あなたはITプロジェクトのSES案件メールを分析する専門家です。
与えられたメール本文から、ITプロジェクトの工程・フェーズに関するキーワードを抽出してください。

【対象とする正規工程名】
要件定義 / 基本設計 / 詳細設計 / 製造 / 開発 / テスト / 運用保守 / リリース / 移行 / 要件調査 / PMO / 設計・構築

【抽出ルール】
・上記の正規工程名に相当するキーワードや略語・表現を抽出する
・例：「概要設計」→「基本設計」、「UT」→「テスト」、「COT」→「カットオーバー（リリース）」
・一般的なビジネス用語（会議、報告等）は除外する
・既に辞書に登録済みのキーワードは除外する
・日本語・英語略語どちらも対象

出力はJSONのみ。candidatesリストに最大20件まで。"""


def load_existing_keywords(dict_path: str) -> Set[str]:
    """既登録キーワードを小文字セットで返す。"""
    existing: Set[str] = set()
    try:
        import yaml
        with open(dict_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        for keywords in (data or {}).values():
            if isinstance(keywords, list):
                for kw in keywords:
                    if kw:
                        existing.add(str(kw).strip().lower())
    except Exception as e:
        logger.warn(f"辞書読み込みエラー（既存キーワードチェックをスキップ）: {e}")
    return existing


def sample_email_bodies(
    project_ids: List[str],
    cleaned_map: Dict,
    n: int,
) -> List[Dict[str, str]]:
    sampled = random.sample(project_ids, min(n, len(project_ids)))
    result = []
    for mid in sampled:
        body = (cleaned_map.get(mid) or {}).get("body_text", "")
        if body:
            result.append({"message_id": mid, "body": body[:1500]})
    return result


def extract_candidates_via_llm(
    emails: List[Dict[str, str]],
    existing_keywords: Set[str],
) -> List[Dict[str, str]]:
    bodies_text = "\n\n---\n\n".join(
        f"[{e['message_id']}]\n{e['body']}" for e in emails
    )
    existing_excerpt = ", ".join(sorted(list(existing_keywords))[:50])
    user_prompt = (
        f"【既に辞書登録済みのキーワード例（除外対象）】\n{existing_excerpt}\n\n"
        f"【分析対象メール】\n{bodies_text}"
    )

    result = call_llm(
        system_prompt=SYSTEM_PROMPT,
        user_prompt=user_prompt,
        response_schema=RESPONSE_SCHEMA,
        max_tokens=2048,
    )
    candidates = result.get("candidates") or []

    return [
        c for c in candidates
        if isinstance(c, dict) and c.get("keyword")
        and str(c["keyword"]).strip().lower() not in existing_keywords
    ]


def write_candidates(candidates: List[Dict[str, str]], output_path: str) -> None:
    ensure_dir(str(Path(output_path).parent))
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        f"# 工程辞書候補 - 生成日時: {timestamp}",
        f"# 件数: {len(candidates)}件",
        "#",
        "# 確認後、common/phase_dictionary.txt に手動で追記してください",
        "# フォーマット: 正規工程名:",
        "#               - キーワード",
        "",
    ]

    # 正規工程名別にグループ化
    by_phase: Dict[str, List[str]] = {}
    for c in candidates:
        phase   = c.get("phase", "未分類")
        keyword = c.get("keyword", "")
        reason  = c.get("reason", "")
        if keyword:
            if phase not in by_phase:
                by_phase[phase] = []
            reason_str = f"  # {reason}" if reason else ""
            by_phase[phase].append(f"  - {keyword}{reason_str}")

    for phase, entries in sorted(by_phase.items()):
        lines.append(f"{phase}:")
        lines.extend(entries)
        lines.append("")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    logger.info(f"候補出力: {output_path} ({len(candidates)}件)")


def main() -> None:
    logger.info("工程辞書候補提案ツールを開始します")

    for path in [INPUT_PROJECTS, INPUT_CLEANED, PHASE_DICT_PATH]:
        if not Path(path).exists():
            logger.error(f"ファイルが存在しません: {path}")
            sys.exit(1)

    try:
        import yaml  # noqa: F401
    except ImportError:
        logger.error("PyYAMLがインストールされていません: pip install pyyaml")
        sys.exit(1)

    try:
        project_ids = [r["message_id"] for r in read_jsonl_as_list(INPUT_PROJECTS)]
        cleaned_map = read_jsonl_as_dict(INPUT_CLEANED)
    except Exception as e:
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    existing_keywords = load_existing_keywords(PHASE_DICT_PATH)
    logger.info(f"既登録キーワード数: {len(existing_keywords)}件")

    emails = sample_email_bodies(project_ids, cleaned_map, MAX_SAMPLES)
    logger.info(f"サンプリング: {len(emails)}件のメールを使用")

    all_candidates: List[Dict[str, str]] = []
    seen_keywords: Set[str] = set()

    for i in range(0, len(emails), BATCH_SIZE):
        batch = emails[i:i + BATCH_SIZE]
        logger.info(f"LLM呼び出し: バッチ {i // BATCH_SIZE + 1} / {(len(emails) - 1) // BATCH_SIZE + 1}")
        candidates = extract_candidates_via_llm(batch, existing_keywords)

        for c in candidates:
            kw_lower = str(c.get("keyword", "")).lower()
            if kw_lower and kw_lower not in seen_keywords:
                seen_keywords.add(kw_lower)
                all_candidates.append(c)

    logger.info(f"候補合計: {len(all_candidates)}件（重複除去済み）")
    write_candidates(all_candidates, OUTPUT_CANDIDATES)
    logger.ok(f"完了: {OUTPUT_CANDIDATES} に出力しました")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
アシスタントツール①: ロケーション辞書候補抽出
（手動実行専用 / pipeline対象外）

入力①: 01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②: 02-2_classify_output_file_project_resource/01_result/projects.jsonl
処理 : メール本文中の場所ラベル行前後をキーワードgrepで抽出し、LLMで未登録候補を提案
出力 : 10_assistance_tool/location_dictionary_candidates.txt
"""

import re
import os
import sys
import unicodedata
from pathlib import Path
from typing import Dict, List, Set

_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list
from common.llm_client import call_llm
from common.logger import get_logger

STEP_NAME = "suggest_location_dictionary"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_PROJECTS = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl"
)
OUTPUT_CANDIDATES = os.environ.get(
    "DICTIONARY_CANDIDATES_FILE",
    str(_STEP_DIR / "10_assistance_tool" / "location_dictionary_candidates.txt"),
)
DICT_PATH = str(_STEP_DIR / "00_tool" / "location_dictionary.txt")

# 03-10 本体 (location_parser.py) と同じラベル正規表現
_LABEL_RE = re.compile(r"(場\s*所|勤務地|作業場所|勤務場所|就業場所|最寄|最寄駅|拠点)")

CONTEXT_LINES = 2   # ラベルヒット行の前後何行を抽出するか
BATCH_CHAR_LIMIT = 6000  # 1バッチあたりのLLMに渡す文字数上限
BATCH_RECORD_LIMIT = 50  # 1バッチあたりの最大レコード数


def _normalize(text: str) -> str:
    """NFKC正規化（全角英数→半角等）。"""
    return unicodedata.normalize("NFKC", text)


def load_existing_keywords() -> Set[str]:
    """既存辞書のキーワード一覧をセットで返す。"""
    existing = set()
    try:
        import yaml
        with open(DICT_PATH, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        for kws in (data or {}).values():
            if isinstance(kws, list):
                for kw in kws:
                    if kw:
                        existing.add(str(kw).strip())
    except Exception as e:
        logger.warn(f"既存辞書の読み込みに失敗しました（スキップ）: {e}")
    return existing


def extract_location_snippets(body: str) -> str:
    """メール本文から場所ラベル行の前後CONTEXT_LINES行を抽出して結合する。"""
    lines = body.splitlines()
    hit_indices = set()
    for i, line in enumerate(lines):
        if _LABEL_RE.search(_normalize(line)):
            for j in range(max(0, i - CONTEXT_LINES), min(len(lines), i + CONTEXT_LINES + 1)):
                hit_indices.add(j)
    if not hit_indices:
        return ""
    return "\n".join(lines[i] for i in sorted(hit_indices))


def build_batches(snippets: List[Dict[str, str]]) -> List[List[Dict[str, str]]]:
    """スニペットを文字数・レコード数上限でバッチに分割する。"""
    batches = []
    current_batch = []
    current_chars = 0
    for item in snippets:
        item_chars = len(item["snippet"])
        if current_batch and (
            len(current_batch) >= BATCH_RECORD_LIMIT
            or current_chars + item_chars > BATCH_CHAR_LIMIT
        ):
            batches.append(current_batch)
            current_batch = []
            current_chars = 0
        current_batch.append(item)
        current_chars += item_chars
    if current_batch:
        batches.append(current_batch)
    return batches


def extract_candidates_via_llm(
    batch: List[Dict[str, str]], existing_keywords: Set[str]
) -> List[str]:
    """LLMを使ってロケーション候補キーワードを抽出する。"""
    existing_sample = sorted(list(existing_keywords))[:30]
    combined_text = "\n\n---\n\n".join(
        f"[{item['message_id']}]\n{item['snippet']}" for item in batch
    )

    prompt = f"""以下は案件メールの「作業場所・勤務地」周辺テキストです。
各スニペットから作業場所・勤務地を示すキーワードを抽出してください。

【既存辞書のキーワード例（これらはすでに登録済み）】
{chr(10).join(existing_sample)}

【対象スニペット（{len(batch)}件）】
{combined_text}

【出力形式】
未登録のキーワード候補を地方別にYAML形式で出力してください。
分類名は 北海道地方 / 東北地方 / 関東地方 / 中部地方 / 近畿地方 / 中国地方 / 四国地方 / 九州地方 / 沖縄地方 のいずれかにしてください。
都道府県名・市区町村名・主要駅名・ビル名・エリア名などを含めてください。
既存辞書に登録済みのものは除外してください。

出力例:
関東地方:
  - 秋葉原UDX
  - 竹橋
  - 内幸町
近畿地方:
  - 西梅田
  - 南港
"""

    system_prompt = (
        "あなたは日本語の案件メールからロケーション情報を抽出する専門家です。"
        "与えられたテキストから作業場所・勤務地を示すキーワードを抽出してください。"
    )

    schema = {"candidates": {}}
    result = call_llm(
        system_prompt=system_prompt,
        user_prompt=prompt,
        response_schema=schema,
        max_tokens=4096,
    )
    return result


def main() -> None:
    logger.info("=== ロケーション辞書候補抽出ツール開始 ===")

    for path in [INPUT_PROJECTS, INPUT_CLEANED]:
        if not Path(path).exists():
            logger.error(f"入力ファイルが存在しません: {path}")
            sys.exit(1)

    try:
        project_ids = [r["message_id"] for r in read_jsonl_as_list(INPUT_PROJECTS)]
        cleaned_map = read_jsonl_as_dict(INPUT_CLEANED)
    except Exception as e:
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    logger.info(f"案件メール全件数: {len(project_ids)}件")

    # --- 場所ラベル前後のスニペット抽出 ---
    snippets = []
    for mid in project_ids:
        body = (cleaned_map.get(mid) or {}).get("body_text", "")
        if not body:
            continue
        snippet = extract_location_snippets(body)
        if snippet:
            snippets.append({"message_id": mid, "snippet": snippet})

    logger.info(f"ラベルヒット件数: {len(snippets)}件 / 全{len(project_ids)}件")

    if not snippets:
        output_path = Path(OUTPUT_CANDIDATES)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("# ロケーション辞書候補\n")
            f.write("# 今回実行結果: ラベルヒット0件 / 候補数: 0件\n\n")
            f.write("# 新規候補なし\n")
        logger.warn("ラベルにヒットするメールが0件です。今回の0件結果を出力しました")
        return

    # --- スニペット内容のサンプル表示（異常チェック用） ---
    logger.info("--- スニペットサンプル（先頭3件） ---")
    for item in snippets[:3]:
        preview = item["snippet"].replace("\n", " | ")[:200]
        logger.info(f"  [{item['message_id']}] {preview}")
    logger.info("--- サンプルここまで ---")

    existing_keywords = load_existing_keywords()
    logger.info(f"既存辞書キーワード数: {len(existing_keywords)}件")

    # --- バッチ分割してLLM呼び出し ---
    batches = build_batches(snippets)
    logger.info(f"LLMバッチ数: {batches.__len__()}バッチ")

    all_candidates = {}
    for i, batch in enumerate(batches):
        logger.info(f"LLM呼び出し: バッチ {i + 1} / {len(batches)} ({len(batch)}件)")
        result = extract_candidates_via_llm(batch, existing_keywords)
        candidates = result.get("candidates", result) if isinstance(result, dict) else {}
        if isinstance(candidates, dict):
            for region, keywords in candidates.items():
                if isinstance(keywords, list):
                    if region not in all_candidates:
                        all_candidates[region] = []
                    all_candidates[region].extend(keywords)

    # --- 重複除去 ---
    for region in all_candidates:
        seen = set()
        unique = []
        for kw in all_candidates[region]:
            kw_str = str(kw).strip()
            if kw_str and kw_str not in seen and kw_str not in existing_keywords:
                seen.add(kw_str)
                unique.append(kw_str)
        all_candidates[region] = unique

    # --- 空リージョン除去 ---
    all_candidates = {r: kws for r, kws in all_candidates.items() if kws}

    total_count = sum(len(kws) for kws in all_candidates.values())

    # --- 出力 ---
    output_path = Path(OUTPUT_CANDIDATES)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# ロケーション辞書候補\n")
        f.write("# LLMにより自動生成された候補です。人間がチェックしてから location_dictionary.txt に追記してください。\n")
        f.write(f"# ラベルヒット: {len(snippets)}件 / 候補数: {total_count}件\n\n")
        if all_candidates:
            import yaml
            f.write(yaml.dump(all_candidates, allow_unicode=True, default_flow_style=False))
        else:
            f.write("# 新規候補なし\n")

    logger.ok(f"候補ファイル出力完了: {output_path} ({total_count}件)")
    logger.info("内容を確認し、必要なキーワードを location_dictionary.txt に手動で追記してください。")


if __name__ == "__main__":
    main()

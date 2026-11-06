#!/usr/bin/env python3
"""
辞書候補重複チェック・除去スクリプト

phase_dictionary_candidates.txt のエントリーのうち、
phase_dictionary.txt に既登録のものを除去する。

【マッチングルール】
・完全一致（コメント・クォートを除去した後の文字列で比較）
・辞書側はコメントアウト行も登録済みとして扱う
  例: phase_dictionary.txt に `#  - 要件定義` → 要件定義 は登録済み
  例: phase_dictionary.txt に `  - 要件定義 #Sample` → 要件定義 は登録済み

使用方法:
    python3 cleanup_phase_candidates.py           # 実行（上書き）
    python3 cleanup_phase_candidates.py --dry-run # 確認のみ（ファイル更新なし）
"""

import re
import os
import sys
from pathlib import Path

import yaml

BASE_DIR = Path(__file__).parent.parent
PROJECT_ROOT = BASE_DIR.parent
DICTIONARY_FILE = PROJECT_ROOT / "common" / "phase_dictionary.txt"
CANDIDATES_FILE = Path(os.environ.get(
    "DICTIONARY_CANDIDATES_FILE", str(Path(__file__).parent / "phase_dictionary_candidates.txt")
))

# カテゴリヘッダー: 先頭が非スペース・非# で `:` で終わる行
CATEGORY_PATTERN = re.compile(r'^[^\s#].+:\s*$')
# フェーズエントリー: `  - フェーズ名` のパターン（コメントアウトでない）
PHASE_ENTRY_PATTERN = re.compile(r'^\s+-\s+')


def extract_phase_name(line: str) -> str:
    """
    行からフェーズ名を抽出する（コメント・クォート除去）。

    '  - 要件定義  # comment' -> '要件定義'
    '  - "要件定義"'          -> '要件定義'
    '#  - 要件定義'           -> '要件定義'  (辞書側のコメントアウト行も対象)
    マッチしない場合は None を返す。
    """
    match = re.match(r'^#?\s*-\s+(.+?)\s*$', line.strip())
    if match:
        try:
            value = yaml.safe_load(match.group(1))
        except yaml.YAMLError:
            return None
        return str(value).strip() if isinstance(value, str) else None
    return None


def load_registered_phases(filepath: Path) -> set:
    """辞書ファイルから全フェーズ名を取得（コメントアウト行も含む）"""
    phases = set()
    try:
        with open(filepath, encoding='utf-8') as f:
            for line in f:
                phase = extract_phase_name(line)
                if phase:
                    phases.add(phase)
    except FileNotFoundError:
        print(f"[ERROR] 辞書ファイルが見つかりません: {filepath}", file=sys.stderr)
        sys.exit(1)
    return phases


def remove_registered_entries(lines: list, registered_phases: set) -> tuple:
    """
    フェーズエントリー行を走査し、登録済みのものを除去する。
    Returns: (new_lines, removed_list, kept_list)
    """
    new_lines = []
    removed = []
    kept = []

    for line in lines:
        stripped = line.strip()
        is_phase_entry = (
            PHASE_ENTRY_PATTERN.match(line)  # strip前の行で先頭スペースを判定
            and not stripped.startswith('#')
        )

        if is_phase_entry:
            phase = extract_phase_name(line)
            if phase and phase in registered_phases:
                removed.append(phase)
                continue  # 除去
            elif phase:
                kept.append(phase)

        new_lines.append(line)

    return new_lines, removed, kept


def remove_empty_categories(lines: list) -> list:
    """フェーズエントリーが残っていないカテゴリヘッダー行を除去する"""
    result = []
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        is_category = (
            CATEGORY_PATTERN.match(stripped)
            and not stripped.startswith('#')
        )

        if is_category:
            # 次のカテゴリヘッダーまでにフェーズエントリーが存在するか確認
            j = i + 1
            has_phases = False
            while j < len(lines):
                ahead = lines[j].strip()
                if CATEGORY_PATTERN.match(ahead) and not ahead.startswith('#'):
                    break
                if PHASE_ENTRY_PATTERN.match(lines[j]) and not ahead.startswith('#'):
                    has_phases = True
                    break
                j += 1

            if has_phases:
                result.append(line)
            # フェーズなし → カテゴリヘッダーをスキップ
        else:
            result.append(line)

        i += 1

    return result


def cleanup_blank_lines(lines: list) -> list:
    """連続する空行を1行に圧縮し、ファイル末尾を改行1つで終わらせる"""
    result = []
    prev_blank = False
    for line in lines:
        is_blank = not line.strip()
        if is_blank and prev_blank:
            continue  # 連続空行をスキップ
        result.append(line)
        prev_blank = is_blank

    # 末尾を改行1つで終わらせる
    while result and not result[-1].strip():
        result.pop()
    result.append('\n')

    return result


def main():
    dry_run = '--dry-run' in sys.argv

    print(f"辞書ファイル : {DICTIONARY_FILE}")
    print(f"候補ファイル : {CANDIDATES_FILE}")
    if dry_run:
        print("[DRY RUN モード] ファイルは更新されません")
    print()

    # 辞書ファイルから登録済みフェーズを取得
    registered_phases = load_registered_phases(DICTIONARY_FILE)
    print(f"辞書登録済みフェーズ数: {len(registered_phases)}")

    # 候補ファイル読み込み
    try:
        with open(CANDIDATES_FILE, encoding='utf-8') as f:
            original_lines = f.readlines()
    except FileNotFoundError:
        print(f"[ERROR] 候補ファイルが見つかりません: {CANDIDATES_FILE}", file=sys.stderr)
        sys.exit(1)

    # 登録済みエントリーを除去
    new_lines, removed, kept = remove_registered_entries(original_lines, registered_phases)

    # 空になったカテゴリヘッダーを除去
    new_lines = remove_empty_categories(new_lines)

    # 連続空行の圧縮
    new_lines = cleanup_blank_lines(new_lines)

    for index, line in enumerate(new_lines):
        if line.startswith("# 件数:"):
            new_lines[index] = f"# 件数: {len(kept)}件\n"
            break

    # 結果表示
    print(f"除去したエントリー数: {len(removed)}")
    print(f"残存エントリー数    : {len(kept)}")

    if removed:
        print(f"\n【除去したエントリー（{len(removed)}件）】")
        for p in removed:
            print(f"  - {p}")

    if kept:
        print(f"\n【残存エントリー（{len(kept)}件）】")
        for p in kept:
            print(f"  - {p}")
    else:
        print("\n【残存エントリーなし】")

    if dry_run:
        print("\n[DRY RUN] ファイルは更新されませんでした。")
        return

    with open(CANDIDATES_FILE, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)

    print(f"\n[OK] {CANDIDATES_FILE.name} を更新しました。")


if __name__ == '__main__':
    main()

"""
Success Cache 共通モジュール
07-1 の正常評価結果を comparison_key（4フィールド）単位で保持し、
06-80 の重複判定 / 08-1 の過去評価結果復元の正本として使用する。

方針:
- identity は comparison_key（project_from / project_subject / resource_from / resource_subject）
- message_id は「今回run内の追跡」用であり identity ではない
- cache不整合は黙って無視せず SuccessCacheError で停止する

Step側でcacheのJSON構造を直接組み立てないこと。必ずこのモジュールを使用すること。
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

from common.json_utils import read_jsonl, write_jsonl

CACHE_VERSION = 1
CACHE_FILENAME = "success_cache_requirement_skill_ai_matching.jsonl"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SUCCESS_CACHE_PATH = (
    PROJECT_ROOT
    / "08-1_restore_and_merge_requirement_skill_ai_matching/01_result"
    / CACHE_FILENAME
)

COMPARISON_KEY_FIELDS = (
    "project_from",
    "project_subject",
    "resource_from",
    "resource_subject",
)

ComparisonKey = Tuple[str, str, str, str]


class SuccessCacheError(Exception):
    """Success Cacheの不整合。Cache MISSとして扱わず、呼び出し元で停止させる。"""


# ---------------------------------------------------------------- comparison_key


def build_comparison_key(
    project_from: str,
    project_subject: str,
    resource_from: str,
    resource_subject: str,
) -> ComparisonKey:
    """4フィールドから comparison_key タプルを作る。"""
    return (
        project_from or "",
        project_subject or "",
        resource_from or "",
        resource_subject or "",
    )


def comparison_key_from_diff_record(record: Dict[str, Any]) -> ComparisonKey:
    """06-80 diff_file レコード（from/subject入り）から comparison_key を作る。"""
    project_info = record.get("project_info") or {}
    resource_info = record.get("resource_info") or {}
    return build_comparison_key(
        project_info.get("from", ""),
        project_info.get("subject", ""),
        resource_info.get("from", ""),
        resource_info.get("subject", ""),
    )


def comparison_key_from_dict(value: Dict[str, Any]) -> ComparisonKey:
    """cache上の comparison_key dict をタプル化する。"""
    if not isinstance(value, dict):
        raise SuccessCacheError(f"comparison_key がdictでない: {value!r}")
    return build_comparison_key(*[value.get(field, "") for field in COMPARISON_KEY_FIELDS])


def comparison_key_to_dict(key: ComparisonKey) -> Dict[str, str]:
    """comparison_key タプルを4フィールド構造のdictへ戻す（連結文字列にしない）。"""
    return {field: key[index] for index, field in enumerate(COMPARISON_KEY_FIELDS)}


def is_complete_comparison_key(key: ComparisonKey) -> bool:
    """4フィールドすべてが非空文字列か。"""
    return all(isinstance(part, str) and part.strip() != "" for part in key)


def format_comparison_key(key: ComparisonKey) -> str:
    return " / ".join(
        f"{field}={key[index]}" for index, field in enumerate(COMPARISON_KEY_FIELDS)
    )


# ---------------------------------------------------------------- entry


def build_cache_entry(
    comparison_key: ComparisonKey,
    project_message_id: str,
    resource_message_id: str,
    required_skills: List[Any],
    optional_skills: List[Any],
    evaluation_meta: Dict[str, Any],
) -> Dict[str, Any]:
    """Success Cache 1エントリを組み立てる（07-1正常結果の再利用に必要な最小限）。"""
    return {
        "cache_version": CACHE_VERSION,
        "comparison_key": comparison_key_to_dict(comparison_key),
        "source_message_ids": {
            "project_message_id": project_message_id,
            "resource_message_id": resource_message_id,
        },
        "required_skills": required_skills,
        "optional_skills": optional_skills,
        "evaluation_meta": evaluation_meta,
    }


def _validate_skill_list(entry: Dict[str, Any], field: str, where: str) -> None:
    value = entry.get(field)
    if not isinstance(value, list):
        raise SuccessCacheError(f"{where}: 必須評価schema欠落 ({field} がlistでない)")
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise SuccessCacheError(f"{where}: {field}[{index}] がdictでない")
        if "skill" not in item or "match" not in item:
            raise SuccessCacheError(
                f"{where}: {field}[{index}] に skill/match がない"
            )


def validate_cache_entry(entry: Any, where: str) -> ComparisonKey:
    """
    1エントリを検証し comparison_key タプルを返す。
    不整合は SuccessCacheError（Cache MISS扱いにしない）。
    """
    if not isinstance(entry, dict):
        raise SuccessCacheError(f"{where}: cacheエントリがdictでない")

    version = entry.get("cache_version")
    if version != CACHE_VERSION:
        raise SuccessCacheError(
            f"{where}: cache_version不一致 (期待={CACHE_VERSION} 実際={version!r})"
        )

    if "comparison_key" not in entry:
        raise SuccessCacheError(f"{where}: comparison_key がない")
    key = comparison_key_from_dict(entry["comparison_key"])
    if not is_complete_comparison_key(key):
        raise SuccessCacheError(f"{where}: comparison_key に空値がある ({format_comparison_key(key)})")

    source_message_ids = entry.get("source_message_ids")
    if not isinstance(source_message_ids, dict):
        raise SuccessCacheError(f"{where}: source_message_ids がdictでない")
    for field in ("project_message_id", "resource_message_id"):
        value = source_message_ids.get(field)
        if not isinstance(value, str) or not value.strip():
            raise SuccessCacheError(f"{where}: source_message_ids.{field} が空")

    _validate_skill_list(entry, "required_skills", where)
    _validate_skill_list(entry, "optional_skills", where)

    if not isinstance(entry.get("evaluation_meta"), dict):
        raise SuccessCacheError(f"{where}: 必須評価schema欠落 (evaluation_meta がdictでない)")

    return key


# ---------------------------------------------------------------- load / upsert


def load_success_cache(cache_path: str) -> Dict[ComparisonKey, Dict[str, Any]]:
    """
    Success Cache を読み込み comparison_key → entry の辞書で返す。
    ファイル未存在は空cache（全件Cache MISS）として扱う。
    重複キー・schema不整合・version不一致は SuccessCacheError。
    """
    path = Path(cache_path)
    if not path.exists():
        return {}

    cache: Dict[ComparisonKey, Dict[str, Any]] = {}
    line_no = 0
    try:
        records = list(read_jsonl(str(path)))
    except ValueError as e:
        raise SuccessCacheError(f"Success Cacheのパースに失敗: {e}") from e

    for record in records:
        line_no += 1
        where = f"{path.name} 行{line_no}"
        key = validate_cache_entry(record, where)
        if key in cache:
            raise SuccessCacheError(
                f"{where}: comparison_keyがcache内に複数存在する ({format_comparison_key(key)})"
            )
        cache[key] = record
    return cache


def upsert_success_cache(
    cache_path: str,
    new_entries: List[Dict[str, Any]],
) -> Dict[str, int]:
    """
    既存cache + 今回の正常結果を comparison_key 単位で upsert し、
    同一ディレクトリ内で atomic replace する。

    - 既存の今回未更新entryは保持する
    - 同一キーに今回の成功結果がある場合だけ評価結果 / source_message_ids を差し替える
    - new_entries 内に同一 comparison_key が複数ある場合は last-write-wins にせず停止する
    - 更新に失敗した場合は旧cacheを残す（例外を送出する）
    """
    path = Path(cache_path)

    # cacheファイルへ触れる前に new_entries を全件検証する（同一キー重複は明示エラー）
    new_keys: List[ComparisonKey] = []
    seen_new_keys: Dict[ComparisonKey, int] = {}
    for index, entry in enumerate(new_entries):
        key = validate_cache_entry(entry, f"upsert対象[{index}]")
        if key in seen_new_keys:
            raise SuccessCacheError(
                "同一upsert内にcomparison_keyが複数存在する "
                f"(upsert対象[{seen_new_keys[key]}] と upsert対象[{index}]): "
                f"{format_comparison_key(key)}"
            )
        seen_new_keys[key] = index
        new_keys.append(key)

    cache = load_success_cache(str(path))
    before_count = len(cache)

    updated = 0
    inserted = 0
    for key, entry in zip(new_keys, new_entries):
        if key in cache:
            updated += 1
        else:
            inserted += 1
        cache[key] = entry

    ordered = [cache[key] for key in sorted(cache.keys())]

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".upsert.tmp")
    try:
        write_jsonl(str(tmp_path), ordered)
        os.replace(str(tmp_path), str(path))
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    return {
        "before_count": before_count,
        "after_count": len(ordered),
        "inserted": inserted,
        "updated": updated,
    }

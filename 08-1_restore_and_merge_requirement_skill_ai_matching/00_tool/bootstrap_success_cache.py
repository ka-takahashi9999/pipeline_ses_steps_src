"""
08-1 Success Cache bootstrap（一回限りの初期投入 / 通常の08-1処理とは分離）

同一runの
  07-1正常結果 (requirement_skill_ai_matching.jsonl)
  06-80 diff_file (duplicate_proposal_check_diff_file.jsonl)
を message_id ペアで join し、diff側の comparison_key を Success Cache へ保存する。

安全仕様:
- 既定は検証のみ（--dry-run 相当）。--apply を明示した場合だけcacheを作成する
- Success Cacheが既に存在する場合は上書き拒否
- Pipeline同時実行中は実行拒否
- 元データのrun日付 / 件数 / comparison_keyの空値・重複を事前検証する
- 検証NGが1件でもあれば作成せず停止する

usage:
  python3 bootstrap_success_cache.py [--expected-count 1673] [--expected-run-date 20260814]
  python3 bootstrap_success_cache.py --apply --expected-count 1673 --expected-run-date 20260814
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger
from common.success_cache import (
    SUCCESS_CACHE_PATH,
    build_cache_entry,
    comparison_key_from_dict,
    comparison_key_from_diff_record,
    is_complete_comparison_key,
    load_success_cache,
    validate_cache_entry,
)

STEP_NAME = "08-1_bootstrap_success_cache"

DEFAULT_AI_RESULT = (
    project_root
    / "07-1_requirement_skill_ai_matching/01_result/requirement_skill_ai_matching.jsonl"
)
DEFAULT_DIFF_FILE = (
    project_root
    / "06-80_duplicate_proposal_check/01_result/duplicate_proposal_check_diff_file.jsonl"
)

RUNNING_PIPELINE_PATTERNS = (
    "run_full_pipeline",
    "duplicate_proposal_check.py",
    "requirement_skill_ai_matching.py",
    "restore_and_merge_requirement_skill_ai_matching.py",
)


def build_message_id_key(record: dict) -> Tuple[str, str]:
    return (
        record.get("project_info", {}).get("message_id", ""),
        record.get("resource_info", {}).get("message_id", ""),
    )


def detect_running_pipeline() -> List[str]:
    """Pipeline関連プロセスが実行中なら該当行を返す（read-only）。"""
    hits: List[str] = []
    for pattern in RUNNING_PIPELINE_PATTERNS:
        try:
            proc = subprocess.run(
                ["pgrep", "-af", pattern],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        except OSError:
            # pgrepが使えない環境では検出不能。安全側に倒して明示エラーにする。
            raise RuntimeError("pgrepが実行できないためPipeline同時実行の確認ができません")
        for line in proc.stdout.decode("utf-8", errors="replace").splitlines():
            if not line.strip():
                continue
            if "bootstrap_success_cache.py" in line:
                continue
            hits.append(line.strip())
    return hits


def file_run_date(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y%m%d")


def validate_and_build(
    ai_result_path: Path,
    diff_path: Path,
    cache_path: Path,
    expected_count: int,
    expected_run_date: str,
    logger,
) -> Tuple[List[dict], List[str]]:
    """検証を行い、(cacheエントリ, 検証NGメッセージ) を返す。"""
    issues: List[str] = []

    # 1. Success Cache未存在
    if cache_path.exists():
        issues.append(f"Success Cacheが既に存在するため上書き拒否: {cache_path}")

    # 2. Pipeline同時実行なし
    running = detect_running_pipeline()
    if running:
        issues.append(f"Pipeline関連プロセスが実行中: {running[:3]}")

    # 3. 元データ存在
    for path, label in ((ai_result_path, "07-1正常結果"), (diff_path, "06-80 diff_file")):
        if not path.exists():
            issues.append(f"{label}が存在しない: {path}")
    if issues:
        return [], issues

    # 4. 元データのrun日付
    if expected_run_date:
        for path, label in ((ai_result_path, "07-1正常結果"), (diff_path, "06-80 diff_file")):
            actual = file_run_date(path)
            if actual != expected_run_date:
                issues.append(
                    f"{label}のrun日付不一致: 期待={expected_run_date} 実際={actual} ({path.name})"
                )

    ai_results = read_jsonl_as_list(str(ai_result_path))
    diff_records = read_jsonl_as_list(str(diff_path))
    logger.info(f"07-1正常結果={len(ai_results)}件 06-80 diff_file={len(diff_records)}件")

    # 5. 件数一致
    if expected_count and len(ai_results) != expected_count:
        issues.append(f"07-1正常結果の件数不一致: 期待={expected_count} 実際={len(ai_results)}")

    # 6. diff側 message_id ペアの一意性
    diff_map: Dict[Tuple[str, str], dict] = {}
    for record in diff_records:
        message_key = build_message_id_key(record)
        if message_key in diff_map:
            issues.append(f"diff_file内でmessage_idペアが重複: {message_key}")
            continue
        diff_map[message_key] = record

    entries: List[dict] = []
    seen_keys: Dict[Tuple[str, str, str, str], Tuple[str, str]] = {}
    join_miss = 0
    empty_key = 0

    for record in ai_results:
        message_key = build_message_id_key(record)
        diff_record = diff_map.get(message_key)
        if diff_record is None:
            join_miss += 1
            issues.append(f"07-1正常結果がdiff_fileとjoinできない: {message_key}")
            continue

        comparison_key = comparison_key_from_diff_record(diff_record)
        if not is_complete_comparison_key(comparison_key):
            empty_key += 1
            issues.append(f"comparison_keyに空値: {message_key}")
            continue

        if comparison_key in seen_keys:
            issues.append(
                f"comparison_keyが重複: {message_key} と {seen_keys[comparison_key]}"
            )
            continue
        seen_keys[comparison_key] = message_key

        required_skills = record.get(
            "required_skills", record.get("project_info", {}).get("required_skills")
        )
        optional_skills = record.get(
            "optional_skills", record.get("project_info", {}).get("optional_skills")
        )
        if required_skills is None or optional_skills is None:
            issues.append(f"required_skills/optional_skillsが欠落: {message_key}")
            continue

        entry = build_cache_entry(
            comparison_key,
            message_key[0],
            message_key[1],
            required_skills,
            optional_skills,
            record.get("evaluation_meta", {}),
        )
        try:
            validate_cache_entry(entry, f"bootstrap {message_key}")
        except Exception as e:  # SuccessCacheError含む
            issues.append(f"schema検証NG: {message_key}: {e}")
            continue
        entries.append(entry)

    logger.info(
        "検証内訳: "
        f"join対象={len(ai_results)} join失敗={join_miss} comparison_key空={empty_key} "
        f"comparison_key一意={len(seen_keys)} 生成entry={len(entries)}"
    )

    # 7. 集合一致（1件も欠落させない）
    if expected_count and len(entries) != expected_count:
        issues.append(f"生成entry件数不一致: 期待={expected_count} 実際={len(entries)}")

    return entries, issues


def main() -> None:
    parser = argparse.ArgumentParser(description="Success Cache bootstrap")
    parser.add_argument("--apply", action="store_true", help="検証PASS時にcacheを作成する")
    parser.add_argument("--dry-run", action="store_true", help="検証のみ（既定動作）")
    parser.add_argument("--expected-count", type=int, default=0)
    parser.add_argument("--expected-run-date", default="")
    parser.add_argument("--ai-result", default=str(DEFAULT_AI_RESULT))
    parser.add_argument("--diff-file", default=str(DEFAULT_DIFF_FILE))
    parser.add_argument("--cache", default=str(SUCCESS_CACHE_PATH))
    args = parser.parse_args()

    logger = get_logger(STEP_NAME)
    cache_path = Path(args.cache)

    try:
        entries, issues = validate_and_build(
            Path(args.ai_result),
            Path(args.diff_file),
            cache_path,
            args.expected_count,
            args.expected_run_date,
            logger,
        )
    except Exception as e:
        logger.error(f"検証処理に失敗: {e}")
        sys.exit(1)

    if issues:
        for issue in issues[:10]:
            logger.error(f"[NG] {issue}")
        if len(issues) > 10:
            logger.error(f"[NG] ... 他 {len(issues) - 10} 件")
        logger.error(f"検証NG {len(issues)}件のためSuccess Cacheを作成せず停止")
        sys.exit(1)

    logger.ok(f"検証PASS: 作成対象={len(entries)}件")

    if not args.apply or args.dry_run:
        logger.info("--apply が指定されていないため作成しません（検証のみ）")
        return

    ordered = sorted(entries, key=lambda e: comparison_key_from_dict(e["comparison_key"]))
    write_jsonl(str(cache_path), ordered)

    created = load_success_cache(str(cache_path))
    if len(created) != len(entries):
        logger.error(
            f"作成後の再読込件数が一致しません: 期待={len(entries)} 実際={len(created)}"
        )
        sys.exit(1)

    logger.ok(f"Success Cacheを作成: {cache_path} 件数={len(created)}")


if __name__ == "__main__":
    main()

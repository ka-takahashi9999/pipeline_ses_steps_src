"""
07-1_requirement_skill_ai_matching confirm スクリプト

確認観点:
1. 入力件数 = 正常系件数 + 失敗系件数
2. 正常系の各レコードに project_info.message_id / resource_info.message_id があること
3. required_skills / optional_skills の各要素キーが skill / match / note のみであること
4. skill 文言が元データ（03-50）から変わっていないこと
5. match が true / false のみで null がないこと
6. note が文字列で、空文字でなく、30文字以内であること
7. required_skills / optional_skills の件数が元案件データと一致すること
8. run_metadata.json の件数条件が limit 実行有無と一致すること
9. 失敗系 JSONL の必須キーと error_type が妥当であること
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.json_utils import count_jsonl, read_jsonl_as_list
from common.logger import get_logger

STEP_NAME = "07-1_requirement_skill_ai_matching_confirm"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_PAIRS = (
    project_root
    / "06-80_duplicate_proposal_check/01_result/duplicate_proposal_check.jsonl"
)
INPUT_PROJECT_SKILLS = (
    project_root
    / "03-50_extract_project_required_skills/01_result/extract_project_required_skills.jsonl"
)
OUTPUT_RESULT = STEP_DIR / "01_result/requirement_skill_ai_matching.jsonl"
OUTPUT_ERROR = STEP_DIR / "01_result/99_error_requirement_skill_ai_matching.jsonl"
RUN_METADATA = STEP_DIR / "01_result/run_metadata.json"
CONFIRM_RESULT = Path(__file__).resolve().parent / "confirm_result_requirement_skill_ai_matching.txt"
ALLOWED_ERROR_TYPES = {
    "missing_project_required_skills",
    "missing_resource_skillsheet",
    "llm_call_error",
    "llm_parse_error",
    "invalid_output_schema",
    # P0で正式導入済みの失敗系（仕様どおり分類・記録されたerrorとして受理する）
    "project_skill_count_exceeded",  # スキル40件超skip
    "llm_output_truncated",  # finish_reason=length 検知
}


def _load_project_skills_map() -> Dict[str, Dict[str, Any]]:
    pmap: Dict[str, Dict[str, Any]] = {}
    if not INPUT_PROJECT_SKILLS.exists():
        return pmap
    for rec in read_jsonl_as_list(str(INPUT_PROJECT_SKILLS)):
        mid = rec.get("message_id")
        if mid:
            pmap[str(mid)] = rec
    return pmap


def _check_skill_list(
    skills: List[Any],
    original_skills: List[Dict[str, Any]],
    field: str,
) -> List[str]:
    """スキルリストのスキーマ・整合性チェック。問題点のリストを返す。"""
    issues = []

    if not isinstance(skills, list):
        issues.append(f"{field}がリストでない")
        return issues

    if len(skills) != len(original_skills):
        issues.append(
            f"{field}件数不一致: 元={len(original_skills)} 結果={len(skills)}"
        )
        # 件数が違っても以降の要素チェックは可能な範囲でやる

    for i, item in enumerate(skills):
        prefix = f"{field}[{i}]"
        if not isinstance(item, dict):
            issues.append(f"{prefix}がdictでない")
            continue
        # キー構成チェック
        if set(item.keys()) != {"skill", "match", "note"}:
            issues.append(f"{prefix}の不正キー構成: {sorted(item.keys())}")
        # skill 文言チェック（元データと比較）
        if i < len(original_skills):
            expected_skill = original_skills[i].get("skill", "")
            if item.get("skill") != expected_skill:
                issues.append(
                    f"{prefix}skill変更検出: 元='{expected_skill}' 結果='{item.get('skill')}'"
                )
        # match チェック
        if item.get("match") not in (True, False):
            issues.append(f"{prefix}matchがtrue/false以外: {item.get('match')!r}")
        # note チェック
        note = item.get("note")
        if not isinstance(note, str) or not note.strip():
            issues.append(f"{prefix}noteが空またはnull")
        elif len(note) > 30:
            issues.append(f"{prefix}noteが30文字超: {len(note)}文字")

    return issues


def _load_run_metadata() -> Optional[Dict[str, Any]]:
    if not RUN_METADATA.exists():
        return None
    with open(RUN_METADATA, "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    logger = get_logger(STEP_NAME)
    logger.info("confirm開始")

    errors: List[str] = []
    lines: List[str] = ["=== 07-1_requirement_skill_ai_matching confirm結果 ===", ""]

    # ファイル存在確認
    for path, label in [
        (OUTPUT_RESULT, "正常系出力"),
        (OUTPUT_ERROR, "失敗系出力"),
        (RUN_METADATA, "run metadata"),
    ]:
        if not path.exists():
            msg = f"[NG] ファイルが存在しない: {path.name} ({label})"
            lines.append(msg)
            errors.append(msg)
        else:
            lines.append(f"[OK] ファイル存在確認: {path.name}")

    if errors:
        # ファイルがない場合はここで打ち切り
        _write_result(lines, errors, logger)
        return

    # 件数取得
    input_count = count_jsonl(str(INPUT_PAIRS)) if INPUT_PAIRS.exists() else 0
    ok_count = count_jsonl(str(OUTPUT_RESULT))
    err_count = count_jsonl(str(OUTPUT_ERROR))
    total_out = ok_count + err_count
    run_metadata = _load_run_metadata()

    lines += [
        "【件数サマリ】",
        f"06-80 MISS入力ペア数: {input_count}",
        f"正常系 出力件数    : {ok_count}",
        f"失敗系 出力件数    : {err_count}",
        f"出力合計           : {total_out}",
        "",
    ]

    # チェック1: run_metadata と件数整合
    required_keys = {"input_count", "processed_count", "limit", "is_limited_run"}
    if run_metadata is None:
        msg = "[NG] run_metadata.json の読み込みに失敗"
        lines.append(msg)
        errors.append(msg)
    elif not required_keys.issubset(run_metadata.keys()):
        msg = (
            "[NG] run_metadata.json の必須キー不足: "
            f"{sorted(required_keys - set(run_metadata.keys()))}"
        )
        lines.append(msg)
        errors.append(msg)
    else:
        meta_input_count = run_metadata.get("input_count")
        processed_count = run_metadata.get("processed_count")
        limit = run_metadata.get("limit")
        is_limited_run = run_metadata.get("is_limited_run")

        lines += [
            "【run_metadata】",
            f"input_count      : {meta_input_count}",
            f"processed_count  : {processed_count}",
            f"limit            : {limit}",
            f"is_limited_run   : {is_limited_run}",
            "",
        ]

        if meta_input_count != input_count:
            msg = (
                "[NG] run_metadata.input_count 不一致: "
                f"metadata={meta_input_count} actual={input_count}"
            )
            lines.append(msg)
            errors.append(msg)
        elif not isinstance(processed_count, int):
            msg = "[NG] run_metadata.processed_count が int でない"
            lines.append(msg)
            errors.append(msg)
        elif not isinstance(is_limited_run, bool):
            msg = "[NG] run_metadata.is_limited_run が bool でない"
            lines.append(msg)
            errors.append(msg)
        elif is_limited_run is False and total_out != input_count:
            msg = (
                "[NG] 非limit実行の件数不一致: "
                f"input_count={input_count} total_out={total_out}"
            )
            lines.append(msg)
            errors.append(msg)
        elif is_limited_run is True and total_out != processed_count:
            msg = (
                "[NG] limit実行の件数不一致: "
                f"processed_count={processed_count} total_out={total_out}"
            )
            lines.append(msg)
            errors.append(msg)
        else:
            lines.append("[OK] run_metadata と件数整合を確認")

    # 03-50 スキルマップ読み込み（チェック4・7用）
    project_skills_map = _load_project_skills_map()

    # 正常系レコードの詳細チェック
    if ok_count > 0:
        ok_records = read_jsonl_as_list(str(OUTPUT_RESULT))
        schema_issues: List[str] = []
        sample_size = min(50, len(ok_records))

        lines.append(f"\n【正常系スキーマチェック（全{len(ok_records)}件）】")

        for rec in ok_records:
            p_mid = rec.get("project_info", {}).get("message_id", "")
            r_mid = rec.get("resource_info", {}).get("message_id", "")

            # チェック2: message_id存在
            if not p_mid:
                schema_issues.append(f"project_info.message_idが空のレコードあり")
            if not r_mid:
                schema_issues.append(f"resource_info.message_idが空のレコードあり")

            # 03-50 元データ取得
            proj_rec = project_skills_map.get(p_mid, {})
            orig_required = proj_rec.get("required_skills") or []
            orig_optional = proj_rec.get("optional_skills") or []

            # チェック3・4・5・6・7: required_skills
            issues = _check_skill_list(
                rec.get("required_skills", []),
                orig_required,
                f"required_skills(p={p_mid})",
            )
            schema_issues.extend(issues)

            # チェック3・4・5・6・7: optional_skills
            issues = _check_skill_list(
                rec.get("optional_skills", []),
                orig_optional,
                f"optional_skills(p={p_mid})",
            )
            schema_issues.extend(issues)

        if schema_issues:
            # 最初の10件を出力
            for issue in schema_issues[:10]:
                msg = f"[NG] {issue}"
                lines.append(msg)
                errors.append(msg)
            if len(schema_issues) > 10:
                lines.append(f"  ... 他 {len(schema_issues) - 10} 件のNG")
                errors.append(f"スキーマNG合計: {len(schema_issues)}件")
        else:
            lines.append("[OK] スキーマ全チェック通過")

        # evaluation_meta の存在確認（先頭5件）
        meta_ok = all("evaluation_meta" in r for r in ok_records[:5])
        if meta_ok:
            lines.append("[OK] evaluation_meta キー存在確認（先頭5件）")
        else:
            msg = "[NG] evaluation_meta キーが一部レコードにない"
            lines.append(msg)
            errors.append(msg)

    # 失敗系レコードの確認
    if err_count > 0:
        err_records = read_jsonl_as_list(str(OUTPUT_ERROR))
        lines.append(f"\n【失敗系サマリ（全{len(err_records)}件）】")
        err_type_counts: Dict[str, int] = {}
        err_issues: List[str] = []
        for rec in err_records:
            etype = rec.get("error_type", "unknown")
            err_type_counts[etype] = err_type_counts.get(etype, 0) + 1
            p_mid = rec.get("project_info", {}).get("message_id", "")
            r_mid = rec.get("resource_info", {}).get("message_id", "")
            emsg = rec.get("error_message", "")

            if not p_mid:
                err_issues.append("失敗系: project_info.message_idが空")
            if not r_mid:
                err_issues.append("失敗系: resource_info.message_idが空")
            if not isinstance(etype, str) or not etype.strip():
                err_issues.append("失敗系: error_typeが空")
            elif etype not in ALLOWED_ERROR_TYPES:
                err_issues.append(f"失敗系: 許可外error_type='{etype}'")
            if not isinstance(emsg, str) or not emsg.strip():
                err_issues.append("失敗系: error_messageが空")
        for etype, cnt in sorted(err_type_counts.items(), key=lambda x: -x[1]):
            lines.append(f"  {etype}: {cnt}件")
        if err_issues:
            for issue in err_issues[:10]:
                msg = f"[NG] {issue}"
                lines.append(msg)
                errors.append(msg)
            if len(err_issues) > 10:
                lines.append(f"  ... 他 {len(err_issues) - 10} 件のNG")
                errors.append(f"失敗系スキーマNG合計: {len(err_issues)}件")
        else:
            lines.append("[OK] 失敗系レコード確認")

    lines += ["", "【結果】NG" if errors else "【結果】OK"]

    _write_result(lines, errors, logger)


def _write_result(lines: List[str], errors: List[str], logger: Any) -> None:
    result_text = "\n".join(lines)

    for line in lines:
        if "[NG]" in line or line.strip() == "【結果】NG":
            logger.error(line)
        else:
            logger.info(line)

    CONFIRM_RESULT.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIRM_RESULT, "w", encoding="utf-8") as f:
        f.write(result_text + "\n")

    logger.info(f"confirm結果ファイル: {CONFIRM_RESULT}")

    if errors:
        logger.error("confirm NG — Pipeline停止")
        sys.exit(1)

    logger.ok("confirm OK")


if __name__ == "__main__":
    main()

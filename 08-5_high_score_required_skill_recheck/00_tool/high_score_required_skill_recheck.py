"""
08-5_high_score_required_skill_recheck

08-4 の高スコア帯を対象に、必須スキル充足の確度を再チェックする。
案件本文（01-4）を参照し、技術領域の一致度・必須スキルの文脈評価も行う。
07-1 の判定結果は変更せず、営業可否・単価・商流等は判定しない。
"""

import argparse
import json
import sys
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_execution_time
from common.json_utils import append_jsonl, read_jsonl
from common.llm_client import call_llm
from common.logger import get_logger
from common.skill_policy import AUTO_TRUE_RECHECK_REASON, is_auto_true_skill

STEP_NAME = "08-5_high_score_required_skill_recheck"
STEP_DIR = Path(__file__).resolve().parents[1]

RECHECK_LLM_MODEL = "gpt-4o"
RECHECK_SKILLSHEET_MAX_CHARS = 10000
RECHECK_PROJECT_BODY_MAX_CHARS = 3000

INPUT_SCORE_FILES: Tuple[Tuple[str, Path], ...] = (
    (
        "100percent",
        project_root / "08-4_match_score_sort/01_result/match_score_sort_100percent.jsonl",
    ),
    (
        "80to99percent",
        project_root / "08-4_match_score_sort/01_result/match_score_sort_80to99percent.jsonl",
    ),
)
INPUT_SKILLSHEETS = (
    project_root / "04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl"
)
INPUT_CLEANED_EMAILS = (
    project_root / "01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl"
)

OUTPUT_ALL = STEP_DIR / "01_result/high_score_required_skill_recheck_all.jsonl"
OUTPUT_CONFIRMED = (
    STEP_DIR / "01_result/high_score_required_skill_recheck_confirmed.jsonl"
)
OUTPUT_HUMAN_REVIEW = (
    STEP_DIR / "01_result/high_score_required_skill_recheck_human_review.jsonl"
)
OUTPUT_NOT_CONFIRMED = (
    STEP_DIR / "01_result/high_score_required_skill_recheck_not_confirmed.jsonl"
)
OUTPUT_ERROR = (
    STEP_DIR / "01_result/99_error_high_score_required_skill_recheck.jsonl"
)

VALID_CONFIDENCES = {"confirmed", "human_review", "not_confirmed"}
STATUS_CONFIRMED = "required_skill_confirmed"
STATUS_HUMAN_REVIEW = "required_skill_human_review"
STATUS_NOT_CONFIRMED = "required_skill_not_confirmed"

SYSTEM_PROMPT = """あなたはIT人材評価の専門家です。
このstepは、07-1の判定を否定する目的ではなく、高スコア候補について必須スキル充足の確認粒度を上げる目的です。
営業可否、単価、商流、年齢、稼働、場所、外国籍は判定しないでください。
案件本文は技術領域と必須スキルの文脈理解のみに使用すること。条件面（単価・場所・年齢等）での判定は引き続き行わないこと。

【判定分類】
- confirmed: 必須スキルを満たす根拠がスキルシート上で明確
- human_review: 近い根拠はあるが、表現が曖昧、根拠が間接的、本人/営業確認が必要
- not_confirmed: 根拠がほぼない、技術スタックが明確に違う、年数が明確に不足

【営業確認前提で confirmed 固定とするスキル】
- コミュニケーション / 協調性 / 柔軟性 / 主体性 / 積極性 / 報連相 / 報告・連絡・相談 /
  チームワーク / 責任感 / 自立 / 自走 / 一人称 を主題とするスキル、および
  「スキルシートから直接判定不能な人物像・スタンス系」スキル
- これらに該当し、かつスキル文言に技術語（言語名／製品名／設計・開発・構築・運用 等の工程語）を含まない場合は、
  confidence=confirmed, recheck_match=true, reason="営業確認前提で固定" とする
- スキル文言に技術語を含む場合（例:「Javaを一人称で対応できる方」）は通常評価（技術要件本体で判定）

【重要方針】
- このstepは落としすぎない
- 判断に迷う場合は human_review にする
- 「大量データ処理」「豊富な経験」「上流経験」「大規模」など曖昧な条件は、近い経験があれば not_confirmed ではなく human_review にする
- 括弧内条件は無視しないが、曖昧な条件は人間確認扱いにできる
- 技術名・工程・類似経験がある場合は、いきなり not_confirmed にしない
- not_confirmed は、必須スキルの根拠がほぼない、技術スタックが明確に違う、年数が明確に不足する場合に限定する

【技術領域の一致度チェック】
- 案件本文から案件の主要技術領域（例: インフラ基盤構築、バックエンド開発、フロントエンド開発等）を判断する
- 要員スキルシートから要員の主要技術領域を判断する
- category_match を以下の基準で判定する:
  - match: 案件と要員の技術領域が一致、または要員が案件の技術領域に対応できる根拠がある
  - mismatch: 案件と要員の技術領域が明らかに異なり、対応できる根拠がない（例: インフラ案件にアプリ開発のみの要員）
  - unclear: 判断が難しい場合
- mismatch でも対応できそうな根拠があれば match にしてよい
- category_note に「案件: ○○ / 要員: ○○」の形式で技術領域を簡潔に記載する

【必須スキルの文脈評価】
- 各必須スキルの判定では、案件が求める文脈（例: ミドルウェアとしての構築経験）と要員の使用文脈（例: DB操作のみ）の違いを考慮する
- reason に文脈の違いがあれば明記すること（例:「OracleのDB操作経験はあるが、ミドルウェアとしての構築経験の根拠なし」）

【出力ルール】
- JSONのみ返すこと。説明文・```マーク不要
- required_skill_checks の件数は入力 required_skills と同数にする
- skill文言は入力から一切変更しない
- confidence は confirmed / human_review / not_confirmed のみ
- reason は短く、根拠または確認ポイントを書く。空文字/null禁止
- evidence はスキルシートから短い根拠断片を抜き出す。根拠がない場合は空文字
- recheck_match は confirmed と human_review なら true、not_confirmed なら false
- category_match は match / mismatch / unclear のみ
- category_note は短く技術領域を記載する。空文字/null禁止
"""

VALID_CATEGORY_MATCHES = {"match", "mismatch", "unclear"}


def _is_no_match_record(record: Dict[str, Any]) -> bool:
    return record.get("status") == "no_match"


def _load_skillsheet_map() -> Dict[str, Dict[str, Any]]:
    skillsheet_map: Dict[str, Dict[str, Any]] = {}
    for rec in read_jsonl(str(INPUT_SKILLSHEETS)):
        mid = rec.get("message_id")
        if mid:
            skillsheet_map[str(mid)] = rec
    return skillsheet_map


def _load_cleaned_email_map() -> Dict[str, str]:
    """01-4のクリーニング済みメール本文をmessage_id→body_textのmapで返す。"""
    email_map: Dict[str, str] = {}
    for rec in read_jsonl(str(INPUT_CLEANED_EMAILS)):
        mid = rec.get("message_id")
        if mid:
            body = str(rec.get("body_text") or "").strip()
            if body:
                email_map[str(mid)] = body
    return email_map


def _truncate_project_body(text: str) -> str:
    if len(text) <= RECHECK_PROJECT_BODY_MAX_CHARS:
        return text
    truncated = text[:RECHECK_PROJECT_BODY_MAX_CHARS]
    last_nl = truncated.rfind("\n")
    if last_nl > int(RECHECK_PROJECT_BODY_MAX_CHARS * 0.8):
        return truncated[:last_nl]
    return truncated


def _required_skills_from_record(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    skills = record.get("project_info", {}).get("required_skills") or []
    normalized: List[Dict[str, Any]] = []
    for item in skills:
        if isinstance(item, dict):
            normalized.append(item)
        else:
            normalized.append({"skill": str(item), "match": False, "note": ""})
    return normalized


def _skill_text(skill: Dict[str, Any]) -> str:
    return str(skill.get("skill", ""))


def _truncate_skillsheet(text: str) -> str:
    if len(text) <= RECHECK_SKILLSHEET_MAX_CHARS:
        return text
    truncated = text[:RECHECK_SKILLSHEET_MAX_CHARS]
    last_nl = truncated.rfind("\n")
    if last_nl > int(RECHECK_SKILLSHEET_MAX_CHARS * 0.8):
        return truncated[:last_nl]
    return truncated


def _build_schema(required_skills: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "required_skill_checks": [
            {
                "skill": _skill_text(skill),
                "original_match": skill.get("match") is True,
                "recheck_match": True,
                "confidence": "human_review",
                "reason": "",
                "evidence": "",
            }
            for skill in required_skills
        ],
        "category_match": "unclear",
        "category_note": "",
    }


def _build_user_prompt(
    required_skills: List[Dict[str, Any]],
    skillsheet_text: str,
    project_body_text: str,
) -> str:
    skills_input = [
        {
            "skill": _skill_text(skill),
            "original_match": skill.get("match") is True,
            "original_note": str(skill.get("note", "")),
        }
        for skill in required_skills
    ]
    prompt_parts = []
    if project_body_text:
        prompt_parts.append("【案件メール本文】\n" + project_body_text)
    prompt_parts.append(
        "【再チェック対象 required_skills】\n"
        + json.dumps(skills_input, ensure_ascii=False, indent=2)
    )
    prompt_parts.append("【要員スキルシート本文】\n" + skillsheet_text)
    prompt_parts.append(
        "上記の案件本文とスキルシートを根拠に、以下を判定してください。\n"
        "1. 各必須スキルの confidence / reason / evidence（案件が求める文脈を考慮すること）\n"
        "2. category_match / category_note（案件と要員の技術領域の一致度）"
    )
    return "\n\n".join(prompt_parts)


def _fallback_checks(
    required_skills: List[Dict[str, Any]],
    reason: str,
    evidence: str = "",
) -> List[Dict[str, Any]]:
    return [
        {
            "skill": _skill_text(skill),
            "original_match": skill.get("match") is True,
            "recheck_match": True,
            "confidence": "human_review",
            "reason": reason,
            "evidence": evidence,
        }
        for skill in required_skills
    ]


def _validate_required_skill_checks(
    required_skills: List[Dict[str, Any]],
    checks: Any,
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    if not isinstance(checks, list):
        return None, "required_skill_checksがlistでない"
    if len(checks) != len(required_skills):
        return (
            None,
            f"required_skill_checks件数不一致: 入力={len(required_skills)} 出力={len(checks)}",
        )

    normalized: List[Dict[str, Any]] = []
    for i, (orig, check) in enumerate(zip(required_skills, checks)):
        if not isinstance(check, dict):
            return None, f"required_skill_checks[{i}]がdictでない"

        expected_skill = _skill_text(orig)
        if check.get("skill") != expected_skill:
            return (
                None,
                f"required_skill_checks[{i}]のskill不一致: "
                f"入力={expected_skill!r} 出力={check.get('skill')!r}",
            )

        confidence = check.get("confidence")
        if confidence not in VALID_CONFIDENCES:
            return None, f"required_skill_checks[{i}]のconfidence不正: {confidence!r}"

        reason = check.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            return None, f"required_skill_checks[{i}]のreasonが空またはnull"

        evidence = check.get("evidence")
        if evidence is None:
            evidence = ""
        if not isinstance(evidence, str):
            evidence = str(evidence)

        normalized.append(
            {
                "skill": expected_skill,
                "original_match": orig.get("match") is True,
                "recheck_match": confidence != "not_confirmed",
                "confidence": confidence,
                "reason": reason.strip(),
                "evidence": evidence.strip(),
            }
        )
    return normalized, None


def _apply_auto_true_override(checks: List[Dict[str, Any]]) -> int:
    """営業確認前提で○固定とすべきスキルの confidence を confirmed に上書きする。

    07-1 と同じポリシー（common.skill_policy.is_auto_true_skill）を用いる。
    LLM が human_review / not_confirmed と判定しても、人物像・コミュ系・一人称系で
    技術語を含まないスキルはスキルシート根拠がなくても confirmed 固定とする。
    """
    count = 0
    for check in checks:
        skill = check.get("skill", "")
        if not is_auto_true_skill(skill):
            continue
        if check.get("confidence") != "confirmed" or not check.get("recheck_match"):
            count += 1
        check["confidence"] = "confirmed"
        check["recheck_match"] = True
        check["reason"] = AUTO_TRUE_RECHECK_REASON
    return count


def _decide_recheck_status(checks: List[Dict[str, Any]]) -> str:
    confidences = [check.get("confidence") for check in checks]
    if any(conf == "not_confirmed" for conf in confidences):
        return STATUS_NOT_CONFIRMED
    if any(conf == "human_review" for conf in confidences):
        return STATUS_HUMAN_REVIEW
    return STATUS_CONFIRMED


def _add_recheck_result(
    record: Dict[str, Any],
    source_score_band: str,
    checks: List[Dict[str, Any]],
    skillsheet_chars_used: int,
    category_match: str = "unclear",
    category_note: str = "",
    apply_auto_true_override: bool = False,
) -> Dict[str, Any]:
    result = deepcopy(record)
    # 07-1 と同じポリシーで、コミュ系・一人称系など営業確認前提の非技術スキルは
    # LLM正常応答かつschema検証済みの経路に限り confidence=confirmed 固定へ上書きする。
    if apply_auto_true_override:
        _apply_auto_true_override(checks)
    status = _decide_recheck_status(checks)
    result["source_score_band"] = source_score_band
    result["recheck_info"] = {
        "recheck_status": status,
        "model": RECHECK_LLM_MODEL,
        "skillsheet_chars_used": skillsheet_chars_used,
        "required_skill_count": len(checks),
        "confirmed_count": sum(
            1 for check in checks if check.get("confidence") == "confirmed"
        ),
        "human_review_count": sum(
            1 for check in checks if check.get("confidence") == "human_review"
        ),
        "not_confirmed_count": sum(
            1 for check in checks if check.get("confidence") == "not_confirmed"
        ),
    }
    result["required_skill_checks"] = checks
    result["category_match"] = category_match
    result["category_note"] = category_note or ""
    return result


def _make_error(
    record: Dict[str, Any],
    source_score_band: str,
    error_type: str,
    error_message: str,
) -> Dict[str, Any]:
    return {
        "project_info": {
            "message_id": record.get("project_info", {}).get("message_id", "")
        },
        "resource_info": {
            "message_id": record.get("resource_info", {}).get("message_id", "")
        },
        "source_score_band": source_score_band,
        "error_type": error_type,
        "error_message": error_message[:1000],
    }


def _extract_category_fields(
    llm_response: Dict[str, Any],
) -> Tuple[str, str]:
    """LLMレスポンスからcategory_match/category_noteを取得・正規化する。"""
    cat_match = str(llm_response.get("category_match", "unclear")).strip().lower()
    if cat_match not in VALID_CATEGORY_MATCHES:
        cat_match = "unclear"
    cat_note = str(llm_response.get("category_note") or "").strip()
    if not cat_note:
        cat_note = "判定不明"
    return cat_match, cat_note


def _process_record(
    record: Dict[str, Any],
    source_score_band: str,
    skillsheet_map: Dict[str, Dict[str, Any]],
    cleaned_email_map: Dict[str, str],
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    required_skills = _required_skills_from_record(record)
    resource_mid = str(record.get("resource_info", {}).get("message_id", ""))
    project_mid = str(record.get("project_info", {}).get("message_id", ""))
    skillsheet_rec = skillsheet_map.get(resource_mid)

    # 案件本文取得（なくてもエラーにはしない）
    project_body_raw = cleaned_email_map.get(project_mid, "")
    project_body_text = _truncate_project_body(project_body_raw) if project_body_raw else ""

    if not skillsheet_rec:
        checks = _fallback_checks(required_skills, "スキルシート欠落のため人間確認")
        return (
            _add_recheck_result(record, source_score_band, checks, 0),
            _make_error(
                record,
                source_score_band,
                "missing_resource_skillsheet",
                f"04-1にmessage_id={resource_mid}のデータなし",
            ),
        )

    raw_skillsheet = str(skillsheet_rec.get("skillsheet") or "").strip()
    if not skillsheet_rec.get("success", False) or not raw_skillsheet:
        checks = _fallback_checks(required_skills, "スキルシート取得不可のため人間確認")
        return (
            _add_recheck_result(record, source_score_band, checks, 0),
            _make_error(
                record,
                source_score_band,
                "missing_resource_skillsheet",
                "skillsheet.success=falseまたはskillsheetが空",
            ),
        )

    skillsheet_text = _truncate_skillsheet(raw_skillsheet)
    skillsheet_chars_used = len(skillsheet_text)
    schema = _build_schema(required_skills)
    user_prompt = _build_user_prompt(required_skills, skillsheet_text, project_body_text)

    try:
        llm_response = call_llm(
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user_prompt,
            response_schema=schema,
            model=RECHECK_LLM_MODEL,
            temperature=0.0,
            max_tokens=4096,
            max_retries=3,
        )
    except ValueError as e:
        checks = _fallback_checks(required_skills, "LLM出力不正のため人間確認")
        return (
            _add_recheck_result(record, source_score_band, checks, skillsheet_chars_used),
            _make_error(record, source_score_band, "llm_parse_error", str(e)),
        )
    except Exception as e:
        checks = _fallback_checks(required_skills, "LLM呼び出し失敗のため人間確認")
        return (
            _add_recheck_result(record, source_score_band, checks, skillsheet_chars_used),
            _make_error(record, source_score_band, "llm_call_error", str(e)),
        )

    checks, validation_error = _validate_required_skill_checks(
        required_skills, llm_response.get("required_skill_checks")
    )
    category_match, category_note = _extract_category_fields(llm_response)

    if validation_error:
        fallback_checks = _fallback_checks(
            required_skills, "LLM出力検証エラーのため人間確認"
        )
        return (
            _add_recheck_result(
                record, source_score_band, fallback_checks, skillsheet_chars_used,
                category_match, category_note,
            ),
            _make_error(
                record,
                source_score_band,
                "invalid_output_schema",
                validation_error,
            ),
        )

    return (
        _add_recheck_result(
            record, source_score_band, checks, skillsheet_chars_used,
            category_match, category_note, apply_auto_true_override=True,
        ),
        None,
    )


def _init_output_files() -> None:
    for path in (
        OUTPUT_ALL,
        OUTPUT_CONFIRMED,
        OUTPUT_HUMAN_REVIEW,
        OUTPUT_NOT_CONFIRMED,
        OUTPUT_ERROR,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8"):
            pass


def _write_result(record: Dict[str, Any]) -> None:
    append_jsonl(str(OUTPUT_ALL), record)
    status = record.get("recheck_info", {}).get("recheck_status")
    if status == STATUS_CONFIRMED:
        append_jsonl(str(OUTPUT_CONFIRMED), record)
    elif status == STATUS_NOT_CONFIRMED:
        append_jsonl(str(OUTPUT_NOT_CONFIRMED), record)
    else:
        append_jsonl(str(OUTPUT_HUMAN_REVIEW), record)


def main() -> None:
    parser = argparse.ArgumentParser(description="08-5 High Score Required Skill Recheck")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="処理件数上限（小規模テスト用）。省略時は全件処理",
    )
    args = parser.parse_args()

    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))

    for _, path in INPUT_SCORE_FILES:
        if not path.exists():
            logger.error(f"入力ファイルが見つかりません: {path}")
            sys.exit(1)
    if not INPUT_SKILLSHEETS.exists():
        logger.error(f"入力ファイルが見つかりません: {INPUT_SKILLSHEETS}")
        sys.exit(1)
    if not INPUT_CLEANED_EMAILS.exists():
        logger.error(f"入力ファイルが見つかりません: {INPUT_CLEANED_EMAILS}")
        sys.exit(1)

    _init_output_files()
    skillsheet_map = _load_skillsheet_map()
    logger.info(f"04-1スキルシート読み込み完了: {len(skillsheet_map)}件")
    cleaned_email_map = _load_cleaned_email_map()
    logger.info(f"01-4クリーニング済みメール読み込み完了: {len(cleaned_email_map)}件")

    processed_count = 0
    error_count = 0
    skipped_no_match_count = 0
    start_time = time.time()

    for source_score_band, input_path in INPUT_SCORE_FILES:
        for record in read_jsonl(str(input_path)):
            if _is_no_match_record(record):
                skipped_no_match_count += 1
                continue
            if args.limit is not None and processed_count >= args.limit:
                break

            try:
                result_record, error_record = _process_record(
                    record, source_score_band, skillsheet_map, cleaned_email_map
                )
            except Exception as e:
                result_record = _add_recheck_result(
                    record,
                    source_score_band,
                    _fallback_checks(
                        _required_skills_from_record(record),
                        "予期しないエラーのため人間確認",
                    ),
                    0,
                )
                error_record = _make_error(
                    record, source_score_band, "unexpected_error", str(e)
                )

            _write_result(result_record)
            processed_count += 1

            if error_record:
                append_jsonl(str(OUTPUT_ERROR), error_record)
                error_count += 1

            if processed_count % 10 == 0:
                logger.info(
                    f"処理中: processed={processed_count} errors={error_count}"
                )

        if args.limit is not None and processed_count >= args.limit:
            break

    elapsed = time.time() - start_time
    write_execution_time(
        str(dirs["execution_time"]),
        "high_score_required_skill_recheck",
        elapsed,
        processed_count,
    )
    logger.info(
        "完了: "
        f"processed={processed_count} errors={error_count} "
        f"skipped_no_match={skipped_no_match_count} elapsed={elapsed:.1f}s"
    )


if __name__ == "__main__":
    main()

"""
07-1_requirement_skill_ai_matching
06-12 通過ペアに対し、案件の required_skills / optional_skills を
要員スキルシート本文を根拠に LLM で評価する。

LLM使用許可step。手動実行推奨（nohup使用）。
小規模テスト: python3 requirement_skill_ai_matching.py --limit 100
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_execution_time
from common.json_utils import append_jsonl, read_jsonl
from common.llm_client import call_llm
from common.logger import get_logger
from common.skill_policy import (
    AUTO_TRUE_NOTE,
    TECHNICAL_HINT_KEYWORDS,
    is_auto_true_skill,
)

STEP_NAME = "07-1_requirement_skill_ai_matching"
STEP_DIR = Path(__file__).resolve().parents[1]
LLM_MODEL = "gpt-4o-mini"

INPUT_PAIRS = (
    project_root
    / "06-80_duplicate_proposal_check/01_result/duplicate_proposal_check.jsonl"
)
INPUT_PROJECT_SKILLS = (
    project_root
    / "03-50_extract_project_required_skills/01_result/extract_project_required_skills.jsonl"
)
INPUT_SKILLSHEETS = (
    project_root / "04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl"
)

OUTPUT_RESULT = STEP_DIR / "01_result/requirement_skill_ai_matching.jsonl"
OUTPUT_ERROR = STEP_DIR / "01_result/99_error_requirement_skill_ai_matching.jsonl"
OUTPUT_RUN_METADATA = STEP_DIR / "01_result/run_metadata.json"

_AMBIGUOUS_MODIFIER_PATTERNS = [
    "を一人称で対応できる方",
    "で一人称で対応できる方",
    "を一人称で対応可能",
    "で一人称で対応可能",
    "を一人称で推進できる方",
    "で一人称で推進できる方",
    "一人称で対応できる方",
    "一人称で対応可能",
    "一人称で推進できる方",
    "対応できる方",
    "推進できる方",
    "対応可能",
    "可能な方",
]

SYSTEM_PROMPT = """あなたはIT人材評価の専門家です。
案件の必須スキル・尚可スキル一覧を要員のスキルシート本文を根拠として評価してください。

【評価ルール】
- 各skillに対して、スキルシートに根拠があればmatch=true、なければmatch=false
- 経験年数・特定技術など定量・技術要件は本文根拠がなければfalse
- コミュニケーション能力・協調性・報連相など営業確認前提の非技術要件そのものはtrue固定扱い
- 技術語・工程語を含むskillは、曖昧修飾句を無視して技術要件本体だけで判定する
- 「一人称」「対応できる方」「推進できる方」「可能な方」などの曖昧修飾句だけを理由にtrueにしない
- noteには判定根拠を1行30文字以内で必ず記載すること
- 固定trueのnote例: "営業確認前提で固定true"
- 技術スキルのnote例: "Scala経験の記載あり" / "該当経験の記載なし"
- matchはtrue/falseのみ。nullを返してはならない
- noteはnull禁止・空文字禁止・30文字以内厳守
- skillキーの文言は絶対に変更禁止（値のコピーは正確に）
- JSONのみを返すこと。説明文・```マーク不要

【除外条件・レベル要件の厳密判定】
- skill内に「※〜対象外」「〜のみは不可」「〜を除く」等の除外条件がある場合、除外対象に該当する経験しか持たない要員はmatch=falseとすること
  例: 「PL経験(※PMOのみの方は対象外)」→ PMO経験のみでPL経験がなければfalse
- 「ビジネスレベル」「〜年以上」「上級」等のレベル・年数指定がある場合、スキルシートにそのレベルを裏付ける具体的根拠（点数・年数・実務記載）がなければmatch=falseとすること
  例: 「ビジネスレベルの英語力」→ 語学欄に「英語」とだけ記載されレベル不明ならfalse
- 単語の存在だけでtrueにしない。要求されている水準を満たす根拠があるかを確認すること"""


def _has_technical_focus(skill: str) -> bool:
    if re.search(r"[A-Za-z0-9][A-Za-z0-9#+./_-]*", skill):
        return True
    return any(keyword in skill for keyword in TECHNICAL_HINT_KEYWORDS)


def _extract_judgement_focus(skill: str) -> str:
    if not _has_technical_focus(skill):
        return skill

    focus = skill
    for pattern in _AMBIGUOUS_MODIFIER_PATTERNS:
        if pattern in focus:
            focus = focus.split(pattern, 1)[0]
            break
    return focus.strip(" 、,。")


def _truncate_skillsheet(text: str, max_chars: int = 5000) -> str:
    """改行単位で切り詰める。精度を落とす粗い切り捨ては避ける。"""
    if len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    last_nl = truncated.rfind("\n")
    if last_nl > int(max_chars * 0.8):
        return truncated[:last_nl] + "\n...(以下省略)"
    return truncated + "...(以下省略)"


def _build_user_prompt(
    required_skills: List[Dict[str, Any]],
    optional_skills: List[Dict[str, Any]],
    skillsheet_text: str,
) -> str:
    skills_input = {
        "required_skills": [
            {
                "skill": s["skill"],
                "judgement_focus": _extract_judgement_focus(s["skill"]),
            }
            for s in required_skills
        ],
        "optional_skills": [
            {
                "skill": s["skill"],
                "judgement_focus": _extract_judgement_focus(s["skill"]),
            }
            for s in optional_skills
        ],
    }
    return (
        "【評価対象スキル一覧（skillの文言は変更禁止）】\n"
        + json.dumps(skills_input, ensure_ascii=False, indent=2)
        + "\n\n【要員スキルシート本文】\n"
        + skillsheet_text
        + "\n\njudgement_focus が skill と異なる場合は、曖昧修飾句を除いた"
        " judgement_focus だけを根拠に判定すること。"
        + "\n上記スキルシートを根拠に各skillのmatch(true/false)とnote(30文字以内)を"
        "埋めたJSONを返すこと。skill文言は絶対に変更禁止。"
    )


def _validate_skills(
    original: List[Dict[str, Any]],
    result: List[Any],
    field: str,
) -> Optional[str]:
    """スキルリストの出力スキーマを検証。エラー文字列を返す（問題なしはNone）。"""
    if not isinstance(result, list):
        return f"{field}がリストでない"
    if len(original) != len(result):
        return f"{field}の件数不一致: 元={len(original)} 結果={len(result)}"
    for i, (orig, res) in enumerate(zip(original, result)):
        if not isinstance(res, dict):
            return f"{field}[{i}]がdictでない"
        if set(res.keys()) != {"skill", "match", "note"}:
            return f"{field}[{i}]の不正なキー構成: {sorted(res.keys())}"
        if res.get("skill") != orig["skill"]:
            return (
                f"{field}[{i}]のskillが変更された: "
                f"元='{orig['skill']}' 結果='{res.get('skill')}'"
            )
        if res.get("match") not in (True, False):
            return f"{field}[{i}]のmatchがtrue/false以外: {res.get('match')!r}"
        note = res.get("note")
        if not isinstance(note, str) or not note.strip():
            return f"{field}[{i}]のnoteが空またはnull"
        if len(note) > 30:
            return f"{field}[{i}]のnoteが30文字超: {len(note)}文字 '{note}'"
    return None


def _count_soft_auto_true(skills: List[Dict[str, Any]]) -> int:
    return sum(
        1 for s in skills if is_auto_true_skill(s.get("skill", "")) and s.get("match") is True
    )


def _apply_soft_skill_auto_true(skills: List[Dict[str, Any]]) -> int:
    count = 0
    for skill in skills:
        if is_auto_true_skill(skill.get("skill", "")):
            if (
                skill.get("match") is not True
                or skill.get("note") != AUTO_TRUE_NOTE
            ):
                count += 1
            skill["match"] = True
            skill["note"] = AUTO_TRUE_NOTE
    return count


def _classify_validation_error(err_msg: str) -> str:
    parse_like_markers = [
        "リストでない",
        "dictでない",
        "不正なキー構成",
    ]
    if any(marker in err_msg for marker in parse_like_markers):
        return "llm_parse_error"
    return "invalid_output_schema"


def _make_error(p_mid: str, r_mid: str, etype: str, emsg: str) -> Dict[str, Any]:
    return {
        "project_info": {"message_id": p_mid},
        "resource_info": {"message_id": r_mid},
        "error_type": etype,
        "error_message": emsg,
    }


def process_pair(
    pair: Dict[str, Any],
    project_skills_map: Dict[str, Any],
    skillsheet_map: Dict[str, Any],
    logger: Any,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    1ペアを処理。
    Returns: (result_record, error_record) — どちらか一方がNoneでない。
    """
    p_mid = pair.get("project_info", {}).get("message_id", "")
    r_mid = pair.get("resource_info", {}).get("message_id", "")

    # 03-50 join
    proj_rec = project_skills_map.get(p_mid)
    if not proj_rec:
        return None, _make_error(p_mid, r_mid, "missing_project_required_skills",
                                  f"03-50にmessage_id={p_mid}のデータなし")

    required_skills: List[Dict[str, Any]] = proj_rec.get("required_skills") or []
    optional_skills: List[Dict[str, Any]] = proj_rec.get("optional_skills") or []

    # 04-1 join
    ss_rec = skillsheet_map.get(r_mid)
    if not ss_rec:
        return None, _make_error(p_mid, r_mid, "missing_resource_skillsheet",
                                  f"04-1にmessage_id={r_mid}のデータなし")
    if not ss_rec.get("success", False):
        return None, _make_error(p_mid, r_mid, "missing_resource_skillsheet",
                                  "skillsheet.success=false")
    ss_text = ss_rec.get("skillsheet", "").strip()
    if not ss_text:
        return None, _make_error(p_mid, r_mid, "missing_resource_skillsheet",
                                  "skillsheetが空")

    skillsheet_source = ss_rec.get("source", "unknown")
    ss_truncated = _truncate_skillsheet(ss_text)

    # スキーマテンプレート（LLMへの出力形式ヒント）
    schema = {
        "required_skills": [
            {"skill": s["skill"], "match": False, "note": ""}
            for s in required_skills
        ],
        "optional_skills": [
            {"skill": s["skill"], "match": False, "note": ""}
            for s in optional_skills
        ],
    }

    user_prompt = _build_user_prompt(required_skills, optional_skills, ss_truncated)

    try:
        llm_resp = call_llm(
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user_prompt,
            response_schema=schema,
            model=LLM_MODEL,
            temperature=0.0,
            max_tokens=2048,
            max_retries=3,
        )
    except ValueError as e:
        return None, _make_error(p_mid, r_mid, "llm_parse_error", str(e)[:300])
    except Exception as e:
        return None, _make_error(p_mid, r_mid, "llm_call_error", str(e)[:1000])

    res_required = llm_resp.get("required_skills")
    res_optional = llm_resp.get("optional_skills")

    if res_required is None or res_optional is None:
        return None, _make_error(
            p_mid, r_mid, "llm_parse_error",
            "レスポンスにrequired_skills/optional_skillsキーなし",
        )

    # required_skills 検証
    err_msg = _validate_skills(required_skills, res_required, "required_skills")
    if err_msg:
        return None, _make_error(
            p_mid, r_mid, _classify_validation_error(err_msg), err_msg
        )

    # optional_skills 検証
    err_msg = _validate_skills(optional_skills, res_optional, "optional_skills")
    if err_msg:
        return None, _make_error(
            p_mid, r_mid, _classify_validation_error(err_msg), err_msg
        )

    soft_count = _apply_soft_skill_auto_true(res_required)
    soft_count += _apply_soft_skill_auto_true(res_optional)

    result = {
        "project_info": {"message_id": p_mid},
        "resource_info": {"message_id": r_mid},
        "required_skills": res_required,
        "optional_skills": res_optional,
        "evaluation_meta": {
            "skillsheet_source": skillsheet_source,
            "llm_model": LLM_MODEL,
            "soft_skill_auto_true_count": soft_count,
        },
    }
    return result, None


def main() -> None:
    parser = argparse.ArgumentParser(description="07-1 Requirement Skill AI Matching")
    parser.add_argument(
        "--limit", type=int, default=None,
        help="処理件数上限（小規模テスト用）。省略時は全件処理"
    )
    args = parser.parse_args()

    logger = get_logger(STEP_NAME)
    logger.info(f"開始 limit={args.limit}")

    dirs = ensure_result_dirs(str(STEP_DIR))

    # 入力ファイル存在確認
    for path, label in [
        (INPUT_PAIRS, "06-20新規ペア"),
        (INPUT_PROJECT_SKILLS, "03-50プロジェクトスキル"),
        (INPUT_SKILLSHEETS, "04-1スキルシート"),
    ]:
        if not path.exists():
            logger.error(f"入力ファイルが見つかりません: {path} ({label})")
            sys.exit(1)

    # データ読み込み
    logger.info("03-50 プロジェクトスキル読み込み中...")
    project_skills_map: Dict[str, Any] = {}
    for rec in read_jsonl(str(INPUT_PROJECT_SKILLS)):
        mid = rec.get("message_id")
        if mid:
            project_skills_map[str(mid)] = rec
    logger.info(f"03-50 完了: {len(project_skills_map)}件")

    logger.info("04-1 スキルシート読み込み中...")
    skillsheet_map: Dict[str, Any] = {}
    for rec in read_jsonl(str(INPUT_SKILLSHEETS)):
        mid = rec.get("message_id")
        if mid:
            skillsheet_map[str(mid)] = rec
    logger.info(f"04-1 完了: {len(skillsheet_map)}件")

    input_count = 0
    if INPUT_PAIRS.exists():
        for _ in read_jsonl(str(INPUT_PAIRS)):
            input_count += 1

    # 出力ファイルを初期化
    OUTPUT_RESULT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_ERROR.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_RESULT, "w", encoding="utf-8"):
        pass
    with open(OUTPUT_ERROR, "w", encoding="utf-8"):
        pass

    ok_count = 0
    err_count = 0
    start_time = time.time()

    for i, pair in enumerate(read_jsonl(str(INPUT_PAIRS))):
        if args.limit is not None and i >= args.limit:
            break

        p_mid = pair.get("project_info", {}).get("message_id", f"idx{i}")
        r_mid = pair.get("resource_info", {}).get("message_id", f"idx{i}")

        try:
            result, error = process_pair(pair, project_skills_map, skillsheet_map, logger)
        except Exception as e:
            logger.error(f"予期しないエラー pair={i}: {e}", message_id=p_mid)
            error = _make_error(p_mid, r_mid, "llm_call_error", str(e)[:1000])
            result = None

        if result is not None:
            append_jsonl(str(OUTPUT_RESULT), result)
            ok_count += 1
            logger.ok(
                f"[{i + 1}] OK p={p_mid} r={r_mid}",
                message_id=p_mid,
            )
        else:
            append_jsonl(str(OUTPUT_ERROR), error)
            err_count += 1
            logger.warn(
                f"[{i + 1}] ERR type={error.get('error_type')} p={p_mid}",
                message_id=p_mid,
            )

    elapsed = time.time() - start_time
    total = ok_count + err_count

    run_metadata = {
        "input_count": input_count,
        "processed_count": total,
        "limit": args.limit,
        "is_limited_run": args.limit is not None,
    }
    with open(OUTPUT_RUN_METADATA, "w", encoding="utf-8") as f:
        json.dump(run_metadata, f, ensure_ascii=False, indent=2)

    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)
    logger.info(
        f"完了 total={total} ok={ok_count} err={err_count} elapsed={elapsed:.1f}s"
    )


if __name__ == "__main__":
    main()

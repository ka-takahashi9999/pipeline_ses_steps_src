"""
07-1_requirement_skill_ai_matching
06-80 の新規(Cache MISS)ペアに対し、案件の required_skills / optional_skills を
要員スキルシート本文を根拠に LLM で評価する。

LLM使用許可step。手動実行推奨（nohup使用）。
小規模テスト: python3 requirement_skill_ai_matching.py --limit 100

入力スキルシートは 04-2 normalize_skillsheets_text。
04-1 raw skillsheet を入力にしていた旧実装は削除済みで、本ファイルが 07-1 唯一のactive実装。
"""

import argparse
import copy
import hashlib
import json
import math
import os
import re
import sys
import time
from collections import defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Sequence, Set, Tuple

project_root = Path(__file__).resolve().parents[3]
tool_dir = Path(__file__).resolve().parent.parent
normalized_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(tool_dir))
sys.path.insert(0, str(normalized_dir))

from common.file_utils import ensure_result_dirs, write_execution_time
from common.json_utils import append_jsonl, read_jsonl, write_jsonl
from common.llm_client import (
    LLMOutputTruncatedError,
    build_chat_completion_payload,
    call_llm,
)
from common.logger import get_logger
from common.success_cache import (
    SUCCESS_CACHE_PATH,
    comparison_key_from_diff_record,
    load_success_cache,
)
from common.skillsheet_ai_context import build_skillsheet_ai_context
from common.skill_policy import (
    AUTO_TRUE_NOTE,
    TECHNICAL_HINT_KEYWORDS,
    is_auto_true_skill,
)
from config import CONCURRENT_INITIAL, CONCURRENT_MAX, ENABLE_07_1_CONCURRENT
from retention_guard import build_retention_sidecar

STEP_NAME = "07-1_requirement_skill_ai_matching"
STEP_DIR = Path(__file__).resolve().parents[2]
LLM_MODEL = "gpt-4o-mini"
MAX_PROJECT_SKILLS_PER_PAIR = 40
LLM_TELEMETRY_CONTEXT = {
    "step": STEP_NAME,
    "output_dir": str(STEP_DIR / "99_execution_time"),
}

# 表示用note（09-1のメール表示に使う判定根拠）の上限文字数
NOTE_MAX_CHARS = 30

INPUT_PAIRS = (
    project_root
    / "06-80_duplicate_proposal_check/01_result/duplicate_proposal_check.jsonl"
)
INPUT_PROJECT_SKILLS = (
    project_root
    / "03-50_extract_project_required_skills/01_result/extract_project_required_skills.jsonl"
)
INPUT_SKILLSHEETS = (
    project_root / "04-2_normalize_skillsheets_text/01_result/normalize_skillsheets_text.jsonl"
)
INPUT_DUPLICATE_PAIRS = (
    project_root
    / "06-80_duplicate_proposal_check/01_result/"
    "99_duplicate_duplicate_proposal_check.jsonl"
)
INPUT_DIFF_FILE = (
    project_root
    / "06-80_duplicate_proposal_check/01_result/duplicate_proposal_check_diff_file.jsonl"
)
SUCCESS_CACHE_FILE = SUCCESS_CACHE_PATH

OUTPUT_RESULT = STEP_DIR / "01_result/requirement_skill_ai_matching.jsonl"
OUTPUT_ERROR = STEP_DIR / "01_result/99_error_requirement_skill_ai_matching.jsonl"
OUTPUT_RUN_METADATA = STEP_DIR / "01_result/run_metadata.json"
OUTPUT_RETENTION_SIDECAR = (
    STEP_DIR / "01_result/requirement_skill_retention_candidates.jsonl"
)
CONCURRENT_CHECKPOINT_ROOT = STEP_DIR / "01_result/concurrent_checkpoints"
MAX_CONCURRENCY_HARD_LIMIT = 4

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


def _normalize_note_lengths(result: List[Any]) -> int:
    """表示用noteの文字数超過だけを上限文字数へ切り詰める。切り詰めた件数を返す。

    補正対象は「非空の文字列だが NOTE_MAX_CHARS を超えた note」のみ。
    null / 空文字 / 非文字列 は補正せず、_validate_skills でerrorとして扱う
    （schema validationは緩めない）。
    """
    truncated = 0
    if not isinstance(result, list):
        return truncated
    for item in result:
        if not isinstance(item, dict):
            continue
        note = item.get("note")
        if not isinstance(note, str) or not note.strip():
            continue
        if len(note) > NOTE_MAX_CHARS:
            item["note"] = note[:NOTE_MAX_CHARS]
            truncated += 1
    return truncated


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
        match = res.get("match")
        if not isinstance(match, bool):
            # 1 / 0 は Python上 True / False と等価比較されるため型で判定する
            return f"{field}[{i}]のmatchがtrue/false以外: {match!r}"
        note = res.get("note")
        if not isinstance(note, str) or not note.strip():
            return f"{field}[{i}]のnoteが空またはnull"
        if len(note) > NOTE_MAX_CHARS:
            return (
                f"{field}[{i}]のnoteが{NOTE_MAX_CHARS}文字超: "
                f"{len(note)}文字 '{note}'"
            )
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
    stats: Optional[Dict[str, int]] = None,
    llm_call: Optional[Callable[..., Dict[str, Any]]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    1ペアを処理。
    Returns: (result_record, error_record) — どちらか一方がNoneでない。

    stats を渡すと note_truncated_count（表示用note切り詰め件数）を加算する。
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
    total_skill_count = len(required_skills) + len(optional_skills)
    if total_skill_count > MAX_PROJECT_SKILLS_PER_PAIR:
        return None, _make_error(
            p_mid,
            r_mid,
            "project_skill_count_exceeded",
            "project skills count exceeded: "
            f"required={len(required_skills)} optional={len(optional_skills)} "
            f"total={total_skill_count} limit={MAX_PROJECT_SKILLS_PER_PAIR}",
        )

    # 04-2 join
    ss_rec = skillsheet_map.get(r_mid)
    if not ss_rec:
        return None, _make_error(p_mid, r_mid, "missing_resource_skillsheet",
                                  f"04-2にmessage_id={r_mid}のデータなし")
    if not ss_rec.get("success", False):
        return None, _make_error(p_mid, r_mid, "missing_resource_skillsheet",
                                  "skillsheet.success=false")
    ss_text = ss_rec.get("skillsheet", "").strip()
    if not ss_text:
        return None, _make_error(p_mid, r_mid, "missing_resource_skillsheet",
                                  "skillsheetが空")

    skillsheet_source = ss_rec.get("source", "unknown")
    ss_context = build_skillsheet_ai_context(ss_text)
    ss_truncated = _truncate_skillsheet(ss_context)

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
        execute_llm = llm_call or call_llm
        llm_resp = execute_llm(
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user_prompt,
            response_schema=schema,
            model=LLM_MODEL,
            temperature=0.0,
            max_tokens=2048,
            max_retries=3,
            telemetry_context=LLM_TELEMETRY_CONTEXT,
        )
    except LLMOutputTruncatedError as e:
        return None, _make_error(p_mid, r_mid, "llm_output_truncated", str(e)[:300])
    except ValueError as e:
        return None, _make_error(p_mid, r_mid, "llm_parse_error", str(e)[:300])
    except Exception as e:
        return None, _make_error(p_mid, r_mid, "llm_call_error", str(e)[:1000])

    res_required = llm_resp.get("required_skills")
    res_optional = llm_resp.get("optional_skills")

    # JSON parseは成功しているためschema不正として扱う（llm_parse_errorはparse失敗のみ）
    if res_required is None or res_optional is None:
        return None, _make_error(
            p_mid, r_mid, "invalid_output_schema",
            "レスポンスにrequired_skills/optional_skillsキーなし",
        )

    # 表示用noteの文字数超過のみ検証前に正規化（schema検証自体は緩めない）
    note_truncated = _normalize_note_lengths(res_required)
    note_truncated += _normalize_note_lengths(res_optional)
    if note_truncated:
        if stats is not None:
            stats["note_truncated_count"] = (
                stats.get("note_truncated_count", 0) + note_truncated
            )
        logger.warn(
            f"noteが{NOTE_MAX_CHARS}文字超のため切り詰め: {note_truncated}件 "
            f"p={p_mid} r={r_mid}",
            message_id=p_mid,
        )

    # required_skills 検証（parse成功後のschema不正は invalid_output_schema）
    err_msg = _validate_skills(required_skills, res_required, "required_skills")
    if err_msg:
        return None, _make_error(p_mid, r_mid, "invalid_output_schema", err_msg)

    # optional_skills 検証
    err_msg = _validate_skills(optional_skills, res_optional, "optional_skills")
    if err_msg:
        return None, _make_error(p_mid, r_mid, "invalid_output_schema", err_msg)

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


def _json_hash(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _request_identity(ordinal: int, project_mid: str, resource_mid: str) -> str:
    raw = f"{ordinal}\0{project_mid}\0{resource_mid}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _request_body_hash(call_kwargs: Dict[str, Any]) -> str:
    payload = build_chat_completion_payload(
        call_kwargs["system_prompt"],
        call_kwargs["user_prompt"],
        call_kwargs["response_schema"],
        call_kwargs.get("model", "gpt-4o-mini"),
        call_kwargs.get("temperature", 0.0),
        call_kwargs.get("max_tokens", 1024),
    )
    return _json_hash(payload)


def _skill_contract_hash(required: Any, optional: Any) -> str:
    return _json_hash(
        {
            "required_skills": [
                item.get("skill") if isinstance(item, dict) else None
                for item in (required if isinstance(required, list) else [])
            ],
            "optional_skills": [
                item.get("skill") if isinstance(item, dict) else None
                for item in (optional if isinstance(optional, list) else [])
            ],
        }
    )


def _capture_response(response_schema: Dict[str, Any]) -> Dict[str, Any]:
    response = copy.deepcopy(response_schema)
    for field in ("required_skills", "optional_skills"):
        for item in response.get(field, []):
            item["match"] = False
            item["note"] = "request contract capture"
    return response


def capture_request_contract(
    pair: Dict[str, Any],
    project_skills_map: Dict[str, Any],
    skillsheet_map: Dict[str, Any],
    logger: Any,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """process_pair自身からAPI kwargsを取得する。APIは呼ばない。"""
    captured: Dict[str, Any] = {}

    def capture_call(**kwargs: Any) -> Dict[str, Any]:
        captured.update(kwargs)
        return _capture_response(kwargs["response_schema"])

    _, error = process_pair(
        pair,
        project_skills_map,
        skillsheet_map,
        logger,
        llm_call=capture_call,
    )
    return (captured or None), error


def build_concurrent_items(
    pairs: Sequence[Dict[str, Any]],
    project_skills_map: Dict[str, Any],
    skillsheet_map: Dict[str, Any],
    logger: Any,
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for ordinal, pair in enumerate(pairs):
        project_mid = str(pair.get("project_info", {}).get("message_id", ""))
        resource_mid = str(pair.get("resource_info", {}).get("message_id", ""))
        call_kwargs, preflight_error = capture_request_contract(
            pair, project_skills_map, skillsheet_map, logger
        )
        request_hash = (
            _request_body_hash(call_kwargs)
            if call_kwargs is not None
            else _json_hash({"no_api_request": preflight_error})
        )
        response_schema = (
            call_kwargs.get("response_schema", {})
            if call_kwargs is not None
            else {}
        )
        items.append(
            {
                "ordinal": ordinal,
                "project_message_id": project_mid,
                "resource_message_id": resource_mid,
                "request_identity": _request_identity(
                    ordinal, project_mid, resource_mid
                ),
                "request_body_sha256": request_hash,
                "skill_contract_sha256": (
                    _skill_contract_hash(
                        response_schema.get("required_skills"),
                        response_schema.get("optional_skills"),
                    )
                    if call_kwargs is not None
                    else ""
                ),
                "api_request": call_kwargs is not None,
                "is_project_warm_one": False,
                "pair": pair,
                "preflight_error": preflight_error,
            }
        )

    first_request_by_project: Dict[str, int] = {}
    for item in items:
        if item["api_request"]:
            first_request_by_project.setdefault(
                item["project_message_id"], item["ordinal"]
            )
    for item in items:
        item["is_project_warm_one"] = bool(
            item["api_request"]
            and item["ordinal"]
            == first_request_by_project.get(item["project_message_id"])
        )
    return items


def concurrent_manifest_record(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ordinal": item["ordinal"],
        "project_message_id": item["project_message_id"],
        "resource_message_id": item["resource_message_id"],
        "request_identity": item["request_identity"],
        "request_body_sha256": item["request_body_sha256"],
        "skill_contract_sha256": item["skill_contract_sha256"],
        "api_request": item["api_request"],
        "is_project_warm_one": item["is_project_warm_one"],
    }


def load_current_cache_hit_results() -> List[Dict[str, Any]]:
    """今回runのCache HITをSuccess Cacheからread-onlyで復元する。"""
    for path, label in (
        (INPUT_DUPLICATE_PAIRS, "06-80 Cache HIT pair"),
        (INPUT_DIFF_FILE, "06-80 diff file"),
    ):
        if not path.exists():
            raise RuntimeError("{}が見つかりません: {}".format(label, path))

    hit_keys = {
        (
            str(row.get("project_info", {}).get("message_id", "")),
            str(row.get("resource_info", {}).get("message_id", "")),
        )
        for row in read_jsonl(str(INPUT_DUPLICATE_PAIRS))
    }
    cache = load_success_cache(str(SUCCESS_CACHE_FILE))
    restored: List[Dict[str, Any]] = []
    for diff_record in read_jsonl(str(INPUT_DIFF_FILE)):
        message_key = (
            str(diff_record.get("project_info", {}).get("message_id", "")),
            str(diff_record.get("resource_info", {}).get("message_id", "")),
        )
        if message_key not in hit_keys:
            continue
        cache_entry = cache.get(comparison_key_from_diff_record(diff_record))
        if cache_entry is None:
            raise RuntimeError(
                "Cache HITに対応するSuccess Cache entryがありません: {} / {}".format(
                    message_key[0], message_key[1]
                )
            )
        restored.append(
            {
                "project_info": {"message_id": message_key[0]},
                "resource_info": {"message_id": message_key[1]},
                "required_skills": copy.deepcopy(
                    cache_entry.get("required_skills", [])
                ),
                "optional_skills": copy.deepcopy(
                    cache_entry.get("optional_skills", [])
                ),
                "evaluation_meta": copy.deepcopy(
                    cache_entry.get("evaluation_meta", {})
                ),
                "duplicate_proposal_check": True,
            }
        )
    return restored


def cache_hit_results_for_run(limit: Optional[int]) -> List[Dict[str, Any]]:
    """limited runでは未処理のCache HITをsidecarへ混入させない。"""
    return [] if limit is not None else load_current_cache_hit_results()


class AdaptiveConcurrency:
    def __init__(self, initial: int, maximum: int):
        if (
            initial < 1
            or maximum < initial
            or maximum > MAX_CONCURRENCY_HARD_LIMIT
        ):
            raise ValueError(
                "concurrencyは1 <= initial <= max <= {}".format(
                    MAX_CONCURRENCY_HARD_LIMIT
                )
            )
        self.current = initial
        self.maximum = maximum
        self.success_streak = 0

    @staticmethod
    def _positive_number(value: Any) -> Optional[float]:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if number >= 0 else None

    def observe(self, checkpoint: Dict[str, Any]) -> None:
        telemetry = checkpoint.get("telemetry") or {}
        if telemetry.get("rate_limit_429_count", 0):
            self.success_streak = 0
            self.current = max(1, self.current - 1)
            return
        if telemetry.get("api_failure") or checkpoint.get("status") != "success":
            self.success_streak = 0
            self.current = max(1, self.current - 1)
            return
        if float(telemetry.get("latency_seconds", 0.0)) > 45.0:
            self.success_streak = 0
            self.current = max(1, self.current - 1)
            return
        headers = telemetry.get("rate_limit_headers") or {}
        remaining_requests = self._positive_number(
            headers.get("x-ratelimit-remaining-requests")
        )
        remaining_tokens = self._positive_number(
            headers.get("x-ratelimit-remaining-tokens")
        )
        if remaining_requests is None or remaining_tokens is None:
            self.success_streak = 0
            return
        if (
            remaining_requests <= self.current * 2
            or remaining_tokens <= self.current * 12000
        ):
            self.success_streak = 0
            self.current = max(1, self.current - 1)
            return
        self.success_streak += 1
        if self.success_streak >= 2 and self.current < self.maximum:
            self.success_streak = 0
            self.current += 1


def _concurrent_worker(
    item: Dict[str, Any],
    project_skills_map: Dict[str, Any],
    skillsheet_map: Dict[str, Any],
    logger: Any,
    concurrency_at_submit: int,
) -> Dict[str, Any]:
    started = time.monotonic()
    attempts: List[Dict[str, Any]] = []
    request_body_mismatch = False

    def observer(attempt: Dict[str, Any]) -> None:
        attempts.append(dict(attempt))

    def observed_call(**kwargs: Any) -> Dict[str, Any]:
        nonlocal request_body_mismatch
        actual_hash = _request_body_hash(kwargs)
        if actual_hash != item["request_body_sha256"]:
            request_body_mismatch = True
            raise RuntimeError("preflight後にproduction request bodyが変化しました")
        return call_llm(
            **kwargs,
            response_observer=observer,
            use_bounded_retry_backoff=True,
        )

    stats: Dict[str, int] = {"note_truncated_count": 0}
    result, error = process_pair(
        item["pair"],
        project_skills_map,
        skillsheet_map,
        logger,
        stats,
        observed_call,
    )
    status = "success" if result is not None and error is None else "error"
    final_headers = attempts[-1].get("rate_limit_headers", {}) if attempts else {}
    return {
        **concurrent_manifest_record(item),
        "completion_state": "completed",
        "status": status,
        "result": result,
        "error": error,
        "concurrency_at_submit": concurrency_at_submit,
        "note_truncated_count": stats["note_truncated_count"],
        "telemetry": {
            "latency_seconds": time.monotonic() - started,
            "attempts": attempts,
            "retry_count": max(0, len(attempts) - 1),
            "rate_limit_429_count": sum(
                attempt.get("status_code") == 429 for attempt in attempts
            ),
            "api_failure": status != "success",
            "rate_limit_headers": final_headers,
            "request_body_mismatch": request_body_mismatch,
        },
    }


def _valid_completed_skill_list(value: Any) -> bool:
    if not isinstance(value, list):
        return False
    return all(
        isinstance(item, dict)
        and set(item) == {"skill", "match", "note"}
        and isinstance(item.get("skill"), str)
        and bool(item.get("skill"))
        and isinstance(item.get("match"), bool)
        and isinstance(item.get("note"), str)
        and bool(item.get("note"))
        and len(item.get("note")) <= NOTE_MAX_CHARS
        for item in value
    )


def _valid_checkpoint_payload(
    row: Dict[str, Any], expected: Dict[str, Any]
) -> bool:
    status = row.get("status")
    result = row.get("result")
    error = row.get("error")
    project_mid = row.get("project_message_id")
    resource_mid = row.get("resource_message_id")
    if status == "success":
        if not isinstance(result, dict) or error is not None:
            return False
        project_info = result.get("project_info")
        resource_info = result.get("resource_info")
        if not isinstance(project_info, dict) or not isinstance(resource_info, dict):
            return False
        if (
            project_info.get("message_id") != project_mid
            or resource_info.get("message_id") != resource_mid
        ):
            return False
        required = result.get("required_skills")
        optional = result.get("optional_skills")
        if not (
            _valid_completed_skill_list(required)
            and _valid_completed_skill_list(optional)
            and _skill_contract_hash(required, optional)
            == expected.get("skill_contract_sha256")
        ):
            return False
        evaluation_meta = result.get("evaluation_meta")
        if not isinstance(evaluation_meta, dict) or set(evaluation_meta) != {
            "skillsheet_source",
            "llm_model",
            "soft_skill_auto_true_count",
        }:
            return False
        soft_count = evaluation_meta.get("soft_skill_auto_true_count")
        return (
            isinstance(evaluation_meta.get("skillsheet_source"), str)
            and bool(evaluation_meta.get("skillsheet_source"))
            and evaluation_meta.get("llm_model") == LLM_MODEL
            and isinstance(soft_count, int)
            and not isinstance(soft_count, bool)
            and 0 <= soft_count <= len(required) + len(optional)
        )
    if status == "error":
        if result is not None or not isinstance(error, dict):
            return False
        project_info = error.get("project_info")
        resource_info = error.get("resource_info")
        if not isinstance(project_info, dict) or not isinstance(resource_info, dict):
            return False
        return (
            project_info.get("message_id") == project_mid
            and resource_info.get("message_id") == resource_mid
            and isinstance(error.get("error_type"), str)
            and bool(error.get("error_type"))
            and isinstance(error.get("error_message"), str)
        )
    return False


def collect_concurrent_checkpoints(
    manifest: Sequence[Dict[str, Any]],
    checkpoints: Sequence[Dict[str, Any]],
    allow_missing: bool = False,
) -> Dict[str, Any]:
    expected = {row["request_identity"]: row for row in manifest}
    seen: Dict[str, Dict[str, Any]] = {}
    duplicate: List[str] = []
    unknown: List[str] = []
    malformed: List[str] = []
    for index, row in enumerate(checkpoints, 1):
        if not isinstance(row, dict):
            malformed.append("line={}:not_object".format(index))
            continue
        identity = row.get("request_identity")
        if not isinstance(identity, str) or not identity:
            malformed.append("line={}:identity".format(index))
            continue
        if identity not in expected:
            unknown.append(identity)
            continue
        if identity in seen:
            duplicate.append(identity)
            continue
        expected_row = expected[identity]
        valid = (
            row.get("ordinal") == expected_row.get("ordinal")
            and row.get("project_message_id")
            == expected_row.get("project_message_id")
            and row.get("resource_message_id")
            == expected_row.get("resource_message_id")
            and row.get("request_body_sha256")
            == expected_row.get("request_body_sha256")
            and row.get("skill_contract_sha256")
            == expected_row.get("skill_contract_sha256")
            and row.get("api_request") is expected_row.get("api_request")
            and row.get("completion_state") == "completed"
            and row.get("status") in ("success", "error")
            and _valid_checkpoint_payload(row, expected_row)
        )
        if not valid:
            malformed.append("line={}:{}".format(index, identity))
            continue
        seen[identity] = row
    missing = sorted(set(expected) - set(seen))
    if duplicate or unknown or malformed or (missing and not allow_missing):
        raise ValueError(
            "checkpoint不整合: duplicate={} unknown={} malformed={} missing={}".format(
                len(duplicate), len(unknown), len(malformed), len(missing)
            )
        )
    ordered = [
        seen[row["request_identity"]]
        for row in sorted(manifest, key=lambda value: value["ordinal"])
        if row["request_identity"] in seen
    ]
    return {
        "ordered": ordered,
        "seen": seen,
        "duplicate": duplicate,
        "unknown": unknown,
        "malformed": malformed,
        "missing": missing,
    }


def run_concurrent_scheduler(
    items: Sequence[Dict[str, Any]],
    project_skills_map: Dict[str, Any],
    skillsheet_map: Dict[str, Any],
    logger: Any,
    checkpoint_path: Path,
    existing_checkpoints: Optional[Sequence[Dict[str, Any]]] = None,
) -> Tuple[List[Dict[str, Any]], AdaptiveConcurrency, bool]:
    manifest = [concurrent_manifest_record(item) for item in items]
    existing = list(existing_checkpoints or [])
    resume_state = collect_concurrent_checkpoints(
        manifest, existing, allow_missing=True
    )
    completed: Dict[str, Dict[str, Any]] = dict(resume_state["seen"])
    if any(
        row.get("api_request") is True and row.get("status") != "success"
        for row in completed.values()
    ):
        raise ValueError("API error checkpointを含むrunは安全のためresumeしません")

    for item in items:
        if item["api_request"] or item["request_identity"] in completed:
            continue
        checkpoint = {
            **concurrent_manifest_record(item),
            "completion_state": "completed",
            "status": "error",
            "result": None,
            "error": item["preflight_error"],
            "concurrency_at_submit": 0,
            "note_truncated_count": 0,
            "telemetry": {
                "latency_seconds": 0.0,
                "attempts": [],
                "retry_count": 0,
                "rate_limit_429_count": 0,
                "api_failure": False,
                "rate_limit_headers": {},
                "request_body_mismatch": False,
            },
        }
        append_jsonl(str(checkpoint_path), checkpoint)
        completed[item["request_identity"]] = checkpoint

    by_project: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in items:
        if item["api_request"]:
            by_project[item["project_message_id"]].append(item)
    for rows in by_project.values():
        rows.sort(key=lambda value: value["ordinal"])

    ready: Deque[Dict[str, Any]] = deque()
    followers: Dict[str, Deque[Dict[str, Any]]] = {}
    warmed: Set[str] = set()
    for project_mid, rows in sorted(
        by_project.items(), key=lambda entry: entry[1][0]["ordinal"]
    ):
        leader = rows[0]
        if leader["request_identity"] in completed:
            warmed.add(project_mid)
        else:
            ready.append(leader)
        followers[project_mid] = deque(
            row for row in rows[1:] if row["request_identity"] not in completed
        )
        if project_mid in warmed:
            ready.extend(followers[project_mid])
            followers[project_mid].clear()

    controller = AdaptiveConcurrency(CONCURRENT_INITIAL, CONCURRENT_MAX)
    stopped = False
    in_flight: Dict[Any, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=CONCURRENT_MAX) as executor:
        while ready or in_flight:
            while ready and not stopped and len(in_flight) < controller.current:
                item = ready.popleft()
                future = executor.submit(
                    _concurrent_worker,
                    item,
                    project_skills_map,
                    skillsheet_map,
                    logger,
                    len(in_flight) + 1,
                )
                in_flight[future] = item
            if not in_flight:
                break
            done, _ = wait(set(in_flight), return_when=FIRST_COMPLETED)
            for future in done:
                item = in_flight.pop(future)
                try:
                    checkpoint = future.result()
                except Exception as error:
                    checkpoint = {
                        **concurrent_manifest_record(item),
                        "completion_state": "completed",
                        "status": "error",
                        "result": None,
                        "error": _make_error(
                            item["project_message_id"],
                            item["resource_message_id"],
                            "llm_call_error",
                            str(error)[:1000],
                        ),
                        "concurrency_at_submit": 0,
                        "note_truncated_count": 0,
                        "telemetry": {
                            "latency_seconds": 0.0,
                            "attempts": [],
                            "retry_count": 0,
                            "rate_limit_429_count": 0,
                            "api_failure": True,
                            "rate_limit_headers": {},
                            "request_body_mismatch": False,
                        },
                    }
                append_jsonl(str(checkpoint_path), checkpoint)
                completed[checkpoint["request_identity"]] = checkpoint
                controller.observe(checkpoint)
                project_mid = item["project_message_id"]
                if item["is_project_warm_one"] and checkpoint["status"] == "success":
                    warmed.add(project_mid)
                    ready.extend(followers[project_mid])
                    followers[project_mid].clear()
                total_retries = sum(
                    row.get("telemetry", {}).get("retry_count", 0)
                    for row in completed.values()
                )
                retry_limit = max(3, int(math.ceil(len(items) * 0.10)))
                if checkpoint["status"] != "success":
                    stopped = True
                elif total_retries >= retry_limit:
                    stopped = True

    return list(completed.values()), controller, stopped


def _safe_concurrent_run_dir(run_id: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}", run_id):
        raise ValueError("concurrent run_idは英数字開始の80文字以内")
    root = CONCURRENT_CHECKPOINT_ROOT.resolve()
    run_dir = (CONCURRENT_CHECKPOINT_ROOT / run_id).resolve()
    if root not in run_dir.parents:
        raise ValueError("concurrent checkpoint root外です")
    return run_dir


def _run_concurrent(
    args: argparse.Namespace,
    logger: Any,
    dirs: Dict[str, str],
    project_skills_map: Dict[str, Any],
    skillsheet_map: Dict[str, Any],
    input_count: int,
) -> None:
    pairs = list(read_jsonl(str(INPUT_PAIRS)))
    if args.limit is not None:
        pairs = pairs[: args.limit]
    items = build_concurrent_items(pairs, project_skills_map, skillsheet_map, logger)
    manifest = [concurrent_manifest_record(item) for item in items]
    run_id = args.concurrent_run_id or os.environ.get("RUN_ID", "").strip()
    if not run_id:
        run_id = datetime.now().astimezone().strftime("run_%Y%m%d_%H%M%S")
    run_dir = _safe_concurrent_run_dir(run_id)
    if run_dir.exists() and not args.resume_concurrent:
        raise RuntimeError("concurrent checkpoint runが既に存在します: {}".format(run_id))
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "manifest.jsonl"
    checkpoint_path = run_dir / "checkpoint.jsonl"
    if manifest_path.exists():
        if list(read_jsonl(str(manifest_path))) != manifest:
            raise RuntimeError("resume manifest/request hashが現在入力と一致しません")
    else:
        write_jsonl(str(manifest_path), manifest)
    existing = (
        list(read_jsonl(str(checkpoint_path))) if checkpoint_path.exists() else []
    )
    # API fan-out前にCache HIT側のretention入力も検証する。
    cache_hit_results = cache_hit_results_for_run(args.limit)
    started = time.time()
    checkpoints, controller, stopped = run_concurrent_scheduler(
        items,
        project_skills_map,
        skillsheet_map,
        logger,
        checkpoint_path,
        existing,
    )
    collected = collect_concurrent_checkpoints(
        manifest, checkpoints, allow_missing=stopped
    )
    if stopped or collected["missing"]:
        raise RuntimeError(
            "concurrent run停止: stopped={} missing={}".format(
                stopped, len(collected["missing"])
            )
        )
    ordered = collected["ordered"]
    if any(
        row.get("telemetry", {}).get("request_body_mismatch") for row in ordered
    ):
        raise RuntimeError("request body mismatchを検出しました")
    results = [row["result"] for row in ordered if row["status"] == "success"]
    errors = [row["error"] for row in ordered if row["status"] == "error"]
    retained, retention_stats = build_retention_sidecar(
        results + cache_hit_results, skillsheet_map
    )
    retention_stats["cache_hit_rows_evaluated"] = len(cache_hit_results)

    write_jsonl(str(OUTPUT_RESULT), results)
    write_jsonl(str(OUTPUT_ERROR), errors)
    write_jsonl(str(OUTPUT_RETENTION_SIDECAR), retained)
    total = len(results) + len(errors)
    run_metadata = {
        "input_count": input_count,
        "processed_count": total,
        "limit": args.limit,
        "is_limited_run": args.limit is not None,
        "note_truncated_count": sum(
            int(row.get("note_truncated_count", 0)) for row in ordered
        ),
        "concurrent_execution": True,
        "concurrent_run_id": run_id,
        "peak_concurrency": max(
            [int(row.get("concurrency_at_submit", 0)) for row in ordered] + [0]
        ),
        "retention_guard": retention_stats,
    }
    OUTPUT_RUN_METADATA.write_text(
        json.dumps(run_metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    elapsed = time.time() - started
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)
    logger.info(
        "concurrent完了 total={} ok={} err={} peak={} retained={}".format(
            total,
            len(results),
            len(errors),
            run_metadata["peak_concurrency"],
            retention_stats["retained_pairs"],
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="07-1 Requirement Skill AI Matching")
    parser.add_argument(
        "--limit", type=int, default=None,
        help="処理件数上限（小規模テスト用）。省略時は全件処理"
    )
    parser.add_argument(
        "--concurrent-run-id",
        default="",
        help="concurrent checkpointのrun identity（flag=1時のみ）",
    )
    parser.add_argument(
        "--resume-concurrent",
        action="store_true",
        help="同一run identityの完了済みrequestを再送せず再開（flag=1時のみ）",
    )
    args = parser.parse_args()

    logger = get_logger(STEP_NAME)
    logger.info(f"開始 limit={args.limit}")

    dirs = ensure_result_dirs(str(STEP_DIR))

    # 入力ファイル存在確認
    for path, label in [
        (INPUT_PAIRS, "06-80新規(Cache MISS)ペア"),
        (INPUT_PROJECT_SKILLS, "03-50プロジェクトスキル"),
        (INPUT_SKILLSHEETS, "04-2 normalizedスキルシート"),
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

    logger.info("04-2 normalizedスキルシート読み込み中...")
    skillsheet_map: Dict[str, Any] = {}
    for rec in read_jsonl(str(INPUT_SKILLSHEETS)):
        mid = rec.get("message_id")
        if mid:
            skillsheet_map[str(mid)] = rec
    logger.info(f"04-2 完了: {len(skillsheet_map)}件")

    input_count = 0
    if INPUT_PAIRS.exists():
        for _ in read_jsonl(str(INPUT_PAIRS)):
            input_count += 1

    if ENABLE_07_1_CONCURRENT:
        if args.resume_concurrent and not (
            args.concurrent_run_id or os.environ.get("RUN_ID", "").strip()
        ):
            raise SystemExit("--resume-concurrentにはrun identityが必要です")
        AdaptiveConcurrency(CONCURRENT_INITIAL, CONCURRENT_MAX)
        logger.info(
            "ENABLE_07_1_CONCURRENT=True initial={} max={}".format(
                CONCURRENT_INITIAL, CONCURRENT_MAX
            )
        )
        try:
            _run_concurrent(
                args,
                logger,
                dirs,
                project_skills_map,
                skillsheet_map,
                input_count,
            )
        except Exception as error:
            logger.error("concurrent処理失敗: {}: {}".format(
                type(error).__name__, error
            ))
            sys.exit(1)
        return

    logger.info("ENABLE_07_1_CONCURRENT=False: 従来serial pathを使用")

    # 出力ファイルを初期化
    OUTPUT_RESULT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_ERROR.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_RESULT, "w", encoding="utf-8"):
        pass
    with open(OUTPUT_ERROR, "w", encoding="utf-8"):
        pass

    ok_count = 0
    err_count = 0
    run_stats: Dict[str, int] = {"note_truncated_count": 0}
    start_time = time.time()

    for i, pair in enumerate(read_jsonl(str(INPUT_PAIRS))):
        if args.limit is not None and i >= args.limit:
            break

        p_mid = pair.get("project_info", {}).get("message_id", f"idx{i}")
        r_mid = pair.get("resource_info", {}).get("message_id", f"idx{i}")

        try:
            result, error = process_pair(
                pair, project_skills_map, skillsheet_map, logger, run_stats
            )
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
        # 表示用note切り詰め件数（run単位の観測用。result JSONLスキーマは変更しない）
        "note_truncated_count": run_stats["note_truncated_count"],
    }
    with open(OUTPUT_RUN_METADATA, "w", encoding="utf-8") as f:
        json.dump(run_metadata, f, ensure_ascii=False, indent=2)

    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)
    logger.info(
        f"完了 total={total} ok={ok_count} err={err_count} "
        f"note_truncated_count={run_stats['note_truncated_count']} "
        f"elapsed={elapsed:.1f}s"
    )


if __name__ == "__main__":
    main()

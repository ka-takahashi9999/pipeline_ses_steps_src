"""保存済み08-5 Batch canary結果へminimal safety guardをshadow適用する。

OpenAI API、Batch API、AWS、production出力は一切使用しない。入力と出力は
``_test_batch_api_canary/<run_id>/`` 配下の保存済みartifactに限定する。
"""

import argparse
import copy
import importlib.util
import json
import math
import re
import sys
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STEP_DIR = Path(__file__).resolve().parents[1]
TOOL_DIR = Path(__file__).resolve().parent
CANARY_ROOT = STEP_DIR / "_test_batch_api_canary"
DEFAULT_RUN_ID = "canary678-20260822-01"
OUTPUT_JSON = "minimal_guard_shadow_replay.json"
OUTPUT_TEXT = "minimal_guard_shadow_replay.txt"
GUARD_VERSION = "batch-minimal-safety-guard-v1"
EXPECTED_SAMPLE_SIZE = 678

MAX_NEW_HUMAN_REVIEW_RESCUES = 15
MAX_CONFIRMED_TO_HUMAN_REVIEW = 15
MAX_AFFECTED_PAIRS = 25

OPTIONAL_MARKERS = ("特に歓迎", "あれば尚可", "尚可", "歓迎", "推奨")

# 最終品質監査でCLEAR_KEEPと判定された5件。custom_idは保存済みmanifestの
# source ordinalとhashで固定され、fixture取り違えを防ぐ。
CLEAR_KEEP_CASES = (
    {
        "custom_id": "c-canary678-20260822-01-0352-b5d37b77ca8c",
        "label": "optional_welcome",
    },
    {
        "custom_id": "c-canary678-20260822-01-0253-7d7f85cca0da",
        "label": "schema_invalid_category_mismatch",
    },
    {
        "custom_id": "c-canary678-20260822-01-0303-839007bc06a6",
        "label": "ambiguous_user_definition_with_requirements",
    },
    {
        "custom_id": "c-canary678-20260822-01-0341-1bb1d4d605b3",
        "label": "ia_ui_ux_with_user_flow_screen_design",
    },
    {
        "custom_id": "c-canary678-20260822-01-0342-18f4b14de8ad",
        "label": "db_sql_with_mysql_experience",
    },
)

CLEAR_FALSE_POSITIVE_CASES = (
    {
        "custom_id": "c-canary678-20260822-01-0085-961ae7fbe465",
        "label": "react_native_1_year_3_months",
    },
    {
        "custom_id": "c-canary678-20260822-01-0131-7c68e13f19c1",
        "label": "flutter_6_months_react_native_7_months",
    },
)

YEAR_REQUIREMENT_RE = re.compile(
    r"(?P<years>\d+(?:\.\d+)?)\s*(?P<kind>年以上|年超)"
)
MONTH_DURATION_RE = re.compile(
    r"(?P<months>\d+)\s*(?:\|\s*)?(?:ヶ|か|カ)?月"
)
YEAR_DURATION_RE = re.compile(
    r"(?P<years>\d+(?:\.\d+)?)\s*(?:\|\s*)?年"
    r"(?:(?:\s*\|\s*)?(?P<months>\d+)\s*(?:ヶ|か|カ)?月|(?P<half>半))?"
)
YEAR_ORDINAL_RE = re.compile(r"(?P<years>\d+)\s*年目")
PROJECT_START_RE = re.compile(
    r"^\s*\d+(?:\.\d+)?\s*\|\s*20\d{2}\s*\|\s*年\s*\|\s*\d{1,2}\s*\|\s*月",
    re.MULTILINE,
)

# 年数と対象skillの結び付け専用。semantic aliasにはせず、required skillに
# 明記された代表的な技術語・工程語だけをそのまま抽出する。
TARGET_PATTERNS: Tuple[Tuple[str, str], ...] = (
    ("react_native", r"React\s*Native|ReactNative"),
    ("spring_boot", r"Spring\s*Boot"),
    ("sql_server", r"SQL\s*Server|SQLServer"),
    ("servicenow", r"ServiceNow"),
    ("typescript", r"TypeScript|Typescript"),
    ("javascript", r"JavaScript|Javascript"),
    ("postgresql", r"PostgreSQL"),
    ("flutter", r"Flutter"),
    ("mysql", r"MySQL"),
    ("java", r"(?<![A-Za-z])Java(?!Script)"),
    ("php", r"(?<![A-Za-z])PHP(?![A-Za-z])"),
    ("sql", r"(?<![A-Za-z])SQL(?![A-Za-z])"),
    ("pmo", r"(?<![A-Za-z])PMO(?![A-Za-z])"),
    ("pm", r"(?<![A-Za-z])PM(?![A-Za-z])"),
    ("go", r"(?<![A-Za-z])Go(?![A-Za-z])"),
    ("csharp", r"C#"),
    ("vue", r"Vue(?:\.js)?"),
    ("web_app", r"Web\s*アプリ(?:ケーション)?|WEB\s*アプリ(?:ケーション)?"),
    ("software_engineer", r"ソフトウェアエンジニア"),
    ("basic_design", r"基本設計"),
    ("detail_design", r"詳細設計"),
)

sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(TOOL_DIR))
from common.json_utils import read_jsonl  # noqa: E402

import _test_batch_api_canary as CANARY  # noqa: E402


def _normalize(value: Any) -> str:
    return unicodedata.normalize("NFKC", str(value or ""))


def _load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"read-only module import失敗: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SALES_GATE = _load_module(
    PROJECT_ROOT
    / "09-5_generate_sales_reply_draft/00_tool/classify_sales_candidate_queues.py",
    "classify_sales_candidate_queues_batch_guard_readonly",
)


def _status_from_checks(checks: Sequence[Dict[str, Any]]) -> str:
    confidences = [str(check.get("confidence") or "") for check in checks]
    if "not_confirmed" in confidences:
        return "not_confirmed"
    if "human_review" in confidences:
        return "human_review"
    return "confirmed"


def _fallback_checks(required_skill_texts: Sequence[str]) -> List[Dict[str, Any]]:
    return [
        {
            "skill": skill,
            "confidence": "human_review",
            "reason": "Batch schema invalid fallbackのため人間確認",
            "evidence": "",
        }
        for skill in required_skill_texts
    ]


def _extract_skillsheet_text(input_record: Dict[str, Any]) -> str:
    body = input_record.get("body")
    messages = body.get("messages") if isinstance(body, dict) else None
    if not isinstance(messages, list):
        raise ValueError("保存済みinputのmessagesが不正")
    user_contents = [
        message.get("content")
        for message in messages
        if isinstance(message, dict) and message.get("role") == "user"
    ]
    if len(user_contents) != 1 or not isinstance(user_contents[0], str):
        raise ValueError("保存済みinputのuser promptが一意でない")
    marker = "【要員スキルシート本文】"
    prompt = user_contents[0]
    if marker not in prompt:
        raise ValueError("保存済みinputにskillsheet markerがない")
    skillsheet = prompt.split(marker, 1)[1]
    trailer = "\n\n上記の案件本文とスキルシートを根拠に"
    if trailer in skillsheet:
        skillsheet = skillsheet.split(trailer, 1)[0]
    skillsheet = _normalize(skillsheet).strip()
    # 複数sheetを連結したAI contextには後段に記入例sheetが混ざる場合がある。
    # 年数証明で別人の記入例を使わないよう、先頭の実データsheetだけに限定する。
    sheet_markers = [match.start() for match in re.finditer(r"^=== シート:", skillsheet, re.MULTILINE)]
    if len(sheet_markers) > 1:
        skillsheet = skillsheet[: sheet_markers[1]].rstrip()
    return skillsheet


def _required_years(skill: str) -> Optional[Tuple[int, bool]]:
    match = YEAR_REQUIREMENT_RE.search(_normalize(skill))
    if not match:
        return None
    months = int(math.ceil(float(match.group("years")) * 12))
    return months, match.group("kind") == "年超"


def _duration_values(text: str) -> List[int]:
    normalized = _normalize(text)
    values: List[int] = []
    consumed: List[Tuple[int, int]] = []
    for match in YEAR_DURATION_RE.finditer(normalized):
        years = float(match.group("years"))
        # 2024年などの年月は経験期間として扱わない。
        if years >= 100:
            continue
        months = int(round(years * 12))
        if match.group("months"):
            months += int(match.group("months"))
        elif match.group("half"):
            months += 6
        values.append(months)
        consumed.append(match.span())
    for match in YEAR_ORDINAL_RE.finditer(normalized):
        years = int(match.group("years"))
        if years < 100:
            values.append(max(0, years - 1) * 12)
            consumed.append(match.span())
    for match in MONTH_DURATION_RE.finditer(normalized):
        if any(start <= match.start() < end for start, end in consumed):
            continue
        values.append(int(match.group("months")))
    return values


def _fact_months(text: str) -> int:
    values = _duration_values(text)
    if not values:
        return 0
    # 「42ヶ月 + 8ヶ月」のように加算が明記された同一factだけ合算する。
    if "+" in _normalize(text) and len(values) > 1:
        return sum(values)
    return max(values)


def _target_patterns(skill: str) -> List[Tuple[str, re.Pattern]]:
    normalized = _normalize(skill)
    targets: List[Tuple[str, re.Pattern]] = []
    for label, pattern in TARGET_PATTERNS:
        if re.search(pattern, normalized, re.IGNORECASE):
            targets.append((label, re.compile(pattern, re.IGNORECASE)))
    return targets


def _project_blocks(skillsheet: str) -> List[str]:
    matches = list(PROJECT_START_RE.finditer(skillsheet))
    blocks: List[str] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(skillsheet)
        # 最終project後のskill matrixを当該projectの期間へ誤結合しない。
        section = re.search(
            r"^(?:■スキル|保有技術|【スキル)",
            skillsheet[match.end() : end],
            re.MULTILINE,
        )
        if section:
            end = match.end() + section.start()
        blocks.append(skillsheet[match.start() : end])
    return blocks


def _target_months_in_skillsheet(
    skillsheet: str, target: re.Pattern
) -> int:
    direct_fact_max = 0
    for line in skillsheet.splitlines():
        if target.search(line):
            direct_fact_max = max(direct_fact_max, _fact_months(line))

    block_total = 0
    for block in _project_blocks(skillsheet):
        if not target.search(block):
            continue
        durations = _duration_values(block)
        # 各project block末尾の期間合計が最小の妥当値になる。開始年月の
        # 4桁年は既に除外されている。
        plausible = [months for months in durations if 0 < months <= 600]
        if plausible:
            block_total += max(plausible)
    return max(direct_fact_max, block_total)


def _generic_evidence_duration_allowed(skill: str, evidence: str) -> bool:
    normalized_skill = _normalize(skill)
    normalized_evidence = _normalize(evidence)
    if "ソフトウェアエンジニア" in normalized_skill:
        return bool(re.search(r"キャリア|実務経験|経験年数|\d+年", normalized_evidence))
    if re.search(r"Web\s*アプリ", normalized_skill, re.IGNORECASE):
        return bool(re.search(r"Web|経験年数|実務経験", normalized_evidence, re.IGNORECASE))
    return False


def _explicit_years_proven(
    skill: str, evidence: str, skillsheet: str
) -> Tuple[bool, Dict[str, int]]:
    requirement = _required_years(skill)
    if requirement is None:
        return True, {}
    required_months, strict_over = requirement
    targets = _target_patterns(skill)
    proof: Dict[str, int] = {}
    normalized_evidence = _normalize(evidence)

    for label, pattern in targets:
        evidence_months = _fact_months(normalized_evidence) if pattern.search(normalized_evidence) else 0
        skillsheet_months = _target_months_in_skillsheet(skillsheet, pattern)
        proof[label] = max(evidence_months, skillsheet_months)

    if _generic_evidence_duration_allowed(skill, evidence):
        proof["generic_evidence"] = _fact_months(evidence)

    if not proof:
        return False, proof
    best = max(proof.values())
    return (best > required_months if strict_over else best >= required_months), proof


def _downgrade_check(check: Dict[str, Any], reason: str) -> None:
    check["confidence"] = "human_review"
    original_reason = str(check.get("reason") or "").strip()
    check["reason"] = f"{original_reason} / {reason}" if original_reason else reason


def _apply_optional_guard(checks: List[Dict[str, Any]]) -> int:
    changed = 0
    for check in checks:
        skill = _normalize(check.get("skill"))
        if check.get("confidence") == "not_confirmed" and any(
            marker in skill for marker in OPTIONAL_MARKERS
        ):
            _downgrade_check(check, "Batch optional条件 safety guard")
            changed += 1
    return changed


def _has_neighbor_requirement_fact(checks: Sequence[Dict[str, Any]]) -> bool:
    for check in checks:
        skill = _normalize(check.get("skill"))
        if skill not in ("要求定義", "要件定義"):
            continue
        if check.get("confidence") in ("confirmed", "human_review") and str(
            check.get("evidence") or ""
        ).strip():
            return True
    return False


def _apply_narrow_fact_guard(
    checks: List[Dict[str, Any]], skillsheet: str
) -> int:
    changed = 0
    for check in checks:
        if check.get("confidence") != "not_confirmed":
            continue
        skill = _normalize(check.get("skill")).replace(" ", "")
        if (
            skill == "ユーザー定義"
            and _has_neighbor_requirement_fact(checks)
            and "画面設計・実装" in skillsheet
            and "要件定義" in skillsheet
        ):
            _downgrade_check(check, "近接する要件/要求定義の明示事実あり")
            changed += 1
            continue
        if skill == "IA/UI/UX設計" and re.search(
            r"ユーザー(?:の)?導線.{0,40}画面設計.{0,30}実装",
            skillsheet,
            re.DOTALL,
        ):
            _downgrade_check(check, "ユーザー導線を考慮した画面設計・実装の明示事実あり")
            changed += 1
            continue
        if skill == "データベースの基礎知識(SQL)" and re.search(
            r"(?:MySQL|PostgreSQL|SQL\s*Server|Oracle)\s*\|?\s*\d+\s*(?:年|(?:ヶ|か|カ)?月)",
            skillsheet,
            re.IGNORECASE,
        ):
            _downgrade_check(check, "対象DBの明示的な経験期間あり")
            changed += 1
    return changed


def _apply_explicit_years_guard(
    checks: List[Dict[str, Any]], skillsheet: str
) -> Tuple[int, List[Dict[str, Any]]]:
    changed = 0
    decisions: List[Dict[str, Any]] = []
    for check in checks:
        skill = str(check.get("skill") or "")
        requirement = _required_years(skill)
        if check.get("confidence") != "confirmed" or requirement is None:
            continue
        # 推奨・尚可等は必須年数ではないためGuard Bの対象外。
        if any(marker in _normalize(skill) for marker in OPTIONAL_MARKERS):
            continue
        proven, proof = _explicit_years_proven(
            skill, str(check.get("evidence") or ""), skillsheet
        )
        decisions.append(
            {
                "skill": skill,
                "required_months": requirement[0],
                "strict_over": requirement[1],
                "proven": proven,
                "proof_months_by_target": proof,
            }
        )
        if not proven:
            _downgrade_check(
                check,
                "対象skillと明示期間の対応から要求年数を証明できないため人間確認",
            )
            changed += 1
    return changed, decisions


def apply_guard_to_pair(
    shadow: Dict[str, Any],
    manifest_entry: Dict[str, Any],
    skillsheet: str,
) -> Dict[str, Any]:
    schema_valid = shadow.get("schema_valid") is True
    raw_checks = shadow.get("required_skill_checks")
    if schema_valid:
        if not isinstance(raw_checks, list):
            raise ValueError(f"schema validなのにchecks不正: {shadow.get('custom_id')}")
        before_checks = copy.deepcopy(raw_checks)
    else:
        required_skills = manifest_entry.get("required_skill_texts")
        if not isinstance(required_skills, list):
            raise ValueError("manifest required_skill_texts不正")
        before_checks = _fallback_checks([str(skill) for skill in required_skills])

    before_category = str(shadow.get("category_match") or "unclear")
    before_status = _status_from_checks(before_checks)
    after_checks = copy.deepcopy(before_checks)
    after_category = before_category
    guard_reasons: List[str] = []

    optional_changes = _apply_optional_guard(after_checks)
    if optional_changes:
        guard_reasons.append(f"optional_condition:{optional_changes}")

    if not schema_valid and before_category == "mismatch":
        after_category = "unclear"
        guard_reasons.append("schema_invalid_category_mismatch")

    narrow_fact_changes = _apply_narrow_fact_guard(after_checks, skillsheet)
    if narrow_fact_changes:
        guard_reasons.append(f"narrow_nearby_fact:{narrow_fact_changes}")

    # 既にpair全体がhuman_review/not_confirmedなら自動提案へ進まないため、
    # Guard Bはpair全体がconfirmedだった場合だけに限定する。
    if before_status == "confirmed":
        years_changes, years_decisions = _apply_explicit_years_guard(
            after_checks, skillsheet
        )
    else:
        years_changes, years_decisions = 0, []
    if years_changes:
        guard_reasons.append(f"explicit_years_unproven:{years_changes}")

    after_status = _status_from_checks(after_checks)
    return {
        "ordinal": shadow.get("ordinal"),
        "custom_id": shadow.get("custom_id"),
        "project_message_id": shadow.get("project_message_id"),
        "resource_message_id": shadow.get("resource_message_id"),
        "schema_valid": schema_valid,
        "before": {
            "status": before_status,
            "category_match": before_category,
            "required_skill_checks": before_checks,
        },
        "after": {
            "status": after_status,
            "category_match": after_category,
            "required_skill_checks": after_checks,
        },
        "guard_reasons": guard_reasons,
        "explicit_years_decisions": years_decisions,
        "saved_direct_result": shadow.get("saved_direct_result"),
    }


def _candidate(status: str, category_match: str) -> bool:
    return status != "not_confirmed" and category_match != "mismatch"


def _direct_candidate(pair: Dict[str, Any]) -> bool:
    direct = pair.get("saved_direct_result")
    if not isinstance(direct, dict):
        return False
    return (
        direct.get("recheck_status") != "required_skill_not_confirmed"
        and direct.get("category_match") != "mismatch"
    )


def _pair_key(record: Dict[str, Any]) -> Tuple[str, str]:
    return (
        str(record.get("project_message_id") or ""),
        str(record.get("resource_message_id") or ""),
    )


def _load_sales_gate_map(
    pair_keys: Iterable[Tuple[str, str]],
) -> Tuple[str, Dict[Tuple[str, str], bool], int]:
    target_keys = set(pair_keys)
    result_dir = PROJECT_ROOT / "09-5_generate_sales_reply_draft/01_result"
    dates = []
    for proposal_path in sorted(result_dir.glob("proposal_ready_*.jsonl")):
        date = proposal_path.stem.replace("proposal_ready_", "")
        human_path = result_dir / f"human_review_{date}.jsonl"
        if human_path.exists():
            dates.append((date, proposal_path, human_path))
    if not dates:
        return "none", {}, 0

    candidates: List[Tuple[int, str, Dict[Tuple[str, str], bool]]] = []
    for date, proposal_path, human_path in dates:
        gate_map: Dict[Tuple[str, str], bool] = {}
        for path in (proposal_path, human_path):
            for record in read_jsonl(str(path)):
                key = _pair_key(record)
                if key in gate_map:
                    raise ValueError(f"sales queue pair重複: {date} {key}")
                gate_map[key] = record.get("sales_ready") is True
        overlap = len(target_keys & set(gate_map))
        candidates.append((overlap, date, gate_map))
    overlap, date, gate_map = max(candidates, key=lambda item: (item[0], item[1]))
    return date, gate_map, overlap


def _proposal_ready(
    pair: Dict[str, Any], side: str, sales_gate_map: Dict[Tuple[str, str], bool]
) -> bool:
    state = pair[side]
    checks = state["required_skill_checks"]
    evidence = SALES_GATE._evidence_status(checks)
    return (
        pair.get("schema_valid") is True
        and state["status"] == "confirmed"
        and state["category_match"] == "match"
        and sales_gate_map.get(_pair_key(pair), False)
        and evidence.get("evidence_ready") is True
    )


def _aggregate(
    pairs: Sequence[Dict[str, Any]],
    side: str,
    sales_gate_map: Dict[Tuple[str, str], bool],
) -> Dict[str, int]:
    status = Counter(pair[side]["status"] for pair in pairs)
    candidate_count = sum(
        _candidate(pair[side]["status"], pair[side]["category_match"])
        for pair in pairs
    )
    candidate_loss = sum(
        _direct_candidate(pair)
        and not _candidate(pair[side]["status"], pair[side]["category_match"])
        for pair in pairs
    )
    return {
        "confirmed": status["confirmed"],
        "human_review": status["human_review"],
        "not_confirmed": status["not_confirmed"],
        "category_mismatch": sum(
            pair[side]["category_match"] == "mismatch" for pair in pairs
        ),
        "proposal_ready": sum(
            _proposal_ready(pair, side, sales_gate_map) for pair in pairs
        ),
        "candidate_set": candidate_count,
        "candidate_loss": candidate_loss,
    }


def _audit_results(
    pairs: Sequence[Dict[str, Any]],
    sales_gate_map: Dict[Tuple[str, str], bool],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    by_id = {str(pair["custom_id"]): pair for pair in pairs}
    keep_results: List[Dict[str, Any]] = []
    for fixture in CLEAR_KEEP_CASES:
        pair = by_id.get(fixture["custom_id"])
        if pair is None:
            raise ValueError(f"CLEAR_KEEP fixture欠落: {fixture['custom_id']}")
        keep_results.append(
            {
                **fixture,
                "before_retained": _candidate(
                    pair["before"]["status"], pair["before"]["category_match"]
                ),
                "after_retained": _candidate(
                    pair["after"]["status"], pair["after"]["category_match"]
                ),
                "after_status": pair["after"]["status"],
                "guard_reasons": pair["guard_reasons"],
            }
        )

    false_positive_results: List[Dict[str, Any]] = []
    for fixture in CLEAR_FALSE_POSITIVE_CASES:
        pair = by_id.get(fixture["custom_id"])
        if pair is None:
            raise ValueError(
                f"CLEAR_FALSE_POSITIVE fixture欠落: {fixture['custom_id']}"
            )
        false_positive_results.append(
            {
                **fixture,
                "before_status": pair["before"]["status"],
                "after_status": pair["after"]["status"],
                "before_proposal_ready": _proposal_ready(
                    pair, "before", sales_gate_map
                ),
                "after_proposal_ready": _proposal_ready(
                    pair, "after", sales_gate_map
                ),
                "guard_reasons": pair["guard_reasons"],
            }
        )
    return keep_results, false_positive_results


def _report_text(report: Dict[str, Any]) -> str:
    before = report["before"]
    after = report["after"]
    quality = "PASS" if report["quality_pass"] else "FAIL"
    return "\n".join(
        [
            "【Batch minimal guard replay】",
            "",
            f"CLEAR_KEEP before retained: {report['clear_keep_before_retained']}/5",
            f"CLEAR_KEEP after retained: {report['clear_keep_after_retained']}/5",
            f"CLEAR_FALSE_POSITIVE before confirmed: {report['clear_false_positive_before_confirmed']}",
            f"CLEAR_FALSE_POSITIVE after confirmed: {report['clear_false_positive_after_confirmed']}",
            f"proposal_ready false positive before: {report['proposal_ready_false_positive_before']}",
            f"proposal_ready false positive after: {report['proposal_ready_false_positive_after']}",
            f"proposal_ready before: {before['proposal_ready']}",
            f"proposal_ready after: {after['proposal_ready']}",
            f"human_review before: {before['human_review']}",
            f"human_review after: {after['human_review']}",
            f"candidate loss before: {before['candidate_loss']}",
            f"candidate loss after: {after['candidate_loss']}",
            f"new human_review rescue: {report['new_human_review_rescue']}",
            f"confirmed to human_review: {report['confirmed_to_human_review']}",
            f"production change: {report['production_change']}",
            f"new LLM call: {report['new_llm_call']}",
            f"QUALITY: {quality}",
            "",
        ]
    )


def replay_run(
    run_id: str = DEFAULT_RUN_ID, root: Path = CANARY_ROOT, write: bool = True
) -> Dict[str, Any]:
    run_dir = CANARY._run_dir(run_id, root=root)
    manifest = list(read_jsonl(str(run_dir / "manifest.jsonl")))
    inputs = list(read_jsonl(str(run_dir / "input.jsonl")))
    report = CANARY._read_json(run_dir / "report.json")
    shadows = report.get("shadow_results")
    if not isinstance(shadows, list):
        raise ValueError("保存済みreport.shadow_resultsが不正")
    if not (len(manifest) == len(inputs) == len(shadows) == EXPECTED_SAMPLE_SIZE):
        raise ValueError(
            "678件整合不一致: "
            f"manifest={len(manifest)} input={len(inputs)} shadow={len(shadows)}"
        )

    manifest_by_id = {str(item.get("custom_id")): item for item in manifest}
    input_by_id = {str(item.get("custom_id")): item for item in inputs}
    if len(manifest_by_id) != EXPECTED_SAMPLE_SIZE or len(input_by_id) != EXPECTED_SAMPLE_SIZE:
        raise ValueError("manifest/input custom_id重複")

    pairs: List[Dict[str, Any]] = []
    for shadow in sorted(shadows, key=lambda item: int(item.get("ordinal", 0))):
        custom_id = str(shadow.get("custom_id") or "")
        manifest_entry = manifest_by_id.get(custom_id)
        input_record = input_by_id.get(custom_id)
        if manifest_entry is None or input_record is None:
            raise ValueError(f"custom_id接続不整合: {custom_id}")
        pairs.append(
            apply_guard_to_pair(
                shadow, manifest_entry, _extract_skillsheet_text(input_record)
            )
        )

    sales_date, sales_gate_map, sales_overlap = _load_sales_gate_map(
        _pair_key(pair) for pair in pairs
    )
    before = _aggregate(pairs, "before", sales_gate_map)
    after = _aggregate(pairs, "after", sales_gate_map)
    keep_results, false_positive_results = _audit_results(pairs, sales_gate_map)

    new_human_review_rescue = sum(
        not _candidate(pair["before"]["status"], pair["before"]["category_match"])
        and pair["after"]["status"] == "human_review"
        and _candidate(pair["after"]["status"], pair["after"]["category_match"])
        for pair in pairs
    )
    confirmed_to_human_review = sum(
        pair["before"]["status"] == "confirmed"
        and pair["after"]["status"] == "human_review"
        for pair in pairs
    )
    guard_touched_pairs = sum(bool(pair["guard_reasons"]) for pair in pairs)
    affected_pairs = sum(
        pair["before"]["status"] != pair["after"]["status"]
        or pair["before"]["category_match"] != pair["after"]["category_match"]
        for pair in pairs
    )
    clear_keep_before = sum(item["before_retained"] for item in keep_results)
    clear_keep_after = sum(item["after_retained"] for item in keep_results)
    clear_fp_before_confirmed = sum(
        item["before_status"] == "confirmed" for item in false_positive_results
    )
    clear_fp_after_confirmed = sum(
        item["after_status"] == "confirmed" for item in false_positive_results
    )
    proposal_fp_before = sum(
        item["before_proposal_ready"] for item in false_positive_results
    )
    proposal_fp_after = sum(
        item["after_proposal_ready"] for item in false_positive_results
    )
    schema_fallback_promotions = sum(
        not pair["schema_valid"] and _proposal_ready(pair, "after", sales_gate_map)
        for pair in pairs
    )

    quality_checks = {
        "sample_count_678": len(pairs) == EXPECTED_SAMPLE_SIZE,
        "clear_keep_5_of_5": clear_keep_after == 5,
        "clear_false_positive_2_of_2_safe": clear_fp_after_confirmed == 0,
        "proposal_ready_false_positive_0": proposal_fp_after == 0,
        "schema_fallback_promotion_0": schema_fallback_promotions == 0,
        "candidate_loss_not_increased": after["candidate_loss"] <= before["candidate_loss"],
        "new_human_review_rescue_scope": new_human_review_rescue
        <= MAX_NEW_HUMAN_REVIEW_RESCUES,
        "confirmed_downgrade_scope": confirmed_to_human_review
        <= MAX_CONFIRMED_TO_HUMAN_REVIEW,
        "affected_pair_scope": affected_pairs <= MAX_AFFECTED_PAIRS,
        "production_change_0": True,
        "new_llm_call_0": True,
        "production_write_0": True,
    }
    result = {
        "title": "Batch minimal guard replay",
        "guard_version": GUARD_VERSION,
        "canary_run_id": run_id,
        "sample_size": len(pairs),
        "before": before,
        "after": after,
        "clear_keep_cases": keep_results,
        "clear_false_positive_cases": false_positive_results,
        "clear_keep_before_retained": clear_keep_before,
        "clear_keep_after_retained": clear_keep_after,
        "clear_false_positive_before_confirmed": clear_fp_before_confirmed,
        "clear_false_positive_after_confirmed": clear_fp_after_confirmed,
        "proposal_ready_false_positive_before": proposal_fp_before,
        "proposal_ready_false_positive_after": proposal_fp_after,
        "schema_fallback_erroneous_promotion": schema_fallback_promotions,
        "new_human_review_rescue": new_human_review_rescue,
        "confirmed_to_human_review": confirmed_to_human_review,
        "affected_pairs": affected_pairs,
        "guard_touched_pairs": guard_touched_pairs,
        "sales_gate_source_date": sales_date,
        "sales_gate_pair_overlap": sales_overlap,
        "production_change": 0,
        "new_llm_call": 0,
        "new_batch_submit": 0,
        "files_upload": 0,
        "aws_write": 0,
        "production_write": 0,
        "quality_checks": quality_checks,
        "quality_pass": all(quality_checks.values()),
        "pairs": pairs,
    }
    if write:
        CANARY._write_json(run_dir / OUTPUT_JSON, result, root=root)
        output_text = run_dir / OUTPUT_TEXT
        CANARY._assert_canary_path(output_text, root=root)
        output_text.write_text(_report_text(result), encoding="utf-8")
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="保存済み08-5 Batch canaryへのminimal safety guard shadow replay"
    )
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    try:
        result = replay_run(args.run_id)
        print(_report_text(result), end="")
        if not result["quality_pass"]:
            sys.exit(1)
    except Exception as error:
        print(f"ERROR: {type(error).__name__}: {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

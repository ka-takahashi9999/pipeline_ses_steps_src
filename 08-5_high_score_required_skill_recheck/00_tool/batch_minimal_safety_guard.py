"""08-5 production結果に適用するminimal safety guard。

このmoduleは判定をconfirmedへ昇格させない。曖昧または安全に証明できない
AI判定をhuman_reviewへ戻すことだけを責務とする。Batchの従来guardと
legacyの局所guardが同じ年数/複合条件判定を共有する。
"""

import copy
import math
import re
import unicodedata
from typing import Any, Dict, List, Optional, Sequence, Tuple


GUARD_VERSION = "minimal-safety-guard-v2"
OPTIONAL_MARKERS = ("特に歓迎", "あれば尚可", "尚可", "歓迎", "推奨")
YEAR_REQUIREMENT_RE = re.compile(
    r"(?P<years>\d+(?:\.\d+)?)\s*(?P<kind>年以上|年超)"
)
MONTH_DURATION_RE = re.compile(r"(?P<months>\d+)\s*(?:\|\s*)?(?:ヶ|か|カ)?月")
YEAR_DURATION_RE = re.compile(
    r"(?P<years>\d+(?:\.\d+)?)\s*(?:\|\s*)?年"
    r"(?:(?:\s*\|\s*)?(?P<months>\d+)\s*(?:ヶ|か|カ)?月|(?P<half>半))?"
)
YEAR_ORDINAL_RE = re.compile(r"(?P<years>\d+)\s*年目")
PROJECT_START_RE = re.compile(
    r"^\s*\d+(?:\.\d+)?\s*\|\s*20\d{2}\s*\|\s*年\s*\|\s*\d{1,2}\s*\|\s*月",
    re.MULTILINE,
)
GIT_REQUIRED_RE = re.compile(r"(?<![A-Za-z])Git(?![A-Za-z])|GitHub", re.IGNORECASE)
GIT_SOURCE_RE = re.compile(
    r"(?<![A-Za-z])Git(?![A-Za-z])|GitHub|GitLab|Bitbucket",
    re.IGNORECASE,
)
GIT_COPILOT_RE = re.compile(r"GitHub\s*Copilot(?:\s+(?:Business|Enterprise))?", re.IGNORECASE)
TEAM_REQUIRED_RE = re.compile(r"チーム|複数名|Scrum|Agile", re.IGNORECASE)
TEAM_EVIDENCE_RE = re.compile(
    r"チーム開発|複数名(?:で|の)?開発|チーム|テックリード|"
    r"リーダー|メンバー|Scrum|Agile|人数|体制|\d+\s*(?:名|人)",
    re.IGNORECASE,
)
REVIEW_REQUIRED_RE = re.compile(
    r"Pull\s*Request|プルリクエスト|Merge\s*Request|"
    r"code\s*review|コードレビュー|レビュー運用",
    re.IGNORECASE,
)

# semantic alias辞書ではない。明示年数と同一required skill内の対象語を
# 誤結合しないための、shadow検証済みの限定的な識別子だけを保持する。
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


def _normalize(value: Any) -> str:
    return unicodedata.normalize("NFKC", str(value or ""))


def sanitize_skillsheet_for_guard(text: str) -> str:
    """記入例・sampleや連結された別sheetを年数根拠から除外する。"""
    normalized = _normalize(text).strip()
    sheet_markers = [
        match.start()
        for match in re.finditer(r"^=== シート:", normalized, re.MULTILINE)
    ]
    if len(sheet_markers) > 1:
        normalized = normalized[: sheet_markers[1]].rstrip()
    sample_marker = re.search(
        r"^(?:={2,}\s*)?(?:記入例|入力例|サンプル|sample)(?:\s*={2,})?\s*$",
        normalized,
        re.IGNORECASE | re.MULTILINE,
    )
    if sample_marker:
        normalized = normalized[: sample_marker.start()].rstrip()
    return normalized


def _status_from_checks(checks: Sequence[Dict[str, Any]]) -> str:
    confidences = [str(check.get("confidence") or "") for check in checks]
    if "not_confirmed" in confidences:
        return "not_confirmed"
    if "human_review" in confidences:
        return "human_review"
    return "confirmed"


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
    if "+" in _normalize(text) and len(values) > 1:
        return sum(values)
    return max(values)


def _target_patterns(skill: str) -> List[Tuple[str, re.Pattern]]:
    normalized = _normalize(skill)
    return [
        (label, re.compile(pattern, re.IGNORECASE))
        for label, pattern in TARGET_PATTERNS
        if re.search(pattern, normalized, re.IGNORECASE)
    ]


def _project_blocks(skillsheet: str) -> List[str]:
    matches = list(PROJECT_START_RE.finditer(skillsheet))
    blocks: List[str] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(skillsheet)
        section = re.search(
            r"^(?:■スキル|保有技術|【スキル)",
            skillsheet[match.end() : end],
            re.MULTILINE,
        )
        if section:
            end = match.end() + section.start()
        blocks.append(skillsheet[match.start() : end])
    return blocks


def _target_months_in_skillsheet(skillsheet: str, target: re.Pattern) -> int:
    direct_fact_max = 0
    for line in skillsheet.splitlines():
        if target.search(line):
            direct_fact_max = max(direct_fact_max, _fact_months(line))
    block_total = 0
    for block in _project_blocks(skillsheet):
        if not target.search(block):
            continue
        plausible = [months for months in _duration_values(block) if 0 < months <= 600]
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


def explicit_years_proven(
    skill: str, evidence: str, skillsheet: str
) -> Tuple[bool, Dict[str, int]]:
    requirement = _required_years(skill)
    if requirement is None:
        return True, {}
    required_months, strict_over = requirement
    proof: Dict[str, int] = {}
    normalized_evidence = _normalize(evidence)
    safe_skillsheet = sanitize_skillsheet_for_guard(skillsheet)
    # sample/記入例を除去したsheetでは、LLM evidenceが除外部分を引用した可能性を
    # 安全に否定できないため、evidence単独の期間証明を使用しない。
    evidence_duration_allowed = safe_skillsheet == _normalize(skillsheet).strip()
    for label, pattern in _target_patterns(skill):
        evidence_months = (
            _fact_months(normalized_evidence)
            if evidence_duration_allowed and pattern.search(normalized_evidence)
            else 0
        )
        skillsheet_months = _target_months_in_skillsheet(safe_skillsheet, pattern)
        proof[label] = max(evidence_months, skillsheet_months)
    if evidence_duration_allowed and _generic_evidence_duration_allowed(skill, evidence):
        proof["generic_evidence"] = _fact_months(evidence)
    if not proof:
        return False, proof
    best = max(proof.values())
    return (best > required_months if strict_over else best >= required_months), proof


def explicit_years_contradicted(
    skill: str, evidence: str, skillsheet: str
) -> Tuple[bool, Dict[str, int]]:
    """単一対象技術の明示期間がrequired年数未満の場合だけTrue。

    期間記載がない、複数技術のOR条件、対象技術と期間の対応が
    不明な場合は自動降格しない。
    """
    requirement = _required_years(skill)
    if requirement is None:
        return False, {}
    targets = _target_patterns(skill)
    if len(targets) != 1:
        return False, {}
    _, proof = explicit_years_proven(skill, evidence, skillsheet)
    actual_months = proof.get(targets[0][0], 0)
    if actual_months <= 0:
        return False, proof
    required_months, strict_over = requirement
    contradicted = (
        actual_months <= required_months
        if strict_over
        else actual_months < required_months
    )
    return contradicted, proof


def _downgrade(check: Dict[str, Any], reason: str) -> None:
    check["confidence"] = "human_review"
    check["recheck_match"] = True
    original_reason = str(check.get("reason") or "").strip()
    check["reason"] = f"{original_reason} / {reason}" if original_reason else reason


def _apply_optional_guard(checks: List[Dict[str, Any]]) -> int:
    changed = 0
    for check in checks:
        if check.get("confidence") == "not_confirmed" and any(
            marker in _normalize(check.get("skill")) for marker in OPTIONAL_MARKERS
        ):
            _downgrade(check, "Batch optional条件 safety guard")
            changed += 1
    return changed


def _has_neighbor_requirement_fact(checks: Sequence[Dict[str, Any]]) -> bool:
    return any(
        _normalize(check.get("skill")) in ("要求定義", "要件定義")
        and check.get("confidence") in ("confirmed", "human_review")
        and bool(str(check.get("evidence") or "").strip())
        for check in checks
    )


def _apply_narrow_fact_guard(
    checks: List[Dict[str, Any]], skillsheet: str
) -> int:
    changed = 0
    safe_skillsheet = sanitize_skillsheet_for_guard(skillsheet)
    for check in checks:
        if check.get("confidence") != "not_confirmed":
            continue
        skill = _normalize(check.get("skill")).replace(" ", "")
        if (
            skill == "ユーザー定義"
            and _has_neighbor_requirement_fact(checks)
            and "画面設計・実装" in safe_skillsheet
            and "要件定義" in safe_skillsheet
        ):
            _downgrade(check, "近接する要件/要求定義の明示事実あり")
            changed += 1
        elif skill == "IA/UI/UX設計" and re.search(
            r"ユーザー(?:の)?導線.{0,40}画面設計.{0,30}実装",
            safe_skillsheet,
            re.DOTALL,
        ):
            _downgrade(check, "ユーザー導線を考慮した画面設計・実装の明示事実あり")
            changed += 1
        elif skill == "データベースの基礎知識(SQL)" and re.search(
            r"(?:MySQL|PostgreSQL|SQL\s*Server|Oracle)\s*\|?\s*\d+\s*(?:年|(?:ヶ|か|カ)?月)",
            safe_skillsheet,
            re.IGNORECASE,
        ):
            _downgrade(check, "対象DBの明示的な経験期間あり")
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
        if any(marker in _normalize(skill) for marker in OPTIONAL_MARKERS):
            continue
        proven, proof = explicit_years_proven(
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
            _downgrade(
                check,
                "対象skillと明示期間の対応から要求年数を証明できないため人間確認",
            )
            changed += 1
    return changed, decisions


def _apply_explicit_years_contradiction_guard(
    checks: List[Dict[str, Any]], skillsheet: str
) -> Tuple[int, List[Dict[str, Any]]]:
    changed = 0
    decisions: List[Dict[str, Any]] = []
    for check in checks:
        if check.get("confidence") != "confirmed":
            continue
        skill = str(check.get("skill") or "")
        contradicted, proof = explicit_years_contradicted(
            skill, str(check.get("evidence") or ""), skillsheet
        )
        if not proof:
            continue
        decisions.append(
            {
                "skill": skill,
                "contradicted": contradicted,
                "proof_months_by_target": proof,
            }
        )
        if contradicted:
            _downgrade(check, "対象skillの明示期間がrequired年数未満のため人間確認")
            changed += 1
    return changed, decisions


def _apply_git_compound_guard(
    checks: List[Dict[str, Any]], skillsheet: str
) -> int:
    """Git系の明示的な複合requiredを、一部の証拠だけでconfirmedにしない。"""
    normalized_sheet = sanitize_skillsheet_for_guard(skillsheet)
    if not normalized_sheet:
        return 0
    # GitHub CopilotはAI coding toolであり、単独ではGit/GitHub運用証拠ではない。
    source_sheet = GIT_COPILOT_RE.sub("", normalized_sheet)
    changed = 0
    for check in checks:
        if check.get("confidence") != "confirmed":
            continue
        skill = _normalize(check.get("skill"))
        # Copilot利用自体を求めるrequiredはソース管理compoundではない。
        source_control_skill = GIT_COPILOT_RE.sub("", skill)
        if (
            not GIT_REQUIRED_RE.search(source_control_skill)
            or not TEAM_REQUIRED_RE.search(source_control_skill)
        ):
            continue
        evidence = _normalize(check.get("evidence"))
        source_context = source_sheet + "\n" + GIT_COPILOT_RE.sub("", evidence)
        evidence_context = normalized_sheet + "\n" + evidence
        source_proven = bool(GIT_SOURCE_RE.search(source_context))
        team_proven = bool(TEAM_EVIDENCE_RE.search(evidence_context))
        review_proven = bool(REVIEW_REQUIRED_RE.search(evidence_context))
        needs_review = bool(REVIEW_REQUIRED_RE.search(skill))
        missing: List[str] = []
        if not source_proven:
            missing.append("Git/GitHub等のソース管理実績")
        if not team_proven:
            missing.append("チーム開発実績")
        if needs_review and not review_proven:
            missing.append("PR/MR/code review運用実績")
        if missing:
            _downgrade(
                check,
                "compound requiredの直接証拠不足: " + " / ".join(missing),
            )
            changed += 1
    return changed


def apply_local_false_positive_guard(
    checks: Sequence[Dict[str, Any]], skillsheet: str
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Batch/legacy共通の局所guard。confirmedの全件一致時だけ保守側へ降格する。"""
    guarded = copy.deepcopy(list(checks))
    before_status = _status_from_checks(guarded)
    reasons: List[str] = []
    years_decisions: List[Dict[str, Any]] = []
    if before_status == "confirmed":
        years_changes, years_decisions = _apply_explicit_years_contradiction_guard(
            guarded, skillsheet
        )
        compound_changes = _apply_git_compound_guard(guarded, skillsheet)
        if years_changes:
            reasons.append(f"explicit_years_contradicted:{years_changes}")
        if compound_changes:
            reasons.append(f"git_compound_evidence_missing:{compound_changes}")
    return guarded, {
        "guard_version": GUARD_VERSION,
        "before_status": before_status,
        "after_status": _status_from_checks(guarded),
        "guard_reasons": reasons,
        "explicit_years_contradiction_decisions": years_decisions,
        "promoted_to_confirmed": 0,
    }


def apply_minimal_safety_guard(
    checks: Sequence[Dict[str, Any]],
    category_match: str,
    schema_valid: bool,
    skillsheet: str,
) -> Tuple[List[Dict[str, Any]], str, Dict[str, Any]]:
    """Batch結果を保守側へだけ変更し、guard metadataと共に返す。"""
    before_checks = copy.deepcopy(list(checks))
    guarded = copy.deepcopy(list(checks))
    before_status = _status_from_checks(before_checks)
    before_confirmed_count = sum(
        check.get("confidence") == "confirmed" for check in before_checks
    )
    reasons: List[str] = []

    guarded, local_metadata = apply_local_false_positive_guard(guarded, skillsheet)
    reasons.extend(local_metadata["guard_reasons"])

    optional_changes = _apply_optional_guard(guarded)
    if optional_changes:
        reasons.append(f"optional_condition:{optional_changes}")

    guarded_category = str(category_match or "unclear")
    if not schema_valid and guarded_category == "mismatch":
        guarded_category = "unclear"
        reasons.append("schema_invalid_category_mismatch")

    narrow_changes = _apply_narrow_fact_guard(guarded, skillsheet)
    if narrow_changes:
        reasons.append(f"narrow_nearby_fact:{narrow_changes}")

    if before_status == "confirmed":
        years_changes, years_decisions = _apply_explicit_years_guard(
            guarded, skillsheet
        )
    else:
        years_changes, years_decisions = 0, []
    if years_changes:
        reasons.append(f"explicit_years_unproven:{years_changes}")

    after_confirmed_count = sum(
        check.get("confidence") == "confirmed" for check in guarded
    )
    if after_confirmed_count > before_confirmed_count:
        raise AssertionError("minimal safety guardがconfirmedへ昇格しました")
    return guarded, guarded_category, {
        "guard_version": GUARD_VERSION,
        "before_status": before_status,
        "after_status": _status_from_checks(guarded),
        "guard_reasons": reasons,
        "explicit_years_decisions": years_decisions,
        "explicit_years_contradiction_decisions": local_metadata[
            "explicit_years_contradiction_decisions"
        ],
        "promoted_to_confirmed": 0,
    }

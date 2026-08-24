"""07-1 concurrent結果を08-5再確認へ安全に追加到達させるfail-closed guard。"""

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple


RETENTION_GATE_RATE = 0.8
TECH_TOKEN_RE = re.compile(
    r"(?<![A-Za-z0-9])([A-Za-z][A-Za-z0-9+#-]*(?:\.[A-Za-z0-9+#-]+)*)"
)
REQUIRED_DURATION_RE = re.compile(
    r"(?:[0-9０-９]+(?:\.[0-9０-９]+)?|[一二三四五六七八九十百]+)\s*"
    r"(?:(?:ヶ|か|カ|ケ|ヵ)?月|年(?:間)?)"
)
DURATION_VALUE_RE = re.compile(
    r"(?:[0-9０-９]+(?:\.[0-9０-９]+)?|[一二三四五六七八九十百]+)\s*"
    r"(?:(?:ヶ|か|カ|ケ|ヵ)?月|年(?:間)?)"
)
PRACTICAL_MARKER_RE = re.compile(
    r"(?:実務|業務|案件|開発|実装|構築|製造|改修|運用|保守|担当|対応|"
    r"作成|設計|テスト|導入|スクレイピング|デプロイ)"
)
NON_PRACTICAL_ONLY_RE = re.compile(
    r"(?:自己学習|独学|学習中?|研修|資格|勉強|個人開発)"
)
ALTERNATIVE_CUE_RE = re.compile(r"(?:または|もしくは|或いは|\bor\b)", re.I)
CONJUNCTION_CUE_RE = re.compile(
    r"(?:および|及び|かつ|且つ|ならびに|並びに|\band\b|"
    r"(?<=[A-Za-z0-9+#.])\s*と(?!して|した|する|の|も)|"
    r"と\s*(?=[A-Za-z0-9]))",
    re.I,
)
TOKEN_STOP_WORDS = {"and", "or", "etc", "web", "api", "os", "ai", "it"}
ACTION_EVIDENCE_RULES = (
    (re.compile(r"設計"), re.compile(r"(?:設計|アーキテクチャ)")),
    (
        re.compile(r"(?:コーディング|プログラミング)"),
        re.compile(
            r"(?:コーディング|プログラミング|実装|製造|改修|作成|構築|"
            r"スクレイピング|自動テスト)"
        ),
    ),
    (re.compile(r"開発"), re.compile(r"(?:開発|実装|製造|改修|作成|構築)")),
    (re.compile(r"構築"), re.compile(r"(?:構築|実装|作成|セットアップ|環境構成)")),
)


def _technology_tokens(required_skill: str) -> List[str]:
    tokens: List[str] = []
    for raw in TECH_TOKEN_RE.findall(required_skill):
        normalized = raw.lower()
        if normalized in TOKEN_STOP_WORDS or len(normalized) < 2:
            continue
        if normalized not in tokens:
            tokens.append(normalized)
    return tokens


def _contains_token(text: str, token: str) -> bool:
    return bool(
        re.search(
            r"(?<![A-Za-z0-9]){}(?![A-Za-z0-9])".format(re.escape(token)),
            text,
            re.IGNORECASE,
        )
    )


def safe_retention_tokens(required_skill: str) -> List[str]:
    tokens = _technology_tokens(required_skill)
    if not tokens or CONJUNCTION_CUE_RE.search(required_skill):
        return []
    alternative_count = len(ALTERNATIVE_CUE_RE.findall(required_skill))
    if len(tokens) == 1:
        return tokens if alternative_count == 0 else []
    return tokens if alternative_count == len(tokens) - 1 else []


def duration_only_false_note(note: str, tokens: Sequence[str]) -> List[str]:
    lowered = note.lower().strip()
    if not lowered or not tokens:
        return []
    mentioned = [token for token in tokens if _contains_token(lowered, token)]
    if not mentioned and len(tokens) != 1:
        return []
    technology = r"(?:{})".format("|".join(re.escape(token) for token in tokens))
    subject = r"(?:(?:{})\s*(?:の)?\s*)?".format(technology)
    experience = r"(?:(?:実務|業務)?経験(?:期間|年数)?|期間|年数)"
    duration_only_re = re.compile(
        r"^{}{}\s*(?:が|は)?\s*(?:{}\s*(?:のみ|だけ|しか(?:ない)?|未満|で不足)|"
        r"(?:短い|短期|不足(?:している)?))\s*[。．.]?$".format(
            subject, experience, DURATION_VALUE_RE.pattern
        ),
        re.I,
    )
    if not duration_only_re.fullmatch(lowered):
        return []
    return mentioned or list(tokens)


def _required_actions_supported(required_skill: str, evidence_line: str) -> bool:
    applicable = [
        evidence_re
        for required_re, evidence_re in ACTION_EVIDENCE_RULES
        if required_re.search(required_skill)
    ]
    return all(evidence_re.search(evidence_line) for evidence_re in applicable)


def _direct_practical_evidence(
    required_skill: str, skillsheet: str, note_tokens: Sequence[str]
) -> Optional[Dict[str, str]]:
    for raw_line in skillsheet.splitlines():
        line = raw_line.strip()
        matching = [token for token in note_tokens if _contains_token(line, token)]
        if not matching:
            continue
        if NON_PRACTICAL_ONLY_RE.search(line) or not PRACTICAL_MARKER_RE.search(line):
            continue
        if not _required_actions_supported(required_skill, line):
            continue
        return {"matched_token": matching[0], "evidence": line[:240]}
    return None


def evaluate_required_skill(
    required_skill: Dict[str, Any], skillsheet: str
) -> Optional[Dict[str, Any]]:
    """全guard条件を満たすrequired skillだけaudit情報を返す。"""
    if required_skill.get("match") is not False:
        return None
    skill_name = str(required_skill.get("skill", "")).strip()
    note = str(required_skill.get("note", "")).strip()
    if not skill_name or REQUIRED_DURATION_RE.search(skill_name):
        return None
    tokens = safe_retention_tokens(skill_name)
    if not tokens:
        return None
    note_tokens = duration_only_false_note(note, tokens)
    if not note_tokens:
        return None
    evidence = _direct_practical_evidence(skill_name, skillsheet, note_tokens)
    if evidence is None:
        return None
    return {
        "required_skill": skill_name,
        "is_required": True,
        "explicit_min_duration": False,
        "concurrent_match": False,
        "concurrent_false_reason": note,
        "false_reason_duration_only": True,
        "matched_token": evidence["matched_token"],
        "skillsheet_direct_evidence": evidence["evidence"],
        "guard_reason": (
            "no_min_duration_direct_practical_evidence_duration_only_false"
        ),
    }


def required_rate(row: Dict[str, Any]) -> float:
    skills = row.get("required_skills") or []
    if not skills:
        return 0.0
    return sum(item.get("match") is True for item in skills) / len(skills)


def _optional_rate(row: Dict[str, Any]) -> float:
    skills = row.get("optional_skills") or []
    if not skills:
        return 0.0
    return sum(item.get("match") is True for item in skills) / len(skills)


def build_retention_sidecar(
    rows: Sequence[Dict[str, Any]], skillsheet_map: Dict[str, Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """canonical rowを変更せず、08-5追加入力recordと監査件数を返す。"""
    retained: List[Dict[str, Any]] = []
    mixed_retained = 0
    non_duration_retained = 0
    for row in rows:
        rate = required_rate(row)
        if rate >= RETENTION_GATE_RATE:
            continue
        resource_mid = str(row.get("resource_info", {}).get("message_id", ""))
        skillsheet = str(skillsheet_map.get(resource_mid, {}).get("skillsheet", ""))
        eligible = []
        for index, skill in enumerate(row.get("required_skills") or []):
            decision = evaluate_required_skill(skill, skillsheet)
            if decision is not None:
                eligible.append({"skill_index": index, **decision})
        if not eligible:
            continue
        optional_rate = _optional_rate(row)
        retained.append(
            {
                "project_info": {
                    "message_id": str(
                        row.get("project_info", {}).get("message_id", "")
                    ),
                    "required_skills": row.get("required_skills") or [],
                    "optional_skills": row.get("optional_skills") or [],
                },
                "resource_info": {"message_id": resource_mid},
                "duplicate_proposal_check": False,
                "match_info": {
                    "required_skills_match_rate": rate,
                    "optional_skills_match_rate": optional_rate,
                    "total_skills_match_rate": rate + optional_rate,
                },
                "retention_guard": {
                    "destination": "08-5_recheck_only",
                    "proposal_ready_direct": False,
                    "eligible_required_skills": eligible,
                },
            }
        )
        mixed_retained += sum(
            bool(ALTERNATIVE_CUE_RE.search(item["required_skill"]))
            and bool(CONJUNCTION_CUE_RE.search(item["required_skill"]))
            for item in eligible
        )
        non_duration_retained += sum(
            not duration_only_false_note(
                item["concurrent_false_reason"],
                safe_retention_tokens(item["required_skill"]),
            )
            for item in eligible
        )
    return retained, {
        "retained_pairs": len(retained),
        "mixed_or_and_retained": mixed_retained,
        "non_duration_reason_retained": non_duration_retained,
        "guard_false_to_true": 0,
        "proposal_ready_direct_promotion": 0,
    }

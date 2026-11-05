"""Generic shadow-only comparison helpers for skill registry M0."""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from common.skill_registry import RegistrySnapshot, SkillMatcher


def _unique(values: Iterable[str]) -> List[str]:
    return list(dict.fromkeys(value for value in values if value))


def _filter_skills(values: Sequence[str], excluded: Iterable[str]) -> List[str]:
    excluded_set = set(excluded)
    return [value for value in _unique(values or []) if value not in excluded_set]


@dataclass(frozen=True)
class SkillGateResult:
    passed: bool
    matched_skills: Tuple[str, ...]
    project_skills_used: Tuple[str, ...]
    resource_skills_used: Tuple[str, ...]
    primary_skills_used: Tuple[str, ...]
    reason: str

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "matched_skills": list(self.matched_skills),
            "project_skills_used": list(self.project_skills_used),
            "resource_skills_used": list(self.resource_skills_used),
            "primary_skills_used": list(self.primary_skills_used),
            "reason": self.reason,
        }


def judge_skill_gate(
    project_skills: Sequence[str],
    resource_skills: Sequence[str],
    primary_skills: Sequence[str],
    excluded_skills: Sequence[str],
) -> SkillGateResult:
    filtered_project = _filter_skills(project_skills, excluded_skills)
    filtered_resource = _filter_skills(resource_skills, excluded_skills)
    filtered_primary = _filter_skills(primary_skills, excluded_skills)
    if not project_skills or not resource_skills or not filtered_project or not filtered_resource:
        return SkillGateResult(
            False,
            (),
            tuple(filtered_project),
            tuple(filtered_resource),
            tuple(filtered_primary),
            "empty_skill_side",
        )
    resource_set = set(filtered_resource)
    compared = filtered_primary if filtered_primary else filtered_project
    matched = tuple(value for value in compared if value in resource_set)
    return SkillGateResult(
        bool(matched),
        matched,
        tuple(filtered_project),
        tuple(filtered_resource),
        tuple(filtered_primary),
        "matched_primary_skill" if matched and filtered_primary else (
            "matched_skill" if matched else "no_exact_skill_overlap"
        ),
    )


def _normalize(value: str) -> str:
    return unicodedata.normalize("NFKC", value or "").casefold()


def _registry_keyword(
    keyword: str,
    snapshot: RegistrySnapshot,
    legacy_alias_map: Mapping[str, Sequence[str]],
    canonicalize_canonicals: Iterable[str],
) -> Tuple[str, Tuple[str, ...], Optional[str]]:
    canonical = snapshot.resolve_surface(keyword)
    if canonical is not None:
        if canonical not in canonicalize_canonicals:
            return keyword, tuple(legacy_alias_map.get(keyword, [keyword])), None
        registry_surfaces = snapshot.aliases_for(canonical) or (canonical,)
        return canonical, tuple(registry_surfaces), canonical
    return keyword, tuple(legacy_alias_map.get(keyword, [keyword])), None


@dataclass(frozen=True)
class RequiredGateResult:
    passed: bool
    matched_keywords: Tuple[str, ...]
    matched_phase_keywords: Tuple[str, ...]
    matched_in: Tuple[str, ...]
    canonical_groups: Tuple[str, ...]
    reason: str

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "matched_keywords": list(self.matched_keywords),
            "matched_phase_keywords": list(self.matched_phase_keywords),
            "matched_in": list(self.matched_in),
            "canonical_groups": list(self.canonical_groups),
            "reason": self.reason,
        }


def judge_required_gate_legacy_substring(
    required_skill_keywords: Sequence[str],
    strong_phase_keywords: Sequence[str],
    weak_phase_keywords: Sequence[str],
    skillsheet_text: str,
    email_body_text: str,
    snapshot: RegistrySnapshot,
    legacy_alias_map: Mapping[str, Sequence[str]],
    canonicalize_canonicals: Optional[Iterable[str]] = None,
) -> RequiredGateResult:
    if not required_skill_keywords:
        reason = (
            "rejected_no_required_conditions"
            if not strong_phase_keywords and not weak_phase_keywords
            else "not_matched"
        )
        return RequiredGateResult(False, (), (), (), (), reason)

    canonicalize_set = set(
        snapshot.canonical_names
        if canonicalize_canonicals is None
        else canonicalize_canonicals
    )
    prepared = [
        _registry_keyword(keyword, snapshot, legacy_alias_map, canonicalize_set)
        for keyword in required_skill_keywords
    ]
    texts = (("skillsheet", skillsheet_text or ""), ("email_body", email_body_text or ""))
    best_matched: List[str] = []
    best_groups: List[str] = []
    for source_name, text in texts:
        normalized_text = _normalize(text)
        matched: List[str] = []
        groups: List[str] = []
        for output_keyword, aliases, group in prepared:
            if any(_normalize(alias) in normalized_text for alias in aliases):
                matched.append(output_keyword)
                if group is not None:
                    groups.append(group)
        if not matched:
            continue
        if strong_phase_keywords:
            phase_hits = [
                keyword
                for keyword in strong_phase_keywords
                if _normalize(keyword) in normalized_text
            ]
            if phase_hits:
                return RequiredGateResult(
                    True,
                    tuple(_unique(matched)),
                    tuple(_unique(phase_hits)),
                    (source_name,),
                    tuple(_unique(groups)),
                    "matched_required_skill_and_phase",
                )
            if len(matched) > len(best_matched):
                best_matched = list(matched)
                best_groups = list(groups)
            continue
        if weak_phase_keywords:
            phase_hits = [
                keyword
                for keyword in weak_phase_keywords
                if _normalize(keyword) in normalized_text
            ]
            return RequiredGateResult(
                True,
                tuple(_unique(matched)),
                tuple(_unique(phase_hits)),
                (source_name,),
                tuple(_unique(groups)),
                (
                    "matched_required_skill_and_phase"
                    if phase_hits
                    else "matched_required_skill_only"
                ),
            )
        return RequiredGateResult(
            True,
            tuple(_unique(matched)),
            (),
            (source_name,),
            tuple(_unique(groups)),
            "matched_required_skill_only",
        )
    return RequiredGateResult(
        False,
        tuple(_unique(best_matched)),
        (),
        (),
        tuple(_unique(best_groups)),
        "not_matched",
    )


def canonical_groups_for_skill_diff(
    snapshot: RegistrySnapshot,
    legacy_values: Sequence[str],
    registry_values: Sequence[str],
) -> Tuple[str, ...]:
    groups: List[str] = []
    legacy_set = set(legacy_values or [])
    registry_set = set(registry_values or [])
    changed_values = [
        value
        for value in list(legacy_values or []) + list(registry_values or [])
        if value in legacy_set.symmetric_difference(registry_set)
    ]
    if not changed_values and list(legacy_values or []) != list(registry_values or []):
        legacy_list = list(legacy_values or [])
        registry_list = list(registry_values or [])
        for index in range(max(len(legacy_list), len(registry_list))):
            legacy_value = legacy_list[index] if index < len(legacy_list) else None
            registry_value = registry_list[index] if index < len(registry_list) else None
            if legacy_value != registry_value:
                if legacy_value is not None:
                    changed_values.append(legacy_value)
                if registry_value is not None:
                    changed_values.append(registry_value)
    for value in changed_values:
        canonical = snapshot.resolve_surface(value)
        if canonical is not None:
            groups.append(canonical)
        elif snapshot.entry_for_canonical(value) is not None:
            groups.append(value)
    return tuple(_unique(groups))


def expected_difference(
    difference_type: str,
    legacy_passed: Optional[bool],
    registry_passed: Optional[bool],
    canonical_groups: Sequence[str],
    improvement_canonicals: Sequence[str],
) -> Tuple[bool, str]:
    groups = set(canonical_groups)
    improvements = set(improvement_canonicals)
    if not groups or not groups.issubset(improvements):
        return False, "non_improvement_canonical_changed"
    if legacy_passed is True and registry_passed is False:
        return False, "pass_to_reject"
    if difference_type in {
        "extraction_changed",
        "primary_changed",
        "reject_to_pass",
        "match_detail_changed",
    }:
        return True, "registry_canonical_improvement"
    return False, "unclassified_difference"

#!/usr/bin/env python3
"""Variable-cardinality contracts for the test-only 99-1 lab."""

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence


class CardinalityAuthority(str, Enum):
    DECLARED_COUNT = "DECLARED_COUNT"
    STRUCTURAL_COMPLETE = "STRUCTURAL_COMPLETE"
    CONTAINER_ENUMERATION = "CONTAINER_ENUMERATION"
    SNAPSHOT_SET = "SNAPSHOT_SET"
    UNKNOWN = "UNKNOWN"


class DeliverySemantics(str, Enum):
    SNAPSHOT = "SNAPSHOT"
    INCREMENTAL = "INCREMENTAL"
    UNKNOWN = "UNKNOWN"


class ContainerKind(str, Enum):
    INLINE_BODY = "INLINE_BODY"
    ATTACHMENT_LIST = "ATTACHMENT_LIST"
    ATTACHMENT_FILE = "ATTACHMENT_FILE"
    ARCHIVE = "ARCHIVE"
    SPREADSHEET = "SPREADSHEET"
    PDF = "PDF"
    WEB_PAGE = "WEB_PAGE"
    WEB_LIST = "WEB_LIST"
    GOOGLE_SHEET = "GOOGLE_SHEET"
    LINK_BUNDLE = "LINK_BUNDLE"


class EnumerationStatus(str, Enum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"
    UNSUPPORTED = "UNSUPPORTED"


PARSE_STATUSES = {
    "PARSED",
    "PARTIAL",
    "UNSUPPORTED",
    "HUMAN_REVIEW",
    "SYSTEM_FAILURE",
}


@dataclass(frozen=True)
class CardinalityEvidence:
    authority: str
    source: str
    count: Optional[int]
    complete: bool
    is_primary: bool = False
    reasons: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.authority not in {value.value for value in CardinalityAuthority}:
            raise ValueError(f"unsupported cardinality authority: {self.authority}")
        if self.count is not None and (
            isinstance(self.count, bool) or not isinstance(self.count, int) or self.count < 0
        ):
            raise ValueError("cardinality evidence count must be a non-negative integer")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CompletenessResult:
    status: str
    reasons: List[str]
    expected_count: Optional[int]
    candidate_count: int
    checks: Dict[str, bool]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Container:
    container_id: str
    parent_container_id: str
    kind: str
    locator: str
    content_fingerprint: str
    enumeration_status: str
    completeness: bool
    candidate_count: int
    child_container_refs: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)
    required: bool = False

    def __post_init__(self) -> None:
        if self.kind not in {value.value for value in ContainerKind}:
            raise ValueError(f"unsupported container kind: {self.kind}")
        if self.enumeration_status not in {value.value for value in EnumerationStatus}:
            raise ValueError(f"unsupported enumeration status: {self.enumeration_status}")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Source:
    source_id: str
    source_type: str
    source_company: str
    source_fingerprint: str
    delivery_semantics: str
    acquisition_status: str
    cardinality_evidence: List[CardinalityEvidence]
    container_references: List[str]
    configured_primary_authority: str
    configured_cross_check_authorities: List[str]
    artifact_relations_resolved: bool = True
    artifact_relation_reasons: List[str] = field(default_factory=list)
    completeness_result: Optional[CompletenessResult] = None

    def __post_init__(self) -> None:
        if self.delivery_semantics not in {value.value for value in DeliverySemantics}:
            raise ValueError(f"unsupported delivery semantics: {self.delivery_semantics}")
        if self.acquisition_status not in {"COMPLETE", "INCOMPLETE"}:
            raise ValueError(f"unsupported acquisition status: {self.acquisition_status}")
        valid_authorities = {value.value for value in CardinalityAuthority}
        if self.configured_primary_authority not in valid_authorities:
            raise ValueError(
                "unsupported configured primary authority: "
                + self.configured_primary_authority
            )
        if any(
            authority not in valid_authorities
            for authority in self.configured_cross_check_authorities
        ):
            raise ValueError("unsupported configured cross-check authority")

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["cardinality_evidence"] = [
            evidence.to_dict() for evidence in self.cardinality_evidence
        ]
        result["completeness_result"] = (
            self.completeness_result.to_dict() if self.completeness_result else {}
        )
        return result


@dataclass
class ItemCandidate:
    candidate_index: int
    identifier: str
    source_container_id: str
    body_text: str
    parse_success: bool
    parse_reasons: List[str] = field(default_factory=list)
    item_artifacts: List[Dict[str, Any]] = field(default_factory=list)
    artifact_relation_resolved: bool = True
    identity_evidence: Dict[str, Any] = field(default_factory=dict)
    logical_item_id: str = ""


def evaluate_completeness(
    source: Source,
    containers: Sequence[Container],
    candidates: Sequence[ItemCandidate],
) -> CompletenessResult:
    """Apply the source-atomic gate; only PARSED permits canonical emission."""
    checks: Dict[str, bool] = {}
    reasons: List[str] = []

    checks["source_acquisition_complete"] = source.acquisition_status == "COMPLETE"
    if not checks["source_acquisition_complete"]:
        reasons.append("source_acquisition_incomplete")

    container_by_id = {container.container_id: container for container in containers}
    required_ids = {
        container.container_id for container in containers if container.required
    }
    checks["required_containers_found"] = bool(required_ids) and required_ids <= set(
        source.container_references
    ) and required_ids <= set(container_by_id)
    if not checks["required_containers_found"]:
        reasons.append("required_container_missing")

    referenced_containers = [
        container_by_id[container_id]
        for container_id in source.container_references
        if container_id in container_by_id
    ]
    checks["container_enumeration_complete"] = bool(referenced_containers) and all(
        container.enumeration_status == EnumerationStatus.COMPLETE.value
        and container.completeness
        for container in referenced_containers
    )
    if not checks["container_enumeration_complete"]:
        reasons.append("container_enumeration_incomplete")

    primary_rows = [
        evidence for evidence in source.cardinality_evidence if evidence.is_primary
    ]
    checks["primary_evidence_exactly_one"] = len(primary_rows) == 1
    if not checks["primary_evidence_exactly_one"]:
        reasons.append(f"primary_evidence_count:{len(primary_rows)}:expected:1")
    primary = primary_rows[0] if len(primary_rows) == 1 else None
    checks["primary_authority_matches"] = (
        primary is not None
        and primary.authority == source.configured_primary_authority
    )
    if not checks["primary_authority_matches"]:
        reasons.append("primary_authority_missing_or_mismatch")
    checks["primary_count_known"] = (
        primary is not None
        and primary.authority != CardinalityAuthority.UNKNOWN.value
        and primary.count is not None
    )
    if not checks["primary_count_known"]:
        reasons.append("primary_cardinality_unknown")
    checks["primary_complete"] = primary is not None and primary.complete
    if not checks["primary_complete"]:
        reasons.append("primary_cardinality_incomplete")
    checks["primary_unambiguous"] = primary is not None and not primary.reasons
    if not checks["primary_unambiguous"]:
        reasons.append("primary_cardinality_ambiguous")

    primary_valid = all(
        checks[name]
        for name in (
            "primary_evidence_exactly_one",
            "primary_authority_matches",
            "primary_count_known",
            "primary_complete",
            "primary_unambiguous",
        )
    )
    expected_count = primary.count if primary_valid and primary is not None else None

    cross_checks = [
        evidence for evidence in source.cardinality_evidence if not evidence.is_primary
    ]
    actual_cross_check_authorities = sorted(
        evidence.authority for evidence in cross_checks
    )
    configured_cross_check_authorities = sorted(
        source.configured_cross_check_authorities
    )
    checks["cross_checks_present"] = (
        actual_cross_check_authorities == configured_cross_check_authorities
    )
    if not checks["cross_checks_present"]:
        reasons.append(
            "cross_check_evidence_mismatch:configured:"
            + ",".join(configured_cross_check_authorities)
            + ":actual:"
            + ",".join(actual_cross_check_authorities)
        )
    checks["cross_checks_known"] = checks["cross_checks_present"] and all(
        evidence.authority != CardinalityAuthority.UNKNOWN.value
        and evidence.count is not None
        for evidence in cross_checks
    )
    if not checks["cross_checks_known"]:
        reasons.append("cross_check_cardinality_unknown")
    checks["cross_checks_complete"] = checks["cross_checks_present"] and all(
        evidence.complete for evidence in cross_checks
    )
    if not checks["cross_checks_complete"]:
        reasons.append("cross_check_cardinality_incomplete")
    checks["cross_checks_unambiguous"] = checks["cross_checks_present"] and all(
        not evidence.reasons for evidence in cross_checks
    )
    if not checks["cross_checks_unambiguous"]:
        reasons.append("cross_check_cardinality_ambiguous")
    checks["cross_checks_match_primary"] = (
        primary_valid
        and checks["cross_checks_known"]
        and all(evidence.count == expected_count for evidence in cross_checks)
    )
    if not checks["cross_checks_match_primary"]:
        cross_check_counts = sorted(
            {
                evidence.count
                for evidence in cross_checks
                if evidence.count is not None
            }
        )
        if expected_count is not None and cross_check_counts:
            reasons.append(
                "cardinality_evidence_conflict:"
                + ",".join(
                    str(value)
                    for value in sorted(set(cross_check_counts) | {expected_count})
                )
            )

    checks["cardinality_known"] = primary_valid
    checks["cardinality_evidence_consistent"] = primary_valid and all(
        checks[name]
        for name in (
            "cross_checks_present",
            "cross_checks_known",
            "cross_checks_complete",
            "cross_checks_unambiguous",
            "cross_checks_match_primary",
        )
    )

    checks["candidate_count_matches"] = (
        expected_count is not None and expected_count == len(candidates)
    )
    if not checks["candidate_count_matches"]:
        reasons.append(
            f"candidate_count:{len(candidates)}:expected:"
            + (str(expected_count) if expected_count is not None else "unknown")
        )

    checks["all_candidates_parsed"] = all(candidate.parse_success for candidate in candidates)
    if not checks["all_candidates_parsed"]:
        reasons.append("candidate_parse_incomplete")
        for candidate in candidates:
            reasons.extend(candidate.parse_reasons)

    checks["artifact_relations_resolved"] = source.artifact_relations_resolved and all(
        candidate.artifact_relation_resolved for candidate in candidates
    )
    if not checks["artifact_relations_resolved"]:
        reasons.append("artifact_relation_ambiguous")
        reasons.extend(source.artifact_relation_reasons)

    logical_ids = [candidate.logical_item_id for candidate in candidates]
    checks["identity_collision_free"] = all(logical_ids) and len(logical_ids) == len(
        set(logical_ids)
    )
    if not candidates:
        checks["identity_collision_free"] = True
    if not checks["identity_collision_free"]:
        reasons.append("delivery_identity_collision")

    only_unknown_evidence = bool(source.cardinality_evidence) and all(
        evidence.authority == CardinalityAuthority.UNKNOWN.value
        for evidence in source.cardinality_evidence
    )
    if not checks["cardinality_known"] and only_unknown_evidence:
        status = "UNSUPPORTED"
    elif not checks["cardinality_evidence_consistent"]:
        status = "PARTIAL"
    elif not all(
        checks[name]
        for name in (
            "source_acquisition_complete",
            "required_containers_found",
            "container_enumeration_complete",
            "cardinality_known",
            "candidate_count_matches",
            "all_candidates_parsed",
        )
    ):
        status = "PARTIAL"
    elif not checks["artifact_relations_resolved"] or not checks["identity_collision_free"]:
        status = "HUMAN_REVIEW"
    elif all(checks.values()):
        status = "PARSED"
    else:
        status = "PARTIAL"

    result = CompletenessResult(
        status=status,
        reasons=list(dict.fromkeys(reasons)),
        expected_count=expected_count,
        candidate_count=len(candidates),
        checks=checks,
    )
    source.completeness_result = result
    return result

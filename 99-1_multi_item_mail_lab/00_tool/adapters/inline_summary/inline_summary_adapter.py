#!/usr/bin/env python3
"""Config-driven variable-cardinality adapter for inline multi-item mail."""

import hashlib
import json
import re
from dataclasses import dataclass, field
from email.utils import parseaddr
from pathlib import Path
from typing import Any, Dict, List, Match, Optional, Pattern, Sequence, Tuple

from identity import (
    artifact_set_fingerprint,
    attachment_fingerprint,
    body_fingerprint,
    canonical_subject,
    derived_item_id,
    logical_item_id,
    normalize_block_identifier,
    normalize_content,
    version_fingerprint,
)
from variable_item_core import (
    CardinalityAuthority,
    CardinalityEvidence,
    Container,
    ContainerKind,
    DeliverySemantics,
    EnumerationStatus,
    ItemCandidate,
    Source,
    evaluate_completeness,
)


ADAPTER_ID = "inline_summary"
ADAPTER_VERSION = "2.0.0"
VALID_STATUSES = {
    "PARSED",
    "PARTIAL",
    "UNSUPPORTED",
    "HUMAN_REVIEW",
    "SYSTEM_FAILURE",
}


@dataclass(frozen=True)
class ParseResult:
    status: str
    reasons: List[str]
    items: List[Dict[str, Any]]
    source: Dict[str, Any] = field(default_factory=dict)
    containers: List[Dict[str, Any]] = field(default_factory=list)


class InlineSummaryAdapter:
    """Enumerate 0..N complete inline blocks and resolve configured artifacts."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        if "expected_item_count" in config:
            raise ValueError("runtime expected_item_count is forbidden")
        self._anchor_regex: Pattern[str] = re.compile(config["item_start_regex"])
        self._separator_regex: Pattern[str] = re.compile(config["separator_regex"])
        self._footer_regex: Pattern[str] = re.compile(config["footer_end_regex"])
        self._subject_regex: Pattern[str] = re.compile(
            config["selectors"]["subject_regex"], re.IGNORECASE
        )
        self._cardinality_config = config["cardinality"]
        primary = self._cardinality_config["primary"]
        self._primary_authority = primary["authority"]
        self._declared_count_regex: Optional[Pattern[str]] = None
        if self._primary_authority == CardinalityAuthority.DECLARED_COUNT.value:
            self._declared_count_regex = re.compile(primary["count_regex"], re.IGNORECASE)
            if "count" not in self._declared_count_regex.groupindex:
                raise ValueError("declared count regex must contain named count group")

        mapping_config = config["attachment_mapping"]
        if mapping_config["strategy"] != "ONE_ARTIFACT_PER_ITEM_EXACT_KEY":
            raise ValueError("unsupported attachment mapping strategy")
        self._filename_field = mapping_config["filename_field"]
        self._filename_identifier_group = mapping_config["identifier_group"]
        self._filename_identifier_regex: Pattern[str] = re.compile(
            mapping_config["filename_identifier_regex"], re.IGNORECASE
        )
        if self._filename_identifier_group not in self._filename_identifier_regex.groupindex:
            raise ValueError("filename identifier regex must contain configured named group")

    @classmethod
    def from_file(cls, path: Path) -> "InlineSummaryAdapter":
        with path.open(encoding="utf-8") as file_object:
            return cls(json.load(file_object))

    def matches(self, mail: Dict[str, Any]) -> bool:
        sender = parseaddr(str(mail.get("from", "")))[1]
        sender_domain = sender.rsplit("@", 1)[-1].casefold() if "@" in sender else ""
        expected_domain = self.config["selectors"]["sender_domain"].casefold()
        return sender_domain == expected_domain and bool(
            self._subject_regex.search(str(mail.get("subject", "")))
        )

    @staticmethod
    def _normalize_newlines(value: str) -> str:
        return (value or "").replace("\r\n", "\n").replace("\r", "\n")

    @staticmethod
    def _source_fingerprint(mail: Dict[str, Any]) -> str:
        serialized = json.dumps(
            mail,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return "sha256:" + hashlib.sha256(serialized).hexdigest()

    def _new_source(self, mail: Dict[str, Any]) -> Source:
        attachments = mail.get("attachments")
        acquisition_complete = (
            isinstance(mail.get("message_id"), str)
            and bool(mail.get("message_id"))
            and isinstance(mail.get("body_text"), str)
            and isinstance(attachments, list)
        )
        return Source(
            source_id=str(mail.get("message_id", "")),
            source_type="EMAIL",
            source_company=self.config["source_company"],
            source_fingerprint=self._source_fingerprint(mail),
            delivery_semantics=self.config.get(
                "delivery_semantics", DeliverySemantics.UNKNOWN.value
            ),
            acquisition_status="COMPLETE" if acquisition_complete else "INCOMPLETE",
            cardinality_evidence=[],
            container_references=[],
            configured_primary_authority=self._primary_authority,
            configured_cross_check_authorities=[
                row["authority"]
                for row in self._cardinality_config.get("cross_checks", [])
            ],
        )

    def _enumerate_blocks(
        self,
        body_text: str,
        anchors: Sequence[Match[str]],
        body_container_id: str,
    ) -> Tuple[List[ItemCandidate], List[str], bool]:
        separators = list(self._separator_regex.finditer(body_text))
        candidates: List[ItemCandidate] = []
        structural_reasons: List[str] = []
        if not anchors:
            if self._footer_regex.search(body_text) is None:
                structural_reasons.append("footer_end_marker_missing")
            return candidates, structural_reasons, not structural_reasons

        for index, anchor in enumerate(anchors):
            candidate_reasons: List[str] = []
            if index + 1 < len(anchors):
                next_anchor_start = anchors[index + 1].start()
                boundaries = [
                    match.start()
                    for match in separators
                    if anchor.end() <= match.start() < next_anchor_start
                ]
                if boundaries:
                    end = boundaries[-1]
                else:
                    end = next_anchor_start
                    candidate_reasons.append(
                        f"item_{index + 1}:separator_before_next_anchor_missing"
                    )
            else:
                footer_match = self._footer_regex.search(body_text, anchor.end())
                if footer_match is None:
                    end = len(body_text)
                    candidate_reasons.append("final_item:footer_end_marker_missing")
                else:
                    end = footer_match.start()

            block = normalize_content(body_text[anchor.start() : end])
            missing_markers = [
                marker
                for marker in self.config["required_profile_markers"]
                if marker not in block
            ]
            if missing_markers:
                candidate_reasons.append(
                    f"item_{index + 1}:required_markers_missing:"
                    + ",".join(missing_markers)
                )
            candidate = ItemCandidate(
                candidate_index=index + 1,
                identifier=anchor.group("identifier"),
                source_container_id=body_container_id,
                body_text=block,
                parse_success=not candidate_reasons,
                parse_reasons=candidate_reasons,
            )
            normalized_identifier = normalize_block_identifier(candidate.identifier)
            candidate.identity_evidence = {
                "strategy": "COMPANY_ITEM_TYPE_NORMALIZED_IDENTIFIER",
                "source_container_id": body_container_id,
                "raw_identifier": candidate.identifier,
                "normalized_identifier": normalized_identifier,
            }
            if normalized_identifier:
                candidate.logical_item_id = logical_item_id(
                    self.config["source_company"],
                    self.config["item_type"],
                    candidate.identifier,
                )
            else:
                candidate.parse_success = False
                candidate.parse_reasons.append(
                    f"item_{index + 1}:identity_identifier_empty"
                )
            structural_reasons.extend(candidate.parse_reasons)
            candidates.append(candidate)
        return candidates, structural_reasons, not structural_reasons

    def _declared_count_evidence(self, mail: Dict[str, Any]) -> CardinalityEvidence:
        primary = self._cardinality_config["primary"]
        source_field = primary.get("source", "subject")
        source_value = str(mail.get(source_field, ""))
        if self._declared_count_regex is None:
            return CardinalityEvidence(
                authority=CardinalityAuthority.UNKNOWN.value,
                source=source_field,
                count=None,
                complete=False,
                is_primary=True,
                reasons=["declared_count_parser_not_configured"],
            )
        counts = []
        for match in self._declared_count_regex.finditer(source_value):
            try:
                counts.append(int(match.group("count")))
            except (TypeError, ValueError):
                pass
        if len(counts) != 1:
            return CardinalityEvidence(
                authority=CardinalityAuthority.DECLARED_COUNT.value,
                source=source_field,
                count=None,
                complete=False,
                is_primary=True,
                reasons=[f"declared_count_candidates:{len(counts)}"],
            )
        return CardinalityEvidence(
            authority=CardinalityAuthority.DECLARED_COUNT.value,
            source=source_field,
            count=counts[0],
            complete=True,
            is_primary=True,
        )

    def _cardinality_evidence(
        self,
        mail: Dict[str, Any],
        anchor_count: int,
        structural_complete: bool,
        structural_reasons: List[str],
    ) -> List[CardinalityEvidence]:
        structural = CardinalityEvidence(
            authority=CardinalityAuthority.STRUCTURAL_COMPLETE.value,
            source="INLINE_BODY",
            count=anchor_count,
            complete=structural_complete,
            is_primary=self._primary_authority
            == CardinalityAuthority.STRUCTURAL_COMPLETE.value,
            reasons=structural_reasons,
        )
        if self._primary_authority == CardinalityAuthority.STRUCTURAL_COMPLETE.value:
            return [structural]
        if self._primary_authority == CardinalityAuthority.DECLARED_COUNT.value:
            return [self._declared_count_evidence(mail), structural]
        return [
            CardinalityEvidence(
                authority=CardinalityAuthority.UNKNOWN.value,
                source="config",
                count=None,
                complete=False,
                is_primary=True,
                reasons=[f"authority_not_implemented:{self._primary_authority}"],
            )
        ]

    def _map_attachments(
        self,
        source: Source,
        candidates: List[ItemCandidate],
        attachments: List[Dict[str, Any]],
    ) -> List[Container]:
        reasons: List[str] = []
        containers: List[Container] = []
        identifiers: List[Optional[str]] = []
        digests: List[str] = []
        for index, attachment in enumerate(attachments):
            container_id = f"{source.source_id}:attachment:{index}"
            filename = attachment.get(self._filename_field) if isinstance(attachment, dict) else None
            digest = ""
            container_reasons: List[str] = []
            identifier: Optional[str] = None
            if not isinstance(attachment, dict):
                container_reasons.append(f"attachment_{index}:not_an_object")
            elif not isinstance(filename, str) or not filename:
                container_reasons.append(f"attachment_{index}:filename_missing")
            else:
                match = self._filename_identifier_regex.fullmatch(filename)
                if match is None:
                    container_reasons.append(f"attachment_{index}:identifier_unextractable")
                else:
                    identifier = normalize_block_identifier(
                        match.group(self._filename_identifier_group)
                    )
                    if not identifier:
                        container_reasons.append(f"attachment_{index}:identifier_empty")
                        identifier = None
                try:
                    digest = attachment_fingerprint(attachment)
                except ValueError as error:
                    container_reasons.append(
                        f"attachment_{index}:content_digest_unavailable:{error}"
                    )
            identifiers.append(identifier)
            digests.append(digest)
            reasons.extend(container_reasons)
            containers.append(
                Container(
                    container_id=container_id,
                    parent_container_id="",
                    kind=ContainerKind.ATTACHMENT_FILE.value,
                    locator=str(filename or ""),
                    content_fingerprint=digest,
                    enumeration_status=EnumerationStatus.COMPLETE.value,
                    completeness=True,
                    candidate_count=1,
                    reasons=container_reasons,
                )
            )

        if len(attachments) != len(candidates):
            reasons.append(
                f"artifact_count:{len(attachments)}:candidate_count:{len(candidates)}"
            )
        used_indices = set()
        for candidate in candidates:
            normalized_identifier = normalize_block_identifier(candidate.identifier)
            matching_indices = [
                index
                for index, identifier in enumerate(identifiers)
                if identifier and identifier == normalized_identifier
            ]
            if len(matching_indices) != 1:
                candidate.artifact_relation_resolved = False
                candidate.parse_reasons.append(
                    f"item_{candidate.candidate_index}:artifact_candidates:{len(matching_indices)}"
                )
                continue
            attachment_index = matching_indices[0]
            if attachment_index in used_indices or not digests[attachment_index]:
                candidate.artifact_relation_resolved = False
                candidate.parse_reasons.append(
                    f"item_{candidate.candidate_index}:artifact_unresolved:{attachment_index}"
                )
                continue
            used_indices.add(attachment_index)
            attachment = attachments[attachment_index]
            candidate.item_artifacts = [
                {
                    "role": "PRIMARY",
                    "artifact_kind": ContainerKind.ATTACHMENT_FILE.value,
                    "stable_locator": str(attachment.get(self._filename_field, "")),
                    "content_sha256": digests[attachment_index],
                    "version_relevant": True,
                    "container_id": containers[attachment_index].container_id,
                    "artifact": attachment,
                }
            ]
        if len(used_indices) != len(attachments):
            reasons.append(
                f"one_artifact_per_item:{len(used_indices)}:artifacts:{len(attachments)}"
            )
        source.artifact_relations_resolved = not reasons
        source.artifact_relation_reasons = reasons
        return containers

    def _build_canonical_body(self, item_block: str, delivery_count: int) -> str:
        template = self.config["canonical_body_classification_context"]
        context = template.format(delivery_count=delivery_count)
        return normalize_content(context + "\n\n" + item_block)

    @staticmethod
    def _audit_artifacts(relations: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        keys = (
            "role",
            "artifact_kind",
            "stable_locator",
            "content_sha256",
            "version_relevant",
            "container_id",
        )
        return [{key: relation[key] for key in keys} for relation in relations]

    def _parse(self, mail: Dict[str, Any]) -> ParseResult:
        source = self._new_source(mail)
        if not self.matches(mail):
            return ParseResult(
                "UNSUPPORTED", ["company_selector_mismatch"], [], source.to_dict(), []
            )

        body_text = self._normalize_newlines(str(mail.get("body_text", "")))
        anchors = list(self._anchor_regex.finditer(body_text))
        body_container_id = f"{source.source_id}:inline_body"
        candidates, structural_reasons, structural_complete = self._enumerate_blocks(
            body_text, anchors, body_container_id
        )
        body_container = Container(
            container_id=body_container_id,
            parent_container_id="",
            kind=ContainerKind.INLINE_BODY.value,
            locator="body_text",
            content_fingerprint=body_fingerprint(body_text),
            enumeration_status=(
                EnumerationStatus.COMPLETE.value
                if structural_complete
                else EnumerationStatus.INCOMPLETE.value
            ),
            completeness=structural_complete,
            candidate_count=len(anchors),
            reasons=structural_reasons,
            required=True,
        )

        attachments_value = mail.get("attachments", [])
        attachments = attachments_value if isinstance(attachments_value, list) else []
        attachment_containers = self._map_attachments(source, candidates, attachments)
        containers = [body_container] + attachment_containers
        source.container_references = [container.container_id for container in containers]
        source.cardinality_evidence = self._cardinality_evidence(
            mail, len(anchors), structural_complete, structural_reasons
        )
        gate = evaluate_completeness(source, containers, candidates)
        if gate.status != "PARSED":
            return ParseResult(
                gate.status,
                gate.reasons,
                [],
                source.to_dict(),
                [container.to_dict() for container in containers],
            )

        expected_count = gate.expected_count
        if expected_count is None:
            raise RuntimeError("PARSED gate returned no expected count")
        items: List[Dict[str, Any]] = []
        for candidate in candidates:
            canonical_body = self._build_canonical_body(
                candidate.body_text, expected_count
            )
            body_digest = body_fingerprint(canonical_body)
            artifact_digest = artifact_set_fingerprint(candidate.item_artifacts)
            relevant_artifact_digest = artifact_set_fingerprint(
                candidate.item_artifacts, version_relevant_only=True
            )
            version_digest = version_fingerprint(body_digest, relevant_artifact_digest)
            derived_id = derived_item_id(candidate.logical_item_id, version_digest)
            raw_artifacts = [relation["artifact"] for relation in candidate.item_artifacts]
            compatibility_attachment_digest = (
                candidate.item_artifacts[0]["content_sha256"]
                if len(candidate.item_artifacts) == 1
                else ""
            )
            items.append(
                {
                    "original_message_id": source.source_id,
                    "logical_item_id": candidate.logical_item_id,
                    "derived_item_id": derived_id,
                    "item_index": candidate.candidate_index,
                    "item_type": self.config["item_type"],
                    "body_text": canonical_body,
                    "body_fingerprint": body_digest,
                    "attachment_fingerprint": compatibility_attachment_digest,
                    "artifact_set_fingerprint": artifact_digest,
                    "version_relevant_artifact_set_fingerprint": relevant_artifact_digest,
                    "version_fingerprint": version_digest,
                    "content_fingerprint": version_digest,
                    "canonical_subject": canonical_subject(
                        self.config["canonical_subject_template"],
                        candidate.logical_item_id,
                        version_digest,
                    ),
                    "attachment": raw_artifacts[0] if len(raw_artifacts) == 1 else {},
                    "attachments": raw_artifacts,
                    "item_artifacts": self._audit_artifacts(candidate.item_artifacts),
                    "attachment_mapping": {
                        "status": "MAPPED",
                        "rule": self.config["attachment_mapping"]["strategy"],
                        "attachment_index": attachments.index(raw_artifacts[0])
                        if len(raw_artifacts) == 1
                        else -1,
                        "filename": str(raw_artifacts[0].get("filename", ""))
                        if len(raw_artifacts) == 1
                        else "",
                        "artifact_count": len(raw_artifacts),
                    },
                    "identity_evidence": candidate.identity_evidence,
                    "html_links": [],
                }
            )

        derived_ids = [item["derived_item_id"] for item in items]
        if len(set(derived_ids)) != len(derived_ids):
            gate.status = "HUMAN_REVIEW"
            gate.reasons.append("delivery_derived_identity_collision")
            source.completeness_result = gate
            return ParseResult(
                gate.status,
                gate.reasons,
                [],
                source.to_dict(),
                [container.to_dict() for container in containers],
            )
        return ParseResult(
            "PARSED",
            [],
            items,
            source.to_dict(),
            [container.to_dict() for container in containers],
        )

    def parse(self, mail: Dict[str, Any]) -> ParseResult:
        try:
            return self._parse(mail)
        except Exception as error:
            source_id = str(mail.get("message_id", "")) if isinstance(mail, dict) else ""
            reasons = [f"parser_failure:{type(error).__name__}:{error}"]
            source = {
                "source_id": source_id,
                "source_type": "EMAIL",
                "source_company": self.config.get("source_company", ""),
                "source_fingerprint": "",
                "delivery_semantics": self.config.get(
                    "delivery_semantics", DeliverySemantics.UNKNOWN.value
                ),
                "acquisition_status": "INCOMPLETE",
                "cardinality_evidence": [],
                "container_references": [],
                "configured_primary_authority": self._primary_authority,
                "configured_cross_check_authorities": [
                    row["authority"]
                    for row in self._cardinality_config.get("cross_checks", [])
                ],
                "artifact_relations_resolved": False,
                "artifact_relation_reasons": ["system_failure"],
                "completeness_result": {
                    "status": "SYSTEM_FAILURE",
                    "reasons": reasons,
                    "expected_count": None,
                    "candidate_count": 0,
                    "checks": {},
                },
            }
            return ParseResult("SYSTEM_FAILURE", reasons, [], source, [])

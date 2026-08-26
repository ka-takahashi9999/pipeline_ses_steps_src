#!/usr/bin/env python3
"""Config-driven ordered LINK_BUNDLE parser for the test-only 99-1 lab."""

import hashlib
import json
import re
from dataclasses import dataclass, field
from email.utils import parseaddr
from pathlib import Path
from typing import Any, Dict, List, Pattern, Sequence, Tuple

from identity import (
    artifact_set_fingerprint,
    body_fingerprint,
    derived_item_id,
    logical_item_id,
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
from link_bundle_manifest_contract import (
    MANIFEST_FIELD,
    MANIFEST_SCHEMA_VERSION,
    ordered_entry_digest,
)


ADAPTER_ID = "link_bundle"
ADAPTER_VERSION = "1.1.0"
ITEM_ROLES = {"RESOURCE_ITEM", "PROJECT_ITEM"}
KNOWN_ROLES = {
    "RESOURCE_HEADER",
    "PROJECT_HEADER",
    "RESOURCE_ITEM",
    "PROJECT_ITEM",
    "ACTION",
    "SHARED",
    "NON_ITEM",
}


@dataclass(frozen=True)
class ParseResult:
    status: str
    reasons: List[str]
    items: List[Dict[str, Any]]
    source: Dict[str, Any] = field(default_factory=dict)
    containers: List[Dict[str, Any]] = field(default_factory=list)
    link_enumeration: List[Dict[str, Any]] = field(default_factory=list)
    technical_projection_items: List[Dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class AcquisitionValidation:
    status: str
    core_status: str
    reasons: List[str]
    observed_entry_count: int
    observed_entry_digest: str
    manifest: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "core_status": self.core_status,
            "reasons": self.reasons,
            "observed_entry_count": self.observed_entry_count,
            "observed_entry_digest": self.observed_entry_digest,
            "manifest": self.manifest,
        }


@dataclass(frozen=True)
class ClassifiedLink:
    index: int
    role: str
    text: str
    href: str
    source: str
    section: str = ""
    locator: str = ""
    reasons: Tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "role": self.role,
            "text": self.text,
            "href": self.href,
            "source": self.source,
            "section": self.section,
            "locator": self.locator,
            "reasons": list(self.reasons),
        }


class LinkBundleAdapter:
    """Enumerate every saved link and atomically emit item-specific overlays."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        forbidden = {
            "expected_item_count",
            "expected_resource_count",
            "expected_project_count",
            "expected_link_count",
        }
        present = sorted(forbidden & set(config))
        if present:
            raise ValueError("fixed runtime cardinality is forbidden:" + ",".join(present))

        selectors = config["selectors"]
        self._subject_regex = re.compile(selectors["subject_regex"], re.IGNORECASE)
        grammar = config["link_grammar"]
        self._resource_item_regex = self._compile_item_regex(
            grammar["resource_item_href_regex"]
        )
        self._project_item_regex = self._compile_item_regex(
            grammar["project_item_href_regex"]
        )
        self._resource_header = grammar["resource_header"]
        self._project_header = grammar["project_header"]
        self._action_links = grammar.get("action_links", [])
        self._shared_links = grammar.get("shared_links", [])
        self._non_item_links = grammar.get("non_item_links", [])

    @staticmethod
    def _compile_item_regex(value: str) -> Pattern[str]:
        pattern = re.compile(value)
        if "locator" not in pattern.groupindex:
            raise ValueError("item href regex must contain named locator group")
        return pattern

    @classmethod
    def from_file(cls, path: Path) -> "LinkBundleAdapter":
        with path.open(encoding="utf-8") as file_object:
            return cls(json.load(file_object))

    def matches(self, mail: Dict[str, Any]) -> bool:
        sender = parseaddr(str(mail.get("from", "")))[1]
        sender_domain = sender.rsplit("@", 1)[-1].casefold() if "@" in sender else ""
        return sender_domain == self.config["selectors"]["sender_domain"].casefold() and bool(
            self._subject_regex.search(str(mail.get("subject", "")))
        )

    @staticmethod
    def _fingerprint(value: Any) -> str:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return "sha256:" + hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _exact_match(text: str, href: str, rules: Sequence[Dict[str, str]]) -> bool:
        return any(text == row.get("text") and href == row.get("href") for row in rules)

    @staticmethod
    def _valid_link_object(link: Any) -> Tuple[str, str, str, List[str]]:
        reasons: List[str] = []
        if not isinstance(link, dict):
            return "", "", "", ["link_not_object"]
        text = link.get("text")
        href = link.get("href")
        source = link.get("source", "")
        if not isinstance(text, str):
            reasons.append("link_text_not_string")
            text = ""
        if not isinstance(href, str) or not href:
            reasons.append("link_href_missing")
            href = ""
        if not isinstance(source, str):
            reasons.append("link_source_not_string")
            source = ""
        return text, href, source, reasons

    def _classify_link(self, index: int, link: Any) -> ClassifiedLink:
        text, href, source, reasons = self._valid_link_object(link)
        if reasons:
            return ClassifiedLink(index, "UNKNOWN", text, href, source, reasons=tuple(reasons))
        if text == self._resource_header["text"] and href == self._resource_header["href"]:
            return ClassifiedLink(index, "RESOURCE_HEADER", text, href, source)
        if text == self._project_header["text"] and href == self._project_header["href"]:
            return ClassifiedLink(index, "PROJECT_HEADER", text, href, source)
        if self._exact_match(text, href, self._action_links):
            return ClassifiedLink(index, "ACTION", text, href, source)
        if self._exact_match(text, href, self._shared_links):
            return ClassifiedLink(index, "SHARED", text, href, source)
        if self._exact_match(text, href, self._non_item_links):
            return ClassifiedLink(index, "NON_ITEM", text, href, source)

        resource_match = self._resource_item_regex.fullmatch(href)
        project_match = self._project_item_regex.fullmatch(href)
        if resource_match and not project_match:
            return ClassifiedLink(
                index,
                "RESOURCE_ITEM",
                text,
                href,
                source,
                section="resource",
                locator=resource_match.group("locator"),
            )
        if project_match and not resource_match:
            return ClassifiedLink(
                index,
                "PROJECT_ITEM",
                text,
                href,
                source,
                section="project",
                locator=project_match.group("locator"),
            )
        reason = "ambiguous_item_grammar" if resource_match and project_match else "unrecognized_link"
        return ClassifiedLink(index, "UNKNOWN", text, href, source, reasons=(reason,))

    @staticmethod
    def _validate_acquisition_manifest(
        mail: Dict[str, Any], links_value: Any
    ) -> AcquisitionValidation:
        links = links_value if isinstance(links_value, list) else []
        observed_digest = ordered_entry_digest(links)
        manifest_value = mail.get(MANIFEST_FIELD)
        if manifest_value is None:
            return AcquisitionValidation(
                status="UNVERIFIED",
                core_status="INCOMPLETE",
                reasons=["acquisition_manifest_missing"],
                observed_entry_count=len(links),
                observed_entry_digest=observed_digest,
            )
        if not isinstance(manifest_value, dict):
            return AcquisitionValidation(
                status="INCOMPLETE",
                core_status="INCOMPLETE",
                reasons=["acquisition_manifest_not_object"],
                observed_entry_count=len(links),
                observed_entry_digest=observed_digest,
            )

        manifest = dict(manifest_value)
        reasons: List[str] = []
        if not isinstance(links_value, list):
            reasons.append("acquisition_snapshot_not_list")
        source_id = mail.get("message_id")
        if manifest.get("manifest_schema_version") != MANIFEST_SCHEMA_VERSION:
            reasons.append("acquisition_manifest_schema_mismatch")
        if (
            not isinstance(source_id, str)
            or not source_id
            or manifest.get("source_id") != source_id
        ):
            reasons.append("acquisition_manifest_source_id_mismatch")
        if manifest.get("acquisition_status") != "COMPLETE":
            reasons.append("acquisition_manifest_status_incomplete")
        if manifest.get("extractor_status") != "COMPLETE":
            reasons.append("acquisition_manifest_extractor_incomplete")
        manifest_count = manifest.get("ordered_entry_count")
        if (
            isinstance(manifest_count, bool)
            or not isinstance(manifest_count, int)
            or manifest_count < 0
        ):
            reasons.append("acquisition_manifest_count_invalid")
        elif manifest_count != len(links):
            reasons.append(
                f"acquisition_manifest_count_mismatch:{manifest_count}:observed:{len(links)}"
            )
        manifest_digest = manifest.get("ordered_entry_digest")
        if not isinstance(manifest_digest, str) or not manifest_digest.startswith(
            "sha256:"
        ):
            reasons.append("acquisition_manifest_digest_invalid")
        elif manifest_digest != observed_digest:
            reasons.append("acquisition_manifest_digest_mismatch")
        manifest_reasons = manifest.get("reasons")
        if not isinstance(manifest_reasons, list) or any(
            not isinstance(reason, str) for reason in manifest_reasons
        ):
            reasons.append("acquisition_manifest_reasons_invalid")
        elif manifest_reasons:
            reasons.append("acquisition_manifest_has_reasons")

        return AcquisitionValidation(
            status="INCOMPLETE" if reasons else "VERIFIED_COMPLETE",
            core_status="INCOMPLETE" if reasons else "COMPLETE",
            reasons=reasons,
            observed_entry_count=len(links),
            observed_entry_digest=observed_digest,
            manifest=manifest,
        )

    def _new_source(
        self, mail: Dict[str, Any], acquisition: AcquisitionValidation
    ) -> Source:
        cardinality_config = self.config.get("cardinality", {})
        return Source(
            source_id=str(mail.get("message_id", "")),
            source_type="EMAIL",
            source_company=self.config["source_company"],
            source_fingerprint=self._fingerprint(mail),
            delivery_semantics=self.config.get(
                "delivery_semantics", DeliverySemantics.UNKNOWN.value
            ),
            acquisition_status=acquisition.core_status,
            cardinality_evidence=[],
            container_references=[],
            configured_primary_authority=cardinality_config.get(
                "primary", CardinalityAuthority.CONTAINER_ENUMERATION.value
            ),
            configured_cross_check_authorities=cardinality_config.get(
                "cross_checks", [CardinalityAuthority.STRUCTURAL_COMPLETE.value]
            ),
        )

    def _enumerate(
        self,
        source: Source,
        links: List[Any],
        bundle_id: str,
    ) -> Tuple[List[ClassifiedLink], List[ItemCandidate], List[str]]:
        classified = [self._classify_link(index, link) for index, link in enumerate(links)]
        reasons: List[str] = []
        resource_headers = [row.index for row in classified if row.role == "RESOURCE_HEADER"]
        project_headers = [row.index for row in classified if row.role == "PROJECT_HEADER"]
        if len(resource_headers) != 1:
            reasons.append(f"resource_header_count:{len(resource_headers)}:expected:1")
        if len(project_headers) != 1:
            reasons.append(f"project_header_count:{len(project_headers)}:expected:1")
        if len(resource_headers) == 1 and len(project_headers) == 1:
            if resource_headers[0] >= project_headers[0]:
                reasons.append("header_order_not_resource_then_project")

        current_section = ""
        candidates: List[ItemCandidate] = []
        seen_locators = set()
        for row in classified:
            if row.role == "RESOURCE_HEADER":
                current_section = "resource"
            elif row.role == "PROJECT_HEADER":
                current_section = "project"
            elif row.role in ITEM_ROLES:
                candidate_reasons: List[str] = []
                if row.section != current_section:
                    candidate_reasons.append(
                        f"link_{row.index}:section_mismatch:{row.section}:{current_section or 'none'}"
                    )
                title = normalize_content(row.text)
                if not title:
                    candidate_reasons.append(f"link_{row.index}:empty_item_title")
                stable_locator = f"/boost/{'talents' if row.section == 'resource' else 'projects'}/{row.locator}"
                if stable_locator in seen_locators:
                    candidate_reasons.append(f"link_{row.index}:duplicate_item_locator:{stable_locator}")
                seen_locators.add(stable_locator)
                item_container_id = f"{bundle_id}:web_page:{row.index}"
                logical_id = ""
                if row.locator:
                    logical_id = logical_item_id(
                        self.config["source_company"], row.section, stable_locator
                    )
                else:
                    candidate_reasons.append(f"link_{row.index}:malformed_locator")
                candidates.append(
                    ItemCandidate(
                        candidate_index=len(candidates) + 1,
                        identifier=row.locator,
                        source_container_id=item_container_id,
                        body_text=title,
                        parse_success=not candidate_reasons,
                        parse_reasons=candidate_reasons,
                        identity_evidence={
                            "strategy": "PROVISIONAL_DURABLE_SECTION_STABLE_LOCATOR",
                            "section_type": row.section,
                            "stable_locator": stable_locator,
                            "locator_token": row.locator,
                        },
                        logical_item_id=logical_id,
                    )
                )
                reasons.extend(candidate_reasons)
            if row.role == "UNKNOWN":
                row_reasons = row.reasons or ("unknown_link",)
                reasons.extend(f"link_{row.index}:{reason}" for reason in row_reasons)
        return classified, candidates, reasons

    def _containers(
        self,
        source: Source,
        links: List[Any],
        classified: List[ClassifiedLink],
        candidates: List[ItemCandidate],
        reasons: List[str],
    ) -> List[Container]:
        bundle_id = f"{source.source_id}:link_bundle"
        role_complete = not reasons and all(row.role in KNOWN_ROLES for row in classified)
        resource_count = sum(row.role == "RESOURCE_ITEM" for row in classified)
        project_count = sum(row.role == "PROJECT_ITEM" for row in classified)
        containers = [
            Container(
                container_id=bundle_id,
                parent_container_id="",
                kind=ContainerKind.LINK_BUNDLE.value,
                locator="html_links",
                content_fingerprint=self._fingerprint(links),
                enumeration_status=(
                    EnumerationStatus.COMPLETE.value
                    if role_complete
                    else EnumerationStatus.INCOMPLETE.value
                ),
                completeness=role_complete,
                candidate_count=len(candidates),
                child_container_refs=[
                    f"{bundle_id}:resource_list",
                    f"{bundle_id}:project_list",
                ],
                reasons=reasons,
                required=True,
            ),
            Container(
                container_id=f"{bundle_id}:resource_list",
                parent_container_id=bundle_id,
                kind=ContainerKind.WEB_LIST.value,
                locator=self._resource_header["href"],
                content_fingerprint=self._fingerprint(
                    [row.to_dict() for row in classified if row.section == "resource"]
                ),
                enumeration_status=(
                    EnumerationStatus.COMPLETE.value
                    if role_complete
                    else EnumerationStatus.INCOMPLETE.value
                ),
                completeness=role_complete,
                candidate_count=resource_count,
                required=True,
            ),
            Container(
                container_id=f"{bundle_id}:project_list",
                parent_container_id=bundle_id,
                kind=ContainerKind.WEB_LIST.value,
                locator=self._project_header["href"],
                content_fingerprint=self._fingerprint(
                    [row.to_dict() for row in classified if row.section == "project"]
                ),
                enumeration_status=(
                    EnumerationStatus.COMPLETE.value
                    if role_complete
                    else EnumerationStatus.INCOMPLETE.value
                ),
                completeness=role_complete,
                candidate_count=project_count,
                required=True,
            ),
        ]
        candidate_by_index = {candidate.candidate_index: candidate for candidate in candidates}
        for item_index, row in enumerate(
            (entry for entry in classified if entry.role in ITEM_ROLES), start=1
        ):
            candidate = candidate_by_index[item_index]
            containers.append(
                Container(
                    container_id=candidate.source_container_id,
                    parent_container_id=(
                        f"{bundle_id}:{row.section}_list"
                    ),
                    kind=ContainerKind.WEB_PAGE.value,
                    locator=row.href,
                    content_fingerprint=self._fingerprint(
                        {
                            "section_type": row.section,
                            "title": normalize_content(row.text),
                            "locator": candidate.identity_evidence["stable_locator"],
                        }
                    ),
                    enumeration_status=EnumerationStatus.COMPLETE.value,
                    completeness=candidate.parse_success,
                    candidate_count=1,
                    reasons=candidate.parse_reasons,
                )
            )
        return containers

    def _source_dict(
        self,
        source: Source,
        acquisition: AcquisitionValidation,
        classified: List[ClassifiedLink],
        candidates: List[ItemCandidate],
        links: List[Any],
        containers: List[Container],
    ) -> Dict[str, Any]:
        result = source.to_dict()
        result["core_acquisition_status"] = source.acquisition_status
        result["acquisition_status"] = acquisition.status
        result["source_acquisition_status"] = acquisition.status
        result["acquisition_manifest_validation"] = acquisition.to_dict()
        result["link_role_counts"] = {
            role: sum(row.role == role for row in classified)
            for role in sorted(KNOWN_ROLES | {"UNKNOWN"})
        }
        result["section_counts"] = {
            "resource": sum(row.role == "RESOURCE_ITEM" for row in classified),
            "project": sum(row.role == "PROJECT_ITEM" for row in classified),
        }
        result["links_enumerated"] = len(classified)
        result["items_enumerated"] = len(candidates)
        required_containers = [container for container in containers if container.required]
        container_complete = bool(required_containers) and all(
            container.enumeration_status == EnumerationStatus.COMPLETE.value
            and container.completeness
            for container in required_containers
        )
        classified_count = sum(row.role in KNOWN_ROLES for row in classified)
        result["container_enumeration_status"] = (
            "COMPLETE" if container_complete else "INCOMPLETE"
        )
        result["role_classification_status"] = (
            "PASS" if classified_count == len(classified) else "FAIL"
        )
        result["role_classification_count"] = classified_count
        result["role_classification_total"] = len(classified)
        result["source_atomic_status"] = source.completeness_result.status
        result["auto_union_eligible"] = source.completeness_result.status == "PARSED"
        result["observed_candidate_count"] = len(candidates)
        result["source_artifacts"] = [
            {
                "role": "SOURCE_EVIDENCE",
                "artifact_kind": ContainerKind.LINK_BUNDLE.value,
                "stable_locator": "html_links",
                "content_sha256": self._fingerprint(links),
                "version_relevant": False,
                "version_scope": "MAIL_SNAPSHOT_LIST_ITEM",
            }
        ]
        return result

    def _build_items(
        self,
        source: Source,
        classified: List[ClassifiedLink],
        candidates: List[ItemCandidate],
    ) -> List[Dict[str, Any]]:
        item_rows = [row for row in classified if row.role in ITEM_ROLES]
        items: List[Dict[str, Any]] = []
        for candidate, row in zip(candidates, item_rows):
            section_config = self.config["section_context"][row.section]
            canonical_body = normalize_content(
                section_config["body_context"] + "\n\n" + candidate.body_text
            )
            evidence_payload = {
                "section_type": row.section,
                "normalized_title": candidate.body_text,
                "stable_locator": candidate.identity_evidence["stable_locator"],
            }
            evidence_digest = self._fingerprint(evidence_payload)
            candidate.item_artifacts = [
                {
                    "role": "PRIMARY",
                    "artifact_kind": ContainerKind.WEB_PAGE.value,
                    "stable_locator": candidate.identity_evidence["stable_locator"],
                    "content_sha256": evidence_digest,
                    "version_relevant": True,
                    "container_id": candidate.source_container_id,
                }
            ]
            body_digest = body_fingerprint(canonical_body)
            artifact_digest = artifact_set_fingerprint(candidate.item_artifacts)
            relevant_digest = artifact_set_fingerprint(
                candidate.item_artifacts, version_relevant_only=True
            )
            version_digest = version_fingerprint(body_digest, relevant_digest)
            derived_id = derived_item_id(candidate.logical_item_id, version_digest)
            logical_short = candidate.logical_item_id.removeprefix("li_")[:10]
            version_short = version_digest.removeprefix("sha256:")[:12]
            subject = section_config["subject_template"].format(
                title=candidate.body_text,
                logical_short=logical_short,
                version_short=version_short,
            )
            items.append(
                {
                    "original_message_id": source.source_id,
                    "logical_item_id": candidate.logical_item_id,
                    "derived_item_id": derived_id,
                    "item_index": candidate.candidate_index,
                    "section_type": row.section,
                    "body_text": canonical_body,
                    "body_fingerprint": body_digest,
                    "artifact_set_fingerprint": artifact_digest,
                    "version_relevant_artifact_set_fingerprint": relevant_digest,
                    "version_fingerprint": version_digest,
                    "content_fingerprint": version_digest,
                    "canonical_subject": subject,
                    "attachments": [],
                    "item_artifacts": [
                        {
                            key: relation[key]
                            for key in (
                                "role",
                                "artifact_kind",
                                "stable_locator",
                                "content_sha256",
                                "version_relevant",
                                "container_id",
                            )
                        }
                        for relation in candidate.item_artifacts
                    ],
                    "identity_evidence": candidate.identity_evidence,
                    "identity_durability": "PROVISIONAL_DURABLE",
                    "version_scope": "MAIL_SNAPSHOT_LIST_ITEM",
                    "classification_context_evidence": section_config[
                        "context_evidence"
                    ],
                    "html_links": [
                        {"text": row.text, "href": row.href, "source": row.source}
                    ],
                }
            )
        return items

    def _parse(self, mail: Dict[str, Any]) -> ParseResult:
        links_value = mail.get("html_links")
        acquisition = self._validate_acquisition_manifest(mail, links_value)
        source = self._new_source(mail, acquisition)
        if not self.matches(mail):
            return ParseResult(
                "UNSUPPORTED", ["company_selector_mismatch"], [], source.to_dict(), []
            )
        links = links_value if isinstance(links_value, list) else []
        bundle_id = f"{source.source_id}:link_bundle"
        classified, candidates, structural_reasons = self._enumerate(
            source, links, bundle_id
        )
        containers = self._containers(
            source, links, classified, candidates, structural_reasons
        )
        source.container_references = [container.container_id for container in containers]
        structural_complete = not structural_reasons
        structural_item_count = sum(row.role in ITEM_ROLES for row in classified)
        source.cardinality_evidence = [
            CardinalityEvidence(
                authority=CardinalityAuthority.CONTAINER_ENUMERATION.value,
                source="LINK_BUNDLE",
                count=len(candidates),
                complete=structural_complete,
                is_primary=True,
                reasons=structural_reasons,
            ),
            CardinalityEvidence(
                authority=CardinalityAuthority.STRUCTURAL_COMPLETE.value,
                source="RESOURCE_WEB_LIST+PROJECT_WEB_LIST",
                count=structural_item_count,
                complete=structural_complete,
                reasons=structural_reasons,
            ),
        ]
        gate = evaluate_completeness(source, containers, candidates)
        source_dict = self._source_dict(
            source, acquisition, classified, candidates, links, containers
        )
        technical_items: List[Dict[str, Any]] = []
        if structural_complete and all(candidate.parse_success for candidate in candidates):
            technical_items = self._build_items(source, classified, candidates)
            if len({item["logical_item_id"] for item in technical_items}) != len(
                technical_items
            ):
                technical_items = []
        if gate.status != "PARSED":
            return ParseResult(
                gate.status,
                gate.reasons,
                [],
                source_dict,
                [container.to_dict() for container in containers],
                [row.to_dict() for row in classified],
                technical_items,
            )
        items = technical_items
        if len({item["logical_item_id"] for item in items}) != len(items):
            gate.status = "HUMAN_REVIEW"
            gate.reasons.append("delivery_logical_identity_collision")
            source.completeness_result = gate
            return ParseResult(
                gate.status,
                gate.reasons,
                [],
                self._source_dict(
                    source, acquisition, classified, candidates, links, containers
                ),
                [container.to_dict() for container in containers],
                [row.to_dict() for row in classified],
            )
        return ParseResult(
            "PARSED",
            [],
            items,
            source_dict,
            [container.to_dict() for container in containers],
            [row.to_dict() for row in classified],
            technical_items,
        )

    def parse(self, mail: Dict[str, Any]) -> ParseResult:
        try:
            return self._parse(mail)
        except Exception as error:
            source_id = str(mail.get("message_id", "")) if isinstance(mail, dict) else ""
            reasons = [f"parser_failure:{type(error).__name__}:{error}"]
            return ParseResult(
                "SYSTEM_FAILURE",
                reasons,
                [],
                {
                    "source_id": source_id,
                    "source_type": "EMAIL",
                    "source_company": self.config.get("source_company", ""),
                    "acquisition_status": "INCOMPLETE",
                    "source_acquisition_status": "INCOMPLETE",
                    "core_acquisition_status": "INCOMPLETE",
                    "container_enumeration_status": "INCOMPLETE",
                    "role_classification_status": "FAIL",
                    "source_atomic_status": "SYSTEM_FAILURE",
                    "auto_union_eligible": False,
                    "completeness_result": {
                        "status": "SYSTEM_FAILURE",
                        "reasons": reasons,
                        "expected_count": None,
                        "candidate_count": 0,
                        "checks": {},
                    },
                },
                [],
                [],
            )

#!/usr/bin/env python3
"""Config-driven ATTACHMENT_LIST parser for the test-only 99-1 lab."""

import base64
import binascii
import copy
import hashlib
import io
import json
import re
import zipfile
from dataclasses import dataclass, field
from email.utils import parseaddr
from pathlib import Path
from typing import Any, Dict, List, Pattern, Sequence, Tuple

from attachment_manifest_contract import (
    MANIFEST_FIELD,
    MANIFEST_SCHEMA_VERSION,
    canonical_ordered_entries,
    ordered_attachment_digest,
    source_payload_digest,
    validate_authoritative_attachment_entries,
)
from identity import (
    artifact_set_fingerprint,
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


ADAPTER_ID = "attachment_list"
ADAPTER_VERSION = "1.0.1"
ATTACHMENT_ROLES = {
    "ITEM_ATTACHMENT",
    "SHARED",
    "SUPPORTING",
    "ARCHIVE",
    "INLINE_ASSET",
    "UNKNOWN",
}


@dataclass(frozen=True)
class ParseResult:
    status: str
    reasons: List[str]
    items: List[Dict[str, Any]]
    source: Dict[str, Any] = field(default_factory=dict)
    containers: List[Dict[str, Any]] = field(default_factory=list)
    attachment_enumeration: List[Dict[str, Any]] = field(default_factory=list)
    technical_projection_items: List[Dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class AcquisitionValidation:
    status: str
    core_status: str
    manifest_contract_status: str
    attachment_integrity_status: str
    reasons: List[str]
    observed_ordered_count: int
    observed_ordered_digest: str
    manifest: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "core_status": self.core_status,
            "manifest_contract_status": self.manifest_contract_status,
            "attachment_integrity_status": self.attachment_integrity_status,
            "reasons": self.reasons,
            "observed_ordered_count": self.observed_ordered_count,
            "observed_ordered_digest": self.observed_ordered_digest,
            "manifest": self.manifest,
        }


@dataclass(frozen=True)
class ClassifiedAttachment:
    position: int
    source_entry_id: str
    filename: str
    mime_type: str
    declared_size: Any
    decoded_size: int
    content_digest: str
    disposition: str
    content_id: str
    role: str
    identifier: str = ""
    station: str = ""
    xlsx_valid: bool = False
    reasons: Tuple[str, ...] = ()

    def manifest_entry(self) -> Dict[str, Any]:
        return {
            "position": self.position,
            "source_entry_id": self.source_entry_id,
            "filename": self.filename,
            "mime_type": self.mime_type,
            "declared_size": self.declared_size,
            "content_digest": self.content_digest,
            "disposition": self.disposition,
            "content_id": self.content_id,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            **self.manifest_entry(),
            "decoded_size": self.decoded_size,
            "role": self.role,
            "identifier": self.identifier,
            "station": self.station,
            "xlsx_valid": self.xlsx_valid,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class Profile:
    index: int
    identifier: str
    normalized_identifier: str
    station: str
    normalized_station: str
    body_text: str
    complete: bool
    reasons: Tuple[str, ...] = ()


class AttachmentListAdapter:
    """Enumerate a saved attachment list and fail closed at source scope."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        forbidden = {
            "expected_item_count",
            "expected_attachment_count",
            "expected_profile_count",
        }
        present = sorted(forbidden & set(config))
        if present:
            raise ValueError("fixed runtime cardinality is forbidden:" + ",".join(present))
        selectors = config["selectors"]
        self._subject_regex = re.compile(selectors["subject_regex"], re.IGNORECASE)
        inline = config["inline_profile"]
        self._declared_regex = re.compile(inline["declared_count_regex"])
        self._item_start_regex = re.compile(inline["item_start_regex"])
        self._station_regex = re.compile(inline["station_regex"])
        self._footer_regex = re.compile(inline["footer_regex"])
        self._required_profile_markers = inline["required_profile_markers"]
        roles = config["attachment_roles"]
        self._item_filename_regex = self._compile_identifier_regex(
            roles["item_filename_regex"]
        )
        self._shared_filename_regex = re.compile(roles["shared_filename_regex"])
        self._supporting_filename_regex = re.compile(
            roles["supporting_filename_regex"]
        )
        self._inline_mime_regex = re.compile(roles["inline_mime_regex"])
        self._xlsx_mime_types = set(roles["xlsx_mime_types"])

    @staticmethod
    def _compile_identifier_regex(value: str) -> Pattern[str]:
        pattern = re.compile(value, re.IGNORECASE)
        if "identifier" not in pattern.groupindex:
            raise ValueError("item filename regex requires an identifier group")
        return pattern

    @classmethod
    def from_file(cls, path: Path) -> "AttachmentListAdapter":
        with path.open(encoding="utf-8") as file_object:
            return cls(json.load(file_object))

    def matches(self, mail: Dict[str, Any]) -> bool:
        sender = parseaddr(str(mail.get("from", "")))[1]
        domain = sender.rsplit("@", 1)[-1].casefold() if "@" in sender else ""
        return domain == self.config["selectors"]["sender_domain"].casefold() and bool(
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
    def _decode_attachment(attachment: Dict[str, Any]) -> Tuple[bytes, List[str]]:
        reasons: List[str] = []
        encoded = attachment.get("data")
        if not isinstance(encoded, str) or not encoded:
            return b"", ["attachment_data_missing"]
        try:
            encoded_bytes = encoded.encode("ascii")
            padding = b"=" * (-len(encoded_bytes) % 4)
            payload = base64.b64decode(
                encoded_bytes + padding, altchars=b"-_", validate=True
            )
        except (UnicodeEncodeError, binascii.Error, ValueError):
            return b"", ["attachment_base64url_invalid"]
        declared_size = attachment.get("size")
        if (
            isinstance(declared_size, bool)
            or not isinstance(declared_size, int)
            or declared_size < 0
        ):
            reasons.append("attachment_declared_size_invalid")
        elif len(payload) != declared_size:
            reasons.append(
                f"attachment_size_mismatch:{len(payload)}:declared:{declared_size}"
            )
        return payload, reasons

    @staticmethod
    def _integrity_reasons(attachment: ClassifiedAttachment) -> List[str]:
        integrity_prefixes = (
            "attachment_not_object",
            "attachment_source_entry_id_missing",
            "attachment_filename_missing",
            "attachment_mime_missing",
            "attachment_data_missing",
            "attachment_base64url_invalid",
            "attachment_declared_size_invalid",
            "attachment_size_mismatch",
        )
        return [
            "attachment:" + str(attachment.position) + ":" + reason
            for reason in attachment.reasons
            if reason.startswith(integrity_prefixes)
        ]

    @staticmethod
    def _valid_xlsx(payload: bytes) -> bool:
        try:
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                names = set(archive.namelist())
                if "[Content_Types].xml" not in names or "xl/workbook.xml" not in names:
                    return False
                archive.read("[Content_Types].xml")
                archive.read("xl/workbook.xml")
                return archive.testzip() is None
        except (OSError, KeyError, zipfile.BadZipFile):
            return False

    def _classify_attachment(
        self, position: int, attachment: Any
    ) -> ClassifiedAttachment:
        if not isinstance(attachment, dict):
            return ClassifiedAttachment(
                position,
                "part-attachment-" + str(position),
                "",
                "",
                None,
                0,
                "",
                "",
                "",
                "UNKNOWN",
                reasons=("attachment_not_object",),
            )
        filename = attachment.get("filename", "")
        mime_type = attachment.get("mime_type", "")
        disposition = attachment.get("disposition", "")
        content_id = attachment.get("content_id", "")
        reasons: List[str] = []
        source_entry_id = attachment.get(
            "source_entry_id", "part-attachment-" + str(position)
        )
        if not isinstance(source_entry_id, str) or not source_entry_id.strip():
            reasons.append("attachment_source_entry_id_missing")
            source_entry_id = "" if not isinstance(source_entry_id, str) else source_entry_id
        if not isinstance(filename, str) or not filename.strip():
            reasons.append("attachment_filename_missing")
            filename = ""
        if not isinstance(mime_type, str) or not mime_type.strip():
            reasons.append("attachment_mime_missing")
            mime_type = ""
        payload, decode_reasons = self._decode_attachment(attachment)
        reasons.extend(decode_reasons)
        digest = "sha256:" + hashlib.sha256(payload).hexdigest() if payload else ""
        xlsx_valid = self._valid_xlsx(payload) if payload else False
        filename_match = self._item_filename_regex.fullmatch(filename)
        role = "UNKNOWN"
        identifier = ""
        station = ""
        lower_filename = filename.casefold()
        if lower_filename.endswith(".zip") or mime_type.casefold() in {
            "application/zip",
            "application/x-zip-compressed",
        }:
            role = "ARCHIVE"
        elif disposition.casefold() == "inline" or (
            content_id and self._inline_mime_regex.fullmatch(mime_type)
        ):
            role = "INLINE_ASSET"
        elif self._shared_filename_regex.fullmatch(filename):
            role = "SHARED"
        elif self._supporting_filename_regex.fullmatch(filename):
            role = "SUPPORTING"
        elif filename_match and mime_type in self._xlsx_mime_types and xlsx_valid:
            role = "ITEM_ATTACHMENT"
            identifier = filename_match.group("identifier")
            if "station" in filename_match.re.groupindex:
                station = filename_match.group("station") or ""
        if role == "ITEM_ATTACHMENT" and decode_reasons:
            role = "UNKNOWN"
        if filename_match and not xlsx_valid and role == "UNKNOWN":
            reasons.append("item_attachment_xlsx_invalid")
        if role == "UNKNOWN" and not reasons:
            reasons.append("attachment_role_unknown")
        return ClassifiedAttachment(
            position=position,
            source_entry_id=source_entry_id,
            filename=filename,
            mime_type=mime_type,
            declared_size=attachment.get("size"),
            decoded_size=len(payload),
            content_digest=digest,
            disposition=str(disposition) if isinstance(disposition, str) else "",
            content_id=str(content_id) if isinstance(content_id, str) else "",
            role=role,
            identifier=identifier,
            station=station,
            xlsx_valid=xlsx_valid,
            reasons=tuple(reasons),
        )

    def _enumerate_attachments(
        self, mail: Dict[str, Any]
    ) -> Tuple[List[ClassifiedAttachment], str, List[str]]:
        attachments = mail.get("attachments")
        if not isinstance(attachments, list):
            return [], EnumerationStatus.INCOMPLETE.value, ["attachments_not_list"]
        rows = [
            self._classify_attachment(position, attachment)
            for position, attachment in enumerate(attachments)
        ]
        return rows, EnumerationStatus.COMPLETE.value, []

    def _parse_profiles(
        self, body_text: str
    ) -> Tuple[List[Profile], Any, bool, List[str]]:
        reasons: List[str] = []
        declared_matches = list(self._declared_regex.finditer(body_text))
        declared_count = None
        if len(declared_matches) == 1:
            declared_count = int(declared_matches[0].group("count"))
        else:
            reasons.append(f"declared_count_candidates:{len(declared_matches)}")
        footer = self._footer_regex.search(body_text)
        if not footer:
            reasons.append("profile_footer_missing")
        starts = list(self._item_start_regex.finditer(body_text))
        profiles: List[Profile] = []
        for index, match in enumerate(starts):
            end = starts[index + 1].start() if index + 1 < len(starts) else (
                footer.start() if footer and footer.start() > match.start() else len(body_text)
            )
            block = normalize_content(body_text[match.start():end])
            profile_reasons = [
                "profile_marker_missing:" + marker
                for marker in self._required_profile_markers
                if marker not in block
            ]
            station_match = self._station_regex.search(block)
            station = station_match.group("station").strip() if station_match else ""
            if not station:
                profile_reasons.append("profile_station_missing")
            identifier = match.group("identifier").strip()
            normalized_identifier = normalize_block_identifier(identifier)
            if not normalized_identifier:
                profile_reasons.append("profile_identifier_empty")
            profiles.append(
                Profile(
                    index=index,
                    identifier=identifier,
                    normalized_identifier=normalized_identifier,
                    station=station,
                    normalized_station=self._normalize_station(station),
                    body_text=block,
                    complete=not profile_reasons,
                    reasons=tuple(profile_reasons),
                )
            )
        structural_complete = bool(footer) and all(profile.complete for profile in profiles)
        if not structural_complete:
            reasons.extend(
                reason for profile in profiles for reason in profile.reasons
            )
        return profiles, declared_count, structural_complete, reasons

    @staticmethod
    def _normalize_station(value: str) -> str:
        normalized = normalize_block_identifier(value)
        if normalized.endswith("駅"):
            normalized = normalized[:-1]
        for prefix in ("東急東横線", "小田急線", "都営地下鉄新宿線", "東海道線", "青梅線", "京王線", "相鉄線"):
            normalized_prefix = normalize_block_identifier(prefix)
            if normalized.startswith(normalized_prefix):
                normalized = normalized[len(normalized_prefix):]
        return normalized

    @staticmethod
    def _observed_manifest_entries(
        attachments: Sequence[ClassifiedAttachment],
    ) -> List[Dict[str, Any]]:
        return [attachment.manifest_entry() for attachment in attachments]

    def _validate_manifest(
        self, mail: Dict[str, Any], attachments: Sequence[ClassifiedAttachment]
    ) -> AcquisitionValidation:
        observed_entries = self._observed_manifest_entries(attachments)
        observed_digest = ordered_attachment_digest(observed_entries)
        manifest = mail.get(MANIFEST_FIELD)
        observed_integrity_reasons = [
            reason
            for attachment in attachments
            for reason in self._integrity_reasons(attachment)
        ]
        if not isinstance(manifest, dict):
            return AcquisitionValidation(
                "UNVERIFIED",
                "INCOMPLETE",
                "UNVERIFIED",
                "PASS" if not observed_integrity_reasons else "FAIL",
                ["source_owned_attachment_manifest_missing"]
                + observed_integrity_reasons,
                len(observed_entries),
                observed_digest,
                {},
            )
        reasons: List[str] = []
        if manifest.get("manifest_schema_version") != MANIFEST_SCHEMA_VERSION:
            reasons.append("manifest_schema_mismatch")
        if manifest.get("source_id") != mail.get("message_id"):
            reasons.append("manifest_source_id_mismatch")
        if manifest.get("acquisition_status") != "COMPLETE":
            reasons.append("manifest_acquisition_status_incomplete")
        if manifest.get("extractor_status") != "COMPLETE":
            reasons.append("manifest_extractor_status_incomplete")
        if manifest.get("reasons") != []:
            reasons.append("manifest_reasons_not_empty")
        authoritative = manifest.get("authoritative_attachment_entries")
        if not isinstance(authoritative, list):
            authoritative = []
            reasons.append("manifest_authoritative_entries_missing")
        entry_contract_reasons = validate_authoritative_attachment_entries(
            manifest.get("authoritative_attachment_entries")
        )
        reasons.extend(entry_contract_reasons)
        canonical_authoritative = canonical_ordered_entries(authoritative)
        if [entry.get("position") if isinstance(entry, dict) else None for entry in authoritative] != list(
            range(len(authoritative))
        ):
            reasons.append("manifest_positions_invalid")
        expected_count = manifest.get("expected_ordered_count")
        if (
            isinstance(expected_count, bool)
            or not isinstance(expected_count, int)
            or expected_count < 0
            or expected_count != len(canonical_authoritative)
        ):
            reasons.append("manifest_count_mismatch")
        authoritative_digest = ordered_attachment_digest(canonical_authoritative)
        if manifest.get("expected_ordered_digest") != authoritative_digest:
            reasons.append("manifest_digest_mismatch")
        if manifest.get("source_payload_digest") != source_payload_digest(
            mail, canonical_authoritative
        ):
            reasons.append("source_payload_digest_mismatch")
        if expected_count != len(observed_entries):
            reasons.append("observed_attachment_count_mismatch")
        if manifest.get("expected_ordered_digest") != observed_digest:
            reasons.append("observed_attachment_ordered_digest_mismatch")
        if canonical_ordered_entries(observed_entries) != canonical_authoritative:
            reasons.append("observed_attachment_entries_mismatch")
        reasons.extend(observed_integrity_reasons)
        manifest_contract_reasons = [
            reason
            for reason in reasons
            if reason.startswith("manifest_")
            or reason.startswith("source_payload_digest_")
        ]
        observed_matches_authority = (
            canonical_ordered_entries(observed_entries) == canonical_authoritative
        )
        return AcquisitionValidation(
            "VERIFIED_COMPLETE" if not reasons else "INCOMPLETE",
            "COMPLETE" if not reasons else "INCOMPLETE",
            "PASS" if not manifest_contract_reasons else "FAIL",
            "PASS"
            if not observed_integrity_reasons and observed_matches_authority
            else "FAIL",
            list(dict.fromkeys(reasons)),
            len(observed_entries),
            observed_digest,
            copy.deepcopy(manifest),
        )

    def _map(
        self,
        profiles: Sequence[Profile],
        attachments: Sequence[ClassifiedAttachment],
    ) -> Tuple[Dict[int, ClassifiedAttachment], bool, List[str], int]:
        reasons: List[str] = []
        item_attachments = [
            attachment
            for attachment in attachments
            if attachment.role == "ITEM_ATTACHMENT"
        ]
        by_key: Dict[str, List[ClassifiedAttachment]] = {}
        for attachment in item_attachments:
            key = normalize_block_identifier(attachment.identifier)
            by_key.setdefault(key, []).append(attachment)
        duplicate_keys = sorted(key for key, values in by_key.items() if len(values) > 1)
        if duplicate_keys:
            reasons.append("duplicate_attachment_identity:" + ",".join(duplicate_keys))
        profile_keys = [profile.normalized_identifier for profile in profiles]
        if len(profile_keys) != len(set(profile_keys)):
            reasons.append("duplicate_profile_identity")
        mapping: Dict[int, ClassifiedAttachment] = {}
        mapped_positions = set()
        station_audit_matches = 0
        for profile in profiles:
            candidates = by_key.get(profile.normalized_identifier, [])
            if len(candidates) != 1:
                reasons.append(
                    f"mapping_candidate_count:{profile.normalized_identifier}:{len(candidates)}"
                )
                continue
            attachment = candidates[0]
            mapping[profile.index] = attachment
            mapped_positions.add(attachment.position)
            if attachment.station and self._normalize_station(attachment.station) == profile.normalized_station:
                station_audit_matches += 1
        unused = [
            attachment.position
            for attachment in item_attachments
            if attachment.position not in mapped_positions
        ]
        if unused:
            reasons.append("unused_item_attachment:" + ",".join(map(str, unused)))
        unsafe_roles = [
            attachment.position
            for attachment in attachments
            if attachment.role in {"UNKNOWN", "ARCHIVE"}
        ]
        if unsafe_roles:
            reasons.append("unsafe_attachment_role:" + ",".join(map(str, unsafe_roles)))
        return mapping, not reasons, reasons, station_audit_matches

    def _materialize_item(
        self,
        mail: Dict[str, Any],
        profile: Profile,
        attachment: ClassifiedAttachment,
        delivery_count: int,
    ) -> Dict[str, Any]:
        relation = {
            "role": "PRIMARY",
            "artifact_kind": "ATTACHMENT_FILE",
            "stable_locator": "attachment-key:" + profile.normalized_identifier,
            "content_sha256": attachment.content_digest,
            "version_relevant": True,
        }
        body_text = normalize_content(
            self.config["canonical_body_classification_context"].format(
                delivery_count=delivery_count
            )
            + "\n\n"
            + profile.body_text
        )
        body_digest = body_fingerprint(body_text)
        artifacts_digest = artifact_set_fingerprint([relation])
        version_digest = version_fingerprint(body_digest, artifacts_digest)
        logical_id = logical_item_id(
            self.config["source_company"], "resource", profile.identifier
        )
        return {
            "item_index": profile.index,
            "identifier": profile.identifier,
            "normalized_identifier": profile.normalized_identifier,
            "station": profile.station,
            "body_text": body_text,
            "attachments": [copy.deepcopy(mail["attachments"][attachment.position])],
            "html_links": [],
            "item_artifacts": [relation],
            "logical_item_id": logical_id,
            "body_fingerprint": body_digest,
            "artifact_set_fingerprint": artifacts_digest,
            "version_relevant_artifact_set_fingerprint": artifacts_digest,
            "version_fingerprint": version_digest,
            "content_fingerprint": version_digest,
            "derived_item_id": derived_item_id(logical_id, version_digest),
            "canonical_subject": canonical_subject(
                self.config["canonical_subject_template"], logical_id, version_digest
            ),
            "identity_evidence": {
                "strategy": "INLINE_PROFILE_EXACT_KEY",
                "normalized_identifier": profile.normalized_identifier,
            },
            "identity_durability": "PROVISIONAL_DURABLE",
            "version_scope": "PROFILE_PLUS_MAPPED_XLSX",
            "attachment_mapping": {
                "strategy": "ONE_ARTIFACT_PER_ITEM_EXACT_KEY",
                "attachment_position": attachment.position,
                "filename": attachment.filename,
                "normalized_identifier": profile.normalized_identifier,
            },
            "classification_context_evidence": {
                "source": "canonical_body_semantics",
                "item_type_used": False,
            },
        }

    def parse(self, mail: Dict[str, Any]) -> ParseResult:
        try:
            return self._parse(mail)
        except Exception as error:
            reason = "system_failure:" + type(error).__name__ + ":" + str(error)
            return ParseResult(
                "SYSTEM_FAILURE",
                [reason],
                [],
                source={
                    "source_acquisition_status": "UNVERIFIED",
                    "manifest_contract_status": "UNVERIFIED",
                    "attachment_integrity_status": "FAIL",
                    "container_enumeration_status": "INCOMPLETE",
                    "inline_structure_status": "FAIL",
                    "attachment_mapping_status": "FAIL",
                    "source_atomic_status": "SYSTEM_FAILURE",
                    "auto_union_eligible": False,
                    "completeness_result": {
                        "status": "SYSTEM_FAILURE",
                        "reasons": [reason],
                        "checks": {},
                    },
                },
            )

    def _parse(self, mail: Dict[str, Any]) -> ParseResult:
        body_text = str(mail.get("body_text", ""))
        classified, enumeration_status, enumeration_reasons = self._enumerate_attachments(mail)
        acquisition = self._validate_manifest(mail, classified)
        profiles, declared_count, structural_complete, inline_reasons = self._parse_profiles(body_text)
        mapping, mapping_pass, mapping_reasons, station_matches = self._map(
            profiles, classified
        )
        item_attachments = [row for row in classified if row.role == "ITEM_ATTACHMENT"]
        role_safe = all(row.role != "UNKNOWN" for row in classified)
        archive_present = any(row.role == "ARCHIVE" for row in classified)
        container_complete = enumeration_status == EnumerationStatus.COMPLETE.value
        evidence = [
            CardinalityEvidence(
                authority=CardinalityAuthority.CONTAINER_ENUMERATION.value,
                source="ATTACHMENT_LIST:ITEM_ATTACHMENT",
                count=len(item_attachments) if container_complete else None,
                complete=container_complete and role_safe and not archive_present,
                is_primary=True,
                reasons=[] if container_complete and role_safe and not archive_present else [
                    "item_attachment_role_enumeration_incomplete"
                ],
            ),
            CardinalityEvidence(
                authority=CardinalityAuthority.DECLARED_COUNT.value,
                source="INLINE_BODY",
                count=declared_count,
                complete=declared_count is not None,
                reasons=[] if declared_count is not None else ["declared_count_unknown"],
            ),
            CardinalityEvidence(
                authority=CardinalityAuthority.STRUCTURAL_COMPLETE.value,
                source="INLINE_BODY",
                count=len(profiles),
                complete=structural_complete,
                reasons=[] if structural_complete else ["inline_structure_incomplete"],
            ),
        ]
        source_id = str(mail.get("message_id", ""))
        attachment_list_id = source_id + ":attachments"
        body_container_id = source_id + ":inline-body"
        child_ids = [source_id + ":attachment:" + str(row.position) for row in classified]
        containers = [
            Container(
                container_id=body_container_id,
                parent_container_id=source_id,
                kind=ContainerKind.INLINE_BODY.value,
                locator="body_text",
                content_fingerprint=body_fingerprint(body_text),
                enumeration_status=EnumerationStatus.COMPLETE.value if structural_complete else EnumerationStatus.INCOMPLETE.value,
                completeness=structural_complete,
                candidate_count=len(profiles),
                required=True,
                reasons=[] if structural_complete else inline_reasons,
            ),
            Container(
                container_id=attachment_list_id,
                parent_container_id=source_id,
                kind=ContainerKind.ATTACHMENT_LIST.value,
                locator="attachments",
                content_fingerprint=ordered_attachment_digest(
                    self._observed_manifest_entries(classified)
                ),
                enumeration_status=enumeration_status,
                completeness=container_complete,
                candidate_count=len(item_attachments),
                child_container_refs=child_ids,
                required=True,
                reasons=enumeration_reasons,
            ),
        ]
        containers.extend(
            Container(
                container_id=child_id,
                parent_container_id=attachment_list_id,
                kind=ContainerKind.ARCHIVE.value if row.role == "ARCHIVE" else ContainerKind.ATTACHMENT_FILE.value,
                locator="attachments[" + str(row.position) + "]",
                content_fingerprint=row.content_digest or self._fingerprint(row.to_dict()),
                enumeration_status=EnumerationStatus.UNSUPPORTED.value if row.role == "ARCHIVE" else EnumerationStatus.COMPLETE.value,
                completeness=not row.reasons and row.role != "ARCHIVE",
                candidate_count=1 if row.role == "ITEM_ATTACHMENT" else 0,
                reasons=list(row.reasons) + (["archive_container_not_expanded"] if row.role == "ARCHIVE" else []),
            )
            for child_id, row in zip(child_ids, classified)
        )
        candidates: List[ItemCandidate] = []
        for profile in profiles:
            attachment = mapping.get(profile.index)
            candidate_reasons = list(profile.reasons)
            if attachment is None:
                candidate_reasons.append("mapped_attachment_missing")
            logical_id = logical_item_id(
                self.config["source_company"], "resource", profile.identifier
            ) if profile.normalized_identifier else ""
            relation = [] if attachment is None else [{
                "role": "PRIMARY",
                "artifact_kind": "ATTACHMENT_FILE",
                "stable_locator": "attachment-key:" + profile.normalized_identifier,
                "content_sha256": attachment.content_digest,
                "version_relevant": True,
            }]
            candidates.append(
                ItemCandidate(
                    candidate_index=profile.index,
                    identifier=profile.identifier,
                    source_container_id=attachment_list_id,
                    body_text=profile.body_text,
                    parse_success=profile.complete and attachment is not None,
                    parse_reasons=candidate_reasons,
                    item_artifacts=relation,
                    artifact_relation_resolved=attachment is not None,
                    identity_evidence={"normalized_identifier": profile.normalized_identifier},
                    logical_item_id=logical_id,
                )
            )
        source = Source(
            source_id=source_id,
            source_type="EMAIL",
            source_company=self.config["source_company"],
            source_fingerprint=self._fingerprint(
                {"message_id": source_id, "body": body_text, "attachments": [row.manifest_entry() for row in classified]}
            ),
            delivery_semantics=DeliverySemantics.UNKNOWN.value,
            acquisition_status=acquisition.core_status,
            cardinality_evidence=evidence,
            container_references=[body_container_id, attachment_list_id],
            configured_primary_authority=CardinalityAuthority.CONTAINER_ENUMERATION.value,
            configured_cross_check_authorities=[
                CardinalityAuthority.DECLARED_COUNT.value,
                CardinalityAuthority.STRUCTURAL_COMPLETE.value,
            ],
            artifact_relations_resolved=mapping_pass and role_safe and not archive_present,
            artifact_relation_reasons=mapping_reasons,
        )
        completeness = evaluate_completeness(source, containers, candidates)
        source_atomic_status = "UNSUPPORTED" if archive_present else completeness.status
        technical_compatible = all(
            completeness.checks.get(name, False)
            for name in (
                "required_containers_found",
                "container_enumeration_complete",
                "cardinality_known",
                "cardinality_evidence_consistent",
                "candidate_count_matches",
                "all_candidates_parsed",
                "artifact_relations_resolved",
                "identity_collision_free",
            )
        ) and not archive_present
        projected = [
            self._materialize_item(mail, profile, mapping[profile.index], len(profiles))
            for profile in profiles
        ] if technical_compatible else []
        eligible = projected if source_atomic_status == "PARSED" else []
        source_dict = source.to_dict()
        source_dict.update(
            {
                "source_acquisition_status": acquisition.status,
                "manifest_contract_status": acquisition.manifest_contract_status,
                "attachment_integrity_status": acquisition.attachment_integrity_status,
                "attachment_acquisition_validation": acquisition.to_dict(),
                "container_enumeration_status": enumeration_status,
                "inline_structure_status": "PASS" if structural_complete else "FAIL",
                "attachment_mapping_status": "PASS" if mapping_pass else "FAIL",
                "attachment_role_status": "PASS" if role_safe and not archive_present else ("UNSUPPORTED" if archive_present else "FAIL"),
                "source_atomic_status": source_atomic_status,
                "auto_union_eligible": source_atomic_status == "PARSED",
                "declared_count": declared_count,
                "profile_count": len(profiles),
                "item_attachment_count": len(item_attachments),
                "mapping_count": len(mapping),
                "station_audit_matches": station_matches,
                "false_substring_matches": 0,
                "attachment_role_counts": {
                    role: sum(row.role == role for row in classified)
                    for role in sorted(ATTACHMENT_ROLES)
                },
                "mapping_strategy": "ONE_ARTIFACT_PER_ITEM_EXACT_KEY",
            }
        )
        reasons = list(
            dict.fromkeys(
                acquisition.reasons
                + enumeration_reasons
                + inline_reasons
                + mapping_reasons
                + completeness.reasons
                + (["archive_container_unverified"] if archive_present else [])
            )
        )
        return ParseResult(
            source_atomic_status,
            reasons,
            eligible,
            source_dict,
            [container.to_dict() for container in containers],
            [row.to_dict() for row in classified],
            projected,
        )

#!/usr/bin/env python3
"""Exact Source Acquisition digest/schema contract for the isolated prototype."""

import copy
import hashlib
import json
import re
import unicodedata
from datetime import datetime
from typing import Any, Dict, List, Sequence, Set, Tuple


PROFILE_REGISTRY_VERSION = "AcquisitionProfileRegistry.v1"
PROFILE_VERSION = "AcquisitionProfile.v1"
RESOLVED_SCOPE_VERSION = "ResolvedPlannedScope.v1"
PLANNED_CONTAINER_SET_VERSION = "PlannedContainerSet.v1"
ATTEMPT_PLAN_VERSION = "AcquisitionAttemptPlan.v1"
MANIFEST_VERSION = "AcquisitionManifest.v1"
SNAPSHOT_ENTRY_VERSION = "SnapshotEntry.v1"

ENTRY_RAW_DOMAIN = "99-1/source-acquisition/entry-raw/v1"
ORDERED_SNAPSHOT_SET_DOMAIN = "99-1/source-acquisition/ordered-snapshot-set/v1"
MANIFEST_DOMAIN = "99-1/source-acquisition/manifest/v1"
PROFILE_DOMAIN = "99-1/source-acquisition/profile/v1"
RESOLVED_SCOPE_DOMAIN = "99-1/source-acquisition/resolved-planned-scope/v1"
PLANNED_CONTAINER_SET_DOMAIN = "99-1/source-acquisition/planned-container-set/v1"

DIGEST_PATTERN = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
DATETIME_PATTERN = re.compile(
    r"\A[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z\Z"
)
INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1

MANIFEST_FIELDS = (
    "manifest_schema_version", "source_id", "attempt_id", "manifest_finalized_at",
    "acquisition_profile_id", "acquisition_profile_version",
    "acquisition_profile_digest", "resolved_planned_scope_digest",
    "planned_container_set_digest", "actual_container_entries", "snapshot_entries",
    "snapshot_count", "ordered_snapshot_set_digest", "source_version", "manifest_digest",
)
MANIFEST_DIGEST_FIELDS = tuple(field for field in MANIFEST_FIELDS if field != "manifest_digest")
SNAPSHOT_ENTRY_FIELDS = (
    "snapshot_entry_schema_version", "sequence", "entry_id", "planned_container_id",
    "page_or_tab_id", "locator_binding_id", "requested_range", "returned_range",
    "retrieved_at", "source_version", "http_status", "content_type", "byte_length",
    "raw_artifact_ref", "entry_raw_digest", "next_locator_metadata", "terminal",
    "terminal_evidence",
)
SNAPSHOT_ENTRY_REQUIRED_FIELDS = tuple(field for field in SNAPSHOT_ENTRY_FIELDS if field != "http_status")
ORDERED_SNAPSHOT_SET_FIELDS = (
    "sequence", "entry_id", "planned_container_id", "page_or_tab_id",
    "locator_binding_id", "requested_range", "returned_range", "source_version",
    "content_type", "byte_length", "entry_raw_digest", "next_locator_metadata",
    "terminal", "terminal_evidence",
)
PROFILE_DIGEST_FIELDS = (
    "profile_schema_version", "acquisition_profile_id", "acquisition_profile_version",
    "provider_id", "source_class", "acquisition_method", "planned_container_template",
    "scope_template", "optional_absence_rules", "version_policy", "presentation_policy",
    "business_state_mapping",
)
PROFILE_FIELDS = PROFILE_DIGEST_FIELDS + ("acquisition_profile_digest",)
RESOLVED_SCOPE_DIGEST_FIELDS = (
    "resolved_scope_schema_version", "source_id", "attempt_id", "acquisition_profile_id",
    "acquisition_profile_version", "acquisition_profile_digest", "provider_id",
    "source_class", "locator_binding_id", "acquisition_method", "scope_payload",
    "presentation_policy", "version_requirement", "termination_rule",
)
RESOLVED_SCOPE_FIELDS = RESOLVED_SCOPE_DIGEST_FIELDS + ("resolved_planned_scope_digest",)
PLANNED_CONTAINER_ENTRY_FIELDS = (
    "sequence", "planned_container_id", "container_kind", "required", "logical_role",
    "locator_ref", "locator_binding_id", "acquisition_profile_id",
    "acquisition_profile_version", "acquisition_profile_digest", "optional_absence_rule",
)
PLANNED_CONTAINER_SET_DIGEST_FIELDS = (
    "planned_container_set_schema_version", "source_id", "attempt_id",
    "acquisition_profile_id", "acquisition_profile_version", "acquisition_profile_digest",
    "resolved_planned_scope_digest", "planned_container_entries",
)
PLANNED_CONTAINER_SET_FIELDS = PLANNED_CONTAINER_SET_DIGEST_FIELDS + ("planned_container_set_digest",)
SCOPED_VALUE_FIELDS = ("state", "value")
VERSION_OBSERVATION_FIELDS = (
    "version_kind", "version_scope", "version_strength", "version_binding_id",
    "provider_authority_ref", "observed_at",
)
SOURCE_VERSION_FIELDS = VERSION_OBSERVATION_FIELDS + ("pre_version", "post_version")
NEXT_LOCATOR_FIELDS = ("state", "next_locator_binding_id")
TERMINAL_EVIDENCE_FIELDS = ("evidence_kind", "evidence_binding_id")
ACTUAL_CONTAINER_FIELDS = (
    "sequence", "container_id", "container_kind", "logical_role", "locator_ref",
    "locator_binding_id", "acquisition_profile_id", "acquisition_profile_version",
    "acquisition_profile_digest", "snapshot_entry_ids",
)
VERSION_KINDS = {
    "IMMUTABLE_REVISION", "SNAPSHOT_TOKEN", "ETAG", "LAST_MODIFIED", "TIMESTAMP",
    "PROVIDER_VERSION", "COMPOSITE_REVISION", "UNKNOWN",
}
VERSION_SCOPES = {
    "SOURCE_WIDE", "WORKBOOK_WIDE", "LIST_WIDE", "TAB", "PAGE", "RESPONSE",
    "RANGE", "UNKNOWN",
}
VERSION_STRENGTHS = {"STRONG", "WEAK", "UNKNOWN"}
CONTAINER_KINDS = {
    "INLINE_BODY", "ATTACHMENT_LIST", "ATTACHMENT_FILE", "ARCHIVE", "SPREADSHEET",
    "PDF", "WEB_PAGE", "WEB_LIST", "GOOGLE_SHEET", "LINK_BUNDLE",
}


class ContractError(ValueError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _nfc_string(value: str) -> str:
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise ContractError("lone_surrogate_rejected")
    return unicodedata.normalize("NFC", value)


def _canonical_string(value: str) -> str:
    output = ['"']
    for character in _nfc_string(value):
        codepoint = ord(character)
        if character == '"':
            output.append('\\"')
        elif character == "\\":
            output.append("\\\\")
        elif codepoint <= 0x1F:
            output.append("\\u00" + format(codepoint, "02x"))
        else:
            output.append(character)
    output.append('"')
    return "".join(output)


def _canonical_text(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        if value < INT64_MIN or value > INT64_MAX:
            raise ContractError("integer_out_of_int64_range")
        return str(value)
    if isinstance(value, float):
        raise ContractError("float_rejected")
    if isinstance(value, str):
        return _canonical_string(value)
    if isinstance(value, list):
        return "[" + ",".join(_canonical_text(item) for item in value) + "]"
    if isinstance(value, dict):
        normalized_items: Dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ContractError("object_key_not_string")
            normalized_key = _nfc_string(key)
            if normalized_key in normalized_items:
                raise ContractError("nfc_key_collision")
            normalized_items[normalized_key] = item
        return "{" + ",".join(
            _canonical_string(key) + ":" + _canonical_text(normalized_items[key])
            for key in sorted(normalized_items)
        ) + "}"
    raise ContractError("unsupported_json_type:" + type(value).__name__)


def canonical_json_bytes(value: Any) -> bytes:
    return _canonical_text(value).encode("utf-8")


def parse_canonical_json(payload: bytes) -> Any:
    if not isinstance(payload, bytes):
        raise ContractError("canonical_json_input_must_be_bytes")
    if payload.startswith(b"\xef\xbb\xbf"):
        raise ContractError("bom_rejected")
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise ContractError("invalid_utf8") from error

    def parse_int(value: str) -> int:
        parsed = int(value, 10)
        if parsed < INT64_MIN or parsed > INT64_MAX:
            raise ContractError("integer_out_of_int64_range")
        return parsed

    def reject_float(value: str) -> Any:
        raise ContractError("float_rejected:" + value)

    def object_pairs(pairs: Sequence[Tuple[str, Any]]) -> Dict[str, Any]:
        raw_keys: Set[str] = set()
        normalized: Dict[str, Any] = {}
        for raw_key, item in pairs:
            if raw_key in raw_keys:
                raise ContractError("duplicate_json_key")
            raw_keys.add(raw_key)
            normalized_key = _nfc_string(raw_key)
            if normalized_key in normalized:
                raise ContractError("nfc_key_collision")
            normalized[normalized_key] = item
        return normalized

    try:
        value = json.loads(
            text, object_pairs_hook=object_pairs, parse_int=parse_int,
            parse_float=reject_float, parse_constant=reject_float,
        )
    except ContractError:
        raise
    except (json.JSONDecodeError, UnicodeError) as error:
        raise ContractError("invalid_json") from error
    if canonical_json_bytes(value) != payload:
        raise ContractError("json_not_canonical")
    return value


def _domain_digest(domain: str, payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(domain.encode("utf-8") + b"\x00" + payload).hexdigest()


def calculate_entry_raw_digest(raw_bytes: bytes) -> str:
    if not isinstance(raw_bytes, bytes):
        raise ContractError("raw_artifact_must_be_bytes")
    return _domain_digest(ENTRY_RAW_DOMAIN, raw_bytes)


def _exact_projection(value: Dict[str, Any], fields: Sequence[str]) -> Dict[str, Any]:
    return {field: copy.deepcopy(value[field]) for field in fields}


def calculate_ordered_snapshot_set_digest(entries: Sequence[Dict[str, Any]]) -> str:
    projection = [_exact_projection(entry, ORDERED_SNAPSHOT_SET_FIELDS) for entry in entries]
    return _domain_digest(ORDERED_SNAPSHOT_SET_DOMAIN, canonical_json_bytes(projection))


def calculate_manifest_digest(manifest: Dict[str, Any]) -> str:
    return _domain_digest(
        MANIFEST_DOMAIN, canonical_json_bytes(_exact_projection(manifest, MANIFEST_DIGEST_FIELDS))
    )


def calculate_profile_digest(profile: Dict[str, Any]) -> str:
    return _domain_digest(
        PROFILE_DOMAIN, canonical_json_bytes(_exact_projection(profile, PROFILE_DIGEST_FIELDS))
    )


def calculate_resolved_scope_digest(scope: Dict[str, Any]) -> str:
    return _domain_digest(
        RESOLVED_SCOPE_DOMAIN,
        canonical_json_bytes(_exact_projection(scope, RESOLVED_SCOPE_DIGEST_FIELDS)),
    )


def calculate_planned_container_set_digest(container_set: Dict[str, Any]) -> str:
    projection = _exact_projection(container_set, PLANNED_CONTAINER_SET_DIGEST_FIELDS)
    projection["planned_container_entries"] = [
        _exact_projection(entry, PLANNED_CONTAINER_ENTRY_FIELDS)
        for entry in container_set["planned_container_entries"]
    ]
    return _domain_digest(PLANNED_CONTAINER_SET_DOMAIN, canonical_json_bytes(projection))


def _exact_fields(value: Any, required: Sequence[str], allowed: Sequence[str], prefix: str) -> List[str]:
    if not isinstance(value, dict):
        return [prefix + ":not_object"]
    reasons: List[str] = []
    reasons.extend(prefix + ":missing_field:" + field for field in sorted(set(required) - set(value)))
    reasons.extend(prefix + ":unknown_field:" + field for field in sorted(set(value) - set(allowed)))
    return reasons


def _string(value: Any, prefix: str, non_empty: bool = True) -> List[str]:
    if not isinstance(value, str):
        return [prefix + ":not_string"]
    try:
        normalized = _nfc_string(value)
    except ContractError as error:
        return [prefix + ":" + error.reason]
    if non_empty and not normalized:
        return [prefix + ":empty"]
    return []


def _identifier(value: Any, prefix: str) -> List[str]:
    return _string(value, prefix, non_empty=True)


def _digest(value: Any, prefix: str) -> List[str]:
    return [] if isinstance(value, str) and DIGEST_PATTERN.fullmatch(value) else [prefix + ":invalid_digest"]


def _int64(value: Any, prefix: str, non_negative: bool = False) -> List[str]:
    if isinstance(value, bool) or not isinstance(value, int):
        return [prefix + ":not_int64"]
    if value < INT64_MIN or value > INT64_MAX:
        return [prefix + ":out_of_int64_range"]
    if non_negative and value < 0:
        return [prefix + ":negative"]
    return []


def _datetime(value: Any, prefix: str) -> List[str]:
    if not isinstance(value, str) or DATETIME_PATTERN.fullmatch(value) is None:
        return [prefix + ":invalid_datetime_format"]
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return [prefix + ":invalid_datetime_value"]
    return []


def _enum(value: Any, allowed: Set[str], prefix: str) -> List[str]:
    return [] if isinstance(value, str) and value in allowed else [prefix + ":invalid_enum"]


def validate_scoped_value(value: Any, prefix: str) -> List[str]:
    reasons = _exact_fields(value, SCOPED_VALUE_FIELDS, SCOPED_VALUE_FIELDS, prefix)
    if reasons:
        return reasons
    reasons.extend(_enum(value["state"], {"VALUE", "NOT_APPLICABLE", "UNRESOLVED"}, prefix + ":state"))
    reasons.extend(_string(value["value"], prefix + ":value", non_empty=False))
    if value["state"] == "VALUE" and value["value"] == "":
        reasons.append(prefix + ":value_required")
    if value["state"] != "VALUE" and value["value"] != "":
        reasons.append(prefix + ":value_must_be_empty")
    return reasons


def validate_version_observation(value: Any, prefix: str) -> List[str]:
    reasons = _exact_fields(value, VERSION_OBSERVATION_FIELDS, VERSION_OBSERVATION_FIELDS, prefix)
    if reasons:
        return reasons
    reasons.extend(_enum(value["version_kind"], VERSION_KINDS, prefix + ":version_kind"))
    reasons.extend(_enum(value["version_scope"], VERSION_SCOPES, prefix + ":version_scope"))
    reasons.extend(_enum(value["version_strength"], VERSION_STRENGTHS, prefix + ":version_strength"))
    reasons.extend(_identifier(value["version_binding_id"], prefix + ":version_binding_id"))
    reasons.extend(_identifier(value["provider_authority_ref"], prefix + ":provider_authority_ref"))
    reasons.extend(_datetime(value["observed_at"], prefix + ":observed_at"))
    return reasons


def validate_source_version(value: Any, prefix: str = "source_version") -> List[str]:
    reasons = _exact_fields(value, SOURCE_VERSION_FIELDS, SOURCE_VERSION_FIELDS, prefix)
    if reasons:
        return reasons
    reasons.extend(validate_version_observation(_exact_projection(value, VERSION_OBSERVATION_FIELDS), prefix))
    reasons.extend(validate_version_observation(value["pre_version"], prefix + ":pre_version"))
    reasons.extend(validate_version_observation(value["post_version"], prefix + ":post_version"))
    return reasons


def validate_next_locator(value: Any, prefix: str) -> List[str]:
    reasons = _exact_fields(value, NEXT_LOCATOR_FIELDS, NEXT_LOCATOR_FIELDS, prefix)
    if reasons:
        return reasons
    reasons.extend(_enum(value["state"], {"PRESENT", "ABSENT", "NOT_APPLICABLE", "UNRESOLVED"}, prefix + ":state"))
    reasons.extend(_string(value["next_locator_binding_id"], prefix + ":next_locator_binding_id", non_empty=False))
    if value["state"] == "PRESENT" and not value["next_locator_binding_id"]:
        reasons.append(prefix + ":next_locator_binding_id_required")
    if value["state"] != "PRESENT" and value["next_locator_binding_id"]:
        reasons.append(prefix + ":next_locator_binding_id_must_be_empty")
    return reasons


def validate_terminal_evidence(value: Any, prefix: str) -> List[str]:
    reasons = _exact_fields(value, TERMINAL_EVIDENCE_FIELDS, TERMINAL_EVIDENCE_FIELDS, prefix)
    if reasons:
        return reasons
    binding_required = {"PROVIDER_END_MARKER", "NO_NEXT_LOCATOR", "PREBOUND_SCOPE_END"}
    reasons.extend(_enum(value["evidence_kind"], binding_required | {"NOT_APPLICABLE", "UNRESOLVED"}, prefix + ":evidence_kind"))
    reasons.extend(_string(value["evidence_binding_id"], prefix + ":evidence_binding_id", non_empty=False))
    if value["evidence_kind"] in binding_required and not value["evidence_binding_id"]:
        reasons.append(prefix + ":evidence_binding_id_required")
    if value["evidence_kind"] not in binding_required and value["evidence_binding_id"]:
        reasons.append(prefix + ":evidence_binding_id_must_be_empty")
    return reasons


def validate_snapshot_entry(entry: Any, http_status_required: bool = False, prefix: str = "snapshot_entry") -> List[str]:
    reasons = _exact_fields(entry, SNAPSHOT_ENTRY_REQUIRED_FIELDS, SNAPSHOT_ENTRY_FIELDS, prefix)
    if reasons:
        return reasons
    if http_status_required and "http_status" not in entry:
        reasons.append(prefix + ":missing_field:http_status")
    if entry["snapshot_entry_schema_version"] != SNAPSHOT_ENTRY_VERSION:
        reasons.append(prefix + ":schema_version_mismatch")
    reasons.extend(_int64(entry["sequence"], prefix + ":sequence", non_negative=True))
    for field in ("entry_id", "planned_container_id", "locator_binding_id", "raw_artifact_ref"):
        reasons.extend(_identifier(entry[field], prefix + ":" + field))
    reasons.extend(validate_scoped_value(entry["page_or_tab_id"], prefix + ":page_or_tab_id"))
    reasons.extend(validate_scoped_value(entry["requested_range"], prefix + ":requested_range"))
    reasons.extend(validate_scoped_value(entry["returned_range"], prefix + ":returned_range"))
    reasons.extend(_datetime(entry["retrieved_at"], prefix + ":retrieved_at"))
    reasons.extend(validate_version_observation(entry["source_version"], prefix + ":source_version"))
    if "http_status" in entry:
        reasons.extend(_int64(entry["http_status"], prefix + ":http_status"))
        if isinstance(entry["http_status"], int) and not isinstance(entry["http_status"], bool) and not 100 <= entry["http_status"] <= 599:
            reasons.append(prefix + ":http_status_out_of_range")
    reasons.extend(_string(entry["content_type"], prefix + ":content_type"))
    reasons.extend(_int64(entry["byte_length"], prefix + ":byte_length", non_negative=True))
    reasons.extend(_digest(entry["entry_raw_digest"], prefix + ":entry_raw_digest"))
    reasons.extend(validate_next_locator(entry["next_locator_metadata"], prefix + ":next_locator_metadata"))
    reasons.extend(_enum(entry["terminal"], {"TERMINAL", "NOT_TERMINAL", "UNRESOLVED"}, prefix + ":terminal"))
    reasons.extend(validate_terminal_evidence(entry["terminal_evidence"], prefix + ":terminal_evidence"))
    try:
        canonical_json_bytes(entry)
    except ContractError as error:
        reasons.append(prefix + ":canonical:" + error.reason)
    return list(dict.fromkeys(reasons))


def validate_actual_container(entry: Any, prefix: str) -> List[str]:
    reasons = _exact_fields(entry, ACTUAL_CONTAINER_FIELDS, ACTUAL_CONTAINER_FIELDS, prefix)
    if reasons:
        return reasons
    reasons.extend(_int64(entry["sequence"], prefix + ":sequence", non_negative=True))
    for field in (
        "container_id", "logical_role", "locator_ref", "locator_binding_id",
        "acquisition_profile_id", "acquisition_profile_version",
    ):
        reasons.extend(_identifier(entry[field], prefix + ":" + field))
    reasons.extend(_enum(entry["container_kind"], CONTAINER_KINDS, prefix + ":container_kind"))
    reasons.extend(_digest(entry["acquisition_profile_digest"], prefix + ":acquisition_profile_digest"))
    if not isinstance(entry["snapshot_entry_ids"], list):
        reasons.append(prefix + ":snapshot_entry_ids_not_array")
    else:
        for index, value in enumerate(entry["snapshot_entry_ids"]):
            reasons.extend(_identifier(value, prefix + ":snapshot_entry_ids:" + str(index)))
    return reasons


def validate_profile(profile: Any) -> List[str]:
    reasons = _exact_fields(profile, PROFILE_FIELDS, PROFILE_FIELDS, "profile")
    if reasons:
        return reasons
    if profile["profile_schema_version"] != PROFILE_VERSION:
        reasons.append("profile:schema_version_mismatch")
    for field in (
        "acquisition_profile_id", "acquisition_profile_version", "provider_id",
        "source_class", "acquisition_method", "presentation_policy", "business_state_mapping",
    ):
        reasons.extend(_identifier(profile[field], "profile:" + field))
    reasons.extend(_digest(profile["acquisition_profile_digest"], "profile:acquisition_profile_digest"))
    try:
        canonical_json_bytes(_exact_projection(profile, PROFILE_DIGEST_FIELDS))
    except ContractError as error:
        reasons.append("profile:canonical:" + error.reason)
    if not reasons and profile["acquisition_profile_digest"] != calculate_profile_digest(profile):
        reasons.append("profile_digest_mismatch")
    return reasons


def validate_profile_registry(registry: Any) -> List[str]:
    fields = ("schema_version", "registry_id", "profiles")
    reasons = _exact_fields(registry, fields, fields, "profile_registry")
    if reasons:
        return reasons
    if registry["schema_version"] != PROFILE_REGISTRY_VERSION:
        reasons.append("profile_registry:schema_version_mismatch")
    reasons.extend(_identifier(registry["registry_id"], "profile_registry:registry_id"))
    if not isinstance(registry["profiles"], list) or len(registry["profiles"]) != 1:
        reasons.append("profile_count_must_equal_one")
    else:
        reasons.extend(validate_profile(registry["profiles"][0]))
    return reasons


def validate_resolved_scope(scope: Any) -> List[str]:
    reasons = _exact_fields(scope, RESOLVED_SCOPE_FIELDS, RESOLVED_SCOPE_FIELDS, "resolved_scope")
    if reasons:
        return reasons
    if scope["resolved_scope_schema_version"] != RESOLVED_SCOPE_VERSION:
        reasons.append("resolved_scope:schema_version_mismatch")
    for field in RESOLVED_SCOPE_DIGEST_FIELDS:
        if field != "scope_payload":
            reasons.extend(_identifier(scope[field], "resolved_scope:" + field))
    reasons.extend(_digest(scope["acquisition_profile_digest"], "resolved_scope:acquisition_profile_digest"))
    reasons.extend(_digest(scope["resolved_planned_scope_digest"], "resolved_scope:resolved_planned_scope_digest"))
    try:
        canonical_json_bytes(scope["scope_payload"])
    except ContractError as error:
        reasons.append("resolved_scope:scope_payload:" + error.reason)
    if not reasons and scope["resolved_planned_scope_digest"] != calculate_resolved_scope_digest(scope):
        reasons.append("resolved_scope_digest_mismatch")
    return list(dict.fromkeys(reasons))


def validate_planned_container_set(container_set: Any) -> List[str]:
    reasons = _exact_fields(
        container_set, PLANNED_CONTAINER_SET_FIELDS, PLANNED_CONTAINER_SET_FIELDS,
        "planned_container_set",
    )
    if reasons:
        return reasons
    if container_set["planned_container_set_schema_version"] != PLANNED_CONTAINER_SET_VERSION:
        reasons.append("planned_container_set:schema_version_mismatch")
    for field in PLANNED_CONTAINER_SET_DIGEST_FIELDS:
        if field != "planned_container_entries":
            reasons.extend(_identifier(container_set[field], "planned_container_set:" + field))
    reasons.extend(_digest(container_set["acquisition_profile_digest"], "planned_container_set:acquisition_profile_digest"))
    reasons.extend(_digest(container_set["resolved_planned_scope_digest"], "planned_container_set:resolved_planned_scope_digest"))
    entries = container_set["planned_container_entries"]
    if not isinstance(entries, list) or len(entries) != 1:
        reasons.append("planned_container_count_must_equal_one")
    else:
        for index, entry in enumerate(entries):
            prefix = "planned_container_entry:" + str(index)
            entry_reasons = _exact_fields(entry, PLANNED_CONTAINER_ENTRY_FIELDS, PLANNED_CONTAINER_ENTRY_FIELDS, prefix)
            reasons.extend(entry_reasons)
            if entry_reasons:
                continue
            reasons.extend(_int64(entry["sequence"], prefix + ":sequence", non_negative=True))
            for field in (
                "planned_container_id", "container_kind", "logical_role", "locator_ref",
                "locator_binding_id", "acquisition_profile_id", "acquisition_profile_version",
                "optional_absence_rule",
            ):
                reasons.extend(_identifier(entry[field], prefix + ":" + field))
            if not isinstance(entry["required"], bool):
                reasons.append(prefix + ":required_not_bool")
            reasons.extend(_digest(entry["acquisition_profile_digest"], prefix + ":acquisition_profile_digest"))
    if not reasons and container_set["planned_container_set_digest"] != calculate_planned_container_set_digest(container_set):
        reasons.append("planned_container_set_digest_mismatch")
    return list(dict.fromkeys(reasons))


def validate_attempt_plan(plan: Any) -> List[str]:
    fields = (
        "schema_version", "attempt_id", "attempt_ordinal", "attempt_started_at", "provider",
        "acquisition_method", "profile_ref", "resolved_planned_scope",
        "planned_container_set", "candidate_emission", "auto_union",
        "production_integration", "plan_state",
    )
    reasons = _exact_fields(plan, fields, fields, "attempt_plan")
    if reasons:
        return reasons
    if plan["schema_version"] != ATTEMPT_PLAN_VERSION:
        reasons.append("attempt_plan:schema_version_mismatch")
    reasons.extend(_identifier(plan["attempt_id"], "attempt_plan:attempt_id"))
    reasons.extend(_int64(plan["attempt_ordinal"], "attempt_plan:attempt_ordinal", non_negative=True))
    reasons.extend(_datetime(plan["attempt_started_at"], "attempt_plan:attempt_started_at"))
    reasons.extend(validate_resolved_scope(plan["resolved_planned_scope"]))
    reasons.extend(validate_planned_container_set(plan["planned_container_set"]))
    if plan["candidate_emission"] != 0 or plan["auto_union"] is not False or plan["production_integration"] is not False:
        reasons.append("attempt_plan:emission_or_production_contract_mismatch")
    return list(dict.fromkeys(reasons))


def _manifest_schema_reasons(manifest: Any, http_status_required: bool) -> List[str]:
    reasons = _exact_fields(manifest, MANIFEST_FIELDS, MANIFEST_FIELDS, "manifest")
    if reasons:
        return reasons
    if manifest["manifest_schema_version"] != MANIFEST_VERSION:
        reasons.append("manifest:schema_version_mismatch")
    for field in ("source_id", "attempt_id", "acquisition_profile_id", "acquisition_profile_version"):
        reasons.extend(_identifier(manifest[field], "manifest:" + field))
    reasons.extend(_datetime(manifest["manifest_finalized_at"], "manifest:manifest_finalized_at"))
    for field in (
        "acquisition_profile_digest", "resolved_planned_scope_digest",
        "planned_container_set_digest", "ordered_snapshot_set_digest", "manifest_digest",
    ):
        reasons.extend(_digest(manifest[field], "manifest:" + field))
    reasons.extend(_int64(manifest["snapshot_count"], "manifest:snapshot_count", non_negative=True))
    snapshots = manifest["snapshot_entries"] if isinstance(manifest["snapshot_entries"], list) else []
    actual = manifest["actual_container_entries"] if isinstance(manifest["actual_container_entries"], list) else []
    if not isinstance(manifest["snapshot_entries"], list):
        reasons.append("manifest:snapshot_entries_not_array")
    if not isinstance(manifest["actual_container_entries"], list):
        reasons.append("manifest:actual_container_entries_not_array")
    for index, entry in enumerate(snapshots):
        reasons.extend(validate_snapshot_entry(entry, http_status_required, "snapshot_entry:" + str(index)))
    for index, entry in enumerate(actual):
        reasons.extend(validate_actual_container(entry, "actual_container_entry:" + str(index)))
    reasons.extend(validate_source_version(manifest["source_version"]))
    if isinstance(manifest["snapshot_count"], int) and manifest["snapshot_count"] != len(snapshots):
        reasons.append("manifest:snapshot_count_mismatch")
    if [entry.get("sequence") for entry in actual] != list(range(len(actual))):
        reasons.append("manifest:actual_container_sequence_not_contiguous")
    container_ids = [entry.get("container_id") for entry in actual]
    if len(container_ids) != len(set(container_ids)):
        reasons.append("manifest:actual_container_id_not_unique")
    snapshot_ids = [entry.get("entry_id") for entry in snapshots]
    if len(snapshot_ids) != len(set(snapshot_ids)):
        reasons.append("manifest:snapshot_entry_id_not_unique")
    references: List[Any] = []
    for entry in actual:
        if isinstance(entry.get("snapshot_entry_ids"), list):
            references.extend(entry["snapshot_entry_ids"])
    if any(reference not in set(snapshot_ids) for reference in references):
        reasons.append("manifest:snapshot_reference_outside_manifest")
    if len(references) != len(set(references)):
        reasons.append("manifest:snapshot_referenced_by_multiple_containers")
    if set(references) != set(snapshot_ids):
        reasons.append("manifest:orphan_snapshot_entry")
    try:
        canonical_json_bytes(manifest)
    except ContractError as error:
        reasons.append("manifest:canonical:" + error.reason)
    return list(dict.fromkeys(reasons))


def _status_for_reasons(reasons: Sequence[str]) -> Tuple[str, str]:
    values = set(reasons)
    if any(
        reason.startswith(("manifest:", "snapshot_entry:", "actual_container_entry:", "source_version:"))
        or reason in {
            "manifest_digest_mismatch", "snapshot_entry_digest_mismatch",
            "ordered_snapshot_set_digest_mismatch", "attempt_uncommitted",
        }
        for reason in values
    ):
        return "INCOMPLETE", "NONE"
    if "revision_drift" in values:
        return "SNAPSHOT_UNSTABLE", "NONE"
    if any(reason in values for reason in (
        "planned_scope_mismatch", "planned_container_set_mismatch",
        "required_container_missing", "range_gap",
    )):
        return "PARTIAL", "NONE"
    if "presentation_unresolved" in values:
        return "UNVERIFIED", "HUMAN_REVIEW"
    if any(reason in values for reason in (
        "profile_digest_mismatch", "strong_version_unavailable", "version_stability_unverified",
    )):
        return "UNVERIFIED", "NONE"
    return ("OTHER", "NONE") if values else ("VERIFIED_COMPLETE", "NONE")


def validate_manifest(
    manifest: Any,
    profile_registry: Dict[str, Any],
    attempt_plan: Dict[str, Any],
    raw_entries: Dict[str, bytes],
    attempt_state: str = "COMMITTED",
) -> Dict[str, Any]:
    reasons: List[str] = []
    reasons.extend(validate_profile_registry(profile_registry))
    reasons.extend(validate_attempt_plan(attempt_plan))
    profile = profile_registry.get("profiles", [{}])[0]
    http_status_required = profile.get("acquisition_method") == "PUBLIC_SHARED_URL_XLSX_EXPORT"
    reasons.extend(_manifest_schema_reasons(manifest, http_status_required))
    if isinstance(manifest, dict) and set(manifest) == set(MANIFEST_FIELDS):
        if manifest["acquisition_profile_digest"] != profile.get("acquisition_profile_digest"):
            reasons.append("profile_digest_mismatch")
        scope = attempt_plan.get("resolved_planned_scope", {})
        container_set = attempt_plan.get("planned_container_set", {})
        if manifest["resolved_planned_scope_digest"] != scope.get("resolved_planned_scope_digest"):
            reasons.append("planned_scope_mismatch")
        if manifest["planned_container_set_digest"] != container_set.get("planned_container_set_digest"):
            reasons.append("planned_container_set_mismatch")
        entries = manifest.get("snapshot_entries", [])
        if isinstance(entries, list):
            for entry in entries:
                payload = raw_entries.get(entry.get("entry_id"))
                if payload is None or calculate_entry_raw_digest(payload) != entry.get("entry_raw_digest"):
                    reasons.append("snapshot_entry_digest_mismatch")
            try:
                if manifest["ordered_snapshot_set_digest"] != calculate_ordered_snapshot_set_digest(entries):
                    reasons.append("ordered_snapshot_set_digest_mismatch")
            except (ContractError, KeyError, TypeError):
                reasons.append("ordered_snapshot_set_digest_mismatch")
        planned_entries = container_set.get("planned_container_entries", [])
        actual_ids = {
            entry.get("container_id") for entry in manifest.get("actual_container_entries", [])
            if isinstance(entry, dict)
        }
        if any(
            entry.get("required") is True and entry.get("planned_container_id") not in actual_ids
            for entry in planned_entries if isinstance(entry, dict)
        ):
            reasons.append("required_container_missing")
        if any(
            entry.get("returned_range", {}).get("state") != "VALUE"
            for entry in entries if isinstance(entry, dict)
        ):
            reasons.append("range_gap")
        source_version = manifest.get("source_version", {})
        if source_version.get("version_strength") != "STRONG" or source_version.get("version_scope") != "WORKBOOK_WIDE":
            reasons.append("strong_version_unavailable")
        pre = source_version.get("pre_version", {})
        post = source_version.get("post_version", {})
        if pre.get("version_kind") != "UNKNOWN" and post.get("version_kind") != "UNKNOWN":
            if pre.get("version_binding_id") != post.get("version_binding_id"):
                reasons.append("revision_drift")
        else:
            reasons.append("version_stability_unverified")
        if profile.get("presentation_policy") == "UNRESOLVED":
            reasons.append("presentation_unresolved")
        if attempt_state != "COMMITTED":
            reasons.append("attempt_uncommitted")
        try:
            if manifest["manifest_digest"] != calculate_manifest_digest(manifest):
                reasons.append("manifest_digest_mismatch")
        except (ContractError, KeyError, TypeError):
            reasons.append("manifest_digest_mismatch")
    reasons = list(dict.fromkeys(reasons))
    status, review_status = _status_for_reasons(reasons)
    integrity_invalid = any(
        reason.startswith(("manifest:", "snapshot_entry:", "actual_container_entry:", "source_version:"))
        for reason in reasons
    ) or any(reason in reasons for reason in (
        "manifest_digest_mismatch", "snapshot_entry_digest_mismatch",
        "ordered_snapshot_set_digest_mismatch",
    ))
    return {
        "valid": not integrity_invalid,
        "exact_manifest_schema": not any(reason.startswith("manifest:") for reason in reasons),
        "exact_snapshot_entry_schema": not any(reason.startswith("snapshot_entry:") for reason in reasons),
        "digest_conformance": not any(reason in reasons for reason in (
            "profile_digest_mismatch", "resolved_scope_digest_mismatch",
            "planned_container_set_digest_mismatch", "manifest_digest_mismatch",
            "snapshot_entry_digest_mismatch", "ordered_snapshot_set_digest_mismatch",
        )),
        "acquisition_status": status, "review_status": review_status, "eligible": 0,
        "auto_union": False, "candidate_emission": 0, "reasons": reasons,
    }


def finalize_profile(profile: Dict[str, Any]) -> Dict[str, Any]:
    value = copy.deepcopy(profile)
    value["acquisition_profile_digest"] = calculate_profile_digest(value)
    return value


def finalize_resolved_scope(scope: Dict[str, Any]) -> Dict[str, Any]:
    value = copy.deepcopy(scope)
    value["resolved_planned_scope_digest"] = calculate_resolved_scope_digest(value)
    return value


def finalize_planned_container_set(container_set: Dict[str, Any]) -> Dict[str, Any]:
    value = copy.deepcopy(container_set)
    value["planned_container_set_digest"] = calculate_planned_container_set_digest(value)
    return value


def finalize_manifest(manifest: Dict[str, Any]) -> Dict[str, Any]:
    value = copy.deepcopy(manifest)
    value["manifest_digest"] = calculate_manifest_digest(value)
    return value


def offline_negative_proofs(
    manifest: Dict[str, Any], profile_registry: Dict[str, Any],
    attempt_plan: Dict[str, Any], raw_entries: Dict[str, bytes],
) -> List[Dict[str, Any]]:
    cases: List[Dict[str, Any]] = []

    def record(
        name: str, changed_manifest: Dict[str, Any],
        changed_registry: Dict[str, Any] = None, changed_plan: Dict[str, Any] = None,
        changed_raw: Dict[str, bytes] = None, attempt_state: str = "COMMITTED",
    ) -> None:
        cases.append({
            "name": name,
            "result": validate_manifest(
                changed_manifest, changed_registry or profile_registry,
                changed_plan or attempt_plan, raw_entries if changed_raw is None else changed_raw,
                attempt_state=attempt_state,
            ),
        })

    changed_registry = copy.deepcopy(profile_registry)
    changed_registry["profiles"][0]["acquisition_profile_digest"] = "sha256:" + "0" * 64
    record("profile_digest_mismatch", manifest, changed_registry=changed_registry)
    changed = copy.deepcopy(manifest)
    changed["resolved_planned_scope_digest"] = "sha256:" + "1" * 64
    record("planned_scope_mismatch", finalize_manifest(changed))
    changed = copy.deepcopy(manifest)
    changed["actual_container_entries"] = []
    changed["snapshot_entries"] = []
    changed["snapshot_count"] = 0
    changed["ordered_snapshot_set_digest"] = calculate_ordered_snapshot_set_digest([])
    record("required_container_missing", finalize_manifest(changed))
    changed = copy.deepcopy(manifest)
    for field in ("version_kind", "version_scope", "version_strength"):
        changed["source_version"][field] = "UNKNOWN"
    record("strong_version_unavailable", finalize_manifest(changed))
    changed = copy.deepcopy(manifest)
    changed["source_version"].update({
        "version_kind": "PROVIDER_VERSION", "version_scope": "WORKBOOK_WIDE",
        "version_strength": "STRONG",
    })
    changed["source_version"]["pre_version"].update({
        "version_kind": "PROVIDER_VERSION", "version_scope": "WORKBOOK_WIDE",
        "version_strength": "STRONG", "version_binding_id": "revision:before",
    })
    changed["source_version"]["post_version"].update({
        "version_kind": "PROVIDER_VERSION", "version_scope": "WORKBOOK_WIDE",
        "version_strength": "STRONG", "version_binding_id": "revision:after",
    })
    record("revision_drift", finalize_manifest(changed))
    changed = copy.deepcopy(manifest)
    changed["snapshot_entries"][0]["returned_range"] = {"state": "UNRESOLVED", "value": ""}
    changed["ordered_snapshot_set_digest"] = calculate_ordered_snapshot_set_digest(changed["snapshot_entries"])
    record("range_gap", finalize_manifest(changed))
    changed_raw = dict(raw_entries)
    entry_id = manifest["snapshot_entries"][0]["entry_id"]
    changed_raw[entry_id] = changed_raw[entry_id] + b"offline-mutation"
    record("digest_mismatch", manifest, changed_raw=changed_raw)
    changed_registry = copy.deepcopy(profile_registry)
    changed_registry["profiles"][0]["presentation_policy"] = "UNRESOLVED"
    changed_registry["profiles"][0] = finalize_profile(changed_registry["profiles"][0])
    changed_plan = copy.deepcopy(attempt_plan)
    changed_plan["resolved_planned_scope"]["acquisition_profile_digest"] = changed_registry["profiles"][0]["acquisition_profile_digest"]
    changed_plan["resolved_planned_scope"] = finalize_resolved_scope(changed_plan["resolved_planned_scope"])
    changed_plan["planned_container_set"]["acquisition_profile_digest"] = changed_registry["profiles"][0]["acquisition_profile_digest"]
    changed_plan["planned_container_set"]["resolved_planned_scope_digest"] = changed_plan["resolved_planned_scope"]["resolved_planned_scope_digest"]
    changed_plan["planned_container_set"]["planned_container_entries"][0]["acquisition_profile_digest"] = changed_registry["profiles"][0]["acquisition_profile_digest"]
    changed_plan["planned_container_set"] = finalize_planned_container_set(changed_plan["planned_container_set"])
    changed = copy.deepcopy(manifest)
    changed["acquisition_profile_digest"] = changed_registry["profiles"][0]["acquisition_profile_digest"]
    changed["resolved_planned_scope_digest"] = changed_plan["resolved_planned_scope"]["resolved_planned_scope_digest"]
    changed["planned_container_set_digest"] = changed_plan["planned_container_set"]["planned_container_set_digest"]
    changed["actual_container_entries"][0]["acquisition_profile_digest"] = changed["acquisition_profile_digest"]
    record("presentation_unresolved", finalize_manifest(changed), changed_registry, changed_plan)
    record("attempt_uncommitted", manifest, attempt_state="PLANNED")
    return cases

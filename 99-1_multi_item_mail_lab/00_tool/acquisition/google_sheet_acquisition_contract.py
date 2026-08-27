#!/usr/bin/env python3
"""Minimal Source Acquisition Contract for the isolated Google Sheet prototype."""

import copy
import hashlib
import json
import re
from typing import Any, Dict, List, Sequence, Tuple


PROFILE_REGISTRY_VERSION = "AcquisitionProfileRegistry.v1"
ATTEMPT_PLAN_VERSION = "AcquisitionAttemptPlan.v1"
MANIFEST_VERSION = "AcquisitionManifest.v1"
SNAPSHOT_ENTRY_VERSION = "SnapshotEntry.v1"
DIGEST_PATTERN = re.compile(r"\Asha256:[0-9a-f]{64}\Z")

ENTRY_RAW_DIGEST = "ENTRY_RAW_DIGEST"
ORDERED_SNAPSHOT_SET_DIGEST = "ORDERED_SNAPSHOT_SET_DIGEST"
MANIFEST_DIGEST = "MANIFEST_DIGEST"
PROFILE_DIGEST = "PROFILE_DIGEST"
RESOLVED_SCOPE_DIGEST = "RESOLVED_SCOPE_DIGEST"
PLANNED_CONTAINER_SET_DIGEST = "PLANNED_CONTAINER_SET_DIGEST"

ACQUISITION_STATUSES = {
    "VERIFIED_COMPLETE",
    "UNVERIFIED",
    "PARTIAL",
    "AUTH_REQUIRED",
    "SNAPSHOT_UNSTABLE",
    "INCOMPLETE",
    "OTHER",
}


def canonical_json_bytes(value: Any) -> bytes:
    """Existing lab canonical JSON: UTF-8, sorted keys, compact separators."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def digest_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def digest_json(value: Any) -> str:
    return digest_bytes(canonical_json_bytes(value))


def _without_digest(value: Dict[str, Any], digest_field: str) -> Dict[str, Any]:
    canonical = copy.deepcopy(value)
    canonical.pop(digest_field, None)
    return canonical


def calculate_profile_digest(profile: Dict[str, Any]) -> str:
    return digest_json(_without_digest(profile, "profile_digest"))


def calculate_resolved_scope_digest(scope: Dict[str, Any]) -> str:
    return digest_json(scope)


def calculate_planned_container_set_digest(
    containers: Sequence[Dict[str, Any]],
) -> str:
    return digest_json(list(containers))


def calculate_ordered_snapshot_set_digest(
    entries: Sequence[Dict[str, Any]],
) -> str:
    ordered = [
        {
            "position": position,
            "snapshot_entry_id": entry.get("snapshot_entry_id", ""),
            "planned_container_id": entry.get("planned_container_id", ""),
            "entry_raw_digest": entry.get("entry_raw_digest", ""),
            "byte_count": entry.get("byte_count"),
            "media_type": entry.get("media_type", ""),
        }
        for position, entry in enumerate(entries)
    ]
    return digest_json(ordered)


def calculate_manifest_digest(manifest: Dict[str, Any]) -> str:
    return digest_json(_without_digest(manifest, "manifest_digest"))


def bind_profile_digest(registry: Dict[str, Any]) -> Dict[str, Any]:
    bound = copy.deepcopy(registry)
    for profile in bound.get("profiles", []):
        profile["profile_digest"] = calculate_profile_digest(profile)
    return bound


def validate_profile_registry(registry: Any) -> List[str]:
    reasons: List[str] = []
    if not isinstance(registry, dict):
        return ["profile_registry_not_object"]
    if registry.get("schema_version") != PROFILE_REGISTRY_VERSION:
        reasons.append("profile_registry_schema_mismatch")
    profiles = registry.get("profiles")
    if not isinstance(profiles, list) or len(profiles) != 1:
        reasons.append("profile_count_must_equal_one")
        return reasons
    profile = profiles[0]
    required_strings = (
        "profile_id",
        "profile_version",
        "provider",
        "acquisition_method",
        "presentation_policy",
        "profile_digest",
    )
    for field_name in required_strings:
        if not isinstance(profile.get(field_name), str) or not profile[field_name]:
            reasons.append("profile_field_invalid:" + field_name)
    if profile.get("provider") != "GOOGLE_SHEETS":
        reasons.append("profile_provider_mismatch")
    if profile.get("presentation_policy") not in {
        "REQUIRED",
        "NOT_USED",
        "UNRESOLVED",
    }:
        reasons.append("presentation_policy_invalid")
    if profile.get("candidate_emission") != 0:
        reasons.append("candidate_emission_must_be_zero")
    if profile.get("auto_union") is not False:
        reasons.append("auto_union_must_be_false")
    if profile.get("production_integration") is not False:
        reasons.append("production_integration_must_be_false")
    if profile.get("profile_digest") != calculate_profile_digest(profile):
        reasons.append("profile_digest_mismatch")
    return reasons


def validate_attempt_plan(plan: Any) -> List[str]:
    reasons: List[str] = []
    if not isinstance(plan, dict):
        return ["attempt_plan_not_object"]
    if plan.get("schema_version") != ATTEMPT_PLAN_VERSION:
        reasons.append("attempt_plan_schema_mismatch")
    if plan.get("provider") != "GOOGLE_SHEETS":
        reasons.append("attempt_plan_provider_mismatch")
    if plan.get("attempt_ordinal") != 1:
        reasons.append("attempt_ordinal_must_equal_one")
    if plan.get("candidate_emission") != 0 or plan.get("auto_union") is not False:
        reasons.append("attempt_emission_contract_mismatch")
    profile_ref = plan.get("profile_ref")
    if not isinstance(profile_ref, dict):
        reasons.append("profile_ref_missing")
    elif not all(
        isinstance(profile_ref.get(name), str) and profile_ref[name]
        for name in ("profile_id", "profile_version", "profile_digest")
    ):
        reasons.append("profile_ref_invalid")
    scope = plan.get("resolved_planned_scope")
    if not isinstance(scope, dict):
        reasons.append("resolved_planned_scope_missing")
    elif plan.get("resolved_scope_digest") != calculate_resolved_scope_digest(scope):
        reasons.append("resolved_scope_digest_mismatch")
    containers = plan.get("planned_containers")
    if not isinstance(containers, list) or len(containers) != 1:
        reasons.append("planned_container_count_must_equal_one")
    else:
        container = containers[0]
        for field_name in (
            "planned_container_id",
            "container_kind",
            "logical_role",
            "locator_ref",
            "locator_binding_id",
            "profile_id",
            "profile_version",
            "profile_digest",
        ):
            if not isinstance(container.get(field_name), str) or not container[field_name]:
                reasons.append("planned_container_field_invalid:" + field_name)
        if container.get("container_kind") != "GOOGLE_SHEET":
            reasons.append("planned_container_kind_mismatch")
        if container.get("required") is not True:
            reasons.append("planned_container_required_must_be_true")
    if isinstance(containers, list) and plan.get(
        "planned_container_set_digest"
    ) != calculate_planned_container_set_digest(containers):
        reasons.append("planned_container_set_digest_mismatch")
    return reasons


def validate_snapshot_entry(entry: Any) -> List[str]:
    reasons: List[str] = []
    if not isinstance(entry, dict):
        return ["snapshot_entry_not_object"]
    if entry.get("schema_version") != SNAPSHOT_ENTRY_VERSION:
        reasons.append("snapshot_entry_schema_mismatch")
    for field_name in (
        "snapshot_entry_id",
        "planned_container_id",
        "relative_path",
        "media_type",
        "entry_raw_digest",
        "acquired_at",
    ):
        if not isinstance(entry.get(field_name), str) or not entry[field_name]:
            reasons.append("snapshot_entry_field_invalid:" + field_name)
    if not isinstance(entry.get("byte_count"), int) or entry["byte_count"] < 0:
        reasons.append("snapshot_entry_byte_count_invalid")
    digest = entry.get("entry_raw_digest")
    if not isinstance(digest, str) or DIGEST_PATTERN.fullmatch(digest) is None:
        reasons.append("entry_raw_digest_invalid")
    return reasons


def _status_for_reasons(
    reasons: Sequence[str], manifest: Dict[str, Any]
) -> Tuple[str, str]:
    values = set(reasons)
    if manifest.get("access_status") == "AUTH_REQUIRED":
        return "AUTH_REQUIRED", "NONE"
    if "revision_drift" in values:
        return "SNAPSHOT_UNSTABLE", "NONE"
    if any(
        reason in values
        for reason in (
            "manifest_digest_mismatch",
            "snapshot_entry_digest_mismatch",
            "ordered_snapshot_set_digest_mismatch",
            "attempt_uncommitted",
        )
    ):
        return "INCOMPLETE", "NONE"
    if any(
        reason in values
        for reason in (
            "planned_scope_mismatch",
            "required_container_missing",
            "range_gap",
        )
    ):
        return "PARTIAL", "NONE"
    if "presentation_unresolved" in values:
        return "UNVERIFIED", "HUMAN_REVIEW"
    if any(
        reason in values
        for reason in (
            "profile_digest_mismatch",
            "strong_version_unavailable",
            "version_stability_unverified",
        )
    ):
        return "UNVERIFIED", "NONE"
    if values:
        return "OTHER", "NONE"
    return "VERIFIED_COMPLETE", "NONE"


def validate_manifest(
    manifest: Any,
    profile_registry: Dict[str, Any],
    attempt_plan: Dict[str, Any],
    raw_entries: Dict[str, bytes],
) -> Dict[str, Any]:
    """Validate digests and fail-closed completeness without emitting candidates."""
    reasons: List[str] = []
    if not isinstance(manifest, dict):
        return {
            "valid": False,
            "acquisition_status": "INCOMPLETE",
            "review_status": "NONE",
            "eligible": 0,
            "reasons": ["manifest_not_object"],
        }
    if manifest.get("schema_version") != MANIFEST_VERSION:
        reasons.append("manifest_schema_mismatch")
    profile_reasons = validate_profile_registry(profile_registry)
    if "profile_digest_mismatch" in profile_reasons:
        reasons.append("profile_digest_mismatch")
    elif profile_reasons:
        reasons.extend(profile_reasons)
    plan_reasons = validate_attempt_plan(attempt_plan)
    reasons.extend(plan_reasons)
    profile = profile_registry.get("profiles", [{}])[0]
    profile_ref = attempt_plan.get("profile_ref", {})
    manifest_profile_ref = manifest.get("profile_ref", {})
    expected_profile_ref = {
        "profile_id": profile.get("profile_id"),
        "profile_version": profile.get("profile_version"),
        "profile_digest": profile.get("profile_digest"),
    }
    if profile_ref != expected_profile_ref or manifest_profile_ref != expected_profile_ref:
        reasons.append("profile_digest_mismatch")
    if manifest.get("resolved_scope_digest") != attempt_plan.get(
        "resolved_scope_digest"
    ):
        reasons.append("planned_scope_mismatch")
    if manifest.get("planned_container_set_digest") != attempt_plan.get(
        "planned_container_set_digest"
    ):
        reasons.append("planned_scope_mismatch")

    entries = manifest.get("snapshot_entries")
    if not isinstance(entries, list):
        entries = []
        reasons.append("snapshot_entries_missing")
    for entry in entries:
        reasons.extend(validate_snapshot_entry(entry))
        entry_id = entry.get("snapshot_entry_id", "")
        payload = raw_entries.get(entry_id)
        if payload is None or digest_bytes(payload) != entry.get("entry_raw_digest"):
            reasons.append("snapshot_entry_digest_mismatch")
    expected_snapshot_set_digest = calculate_ordered_snapshot_set_digest(entries)
    if manifest.get("ordered_snapshot_set_digest") != expected_snapshot_set_digest:
        reasons.append("ordered_snapshot_set_digest_mismatch")

    planned = attempt_plan.get("planned_containers", [])
    captured_ids = {entry.get("planned_container_id") for entry in entries}
    if any(
        container.get("required") is True
        and container.get("planned_container_id") not in captured_ids
        for container in planned
    ):
        reasons.append("required_container_missing")

    evidence = manifest.get("completeness_evidence", {})
    if evidence.get("resolved_scope_digest") != attempt_plan.get(
        "resolved_scope_digest"
    ):
        reasons.append("planned_scope_mismatch")
    if evidence.get("range_complete") is not True and entries:
        reasons.append("range_gap")
    version = manifest.get("version_authority", {})
    if version.get("strength") != "STRONG" or version.get("scope") != "WORKBOOK_WIDE":
        reasons.append("strong_version_unavailable")
    if version.get("pre_post_stable") is False:
        reasons.append("revision_drift")
    elif version.get("pre_post_stable") is not True:
        reasons.append("version_stability_unverified")
    if manifest.get("presentation_policy") == "UNRESOLVED":
        reasons.append("presentation_unresolved")
    if manifest.get("attempt_state") != "COMMITTED":
        reasons.append("attempt_uncommitted")
    if manifest.get("manifest_digest") != calculate_manifest_digest(manifest):
        reasons.append("manifest_digest_mismatch")

    reasons = list(dict.fromkeys(reasons))
    status, review_status = _status_for_reasons(reasons, manifest)
    valid = not any(
        reason in reasons
        for reason in (
            "manifest_schema_mismatch",
            "manifest_digest_mismatch",
            "snapshot_entry_digest_mismatch",
            "ordered_snapshot_set_digest_mismatch",
            "attempt_uncommitted",
        )
    )
    return {
        "valid": valid,
        "acquisition_status": status,
        "review_status": review_status,
        "eligible": 0,
        "auto_union": False,
        "candidate_emission": 0,
        "reasons": reasons,
    }


def finalize_manifest(manifest: Dict[str, Any]) -> Dict[str, Any]:
    finalized = copy.deepcopy(manifest)
    finalized["manifest_digest"] = calculate_manifest_digest(finalized)
    return finalized


def offline_negative_proofs(
    manifest: Dict[str, Any],
    profile_registry: Dict[str, Any],
    attempt_plan: Dict[str, Any],
    raw_entries: Dict[str, bytes],
) -> List[Dict[str, Any]]:
    """Mutate a saved artifact offline; no provider access is performed."""
    cases = []

    def record(
        name: str,
        changed_manifest: Dict[str, Any],
        changed_registry: Dict[str, Any] = None,
        changed_plan: Dict[str, Any] = None,
        changed_raw: Dict[str, bytes] = None,
    ) -> Dict[str, Any]:
        result = validate_manifest(
            changed_manifest,
            changed_registry or profile_registry,
            changed_plan or attempt_plan,
            changed_raw if changed_raw is not None else raw_entries,
        )
        return {"name": name, "result": result}

    changed_registry = copy.deepcopy(profile_registry)
    changed_registry["profiles"][0]["profile_digest"] = "sha256:" + "0" * 64
    cases.append(record("profile_digest_mismatch", manifest, changed_registry=changed_registry))

    changed = copy.deepcopy(manifest)
    changed["resolved_scope_digest"] = "sha256:" + "1" * 64
    changed = finalize_manifest(changed)
    cases.append(record("planned_scope_mismatch", changed))

    changed = copy.deepcopy(manifest)
    changed["snapshot_entries"] = []
    changed["ordered_snapshot_set_digest"] = calculate_ordered_snapshot_set_digest([])
    changed = finalize_manifest(changed)
    cases.append(record("required_container_missing", changed, changed_raw={}))

    changed = copy.deepcopy(manifest)
    changed["version_authority"] = {
        "version_kind": "UNAVAILABLE",
        "scope": "UNKNOWN",
        "strength": "UNKNOWN",
        "pre_post_stable": None,
    }
    changed = finalize_manifest(changed)
    cases.append(record("strong_version_unavailable", changed))

    changed = copy.deepcopy(manifest)
    changed["version_authority"]["pre_post_stable"] = False
    changed = finalize_manifest(changed)
    cases.append(record("revision_drift", changed))

    changed = copy.deepcopy(manifest)
    changed["completeness_evidence"]["range_complete"] = False
    changed = finalize_manifest(changed)
    cases.append(record("range_gap", changed))

    changed_raw = dict(raw_entries)
    if changed.get("snapshot_entries"):
        entry_id = changed["snapshot_entries"][0]["snapshot_entry_id"]
        changed_raw[entry_id] = changed_raw.get(entry_id, b"") + b"offline-mutation"
    cases.append(record("digest_mismatch", manifest, changed_raw=changed_raw))

    changed = copy.deepcopy(manifest)
    changed["presentation_policy"] = "UNRESOLVED"
    changed = finalize_manifest(changed)
    cases.append(record("presentation_unresolved", changed))

    changed = copy.deepcopy(manifest)
    changed["attempt_state"] = "PLANNED"
    changed = finalize_manifest(changed)
    cases.append(record("attempt_uncommitted", changed))
    return cases

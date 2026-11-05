#!/usr/bin/env python3
"""Run the five-group skill registry M0 shadow comparison.

This tool is read-only with respect to canonical pipeline artifacts.  It writes
only to the explicit output directory supplied by the caller.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import shutil
import sys
import tempfile
import time
import tracemalloc
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.skill_canonical_map import SKILL_ALIAS_MAP, SKILL_CANONICAL_MAP
from common.skill_registry import (
    RegistrySnapshot,
    build_effective_matcher,
    build_registry_matcher,
    execution_fingerprint,
    hits_to_skill_output,
    implementation_identity,
    load_registry,
    load_registry_for_mode,
    shadow_primary_skills,
)
from common.skill_registry_shadow import (
    canonical_groups_for_skill_diff,
    expected_difference,
    judge_required_gate_legacy_substring,
    judge_skill_gate,
)
from common.skill_registry_runtime import load_runtime_context


PROJECT_DICTIONARY = ROOT / "03-8_extract_project_skill_category/00_tool/skill_dictionary.txt"
RESOURCE_DICTIONARY = ROOT / "05-8_extract_resource_skill_category/00_tool/skill_dictionary.txt"
REGISTRY_PATH = ROOT / "common/skill_registry.yaml"

IMPLEMENTATION_FILES = (
    ROOT / "common" / "skill_canonical_map.py",
    ROOT / "common" / "skill_registry.py",
    ROOT / "common" / "skill_registry_shadow.py",
    ROOT
    / "03-8_extract_project_skill_category/00_tool/extract_project_skill_category.py",
    PROJECT_DICTIONARY,
    RESOURCE_DICTIONARY,
    ROOT / "06-8_match_skill_category/00_tool/config.py",
    ROOT / "06-11_match_required_skills_list/00_tool/match_required_skills_list.py",
    ROOT
    / "06-12_filter_required_skills_noise/00_tool/filter_required_skills_noise.py",
    Path(__file__).resolve(),
)


def _read_jsonl(path: Path) -> List[dict]:
    records: List[dict] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError("invalid JSONL {}:{}: {}".format(path, line_number, exc)) from exc
            if not isinstance(value, dict):
                raise ValueError("JSONL record must be object {}:{}".format(path, line_number))
            records.append(value)
    return records


def _read_map(path: Path, key: str = "message_id") -> Dict[str, dict]:
    result: Dict[str, dict] = {}
    for record in _read_jsonl(path):
        value = record.get(key)
        if not isinstance(value, str) or not value:
            raise ValueError("missing {} in {}".format(key, path))
        if value in result:
            raise ValueError("duplicate {}={} in {}".format(key, value, path))
        result[value] = record
    return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, values: Iterable[dict]) -> int:
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for value in values:
            handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n")
            count += 1
    return count


def _pair_key(record: dict) -> Tuple[str, str]:
    return (
        record["project_info"]["message_id"],
        record["resource_info"]["message_id"],
    )


def _pair_id(key: Tuple[str, str]) -> str:
    return "{}::{}".format(key[0], key[1])


def _load_python_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load module: {}".format(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _difference_record(
    *,
    step: str,
    difference_type: str,
    legacy_value: Any,
    registry_value: Any,
    canonical_groups: Sequence[str],
    expected: bool,
    expected_reason: str,
    fingerprint: str,
    message_id: Optional[str] = None,
    pair_key: Optional[Tuple[str, str]] = None,
) -> dict:
    return {
        "difference_id": "{}:{}:{}".format(
            step, message_id or _pair_id(pair_key or ("", "")), difference_type
        ),
        "message_id": message_id,
        "pair_id": _pair_id(pair_key) if pair_key else None,
        "project_message_id": pair_key[0] if pair_key else None,
        "resource_message_id": pair_key[1] if pair_key else None,
        "step": step,
        "difference_type": difference_type,
        "legacy_value": legacy_value,
        "registry_value": registry_value,
        "expected": expected,
        "expected_reason": expected_reason,
        "canonical_groups": list(canonical_groups),
        "registry_fingerprint": fingerprint,
    }


def _registry_hit_records(
    step: str,
    message_id: str,
    hits: Sequence[Any],
    fingerprint: str,
    scope: str,
    ownership: str,
) -> Iterable[dict]:
    for order, hit in enumerate(hits):
        if hit.source != "registry":
            continue
        value = hit.to_dict()
        value.update(
            {
                "step": step,
                "message_id": message_id,
                "scope": scope,
                "order": order,
                "registry_fingerprint": fingerprint,
                "ownership": ownership,
            }
        )
        yield value


def _canonical_artifact_paths() -> Tuple[Path, ...]:
    return (
        ROOT / "03-8_extract_project_skill_category/01_result/extract_project_skill_category.jsonl",
        ROOT / "05-8_extract_resource_skill_category/01_result/extract_resource_skill_category.jsonl",
        ROOT / "06-8_match_skill_category/01_result/matched_pairs_skill_category.jsonl",
        ROOT / "06-8_match_skill_category/01_result/99_no_matched_pairs_skill_category.jsonl",
        ROOT / "06-11_match_required_skills_list/01_result/matched_pairs_required_skills_list.jsonl",
        ROOT / "06-11_match_required_skills_list/01_result/99_not_matched_pairs_required_skills_list.jsonl",
    )


def _artifact_hashes() -> Dict[str, str]:
    return {str(path.relative_to(ROOT)): _sha256(path) for path in _canonical_artifact_paths()}


def _legacy_pair_map(matched_path: Path, rejected_path: Path) -> Dict[Tuple[str, str], dict]:
    result: Dict[Tuple[str, str], dict] = {}
    for path in (matched_path, rejected_path):
        for record in _read_jsonl(path):
            key = _pair_key(record)
            if key in result:
                raise ValueError("duplicate pair {} in {}".format(_pair_id(key), path))
            result[key] = record
    return result


def _legacy_skill_gate_value(record: dict) -> dict:
    match_info = record.get("match_info") or {}
    detail = match_info.get("match_detail") or {}
    return {
        "passed": bool(match_info.get("match_skill_category")),
        "matched_skills": detail.get("matched_skills") or [],
        "project_skills_used": detail.get("project_skills_used") or [],
        "resource_skills_used": detail.get("resource_skills_used") or [],
        "primary_skills_used": detail.get("primary_skills_used") or [],
        "reason": (
            "matched_primary_skill"
            if match_info.get("match_skill_category") and detail.get("primary_skills_used")
            else (
                "matched_skill"
                if match_info.get("match_skill_category")
                else "no_exact_skill_overlap"
            )
        ),
    }


def _legacy_required_gate_value(record: dict) -> dict:
    match_info = record.get("match_info") or {}
    detail = record.get("match_detail") or {}
    return {
        "passed": bool(match_info.get("match_required_skills_list")),
        "matched_keywords": detail.get("matched_keywords") or [],
        "matched_phase_keywords": detail.get("matched_phase_keywords") or [],
        "matched_in": detail.get("matched_in") or [],
        "reason": detail.get("match_reason") or "not_matched",
    }


def _required_gate_value_from_result(
    required_skills: Sequence[str],
    strong_phases: Sequence[str],
    weak_phases: Sequence[str],
    result: Tuple[bool, Sequence[str], Sequence[str], Sequence[str]],
) -> dict:
    passed, matched_skills, matched_phases, matched_in = result
    if not required_skills and not strong_phases and not weak_phases:
        reason = "rejected_no_required_conditions"
    elif passed and matched_phases:
        reason = "matched_required_skill_and_phase"
    elif passed and matched_skills:
        reason = "matched_required_skill_only"
    else:
        reason = "not_matched"
    return {
        "passed": bool(passed),
        "matched_keywords": list(matched_skills),
        "matched_phase_keywords": list(matched_phases),
        "matched_in": list(matched_in),
        "reason": reason,
    }


def run_shadow(
    run_date: str,
    run_id: str,
    output_dir: Path,
    registry_path: Path,
    runtime_context_path: Optional[Path] = None,
) -> dict:
    if output_dir.exists():
        raise FileExistsError("output directory already exists: {}".format(output_dir))
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".skill-registry-shadow-", dir=str(output_dir.parent)))
    start = time.perf_counter()
    try:
        baseline_hashes = _artifact_hashes()
        project_dict_hash = _sha256(PROJECT_DICTIONARY)
        resource_dict_hash = _sha256(RESOURCE_DICTIONARY)
        if project_dict_hash != resource_dict_hash:
            raise ValueError("03-8 / 05-8 legacy dictionaries are not byte-identical")

        production_runtime = None
        if runtime_context_path is not None:
            production_runtime = load_runtime_context(
                runtime_context_path,
                ROOT,
                expected_mode="registry_overlay",
            )
            if production_runtime.run_date != run_date or production_runtime.run_id != run_id:
                raise ValueError("production runtime identity mismatch")
            source_context = json.loads(
                Path(runtime_context_path).read_text(encoding="utf-8")
            )
            implementation_id = production_runtime.implementation_identity
            implementation_file_hashes = source_context["implementation_file_hashes"]
            initial_snapshot = production_runtime.snapshot
            validation_mode = "registry_overlay"
            fingerprint = production_runtime.registry_fingerprint
        else:
            implementation_id = implementation_identity(IMPLEMENTATION_FILES)
            implementation_file_hashes = {
                str(path.resolve().relative_to(ROOT)): _sha256(path)
                for path in IMPLEMENTATION_FILES
            }
            initial_snapshot = load_registry(registry_path)
            validation_mode = "shadow"
        snapshot_path = stage / "registry_snapshot.yaml"
        snapshot_path.write_bytes(initial_snapshot.raw_bytes)
        snapshot = load_registry(snapshot_path)
        if snapshot.registry_sha256 != initial_snapshot.registry_sha256:
            raise ValueError("registry snapshot hash mismatch")
        if production_runtime is None:
            fingerprint = execution_fingerprint(
                snapshot,
                mode="shadow",
                required_search_mode="legacy_substring",
                implementation_id=implementation_id,
            )
        legacy_snapshot = load_registry_for_mode("legacy", stage / "missing-registry.yaml")
        legacy_fingerprint = execution_fingerprint(
            legacy_snapshot,
            mode="legacy",
            required_search_mode="legacy_substring",
            implementation_id=implementation_id,
        )

        context = {
            "run_date": run_date,
            "run_id": run_id,
            "skill_registry_mode": validation_mode,
            "required_search_mode": "legacy_substring",
            "schema_version": snapshot.schema_version,
            "registry_sha256": snapshot.registry_sha256,
            "registry_fingerprint": fingerprint,
            "legacy_fingerprint": legacy_fingerprint,
            "implementation_identity": implementation_id,
            "implementation_file_hashes": implementation_file_hashes,
            "registry_snapshot": "registry_snapshot.yaml",
            "registry_canonicals": list(snapshot.canonical_names),
            "registry_ownership": snapshot.ownership_map,
            "production_canonicals": list(snapshot.production_canonical_names),
            "shadow_canonicals": list(snapshot.shadow_canonical_names),
        }
        _write_json(stage / "execution_context.json", context)

        registry_compile_start = time.perf_counter()
        matcher, overrides = build_effective_matcher(
            snapshot,
            PROJECT_DICTIONARY,
            SKILL_CANONICAL_MAP,
            {"production"},
        )
        registry_matcher = build_registry_matcher(snapshot, {"production"})
        shadow_matcher = build_registry_matcher(snapshot, {"shadow"})
        registry_compile_seconds = time.perf_counter() - registry_compile_start

        improvement_groups = snapshot.production_canonical_names
        registry_canonical_set = set(snapshot.production_canonical_names)
        differences: List[dict] = []
        hit_records: List[dict] = []

        body_map = _read_map(
            ROOT / "01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl"
        )
        project_ids = [
            record["message_id"]
            for record in _read_jsonl(
                ROOT / "02-2_classify_output_file_project_resource/01_result/projects.jsonl"
            )
        ]
        resource_ids = [
            record["message_id"]
            for record in _read_jsonl(
                ROOT / "02-2_classify_output_file_project_resource/01_result/resources.jsonl"
            )
        ]
        legacy_compile_start = time.perf_counter()
        legacy_extract = _load_python_module(
            "skill_shadow_legacy_extract",
            ROOT
            / "03-8_extract_project_skill_category/00_tool/extract_project_skill_category.py",
        )
        legacy_dictionary = legacy_extract.load_skill_dictionary(str(PROJECT_DICTIONARY))
        legacy_compile_seconds = time.perf_counter() - legacy_compile_start
        required_map = _read_map(
            ROOT / "03-50_extract_project_required_skills/01_result/extract_project_required_skills.jsonl"
        )

        shadow_projects: Dict[str, dict] = {}
        shadow_resources: Dict[str, dict] = {}
        legacy_projects: Dict[str, dict] = {}
        legacy_resources: Dict[str, dict] = {}
        extraction_counts = Counter()

        tracemalloc.start()
        legacy_extraction_start = time.perf_counter()
        for message_id in project_ids:
            if message_id not in body_map:
                raise KeyError("missing project legacy input: {}".format(message_id))
            body = body_map[message_id].get("body_text") or ""
            legacy_skills, legacy_by_category, _ = legacy_extract.extract_skills(
                body, legacy_dictionary
            )
            required_texts = [
                value.get("skill", "")
                for value in required_map.get(message_id, {}).get("required_skills", [])
            ]
            legacy_projects[message_id] = {
                "message_id": message_id,
                "skills": legacy_skills,
                "skills_by_category": legacy_by_category,
                "primary_skills": legacy_extract.classify_primary_skills(
                    legacy_skills, required_texts
                ),
            }
        for message_id in resource_ids:
            if message_id not in body_map:
                raise KeyError("missing resource legacy input: {}".format(message_id))
            body = body_map[message_id].get("body_text") or ""
            legacy_skills, legacy_by_category, _ = legacy_extract.extract_skills(
                body, legacy_dictionary
            )
            legacy_resources[message_id] = {
                "message_id": message_id,
                "skills": legacy_skills,
                "skills_by_category": legacy_by_category,
            }
        legacy_extraction_seconds = time.perf_counter() - legacy_extraction_start
        _, legacy_peak_memory_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        tracemalloc.start()
        registry_extraction_start = time.perf_counter()
        for message_id in project_ids:
            if message_id not in body_map:
                raise KeyError("missing project shadow input: {}".format(message_id))
            legacy = legacy_projects[message_id]
            body = body_map[message_id].get("body_text") or ""
            required_texts = [
                value.get("skill", "")
                for value in required_map.get(message_id, {}).get("required_skills", [])
            ]
            hits = matcher.find_hits(body)
            skills, by_category = hits_to_skill_output(hits)
            primary, required_hits = shadow_primary_skills(
                hits,
                required_texts,
                registry_matcher,
                improvement_groups,
                legacy.get("primary_skills") or [],
            )
            shadow = {
                "message_id": message_id,
                "skills": skills,
                "skills_by_category": by_category,
                "primary_skills": primary,
            }
            shadow_projects[message_id] = shadow
            hit_records.extend(
                _registry_hit_records(
                    "03-8", message_id, hits, fingerprint, "body", "production"
                )
            )
            hit_records.extend(
                _registry_hit_records(
                    "03-8-shadow",
                    message_id,
                    shadow_matcher.find_hits(body),
                    fingerprint,
                    "body",
                    "shadow",
                )
            )
            for canonical, values in required_hits.items():
                for value in values:
                    hit_records.append(
                        {
                            "step": "03-8-primary",
                            "message_id": message_id,
                            "scope": "required_text",
                            "order": len(hit_records),
                            "canonical": canonical,
                            "category": snapshot.entry_for_canonical(canonical).category,
                            "matched_surface": value["matched_surface"],
                            "span": value["span"],
                            "match_mode": value["match_mode"],
                            "source": "registry",
                            "registry_fingerprint": fingerprint,
                            "ownership": "production",
                        }
                    )

            extraction_changed = (
                legacy.get("skills") != skills
                or legacy.get("skills_by_category") != by_category
            )
            if extraction_changed:
                groups = canonical_groups_for_skill_diff(
                    snapshot, legacy.get("skills") or [], skills
                )
                expected, reason = expected_difference(
                    "extraction_changed", None, None, groups, improvement_groups
                )
                differences.append(
                    _difference_record(
                        step="03-8",
                        difference_type="extraction_changed",
                        message_id=message_id,
                        legacy_value={
                            "skills": legacy.get("skills") or [],
                            "skills_by_category": legacy.get("skills_by_category") or {},
                        },
                        registry_value={
                            "skills": skills,
                            "skills_by_category": by_category,
                        },
                        canonical_groups=groups,
                        expected=expected,
                        expected_reason=reason,
                        fingerprint=fingerprint,
                    )
                )
                extraction_counts["03-8"] += 1
            if legacy.get("primary_skills") != primary:
                groups = canonical_groups_for_skill_diff(
                    snapshot, legacy.get("primary_skills") or [], primary
                )
                expected, reason = expected_difference(
                    "primary_changed", None, None, groups, improvement_groups
                )
                differences.append(
                    _difference_record(
                        step="03-8-primary",
                        difference_type="primary_changed",
                        message_id=message_id,
                        legacy_value=legacy.get("primary_skills") or [],
                        registry_value=primary,
                        canonical_groups=groups,
                        expected=expected,
                        expected_reason=reason,
                        fingerprint=fingerprint,
                    )
                )
                extraction_counts["primary"] += 1

        for message_id in resource_ids:
            if message_id not in body_map:
                raise KeyError("missing resource shadow input: {}".format(message_id))
            legacy = legacy_resources[message_id]
            body = body_map[message_id].get("body_text") or ""
            hits = matcher.find_hits(body)
            skills, by_category = hits_to_skill_output(hits)
            shadow_resources[message_id] = {
                "message_id": message_id,
                "skills": skills,
                "skills_by_category": by_category,
            }
            hit_records.extend(
                _registry_hit_records(
                    "05-8", message_id, hits, fingerprint, "body", "production"
                )
            )
            hit_records.extend(
                _registry_hit_records(
                    "05-8-shadow",
                    message_id,
                    shadow_matcher.find_hits(body),
                    fingerprint,
                    "body",
                    "shadow",
                )
            )
            if legacy.get("skills") != skills or legacy.get("skills_by_category") != by_category:
                groups = canonical_groups_for_skill_diff(
                    snapshot, legacy.get("skills") or [], skills
                )
                expected, reason = expected_difference(
                    "extraction_changed", None, None, groups, improvement_groups
                )
                differences.append(
                    _difference_record(
                        step="05-8",
                        difference_type="extraction_changed",
                        message_id=message_id,
                        legacy_value={
                            "skills": legacy.get("skills") or [],
                            "skills_by_category": legacy.get("skills_by_category") or {},
                        },
                        registry_value={
                            "skills": skills,
                            "skills_by_category": by_category,
                        },
                        canonical_groups=groups,
                        expected=expected,
                        expected_reason=reason,
                        fingerprint=fingerprint,
                    )
                )
                extraction_counts["05-8"] += 1
        registry_extraction_seconds = time.perf_counter() - registry_extraction_start
        _, registry_peak_memory_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        skill_config = _load_python_module(
            "skill_shadow_06_8_config",
            ROOT / "06-8_match_skill_category/00_tool/config.py",
        )
        excluded_skills = list(skill_config.SKILL_MATCH_EXCLUDED)
        input_68 = _read_jsonl(
            ROOT / "06-7_match_vendor_tiers/01_result/matched_pairs_vendor_tiers.jsonl"
        )
        gate_68_counts = Counter()
        gate_start = time.perf_counter()
        legacy_gate_68_seconds = 0.0
        registry_gate_68_seconds = 0.0
        for pair in input_68:
            key = _pair_key(pair)
            project = shadow_projects[key[0]]
            resource = shadow_resources[key[1]]
            legacy_project = legacy_projects[key[0]]
            legacy_resource = legacy_resources[key[1]]
            gate_call_start = time.perf_counter()
            legacy_result = judge_skill_gate(
                legacy_project["skills"],
                legacy_resource["skills"],
                legacy_project["primary_skills"],
                excluded_skills,
            )
            legacy_gate_68_seconds += time.perf_counter() - gate_call_start
            gate_call_start = time.perf_counter()
            shadow_result = judge_skill_gate(
                project["skills"],
                resource["skills"],
                project["primary_skills"],
                excluded_skills,
            )
            registry_gate_68_seconds += time.perf_counter() - gate_call_start
            legacy_value = legacy_result.to_dict()
            registry_value = shadow_result.to_dict()
            legacy_comparable = {
                "passed": legacy_value["passed"],
                "matched_skills": legacy_value["matched_skills"],
                "reason": legacy_value["reason"],
            }
            registry_comparable = {
                "passed": registry_value["passed"],
                "matched_skills": registry_value["matched_skills"],
                "reason": registry_value["reason"],
            }
            if legacy_comparable != registry_comparable:
                if not legacy_value["passed"] and registry_value["passed"]:
                    difference_type = "reject_to_pass"
                elif legacy_value["passed"] and not registry_value["passed"]:
                    difference_type = "pass_to_reject"
                else:
                    difference_type = "match_detail_changed"
                groups = tuple(
                    value
                    for value in registry_value["matched_skills"]
                    if value in registry_canonical_set
                )
                if not groups:
                    groups = canonical_groups_for_skill_diff(
                        snapshot,
                        legacy_value["matched_skills"],
                        registry_value["matched_skills"],
                    )
                expected, reason = expected_difference(
                    difference_type,
                    legacy_value["passed"],
                    registry_value["passed"],
                    groups,
                    improvement_groups,
                )
                differences.append(
                    _difference_record(
                        step="06-8",
                        difference_type=difference_type,
                        pair_key=key,
                        legacy_value=legacy_value,
                        registry_value=registry_value,
                        canonical_groups=groups,
                        expected=expected,
                        expected_reason=reason,
                        fingerprint=fingerprint,
                    )
                )
                gate_68_counts[difference_type] += 1
        gate_68_seconds = time.perf_counter() - gate_start

        required_module = _load_python_module(
            "skill_shadow_06_11",
            ROOT / "06-11_match_required_skills_list/00_tool/match_required_skills_list.py",
        )
        project_keywords = _read_map(
            ROOT
            / "03-51_extract_project_required_skills_list/01_result/extract_project_required_skills_list.jsonl"
        )
        normalized_skillsheets = _read_map(
            ROOT
            / "04-2_normalize_skillsheets_text/01_result/normalize_skillsheets_text.jsonl"
        )
        input_611 = _read_jsonl(
            ROOT / "06-10_match_location/01_result/matched_pairs_location.jsonl"
        )
        gate_611_counts = Counter()
        new_required_passes: List[Tuple[Tuple[str, str], dict]] = []
        required_start = time.perf_counter()
        legacy_gate_611_seconds = 0.0
        registry_gate_611_seconds = 0.0
        for pair in input_611:
            key = _pair_key(pair)
            project_record = project_keywords.get(key[0], {})
            required_skills = required_module.build_required_skill_keyword_set(project_record)
            strong_phases, weak_phases = required_module.build_required_phase_keywords(project_record)
            normalized = normalized_skillsheets.get(key[1], {})
            skillsheet_text = (
                normalized.get("skillsheet") or ""
                if normalized.get("success") is True
                else ""
            )
            email_text = body_map.get(key[1], {}).get("body_text") or ""
            required_call_start = time.perf_counter()
            legacy_result = required_module.judge_match(
                required_skills,
                strong_phases,
                weak_phases,
                skillsheet_text,
                email_text,
            )
            legacy_gate_611_seconds += time.perf_counter() - required_call_start
            required_call_start = time.perf_counter()
            shadow_result = judge_required_gate_legacy_substring(
                required_skills,
                strong_phases,
                weak_phases,
                skillsheet_text,
                email_text,
                snapshot,
                SKILL_ALIAS_MAP,
                improvement_groups,
            )
            registry_gate_611_seconds += time.perf_counter() - required_call_start
            legacy_value = _required_gate_value_from_result(
                required_skills,
                strong_phases,
                weak_phases,
                legacy_result,
            )
            registry_value = shadow_result.to_dict()
            comparable_registry = {
                "passed": registry_value["passed"],
                "matched_keywords": registry_value["matched_keywords"],
                "matched_phase_keywords": registry_value["matched_phase_keywords"],
                "matched_in": registry_value["matched_in"],
                "reason": registry_value["reason"],
            }
            if legacy_value != comparable_registry:
                if not legacy_value["passed"] and registry_value["passed"]:
                    difference_type = "reject_to_pass"
                elif legacy_value["passed"] and not registry_value["passed"]:
                    difference_type = "pass_to_reject"
                else:
                    difference_type = "match_detail_changed"
                groups = shadow_result.canonical_groups
                expected, reason = expected_difference(
                    difference_type,
                    legacy_value["passed"],
                    registry_value["passed"],
                    groups,
                    improvement_groups,
                )
                differences.append(
                    _difference_record(
                        step="06-11",
                        difference_type=difference_type,
                        pair_key=key,
                        legacy_value=legacy_value,
                        registry_value=registry_value,
                        canonical_groups=groups,
                        expected=expected,
                        expected_reason=reason,
                        fingerprint=fingerprint,
                    )
                )
                gate_611_counts[difference_type] += 1
                if difference_type == "reject_to_pass":
                    new_required_passes.append((key, registry_value))
        required_seconds = time.perf_counter() - required_start

        noise_module = _load_python_module(
            "skill_shadow_06_12",
            ROOT
            / "06-12_filter_required_skills_noise/00_tool/filter_required_skills_noise.py",
        )
        contract_map = _read_map(
            ROOT / "03-30_extract_project_contract_type/01_result/contract_type.jsonl"
        )
        final_pairs = {
            _pair_key(record)
            for record in _read_jsonl(
                ROOT / "06-30_match_contract_type/01_result/matched_pairs_contract_type.jsonl"
            )
        }
        final_projects = {key[0] for key in final_pairs}
        final_resources = {key[1] for key in final_pairs}
        downstream_added: List[Tuple[str, str]] = []
        for key, registry_value in new_required_passes:
            normalized = normalized_skillsheets.get(key[1], {})
            skillsheet_text = (
                normalized.get("skillsheet") or ""
                if normalized.get("success") is True
                else ""
            )
            email_text = body_map.get(key[1], {}).get("body_text") or ""
            combined = noise_module._normalized_text(skillsheet_text, email_text)
            project_record = project_keywords.get(key[0], {})
            kept = [
                keyword
                for keyword in registry_value["matched_keywords"]
                if noise_module.should_keep_keyword(keyword, project_record, combined)[0]
            ]
            contract_type = contract_map.get(key[0], {}).get("contract_type")
            if kept and contract_type in {"quasi_mandate", "outsourcing"}:
                downstream_added.append(key)

        after_hashes = _artifact_hashes()
        canonical_outputs_unchanged = baseline_hashes == after_hashes
        if not canonical_outputs_unchanged:
            raise RuntimeError("canonical pipeline artifacts changed during shadow run")

        differences.sort(key=lambda item: item["difference_id"])
        hit_records.sort(
            key=lambda item: (
                item["step"],
                item["message_id"],
                item["scope"],
                item["span"],
            )
        )
        _write_jsonl(stage / "shadow_differences.jsonl", differences)
        _write_jsonl(stage / "shadow_registry_hits.jsonl", hit_records)
        _write_json(stage / "ownership_overrides.json", [item.to_dict() for item in overrides])

        difference_counts = Counter(item["step"] for item in differences)
        expected_count = sum(item["expected"] is True for item in differences)
        unexpected_count = len(differences) - expected_count
        step_fingerprints = {
            step: fingerprint
            for step in ("03-8", "05-8", "03-8-primary", "06-8", "06-11")
        }
        legacy_full_seconds = (
            legacy_extraction_seconds
            + legacy_gate_68_seconds
            + legacy_gate_611_seconds
        )
        registry_full_seconds = (
            registry_extraction_seconds
            + registry_gate_68_seconds
            + registry_gate_611_seconds
        )
        summary = {
            "status": "succeeded" if unexpected_count == 0 else "unexpected_differences",
            "run_date": run_date,
            "run_id": run_id,
            "registry_group_count": len(snapshot.entries),
            "registry_canonicals": list(snapshot.canonical_names),
            "registry_ownership": snapshot.ownership_map,
            "production_canonicals": list(snapshot.production_canonical_names),
            "shadow_canonicals": list(snapshot.shadow_canonical_names),
            "registry_fingerprint": fingerprint,
            "registry_sha256": snapshot.registry_sha256,
            "implementation_identity": implementation_id,
            "implementation_file_hashes": implementation_file_hashes,
            "legacy_dictionary_sha256": project_dict_hash,
            "legacy_dictionaries_byte_identical": True,
            "input_counts": {
                "projects": len(project_ids),
                "resources": len(resource_ids),
                "06-8_pairs": len(input_68),
                "06-11_pairs": len(input_611),
            },
            "difference_counts": {
                "03-8": extraction_counts["03-8"],
                "05-8": extraction_counts["05-8"],
                "primary": extraction_counts["primary"],
                "06-8": difference_counts["06-8"],
                "06-11": difference_counts["06-11"],
            },
            "06-8_difference_types": dict(gate_68_counts),
            "06-11_difference_types": dict(gate_611_counts),
            "expected_differences": expected_count,
            "unexpected_differences": unexpected_count,
            "improvement_canonicals": list(improvement_groups),
            "downstream_estimate": {
                "new_final_candidate_pairs": len(downstream_added),
                "complete_unmatch_projects_resolved": len(
                    {key[0] for key in downstream_added if key[0] not in final_projects}
                ),
                "complete_unmatch_resources_resolved": len(
                    {key[1] for key in downstream_added if key[1] not in final_resources}
                ),
                "pair_ids": [_pair_id(key) for key in downstream_added],
            },
            "ownership_override_count": len(overrides),
            "registry_hit_count": len(hit_records),
            "step_fingerprints": step_fingerprints,
            "rollback": {
                "legacy_registry_independent": legacy_snapshot is None,
                "legacy_fingerprint": legacy_fingerprint,
                "shadow_output_is_separate": True,
                "canonical_outputs_unchanged": canonical_outputs_unchanged,
            },
            "performance_comparison": {
                "legacy": {
                    "matcher_compile_seconds": round(legacy_compile_seconds, 6),
                    "extraction_seconds": round(legacy_extraction_seconds, 6),
                    "06-8_seconds": round(legacy_gate_68_seconds, 6),
                    "06-11_seconds": round(legacy_gate_611_seconds, 6),
                    "full_processing_seconds": round(legacy_full_seconds, 6),
                    "peak_traced_memory_bytes": legacy_peak_memory_bytes,
                },
                "registry": {
                    "matcher_compile_seconds": round(registry_compile_seconds, 6),
                    "extraction_seconds": round(registry_extraction_seconds, 6),
                    "06-8_seconds": round(registry_gate_68_seconds, 6),
                    "06-11_seconds": round(registry_gate_611_seconds, 6),
                    "full_processing_seconds": round(registry_full_seconds, 6),
                    "peak_traced_memory_bytes": registry_peak_memory_bytes,
                },
                "full_processing_ratio": round(
                    registry_full_seconds / legacy_full_seconds, 6
                ) if legacy_full_seconds else None,
                "peak_memory_ratio": round(
                    registry_peak_memory_bytes / legacy_peak_memory_bytes, 6
                ) if legacy_peak_memory_bytes else None,
            },
            "timing_seconds": {
                "06-8_wall": round(gate_68_seconds, 6),
                "06-11_wall": round(required_seconds, 6),
                "total": round(time.perf_counter() - start, 6),
            },
            "artifact_hashes_before": baseline_hashes,
            "artifact_hashes_after": after_hashes,
        }
        _write_json(stage / "shadow_summary.json", summary)
        os.replace(str(stage), str(output_dir))
        return summary
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--registry", type=Path, default=REGISTRY_PATH)
    parser.add_argument("--runtime-context", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = run_shadow(
        args.run_date,
        args.run_id,
        args.output_dir,
        args.registry,
        runtime_context_path=args.runtime_context,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if summary["unexpected_differences"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

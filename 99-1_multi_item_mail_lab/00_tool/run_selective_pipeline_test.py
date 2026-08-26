#!/usr/bin/env python3
"""Run existing 01-4/02-1/04/05 logic for two 99-1 derived mails only."""

import contextlib
import hashlib
import importlib.util
import json
import re
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


sys.dont_write_bytecode = True

STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

IDENTITY_DIR = STEP_DIR / "00_tool" / "source_identity"
if str(IDENTITY_DIR) not in sys.path:
    sys.path.insert(0, str(IDENTITY_DIR))

from common.file_utils import ensure_result_dirs
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger
from identity import (
    artifact_set_fingerprint,
    attachment_fingerprint,
    canonical_subject,
    version_fingerprint,
)


logger = get_logger("99-1_selective_pipeline_test")
DERIVED_MASTER = STEP_DIR / "01_result" / "derived_mail_master.jsonl"
DERIVED_INPUT_IDS = STEP_DIR / "01_result" / "derived_input_ids.jsonl"
P1_AUDIT = STEP_DIR / "01_result" / "audit_items.jsonl"
P1_CONFIG = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "companies"
    / "netwisdom.config.json.example"
)
SELECTIVE_DIR = STEP_DIR / "01_result" / "selective_pipeline_test"

MODULE_PATHS = {
    "cleanup": PROJECT_ROOT / "01-4_cleanup_email_text/00_tool/cleanup_email_text.py",
    "classify": PROJECT_ROOT
    / "02-1_classify_type_project_resource/00_tool/classify_type_project_resource.py",
    "fetch_skillsheet": PROJECT_ROOT
    / "04-1_fetch_skillsheets_text/00_tool/fetch_skillsheets_text.py",
    "normalize_skillsheet": PROJECT_ROOT
    / "04-2_normalize_skillsheets_text/00_tool/normalize_skillsheets_text.py",
    "resource_budget": PROJECT_ROOT
    / "05-1_extract_resource_budget/00_tool/extract_resource_budget.py",
    "resource_age": PROJECT_ROOT
    / "05-2_extract_resource_age/00_tool/extract_resource_age.py",
    "resource_remote": PROJECT_ROOT
    / "05-3_extract_resource_remote/00_tool/extract_resource_remote.py",
    "resource_foreign": PROJECT_ROOT
    / "05-4_extract_resource_foreign/00_tool/extract_resource_foreign.py",
    "resource_freelance": PROJECT_ROOT
    / "05-5_extract_resource_freelance/00_tool/extract_resource_freelance.py",
    "resource_workload": PROJECT_ROOT
    / "05-6_extract_resource_workload/00_tool/extract_resource_workload.py",
    "resource_vendor": PROJECT_ROOT
    / "05-7_extract_resource_vendor_tiers/00_tool/extract_resource_vendor_tiers.py",
    "resource_skill": PROJECT_ROOT
    / "05-8_extract_resource_skill_category/00_tool/extract_resource_skill_category.py",
    "resource_phase": PROJECT_ROOT
    / "05-9_extract_resource_phase_category/00_tool/extract_resource_phase_category.py",
    "resource_location": PROJECT_ROOT
    / "05-10_extract_resource_location/00_tool/extract_resource_location.py",
}

PRODUCTION_STEP_PATTERNS = (
    "01-3_*",
    "01-4_*",
    "02-*",
    "03-*",
    "04-*",
    "05-*",
    "06-*",
    "07-1_*",
    "08-5_*",
    "09-*",
)
PRODUCTION_ARTIFACT_DIRS = {"01_result", "02_confirm", "99_execution_time"}

FIVE_OUTPUT_FILES = {
    "05-1_extract_resource_budget.jsonl": "resource_budget",
    "05-2_extract_resource_age.jsonl": "resource_age",
    "05-3_extract_resource_remote.jsonl": "resource_remote",
    "05-4_extract_resource_foreign.jsonl": "resource_foreign",
    "05-5_extract_resource_freelance.jsonl": "resource_freelance",
    "05-6_extract_resource_workload.jsonl": "resource_workload",
    "05-7_extract_resource_vendor_tiers.jsonl": "resource_vendor",
    "05-8_extract_resource_skill_category.jsonl": "resource_skill",
    "05-9_extract_resource_phase_category.jsonl": "resource_phase",
    "05-10_extract_resource_location.jsonl": "resource_location",
}

FIVE_REQUIRED_KEYS = {
    "resource_budget": {"message_id", "desired_unit_price", "desired_unit_price_sub_infor"},
    "resource_age": {"message_id", "current_age", "current_age_source", "current_age_raw"},
    "resource_remote": {"message_id", "remote_preference", "remote_source", "remote_raw"},
    "resource_foreign": {"message_id", "nationality", "nationality_source", "nationality_raw"},
    "resource_freelance": {
        "message_id",
        "employment_type",
        "employment_type_source",
        "employment_type_raw",
    },
    "resource_workload": {
        "message_id",
        "workload_min",
        "workload_max",
        "workload_max_source",
        "workload_raw",
    },
    "resource_vendor": {"message_id", "vendor_flow", "vendor_flow_raw"},
    "resource_skill": {"message_id", "skills", "skills_by_category", "skills_raw"},
    "resource_phase": {"message_id", "phases", "phases_raw"},
    "resource_location": {"message_id", "location", "location_raw", "location_source"},
}

FIVE_06_FIELD_TYPES = {
    "resource_budget": {"desired_unit_price": int},
    "resource_age": {"current_age": int},
    "resource_remote": {"remote_preference": str},
    "resource_foreign": {"nationality": str},
    "resource_freelance": {"employment_type": str},
    "resource_workload": {"workload_min": int, "workload_max": int},
    "resource_vendor": {"vendor_flow": int},
    "resource_skill": {"skills": list},
    "resource_phase": {"phases": list},
    "resource_location": {"location": str},
}

NORMALIZED_REQUIRED_KEYS = {
    "message_id",
    "success",
    "source",
    "urls",
    "skillsheet",
    "raw_char_count",
    "clean_char_count",
    "reduction_char_count",
    "reduction_rate",
    "cleanup_flags",
    "cleanup_stats",
    "source_step",
}

GENERIC_ATTACHMENT_MARKERS = {
    "スキルシート",
    "履歴書",
    "技術経歴書",
    "職務経歴書",
    "経歴書",
}


@contextlib.contextmanager
def _prepend_paths(paths: Iterable[Path]) -> Iterable[None]:
    original_path = list(sys.path)
    try:
        for path in reversed(list(paths)):
            sys.path.insert(0, str(path))
        yield
    finally:
        sys.path[:] = original_path


def _load_module(
    key: str,
    transient_import_names: Tuple[str, ...] = (),
) -> ModuleType:
    path = MODULE_PATHS[key]
    if not path.exists():
        raise FileNotFoundError(f"existing pipeline module not found: {path}")

    saved_modules = {
        name: sys.modules.pop(name)
        for name in transient_import_names
        if name in sys.modules
    }
    unique_name = "selective_" + key
    try:
        with _prepend_paths((path.parent,)):
            spec = importlib.util.spec_from_file_location(unique_name, path)
            if spec is None or spec.loader is None:
                raise ImportError(f"cannot load module: {path}")
            module = importlib.util.module_from_spec(spec)
            sys.modules[unique_name] = module
            spec.loader.exec_module(module)
            return module
    finally:
        for name in transient_import_names:
            sys.modules.pop(name, None)
        sys.modules.update(saved_modules)


def _load_existing_modules(
    keys: Optional[Iterable[str]] = None,
) -> Dict[str, ModuleType]:
    modules: Dict[str, ModuleType] = {}
    for key in (list(keys) if keys is not None else MODULE_PATHS):
        transient = ("config",) if key in {"classify", "resource_skill", "resource_phase"} else ()
        modules[key] = _load_module(key, transient)
    return modules


def _production_artifact_snapshot() -> Dict[str, Tuple[int, int]]:
    snapshot: Dict[str, Tuple[int, int]] = {}
    step_dirs: Set[Path] = set()
    for pattern in PRODUCTION_STEP_PATTERNS:
        step_dirs.update(path for path in PROJECT_ROOT.glob(pattern) if path.is_dir())
    for step_dir in sorted(step_dirs):
        for artifact_dir_name in PRODUCTION_ARTIFACT_DIRS:
            artifact_dir = step_dir / artifact_dir_name
            if not artifact_dir.exists():
                continue
            for path in artifact_dir.rglob("*"):
                if path.is_file():
                    stat = path.stat()
                    snapshot[str(path.relative_to(PROJECT_ROOT))] = (
                        stat.st_size,
                        stat.st_mtime_ns,
                    )
    return snapshot


def _index(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    for record in records:
        message_id = record.get("message_id")
        if not isinstance(message_id, str) or not message_id:
            raise ValueError("message_id is missing")
        if message_id in result:
            raise ValueError(f"duplicate message_id: {message_id}")
        result[message_id] = record
    return result


def _expected_attachment_fingerprints(
    audit_records: List[Dict[str, Any]],
    expected_ids: Set[str],
) -> Dict[str, List[str]]:
    expected: Dict[str, List[str]] = {}
    for message_id in expected_ids:
        artifact_sets = {
            tuple(
                sorted(
                    artifact.get("content_sha256")
                    for artifact in record.get("item_artifacts", [])
                    if artifact.get("artifact_kind") == "ATTACHMENT_FILE"
                )
            )
            for record in audit_records
            if record.get("derived_item_id") == message_id
        }
        if len(artifact_sets) != 1:
            raise ValueError(f"P1 attachment audit is inconsistent: {message_id}")
        fingerprints = list(next(iter(artifact_sets)))
        if any(not fingerprint for fingerprint in fingerprints):
            raise ValueError(f"P1 attachment fingerprint is missing: {message_id}")
        expected[message_id] = fingerprints
    return expected


def _exclusive_skillsheet_markers(
    master_records: List[Dict[str, Any]],
) -> Dict[str, Set[str]]:
    marker_sets: Dict[str, Set[str]] = {}
    for record in master_records:
        attachments = record.get("attachments") or []
        filename = str(attachments[0].get("filename", "")) if attachments else ""
        markers = {
            marker
            for marker in re.findall(r"[\u3400-\u9fff]+", Path(filename).stem)
            if marker not in GENERIC_ATTACHMENT_MARKERS
        }
        marker_sets[record["message_id"]] = markers

    exclusive: Dict[str, Set[str]] = {}
    for message_id, markers in marker_sets.items():
        other_markers: Set[str] = set()
        for other_id, other_values in marker_sets.items():
            if other_id != message_id:
                other_markers.update(other_values)
        exclusive[message_id] = markers - other_markers
        if not exclusive[message_id]:
            raise ValueError(f"no item-specific skillsheet marker: {message_id}")
    return exclusive


def _five_schema_error_count(
    results: Dict[str, List[Dict[str, Any]]],
) -> int:
    errors = 0
    for key, records in results.items():
        required_keys = FIVE_REQUIRED_KEYS[key]
        field_types = FIVE_06_FIELD_TYPES[key]
        for record in records:
            if not required_keys <= set(record):
                errors += 1
                continue
            if not isinstance(record.get("message_id"), str) or not record["message_id"]:
                errors += 1
                continue
            if any(
                not isinstance(record.get(field), expected_type)
                for field, expected_type in field_types.items()
            ):
                errors += 1
    return errors


def _exclusive_body_lines(master_records: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
    line_sets = {
        record["message_id"]: {
            line.strip()
            for line in str(record.get("body_text", "")).splitlines()
            if len(line.strip()) >= 4
        }
        for record in master_records
    }
    exclusive: Dict[str, Set[str]] = {}
    for message_id, lines in line_sets.items():
        other_lines: Set[str] = set()
        for other_id, other_set in line_sets.items():
            if other_id != message_id:
                other_lines.update(other_set)
        exclusive[message_id] = lines - other_lines
        if not exclusive[message_id]:
            raise ValueError(f"no item-specific body marker: {message_id}")
    return exclusive


def _build_success_cache_contract(
    master_records: List[Dict[str, Any]],
    audit_records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    with P1_CONFIG.open(encoding="utf-8") as file_object:
        config = json.load(file_object)
    audit_by_derived: Dict[str, Dict[str, Any]] = {}
    for audit in audit_records:
        derived_id = audit.get("derived_item_id")
        if derived_id and derived_id not in audit_by_derived:
            audit_by_derived[derived_id] = audit

    pairs = {(record.get("from"), record.get("subject")) for record in master_records}
    stable_count = 0
    changed_count = 0
    for record in master_records:
        audit = audit_by_derived.get(record["message_id"])
        if audit is None:
            raise ValueError(f"P1 audit join missing: {record['message_id']}")
        stable_subject = canonical_subject(
            config["canonical_subject_template"],
            audit["logical_item_id"],
            audit["version_fingerprint"],
        )
        if stable_subject != record["subject"]:
            raise ValueError(f"canonical subject mismatch: {record['message_id']}")
        stable_count += 1

        changed_attachment = "sha256:" + hashlib.sha256(
            (audit["attachment_fingerprint"] + "|selective-change").encode("ascii")
        ).hexdigest()
        changed_relations = [dict(relation) for relation in audit["item_artifacts"]]
        changed_relations[0]["content_sha256"] = changed_attachment
        changed_artifact_set = artifact_set_fingerprint(
            changed_relations, version_relevant_only=True
        )
        changed_version = version_fingerprint(
            audit["body_fingerprint"], changed_artifact_set
        )
        changed_subject = canonical_subject(
            config["canonical_subject_template"],
            audit["logical_item_id"],
            changed_version,
        )
        if changed_subject == stable_subject:
            raise ValueError(f"version subject did not change: {record['message_id']}")
        changed_count += 1

    return {
        "from_subject_collision": len(master_records) - len(pairs),
        "stable_subject_count": stable_count,
        "version_changed_subject_count": changed_count,
    }


def _build_five_results(
    modules: Dict[str, ModuleType],
    ordered_ids: List[str],
    master_by_id: Dict[str, Dict[str, Any]],
    cleanup_by_id: Dict[str, Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    results = {key: [] for key in FIVE_REQUIRED_KEYS}
    skill_module = modules["resource_skill"]
    phase_module = modules["resource_phase"]
    skill_dictionary = (
        skill_module.load_skill_dictionary(skill_module.SKILL_DICT_PATH)
        if skill_module.ENABLE_SKILL_CATEGORY
        else None
    )
    phase_dictionary = (
        phase_module.load_phase_dictionary(phase_module.PHASE_DICT_PATH)
        if phase_module.ENABLE_PHASE_CATEGORY
        else None
    )
    location_module = modules["resource_location"]
    location_entries = location_module.load_location_dictionary(
        location_module.DICT_PATH
    )
    location_paths = (
        PROJECT_ROOT / "05-10_extract_resource_location/00_tool",
        PROJECT_ROOT / "03-10_extract_project_location/00_tool",
    )

    for message_id in ordered_ids:
        master = master_by_id[message_id]
        body = cleanup_by_id[message_id]["body_text"]
        subject = str(master.get("subject", ""))
        from_field = str(master.get("from", ""))
        results["resource_budget"].append(
            modules["resource_budget"].build_record(message_id, body, subject)
        )
        for key in (
            "resource_age",
            "resource_remote",
            "resource_foreign",
            "resource_freelance",
            "resource_workload",
        ):
            results[key].append(modules[key].build_record(message_id, body))
        results["resource_vendor"].append(
            modules["resource_vendor"].build_record(message_id, from_field, body)
        )
        if skill_dictionary is None:
            skill_record = skill_module.build_passthrough_record(message_id)
        else:
            skill_record = skill_module.build_extracted_record(
                message_id, body, skill_dictionary
            )
        results["resource_skill"].append(skill_record)
        if phase_dictionary is None:
            phase_record = phase_module.build_passthrough_record(message_id)
        else:
            phase_record = phase_module.build_extracted_record(
                message_id, body, phase_dictionary
            )
        results["resource_phase"].append(phase_record)
        with _prepend_paths(location_paths):
            location_record = location_module.build_extracted_record(
                message_id, body, location_entries
            )
        results["resource_location"].append(location_record)

    return results


def build_selective_results(
    stop_after_classification: bool = False,
) -> Dict[str, Any]:
    for path in (DERIVED_MASTER, DERIVED_INPUT_IDS, P1_AUDIT):
        if not path.exists():
            raise FileNotFoundError(f"required P1 artifact not found: {path}")

    production_before = _production_artifact_snapshot()
    modules = _load_existing_modules(("cleanup", "classify"))
    if modules["classify"].USE_LLM_CLASSIFY:
        raise ValueError("02-1 LLM feature flag must be OFF for selective test")

    master_records = read_jsonl_as_list(str(DERIVED_MASTER))
    input_id_records = read_jsonl_as_list(str(DERIVED_INPUT_IDS))
    audit_records = read_jsonl_as_list(str(P1_AUDIT))
    if len(master_records) != len(input_id_records):
        raise ValueError("derived master and validated input cardinality differ")
    master_by_id = _index(master_records)
    ordered_ids = [record["message_id"] for record in input_id_records]
    expected_ids = set(ordered_ids)
    expected_count = len(ordered_ids)
    if len(set(ordered_ids)) != expected_count or set(ordered_ids) != set(master_by_id):
        raise ValueError("derived input IDs do not match derived mail master")
    if not all(message_id.startswith("mi_") for message_id in ordered_ids):
        raise ValueError("non-derived message_id found in selective input")
    original_ids = {
        record.get("original_message_id")
        for record in audit_records
        if record.get("original_message_id")
    }
    if expected_ids & original_ids:
        raise ValueError("derived and original Gmail identities overlap")
    expected_attachment_by_id = _expected_attachment_fingerprints(
        audit_records, expected_ids
    )
    skillsheet_markers = _exclusive_skillsheet_markers(master_records)

    exclusive_lines = _exclusive_body_lines(master_records)
    cleanup_module = modules["cleanup"]
    cleanup_rules = cleanup_module.load_cleanup_rules(cleanup_module.CLEANUP_RULES_PATH)
    cleanup_records: List[Dict[str, Any]] = []
    body_cross_contamination = 0
    profile_marker_retained = 0
    for message_id in ordered_ids:
        cleaned_body, _ = cleanup_module.cleanup_body(
            master_by_id[message_id]["body_text"], cleanup_rules
        )
        if not cleaned_body:
            raise ValueError(f"01-4 removed entire item body: {message_id}")
        own_markers = exclusive_lines[message_id]
        if any(marker in cleaned_body for marker in own_markers):
            profile_marker_retained += 1
        else:
            raise ValueError(f"01-4 removed all item-specific markers: {message_id}")
        for other_id in ordered_ids:
            if other_id == message_id:
                continue
            if any(marker in cleaned_body for marker in exclusive_lines[other_id]):
                body_cross_contamination += 1
        cleanup_records.append({"message_id": message_id, "body_text": cleaned_body})
    cleanup_by_id = _index(cleanup_records)

    classify_module = modules["classify"]
    keywords = classify_module.load_keywords(classify_module.KEYWORDS_PATH)
    classification_records: List[Dict[str, Any]] = []
    for message_id in ordered_ids:
        master = master_by_id[message_id]
        mail_type, _, _ = classify_module.rule_classify(
            str(master.get("subject", "")),
            cleanup_by_id[message_id]["body_text"],
            keywords,
            has_attachment=bool(master.get("attachments")),
        )
        classification_records.append(
            {"message_id": message_id, "mail_type": mail_type}
        )
    resource_records = [
        {"message_id": record["message_id"]}
        for record in classification_records
        if record["mail_type"] == "resource"
    ]
    project_records = [
        {"message_id": record["message_id"]}
        for record in classification_records
        if record["mail_type"] == "project"
    ]
    ambiguous_records = [
        {"message_id": record["message_id"]}
        for record in classification_records
        if record["mail_type"] == "ambiguous"
    ]
    unknown_records = [
        {"message_id": record["message_id"]}
        for record in classification_records
        if record["mail_type"] == "unknown"
    ]

    success_cache = _build_success_cache_contract(master_records, audit_records)
    attachment_identity_records: List[Dict[str, Any]] = []
    attachment_digests: Set[str] = set()
    correct_attachment_mapping = 0
    attachment_cross_contamination = 0
    for message_id in ordered_ids:
        attachments = master_by_id[message_id].get("attachments")
        if not isinstance(attachments, list):
            raise ValueError(f"derived attachments must be a list: {message_id}")
        attachment_values = [attachment_fingerprint(value) for value in attachments]
        mapping_correct = sorted(attachment_values) == sorted(
            expected_attachment_by_id[message_id]
        )
        if not mapping_correct:
            raise ValueError(f"P1 attachment mapping mismatch: {message_id}")
        correct_attachment_mapping += 1
        other_expected_digests = {
            digest
            for other_id, digests in expected_attachment_by_id.items()
            if other_id != message_id
            for digest in digests
        }
        attachment_cross_contamination += sum(
            digest in other_expected_digests for digest in attachment_values
        )
        attachment_digests.update(attachment_values)
        attachment_identity_records.append(
            {
                "message_id": message_id,
                "attachment_fingerprint": attachment_values[0]
                if len(attachment_values) == 1
                else "",
                "skillsheet_fingerprint": "",
                "attachment_count": len(attachment_values),
                "source": "derived_input",
                "mapping_correct": mapping_correct,
            }
        )

    if len(resource_records) != expected_count or project_records or ambiguous_records or unknown_records:
        production_after = _production_artifact_snapshot()
        production_write = int(production_before != production_after)
        if production_write:
            raise ValueError("production artifact changed during selective test")
        report = {
            "result": "FAIL",
            "blocking_stage": "02-1",
            "blocking_reason": f"expected resource={expected_count}",
            "derived_input": len(ordered_ids),
            "cleanup_output": len(cleanup_records),
            "classification_output": len(classification_records),
            "resource_output": len(resource_records),
            "project_classified": len(project_records),
            "ambiguous_classified": len(ambiguous_records),
            "unknown_classified": len(unknown_records),
            "project_route_output": len(project_records),
            "resource_03_bypass_output": len(resource_records),
            "skillsheet_output": 0,
            "normalized_skillsheet_output": 0,
            "five_step_count": 0,
            "five_records_per_step": 0,
            "message_id_continuity": True,
            "join_missing": 0,
            "duplicate_ids": 0,
            "body_cross_contamination": body_cross_contamination,
            "attachment_cross_contamination": attachment_cross_contamination,
            "correct_attachment_mapping": correct_attachment_mapping,
            "attachment_missing": 0,
            "duplicate_attachment_mapping": 0,
            "profile_marker_retained": profile_marker_retained,
            "attachment_identity_distinct": len(attachment_digests),
            "skillsheet_identity_distinct": 0,
            "from_subject_collision": success_cache["from_subject_collision"],
            "success_cache_stable": success_cache["stable_subject_count"] == expected_count,
            "success_cache_version_subject_change": (
                success_cache["version_changed_subject_count"] == expected_count
            ),
            "contract_06_ready": False,
            "selective_03_04_05_contract_completed": False,
            "project_steps_03_executed": False,
            "steps_06_plus_executed": False,
            "llm_api_calls": 0,
            "external_url_calls": 0,
            "production_changes": 0,
            "production_write": production_write,
        }
        return {
            "derived_input": master_records,
            "cleanup": cleanup_records,
            "classification": classification_records,
            "project_route": project_records,
            "resource_03_bypass": resource_records,
            "resource_route": resource_records,
            "fetch_skillsheet": [],
            "normalize_skillsheet": [],
            "attachment_identity": attachment_identity_records,
            "five_results": {key: [] for key in FIVE_REQUIRED_KEYS},
            "report": report,
        }

    if stop_after_classification:
        production_after = _production_artifact_snapshot()
        production_write = int(production_before != production_after)
        if production_write:
            raise ValueError("production artifact changed during selective test")
        report = {
            "result": "PASS",
            "blocking_stage": "",
            "blocking_reason": "",
            "derived_input": len(ordered_ids),
            "cleanup_output": len(cleanup_records),
            "classification_output": len(classification_records),
            "resource_output": len(resource_records),
            "project_classified": len(project_records),
            "ambiguous_classified": len(ambiguous_records),
            "unknown_classified": len(unknown_records),
            "project_route_output": 0,
            "resource_03_bypass_output": len(resource_records),
            "skillsheet_output": 0,
            "normalized_skillsheet_output": 0,
            "five_step_count": 0,
            "five_records_per_step": 0,
            "message_id_continuity": True,
            "join_missing": 0,
            "duplicate_ids": 0,
            "body_cross_contamination": body_cross_contamination,
            "attachment_cross_contamination": attachment_cross_contamination,
            "correct_attachment_mapping": correct_attachment_mapping,
            "attachment_missing": 0,
            "duplicate_attachment_mapping": 0,
            "profile_marker_retained": profile_marker_retained,
            "attachment_identity_distinct": len(attachment_digests),
            "skillsheet_identity_distinct": 0,
            "from_subject_collision": success_cache["from_subject_collision"],
            "success_cache_stable": success_cache["stable_subject_count"] == expected_count,
            "success_cache_version_subject_change": (
                success_cache["version_changed_subject_count"] == expected_count
            ),
            "contract_06_ready": False,
            "selective_03_04_05_contract_completed": False,
            "project_steps_03_executed": False,
            "steps_06_plus_executed": False,
            "llm_api_calls": 0,
            "external_url_calls": 0,
            "production_changes": 0,
            "production_write": production_write,
        }
        return {
            "derived_input": master_records,
            "cleanup": cleanup_records,
            "classification": classification_records,
            "project_route": [],
            "resource_03_bypass": resource_records,
            "resource_route": resource_records,
            "fetch_skillsheet": [],
            "normalize_skillsheet": [],
            "attachment_identity": attachment_identity_records,
            "five_results": {key: [] for key in FIVE_REQUIRED_KEYS},
            "report": report,
        }

    downstream_keys = tuple(
        key for key in MODULE_PATHS if key not in {"cleanup", "classify"}
    )
    modules.update(_load_existing_modules(downstream_keys))

    fetch_module = modules["fetch_skillsheet"]
    normalize_module = modules["normalize_skillsheet"]
    fetch_records: List[Dict[str, Any]] = []
    normalized_records: List[Dict[str, Any]] = []
    attachment_identity_records = []
    attachment_digests = set()
    skillsheet_digests: Set[str] = set()
    skillsheet_content_mapping = 0
    skillsheet_cross_contamination = 0

    def _forbid_url_path(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("external URL path is forbidden in selective replay")

    original_url_builder = fetch_module.build_url_candidates
    fetch_module.build_url_candidates = _forbid_url_path
    try:
        for message_id in ordered_ids:
            attachments = master_by_id[message_id].get("attachments")
            if not isinstance(attachments, list):
                raise ValueError(f"derived attachments must be a list: {message_id}")
            expected_artifacts = expected_attachment_by_id[message_id]
            if len(attachments) != len(expected_artifacts):
                raise ValueError(f"P1 exact artifact count mismatch: {message_id}")
            if len(attachments) != 1:
                raise ValueError(
                    f"P1 downstream compatibility requires its validated single artifact: {message_id}"
                )
            attachment = attachments[0]
            fetch_record = fetch_module.fetch_skillsheet(
                message_id,
                master_by_id[message_id],
                cleanup_by_id[message_id],
            )
            if (
                fetch_record.get("success") is not True
                or fetch_record.get("source") != "attachment"
                or fetch_record.get("urls") is not False
            ):
                raise ValueError(f"04-1 attachment-only fetch failed: {message_id}")
            skillsheet_text = str(fetch_record.get("skillsheet", ""))
            own_marker_found = any(
                marker in skillsheet_text for marker in skillsheet_markers[message_id]
            )
            foreign_marker_found = False
            for other_id in ordered_ids:
                if other_id == message_id:
                    continue
                if any(
                    marker in skillsheet_text
                    for marker in skillsheet_markers[other_id]
                ):
                    foreign_marker_found = True
            if not own_marker_found or foreign_marker_found:
                raise ValueError(f"04 skillsheet item contamination: {message_id}")
            skillsheet_content_mapping += 1
            skillsheet_cross_contamination += int(foreign_marker_found)
            fetch_records.append(fetch_record)
            normalized_record = normalize_module.build_record(fetch_record)
            if not any(
                marker in str(normalized_record.get("skillsheet", ""))
                for marker in skillsheet_markers[message_id]
            ):
                raise ValueError(f"04-2 removed item identity marker: {message_id}")
            normalized_records.append(normalized_record)
            attachment_digest = attachment_fingerprint(attachment)
            skillsheet_digest = "sha256:" + hashlib.sha256(
                skillsheet_text.encode("utf-8")
            ).hexdigest()
            attachment_digests.add(attachment_digest)
            skillsheet_digests.add(skillsheet_digest)
            attachment_identity_records.append(
                {
                    "message_id": message_id,
                    "attachment_fingerprint": attachment_digest,
                    "expected_attachment_fingerprint": expected_attachment_by_id[message_id][0],
                    "skillsheet_fingerprint": skillsheet_digest,
                    "attachment_count": 1,
                    "source": "attachment",
                    "mapping_correct": attachment_digest
                    == expected_attachment_by_id[message_id][0],
                    "own_content_marker_found": own_marker_found,
                    "foreign_content_marker_found": foreign_marker_found,
                }
            )
    finally:
        fetch_module.build_url_candidates = original_url_builder
    if correct_attachment_mapping != expected_count:
        raise ValueError("P1 attachment mapping count is incomplete")

    five_results = _build_five_results(
        modules, ordered_ids, master_by_id, cleanup_by_id
    )
    for key, records in five_results.items():
        if len(records) != expected_count or {record.get("message_id") for record in records} != expected_ids:
            raise ValueError(f"05 join failure: {key}")
        if not all(FIVE_REQUIRED_KEYS[key] <= set(record) for record in records):
            raise ValueError(f"05 schema failure: {key}")

    five_schema_errors = _five_schema_error_count(five_results)
    if five_schema_errors:
        raise ValueError(f"05/06 schema type failure: {five_schema_errors}")
    normalized_schema_errors = sum(
        int(
            not NORMALIZED_REQUIRED_KEYS <= set(record)
            or record.get("success") is not True
            or not isinstance(record.get("skillsheet"), str)
            or not record.get("skillsheet")
        )
        for record in normalized_records
    )
    resource_text_schema_errors = sum(
        int(
            not isinstance(record.get("message_id"), str)
            or not isinstance(record.get("body_text"), str)
            or not record.get("body_text")
        )
        for record in cleanup_records
    )
    if normalized_schema_errors or resource_text_schema_errors:
        raise ValueError("04/06 resource text or skillsheet schema failure")

    all_stage_records = [cleanup_records, classification_records, fetch_records, normalized_records]
    all_stage_records.extend(five_results.values())
    join_missing = sum(
        len(expected_ids - {record.get("message_id") for record in records})
        for records in all_stage_records
    )
    duplicate_ids = sum(
        len(records) - len({record.get("message_id") for record in records})
        for records in all_stage_records
    )
    joined_item_ids = set(expected_ids)
    for records in (fetch_records, normalized_records, *five_results.values()):
        joined_item_ids &= {record.get("message_id") for record in records}
    original_id_join_key_uses = sum(
        record.get("message_id") in original_ids
        for records in all_stage_records
        for record in records
    )
    production_after = _production_artifact_snapshot()
    production_write = int(production_before != production_after)
    if production_write:
        raise ValueError("production artifact changed during selective test")

    report = {
        "result": "PASS",
        "blocking_stage": "",
        "blocking_reason": "",
        "derived_input": len(ordered_ids),
        "cleanup_output": len(cleanup_records),
        "classification_output": len(classification_records),
        "resource_output": len(resource_records),
        "project_classified": len(project_records),
        "ambiguous_classified": len(ambiguous_records),
        "unknown_classified": len(unknown_records),
        "project_route_output": len(project_records),
        "resource_03_bypass_output": len(resource_records),
        "skillsheet_output": len(fetch_records),
        "normalized_skillsheet_output": len(normalized_records),
        "five_step_count": len(five_results),
        "five_records_per_step": expected_count,
        "five_joined_items": len(joined_item_ids),
        "skillsheet_five_joined_items": len(joined_item_ids),
        "five_schema_errors": five_schema_errors,
        "normalized_schema_errors": normalized_schema_errors,
        "resource_text_schema_errors": resource_text_schema_errors,
        "schema_compatibility": (
            five_schema_errors == 0
            and normalized_schema_errors == 0
            and resource_text_schema_errors == 0
        ),
        "message_id_continuity": join_missing == 0 and duplicate_ids == 0,
        "join_missing": join_missing,
        "duplicate_ids": duplicate_ids,
        "body_cross_contamination": body_cross_contamination,
        "attachment_cross_contamination": attachment_cross_contamination,
        "skillsheet_cross_contamination": skillsheet_cross_contamination,
        "correct_attachment_mapping": correct_attachment_mapping,
        "skillsheet_content_mapping": skillsheet_content_mapping,
        "attachment_missing": 0,
        "duplicate_attachment_mapping": 0,
        "profile_marker_retained": profile_marker_retained,
        "attachment_identity_distinct": len(attachment_digests),
        "skillsheet_identity_distinct": len(skillsheet_digests),
        "from_subject_collision": success_cache["from_subject_collision"],
        "success_cache_stable": success_cache["stable_subject_count"] == expected_count,
        "success_cache_version_subject_change": (
            success_cache["version_changed_subject_count"] == expected_count
        ),
        "contract_06_ready": True,
        "selective_03_04_05_contract_completed": True,
        "project_steps_03_executed": False,
        "resource_steps_04_05_executed": True,
        "steps_06_plus_executed": False,
        "original_id_join_key_uses": original_id_join_key_uses,
        "llm_api_calls": 0,
        "external_url_calls": 0,
        "production_changes": 0,
        "production_write": production_write,
    }
    if (
        body_cross_contamination
        or skillsheet_cross_contamination
        or join_missing
        or duplicate_ids
        or original_id_join_key_uses
    ):
        raise ValueError("selective pipeline contract failed")

    return {
        "derived_input": master_records,
        "cleanup": cleanup_records,
        "classification": classification_records,
        "project_route": project_records,
        "resource_03_bypass": resource_records,
        "resource_route": resource_records,
        "fetch_skillsheet": fetch_records,
        "normalize_skillsheet": normalized_records,
        "attachment_identity": attachment_identity_records,
        "five_results": five_results,
        "report": report,
    }


def write_selective_results(results: Dict[str, Any]) -> None:
    dirs = ensure_result_dirs(str(STEP_DIR))
    result_dir = dirs["result"] / "selective_pipeline_test"
    result_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(str(result_dir / "derived_input.jsonl"), results["derived_input"])
    write_jsonl(str(result_dir / "01-4_cleanup.jsonl"), results["cleanup"])
    write_jsonl(str(result_dir / "02-1_classification.jsonl"), results["classification"])
    write_jsonl(str(result_dir / "03_project_input.jsonl"), results["project_route"])
    write_jsonl(str(result_dir / "03_resource_bypass.jsonl"), results["resource_03_bypass"])
    write_jsonl(str(result_dir / "05_resource_input.jsonl"), results["resource_route"])
    write_jsonl(str(result_dir / "04-1_fetch_skillsheets_text.jsonl"), results["fetch_skillsheet"])
    write_jsonl(str(result_dir / "04-2_normalize_skillsheets_text.jsonl"), results["normalize_skillsheet"])
    write_jsonl(str(result_dir / "04_attachment_identity.jsonl"), results["attachment_identity"])
    for filename, key in FIVE_OUTPUT_FILES.items():
        write_jsonl(str(result_dir / filename), results["five_results"][key])
    write_jsonl(str(result_dir / "contract_report.jsonl"), [results["report"]])


def main() -> None:
    results = build_selective_results()
    write_selective_results(results)
    report = results["report"]
    if report["result"] != "PASS":
        logger.error(
            "selective pipeline test HOLD: "
            f"stage={report['blocking_stage']} "
            f"resource={report['resource_output']} "
            f"project={report['project_classified']} "
            f"ambiguous={report['ambiguous_classified']}"
        )
        raise SystemExit(1)
    logger.ok(
        "selective pipeline test OK: "
        f"derived={report['derived_input']} cleanup={report['cleanup_output']} "
        f"resource={report['resource_output']} skillsheet={report['skillsheet_output']} "
        f"05_steps={report['five_step_count']}"
    )


if __name__ == "__main__":
    main()

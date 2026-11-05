#!/usr/bin/env python3
"""Confirm a five-group skill registry M0 shadow result directory."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.skill_registry import implementation_identity, load_registry
from common.skill_registry_runtime import production_implementation_paths


EXPECTED_CANONICALS = (
    "Spring Boot",
    "Node.js",
    "SQL Server",
    "Amazon Aurora",
    "Power BI",
)
EXPECTED_PRODUCTION_CANONICALS = ("Power BI",)
EXPECTED_SHADOW_CANONICALS = (
    "Spring Boot",
    "Node.js",
    "SQL Server",
    "Amazon Aurora",
)
EXPECTED_IMPLEMENTATION_FILES = (
    ROOT / "common/skill_canonical_map.py",
    ROOT / "common/skill_registry.py",
    ROOT / "common/skill_registry_shadow.py",
    ROOT / "03-8_extract_project_skill_category/00_tool/extract_project_skill_category.py",
    ROOT / "03-8_extract_project_skill_category/00_tool/skill_dictionary.txt",
    ROOT / "05-8_extract_resource_skill_category/00_tool/skill_dictionary.txt",
    ROOT / "06-8_match_skill_category/00_tool/config.py",
    ROOT / "06-11_match_required_skills_list/00_tool/match_required_skills_list.py",
    ROOT / "06-12_filter_required_skills_noise/00_tool/filter_required_skills_noise.py",
    ROOT / "00_pipeline/10_assistance_tool/run_skill_registry_shadow.py",
)
REQUIRED_FILES = (
    "execution_context.json",
    "registry_snapshot.yaml",
    "shadow_differences.jsonl",
    "shadow_registry_hits.jsonl",
    "ownership_overrides.json",
    "shadow_summary.json",
)
DEFAULT_EXPECTED_DIFFERENCES = (
    ROOT
    / "00_pipeline/10_assistance_tool/skill_registry_expected_differences_20261104.json"
)


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path):
    values = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise AssertionError("invalid JSONL {}:{}: {}".format(path, line_number, exc))
            assert isinstance(value, dict), "non-object JSONL {}:{}".format(path, line_number)
            values.append(value)
    return values


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _check(condition: bool, message: str, errors: list) -> None:
    if not condition:
        errors.append(message)


def confirm(args: argparse.Namespace) -> dict:
    result_dir = args.result_dir.resolve()
    errors = []
    for name in REQUIRED_FILES:
        _check((result_dir / name).is_file(), "missing {}".format(name), errors)
    if errors:
        return {"status": "failed", "errors": errors}

    summary = _load_json(result_dir / "shadow_summary.json")
    context = _load_json(result_dir / "execution_context.json")
    differences = _load_jsonl(result_dir / "shadow_differences.jsonl")
    hits = _load_jsonl(result_dir / "shadow_registry_hits.jsonl")
    snapshot = load_registry(result_dir / "registry_snapshot.yaml")
    expected_baseline = _load_json(args.expected_differences)

    _check(summary.get("status") == "succeeded", "summary status is not succeeded", errors)
    _check(summary.get("run_date") == args.run_date, "summary RUN_DATE mismatch", errors)
    _check(summary.get("run_id") == args.run_id, "summary RUN_ID mismatch", errors)
    _check(context.get("run_date") == args.run_date, "context RUN_DATE mismatch", errors)
    _check(context.get("run_id") == args.run_id, "context RUN_ID mismatch", errors)
    _check(context.get("skill_registry_mode") == args.expected_mode,
           "skill registry mode mismatch", errors)
    _check(tuple(snapshot.canonical_names) == EXPECTED_CANONICALS, "snapshot groups mismatch", errors)
    _check(summary.get("registry_group_count") == 5, "registry group count is not 5", errors)
    _check(tuple(summary.get("registry_canonicals") or []) == EXPECTED_CANONICALS,
           "summary canonicals mismatch", errors)
    _check(tuple(summary.get("production_canonicals") or []) == EXPECTED_PRODUCTION_CANONICALS,
           "summary production ownership mismatch", errors)
    _check(tuple(summary.get("shadow_canonicals") or []) == EXPECTED_SHADOW_CANONICALS,
           "summary shadow ownership mismatch", errors)
    _check(context.get("registry_ownership") == snapshot.ownership_map,
           "context registry ownership mismatch", errors)
    _check(tuple(context.get("production_canonicals") or []) == EXPECTED_PRODUCTION_CANONICALS,
           "context production ownership mismatch", errors)
    _check(tuple(context.get("shadow_canonicals") or []) == EXPECTED_SHADOW_CANONICALS,
           "context shadow ownership mismatch", errors)
    _check(snapshot.registry_sha256 == summary.get("registry_sha256"),
           "snapshot sha256 mismatch", errors)
    _check(context.get("registry_sha256") == summary.get("registry_sha256"),
           "context registry sha256 mismatch", errors)

    fingerprint = summary.get("registry_fingerprint")
    _check(isinstance(fingerprint, str) and len(fingerprint) == 64,
           "invalid registry fingerprint", errors)
    _check(context.get("registry_fingerprint") == fingerprint,
           "context fingerprint mismatch", errors)
    _check(set((summary.get("step_fingerprints") or {}).values()) == {fingerprint},
           "steps did not use one fingerprint", errors)
    implementation_files = (
        production_implementation_paths(ROOT)
        if args.expected_mode == "registry_overlay"
        else EXPECTED_IMPLEMENTATION_FILES
    )
    expected_implementation_hashes = {
        str(path.resolve().relative_to(ROOT)): _sha256(path)
        for path in implementation_files
    }
    _check(summary.get("implementation_file_hashes") == expected_implementation_hashes,
           "summary implementation dependency hashes mismatch", errors)
    _check(context.get("implementation_file_hashes") == expected_implementation_hashes,
           "context implementation dependency hashes mismatch", errors)
    expected_implementation_identity = implementation_identity(implementation_files)
    _check(summary.get("implementation_identity") == expected_implementation_identity,
           "summary implementation identity mismatch", errors)
    _check(context.get("implementation_identity") == expected_implementation_identity,
           "context implementation identity mismatch", errors)

    counts = summary.get("input_counts") or {}
    _check(counts.get("projects") == args.project_count, "project input count mismatch", errors)
    _check(counts.get("resources") == args.resource_count, "resource input count mismatch", errors)
    _check(summary.get("unexpected_differences") == 0, "unexpected differences are not zero", errors)
    _check(summary.get("expected_differences") == len(differences),
           "difference line count mismatch", errors)
    _check(all(item.get("expected") is True for item in differences),
           "manifest contains unexpected difference", errors)
    _check(summary.get("improvement_canonicals") == ["Power BI"],
           "improvement canonical is not Power BI only", errors)
    _check(all(item.get("canonical_groups") == ["Power BI"] for item in differences),
           "existing four-group or unattributed difference found", errors)
    _check(expected_baseline.get("run_date") == args.run_date,
           "expected difference baseline RUN_DATE mismatch", errors)
    _check(expected_baseline.get("run_id") == args.run_id,
           "expected difference baseline RUN_ID mismatch", errors)
    _check(expected_baseline.get("canonical_groups") == ["Power BI"],
           "expected difference baseline canonical group mismatch", errors)
    _check(
        set(expected_baseline.get("difference_ids") or [])
        == {item.get("difference_id") for item in differences},
        "difference ID set does not match approved shadow baseline",
        errors,
    )

    required_difference_fields = {
        "difference_id", "message_id", "pair_id", "step", "legacy_value",
        "registry_value", "difference_type", "expected", "canonical_groups",
        "registry_fingerprint",
    }
    difference_ids = []
    for item in differences:
        _check(required_difference_fields.issubset(item),
               "difference schema missing field", errors)
        difference_ids.append(item.get("difference_id"))
        _check(item.get("registry_fingerprint") == fingerprint,
               "difference fingerprint mismatch", errors)
        _check(bool(item.get("message_id") or item.get("pair_id")),
               "difference has no identity", errors)
    _check(len(difference_ids) == len(set(difference_ids)), "duplicate difference_id", errors)

    for item in hits:
        span = item.get("span")
        _check(item.get("registry_fingerprint") == fingerprint,
               "hit fingerprint mismatch", errors)
        ownership = item.get("ownership")
        _check(ownership in {"production", "shadow"}, "invalid hit ownership", errors)
        allowed = (
            EXPECTED_PRODUCTION_CANONICALS
            if ownership == "production"
            else EXPECTED_SHADOW_CANONICALS
        )
        _check(item.get("canonical") in allowed,
               "hit canonical does not match ownership", errors)
        _check(
            isinstance(span, list) and len(span) == 2
            and all(isinstance(value, int) for value in span) and span[0] < span[1],
            "invalid hit span",
            errors,
        )
        _check(bool(item.get("matched_surface")), "empty matched_surface", errors)

    rollback = summary.get("rollback") or {}
    _check(rollback.get("legacy_registry_independent") is True,
           "legacy mode depends on registry", errors)
    _check(rollback.get("shadow_output_is_separate") is True,
           "shadow output separation failed", errors)
    _check(rollback.get("canonical_outputs_unchanged") is True,
           "canonical output changed", errors)
    _check(summary.get("artifact_hashes_before") == summary.get("artifact_hashes_after"),
           "canonical artifact hashes changed", errors)
    _check(summary.get("legacy_dictionaries_byte_identical") is True,
           "03-8/05-8 dictionaries differ", errors)
    _check((summary.get("06-8_difference_types") or {}).get("pass_to_reject", 0) == 0,
           "06-8 pass_to_reject found", errors)
    _check((summary.get("06-11_difference_types") or {}).get("pass_to_reject", 0) == 0,
           "06-11 pass_to_reject found", errors)

    performance = summary.get("performance_comparison") or {}
    for mode in ("legacy", "registry"):
        values = performance.get(mode) or {}
        _check(values.get("matcher_compile_seconds", -1) >= 0,
               "{} compile timing missing".format(mode), errors)
        _check(values.get("full_processing_seconds", 0) > 0,
               "{} processing timing missing".format(mode), errors)
        _check(values.get("peak_traced_memory_bytes", 0) > 0,
               "{} peak memory missing".format(mode), errors)

    actual_step_counts = Counter(item.get("step") for item in differences)
    expected_step_counts = summary.get("difference_counts") or {}
    _check(actual_step_counts.get("03-8", 0) == expected_step_counts.get("03-8", 0),
           "03-8 difference count mismatch", errors)
    _check(actual_step_counts.get("05-8", 0) == expected_step_counts.get("05-8", 0),
           "05-8 difference count mismatch", errors)
    _check(actual_step_counts.get("03-8-primary", 0) == expected_step_counts.get("primary", 0),
           "primary difference count mismatch", errors)
    _check(actual_step_counts.get("06-8", 0) == expected_step_counts.get("06-8", 0),
           "06-8 difference count mismatch", errors)
    _check(actual_step_counts.get("06-11", 0) == expected_step_counts.get("06-11", 0),
           "06-11 difference count mismatch", errors)

    return {
        "status": "failed" if errors else "confirmed",
        "errors": errors,
        "run_date": args.run_date,
        "run_id": args.run_id,
        "registry_group_count": summary.get("registry_group_count"),
        "input_counts": counts,
        "difference_counts": expected_step_counts,
        "expected_differences": summary.get("expected_differences"),
        "unexpected_differences": summary.get("unexpected_differences"),
        "registry_fingerprint": fingerprint,
        "canonical_outputs_unchanged": rollback.get("canonical_outputs_unchanged"),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-dir", required=True, type=Path)
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--project-count", type=int, default=1011)
    parser.add_argument("--resource-count", type=int, default=2185)
    parser.add_argument(
        "--expected-differences",
        type=Path,
        default=DEFAULT_EXPECTED_DIFFERENCES,
    )
    parser.add_argument(
        "--expected-mode",
        choices=("shadow", "registry_overlay"),
        default="shadow",
    )
    return parser.parse_args()


def main() -> int:
    result = confirm(parse_args())
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] == "confirmed" else 1


if __name__ == "__main__":
    raise SystemExit(main())

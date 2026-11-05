"""Run-scoped immutable context for the production skill registry overlay."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional, Sequence, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.skill_registry import (
    RegistrySnapshot,
    SkillRegistryError,
    execution_fingerprint,
    implementation_identity,
    load_registry,
    load_registry_for_mode,
)
from common.skill_registry_config import (
    REGISTRY_GROUP_COUNT,
    REQUIRED_SEARCH_MODE,
    SKILL_REGISTRY_CONTEXT_ENV,
    configured_mode,
)


VALID_PRODUCTION_MODES = frozenset({"legacy", "registry_overlay"})
CONTEXT_FILE_NAME = "execution_context.json"
SNAPSHOT_FILE_NAME = "registry_snapshot.yaml"


def production_implementation_paths(root: Path) -> Tuple[Path, ...]:
    root = Path(root).resolve()
    return (
        root / "common/skill_canonical_map.py",
        root / "common/skill_registry.py",
        root / "common/skill_registry_config.py",
        root / "common/skill_registry_runtime.py",
        root / "common/skill_registry_shadow.py",
        root / "03-8_extract_project_skill_category/00_tool/extract_project_skill_category.py",
        root / "03-8_extract_project_skill_category/00_tool/skill_dictionary.txt",
        root / "05-8_extract_resource_skill_category/00_tool/extract_resource_skill_category.py",
        root / "05-8_extract_resource_skill_category/00_tool/skill_dictionary.txt",
        root / "06-8_match_skill_category/00_tool/config.py",
        root / "06-8_match_skill_category/00_tool/match_skill_category.py",
        root / "06-9_match_phase_category/00_tool/match_phase_category.py",
        root / "06-10_match_location/00_tool/match_location.py",
        root / "06-11_match_required_skills_list/00_tool/match_required_skills_list.py",
        root / "06-12_filter_required_skills_noise/00_tool/filter_required_skills_noise.py",
        root / "06-30_match_contract_type/00_tool/match_contract_type.py",
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _dependency_hashes(root: Path, paths: Iterable[Path]) -> Dict[str, str]:
    root = Path(root).resolve()
    return {
        str(Path(path).resolve().relative_to(root)): _sha256(Path(path))
        for path in paths
    }


def _read_json_object(path: Path) -> dict:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise SkillRegistryError("runtime context must be a JSON object")
    return value


@dataclass(frozen=True)
class SkillRegistryRuntime:
    mode: str
    run_date: str
    run_id: str
    snapshot: Optional[RegistrySnapshot]
    registry_fingerprint: str
    implementation_identity: str
    context_path: Path

    @property
    def overlay_enabled(self) -> bool:
        return self.mode == "registry_overlay"


def _validate_mode(mode: str) -> str:
    if mode not in VALID_PRODUCTION_MODES:
        raise SkillRegistryError("invalid production skill registry mode: {}".format(mode))
    return mode


def _context_payload(
    root: Path,
    run_date: str,
    run_id: str,
    mode: str,
    snapshot: Optional[RegistrySnapshot],
    snapshot_name: Optional[str],
) -> dict:
    implementation_paths = production_implementation_paths(root)
    implementation_id = implementation_identity(implementation_paths)
    fingerprint = execution_fingerprint(
        snapshot,
        mode=mode,
        required_search_mode=REQUIRED_SEARCH_MODE,
        implementation_id=implementation_id,
    )
    return {
        "context_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "run_date": run_date,
        "run_id": run_id,
        "skill_registry_mode": mode,
        "required_search_mode": REQUIRED_SEARCH_MODE,
        "schema_version": snapshot.schema_version if snapshot else None,
        "registry_sha256": snapshot.registry_sha256 if snapshot else None,
        "registry_fingerprint": fingerprint,
        "implementation_identity": implementation_id,
        "implementation_file_hashes": _dependency_hashes(root, implementation_paths),
        "registry_snapshot": snapshot_name,
        "registry_canonicals": list(snapshot.canonical_names) if snapshot else [],
        "registry_ownership": snapshot.ownership_map if snapshot else {},
        "production_canonicals": list(snapshot.production_canonical_names) if snapshot else [],
        "shadow_canonicals": list(snapshot.shadow_canonical_names) if snapshot else [],
    }


def create_runtime_context(
    root: Path,
    output_root: Path,
    run_date: str,
    run_id: str,
    mode: Optional[str] = None,
) -> Path:
    """Create or validate a run context atomically and return its JSON path."""
    root = Path(root).resolve()
    mode = _validate_mode(mode or configured_mode())
    destination = Path(output_root).resolve() / run_date / run_id
    context_path = destination / CONTEXT_FILE_NAME
    if context_path.is_file():
        load_runtime_context(context_path, root, expected_mode=mode)
        return context_path
    if destination.exists():
        raise SkillRegistryError(
            "runtime context directory exists without a valid context: {}".format(destination)
        )

    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".skill-registry-context-", dir=str(destination.parent)))
    try:
        snapshot = load_registry_for_mode(mode, root / "common/skill_registry.yaml")
        snapshot_name: Optional[str] = None
        if snapshot is not None:
            if len(snapshot.canonical_names) != REGISTRY_GROUP_COUNT:
                raise SkillRegistryError(
                    "production registry must contain exactly {} groups".format(
                        REGISTRY_GROUP_COUNT
                    )
                )
            snapshot_name = SNAPSHOT_FILE_NAME
            (stage / snapshot_name).write_bytes(snapshot.raw_bytes)
            snapshot = load_registry(stage / snapshot_name)

        payload = _context_payload(root, run_date, run_id, mode, snapshot, snapshot_name)
        (stage / CONTEXT_FILE_NAME).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(str(stage), str(destination))
        load_runtime_context(context_path, root, expected_mode=mode)
        return context_path
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def load_runtime_context(
    context_path: Path,
    root: Path,
    expected_mode: Optional[str] = None,
) -> SkillRegistryRuntime:
    """Load a context and fail closed on any snapshot or implementation drift."""
    context_path = Path(context_path).resolve()
    root = Path(root).resolve()
    payload = _read_json_object(context_path)
    required_fields = {
        "context_version", "run_date", "run_id", "skill_registry_mode",
        "required_search_mode", "schema_version", "registry_sha256",
        "registry_fingerprint", "implementation_identity",
        "implementation_file_hashes", "registry_snapshot", "registry_canonicals",
        "registry_ownership", "production_canonicals", "shadow_canonicals",
    }
    missing = required_fields - set(payload)
    if missing:
        raise SkillRegistryError("runtime context missing fields: {}".format(sorted(missing)))
    if payload.get("context_version") != 1:
        raise SkillRegistryError("unsupported runtime context version")
    mode = _validate_mode(payload.get("skill_registry_mode"))
    configured = _validate_mode(expected_mode or configured_mode())
    if mode != configured:
        raise SkillRegistryError(
            "runtime mode mismatch: context={} configured={}".format(mode, configured)
        )
    if payload.get("required_search_mode") != REQUIRED_SEARCH_MODE:
        raise SkillRegistryError("required search mode mismatch")

    snapshot_path = context_path.parent / str(payload.get("registry_snapshot") or "")
    snapshot = load_registry_for_mode(mode, snapshot_path)
    if snapshot is None:
        if payload.get("registry_snapshot") is not None or payload.get("registry_sha256") is not None:
            raise SkillRegistryError("legacy context must not contain a registry snapshot")
        if payload.get("registry_canonicals") != []:
            raise SkillRegistryError("legacy context must not contain registry canonicals")
        if payload.get("registry_ownership") != {}:
            raise SkillRegistryError("legacy context must not contain registry ownership")
        if payload.get("production_canonicals") != [] or payload.get("shadow_canonicals") != []:
            raise SkillRegistryError("legacy context must not contain ownership canonicals")
    else:
        if len(snapshot.canonical_names) != REGISTRY_GROUP_COUNT:
            raise SkillRegistryError("runtime snapshot registry group count mismatch")
        if snapshot.registry_sha256 != payload.get("registry_sha256"):
            raise SkillRegistryError("runtime snapshot hash mismatch")
        if list(snapshot.canonical_names) != payload.get("registry_canonicals"):
            raise SkillRegistryError("runtime snapshot canonical order mismatch")
        if snapshot.ownership_map != payload.get("registry_ownership"):
            raise SkillRegistryError("runtime snapshot ownership mismatch")
        if list(snapshot.production_canonical_names) != payload.get("production_canonicals"):
            raise SkillRegistryError("runtime production ownership mismatch")
        if list(snapshot.shadow_canonical_names) != payload.get("shadow_canonicals"):
            raise SkillRegistryError("runtime shadow ownership mismatch")

    implementation_paths = production_implementation_paths(root)
    current_hashes = _dependency_hashes(root, implementation_paths)
    if current_hashes != payload.get("implementation_file_hashes"):
        raise SkillRegistryError("runtime implementation dependency hash mismatch")
    implementation_id = implementation_identity(implementation_paths)
    if implementation_id != payload.get("implementation_identity"):
        raise SkillRegistryError("runtime implementation identity mismatch")
    fingerprint = execution_fingerprint(
        snapshot,
        mode=mode,
        required_search_mode=REQUIRED_SEARCH_MODE,
        implementation_id=implementation_id,
    )
    if fingerprint != payload.get("registry_fingerprint"):
        raise SkillRegistryError("runtime registry fingerprint mismatch")

    for environment_name, context_name in (("RUN_DATE", "run_date"), ("RUN_ID", "run_id")):
        environment_value = os.environ.get(environment_name)
        if environment_value and environment_value != payload.get(context_name):
            raise SkillRegistryError(
                "{} does not match runtime context".format(environment_name)
            )

    return SkillRegistryRuntime(
        mode=mode,
        run_date=str(payload["run_date"]),
        run_id=str(payload["run_id"]),
        snapshot=snapshot,
        registry_fingerprint=fingerprint,
        implementation_identity=implementation_id,
        context_path=context_path,
    )


def load_step_runtime(root: Path) -> SkillRegistryRuntime:
    context_value = os.environ.get(SKILL_REGISTRY_CONTEXT_ENV)
    if not context_value:
        raise SkillRegistryError(
            "{} is required for skill registry production steps".format(
                SKILL_REGISTRY_CONTEXT_ENV
            )
        )
    return load_runtime_context(Path(context_value), root)


def step_fingerprint_path(step_dir: Path) -> Path:
    return Path(step_dir) / "02_confirm/skill_registry_fingerprint.json"


def write_step_fingerprint(step_dir: Path, step_name: str, runtime: SkillRegistryRuntime) -> Path:
    path = step_fingerprint_path(step_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "step": step_name,
        "run_date": runtime.run_date,
        "run_id": runtime.run_id,
        "skill_registry_mode": runtime.mode,
        "registry_fingerprint": runtime.registry_fingerprint,
        "implementation_identity": runtime.implementation_identity,
        "execution_context": str(runtime.context_path),
    }
    temporary = path.with_name("{}.tmp.{}".format(path.name, os.getpid()))
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(str(temporary), str(path))
    return path


def require_step_fingerprints(
    root: Path,
    step_names: Sequence[str],
    runtime: SkillRegistryRuntime,
) -> None:
    root = Path(root).resolve()
    for step_name in step_names:
        path = step_fingerprint_path(root / step_name)
        payload = _read_json_object(path)
        expected = {
            "run_date": runtime.run_date,
            "run_id": runtime.run_id,
            "skill_registry_mode": runtime.mode,
            "registry_fingerprint": runtime.registry_fingerprint,
            "implementation_identity": runtime.implementation_identity,
        }
        for key, value in expected.items():
            if payload.get(key) != value:
                raise SkillRegistryError(
                    "{} sidecar mismatch for {}".format(step_name, key)
                )


def propagate_step_fingerprint(
    root: Path,
    producer_step: str,
    consumer_step_dir: Path,
    consumer_step_name: str,
    runtime: SkillRegistryRuntime,
) -> Path:
    """Verify the direct producer and atomically publish the consumer sidecar."""
    require_step_fingerprints(root, (producer_step,), runtime)
    return write_step_fingerprint(consumer_step_dir, consumer_step_name, runtime)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("create", nargs="?")
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--mode", choices=sorted(VALID_PRODUCTION_MODES))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    context_path = create_runtime_context(
        args.root,
        args.output_root,
        args.run_date,
        args.run_id,
        mode=args.mode,
    )
    print(str(context_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

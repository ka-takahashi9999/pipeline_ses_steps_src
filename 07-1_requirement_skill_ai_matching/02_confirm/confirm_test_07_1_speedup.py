"""Confirm a test-only 07-1 speedup run without touching production outputs."""

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


CONFIRM_DIR = Path(__file__).resolve().parent
STEP_DIR = CONFIRM_DIR.parent
PROJECT_ROOT = STEP_DIR.parent
TOOL_PATH = STEP_DIR / "00_tool/_test_07_1_speedup.py"
TEST_OUTPUT_ROOT = STEP_DIR / "_test_07_1_speedup"
sys.path.insert(0, str(PROJECT_ROOT))

from common.json_utils import read_jsonl
from common.logger import get_logger


def _load_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"moduleをloadできません: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


speedup = _load_module("test_07_1_speedup_for_confirm", TOOL_PATH)
logger = get_logger("confirm_test_07_1_speedup")


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"JSON objectでない: {path.name}")
    return value


def _validate_run_dir(run_dir: Path) -> Path:
    resolved = run_dir.resolve()
    root = TEST_OUTPUT_ROOT.resolve()
    if root not in resolved.parents:
        raise ValueError(f"test output root外です: {resolved}")
    return resolved


def confirm_run(run_dir: Path) -> Dict[str, Any]:
    run_dir = _validate_run_dir(run_dir)
    required_files = [
        "manifest.jsonl",
        "results.jsonl",
        "errors.jsonl",
        "checkpoint.jsonl",
        "usage.json",
        "timing.json",
        "report.json",
        "report.txt",
    ]
    issues: List[str] = []
    for filename in required_files:
        if not (run_dir / filename).is_file():
            issues.append(f"missing file: {filename}")
    if issues:
        return {"status": "NG", "issues": issues}

    try:
        manifest = list(read_jsonl(str(run_dir / "manifest.jsonl")))
        checkpoints = list(read_jsonl(str(run_dir / "checkpoint.jsonl")))
        results = list(read_jsonl(str(run_dir / "results.jsonl")))
        errors = list(read_jsonl(str(run_dir / "errors.jsonl")))
        usage = _load_json(run_dir / "usage.json")
        timing = _load_json(run_dir / "timing.json")
        report = _load_json(run_dir / "report.json")
    except Exception as exc:
        return {
            "status": "NG",
            "issues": [f"output parse failure: {type(exc).__name__}: {exc}"],
        }

    if not manifest:
        issues.append("manifestが空")
    manifest_identities = [row.get("request_identity") for row in manifest]
    if len(manifest_identities) != len(set(manifest_identities)):
        issues.append("manifest duplicate identity")
    ordinals = [row.get("ordinal") for row in manifest]
    if ordinals != sorted(ordinals):
        issues.append("manifest ordinal order不整合")
    for index, row in enumerate(manifest, 1):
        required = {
            "ordinal",
            "project_message_id",
            "resource_message_id",
            "request_identity",
            "request_body_sha256",
            "is_project_warm_one",
        }
        if set(row) != required:
            issues.append(f"manifest line {index}: key構成不正")
        if not isinstance(row.get("ordinal"), int):
            issues.append(f"manifest line {index}: ordinal不正")
        if not isinstance(row.get("request_body_sha256"), str) or len(
            row.get("request_body_sha256", "")
        ) != 64:
            issues.append(f"manifest line {index}: request hash不正")

    try:
        collected = speedup.collect_checkpoints(manifest, checkpoints)
    except Exception as exc:
        issues.append(str(exc))
        collected = {"ordered": [], "missing": manifest_identities}
    ordered = collected["ordered"]
    expected_results = [
        row.get("result") for row in ordered if row.get("status") == "success"
    ]
    expected_errors = [
        row.get("error") for row in ordered if row.get("status") == "error"
    ]
    if results != expected_results:
        issues.append("collector results.jsonl不一致")
    if errors != expected_errors:
        issues.append("collector errors.jsonl不一致")

    project_map = {
        str(row["message_id"]): row
        for row in read_jsonl(str(speedup.production.INPUT_PROJECT_SKILLS))
        if row.get("message_id")
    }
    schema_issues: List[str] = []
    for index, result in enumerate(results, 1):
        if not isinstance(result, dict):
            schema_issues.append(f"result {index}: objectでない")
            continue
        project_mid = str(result.get("project_info", {}).get("message_id", ""))
        resource_mid = str(result.get("resource_info", {}).get("message_id", ""))
        original = project_map.get(project_mid)
        if not project_mid or not resource_mid or original is None:
            schema_issues.append(f"result {index}: message_id/join不正")
            continue
        required_error = speedup.production._validate_skills(
            original.get("required_skills") or [],
            result.get("required_skills"),
            "required_skills",
        )
        optional_error = speedup.production._validate_skills(
            original.get("optional_skills") or [],
            result.get("optional_skills"),
            "optional_skills",
        )
        if required_error:
            schema_issues.append(f"result {index}: {required_error}")
        if optional_error:
            schema_issues.append(f"result {index}: {optional_error}")
        evaluation_meta = result.get("evaluation_meta", {})
        if evaluation_meta.get("llm_model") != speedup.production.LLM_MODEL:
            schema_issues.append(f"result {index}: model不一致")
    issues.extend(schema_issues)

    aggregate_usage = speedup._aggregate_usage(ordered)
    for key in (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "total_tokens",
    ):
        if usage.get(key) != aggregate_usage.get(key):
            issues.append(f"usage.{key}不一致")
    if usage.get("cached_input_tokens", 0) > usage.get("input_tokens", 0):
        issues.append("cached input tokens > input tokens")

    expected_count = len(manifest)
    if len(ordered) != expected_count:
        issues.append(
            f"件数不一致 manifest={expected_count} checkpoint={len(ordered)}"
        )
    if len(results) + len(errors) != expected_count:
        issues.append(
            f"出力件数不一致 manifest={expected_count} outputs={len(results) + len(errors)}"
        )
    if report.get("request_count") != expected_count:
        issues.append("report.request_count不一致")
    if report.get("completed_count") != len(ordered):
        issues.append("report.completed_count不一致")
    if report.get("production_write_count") != 0:
        issues.append("production write検出")
    if report.get("quality", {}).get("request_body_mismatch_count") != 0:
        issues.append("request body mismatch検出")
    if timing != report.get("timing"):
        issues.append("timing.jsonとreport不一致")
    if timing.get("peak_concurrency", 0) > speedup.MAX_CONCURRENCY_HARD_LIMIT:
        issues.append("peak concurrency hard limit超過")
    if report.get("api_failure_count", 0) != 0:
        issues.append("API failureあり")

    return {
        "status": "OK" if not issues else "NG",
        "issues": issues,
        "counts": {
            "manifest": len(manifest),
            "checkpoint": len(ordered),
            "results": len(results),
            "errors": len(errors),
            "duplicate": 0,
            "missing": len(collected.get("missing", [])),
            "schema_issues": len(schema_issues),
        },
        "production_write_count": report.get("production_write_count"),
    }


def _text(summary: Dict[str, Any]) -> str:
    lines = ["=== confirm_test_07_1_speedup ===", f"status: {summary['status']}"]
    counts = summary.get("counts", {})
    for key in (
        "manifest",
        "checkpoint",
        "results",
        "errors",
        "duplicate",
        "missing",
        "schema_issues",
    ):
        if key in counts:
            lines.append(f"{key}: {counts[key]}")
    lines.append(f"production write: {summary.get('production_write_count', 'unknown')}")
    for issue in summary.get("issues", [])[:20]:
        lines.append(f"[NG] {issue}")
    return "\n".join(lines) + "\n"


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="confirm test-only 07-1 speedup run")
    parser.add_argument("--run-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    try:
        run_dir = _validate_run_dir(args.run_dir)
        summary = confirm_run(run_dir)
    except Exception as exc:
        logger.error(f"confirm失敗: {type(exc).__name__}: {exc}")
        return 1
    (run_dir / "confirm_test_report.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    text = _text(summary)
    (run_dir / "confirm_test_report.txt").write_text(text, encoding="utf-8")
    if summary["status"] == "OK":
        logger.ok(text.strip())
        return 0
    logger.error(text.strip())
    return 1


if __name__ == "__main__":
    sys.exit(main())

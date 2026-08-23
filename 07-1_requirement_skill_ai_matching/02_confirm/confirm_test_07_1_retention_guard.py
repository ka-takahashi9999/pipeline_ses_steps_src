"""07-1 test専用minimal retention guard replayのoffline confirm。"""

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


CONFIRM_DIR = Path(__file__).resolve().parent
STEP_DIR = CONFIRM_DIR.parent
PROJECT_ROOT = STEP_DIR.parent
TOOL_PATH = STEP_DIR / "00_tool/_test_07_1_candidate_retention_guard.py"
TEST_ROOT = STEP_DIR / "_test_07_1_candidate_retention_guard"

sys.path.insert(0, str(PROJECT_ROOT))

from common.json_utils import read_jsonl


def _load_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError("moduleをloadできません: {}".format(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


guard = _load_module("test_07_1_retention_guard_for_confirm", TOOL_PATH)


def _validate_run_dir(run_dir: Path) -> Path:
    resolved = run_dir.resolve()
    root = TEST_ROOT.resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError("test output root外です: {}".format(resolved))
    return resolved


def _load_report(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError("report.jsonがobjectではありません")
    return value


def confirm_run(run_dir: Path) -> Dict[str, Any]:
    run_dir = _validate_run_dir(run_dir)
    issues: List[str] = []
    for filename in ("report.json", "guard_audit.jsonl", "retained_pairs.jsonl"):
        if not (run_dir / filename).is_file():
            issues.append("missing file: {}".format(filename))
    if issues:
        return {"status": "NG", "issues": issues}

    try:
        report = _load_report(run_dir / "report.json")
        audits = list(read_jsonl(str(run_dir / "guard_audit.jsonl")))
        retained = list(read_jsonl(str(run_dir / "retained_pairs.jsonl")))
        expected_report, expected_audits, expected_retained = guard.analyze_replay(
            report.get("run_id", "")
        )
    except Exception as exc:
        return {
            "status": "NG",
            "issues": ["parse/replay failure: {}: {}".format(type(exc).__name__, exc)],
        }

    if report != expected_report:
        issues.append("reportがread-only再計算結果と不一致")
    if audits != expected_audits:
        issues.append("guard_auditがread-only再計算結果と不一致")
    if retained != expected_retained:
        issues.append("retained_pairsがread-only再計算結果と不一致")
    if report.get("requests") != 500:
        issues.append("input件数が500ではない")
    if report.get("new_retained_pairs") != len(retained):
        issues.append("retained pair件数不整合")
    pair_keys = [
        (row.get("project_message_id"), row.get("resource_message_id"))
        for row in retained
    ]
    if len(pair_keys) != len(set(pair_keys)):
        issues.append("retained pair重複")
    for index, row in enumerate(retained, 1):
        if row.get("required_rate_before", 1.0) >= guard.RETENTION_GATE_RATE:
            issues.append("retained {}: 通常閾値以上".format(index))
        if row.get("retention_destination") != "08-5_recheck_only":
            issues.append("retained {}: destination不正".format(index))
        if row.get("proposal_ready_direct") is not False:
            issues.append("retained {}: proposal_ready直接昇格".format(index))
        if not row.get("eligible_required_skills"):
            issues.append("retained {}: guard根拠なし".format(index))

    expected_zero = (
        "candidate_loss_after",
        "guard_false_to_true",
        "result_rows_changed",
        "input_results_write",
        "proposal_ready_false_positive_after",
        "known_false_positives_affected",
        "condition_violations",
        "mixed_or_and_retained",
        "non_duration_reason_retained",
        "production_write",
        "new_api_calls",
    )
    for key in expected_zero:
        if report.get(key) != 0:
            issues.append("{}.expected=0 actual={}".format(key, report.get(key)))
    if report.get("candidate_loss_before") != 1:
        issues.append("candidate_loss_beforeが1ではない")
    if report.get("known_loss_rescued") is not True:
        issues.append("known candidate loss未救済")
    if report.get("quality") != "PASS":
        issues.append("replay QUALITYがPASSではない")

    return {
        "status": "OK" if not issues else "NG",
        "issues": issues,
        "counts": {
            "input": report.get("requests"),
            "guard_audits": len(audits),
            "retained_pairs": len(retained),
            "candidate_loss_before": report.get("candidate_loss_before"),
            "candidate_loss_after": report.get("candidate_loss_after"),
        },
        "production_write": report.get("production_write"),
        "new_api_calls": report.get("new_api_calls"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="retention guard replay confirm")
    parser.add_argument("run_dir", type=Path, nargs="?", default=guard.DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    summary = confirm_run(args.run_dir)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["status"] == "OK" else 1


if __name__ == "__main__":
    sys.exit(main())

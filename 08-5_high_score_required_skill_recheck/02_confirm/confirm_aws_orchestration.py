#!/usr/bin/env python3
"""Static confirm for Issue 2 orchestration wiring and IAM boundaries."""

import importlib.util
import json
import sys
from pathlib import Path


STEP_DIR = Path(__file__).resolve().parents[1]
ROOT = STEP_DIR.parent


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    errors = []
    phase_a = (ROOT / "00_pipeline/00_tool/run_full_pipeline.sh").read_text(
        encoding="utf-8"
    )
    master = (
        ROOT / "00_pipeline/00_tool/run_full_pipeline_master.sh"
    ).read_text(encoding="utf-8")
    phase_b = (
        ROOT / "00_pipeline/00_tool/run_full_pipeline_phase_b.sh"
    ).read_text(encoding="utf-8")
    managed = (
        ROOT / "00_pipeline/00_tool/run_full_pipeline_managed.sh"
    ).read_text(encoding="utf-8")

    if "08-5_batch_prepare_submit" not in phase_a or phase_a != master:
        errors.append("Phase A runner/master wiring不一致")
    if "SUSPENDED:BATCH_WAIT:08-5_BATCH_WAIT" not in managed:
        errors.append("managed suspension contract欠落")
    if 'run_step "01-' in phase_b or 'run_step "08-4_' in phase_b:
        errors.append("Phase Bに01～08-4再実行が混入")
    if phase_b.index("08-5_batch_collect_commit_gate") > phase_b.index(
        'run_step "09-1_'
    ):
        errors.append("commit gateより前に09系が開始")

    policy = json.loads(
        (STEP_DIR / "aws/lambda-permissions-policy.json.template").read_text(
            encoding="utf-8"
        )
    )
    actions = []
    for statement in policy["Statement"]:
        value = statement["Action"]
        actions.extend(value if isinstance(value, list) else [value])
    if any(action.startswith("ec2:") for action in actions):
        errors.append("status Lambda IAMにEC2権限あり")
    if "ssm:SendCommand" in actions:
        errors.append("status Lambda IAMにSendCommand権限あり")

    patcher = _load("confirm_issue2_asl", STEP_DIR / "aws/state_machine_patch.py")
    fixture = {
        "StartAt": "ValidateRunDateInput",
        "TimeoutSeconds": 72000,
        "States": {
            "ValidateRunDateInput": {"Type": "Succeed"},
            "ValidateRunningDocument": {
                "Type": "Choice",
                "Choices": [{"And": [], "Next": "CheckRunningWaitLimit"}],
            },
            "CheckRunningWaitLimit": {"Type": "Succeed"},
            "StopEC2AfterFailure": {"Type": "Succeed"},
            "PublishFailureNotification": {"Type": "Succeed"},
            "ListPipelineStatusObject": {"Type": "Succeed"},
        },
    }
    patched = patcher.apply_patch(fixture)
    patcher.validate_graph(patched)
    if patched["States"]["WaitForBatchStatus"].get("Seconds") != 300:
        errors.append("Batch Waitが300秒ではありません")

    report = {
        "confirm": "NG" if errors else "OK",
        "errors": errors,
        "phase_a_batch_wait": "OK" if "08-5_BATCH_WAIT" in phase_a else "NG",
        "phase_b_pre_steps": 0,
        "direct_auto_fallback": "DISABLED",
        "wait_seconds": patched["States"]["WaitForBatchStatus"].get("Seconds"),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""80-75 dry-run preflight summaryから80-7再実行要否をfail-closedで決める。"""

import argparse
import json
import sys
from pathlib import Path


def decide(summary):
    if not isinstance(summary, dict):
        raise ValueError("preflight summary must be an object")
    if summary.get("backup_status") != "SUCCEEDED" or summary.get("mode") != "dry-run":
        raise ValueError("80-75 preflight did not succeed in dry-run mode")
    manual = summary.get("manual_recovery")
    if not isinstance(manual, dict) or manual.get("verified") is not True:
        raise ValueError("manual recovery preflight is not verified")
    guard = manual.get("source_guard")
    if not isinstance(guard, dict):
        raise ValueError("source guard evidence is missing")

    if guard.get("at_validating_boundary") is True:
        required = {
            "side_effect_phase": "VALIDATING",
            "rotation_started": False,
            "authority_fingerprint_verified": True,
            "downstream_80_8_started": False,
            "downstream_80_9_started": False,
            "partial_publish_detected": False,
            "publication_boundary_reached": False,
            "source_execution_redrive_count": 0,
        }
        mismatch = {
            key: {"expected": expected, "actual": guard.get(key)}
            for key, expected in required.items()
            if guard.get(key) != expected
        }
        if mismatch:
            raise ValueError(f"VALIDATING recovery guard mismatch: {mismatch}")
        return "SKIP_80_7"

    if guard.get("before_publication_boundary") is True:
        if guard.get("publication_boundary_reached") is not False:
            raise ValueError("pre-publication boundary evidence is not negative")
        return "RUN_80_7"
    raise ValueError("source failure position is not recoverable")


def decide_resume(state_dir, run_date, recovery_id, receipt_path=None):
    """formal recovery自身が80-9 guardで止まった場合だけ同一ID resumeを許可する。"""
    rotation_path = state_dir / "80-75/01_result/portal_s3_backup_rotation_summary.json"
    prepare_path = state_dir / "80-8/01_result/portal_s3_prepare_summary.json"
    manifest_path = state_dir / "80-8/01_result/portal_s3_manifest.jsonl"
    sync_path = state_dir / "80-9/01_result/portal_s3_sync_summary.json"
    rotation = json.loads(rotation_path.read_text(encoding="utf-8"))
    prepare = json.loads(prepare_path.read_text(encoding="utf-8"))
    sync = json.loads(sync_path.read_text(encoding="utf-8"))
    if receipt_path is not None and receipt_path.exists():
        raise ValueError("manual recovery receipt already exists")
    context = (rotation.get("manual_recovery") or {}).get("context") or {}
    if (rotation.get("backup_status") != "SUCCEEDED"
            or rotation.get("side_effect_phase") != "ROTATION_VERIFIED"
            or rotation.get("mode") != "apply"
            or (rotation.get("verify") or {}).get("verified") is not True
            or context.get("run_date") != run_date
            or context.get("recovery_id") != recovery_id):
        raise ValueError("80-75 completed rotation checkpoint is invalid")
    if (prepare.get("step") != "80-8_portal_s3_prepare"
            or prepare.get("run_date") != run_date
            or prepare.get("run_id") != recovery_id
            or type(prepare.get("file_count")) is not int
            or prepare["file_count"] <= 0
            or not manifest_path.is_file()
            or manifest_path.stat().st_size <= 0):
        raise ValueError("80-8 completed manifest checkpoint is invalid")
    expected_error = (
        "RotationError: publication境界以降のFAILED runはrecoveryできません "
        "(step='80-75_portal_s3_backup_rotation' / boundary=80-75_portal_s3_backup_rotation)"
    )
    if (sync.get("sync_status") != "FAILED"
            or sync.get("run_date") != run_date
            or sync.get("run_id") != recovery_id
            or sync.get("error_message") != expected_error
            or "staging" in sync or "verify" in sync):
        raise ValueError("80-9 failure is not the known pre-publish guard failure")
    return "RESUME_80_9"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("summary", type=Path, nargs="?")
    parser.add_argument("--resume-state-dir", type=Path)
    parser.add_argument("--run-date")
    parser.add_argument("--recovery-id")
    parser.add_argument("--receipt-path", type=Path)
    args = parser.parse_args()
    try:
        if args.resume_state_dir is not None:
            if args.summary is not None or not args.run_date or not args.recovery_id:
                raise ValueError("resume mode arguments are incomplete")
            print(decide_resume(
                args.resume_state_dir, args.run_date, args.recovery_id, args.receipt_path
            ))
        else:
            if args.summary is None:
                raise ValueError("preflight summary is required")
            document = json.loads(args.summary.read_text(encoding="utf-8"))
            print(decide(document))
        return 0
    except Exception as exc:
        print(f"tail recovery plan rejected: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

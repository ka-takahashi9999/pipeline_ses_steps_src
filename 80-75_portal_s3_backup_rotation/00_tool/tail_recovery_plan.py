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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("summary", type=Path)
    args = parser.parse_args()
    try:
        document = json.loads(args.summary.read_text(encoding="utf-8"))
        print(decide(document))
        return 0
    except Exception as exc:
        print(f"tail recovery plan rejected: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

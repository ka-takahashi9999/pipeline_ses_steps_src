#!/usr/bin/env python3
"""Write the current pipeline execution status locally and to S3."""

import argparse
import json
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import boto3


SCHEMA_VERSION = "1.0"
VALID_STATUSES = ("RUNNING", "SUCCEEDED", "FAILED")
RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


def env_value(name: str) -> Optional[str]:
    value = os.environ.get(name)
    return value if value else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=env_value("RUN_ID"))
    parser.add_argument("--run-date", default=env_value("RUN_DATE"))
    parser.add_argument("--status", required=True, choices=VALID_STATUSES)
    parser.add_argument("--started-at", default=env_value("PIPELINE_STARTED_AT"))
    parser.add_argument("--finished-at")
    parser.add_argument("--exit-code", type=int)
    parser.add_argument("--current-step", required=True)
    parser.add_argument("--error-message", default="")
    parser.add_argument("--log-s3-uri", default=env_value("PIPELINE_LOG_S3_URI"))
    parser.add_argument(
        "--bucket",
        default=env_value("PIPELINE_S3_BUCKET") or "technoverse",
    )
    parser.add_argument(
        "--base-prefix",
        default=env_value("PIPELINE_S3_BASE_PREFIX") or "pipeline_ses_steps",
    )
    parser.add_argument(
        "--status-prefix",
        default=env_value("PIPELINE_STATUS_PREFIX") or "pipeline-status",
    )
    parser.add_argument(
        "--log-prefix",
        default=env_value("PIPELINE_LOG_PREFIX") or "pipeline-logs",
    )
    parser.add_argument(
        "--region",
        default=env_value("PIPELINE_AWS_REGION") or "ap-northeast-1",
    )
    parser.add_argument(
        "--local-output",
        default=env_value("PIPELINE_LOCAL_STATUS_FILE"),
        help="Optional local status.json path, written atomically.",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Write only --local-output and skip the S3 put (for local verification).",
    )
    return parser.parse_args()


def require_non_empty(name: str, value: Optional[str]) -> str:
    if value is None or not value.strip():
        raise ValueError(f"{name} is required")
    return value.strip()


def validate_run_id(run_id: str) -> None:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(
            "run_id must be 1-128 characters using only letters, digits, '.', '_', or '-'"
        )


def validate_run_date(run_date: str) -> None:
    if not re.fullmatch(r"\d{8}", run_date):
        raise ValueError("run_date must use YYYYMMDD format")
    try:
        datetime.strptime(run_date, "%Y%m%d")
    except ValueError as exc:
        raise ValueError("run_date must be a valid calendar date") from exc


def validate_timestamp(name: str, value: str) -> None:
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO 8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include a timezone")


def normalize_prefix(name: str, value: str) -> str:
    normalized = value.strip("/")
    if not normalized or any(part in ("", ".", "..") for part in normalized.split("/")):
        raise ValueError(f"{name} is invalid: {value}")
    return normalized


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_document(args: argparse.Namespace) -> Dict[str, Any]:
    run_id = require_non_empty("run_id", args.run_id)
    run_date = require_non_empty("run_date", args.run_date)
    started_at = require_non_empty("started_at", args.started_at)
    current_step = require_non_empty("current_step", args.current_step)
    validate_run_id(run_id)
    validate_run_date(run_date)
    validate_timestamp("started_at", started_at)

    base_prefix = normalize_prefix("base_prefix", args.base_prefix)
    log_prefix = normalize_prefix("log_prefix", args.log_prefix)
    log_s3_uri = args.log_s3_uri or (
        f"s3://{args.bucket}/{base_prefix}/{log_prefix}/{run_date}/{run_id}/pipeline.log"
    )

    if args.status == "RUNNING":
        if args.finished_at is not None or args.exit_code is not None:
            raise ValueError("RUNNING status must not have finished_at or exit_code")
        finished_at = None
        finished_at_source = "not_finished"
        exit_code = None
        exit_code_source = "pending"
    else:
        finished_at = require_non_empty("finished_at", args.finished_at)
        validate_timestamp("finished_at", finished_at)
        if args.exit_code is None:
            raise ValueError("exit_code is required for a terminal status")
        exit_code = args.exit_code
        finished_at_source = "managed_wrapper"
        exit_code_source = "managed_wrapper"

        if args.status == "SUCCEEDED" and exit_code != 0:
            raise ValueError("SUCCEEDED status requires exit_code=0")
        if args.status == "FAILED" and exit_code == 0:
            raise ValueError("FAILED status requires a non-zero exit_code")
        if args.status == "FAILED" and not args.error_message.strip():
            raise ValueError("FAILED status requires error_message")

    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "run_date": run_date,
        "status": args.status,
        "started_at": started_at,
        "finished_at": finished_at,
        "finished_at_source": finished_at_source,
        "exit_code": exit_code,
        "exit_code_source": exit_code_source,
        "current_step": current_step,
        "error_message": args.error_message,
        "log_s3_uri": log_s3_uri,
        "updated_at": utc_now(),
    }


def serialize_document(document: Dict[str, Any]) -> bytes:
    return (json.dumps(document, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def write_local_atomic(output_path: Path, body: bytes) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=str(output_path.parent),
            prefix=f".{output_path.name}.",
            delete=False,
        ) as temp_file:
            temp_file.write(body)
            temp_file.flush()
            os.fsync(temp_file.fileno())
            temp_path = Path(temp_file.name)
        os.replace(str(temp_path), str(output_path))
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()


def main() -> int:
    args = parse_args()
    try:
        document = build_document(args)
        body = serialize_document(document)

        if args.local_output:
            write_local_atomic(Path(args.local_output), body)
        elif args.local_only:
            raise ValueError("--local-only requires --local-output")

        if not args.local_only:
            status_prefix = normalize_prefix("status_prefix", args.status_prefix)
            s3_key = (
                f"{normalize_prefix('base_prefix', args.base_prefix)}/"
                f"{status_prefix}/{document['run_date']}/{document['run_id']}/status.json"
            )
            s3_client = boto3.client("s3", region_name=args.region)
            s3_client.put_object(
                Bucket=args.bucket,
                Key=s3_key,
                Body=body,
                ContentType="application/json; charset=utf-8",
            )
        return 0
    except Exception as exc:
        print(f"publish_pipeline_status failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

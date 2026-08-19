#!/usr/bin/env python3
"""
80-7_manage_09_result_retention

09系step（09-1 / 09-2 / 09-3 / 09-4 / 09-5）のRUN_DATE別成果物と
S3 base prefix直下の外部配布ZIPを「今回RUN_DATE」と「直前の正常終了RUN_DATE」
の2世代だけ保持する。

直前の正常終了RUN_DATEの正本は S3 の
  s3://<bucket>/<base_prefix>/<status_prefix>/<RUN_DATE>/<RUN_ID>/status.json
であり、前日計算・ディレクトリmtime・ローカルstatusは使わない。

安全設計:
- 削除開始前に全候補をvalidationし、1件でも異常があれば何も削除せず異常終了する
- 認識できないファイル / 不正日付 / symlink / regular file以外 が存在したら削除しない
- 直前の正常RUN_DATEを安全に決定できず、かつ過去成果物が存在する場合は削除しない
- --dry-run では削除0件（集計のみ）
- 再実行時は削除0件で正常終了する（冪等）
- S3削除は `^mail_display_extract_(\d{8})\.zip$` かつ実在日付のroot objectだけ
- current / previous successful ZIPが無い場合やpreviousを決定できない場合は削除しない

usage:
  manage_09_result_retention.py --dry-run [--run-date YYYYMMDD]
  manage_09_result_retention.py --apply   [--run-date YYYYMMDD]
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_execution_time  # noqa: E402
from common.logger import get_logger  # noqa: E402
from common.pipeline_s3_env import get_config_value, load_pipeline_s3_config  # noqa: E402

STEP_NAME = "80-7_manage_09_result_retention"
STEP_DIR = Path(__file__).resolve().parents[1]
SUMMARY_FILENAME = "manage_09_result_retention_summary.json"

RUN_DATE_RE = re.compile(r"^\d{8}$")
VALID_STATUSES = ("RUNNING", "SUCCEEDED", "FAILED")

# S3 base prefix直下に置く外部配布用09-2 ZIPの正式contract。
# CURRENT mirror内の同名ZIPとは用途が異なり、両方を維持する。
ROOT_DISTRIBUTION_ZIP_RE = re.compile(r"^mail_display_extract_(\d{8})\.zip$")
ROOT_ZIP_BACKUP_GENERATIONS = 1
EXPECTED_ROOT_ZIP_BUCKET = "technoverse"
EXPECTED_ROOT_ZIP_BASE_PREFIX = "pipeline_ses_steps"
ROOT_DISTRIBUTION_ZIP_KEY_RE = re.compile(
    r"^pipeline_ses_steps/mail_display_extract_(\d{8})\.zip$"
)

# status.json の正本: 99-9_publish_pipeline_status/00_tool/publish_pipeline_status.py
SUPPORTED_SCHEMA_VERSIONS = ("1.0",)
RUN_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
STATUS_OBJECT_NAME = "status.json"

# 削除対象（RUN_DATE付き成果物）。kind は dir / file のいずれか。
RETENTION_TARGETS: Tuple[Tuple[str, str, str], ...] = (
    ("09-1_mail_display_format", "dir", r"^mail_display_format_(\d{8})$"),
    ("09-2_extract_high_score_mail_display", "dir", r"^mail_display_extract_(\d{8})$"),
    ("09-2_extract_high_score_mail_display", "file", r"^mail_display_extract_(\d{8})\.zip$"),
    ("09-3_prepare_sales_proposal_input", "file", r"^proposal_input_(\d{8})\.jsonl$"),
    ("09-3_prepare_sales_mail_context", "file", r"^prepare_sales_mail_context_(\d{8})\.jsonl$"),
    (
        "09-4_remove_category_mismatch_sales_candidates",
        "file",
        r"^sales_proposal_candidates_(\d{8})\.jsonl$",
    ),
    (
        "09-4_remove_category_mismatch_sales_candidates",
        "file",
        r"^99_excluded_category_mismatch_sales_candidates_(\d{8})\.jsonl$",
    ),
    ("09-5_generate_sales_reply_draft", "file", r"^generate_sales_reply_draft_(\d{8})\.jsonl$"),
    ("09-5_generate_sales_reply_draft", "dir", r"^reply_preview_(\d{8})$"),
)

# RUN_DATE成果物ではない運用ファイル（保持し、削除対象にしない）
HOLD_PATTERNS: Tuple[str, ...] = (
    r"^error_\d{8}_\d{6}\.log$",
    r"^\.gitkeep$",
)

TARGET_STEPS: Tuple[str, ...] = tuple(
    sorted({step for step, _kind, _pattern in RETENTION_TARGETS})
)


class RetentionError(Exception):
    """削除を行わずに異常終了すべき状態。"""


# ---------------------------------------------------------------------------
# 直前の正常終了RUN_DATE解決（S3 status.json が正本）
# ---------------------------------------------------------------------------


def build_s3_client(region: str):
    """boto3 S3クライアントを生成する（テストから差し替え可能にするため関数化）。"""
    import boto3

    return boto3.client("s3", region_name=region)


def _parse_timestamp(name: str, value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise RetentionError(f"status.json の {name} が不正です: {value!r}")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise RetentionError(f"status.json の {name} がISO8601ではありません: {value!r}") from exc
    if parsed.tzinfo is None:
        raise RetentionError(f"status.json の {name} にタイムゾーンがありません: {value!r}")
    return parsed


def validate_status_document(
    document: Any, expected_run_date: str, expected_run_id: str, s3_uri: str
) -> Dict[str, Any]:
    """
    status.json のschemaを検証する。不正ならRetentionErrorを送出する。

    正本は 99-9_publish_pipeline_status/00_tool/publish_pipeline_status.py が書く
    schema_version 1.0 の成功statusであり、その契約に合わせて検証する。
    """
    if not isinstance(document, dict):
        raise RetentionError(f"status.json がJSON objectではありません: {s3_uri}")

    required_keys = (
        "schema_version",
        "run_id",
        "run_date",
        "status",
        "started_at",
        "finished_at",
        "exit_code",
    )
    missing = [key for key in required_keys if key not in document]
    if missing:
        raise RetentionError(f"status.json に必須キーがありません {missing}: {s3_uri}")

    schema_version = document["schema_version"]
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        raise RetentionError(
            f"status.json のschema_versionが未対応です: {schema_version!r} "
            f"(対応={list(SUPPORTED_SCHEMA_VERSIONS)}) ({s3_uri})"
        )

    status = document["status"]
    if status not in VALID_STATUSES:
        raise RetentionError(f"status.json のstatusが不正です: {status!r} ({s3_uri})")

    run_date = document["run_date"]
    if not isinstance(run_date, str) or not RUN_DATE_RE.match(run_date):
        raise RetentionError(f"status.json のrun_dateが不正です: {run_date!r} ({s3_uri})")
    if not _is_valid_calendar_date(run_date):
        raise RetentionError(f"status.json のrun_dateが実在日付ではありません: {run_date} ({s3_uri})")
    if run_date != expected_run_date:
        raise RetentionError(
            f"status.json のrun_dateがS3 keyと不一致です: {run_date} != {expected_run_date} ({s3_uri})"
        )

    run_id = document["run_id"]
    if not isinstance(run_id, str) or not run_id.strip():
        raise RetentionError(f"status.json のrun_idが空です: {run_id!r} ({s3_uri})")
    if not RUN_ID_RE.fullmatch(run_id):
        raise RetentionError(f"status.json のrun_idが不正です: {run_id!r} ({s3_uri})")
    if run_id != expected_run_id:
        raise RetentionError(
            f"status.json のrun_idがS3 keyと不一致です: {run_id} != {expected_run_id} ({s3_uri})"
        )

    _parse_timestamp("started_at", document["started_at"])

    if status == "RUNNING":
        if document["finished_at"] is not None or document["exit_code"] is not None:
            raise RetentionError(f"RUNNING status に finished_at / exit_code があります: {s3_uri}")
    else:
        if not isinstance(document["exit_code"], int) or isinstance(document["exit_code"], bool):
            raise RetentionError(
                f"status.json のexit_codeが整数ではありません: {document['exit_code']!r} ({s3_uri})"
            )
        finished_at = _parse_timestamp("finished_at", document["finished_at"])
        started_at = _parse_timestamp("started_at", document["started_at"])
        if finished_at < started_at:
            raise RetentionError(f"finished_at が started_at より前です: {s3_uri}")
        if status == "SUCCEEDED" and document["exit_code"] != 0:
            raise RetentionError(f"SUCCEEDED なのに exit_code != 0 です: {s3_uri}")
        if status == "FAILED" and document["exit_code"] == 0:
            raise RetentionError(f"FAILED なのに exit_code == 0 です: {s3_uri}")

    return document


def list_status_run_dates(s3_client, bucket: str, status_prefix: str) -> List[str]:
    """pipeline-status 配下のRUN_DATE一覧を返す。"""
    run_dates: List[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    try:
        pages = paginator.paginate(Bucket=bucket, Prefix=f"{status_prefix}/", Delimiter="/")
        for page in pages:
            for common_prefix in page.get("CommonPrefixes") or []:
                name = common_prefix.get("Prefix", "")[len(status_prefix) + 1 :].strip("/")
                if RUN_DATE_RE.match(name):
                    run_dates.append(name)
    except Exception as exc:  # noqa: BLE001 - S3 LIST失敗は削除せずFAIL
        raise RetentionError(f"S3 status prefix のLISTに失敗しました: {exc}") from exc
    return sorted(set(run_dates))


def parse_status_key(key: str, status_prefix: str, run_date: str) -> str:
    """
    S3 key `<status_prefix>/<RUN_DATE>/<RUN_ID>/status.json` からRUN_IDを取り出す。
    構造が想定と異なる場合はRetentionErrorを送出する。
    """
    prefix = f"{status_prefix}/{run_date}/"
    rest = key[len(prefix) :]
    parts = rest.split("/")
    if len(parts) != 2 or parts[1] != STATUS_OBJECT_NAME or not parts[0]:
        raise RetentionError(f"status.json のS3 key構造が不正です: {key}")
    return parts[0]


def has_successful_run(s3_client, bucket: str, status_prefix: str, run_date: str, logger) -> bool:
    """指定RUN_DATEに正常終了runが1件以上あるか判定する。"""
    prefix = f"{status_prefix}/{run_date}/"
    keys: List[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents") or []:
                key = obj.get("Key", "")
                if key.endswith(f"/{STATUS_OBJECT_NAME}"):
                    keys.append(key)
    except Exception as exc:  # noqa: BLE001
        raise RetentionError(f"S3 status のLISTに失敗しました ({prefix}): {exc}") from exc

    found = False
    for key in sorted(keys):
        s3_uri = f"s3://{bucket}/{key}"
        key_run_id = parse_status_key(key, status_prefix, run_date)
        try:
            body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        except Exception as exc:  # noqa: BLE001
            raise RetentionError(f"S3 status のGETに失敗しました ({s3_uri}): {exc}") from exc
        try:
            document = json.loads(body.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise RetentionError(f"status.json をJSONとして解析できません ({s3_uri}): {exc}") from exc
        document = validate_status_document(document, run_date, key_run_id, s3_uri)
        if document["status"] == "SUCCEEDED" and document["exit_code"] == 0:
            logger.info(f"正常終了run検出: {s3_uri}")
            found = True
    return found


def resolve_previous_successful_run_dates(
    s3_client,
    bucket: str,
    status_prefix: str,
    current_run_date: str,
    logger,
    backup_generations: int = ROOT_ZIP_BACKUP_GENERATIONS,
) -> List[str]:
    """現在RUN_DATEより前の正常終了RUN_DATEを新しい順に指定世代数返す。"""
    if not RUN_DATE_RE.fullmatch(current_run_date) or not _is_valid_calendar_date(
        current_run_date
    ):
        raise RetentionError(f"RUN_DATEが不正です: {current_run_date}")
    if (
        not isinstance(backup_generations, int)
        or isinstance(backup_generations, bool)
        or backup_generations < 0
    ):
        raise RetentionError(f"backup_generationsが不正です: {backup_generations!r}")
    if backup_generations == 0:
        return []

    all_run_dates = list_status_run_dates(s3_client, bucket, status_prefix)
    candidates = [d for d in all_run_dates if d < current_run_date]
    logger.info(
        f"S3 status RUN_DATE: 全{len(all_run_dates)}件 / 現在RUN_DATE未満 {len(candidates)}件"
    )
    selected: List[str] = []
    for run_date in sorted(candidates, reverse=True):
        if has_successful_run(s3_client, bucket, status_prefix, run_date, logger):
            if run_date >= current_run_date:
                raise RetentionError(
                    f"直前の正常終了RUN_DATEが現在RUN_DATE以降です: {run_date} >= {current_run_date}"
                )
            selected.append(run_date)
            if len(selected) >= backup_generations:
                break
    return selected


def resolve_previous_successful_run_date(
    s3_client, bucket: str, status_prefix: str, current_run_date: str, logger
) -> Optional[str]:
    """現在RUN_DATEより前で、正常終了runを持つ最新RUN_DATEを返す。無ければNone。"""
    selected = resolve_previous_successful_run_dates(
        s3_client,
        bucket,
        status_prefix,
        current_run_date,
        logger,
        backup_generations=1,
    )
    return selected[0] if selected else None


def plan_root_distribution_zip_retention(
    object_keys: List[str],
    base_prefix: str,
    current_run_date: str,
    previous_successful_run_dates: List[str],
    backup_generations: int = ROOT_ZIP_BACKUP_GENERATIONS,
) -> Dict[str, Any]:
    """
    S3 base prefix直下の外部配布ZIPについてKEEP / DELETE候補を計画する。

    この純粋関数はS3 LIST/DELETEを行わない。active run()から、
    resolve_previous_successful_run_dates()の結果とroot object一覧を渡して利用する。
    """
    if not RUN_DATE_RE.fullmatch(current_run_date) or not _is_valid_calendar_date(
        current_run_date
    ):
        raise RetentionError(f"RUN_DATEが不正です: {current_run_date}")
    if (
        not isinstance(backup_generations, int)
        or isinstance(backup_generations, bool)
        or backup_generations < 0
    ):
        raise RetentionError(f"backup_generationsが不正です: {backup_generations!r}")

    normalized_prefix = lock_root_distribution_base_prefix(base_prefix)

    validated_previous: List[str] = []
    for run_date in previous_successful_run_dates:
        if not isinstance(run_date, str) or not RUN_DATE_RE.fullmatch(run_date):
            raise RetentionError(f"previous successful RUN_DATEが不正です: {run_date!r}")
        if not _is_valid_calendar_date(run_date):
            raise RetentionError(
                f"previous successful RUN_DATEが実在日付ではありません: {run_date}"
            )
        if run_date >= current_run_date:
            raise RetentionError(
                "previous successful RUN_DATEがcurrent以降です: "
                f"{run_date} >= {current_run_date}"
            )
        validated_previous.append(run_date)

    selected_previous = sorted(set(validated_previous), reverse=True)[:backup_generations]
    keep_run_dates = {current_run_date, *selected_previous}
    root_prefix = f"{normalized_prefix}/"
    targets: List[Tuple[str, str]] = []

    for key in sorted(set(object_keys)):
        if not isinstance(key, str) or not key.startswith(root_prefix):
            continue
        filename = key[len(root_prefix) :]
        if "/" in filename:
            continue
        match = ROOT_DISTRIBUTION_ZIP_RE.fullmatch(filename)
        if not match:
            continue
        run_date = match.group(1)
        if not _is_valid_calendar_date(run_date):
            continue
        if run_date > current_run_date:
            raise RetentionError(
                f"現在RUN_DATE({current_run_date})より新しいroot ZIPがあります: {key}"
            )
        targets.append((key, run_date))

    older_canonical_keys = [
        key for key, run_date in targets if run_date < current_run_date
    ]
    if backup_generations > 0 and not selected_previous and older_canonical_keys:
        raise RetentionError(
            "直前の正常終了RUN_DATEをstatus正本から決定できません。"
            f"古いcanonical root ZIPが{len(older_canonical_keys)}件あるため"
            "DELETE candidateを生成せず停止します"
        )

    return {
        "backup_generations": backup_generations,
        "keep_run_dates": sorted(keep_run_dates),
        "target_keys": [key for key, _run_date in targets],
        "keep_keys": [key for key, run_date in targets if run_date in keep_run_dates],
        "delete_candidate_keys": [
            key for key, run_date in targets if run_date not in keep_run_dates
        ],
    }


def lock_root_distribution_base_prefix(base_prefix: Any) -> str:
    """root ZIP cleanupのbase prefixをcanonical値に完全固定する。"""
    if not isinstance(base_prefix, str) or base_prefix != EXPECTED_ROOT_ZIP_BASE_PREFIX:
        raise RetentionError(
            "root ZIP destination lock違反: base prefixがcanonical値と一致しません "
            f"(actual={base_prefix!r} / expected={EXPECTED_ROOT_ZIP_BASE_PREFIX!r})"
        )
    return EXPECTED_ROOT_ZIP_BASE_PREFIX


def lock_root_distribution_route(bucket: Any, base_prefix: Any) -> Tuple[str, str]:
    """root ZIPのLIST開始前にbucket / prefix / root prefixを完全固定する。"""
    if not isinstance(bucket, str) or bucket != EXPECTED_ROOT_ZIP_BUCKET:
        raise RetentionError(
            "root ZIP destination lock違反: bucketがcanonical値と一致しません "
            f"(actual={bucket!r} / expected={EXPECTED_ROOT_ZIP_BUCKET!r})"
        )
    locked_prefix = lock_root_distribution_base_prefix(base_prefix)
    root_prefix = f"{locked_prefix}/"
    if root_prefix != "pipeline_ses_steps/":
        raise RetentionError(f"root ZIP destination lock違反: root prefix={root_prefix!r}")
    return EXPECTED_ROOT_ZIP_BUCKET, root_prefix


def lock_root_distribution_delete_target(bucket: Any, key: Any) -> str:
    """DELETE直前の実際のbucket/keyを独立に再検証する。"""
    if not isinstance(bucket, str) or bucket != EXPECTED_ROOT_ZIP_BUCKET:
        raise RetentionError(
            "root ZIP DELETE final lock違反: bucketがcanonical値と一致しません "
            f"(actual={bucket!r} / expected={EXPECTED_ROOT_ZIP_BUCKET!r})"
        )
    if not isinstance(key, str):
        raise RetentionError(f"root ZIP DELETE final lock違反: keyが文字列ではありません: {key!r}")
    match = ROOT_DISTRIBUTION_ZIP_KEY_RE.fullmatch(key)
    if not match:
        raise RetentionError(f"root ZIP DELETE final lock違反: canonical keyではありません: {key!r}")
    run_date = match.group(1)
    if not _is_valid_calendar_date(run_date):
        raise RetentionError(f"root ZIP DELETE final lock違反: 実在日付ではありません: {key!r}")
    return run_date


def list_root_distribution_object_keys(
    s3_client, bucket: str, base_prefix: str
) -> List[str]:
    """S3 base prefix直下のobject keyだけをLISTする（配下prefixは対象外）。"""
    locked_bucket, root_prefix = lock_root_distribution_route(bucket, base_prefix)
    keys: List[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    try:
        pages = paginator.paginate(Bucket=locked_bucket, Prefix=root_prefix, Delimiter="/")
        for page in pages:
            for obj in page.get("Contents") or []:
                key = obj.get("Key")
                if not isinstance(key, str) or not key.startswith(root_prefix):
                    raise RetentionError(f"S3 root LISTで不正keyが返却されました: {key!r}")
                if "/" in key[len(root_prefix) :]:
                    raise RetentionError(f"S3 root LISTに配下objectが混入しました: {key}")
                keys.append(key)
    except RetentionError:
        raise
    except Exception as exc:  # noqa: BLE001 - LIST失敗時は削除しない
        raise RetentionError(f"S3 root objectのLISTに失敗しました: {exc}") from exc
    return sorted(set(keys))


def _required_root_zip_keys(
    base_prefix: str, current_run_date: str, previous_successful_run_dates: List[str]
) -> List[str]:
    locked_prefix = lock_root_distribution_base_prefix(base_prefix)
    run_dates = [current_run_date] + sorted(set(previous_successful_run_dates), reverse=True)[
        :ROOT_ZIP_BACKUP_GENERATIONS
    ]
    return [f"{locked_prefix}/mail_display_extract_{run_date}.zip" for run_date in run_dates]


def execute_root_distribution_zip_retention(
    s3_client,
    bucket: str,
    base_prefix: str,
    current_run_date: str,
    previous_successful_run_dates: List[str],
    apply_mode: bool,
    logger,
) -> Dict[str, Any]:
    """root ZIPを事前検証し、apply時だけexact keyを個別削除して再LISTする。"""
    locked_bucket, _root_prefix = lock_root_distribution_route(bucket, base_prefix)
    before_keys = list_root_distribution_object_keys(s3_client, bucket, base_prefix)
    plan = plan_root_distribution_zip_retention(
        before_keys,
        base_prefix,
        current_run_date,
        previous_successful_run_dates,
    )
    required_keys = _required_root_zip_keys(
        base_prefix, current_run_date, previous_successful_run_dates
    )
    missing_required = [key for key in required_keys if key not in plan["keep_keys"]]
    if missing_required:
        raise RetentionError(f"root ZIPの必須KEEP対象が存在しません: {missing_required}")

    delete_candidates = plan["delete_candidate_keys"]
    logger.info(f"root ZIP KEEP: {plan['keep_keys']}")
    logger.info(f"root ZIP DELETE候補: {delete_candidates}")

    deleted_keys: List[str] = []
    if apply_mode:
        for key in delete_candidates:
            lock_root_distribution_delete_target(locked_bucket, key)
            try:
                s3_client.delete_object(Bucket=locked_bucket, Key=key)
            except Exception as exc:  # noqa: BLE001 - 削除失敗はPipeline停止
                raise RetentionError(f"root ZIPのDELETEに失敗しました: {key} ({exc})") from exc
            deleted_keys.append(key)

        after_keys = list_root_distribution_object_keys(s3_client, bucket, base_prefix)
        after_plan = plan_root_distribution_zip_retention(
            after_keys,
            base_prefix,
            current_run_date,
            previous_successful_run_dates,
        )
        missing_after = [key for key in required_keys if key not in after_plan["keep_keys"]]
        if missing_after or after_plan["delete_candidate_keys"]:
            raise RetentionError(
                "root ZIP apply後verifyに失敗しました "
                f"(missing_keep={missing_after} / older={after_plan['delete_candidate_keys']})"
            )
        verified = True
    else:
        verified = False

    return {
        "backup_generations": ROOT_ZIP_BACKUP_GENERATIONS,
        "destination_bucket": locked_bucket,
        "destination_base_prefix": EXPECTED_ROOT_ZIP_BASE_PREFIX,
        "destination_locked": True,
        "keep_run_dates": plan["keep_run_dates"],
        "target_keys": plan["target_keys"],
        "keep_keys": plan["keep_keys"],
        "delete_candidate_keys": delete_candidates,
        "deleted_keys": deleted_keys,
        "verified": verified,
    }


# ---------------------------------------------------------------------------
# ローカル09系成果物の走査・validation
# ---------------------------------------------------------------------------


def _is_valid_calendar_date(run_date: str) -> bool:
    try:
        datetime.strptime(run_date, "%Y%m%d")
    except ValueError:
        return False
    return True


def scan_artifacts(root: Path, current_run_date: str, logger) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    09系 01_result 直下を走査し、RUN_DATE成果物の一覧とHOLD対象の一覧を返す。
    認識できないエントリが1件でもあればRetentionErrorを送出する。
    """
    artifacts: List[Dict[str, Any]] = []
    holds: List[str] = []
    unknown: List[str] = []

    hold_res = [re.compile(p) for p in HOLD_PATTERNS]

    for step in TARGET_STEPS:
        result_dir = root / step / "01_result"
        if not result_dir.is_dir():
            raise RetentionError(f"09系の01_resultが存在しません: {result_dir}")
        if result_dir.is_symlink():
            raise RetentionError(f"01_result がsymlinkです: {result_dir}")

        patterns = [
            (kind, re.compile(pattern))
            for target_step, kind, pattern in RETENTION_TARGETS
            if target_step == step
        ]

        # 走査に失敗したら対象が黙って欠落しないよう即FAILさせる
        try:
            entries = sorted(result_dir.iterdir(), key=lambda p: p.name)
        except OSError as exc:
            raise RetentionError(f"01_result の走査に失敗しました: {result_dir} ({exc})") from exc

        for entry in entries:
            rel = f"{step}/01_result/{entry.name}"
            if entry.is_symlink():
                raise RetentionError(f"symlinkを検出しました（削除しません）: {rel}")

            if any(hold_re.match(entry.name) for hold_re in hold_res):
                holds.append(rel)
                continue

            matched = None
            for kind, pattern_re in patterns:
                match = pattern_re.match(entry.name)
                if match:
                    matched = (kind, match.group(1))
                    break

            if matched is None:
                unknown.append(rel)
                continue

            kind, run_date = matched
            if not _is_valid_calendar_date(run_date):
                raise RetentionError(f"不正な日付を含むエントリです（削除しません）: {rel}")
            if kind == "dir" and not entry.is_dir():
                raise RetentionError(f"ディレクトリのはずがディレクトリではありません: {rel}")
            if kind == "file" and not entry.is_file():
                raise RetentionError(f"通常ファイルのはずが通常ファイルではありません: {rel}")
            if run_date > current_run_date:
                raise RetentionError(
                    f"現在RUN_DATE({current_run_date})より新しい成果物があります（削除しません）: {rel}"
                )

            artifacts.append(
                {
                    "step": step,
                    "run_date": run_date,
                    "kind": kind,
                    "relative_path": rel,
                    "path": entry,
                }
            )

    if unknown:
        for rel in unknown[:3]:
            logger.error(f"[NG] 認識できないエントリ: {rel}")
        raise RetentionError(
            f"認識できないエントリが{len(unknown)}件あります（削除開始前に停止しました）"
        )

    return artifacts, holds


def collect_regular_files(artifact: Dict[str, Any]) -> Tuple[List[Path], List[Path], int]:
    """
    削除候補配下のregular fileとディレクトリを収集する。
    symlink / regular file以外 を検出したらRetentionErrorを送出する。
    """
    path: Path = artifact["path"]
    files: List[Path] = []
    dirs: List[Path] = []
    total_bytes = 0

    if artifact["kind"] == "file":
        if path.is_symlink() or not path.is_file():
            raise RetentionError(f"regular fileではありません: {artifact['relative_path']}")
        total_bytes += path.stat().st_size
        return [path], [], total_bytes

    def _walk_error(exc: OSError) -> None:
        # permission denied / I/O error 等で対象が黙って欠落しないよう即FAILさせる
        raise RetentionError(
            f"削除候補の走査に失敗しました（1ファイルも削除しません）: "
            f"{artifact['relative_path']} ({exc})"
        )

    for dirpath, dirnames, filenames in os.walk(str(path), followlinks=False, onerror=_walk_error):
        current = Path(dirpath)
        dirs.append(current)
        for name in sorted(dirnames):
            child = current / name
            if child.is_symlink():
                raise RetentionError(f"symlinkディレクトリを検出しました: {child}")
        for name in sorted(filenames):
            child = current / name
            if child.is_symlink():
                raise RetentionError(f"symlinkファイルを検出しました: {child}")
            if not child.is_file():
                raise RetentionError(f"regular fileではないエントリを検出しました: {child}")
            total_bytes += child.stat().st_size
            files.append(child)

    return files, dirs, total_bytes


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="削除せず削除候補の集計だけ行う")
    mode.add_argument("--apply", action="store_true", help="validation済みの削除候補を実際に削除する")
    parser.add_argument(
        "--run-date",
        default=os.environ.get("RUN_DATE") or datetime.now().strftime("%Y%m%d"),
        help="今回RUN_DATE（YYYYMMDD）。既定は環境変数RUN_DATE、無ければ本日",
    )
    parser.add_argument(
        "--pipeline-root",
        default=str(project_root),
        help="Pipeline root（focused test用）",
    )
    parser.add_argument("--bucket", default=None, help="status用S3 bucket（既定は設定ファイル）")
    parser.add_argument("--base-prefix", default=None, help="focused test用。productionではcanonical値以外を拒否")
    parser.add_argument("--status-prefix", default=None, help="status prefix（既定は設定ファイル）")
    parser.add_argument("--region", default=None, help="AWS region（既定は設定ファイル）")
    parser.add_argument(
        "--root-zip-only",
        action="store_true",
        help="cutover時のproduction個別検証用。ローカル09成果物は変更せずroot ZIPだけ処理する",
    )
    return parser.parse_args()


def run(args: argparse.Namespace, logger) -> Dict[str, Any]:
    current_run_date = args.run_date
    if not RUN_DATE_RE.match(current_run_date) or not _is_valid_calendar_date(current_run_date):
        raise RetentionError(f"RUN_DATEが不正です: {current_run_date}")

    root = Path(args.pipeline_root).resolve()
    if not root.is_dir():
        raise RetentionError(f"pipeline rootが存在しません: {root}")

    config = load_pipeline_s3_config()
    base_prefix = getattr(args, "base_prefix", None) or get_config_value(
        config, "PIPELINE_S3_BASE_PREFIX"
    )
    if args.bucket and args.status_prefix and args.region:
        bucket = args.bucket
        status_prefix = args.status_prefix
        region = args.region
    else:
        bucket = args.bucket or get_config_value(config, "PIPELINE_S3_BUCKET")
        status_prefix = args.status_prefix or (
            f"{base_prefix}/{get_config_value(config, 'PIPELINE_STATUS_PREFIX')}"
        )
        region = args.region or get_config_value(config, "PIPELINE_AWS_REGION")

    # root ZIPのS3参照より前に、config/env/CLIの値をcanonical値と完全一致検証する。
    lock_root_distribution_route(bucket, base_prefix)

    logger.info(f"mode={'apply' if args.apply else 'dry-run'} / RUN_DATE={current_run_date}")
    logger.info(f"status正本: s3://{bucket}/{status_prefix}/ (region={region})")

    root_zip_only = getattr(args, "root_zip_only", False)
    artifacts, holds = ([], []) if root_zip_only else scan_artifacts(root, current_run_date, logger)
    all_run_dates = sorted({a["run_date"] for a in artifacts})
    older_run_dates = [d for d in all_run_dates if d < current_run_date]

    s3_client = build_s3_client(region)
    previous_run_date = resolve_previous_successful_run_date(
        s3_client, bucket, status_prefix, current_run_date, logger
    )

    if previous_run_date is None:
        if older_run_dates:
            raise RetentionError(
                "直前の正常終了RUN_DATEをS3 statusから決定できませんでした。"
                f"過去成果物が{len(older_run_dates)}RUN_DATE分あるため削除せず停止します"
            )
        logger.warn("直前の正常終了RUN_DATEなし。過去成果物も無いため削除0件で継続します")

    keep_run_dates = sorted({current_run_date} | ({previous_run_date} if previous_run_date else set()))
    logger.info(f"保持RUN_DATE: {', '.join(keep_run_dates)}")

    root_zip = execute_root_distribution_zip_retention(
        s3_client,
        bucket,
        base_prefix,
        current_run_date,
        [previous_run_date] if previous_run_date else [],
        False,
        logger,
    )

    delete_artifacts = [a for a in artifacts if a["run_date"] not in keep_run_dates]

    # 削除開始前に全候補をvalidationする（1件でも異常なら何も削除しない）
    plans: List[Dict[str, Any]] = []
    for artifact in sorted(delete_artifacts, key=lambda a: a["relative_path"]):
        files, dirs, total_bytes = collect_regular_files(artifact)
        plans.append(
            {
                "artifact": artifact,
                "files": files,
                "dirs": dirs,
                "file_count": len(files),
                "total_bytes": total_bytes,
            }
        )

    by_group: Dict[Tuple[str, str], Dict[str, int]] = {}
    for plan in plans:
        key = (plan["artifact"]["step"], plan["artifact"]["run_date"])
        bucket_entry = by_group.setdefault(key, {"file_count": 0, "total_bytes": 0, "entry_count": 0})
        bucket_entry["file_count"] += plan["file_count"]
        bucket_entry["total_bytes"] += plan["total_bytes"]
        bucket_entry["entry_count"] += 1

    planned_files = sum(plan["file_count"] for plan in plans)
    planned_bytes = sum(plan["total_bytes"] for plan in plans)

    deleted_files = 0
    deleted_bytes = 0
    removed_dirs = 0

    if args.apply:
        # local / S3の全候補validation完了後に初めて削除を開始する。
        root_zip = execute_root_distribution_zip_retention(
            s3_client,
            bucket,
            base_prefix,
            current_run_date,
            [previous_run_date] if previous_run_date else [],
            True,
            logger,
        )
        for plan in plans:
            for file_path in plan["files"]:
                if file_path.is_symlink() or not file_path.is_file():
                    raise RetentionError(f"削除直前の再検証に失敗しました: {file_path}")
                size = file_path.stat().st_size
                file_path.unlink()
                deleted_files += 1
                deleted_bytes += size
            # 空ディレクトリだけを深い順に後処理する
            for dir_path in sorted(plan["dirs"], key=lambda p: len(p.parts), reverse=True):
                try:
                    dir_path.rmdir()
                    removed_dirs += 1
                except OSError as exc:
                    raise RetentionError(f"ディレクトリを削除できませんでした: {dir_path} ({exc})") from exc
        logger.ok(f"削除完了: files={deleted_files} / dirs={removed_dirs} / bytes={deleted_bytes}")
    else:
        logger.info(f"dry-runのため削除しません（削除候補 files={planned_files} bytes={planned_bytes}）")

    summary = {
        "step": STEP_NAME,
        "mode": "apply" if args.apply else "dry-run",
        "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline_root": str(root),
        "root_zip_only": root_zip_only,
        "current_run_date": current_run_date,
        "previous_successful_run_date": previous_run_date,
        "previous_successful_run_date_source": "s3_pipeline_status",
        "keep_run_dates": keep_run_dates,
        "status_s3_uri": f"s3://{bucket}/{status_prefix}/",
        "artifact_run_dates": all_run_dates,
        "hold_entry_count": len(holds),
        "hold_entries": holds,
        "planned_entry_count": len(plans),
        "planned_delete_files": planned_files,
        "planned_delete_bytes": planned_bytes,
        "deleted_files": deleted_files,
        "deleted_bytes": deleted_bytes,
        "removed_dirs": removed_dirs,
        "delete_breakdown": [
            {
                "step": step,
                "run_date": run_date,
                "entry_count": values["entry_count"],
                "file_count": values["file_count"],
                "total_bytes": values["total_bytes"],
            }
            for (step, run_date), values in sorted(by_group.items())
        ],
        "root_zip": root_zip,
    }
    return summary


def main() -> int:
    logger = get_logger(STEP_NAME)
    args = parse_args()
    started = time.time()
    dirs = ensure_result_dirs(str(STEP_DIR))
    summary_path = dirs["result"] / SUMMARY_FILENAME

    try:
        summary = run(args, logger)
    except RetentionError as exc:
        logger.error(f"[NG] {exc}")
        return 1
    except Exception as exc:  # noqa: BLE001 - 想定外例外も握りつぶさずFAILさせる
        logger.error(f"[NG] 想定外エラー: {type(exc).__name__}: {exc}")
        return 1

    # summaryは固定名で上書きし、80-7自身の世代を累積させない
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")

    for row in summary["delete_breakdown"][:3]:
        logger.info(
            f"削除候補(代表): {row['step']} {row['run_date']} "
            f"files={row['file_count']} bytes={row['total_bytes']}"
        )
    logger.info(f"summary: {summary_path}")

    write_execution_time(
        str(dirs["execution_time"]),
        STEP_NAME,
        time.time() - started,
        record_count=summary["planned_delete_files"],
    )
    logger.ok("完了")
    return 0


if __name__ == "__main__":
    sys.exit(main())

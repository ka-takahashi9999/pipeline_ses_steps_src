#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 01-1: Gmail取得スクリプト

Gmail APIで対象メールを取得し、1行1レコードのJSONLとして保存する。

認証情報: AWS SSM Parameter Store /gmail/credentials
  {"client_id":"...","client_secret":"...","refresh_token":"..."}

出力: 01_result/fetch_gmail_mail_master.jsonl

使い方:
  python3 fetch_gmail.py --query 'newer_than:1d'
  python3 fetch_gmail.py --after 2025/09/01 --before 2025/09/30 --max 500
"""

import argparse
import base64
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# common モジュールのパス解決
_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import write_jsonl
from common.logger import get_logger

STEP_NAME = "01-1_fetch_gmail"
logger = get_logger(STEP_NAME)

# 出力ファイル名
OUTPUT_FILENAME = "fetch_gmail_mail_master.jsonl"
SSM_PARAM_NAME = "/gmail/credentials"
AWS_REGION = "ap-northeast-1"


# ---------------------------------------------------------------------------
# 認証
# ---------------------------------------------------------------------------

def initialize_gmail_service():
    """SSMからGmail認証情報を取得し、Gmail APIクライアントを初期化する。"""
    logger.info("SSMからGmail認証情報を取得します")
    ssm = boto3.client("ssm", region_name=AWS_REGION)
    param = ssm.get_parameter(Name=SSM_PARAM_NAME, WithDecryption=True)
    creds_info = json.loads(param["Parameter"]["Value"])

    creds = Credentials(
        token=None,
        refresh_token=creds_info["refresh_token"],
        client_id=creds_info["client_id"],
        client_secret=creds_info["client_secret"],
        token_uri="https://oauth2.googleapis.com/token",
    )
    service = build("gmail", "v1", credentials=creds, cache_discovery=False)
    logger.info("Gmail APIクライアントの初期化完了")
    return service


# ---------------------------------------------------------------------------
# メール解析
# ---------------------------------------------------------------------------

def b64url_decode(s: str) -> bytes:
    """Base64URLデコード（パディング補完付き）。"""
    s = s.replace("-", "+").replace("_", "/")
    pad = len(s) % 4
    if pad:
        s += "=" * (4 - pad)
    return base64.b64decode(s)


def html_to_text(html: str) -> str:
    """HTMLを最低限のプレーンテキストに変換する。"""
    html = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", html)
    html = re.sub(r"(?is)<br\s*/?>", "\n", html)
    html = re.sub(r"(?is)</p>", "\n", html)
    text = re.sub(r"(?is)<.*?>", "", html)
    text = re.sub(r"\r\n|\r", "\n", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def extract_headers(payload_headers: List[Dict[str, str]]) -> Dict[str, Any]:
    """メールヘッダーから必要フィールドを抽出する。null禁止のためデフォルト値を設定。"""
    h = {x.get("name", "").lower(): x.get("value", "") for x in (payload_headers or [])}

    to_list: List[str] = []
    if h.get("to"):
        to_list = [addr.strip() for addr in re.split(r",\s*", h["to"]) if addr.strip()]

    return {
        "subject": h.get("subject") or "",
        "from": h.get("from") or "",
        "to": to_list,
        "cc": h.get("cc") or "",
        "reply_to": h.get("reply-to") or h.get("reply_to") or "",
        "date": h.get("date") or "",
    }


def walk_parts(
    part: Dict[str, Any],
    plain_texts: List[str],
    html_texts: List[str],
    attachments: List[Dict[str, Any]],
) -> None:
    """MIMEパートを再帰的に走査してテキスト・添付情報を収集する。"""
    mime = part.get("mimeType", "")
    body = part.get("body") or {}
    data = body.get("data")
    att_id = body.get("attachmentId")
    filename = part.get("filename") or ""

    if att_id and filename:
        attachments.append({
            "filename": filename,
            "attachment_id": att_id,
            "mime_type": mime,
            "size": body.get("size") or 0,
        })
    elif filename and data and not att_id:
        attachments.append({
            "filename": filename,
            "data": data,
            "mime_type": mime,
            "size": body.get("size") or 0,
        })
    elif mime.startswith("text/plain") and data:
        plain_texts.append(b64url_decode(data).decode("utf-8", errors="replace"))
    elif mime.startswith("text/html") and data:
        html_texts.append(html_to_text(b64url_decode(data).decode("utf-8", errors="replace")))

    for sub in (part.get("parts") or []):
        walk_parts(sub, plain_texts, html_texts, attachments)


def extract_body_and_attachments(
    payload: Dict[str, Any],
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    メールペイロードから本文テキストと添付ファイル情報を抽出する。
    text/plain優先、なければtext/htmlをテキスト化する。
    """
    plain_texts: List[str] = []
    html_texts: List[str] = []
    attachments: List[Dict[str, Any]] = []

    if payload.get("parts"):
        for part in payload["parts"]:
            walk_parts(part, plain_texts, html_texts, attachments)
    else:
        body = payload.get("body") or {}
        data = body.get("data")
        mime = payload.get("mimeType", "")
        if mime.startswith("text/plain") and data:
            plain_texts.append(b64url_decode(data).decode("utf-8", errors="replace"))
        elif mime.startswith("text/html") and data:
            html_texts.append(html_to_text(b64url_decode(data).decode("utf-8", errors="replace")))

    if plain_texts:
        body_text = "\n\n".join(t for t in plain_texts if t).strip()
    else:
        body_text = "\n\n".join(t for t in html_texts if t).strip()

    body_text = re.sub(r"[ \t]+\n", "\n", body_text)
    return body_text, attachments


def download_attachment_data(
    service: Any,
    message_id: str,
    attachment_id: str,
    filename: str,
) -> str:
    """添付ファイルのBase64データをダウンロードして返す。失敗時は空文字。"""
    try:
        att_resp = service.users().messages().attachments().get(
            userId="me",
            messageId=message_id,
            id=attachment_id,
        ).execute()
        return att_resp.get("data") or ""
    except Exception as e:
        logger.warn(f"添付ファイルDL失敗 filename={filename}: {e}", message_id=message_id)
        return ""


# ---------------------------------------------------------------------------
# クエリ構築
# ---------------------------------------------------------------------------

def build_query(args: argparse.Namespace) -> str:
    """コマンド引数からGmail検索クエリを構築する。"""
    if args.query:
        return args.query
    q = "in:anywhere"
    if args.after:
        q += f" after:{args.after}"
    if args.before:
        q += f" before:{args.before}"
    return q


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def fetch_all_messages(
    service: Any,
    query: str,
    max_count: int,
) -> List[Dict[str, Any]]:
    """指定クエリでメール一覧を取得し、全件のメッセージIDリストを返す。"""
    message_ids: List[str] = []
    next_page: Optional[str] = None

    while len(message_ids) < max_count:
        fetch_limit = min(100, max_count - len(message_ids))
        resp = service.users().messages().list(
            userId="me",
            q=query,
            pageToken=next_page,
            maxResults=fetch_limit,
        ).execute()

        msgs = resp.get("messages") or []
        for m in msgs:
            mid = m.get("id")
            if mid:
                message_ids.append(mid)
            if len(message_ids) >= max_count:
                break

        next_page = resp.get("nextPageToken")
        if not next_page or not msgs:
            break

    return message_ids


def build_record(
    service: Any,
    message_id: str,
) -> Optional[Dict[str, Any]]:
    """メッセージIDから完全なメールレコードを構築する。"""
    full = service.users().messages().get(
        userId="me",
        id=message_id,
        format="full",
    ).execute()

    payload = full.get("payload") or {}
    headers = extract_headers(payload.get("headers") or [])
    body_text, attachments = extract_body_and_attachments(payload)

    # 添付ファイルデータをダウンロード
    for att in attachments:
        att_id = att.pop("attachment_id", None)
        if att_id:
            att["data"] = download_attachment_data(
                service, message_id, att_id, att.get("filename", "")
            )

    return {
        "message_id": full.get("id") or "",
        "thread_id": full.get("threadId") or "",
        "subject": headers["subject"],
        "from": headers["from"],
        "to": headers["to"],
        "cc": headers["cc"],
        "reply_to": headers["reply_to"],
        "date": headers["date"],
        "body_text": body_text,
        "attachments": attachments,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Gmail取得スクリプト")
    ap.add_argument("--query", help="Gmail検索クエリ（例: 'newer_than:1d'）")
    ap.add_argument("--after", help="取得開始日 YYYY/MM/DD")
    ap.add_argument("--before", help="取得終了日 YYYY/MM/DD")
    ap.add_argument("--max", type=int, default=500, help="最大取得件数")
    ap.add_argument(
        "--out",
        default=str(_STEP_DIR / "01_result" / OUTPUT_FILENAME),
        help="出力JSONLパス",
    )
    args = ap.parse_args()

    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = str(dirs["result"])

    start_time = time.time()
    records: List[Dict[str, Any]] = []

    try:
        service = initialize_gmail_service()
        query = build_query(args)
        logger.info(f"Gmailクエリ: {query} (max={args.max})")

        message_ids = fetch_all_messages(service, query, args.max)
        logger.info(f"メッセージID取得件数: {len(message_ids)}")

        for i, mid in enumerate(message_ids, 1):
            try:
                rec = build_record(service, mid)
                if rec:
                    records.append(rec)
                    logger.ok(f"取得完了 ({i}/{len(message_ids)})", message_id=mid)
            except HttpError as e:
                logger.warn(f"メッセージ取得失敗: {e}", message_id=mid)

        write_jsonl(args.out, records)
        logger.ok(f"{len(records)}件を書き込みました: {args.out}")

    except Exception as e:
        write_error_log(result_dir, e, context=f"query={build_query(args)}")
        logger.error(f"処理失敗: {e}")
        sys.exit(1)

    finally:
        elapsed = time.time() - start_time
        write_execution_time(
            str(dirs["execution_time"]),
            STEP_NAME,
            elapsed,
            record_count=len(records),
        )

    logger.ok(f"Step完了: {len(records)}件")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Build the mail-master-compatible overlay for one derived item."""

from typing import Any, Dict


MAIL_MASTER_KEYS = {
    "message_id",
    "thread_id",
    "date",
    "from",
    "to",
    "cc",
    "reply_to",
    "subject",
    "body_text",
    "attachments",
    "html_links",
}


def build_canonical_overlay(source_mail: Dict[str, Any], item: Dict[str, Any]) -> Dict[str, Any]:
    """Return only the existing mail-master fields; audit metadata stays separate."""
    return {
        "message_id": item["derived_item_id"],
        "thread_id": source_mail.get("thread_id", ""),
        "date": source_mail.get("date", ""),
        "from": source_mail.get("from", ""),
        "to": source_mail.get("to", []),
        "cc": source_mail.get("cc", ""),
        "reply_to": source_mail.get("reply_to", ""),
        "subject": item["canonical_subject"],
        "body_text": item["body_text"],
        "attachments": list(item.get("attachments", [])),
        "html_links": item.get("html_links", []),
    }

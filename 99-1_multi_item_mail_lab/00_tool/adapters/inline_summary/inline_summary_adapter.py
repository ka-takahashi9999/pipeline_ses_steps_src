#!/usr/bin/env python3
"""Config-driven deterministic adapter for inline multi-item mail bodies."""

import json
import re
from dataclasses import dataclass
from email.utils import parseaddr
from pathlib import Path
from typing import Any, Dict, List, Match, Optional, Pattern, Tuple

from identity import (
    canonical_subject,
    content_fingerprint,
    derived_item_id,
    logical_item_id,
    normalize_block_identifier,
    normalize_content,
)


ADAPTER_ID = "inline_summary"
ADAPTER_VERSION = "1.0.0"
VALID_STATUSES = {"PARSED", "PARTIAL", "UNSUPPORTED", "HUMAN_REVIEW"}


@dataclass(frozen=True)
class ParseResult:
    status: str
    reasons: List[str]
    items: List[Dict[str, Any]]


class InlineSummaryAdapter:
    """Split anchor-delimited items and map one attachment to each item."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._anchor_regex: Pattern[str] = re.compile(config["item_start_regex"])
        self._separator_regex: Pattern[str] = re.compile(config["separator_regex"])
        self._footer_regex: Pattern[str] = re.compile(config["footer_end_regex"])
        self._subject_regex: Pattern[str] = re.compile(
            config["selectors"]["subject_regex"], re.IGNORECASE
        )

    @classmethod
    def from_file(cls, path: Path) -> "InlineSummaryAdapter":
        with path.open(encoding="utf-8") as file_object:
            return cls(json.load(file_object))

    def matches(self, mail: Dict[str, Any]) -> bool:
        sender = parseaddr(str(mail.get("from", "")))[1]
        sender_domain = sender.rsplit("@", 1)[-1].casefold() if "@" in sender else ""
        expected_domain = self.config["selectors"]["sender_domain"].casefold()
        return sender_domain == expected_domain and bool(
            self._subject_regex.search(str(mail.get("subject", "")))
        )

    @staticmethod
    def _normalize_newlines(value: str) -> str:
        return (value or "").replace("\r\n", "\n").replace("\r", "\n")

    def _split_blocks(
        self,
        body_text: str,
        anchors: List[Match[str]],
    ) -> Tuple[List[str], List[str]]:
        separators = list(self._separator_regex.finditer(body_text))
        reasons: List[str] = []
        blocks: List[str] = []
        for index, anchor in enumerate(anchors):
            if index + 1 < len(anchors):
                next_anchor_start = anchors[index + 1].start()
                boundary_candidates = [
                    match.start()
                    for match in separators
                    if anchor.end() <= match.start() < next_anchor_start
                ]
                if not boundary_candidates:
                    reasons.append(f"item_{index + 1}:separator_before_next_anchor_missing")
                    continue
                end = boundary_candidates[-1]
            else:
                footer_match = self._footer_regex.search(body_text, anchor.end())
                if footer_match is None:
                    reasons.append("final_item:footer_end_marker_missing")
                    continue
                end = footer_match.start()

            block = normalize_content(body_text[anchor.start() : end])
            missing_markers = [
                marker
                for marker in self.config["required_profile_markers"]
                if marker not in block
            ]
            if missing_markers:
                reasons.append(
                    f"item_{index + 1}:required_markers_missing:"
                    + ",".join(missing_markers)
                )
                continue
            blocks.append(block)
        return blocks, reasons

    def _map_attachments(
        self,
        anchors: List[Match[str]],
        attachments: List[Dict[str, Any]],
    ) -> Tuple[List[Tuple[int, Dict[str, Any]]], List[str], bool]:
        expected_count = self.config["expected_item_count"]
        if len(attachments) != expected_count:
            return [], [f"attachment_count:{len(attachments)}:expected:{expected_count}"], False

        mapped: List[Tuple[int, Dict[str, Any]]] = []
        reasons: List[str] = []
        used_indices = set()
        for item_index, anchor in enumerate(anchors, 1):
            identifier = normalize_block_identifier(anchor.group("identifier"))
            candidates = []
            for attachment_index, attachment in enumerate(attachments):
                filename = str(attachment.get("filename", ""))
                normalized_filename = normalize_block_identifier(filename)
                if identifier and identifier in normalized_filename:
                    candidates.append(attachment_index)
            if len(candidates) != 1:
                reasons.append(
                    f"item_{item_index}:attachment_candidates:{len(candidates)}"
                )
                continue
            attachment_index = candidates[0]
            if attachment_index in used_indices:
                reasons.append(
                    f"item_{item_index}:attachment_reused:{attachment_index}"
                )
                continue
            used_indices.add(attachment_index)
            mapped.append((attachment_index, attachments[attachment_index]))

        is_ambiguous = bool(reasons) or len(used_indices) != expected_count
        if len(used_indices) != expected_count:
            reasons.append(
                f"attachment_one_to_one:{len(used_indices)}:expected:{expected_count}"
            )
        return mapped, reasons, is_ambiguous

    def parse(self, mail: Dict[str, Any]) -> ParseResult:
        if not self.matches(mail):
            return ParseResult("UNSUPPORTED", ["company_selector_mismatch"], [])

        body_text = self._normalize_newlines(str(mail.get("body_text", "")))
        anchors = list(self._anchor_regex.finditer(body_text))
        expected_count = self.config["expected_item_count"]
        if len(anchors) != expected_count:
            return ParseResult(
                "PARTIAL",
                [f"anchor_count:{len(anchors)}:expected:{expected_count}"],
                [],
            )

        blocks, split_reasons = self._split_blocks(body_text, anchors)
        if split_reasons or len(blocks) != expected_count:
            reasons = split_reasons + [
                f"block_count:{len(blocks)}:expected:{expected_count}"
            ]
            return ParseResult("PARTIAL", reasons, [])

        attachments = mail.get("attachments", [])
        if not isinstance(attachments, list):
            return ParseResult("PARTIAL", ["attachments:not_a_list"], [])
        mappings, mapping_reasons, ambiguous = self._map_attachments(anchors, attachments)
        if ambiguous:
            return ParseResult("HUMAN_REVIEW", mapping_reasons, [])

        items: List[Dict[str, Any]] = []
        for item_index, (anchor, block, mapping) in enumerate(
            zip(anchors, blocks, mappings), 1
        ):
            attachment_index, attachment = mapping
            block_identifier = anchor.group("identifier")
            logical_id = logical_item_id(
                self.config["source_company"],
                self.config["item_type"],
                block_identifier,
            )
            fingerprint = content_fingerprint(block)
            derived_id = derived_item_id(logical_id, fingerprint)
            items.append(
                {
                    "original_message_id": str(mail.get("message_id", "")),
                    "logical_item_id": logical_id,
                    "derived_item_id": derived_id,
                    "item_index": item_index,
                    "item_type": self.config["item_type"],
                    "body_text": block,
                    "content_fingerprint": fingerprint,
                    "canonical_subject": canonical_subject(
                        self.config["source_company"],
                        self.config["item_type"],
                        fingerprint,
                    ),
                    "attachment": attachment,
                    "attachment_mapping": {
                        "status": "MAPPED",
                        "rule": self.config["attachment_mapping"]["strategy"],
                        "attachment_index": attachment_index,
                        "filename": str(attachment.get("filename", "")),
                    },
                    "html_links": [],
                }
            )

        derived_ids = [item["derived_item_id"] for item in items]
        if len(set(derived_ids)) != len(derived_ids):
            raise ValueError("duplicate derived_item_id within one source mail")
        return ParseResult("PARSED", [], items)

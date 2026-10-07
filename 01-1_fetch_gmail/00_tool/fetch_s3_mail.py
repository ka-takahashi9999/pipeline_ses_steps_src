#!/usr/bin/env python3
"""Read SES MIME objects (default safety limit 10); never write to S3 or call Gmail.

Selection uses LastModified in [after JST midnight, before JST midnight),
also bounded by start time. --manifest freezes the selection; --replay uses
the restricted local raw cache. Failures preserve the previous output.
The canonical Pipeline route must use --canonical-output and
--require-complete so an empty or truncated day can never be published.
"""
import argparse
import base64
import codecs
import hashlib
import mimetypes
import os
import quopri
import re
import sys
from datetime import datetime, timedelta, timezone
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
from pathlib import Path

import boto3
from botocore.config import Config

STEP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STEP.parent))
from common.json_utils import read_jsonl_as_list, write_jsonl
from fetch_gmail import html_to_text, extract_html_links

BUCKET = "technoverse-ses-mail"
PREFIX = "raw/"
SAMPLE = STEP / "02_confirm/s3_sample"
MAX_BYTES = 25 * 1024 * 1024
JST = timezone(timedelta(hours=9))
# RFC2047 is often used in quoted display names/filenames. Python recovers
# these without loss; keep the exact known syntax warning in provenance.
RECOVERABLE_HEADER_WARNINGS = {
    "encoded word inside quoted string",
    "missing trailing whitespace after encoded-word",
    "missing whitespace before encoded word",
}


def has_decode_replacement_or_surrogate(value):
    """Return true when a parsed header value contains a lossy marker."""
    return "\ufffd" in value or any(0xD800 <= ord(char) <= 0xDFFF for char in value)


def usable_attachment_filename(value):
    """Accept a decoded attachment name only when it is safe and lossless."""
    if not isinstance(value, str) or not value.strip():
        return False
    if (has_decode_replacement_or_surrogate(value)
            or re.search(r"[\x00-\x1f\x7f]", value)
            or "/" in value or "\\" in value
            or re.search(r"=\?[^?\s]+\?[bBqQ]\?[^?]*\?=", value)):
        return False
    return True


def fallback_attachment_filename(number, mime, original_filename=None):
    """Build a deterministic safe name while preserving a MIME-derived suffix."""
    if mime == "message/rfc822":
        extension = ".eml"
    else:
        extension = mimetypes.guess_extension(mime, strict=False) or ".bin"
    suffix_match = re.search(r"\.([A-Za-z0-9]{1,10})$", original_filename or "")
    safe_suffixes = {"pdf", "xls", "xlsx", "doc", "docx", "csv", "txt", "zip", "eml"}
    if (extension == ".bin" and suffix_match
            and suffix_match.group(1).lower() in safe_suffixes):
        extension = "." + suffix_match.group(1).lower()
    if not re.fullmatch(r"\.[A-Za-z0-9]{1,10}", extension):
        extension = ".bin"
    return "part_{}{}".format(number, extension.lower())


def transfer_payload_verified(part, data):
    """Verify decoded bytes for transfer encodings that can silently recover."""
    if data is None:
        return False
    encoding = (part.get("Content-Transfer-Encoding", "") or "").strip().lower()
    if encoding in ("", "7bit", "8bit", "binary"):
        # BytesParser stores the original leaf payload as an ASCII string with
        # non-ASCII bytes represented by surrogateescape. get_payload(False)
        # converts that representation to display Unicode under Python 3.9,
        # so use the retained source representation for an exact byte check.
        payload = getattr(part, "_payload", None)
        if not isinstance(payload, str):
            return False
        try:
            return payload.encode("ascii", errors="surrogateescape") == data
        except UnicodeEncodeError:
            return False
    payload = part.get_payload(decode=False)
    if not isinstance(payload, str):
        return False
    try:
        payload_bytes = payload.encode("ascii", errors="surrogateescape")
    except UnicodeEncodeError:
        return False
    if encoding == "base64":
        try:
            compact = b"".join(payload_bytes.split())
            return base64.b64decode(compact, validate=True) == data
        except (ValueError, base64.binascii.Error):
            return False
    if encoding == "quoted-printable":
        return quopri.decodestring(payload_bytes) == data
    return False


def recoverable_attachment_filename_warning(part, name, raw_value, defects,
                                            payload_verified):
    """Classify only a byte-preserving raw UTF-8 attachment filename warning."""
    targets = {"content-disposition": "filename", "content-type": "name"}
    parameter = targets.get(name.lower())
    if (parameter is None or not payload_verified or part.get_filename() is None
            or len(defects) != 1
            or type(defects[0]).__name__ != "UndecodableBytesDefect"):
        return None
    filename = part.get_filename()
    if has_decode_replacement_or_surrogate(filename):
        return None
    pattern = (r"(?:^|;)\s*" + re.escape(parameter)
               + r"\s*=\s*(?:\"((?:[^\"\\]|\\.)*)\"|([^;\s]*))")
    matches = list(re.finditer(pattern, raw_value, flags=re.IGNORECASE))
    if len(matches) != 1:
        return None
    raw_parameter = (matches[0].group(1) if matches[0].group(1) is not None
                     else matches[0].group(2))
    # Quoted-pair interpretation is intentionally outside this narrow fix.
    if "\\" in raw_parameter:
        return None
    try:
        parameter_bytes = raw_parameter.encode("ascii", errors="surrogateescape")
        if not any(byte >= 0x80 for byte in parameter_bytes):
            return None
        decoded = parameter_bytes.decode("utf-8", errors="strict")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return None
    parsed_header = part.get(name)
    parsed_parameter = getattr(parsed_header, "params", {}).get(parameter)
    if (decoded != parsed_parameter or decoded != filename
            or has_decode_replacement_or_surrogate(str(parsed_header))):
        return None
    return "recovered_raw_utf8_attachment_{}_{}".format(name.lower(), parameter)


def recoverable_unused_message_id_warning(part, name, raw_value, defects,
                                          entry, is_root):
    """Classify syntax-only defects in the unused original Message-ID."""
    if (not is_root or name.lower() != "message-id" or not defects
            or any(type(defect).__name__ != "InvalidHeaderDefect"
                   for defect in defects)):
        return None
    raw_message_ids = [value for header, value in part.raw_items()
                       if header.lower() == "message-id"]
    if len(raw_message_ids) != 1 or raw_message_ids[0] != raw_value:
        return None
    if entry.get("message_id") != identity(entry.get("key", "")):
        return None
    try:
        raw_bytes = raw_value.encode("ascii", errors="strict")
    except UnicodeEncodeError:
        return None
    if (has_decode_replacement_or_surrogate(raw_value)
            or has_decode_replacement_or_surrogate(str(part.get(name)))
            or any(byte == 0x7f or (byte < 0x20 and byte not in (9, 10, 13))
                   for byte in raw_bytes)):
        return None
    folded_lines = re.split(r"\r?\n", raw_value)
    if any(not line.startswith((" ", "\t")) for line in folded_lines[1:]):
        return None
    return "unused_original_message_id_syntax_part_0"


class SelectionStoppedError(ValueError):
    """Safe-to-log selection failure without mail contents or credentials."""


class FetchStoppedError(RuntimeError):
    """Safe-to-log fetch failure after the candidate count is known."""


def iso(value):
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def identity(key):
    return "s3_" + hashlib.sha256((BUCKET + "/" + key).encode()).hexdigest()


def bounds(after, before):
    values = []
    for value in (after, before):
        date = datetime.strptime(value, "%Y-%m-%d")
        if date.strftime("%Y-%m-%d") != value:
            raise ValueError("date must be YYYY-MM-DD")
        values.append(date.replace(tzinfo=JST).astimezone(timezone.utc))
    if values[0] >= values[1]:
        raise ValueError("after must precede before")
    return values


def select(s3, after, before, maximum, now):
    start, end = bounds(after, before)
    end = min(end, now)
    candidates = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        candidates.extend(o for o in page.get("Contents", [])
                          if o["Size"] > 0 and start <= o["LastModified"] < end)
    chosen = sorted(candidates, key=lambda o: (o["LastModified"], o["Key"]))[-maximum:]
    selection_complete = len(candidates) <= maximum
    return [dict(bucket=BUCKET, key=o["Key"], etag=o["ETag"], size=o["Size"],
                 last_modified=iso(o["LastModified"]), message_id=identity(o["Key"]),
                 selection_start=iso(start), selection_end=iso(end),
                 selection_candidate_count=len(candidates), selection_limit=maximum,
                 selection_complete=selection_complete,
                 status="pending") for o in chosen]


def validate_selection_range(entries, after, before, manifest_path=None, now=None):
    """Bind a frozen manifest to its requested JST day before it is reused."""
    expected_start, expected_end = bounds(after, before)
    if expected_end != expected_start + timedelta(days=1):
        raise ValueError("selection must cover one JST day")
    if manifest_path is not None:
        manifest = Path(manifest_path).absolute()
        if manifest.parent.parent.name != after:
            raise ValueError("manifest directory date does not match selection day")
    if not entries:
        return
    starts = {e.get("selection_start") for e in entries}
    ends = {e.get("selection_end") for e in entries}
    if starts != {iso(expected_start)} or len(ends) != 1:
        raise ValueError("manifest selection bounds do not match requested JST day")
    try:
        actual_end_text = next(iter(ends))
        actual_end = datetime.fromisoformat(actual_end_text.replace("Z", "+00:00"))
        if iso(actual_end) != actual_end_text:
            raise ValueError("noncanonical selection end")
    except (AttributeError, TypeError, ValueError):
        raise ValueError("invalid manifest selection end") from None
    current = now or datetime.now(timezone.utc)
    if not expected_start < actual_end <= expected_end:
        raise ValueError("manifest selection end is outside requested JST day")
    if current >= expected_end and actual_end != expected_end:
        raise ValueError("completed JST day requires a complete selection range")
    for entry in entries:
        try:
            modified_text = entry["last_modified"]
            modified = datetime.fromisoformat(modified_text.replace("Z", "+00:00"))
            if iso(modified) != modified_text:
                raise ValueError("noncanonical S3 LastModified")
        except (KeyError, AttributeError, TypeError, ValueError):
            raise ValueError("invalid S3 LastModified") from None
        if not expected_start <= modified < actual_end:
            raise ValueError("S3 LastModified is outside manifest selection range")


def require_complete_selection(entries, maximum):
    """Stop before S3 Get unless the full selection fits the configured limit."""
    if not entries:
        raise SelectionStoppedError(
            "candidate_count=0 limit={} stop_reason=empty_selection".format(maximum))
    candidate_counts = {e.get("selection_candidate_count") for e in entries}
    selection_limits = {e.get("selection_limit") for e in entries}
    complete = all(e.get("selection_complete") is True for e in entries)
    candidate_count = next(iter(candidate_counts)) if len(candidate_counts) == 1 else "unknown"
    if complete and candidate_counts == {len(entries)} and selection_limits == {maximum}:
        return
    if type(candidate_count) is int and candidate_count > maximum:
        reason = "selection_limit_exceeded"
    elif selection_limits != {maximum}:
        reason = "selection_limit_mismatch"
    elif candidate_counts != {len(entries)}:
        reason = "selection_count_mismatch"
    else:
        reason = "selection_not_complete"
    raise SelectionStoppedError(
        "candidate_count={} limit={} stop_reason={}".format(
            candidate_count, maximum, reason))


def parse_mail(raw, entry):
    msg = BytesParser(policy=policy.default).parsebytes(raw)
    issues, plain, html, attachments, sources, warnings = [], [], [], [], [], []
    decoding_sources = []
    part_numbers = {}
    verified_attachment_parts = set()
    verified_body_parts = set()
    attachment_outputs = {}

    def visit(part, number="0"):
        part_numbers[id(part)] = number
        filename = part.get_filename()
        mime = part.get_content_type()
        attached = filename is not None or part.get_content_disposition() == "attachment"
        if attached or mime == "message/rfc822" or (not part.is_multipart() and
                                                    mime not in ("text/plain", "text/html")):
            data = part.get_payload(decode=True)
            if not transfer_payload_verified(part, data):
                # email's list payload has lost the original byte representation.
                # Retain a review candidate and the raw MIME, but never certify
                # regenerated attachments using their own length as evidence.
                issue = ("attachment_original_bytes_unverified_part_" if data is None
                         else "attachment_payload_decode_unverified_part_")
                issues.append(issue + number)
                if data is None:
                    payload = part.get_payload()
                    data = b"\r\n".join(p.as_bytes(policy=policy.SMTP) for p in payload) if isinstance(payload, list) else str(payload).encode()
            else:
                verified_attachment_parts.add(id(part))
            source_index = len(sources)
            sources.append("mime_filename" if filename else "default_part_number")
            attachment = dict(
                filename=filename or fallback_attachment_filename(number, mime),
                data=base64.urlsafe_b64encode(data).decode("ascii"),
                mime_type=mime, size=len(data))
            attachments.append(attachment)
            attachment_outputs[id(part)] = (attachment, source_index, number, mime)
            return
        if part.is_multipart():
            for i, child in enumerate(part.iter_parts()):
                visit(child, number + "_" + str(i))
            return
        decoded = part.get_payload(decode=True)
        data = decoded or b""
        transfer_verified = transfer_payload_verified(part, decoded)
        if not transfer_verified:
            issues.append("body_payload_decode_unverified_part_" + number)
        charset = part.get_content_charset() or "ascii"
        try:
            text = data.decode(charset, errors="strict")
            if transfer_verified:
                verified_body_parts.add(id(part))
        except (LookupError, UnicodeError):
            try:
                if codecs.lookup(charset).name != "iso2022_jp":
                    raise ValueError("unsupported fallback")
                text = decode_iso2022jp_nec(data)
                decoding_sources.append(dict(part=number, source="iso2022_jp_nec_row13_cp932"))
                if transfer_verified:
                    verified_body_parts.add(id(part))
            except (LookupError, UnicodeError, ValueError):
                issues.append("decode_utf8_replace_part_" + number)
                text = data.decode("utf-8", errors="replace")
        (plain if mime == "text/plain" else html).append(text)

    visit(msg)
    for part in msg.walk():
        number = part_numbers.get(id(part), "unknown")
        issues.extend("mime_{}_part_{}".format(type(d).__name__, number)
                      for d in part.defects)
        for name, raw_value in part.raw_items():
            # Python 3.9 headerregistry may raise TypeError for malformed Date.
            # Date validity is handled by the explicit fallback below.
            if name.lower() == "date":
                continue
            value = part.get(name)
            defects = list(getattr(value, "defects", ()))
            lower_name = name.lower()
            attachment_output = attachment_outputs.get(id(part))
            if not defects:
                if (lower_name in ("content-disposition", "content-type")
                        and attachment_output is not None
                        and id(part) in verified_attachment_parts
                        and not usable_attachment_filename(part.get_filename())):
                    attachment, source_index, part_number, mime = attachment_output
                    attachment["filename"] = fallback_attachment_filename(
                        part_number, mime, part.get_filename())
                    sources[source_index] = "deterministic_fallback_header_defect"
                    warnings.append(
                        "attachment_filename_fallback_{}_part_{}".format(
                            lower_name, number))
                    continue
                if has_decode_replacement_or_surrogate(str(value)):
                    issues.append("header_decode_replacement_{}_part_{}".format(
                        name.lower(), number))
                continue
            recovered_warning = recoverable_attachment_filename_warning(
                part, name, raw_value, defects, id(part) in verified_attachment_parts)
            if recovered_warning:
                warnings.append(recovered_warning + "_part_" + number)
                continue
            recovered_warning = recoverable_unused_message_id_warning(
                part, name, raw_value, defects, entry, part is msg)
            if recovered_warning:
                warnings.append(recovered_warning)
                continue
            if (lower_name in ("content-disposition", "content-type")
                    and attachment_output is not None
                    and id(part) in verified_attachment_parts):
                attachment, source_index, part_number, mime = attachment_output
                filename = part.get_filename()
                if not usable_attachment_filename(filename):
                    attachment["filename"] = fallback_attachment_filename(
                        part_number, mime, filename)
                    sources[source_index] = "deterministic_fallback_header_defect"
                    warnings.append(
                        "attachment_filename_fallback_{}_part_{}".format(
                            lower_name, number))
                else:
                    warnings.append(
                        "attachment_filename_header_format_{}_part_{}".format(
                            lower_name, number))
                continue
            value_text = str(value)
            structural_header = lower_name in {"content-transfer-encoding", "content-type"}
            if (structural_header
                    and id(part) in verified_body_parts | verified_attachment_parts):
                structural_header = False
            if (not structural_header
                    and not has_decode_replacement_or_surrogate(value_text)):
                for defect in defects:
                    if (type(defect).__name__ == "InvalidHeaderDefect"
                            and str(defect) in RECOVERABLE_HEADER_WARNINGS):
                        warnings.append(str(defect))
                    else:
                        warnings.append("header_format_{}_part_{}".format(
                            lower_name, number))
                continue
            for defect in defects:
                if type(defect).__name__ == "InvalidHeaderDefect" and str(defect) in RECOVERABLE_HEADER_WARNINGS:
                    warnings.append(str(defect))
                else:
                    issues.append("header_{}_{}_part_{}".format(
                        type(defect).__name__, name.lower(), number))
            if has_decode_replacement_or_surrogate(str(value)):
                issues.append("header_decode_replacement_{}_part_{}".format(
                    name.lower(), number))
    date_source, date_reason = "mail_date", ""
    original_date = next((v for k, v in msg.raw_items() if k.lower() == "date"), "")
    try:
        date = parsedate_to_datetime(original_date)
        if date.tzinfo is None:
            raise ValueError("timezone missing")
    except (TypeError, ValueError, OverflowError):
        date = datetime.fromisoformat(entry["last_modified"].replace("Z", "+00:00"))
        date_source, date_reason = "s3_last_modified_fallback", "missing_invalid_or_naive_date"
    body = "\n".join(plain) if any(t.strip() for t in plain) else "\n".join(html_to_text(t) for t in html)
    if not body.strip() and not attachments:
        issues.append("empty_body_without_attachment")
    record = dict(message_id=entry["message_id"], thread_id="", subject=str(msg.get("Subject", "")),
                  **{"from": str(msg.get("From", ""))},
                  to=[addr for _, addr in getaddresses([str(v) for v in msg.get_all("To", [])]) if addr],
                  cc=str(msg.get("Cc", "")), reply_to=str(msg.get("Reply-To", "")), date=iso(date),
                  body_text=body, attachments=attachments,
                  html_links=[link for text in html for link in extract_html_links(text)])
    entry.update(date_source=date_source, date_reason=date_reason, original_date=original_date,
                 thread_id_source="default_no_gmail_thread", attachment_count=len(attachments),
                 attachment_filename_sources=sources, issues=issues, header_warnings=warnings,
                 empty_body_reason="attachment_only" if not body.strip() and attachments else "",
                 status="needs_review" if issues else "success")
    if decoding_sources:
        entry["body_decoding_sources"] = decoding_sources
    else:
        entry.pop("body_decoding_sources", None)
    return record


def decode_iso2022jp_nec(data):
    """Accept only NEC row-13 characters whose CP932 mapping round-trips.

    All standard text remains decoded by Python's strict ISO-2022-JP codec.
    Unknown sequences still fail; this never replaces or drops source bytes.
    """
    def recover(error):
        pair = error.object[error.start:error.end]
        prefix = error.object[:error.start]
        escape = prefix.rfind(b"\x1b")
        if (not isinstance(error, UnicodeDecodeError) or len(pair) != 2
                or pair[0] != 0x2d or not 0x21 <= pair[1] <= 0x7e
                or prefix[escape:escape + 3] not in (b"\x1b$B", b"\x1b$@")
                or (error.start - escape - 3) % 2):
            raise error
        trail = pair[1] + 0x1f
        if trail >= 0x7f:
            trail += 1
        encoded = bytes((0x87, trail))
        text = encoded.decode("cp932", errors="strict")
        if text.encode("cp932") != encoded:
            raise error
        return text, error.end
    codecs.register_error("s3_iso2022jp_nec_row13", recover)
    return data.decode("iso2022_jp", errors="s3_iso2022jp_nec_row13")


def validate_records(records, entries, maximum=10, require_complete=False, raw_dir=None,
                     expected_after=None, expected_before=None, manifest_path=None,
                     allow_needs_review=False, review_path=None):
    """Validate the success/review partition without exposing mail contents."""
    if not 1 <= len(entries) <= maximum:
        raise ValueError("selection count mismatch or empty selection")
    selection_ids = [e.get("message_id") for e in entries]
    if any(not isinstance(message_id, str) or not message_id for message_id in selection_ids):
        raise ValueError("missing selection identity")
    if len(set(selection_ids)) != len(entries):
        raise ValueError("duplicate selection")
    statuses = {e.get("status") for e in entries}
    if not statuses <= {"success", "needs_review"}:
        raise ValueError("selection contains failed, pending, or unknown status")
    review_entries = [e for e in entries if e.get("status") == "needs_review"]
    success_entries = [e for e in entries if e.get("status") == "success"]
    if review_entries and not allow_needs_review:
        raise ValueError("needs_review is not enabled")
    if not success_entries:
        raise ValueError("no successful mail to publish")
    record_ids = [record.get("message_id") for record in records]
    if (any(not isinstance(message_id, str) or not message_id for message_id in record_ids)
            or len(set(record_ids)) != len(records)):
        raise ValueError("duplicate or missing master identity")
    success_ids = {e["message_id"] for e in success_entries}
    review_ids = {e["message_id"] for e in review_entries}
    if (set(selection_ids) != success_ids | review_ids
            or success_ids & review_ids
            or set(record_ids) != success_ids):
        raise ValueError("selection/master partition mismatch")
    records_by_id = {record["message_id"]: record for record in records}
    if (expected_after is None) != (expected_before is None):
        raise ValueError("both expected selection bounds are required")
    if expected_after is not None:
        validate_selection_range(entries, expected_after, expected_before,
                                 manifest_path=manifest_path)
    if require_complete:
        candidate_counts = {e.get("selection_candidate_count") for e in entries}
        selection_limits = {e.get("selection_limit") for e in entries}
        if (any(e.get("selection_complete") is not True for e in entries)
                or candidate_counts != {len(entries)}
                or selection_limits != {maximum}):
            raise ValueError("selection is not a complete day within the configured limit")
    if review_path is not None:
        review_file = Path(review_path)
        if review_file.is_symlink() or not review_file.is_file() or review_file.stat().st_nlink != 1:
            raise ValueError("missing or invalid needs_review record")
        if read_jsonl_as_list(str(review_file)) != review_entries:
            raise ValueError("needs_review record mismatch")
    string_keys = {"message_id", "thread_id", "subject", "from", "cc", "reply_to", "date", "body_text"}
    for entry in entries:
        if entry.get("bucket") != BUCKET or not entry.get("key", "").startswith(PREFIX):
            raise ValueError("invalid source provenance")
        if entry["message_id"] != identity(entry["key"]):
            raise ValueError("selection identity mismatch")
        if (not isinstance(entry.get("etag"), str) or not entry["etag"]
                or type(entry.get("size")) is not int or not 0 < entry["size"] <= MAX_BYTES):
            raise ValueError("invalid S3 object evidence")
        try:
            if iso(datetime.fromisoformat(entry["last_modified"].replace("Z", "+00:00"))) != entry["last_modified"]:
                raise ValueError("noncanonical S3 LastModified")
        except (KeyError, TypeError, ValueError):
            raise ValueError("invalid S3 LastModified") from None
        if raw_dir is not None:
            raw_hash = entry.get("raw_sha256", "")
            if (not isinstance(raw_hash, str) or len(raw_hash) != 64
                    or any(c not in "0123456789abcdef" for c in raw_hash)):
                raise ValueError("invalid raw hash evidence")
            raw_path = Path(raw_dir) / (entry["message_id"] + ".eml")
            if (raw_path.is_symlink() or not raw_path.is_file()
                    or raw_path.stat().st_nlink != 1 or raw_path.stat().st_size != entry["size"]):
                raise ValueError("invalid raw cache evidence")
            digest = hashlib.sha256()
            with open(raw_path, "rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != raw_hash:
                raise ValueError("raw cache hash mismatch")
        if entry.get("status") == "needs_review":
            if (not isinstance(entry.get("issues"), list) or not entry["issues"]
                    or any(not isinstance(issue, str) or not issue for issue in entry["issues"])):
                raise ValueError("needs_review reason missing")
            expected_raw_ref = str((Path(raw_dir) / (entry["message_id"] + ".eml")).absolute())
            if entry.get("raw_ref") != expected_raw_ref:
                raise ValueError("needs_review raw reference mismatch")
            continue
        record = records_by_id[entry["message_id"]]
        if entry.get("thread_id_source") != "default_no_gmail_thread" or record.get("thread_id") != "":
            raise ValueError("invalid default thread provenance")
        if set(record) != string_keys | {"to", "attachments", "html_links"}:
            raise ValueError("master schema mismatch")
        if any(not isinstance(record[k], str) for k in string_keys):
            raise ValueError("invalid string field")
        if entry["status"] != "success" or entry.get("issues") or record["message_id"] != identity(entry["key"]):
            raise ValueError("unconfirmed parse or identity")
        if record["message_id"] != entry["message_id"] or not record["message_id"]:
            raise ValueError("identity mismatch")
        if not isinstance(record["to"], list) or any(not isinstance(a, str) for a in record["to"]):
            raise ValueError("invalid recipients")
        if not isinstance(record["attachments"], list) or len(record["attachments"]) != entry["attachment_count"]:
            raise ValueError("attachment count mismatch")
        for a in record["attachments"]:
            if set(a) != {"filename", "data", "mime_type", "size"} or type(a["size"]) is not int:
                raise ValueError("attachment schema mismatch")
            if any(not isinstance(a[k], str) for k in ("filename", "data", "mime_type")):
                raise ValueError("attachment string type")
            data = base64.b64decode(a["data"], altchars=b"-_", validate=True)
            if len(data) != a["size"] or not a["filename"]:
                raise ValueError("attachment size/name mismatch")
        if not isinstance(record["html_links"], list):
            raise ValueError("invalid links")
        for link in record["html_links"]:
            if set(link) != {"text", "href", "source"} or any(not isinstance(v, str) for v in link.values()) or link["source"] != "text/html":
                raise ValueError("invalid link schema")
        if not record["body_text"].strip() and entry.get("empty_body_reason") != "attachment_only":
            raise ValueError("unexplained empty body")
        if entry.get("date_source") not in ("mail_date", "s3_last_modified_fallback"):
            raise ValueError("missing date source")
        if iso(datetime.fromisoformat(record["date"].replace("Z", "+00:00"))) != record["date"]:
            raise ValueError("noncanonical date")


def build_summary(entries, records, manifest, review_path):
    """Return a one-line, content-free operational summary."""
    counts = {status: sum(e.get("status") == status for e in entries)
              for status in ("success", "needs_review", "failed", "pending")}
    warning_success = sum(e.get("status") == "success" and bool(e.get("header_warnings"))
                          for e in entries)
    return dict(
        selection=len(entries), success=counts["success"],
        warning_success=warning_success, needs_review=counts["needs_review"],
        failed=counts["failed"], pending=counts["pending"],
        attachments=sum(len(record.get("attachments", [])) for record in records),
        manifest=str(Path(manifest).absolute()),
        needs_review_records=str(Path(review_path).absolute()),
        result="completed_with_review" if counts["needs_review"] else "all_success")


def run(args, s3=None):
    limit = getattr(args, "selection_limit", 10)
    require_complete = getattr(args, "require_complete", False)
    canonical_output = getattr(args, "canonical_output", False)
    allow_needs_review = getattr(args, "allow_needs_review", False)
    if not 1 <= args.maximum <= limit:
        raise ValueError("max must be 1..selection-limit (default 10)")
    if require_complete and args.maximum != limit:
        raise ValueError("complete selection requires max to equal selection-limit")
    if canonical_output and not require_complete:
        raise ValueError("canonical output requires a complete selection")
    bounds(args.after, args.before)
    if args.replay and args.manifest is None:
        raise ValueError("replay requires manifest")
    requested = Path(getattr(args, "sample_dir", None) or SAMPLE).absolute()
    if requested != SAMPLE.absolute():
        daily = STEP / "02_confirm/s3_daily"
        if requested.parent.parent != daily or requested.parent.name != args.after:
            raise ValueError("sample-dir must be s3_daily/YYYY-MM-DD/run-name")
        if requested.resolve() != requested or any(p.is_symlink() for p in (requested, requested.parent, daily)):
            raise ValueError("sample-dir alias")
    root = requested.resolve()
    out = Path(args.out or root / "fetch_gmail_mail_master.jsonl").absolute()
    manifest = Path(args.manifest).absolute() if args.manifest else root / "selection_manifest.jsonl"
    # Explicit roles prevent outputs from replacing raw, backups, candidates,
    # or another manifest. Validate before chmod, SDK creation or any write.
    canonical_master = (STEP / "01_result/fetch_gmail_mail_master.jsonl").absolute()
    if canonical_output:
        if out != canonical_master or out.parent.is_symlink():
            raise ValueError("canonical output must be the Step 01-1 master")
    elif out.parent != root or out.name not in {"master.jsonl", "fetch_gmail_mail_master.jsonl"}:
        raise ValueError("out must be a permitted sample master filename")
    if manifest != root / "selection_manifest.jsonl":
        raise ValueError("manifest must be sample/selection_manifest.jsonl")
    if (root / "raw").is_symlink():
        raise ValueError("raw directory must not be a symlink")
    review_path = root / "needs_review.jsonl"
    summary_path = root / "fetch_summary.jsonl"
    targets = [root / name for name in ("master.jsonl", "fetch_gmail_mail_master.jsonl",
                                       "selection_manifest.jsonl", "parsed_candidate.jsonl",
                                       "needs_review.jsonl", "fetch_summary.jsonl")]
    targets += [p.with_suffix(p.suffix + ".tmp") for p in targets]
    targets += [out, out.with_suffix(out.suffix + ".tmp")]
    targets += list((root / "raw").glob("*"))
    for path in targets:
        if path.is_symlink() or (path.exists() and (not path.is_file() or path.stat().st_nlink != 1)):
            raise ValueError("sample file alias or invalid file type")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    if s3 is None and not args.replay:
        s3 = boto3.client("s3", region_name="ap-northeast-1",
                          config=Config(retries={"max_attempts": 2, "mode": "standard"}, connect_timeout=5, read_timeout=60))
    entries = read_jsonl_as_list(str(manifest)) if args.manifest else select(s3, args.after, args.before, args.maximum, datetime.now(timezone.utc))
    validate_selection_range(entries, args.after, args.before,
                             manifest_path=manifest if requested != SAMPLE.absolute() else None)
    if len(entries) > args.maximum or len({e["key"] for e in entries}) != len(entries):
        raise ValueError("manifest exceeds max or contains duplicates")
    for e in entries:
        if e["bucket"] != BUCKET or not e["key"].startswith(PREFIX) or e["message_id"] != identity(e["key"]):
            raise ValueError("invalid source manifest")
        e["status"] = "pending"
        e.pop("error_type", None)
        e.pop("issues", None)
        e.pop("header_warnings", None)
        e.pop("raw_ref", None)
        for field in ("date_source", "date_reason", "original_date", "thread_id_source",
                      "attachment_count", "attachment_filename_sources",
                      "empty_body_reason", "body_decoding_sources"):
            e.pop(field, None)
    write_jsonl(str(manifest), entries)
    os.chmod(manifest, 0o600)
    if require_complete:
        require_complete_selection(entries, args.maximum)
    raw_dir = root / "raw"
    raw_dir.mkdir(exist_ok=True, mode=0o700)
    records = []
    for e in entries:
        try:
            if not 0 < e["size"] <= MAX_BYTES:
                raise ValueError("object size limit")
            raw_path = raw_dir / (e["message_id"] + ".eml")
            if args.replay:
                raw = raw_path.read_bytes()
                if hashlib.sha256(raw).hexdigest() != e.get("raw_sha256"):
                    raise ValueError("raw cache hash mismatch")
            else:
                response = s3.get_object(Bucket=BUCKET, Key=e["key"], IfMatch=e["etag"])
                try:
                    if response["ETag"] != e["etag"] or iso(response["LastModified"]) != e["last_modified"]:
                        raise ValueError("source changed")
                    raw = response["Body"].read(MAX_BYTES + 1)
                finally:
                    response["Body"].close()
            if len(raw) != e["size"] or len(raw) > MAX_BYTES:
                raise ValueError("object byte count mismatch")
            if not args.replay:
                with open(raw_path, "wb") as handle:
                    os.chmod(raw_path, 0o600)
                    handle.write(raw)
            e["raw_sha256"] = hashlib.sha256(raw).hexdigest()
            e["raw_ref"] = str(raw_path.absolute())
        except Exception as error:
            e.update(status="failed", error_type=type(error).__name__)
            write_jsonl(str(manifest), entries)
            failed_count = sum(item.get("status") == "failed" for item in entries)
            pending_count = sum(item.get("status") == "pending" for item in entries)
            raise FetchStoppedError(
                "candidate_count={} limit={} success=0 needs_review=0 failed={} pending={} "
                "stop_reason=object_fetch_failed error_type={}".format(
                    next(iter({item.get("selection_candidate_count") for item in entries}),
                         "unknown"), limit, failed_count, pending_count,
                    type(error).__name__)) from None
        try:
            records.append(parse_mail(raw, e))
        except Exception as error:
            e.update(status="needs_review", error_type=type(error).__name__,
                     issues=["mail_parse_exception_part_unknown"])
        write_jsonl(str(manifest), entries)
    records_by_id = {record["message_id"]: record for record in records}
    success_records = [records_by_id[e["message_id"]] for e in entries
                       if e.get("status") == "success" and e["message_id"] in records_by_id]
    review_entries = [e for e in entries if e.get("status") == "needs_review"]
    # Review candidates and reasons remain local beside their immutable raw source.
    write_jsonl(str(root / "parsed_candidate.jsonl"), records)
    write_jsonl(str(review_path), review_entries)
    try:
        validate_records(success_records, entries, maximum=limit,
                         require_complete=require_complete,
                         raw_dir=raw_dir, expected_after=args.after, expected_before=args.before,
                         manifest_path=manifest if requested != SAMPLE.absolute() else None,
                         allow_needs_review=allow_needs_review,
                         review_path=review_path)
    except Exception as error:
        candidate_counts = {e.get("selection_candidate_count") for e in entries}
        candidate_count = (next(iter(candidate_counts))
                           if len(candidate_counts) == 1 else "unknown")
        status_counts = {status: sum(e.get("status") == status for e in entries)
                         for status in ("success", "needs_review", "failed", "pending")}
        raise FetchStoppedError(
            "candidate_count={} limit={} success={} needs_review={} failed={} pending={} "
            "stop_reason=validation_failed error_type={}".format(
                candidate_count, limit, status_counts["success"],
                status_counts["needs_review"], status_counts["failed"],
                status_counts["pending"], type(error).__name__)) from None
    summary = build_summary(entries, success_records, manifest, review_path)
    write_jsonl(str(summary_path), [summary])
    write_jsonl(str(out), success_records)
    print("selection={selection} success={success} warning_success={warning_success} "
          "needs_review={needs_review} failed={failed} pending={pending} "
          "attachments={attachments} review_records={needs_review_records} "
          "result={result}".format(**summary))
    return success_records


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--after", required=True)
    parser.add_argument("--before", required=True)
    parser.add_argument("--max", dest="maximum", type=int, default=5)
    parser.add_argument("--selection-limit", type=int, default=10,
                        help="explicitly raise safety ceiling; does not change --max")
    parser.add_argument("--sample-dir", type=Path,
                        help="isolated 02_confirm/s3_daily/YYYY-MM-DD/run-name directory")
    parser.add_argument("--out", help="defaults to sample-dir/fetch_gmail_mail_master.jsonl")
    parser.add_argument("--manifest")
    parser.add_argument("--replay", action="store_true")
    parser.add_argument("--canonical-output", action="store_true",
                        help="allow only the canonical Step 01-1 master output")
    parser.add_argument("--require-complete", action="store_true",
                        help="reject empty or max-truncated selections (Pipeline route)")
    parser.add_argument("--allow-needs-review", action="store_true",
                        help="publish only success records while retaining review entries")
    args = parser.parse_args()
    try:
        run(args)
    except Exception as error:
        if isinstance(error, (SelectionStoppedError, FetchStoppedError)):
            detail = " " + str(error)
        else:
            detail = " candidate_count=unknown limit={} stop_reason=fetch_failed".format(
                args.maximum)
        print("fetch_s3_mail NG: " + type(error).__name__ + detail, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

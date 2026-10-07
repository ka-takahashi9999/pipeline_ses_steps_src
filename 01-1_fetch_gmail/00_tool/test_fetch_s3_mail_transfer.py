#!/usr/bin/env python3
"""Focused tests for MIME transfer-payload verification."""

import copy
import importlib.util
import unittest
from pathlib import Path


TOOL = Path(__file__).with_name("fetch_s3_mail.py")
SPEC = importlib.util.spec_from_file_location("fetch_s3_mail_under_test", str(TOOL))
FETCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FETCH)


def leaf_message(body, encoding=None, charset="utf-8"):
    headers = [
        b"From: source@example.test",
        b"To: target@example.test",
        b"Subject: transfer test",
        b"Date: Tue, 06 Oct 2026 09:00:00 +0900",
        "Content-Type: text/plain; charset={}".format(charset).encode("ascii"),
    ]
    if encoding is not None:
        headers.append("Content-Transfer-Encoding: {}".format(encoding).encode("ascii"))
    raw = b"\r\n".join(headers) + b"\r\n\r\n" + body
    return FETCH.BytesParser(policy=FETCH.policy.default).parsebytes(raw), raw


def entry_for(key):
    return {
        "bucket": FETCH.BUCKET,
        "key": key,
        "message_id": FETCH.identity(key),
        "last_modified": "2026-10-06T00:00:00.000000Z",
    }


class TransferPayloadVerificationTest(unittest.TestCase):

    def test_identity_encodings_compare_retained_source_bytes(self):
        cases = [
            ("8bit", "日本語UTF-8\r\n".encode("utf-8"), "utf-8"),
            ("8bit", "日本語Shift_JIS\r\n".encode("shift_jis"), "shift_jis"),
            ("7bit", b"plain ASCII\r\n", "us-ascii"),
            ("binary", b"binary:\x00\x80\xff\r\n", "latin-1"),
            (None, "encoding omitted\u3000本文\r\n".encode("utf-8"), "utf-8"),
        ]
        for encoding, body, charset in cases:
            with self.subTest(encoding=encoding, charset=charset):
                part, raw = leaf_message(body, encoding, charset)
                decoded = part.get_payload(decode=True)
                retained = part._payload.encode("ascii", errors="surrogateescape")
                self.assertEqual(retained, raw.split(b"\r\n\r\n", 1)[1])
                self.assertEqual(decoded, retained)
                self.assertTrue(FETCH.transfer_payload_verified(part, decoded))
                self.assertFalse(
                    FETCH.transfer_payload_verified(part, decoded + b"changed")
                )

    def test_unrecoverable_identity_source_is_not_verified(self):
        part, _ = leaf_message(b"body\r\n", "8bit")
        decoded = part.get_payload(decode=True)

        non_string = copy.copy(part)
        non_string._payload = ["body"]
        self.assertFalse(FETCH.transfer_payload_verified(non_string, decoded))

        non_surrogate_unicode = copy.copy(part)
        non_surrogate_unicode._payload = "本文"
        self.assertFalse(FETCH.transfer_payload_verified(non_surrogate_unicode, decoded))
        self.assertFalse(FETCH.transfer_payload_verified(part, None))

    def test_base64_and_quoted_printable_checks_are_unchanged(self):
        valid_base64, _ = leaf_message(b"44OG44K544OI\r\n", "base64")
        base64_data = valid_base64.get_payload(decode=True)
        self.assertTrue(FETCH.transfer_payload_verified(valid_base64, base64_data))
        self.assertFalse(FETCH.transfer_payload_verified(valid_base64, base64_data + b"x"))

        invalid_base64, _ = leaf_message(b"%%%\r\n", "base64")
        self.assertFalse(FETCH.transfer_payload_verified(
            invalid_base64, invalid_base64.get_payload(decode=True)
        ))

        quoted, _ = leaf_message(b"quoted=20printable=0A\r\n", "quoted-printable")
        quoted_data = quoted.get_payload(decode=True)
        self.assertTrue(FETCH.transfer_payload_verified(quoted, quoted_data))
        self.assertFalse(FETCH.transfer_payload_verified(quoted, quoted_data + b"x"))

        unknown, _ = leaf_message(b"body\r\n", "x-unknown")
        self.assertFalse(FETCH.transfer_payload_verified(
            unknown, unknown.get_payload(decode=True)
        ))

    def test_strict_charset_failure_is_still_isolated(self):
        valid, valid_raw = leaf_message("正常本文\r\n".encode("utf-8"), "8bit")
        self.assertIsNotNone(valid)
        valid_entry = entry_for("raw/valid-8bit.eml")
        valid_record = FETCH.parse_mail(valid_raw, valid_entry)
        self.assertEqual(valid_entry["status"], "success")
        self.assertEqual(valid_entry["issues"], [])
        self.assertEqual(valid_record["message_id"], valid_entry["message_id"])

        _, broken_raw = leaf_message(b"invalid utf-8: \xff\r\n", "8bit", "utf-8")
        broken_entry = entry_for("raw/broken-8bit.eml")
        broken_record = FETCH.parse_mail(broken_raw, broken_entry)
        self.assertEqual(broken_entry["status"], "needs_review")
        self.assertIn("decode_utf8_replace_part_0", broken_entry["issues"])
        self.assertNotIn("body_payload_decode_unverified_part_0", broken_entry["issues"])
        self.assertEqual(broken_record["message_id"], broken_entry["message_id"])


if __name__ == "__main__":
    unittest.main()

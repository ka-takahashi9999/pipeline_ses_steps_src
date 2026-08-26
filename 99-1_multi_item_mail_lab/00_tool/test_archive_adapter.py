#!/usr/bin/env python3
"""Focused P6 tests for safe ZIP enumeration and variable item cardinality."""

import base64
import copy
import hashlib
import io
import json
import stat
import struct
import sys
import unittest
import zipfile
from dataclasses import replace
from pathlib import Path


sys.dont_write_bytecode = True
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
for import_path in (
    PROJECT_ROOT,
    STEP_DIR / "00_tool" / "adapters" / "archive",
    STEP_DIR / "00_tool" / "adapters" / "attachment_list",
    STEP_DIR / "00_tool" / "core",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.json_utils import read_jsonl_as_list
from archive_fixture_source import (
    build_archive_fixture,
    build_zip_bytes,
    member_definition,
    variable_n_definitions,
)
from archive_parser import (
    ArchiveParser,
    MEMBER_MANIFEST_FIELD,
    SOURCE_ITEM_EVIDENCE_FIELD,
    _parse_structure,
    metadata_limit_reasons,
    ordered_member_digest,
    validate_archive_graph,
    validate_child_container_proof,
)
from attachment_manifest_contract import (
    canonical_ordered_entries,
    ordered_attachment_digest,
    source_payload_digest,
)


CONFIG_PATH = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "archive"
    / "archive_security.v1.json.example"
)
ACTUAL_INPUT = (
    PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)


def _encoded(payload):
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _payload(fixture):
    value = fixture["attachments"][0]["data"]
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _replace_payload(fixture, payload, update_archive_identity=True):
    fixture = copy.deepcopy(fixture)
    digest = "sha256:" + hashlib.sha256(payload).hexdigest()
    fixture["attachments"][0]["data"] = _encoded(payload)
    fixture["attachments"][0]["size"] = len(payload)
    entry = fixture["attachment_acquisition_manifest"]["authoritative_attachment_entries"][0]
    entry["declared_size"] = len(payload)
    entry["content_digest"] = digest
    entries = canonical_ordered_entries(
        fixture["attachment_acquisition_manifest"]["authoritative_attachment_entries"]
    )
    manifest = fixture["attachment_acquisition_manifest"]
    manifest["expected_ordered_digest"] = ordered_attachment_digest(entries)
    manifest["source_payload_digest"] = source_payload_digest(fixture, entries)
    if update_archive_identity:
        fixture[MEMBER_MANIFEST_FIELD]["archive_sha256"] = digest
    return fixture


def _eocd_offset(payload):
    return payload.rfind(b"PK\x05\x06")


def _patch_u16(payload, offset, value):
    mutated = bytearray(payload)
    struct.pack_into("<H", mutated, offset, value)
    return bytes(mutated)


def _patch_method_or_flag(payload, central_offset_delta, local_offset_delta, value):
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    structure = _parse_structure(payload, config)
    member = structure.members[0]
    central_offset = structure.eocd["central_directory_offset"]
    mutated = bytearray(payload)
    struct.pack_into("<H", mutated, central_offset + central_offset_delta, value)
    struct.pack_into("<H", mutated, member.local_header_offset + local_offset_delta, value)
    return bytes(mutated)


def _supporting(name, payload=b"x", method=zipfile.ZIP_STORED, **kwargs):
    return member_definition(
        name,
        payload,
        role="SUPPORTING",
        compression_method=method,
        **kwargs,
    )


class ArchiveAdapterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = ArchiveParser.from_file(CONFIG_PATH)
        cls.config = cls.parser.config

    def _parse(self, definitions, item_count=0, message_id="synthetic-archive-test"):
        return self.parser.parse(build_archive_fixture(definitions, item_count, message_id))

    def assertFailClosed(self, result):
        self.assertNotEqual("PARSED", result.status)
        self.assertEqual(0, result.eligible_item_candidate_count)
        self.assertFalse(result.source["auto_union_eligible"])

    def test_versioned_security_and_credential_contract(self):
        self.assertEqual(
            {
                "max_archive_compressed_bytes": 33554432,
                "max_member_count": 256,
                "max_single_member_uncompressed_bytes": 67108864,
                "max_total_uncompressed_bytes": 134217728,
                "max_expansion_ratio": 100.0,
                "max_filename_unicode_chars": 255,
                "max_full_path_utf8_bytes": 1024,
                "max_path_depth_segments": 16,
                "future_cumulative_nested_member_count": 256,
                "future_cumulative_nested_expanded_bytes": 134217728,
            },
            self.config["limits"],
        )
        self.assertFalse(self.config["credential_matching"]["credential_matching_enabled"])
        self.assertEqual("EPHEMERAL_ONLY", self.config["password_persistence"])

    def test_base64url_and_attachment_size_are_strict(self):
        fixture = build_archive_fixture(variable_n_definitions(1), 1)
        for value in ("%%%", "abcd+efg", "A"):
            with self.subTest(value=value):
                mutated = copy.deepcopy(fixture)
                mutated["attachments"][0]["data"] = value
                result = self.parser.parse(mutated)
                self.assertFailClosed(result)
                self.assertTrue(any("base64url" in reason for reason in result.reasons))
        wrong_size = copy.deepcopy(fixture)
        wrong_size["attachments"][0]["size"] += 1
        result = self.parser.parse(wrong_size)
        self.assertEqual("INCOMPLETE", result.source["source_acquisition_status"])
        self.assertEqual("FAIL", result.source["source_attachment_validation"]["attachment_integrity_status"])
        self.assertFailClosed(result)

    def test_variable_n_same_parser_and_config(self):
        for count in (0, 1, 2, 4, 10):
            with self.subTest(count=count):
                result = self._parse(
                    variable_n_definitions(count), count, "synthetic-archive-n-" + str(count)
                )
                self.assertEqual("PARSED", result.status)
                self.assertEqual(count, result.eligible_item_candidate_count)
                self.assertEqual(count, result.archive["totals"]["item_candidates"])
                self.assertEqual("VERIFIED_COMPLETE", result.source["source_acquisition_status"])
                self.assertEqual("VERIFIED_COMPLETE", result.archive["expected_member_proof"]["status"])

    def test_stored_deflated_roles_directory_and_children(self):
        definitions = variable_n_definitions(
            2,
            [
                _supporting("supporting/readme.pdf", b"pdf", zipfile.ZIP_STORED),
                member_definition("shared/template.xlsx", b"shared", "SHARED", zipfile.ZIP_DEFLATED),
                member_definition("folder/", b"", "DIRECTORY", zipfile.ZIP_STORED, "DIRECTORY"),
            ],
        )
        result = self._parse(definitions, 2)
        self.assertEqual("PARSED", result.status)
        self.assertEqual([0, 8, 0, 8, 0], [row["compression_method"] for row in result.members])
        self.assertEqual(
            ["ITEM_CANDIDATE", "ITEM_CANDIDATE", "SUPPORTING", "SHARED", "DIRECTORY"],
            [row["role"] for row in result.members],
        )
        self.assertEqual(7, len(result.containers))
        self.assertEqual(["SPREADSHEET", "SPREADSHEET", "PDF", "SPREADSHEET", "ATTACHMENT_FILE"], [row["kind"] for row in result.containers[2:]])
        self.assertEqual("PASS", result.archive["graph_status"])

    def test_source_and_member_completeness_layers_are_independent(self):
        fixture = build_archive_fixture(variable_n_definitions(2), 2)
        no_source_manifest = copy.deepcopy(fixture)
        no_source_manifest.pop("attachment_acquisition_manifest")
        source_result = self.parser.parse(no_source_manifest)
        self.assertEqual("UNVERIFIED", source_result.source["source_acquisition_status"])
        self.assertEqual("COMPLETE", source_result.archive["enumeration_status"])
        self.assertEqual("COMPLETE", source_result.archive["integrity_status"])
        self.assertTrue(source_result.archive["archive_complete"])
        self.assertFailClosed(source_result)

        bad_member_proof = copy.deepcopy(fixture)
        bad_member_proof[MEMBER_MANIFEST_FIELD]["expected_ordered_digest"] = "sha256:" + "0" * 64
        member_result = self.parser.parse(bad_member_proof)
        self.assertEqual("VERIFIED_COMPLETE", member_result.source["source_acquisition_status"])
        self.assertEqual("INCOMPLETE", member_result.archive["enumeration_status"])
        self.assertFalse(member_result.archive["archive_complete"])
        self.assertFailClosed(member_result)

    def test_source_item_count_maps_only_item_candidates(self):
        definitions = variable_n_definitions(
            4,
            [
                _supporting("supporting/readme.pdf"),
                member_definition("shared/template.xlsx", b"shared", "SHARED"),
            ],
        )
        valid = self._parse(definitions, 4)
        self.assertEqual(6, valid.archive["totals"]["members"])
        self.assertEqual(4, valid.archive["totals"]["item_candidates"])
        self.assertEqual("MAPPED", valid.source["source_item_cardinality"]["mapping_status"])

        mismatch = build_archive_fixture(variable_n_definitions(3), 3)
        mismatch[SOURCE_ITEM_EVIDENCE_FIELD].update(
            {"count": 4, "item_keys": ["item-001", "item-002", "item-003", "item-004"]}
        )
        mismatch_result = self.parser.parse(mismatch)
        self.assertIn("source_item_candidate_count_mismatch", mismatch_result.reasons)
        self.assertFailClosed(mismatch_result)

        unknown = build_archive_fixture(variable_n_definitions(1), 1)
        unknown.pop(SOURCE_ITEM_EVIDENCE_FIELD)
        unknown_result = self.parser.parse(unknown)
        self.assertEqual("UNKNOWN", unknown_result.source["source_item_cardinality"]["status"])
        self.assertFailClosed(unknown_result)

    def test_independent_authority_catches_middle_changes_and_order(self):
        original_definitions = variable_n_definitions(4)
        original = build_archive_fixture(original_definitions, 4)
        cases = {
            "middle_deletion": original_definitions[:2] + original_definitions[3:],
            "middle_insertion": original_definitions[:2] + [member_definition("item-999.xlsx", b"inserted")] + original_definitions[2:],
            "replacement": original_definitions[:2] + [member_definition("item-003.xlsx", b"replacement")] + original_definitions[3:],
            "order_mismatch": [original_definitions[0], original_definitions[2], original_definitions[1], original_definitions[3]],
        }
        for name, definitions in cases.items():
            with self.subTest(name=name):
                new_fixture = build_archive_fixture(definitions, sum(row["role"] == "ITEM_CANDIDATE" for row in definitions))
                new_fixture[MEMBER_MANIFEST_FIELD] = copy.deepcopy(original[MEMBER_MANIFEST_FIELD])
                new_fixture[MEMBER_MANIFEST_FIELD]["archive_sha256"] = "sha256:" + hashlib.sha256(_payload(new_fixture)).hexdigest()
                result = self.parser.parse(new_fixture)
                self.assertEqual("INCOMPLETE", result.archive["enumeration_status"])
                self.assertIn("member_enumeration_ordered_sequence_mismatch", result.reasons)
                self.assertFailClosed(result)

    def test_crc_failure_keeps_enumeration_but_fails_extraction(self):
        fixture = build_archive_fixture(variable_n_definitions(1), 1)
        payload = _payload(fixture)
        structure = _parse_structure(payload, self.config)
        member = structure.members[0]
        local = struct.unpack_from("<4s5H3L2H", payload, member.local_header_offset)
        data_offset = member.local_header_offset + 30 + local[-2] + local[-1]
        mutated = bytearray(payload)
        mutated[data_offset] ^= 0x01
        result = self.parser.parse(_replace_payload(fixture, bytes(mutated)))
        self.assertEqual("COMPLETE", result.archive["enumeration_status"])
        self.assertEqual("INCOMPLETE", result.archive["integrity_status"])
        self.assertTrue(any(reason.startswith("member_read_failure:0") for reason in result.reasons))
        self.assertFailClosed(result)

    def test_member_size_mismatch_is_detected_during_read(self):
        fixture = build_archive_fixture(variable_n_definitions(1), 1)
        payload = bytearray(_payload(fixture))
        structure = _parse_structure(bytes(payload), self.config)
        member = structure.members[0]
        central = structure.eocd["central_directory_offset"]
        declared = member.uncompressed_size + 1
        struct.pack_into("<L", payload, central + 24, declared)
        struct.pack_into("<L", payload, member.local_header_offset + 22, declared)
        mutated = _replace_payload(fixture, bytes(payload))
        expected = mutated[MEMBER_MANIFEST_FIELD]["authoritative_ordered_members"]
        expected[0]["uncompressed_size"] = declared
        mutated[MEMBER_MANIFEST_FIELD]["expected_ordered_digest"] = ordered_member_digest(expected)
        result = self.parser.parse(mutated)
        self.assertEqual("COMPLETE", result.archive["enumeration_status"])
        self.assertEqual("INCOMPLETE", result.archive["integrity_status"])
        self.assertIn("member_size_mismatch:0", result.reasons)
        self.assertFailClosed(result)

    def test_archive_structure_corruption_matrix(self):
        fixture = build_archive_fixture(variable_n_definitions(2), 2)
        payload = _payload(fixture)
        structure = _parse_structure(payload, self.config)
        eocd = _eocd_offset(payload)
        central = structure.eocd["central_directory_offset"]
        cases = {}
        missing = bytearray(payload)
        missing[eocd:eocd + 4] = b"NOPE"
        cases["eocd_missing"] = bytes(missing)
        cases["truncated_end"] = payload[:-1]
        corrupt_central = bytearray(payload)
        corrupt_central[central:central + 4] = b"NOPE"
        cases["central_corruption"] = bytes(corrupt_central)
        cases["count_mismatch"] = _patch_u16(payload, eocd + 10, 3)
        local_method = bytearray(payload)
        struct.pack_into("<H", local_method, structure.members[0].local_header_offset + 8, 8)
        cases["local_header_inconsistency"] = bytes(local_method)
        for name, mutated in cases.items():
            with self.subTest(name=name):
                result = self.parser.parse(_replace_payload(fixture, mutated))
                self.assertFailClosed(result)
                self.assertFalse(result.archive["archive_complete"])

    def test_path_traversal_and_platform_path_matrix(self):
        unsafe_names = (
            "../item-001.xlsx",
            "..\\item-001.xlsx",
            "/item-001.xlsx",
            "C:/item-001.xlsx",
            "\\\\server\\share\\item-001.xlsx",
            "supporting//file.txt",
            "supporting/./file.txt",
            "supporting/CON.txt",
            "supporting/file. ",
            "supporting/control\x01.txt",
        )
        for name in unsafe_names:
            with self.subTest(name=repr(name)):
                result = self._parse([_supporting(name)])
                self.assertEqual("FAIL", result.archive["security_status"])
                self.assertFailClosed(result)

    def test_path_length_and_depth_exact_and_plus_one(self):
        exact_component = "x" * 255
        exact = self._parse([_supporting("supporting/" + exact_component)])
        self.assertEqual("PASS", exact.archive["security_status"])
        too_long = self._parse([_supporting("supporting/" + exact_component + "x")])
        self.assertEqual("FAIL", too_long.archive["security_status"])

        exact_path = "supporting/" + "/".join(["a" * 200] * 4 + ["b" * 209])
        self.assertEqual(1024, len(exact_path.encode("utf-8")))
        exact_path_result = self._parse([_supporting(exact_path)])
        self.assertEqual("PASS", exact_path_result.archive["security_status"])
        path_plus_one = exact_path[:-209] + "b" * 210
        plus_one_result = self._parse([_supporting(path_plus_one)])
        self.assertEqual("FAIL", plus_one_result.archive["security_status"])

        depth_16 = "supporting/" + "/".join("d" + str(index) for index in range(15))
        depth_17 = depth_16 + "/extra"
        self.assertEqual("PASS", self._parse([_supporting(depth_16)]).archive["security_status"])
        self.assertEqual("FAIL", self._parse([_supporting(depth_17)]).archive["security_status"])

    def test_symlink_and_special_file_fail_closed(self):
        cases = [
            member_definition("supporting/link", b"target", "SUPPORTING", zipfile.ZIP_STORED, "SYMLINK"),
            member_definition("supporting/fifo", b"", "SUPPORTING", zipfile.ZIP_STORED, "SPECIAL", stat.S_IFIFO | 0o644),
            member_definition("supporting/device", b"", "SUPPORTING", zipfile.ZIP_STORED, "SPECIAL", stat.S_IFCHR | 0o644),
            member_definition("supporting/socket", b"", "SUPPORTING", zipfile.ZIP_STORED, "SPECIAL", stat.S_IFSOCK | 0o644),
        ]
        for definition in cases:
            with self.subTest(name=definition["name"]):
                result = self._parse([definition])
                self.assertEqual("FAIL", result.archive["security_status"])
                self.assertFailClosed(result)

    def test_duplicate_normalized_member_matrix(self):
        cases = {
            "casefold": [member_definition("item-001.xlsx", b"a"), member_definition("ITEM-001.XLSX", b"b")],
            "unicode_nfkc": [_supporting("supporting/Ａ.txt"), _supporting("supporting/A.txt")],
            "unicode_composition": [_supporting("supporting/é.txt"), _supporting("supporting/e\u0301.txt")],
            "separator": [_supporting("supporting/a.txt"), _supporting("supporting\\a.txt")],
            "file_directory": [_supporting("supporting/dup"), member_definition("supporting/dup/", b"", "DIRECTORY", zipfile.ZIP_STORED, "DIRECTORY")],
            "exact": [_supporting("supporting/a.txt"), _supporting("supporting/a.txt")],
        }
        for name, definitions in cases.items():
            with self.subTest(name=name):
                item_count = sum(row["role"] == "ITEM_CANDIDATE" for row in definitions)
                result = self._parse(definitions, item_count)
                self.assertTrue(any(reason.startswith("duplicate_normalized_member:") for reason in result.reasons))
                self.assertEqual("FAIL", result.archive["security_status"])
                self.assertFailClosed(result)

    def test_nul_path_is_rejected_before_child_generation(self):
        fixture = build_archive_fixture([_supporting("supporting/x.txt")], 0)
        payload = bytearray(_payload(fixture))
        structure = _parse_structure(bytes(payload), self.config)
        member = structure.members[0]
        central = structure.eocd["central_directory_offset"]
        central_name = central + 46
        local_name = member.local_header_offset + 30
        name_bytes = b"supporting/x.txt"
        nul_position = name_bytes.index(b"x")
        payload[central_name + nul_position] = 0
        payload[local_name + nul_position] = 0
        result = self.parser.parse(_replace_payload(fixture, bytes(payload)))
        self.assertIn("member:0:path_nul", result.reasons)
        self.assertEqual("FAIL", result.archive["security_status"])
        self.assertEqual([], result.containers)
        self.assertFailClosed(result)

    def test_encrypted_aes_method99_and_mixed_are_detect_only(self):
        base_fixture = build_archive_fixture([_supporting("supporting/a.txt")], 0)
        base_payload = _payload(base_fixture)
        encrypted_payload = _patch_method_or_flag(base_payload, 8, 6, 1)
        encrypted = self.parser.parse(_replace_payload(base_fixture, encrypted_payload))
        self.assertEqual("PASSWORD_REQUIRED", encrypted.archive["credential_status"])
        self.assertEqual("UNSUPPORTED", encrypted.status)
        self.assertFailClosed(encrypted)

        aes_extra = struct.pack("<HH7s", 0x9901, 7, b"\x02\x00AE\x03\x08\x00")
        aes_result = self._parse([_supporting("supporting/aes.txt", extra=aes_extra)])
        self.assertEqual("PASSWORD_REQUIRED", aes_result.archive["credential_status"])
        self.assertFailClosed(aes_result)

        method99_payload = _patch_method_or_flag(base_payload, 10, 8, 99)
        method99 = self.parser.parse(_replace_payload(base_fixture, method99_payload))
        self.assertEqual("PASSWORD_REQUIRED", method99.archive["credential_status"])
        self.assertFailClosed(method99)

        mixed_fixture = build_archive_fixture([_supporting("supporting/a.txt"), _supporting("supporting/b.txt")], 0)
        mixed_payload = _payload(mixed_fixture)
        mixed = self.parser.parse(_replace_payload(mixed_fixture, _patch_method_or_flag(mixed_payload, 8, 6, 1)))
        self.assertIn("encrypted_member_detected:0", mixed.reasons)
        self.assertFailClosed(mixed)

    def test_nested_archive_detect_only_child(self):
        nested_payload, _ = build_zip_bytes([_supporting("supporting/inside.txt")])
        for name in ("nested.zip", "supporting/opaque.bin"):
            with self.subTest(name=name):
                result = self._parse(
                    [member_definition(name, nested_payload, "NESTED_ARCHIVE", zipfile.ZIP_DEFLATED)]
                )
                self.assertEqual("UNSUPPORTED", result.status)
                self.assertFalse(result.archive["nested_expansion_performed"])
                self.assertEqual("ARCHIVE", result.containers[2]["kind"])
                self.assertEqual("UNSUPPORTED", result.containers[2]["enumeration_status"])
                self.assertFailClosed(result)

    def test_unknown_role_is_human_review_and_not_ignored(self):
        result = self._parse([member_definition("opaque.bin", b"opaque", "UNKNOWN", zipfile.ZIP_STORED)])
        self.assertEqual("HUMAN_REVIEW", result.status)
        self.assertEqual("HUMAN_REVIEW", result.archive["member_roles_status"])
        self.assertFailClosed(result)

    def test_unsupported_compression_zip64_and_multi_disk(self):
        for method in (zipfile.ZIP_BZIP2, zipfile.ZIP_LZMA):
            with self.subTest(method=method):
                result = self._parse([_supporting("supporting/file.txt", b"payload", method)])
                self.assertEqual("UNSUPPORTED", result.status)
                self.assertFailClosed(result)
        fixture = build_archive_fixture([_supporting("supporting/file.txt")], 0)
        payload = _payload(fixture)
        eocd = _eocd_offset(payload)
        zip64 = self.parser.parse(_replace_payload(fixture, _patch_u16(payload, eocd + 10, 0xFFFF)))
        self.assertEqual("UNSUPPORTED", zip64.status)
        self.assertIn("zip64_unsupported", zip64.reasons)
        multi_disk = self.parser.parse(_replace_payload(fixture, _patch_u16(payload, eocd + 4, 1)))
        self.assertEqual("UNSUPPORTED", multi_disk.status)
        self.assertFailClosed(multi_disk)

    def test_security_metadata_limit_exact_and_plus_one(self):
        payload, _ = build_zip_bytes([_supporting("supporting/base.txt", b"x")])
        base = _parse_structure(payload, self.config).members[0]
        limits = self.config["limits"]
        cases = {
            "archive": (
                metadata_limit_reasons([], limits["max_archive_compressed_bytes"], limits),
                metadata_limit_reasons([], limits["max_archive_compressed_bytes"] + 1, limits),
                "limit_archive_compressed_bytes_exceeded",
            ),
            "member_count": (
                metadata_limit_reasons([replace(base, position=index, collision_key="k" + str(index)) for index in range(256)], 1, limits),
                metadata_limit_reasons([replace(base, position=index, collision_key="k" + str(index)) for index in range(257)], 1, limits),
                "limit_member_count_exceeded",
            ),
            "single_size": (
                metadata_limit_reasons([replace(base, uncompressed_size=67108864, compressed_size=671089)], 1, limits),
                metadata_limit_reasons([replace(base, uncompressed_size=67108865, compressed_size=671089)], 1, limits),
                "limit_single_member_uncompressed_bytes_exceeded:0",
            ),
            "total_size": (
                metadata_limit_reasons([replace(base, uncompressed_size=67108864, compressed_size=671089), replace(base, position=1, collision_key="other", uncompressed_size=67108864, compressed_size=671089)], 1, limits),
                metadata_limit_reasons([replace(base, uncompressed_size=67108864, compressed_size=671089), replace(base, position=1, collision_key="other", uncompressed_size=67108865, compressed_size=671089)], 1, limits),
                "limit_total_uncompressed_bytes_exceeded",
            ),
            "ratio": (
                metadata_limit_reasons([replace(base, uncompressed_size=10000, compressed_size=100)], 1, limits),
                metadata_limit_reasons([replace(base, uncompressed_size=10001, compressed_size=100)], 1, limits),
                "limit_member_expansion_ratio_exceeded:0",
            ),
        }
        for name, (exact, over, reason) in cases.items():
            with self.subTest(name=name):
                self.assertNotIn(reason, exact)
                self.assertIn(reason, over)

    def test_member_count_256_passes_and_257_fails(self):
        definitions_256 = [_supporting("supporting/file-" + str(index) + ".txt") for index in range(256)]
        exact = self._parse(definitions_256)
        self.assertEqual("PARSED", exact.status)
        over = self._parse(definitions_256 + [_supporting("supporting/file-256.txt")])
        self.assertEqual("FAIL", over.archive["security_status"])
        self.assertIn("limit_member_count_exceeded", over.reasons)
        self.assertFailClosed(over)

    def test_archive_byte_limit_exact_and_plus_one(self):
        limit = self.config["limits"]["max_archive_compressed_bytes"]
        empty_payload, _ = build_zip_bytes([member_definition("item-001.xlsx", b"", compression_method=zipfile.ZIP_STORED)])
        overhead = len(empty_payload)
        for target, expected_status in ((limit, "PARSED"), (limit + 1, "UNSUPPORTED")):
            with self.subTest(target=target):
                definition = member_definition(
                    "item-001.xlsx", b"x" * (target - overhead),
                    compression_method=zipfile.ZIP_STORED,
                )
                fixture = build_archive_fixture([definition], 1, "archive-size-" + str(target))
                self.assertEqual(target, len(_payload(fixture)))
                result = self.parser.parse(fixture)
                self.assertEqual(expected_status, result.status)
                if target > limit:
                    self.assertIn("limit_archive_compressed_bytes_exceeded", result.reasons)
                    self.assertFailClosed(result)

    def test_graph_validator_catches_missing_child_and_parent_mismatch(self):
        result = self._parse(variable_n_definitions(2), 2)
        containers = copy.deepcopy(result.containers)
        deleted = containers[:3] + containers[4:]
        self.assertTrue(any(reason.startswith("graph_child_missing:") for reason in validate_archive_graph(deleted, result.source["container_references"])))
        self.assertIn(
            "child_container_ordered_sequence_mismatch",
            validate_child_container_proof(
                _parse_structure(_payload(build_archive_fixture(variable_n_definitions(2), 2)), self.config).members,
                deleted,
            ),
        )
        mismatched = copy.deepcopy(containers)
        mismatched[2]["parent_container_id"] = "wrong-parent"
        graph_reasons = validate_archive_graph(mismatched, result.source["container_references"])
        self.assertTrue(any(reason.startswith("graph_orphan:") for reason in graph_reasons))

    def test_actual_enfast_is_technical_observation_only(self):
        records = read_jsonl_as_list(str(ACTUAL_INPUT))
        selected = [
            record
            for record in records
            if "@enfast-tech.com" in str(record.get("from", "")).casefold()
            and any(str(attachment.get("filename", "")).casefold().endswith(".zip") for attachment in record.get("attachments", []) if isinstance(attachment, dict))
        ]
        self.assertTrue(selected, "saved Enfast technical observation is missing")
        actual = sorted(selected, key=lambda row: str(row.get("date", "")), reverse=True)[0]
        result = self.parser.parse(actual)
        self.assertEqual("UNVERIFIED", result.source["source_acquisition_status"])
        self.assertEqual("COMPLETE", result.archive["enumeration_status"])
        self.assertEqual("COMPLETE", result.archive["integrity_status"])
        self.assertEqual("NOT_ENCRYPTED", result.archive["credential_status"])
        self.assertEqual(1, result.archive["totals"]["members"])
        self.assertEqual(1, len(result.containers) - 2)
        self.assertEqual("SPREADSHEET", result.containers[2]["kind"])
        self.assertEqual(0, result.eligible_item_candidate_count)
        self.assertFalse(result.source["auto_union_eligible"])


if __name__ == "__main__":
    unittest.main()

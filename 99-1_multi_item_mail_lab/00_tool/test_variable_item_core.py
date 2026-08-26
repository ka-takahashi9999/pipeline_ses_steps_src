#!/usr/bin/env python3
"""Parameterized contract tests for the 99-1 variable-item core."""

import base64
import copy
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


sys.dont_write_bytecode = True
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
for import_path in (
    PROJECT_ROOT,
    STEP_DIR / "00_tool" / "adapters" / "inline_summary",
    STEP_DIR / "00_tool" / "canonicalize",
    STEP_DIR / "00_tool" / "core",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from canonical_overlay import MAIL_MASTER_KEYS, build_canonical_overlay
from identity import artifact_set_fingerprint, body_fingerprint, version_fingerprint
from inline_summary_adapter import InlineSummaryAdapter
from variable_item_core import (
    CardinalityEvidence,
    Container,
    ContainerKind,
    EnumerationStatus,
    ItemCandidate,
    Source,
    evaluate_completeness,
)


CONFIG_DIR = STEP_DIR / "10_assistance_tool" / "configs" / "companies"


def _load_config(filename: str):
    with (CONFIG_DIR / filename).open(encoding="utf-8") as file_object:
        return json.load(file_object)


def _attachment(filename: str, payload: bytes):
    return {
        "filename": filename,
        "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "size": len(payload),
        "data": base64.urlsafe_b64encode(payload).decode("ascii"),
    }


def _netwisdom_mail(count: int):
    blocks = []
    attachments = []
    for index in range(count):
        identifier = f"RESOURCE-{index + 1}"
        blocks.append(
            "-" * 20
            + f"\n技術者: {identifier}\n"
            + "-" * 20
            + f"\n【概要】要員 {index + 1}\n面接予定：なし\n結果待ち：なし\n"
        )
        attachments.append(
            _attachment(
                f"skillsheet-{identifier}.xlsx", f"payload-{index + 1}".encode()
            )
        )
    return {
        "message_id": f"synthetic-netwisdom-{count}",
        "from": "Synthetic <test@netwisdom.co.jp>",
        "subject": f"NetWisdom variable {count}",
        "body_text": "ご担当者様\n" + "".join(blocks) + f"以上、{count}名です。",
        "attachments": attachments,
        "html_links": [],
    }


def _ichi_r_mail(structural_count: int, declared_count: int = None):
    declared_count = structural_count if declared_count is None else declared_count
    blocks = []
    attachments = []
    for index in range(structural_count):
        identifier = f"A.{chr(ord('A') + index)}"
        blocks.append(
            f"■氏名：{identifier}\n"
            "■年齢：匿名\n■性別：匿名\n■最寄駅：匿名駅\n"
            "■所属：弊社フリーランス\n■スキル：匿名\n"
            "＝＝＝＝＝＝＝＝＝＝＝\n"
        )
        attachments.append(
            _attachment(
                f"スキルシート({identifier})_20260825.xlsx",
                f"ichi-{index + 1}".encode(),
            )
        )
    footer = "【営業中エンジニア一覧スプレッドシート】"
    body = "ご担当者様\n＝＝＝＝＝＝＝＝＝＝＝\n" + "".join(blocks) + footer
    return {
        "message_id": f"synthetic-ichi-r-{structural_count}-{declared_count}",
        "from": "Synthetic <test@1-r.co.jp>",
        "subject": (
            "サーバエンジニアのご紹介です！"
            f"（{declared_count}名で1人月希望）/【弊社フリーランス】"
        ),
        "body_text": body,
        "attachments": attachments,
        "html_links": [],
    }


def _relation(locator: str, content: str, role: str = "PRIMARY"):
    return {
        "role": role,
        "artifact_kind": "ATTACHMENT_FILE",
        "stable_locator": locator,
        "content_sha256": "sha256:" + content * 64,
        "version_relevant": True,
    }


class VariableCardinalityAdapterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.p1_config = _load_config("netwisdom.config.json.example")
        cls.p2_config = _load_config("ichi_r.config.json.example")
        cls.p1 = InlineSummaryAdapter(copy.deepcopy(cls.p1_config))
        cls.p2 = InlineSummaryAdapter(copy.deepcopy(cls.p2_config))

    def test_structural_complete_accepts_variable_n(self) -> None:
        for count in (0, 1, 2, 4, 10):
            with self.subTest(count=count):
                result = self.p1.parse(_netwisdom_mail(count))
                self.assertEqual("PARSED", result.status)
                self.assertEqual(count, len(result.items))
                completeness = result.source["completeness_result"]
                self.assertEqual(count, completeness["expected_count"])
                self.assertTrue(completeness["checks"]["identity_collision_free"])

    def test_declared_and_structural_counts_accept_variable_n(self) -> None:
        for count in (0, 1, 2, 4, 10):
            with self.subTest(count=count):
                result = self.p2.parse(_ichi_r_mail(count))
                self.assertEqual("PARSED", result.status)
                self.assertEqual(count, len(result.items))
                evidence = result.source["cardinality_evidence"]
                self.assertEqual(
                    ["DECLARED_COUNT", "STRUCTURAL_COMPLETE"],
                    [row["authority"] for row in evidence],
                )
                self.assertEqual([count, count], [row["count"] for row in evidence])
                for item in result.items:
                    self.assertIn(f"{count}名", item["body_text"])

    def test_declared_structural_mismatch_is_partial_and_atomic(self) -> None:
        result = self.p2.parse(_ichi_r_mail(2, declared_count=4))
        self.assertEqual("PARTIAL", result.status)
        self.assertEqual([], result.items)
        self.assertIn("cardinality_evidence_conflict:2,4", result.reasons)

    def test_unknown_authority_emits_nothing(self) -> None:
        config = copy.deepcopy(self.p1_config)
        config["cardinality"]["primary"]["authority"] = "UNKNOWN"
        result = InlineSummaryAdapter(config).parse(_netwisdom_mail(2))
        self.assertEqual("UNSUPPORTED", result.status)
        self.assertEqual([], result.items)

    def test_footer_missing_is_partial_and_atomic(self) -> None:
        mail = _netwisdom_mail(2)
        mail["body_text"] = mail["body_text"].replace("以上、2名です。", "")
        result = self.p1.parse(mail)
        self.assertEqual("PARTIAL", result.status)
        self.assertEqual([], result.items)

    def test_middle_required_marker_missing_is_partial_and_atomic(self) -> None:
        mail = _netwisdom_mail(4)
        mail["body_text"] = mail["body_text"].replace("面接予定：なし\n", "", 1)
        result = self.p1.parse(mail)
        self.assertEqual("PARTIAL", result.status)
        self.assertEqual([], result.items)

    def test_duplicate_identifier_fails_closed(self) -> None:
        mail = _netwisdom_mail(2)
        mail["body_text"] = mail["body_text"].replace(
            "RESOURCE-2", "RESOURCE-1"
        )
        mail["attachments"][1]["filename"] = "skillsheet-RESOURCE-1.xlsx"
        result = self.p1.parse(mail)
        self.assertEqual("HUMAN_REVIEW", result.status)
        self.assertEqual([], result.items)
        self.assertFalse(
            result.source["completeness_result"]["checks"]["identity_collision_free"]
        )

    def test_source_and_container_contracts_are_auditable(self) -> None:
        result = self.p1.parse(_netwisdom_mail(1))
        self.assertEqual("PARSED", result.status)
        self.assertEqual("EMAIL", result.source["source_type"])
        self.assertEqual("UNKNOWN", result.source["delivery_semantics"])
        self.assertTrue(result.source["source_fingerprint"].startswith("sha256:"))
        self.assertEqual(
            ["INLINE_BODY", "ATTACHMENT_FILE"],
            [container["kind"] for container in result.containers],
        )
        self.assertTrue(all(container["completeness"] for container in result.containers))

    def test_attachment_order_change_preserves_item_versions(self) -> None:
        mail = _netwisdom_mail(4)
        first = self.p1.parse(copy.deepcopy(mail))
        reordered = copy.deepcopy(mail)
        reordered["attachments"] = list(reversed(reordered["attachments"]))
        second = self.p1.parse(reordered)
        self.assertEqual("PARSED", first.status)
        self.assertEqual("PARSED", second.status)
        self.assertEqual(
            [item["derived_item_id"] for item in first.items],
            [item["derived_item_id"] for item in second.items],
        )
        self.assertEqual(
            [item["artifact_set_fingerprint"] for item in first.items],
            [item["artifact_set_fingerprint"] for item in second.items],
        )

    def test_system_failure_is_represented_and_atomic(self) -> None:
        with patch.object(self.p1, "_parse", side_effect=RuntimeError("synthetic crash")):
            result = self.p1.parse(_netwisdom_mail(1))
        self.assertEqual("SYSTEM_FAILURE", result.status)
        self.assertEqual([], result.items)
        self.assertEqual(
            "SYSTEM_FAILURE", result.source["completeness_result"]["status"]
        )


class ArtifactRelationCoreTest(unittest.TestCase):
    @staticmethod
    def _gate(relations_by_item):
        count = len(relations_by_item)
        evidence = CardinalityEvidence(
            authority="STRUCTURAL_COMPLETE",
            source="INLINE_BODY",
            count=count,
            complete=True,
            is_primary=True,
        )
        source = Source(
            source_id="artifact-source",
            source_type="EMAIL",
            source_company="Synthetic",
            source_fingerprint="sha256:" + "0" * 64,
            delivery_semantics="UNKNOWN",
            acquisition_status="COMPLETE",
            cardinality_evidence=[evidence],
            container_references=["body"],
        )
        container = Container(
            container_id="body",
            parent_container_id="",
            kind=ContainerKind.INLINE_BODY.value,
            locator="body_text",
            content_fingerprint="sha256:" + "1" * 64,
            enumeration_status=EnumerationStatus.COMPLETE.value,
            completeness=True,
            candidate_count=count,
            required=True,
        )
        candidates = [
            ItemCandidate(
                candidate_index=index,
                identifier=f"ID-{index}",
                source_container_id="body",
                body_text=f"item {index}",
                parse_success=True,
                item_artifacts=relations,
                logical_item_id=f"li_{index}",
            )
            for index, relations in enumerate(relations_by_item, 1)
        ]
        return evaluate_completeness(source, [container], candidates)

    def test_n_items_n_artifacts(self) -> None:
        result = self._gate([[_relation(f"a-{index}", str(index))] for index in range(4)])
        self.assertEqual("PARSED", result.status)

    def test_n_items_zero_artifacts(self) -> None:
        result = self._gate([[], [], [], []])
        self.assertEqual("PARSED", result.status)
        self.assertEqual(
            artifact_set_fingerprint([]), artifact_set_fingerprint(tuple())
        )

    def test_shared_artifact_relation(self) -> None:
        shared = _relation("shared", "a", role="SHARED")
        result = self._gate([[shared], [shared], [shared]])
        self.assertEqual("PARSED", result.status)

    def test_item_with_multiple_artifacts_and_overlay(self) -> None:
        relations = [_relation("primary", "a"), _relation("support", "b", "SUPPORTING")]
        result = self._gate([relations])
        self.assertEqual("PARSED", result.status)
        source_mail = {key: "" for key in MAIL_MASTER_KEYS}
        source_mail.update({"to": [], "attachments": [], "html_links": []})
        item = {
            "derived_item_id": "mi_test",
            "canonical_subject": "stable",
            "body_text": "one item",
            "attachments": [{"filename": "a"}, {"filename": "b"}],
            "html_links": [],
        }
        overlay = build_canonical_overlay(source_mail, item)
        self.assertEqual(2, len(overlay["attachments"]))
        self.assertEqual(MAIL_MASTER_KEYS, set(overlay))

    def test_artifact_order_does_not_change_fingerprint(self) -> None:
        first = [_relation("b", "b"), _relation("a", "a")]
        self.assertEqual(
            artifact_set_fingerprint(first),
            artifact_set_fingerprint(list(reversed(first))),
        )

    def test_artifact_content_change_creates_new_version(self) -> None:
        first_set = artifact_set_fingerprint([_relation("a", "a")])
        second_set = artifact_set_fingerprint([_relation("a", "b")])
        body_digest = body_fingerprint("same body")
        self.assertNotEqual(first_set, second_set)
        self.assertNotEqual(
            version_fingerprint(body_digest, first_set),
            version_fingerprint(body_digest, second_set),
        )


if __name__ == "__main__":
    unittest.main()

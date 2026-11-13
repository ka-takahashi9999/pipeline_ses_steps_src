#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GEN-06 phase SSOTのfocused test。"""

import hashlib
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

from common.phase_extractor import (
    PHASE_DICTIONARY_PATH,
    build_extracted_record,
    build_passthrough_record,
    extract_phases,
    is_valid_phase_record,
    load_phase_dictionary,
    validate_phase_dictionary,
)


ROOT = Path(__file__).resolve().parent.parent
EXPECTED_SHA256 = "f4932d47d81256140965b677182771f8e8ba957cdfd6282f7444d26aedf22619"


def _load_step_module(name: str, relative_path: str):
    sys.modules.pop("config", None)
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _dictionary_signature(phase_dict):
    return [
        (
            phase,
            [(keyword, pattern.pattern, pattern.flags) for keyword, pattern in entries],
        )
        for phase, entries in phase_dict.items()
    ]


class PhaseExtractorTest(unittest.TestCase):
    def test_ssot_dictionary_shape_entries_hash_and_regressions(self):
        raw = PHASE_DICTIONARY_PATH.read_bytes()
        data = validate_phase_dictionary(str(PHASE_DICTIONARY_PATH))

        self.assertEqual(hashlib.sha256(raw).hexdigest(), EXPECTED_SHA256)
        self.assertEqual(len(data), 12)
        self.assertEqual(sum(len(keywords) for keywords in data.values()), 175)
        self.assertIn("要件策定", data["要件定義"])
        self.assertIn("試験工程", data["テスト"])
        self.assertIn("移管計画の策定", data["移行"])
        self.assertIn("業務要件の確認", data["要件調査"])
        self.assertIn("運用試験", data["テスト"])
        self.assertIn("運用フローの設計", data["運用保守"])
        self.assertIn("WBS作成", data["PMO"])
        self.assertIn("性能試験", data["テスト"])
        self.assertIn("運用引継ぎ", data["運用保守"])
        self.assertIn("運用手順書", data["運用保守"])
        self.assertIn("現行解析", data["要件調査"])
        self.assertIn("運用方法の検討", data["運用保守"])
        all_keywords = {keyword for keywords in data.values() for keyword in keywords}
        self.assertNotIn("リファクタリング", all_keywords)
        self.assertNotIn("障害時顧客対応", all_keywords)
        for held_keyword in (
            "障害調査",
            "ジョブ設計",
            "受入",
            "設計",
            "調査",
            "運用",
            "評価",
            "業務要求整理",
            "受け入れ",
            "本番導入",
            "顧客折衝",
            "仕様調整",
            "構成管理",
            "PM/PL",
            "品質管理",
            "QA",
            "システム導入",
        ):
            self.assertNotIn(held_keyword, all_keywords)

    def test_load_is_deterministic_and_keyword_order_is_stable(self):
        first = load_phase_dictionary(str(PHASE_DICTIONARY_PATH))
        second = load_phase_dictionary(str(PHASE_DICTIONARY_PATH))
        self.assertEqual(_dictionary_signature(first), _dictionary_signature(second))
        for entries in first.values():
            lengths = [len(keyword) for keyword, _pattern in entries]
            self.assertEqual(lengths, sorted(lengths, reverse=True))

    def test_dictionary_validation_rejects_invalid_definitions(self):
        cases = [
            ("要件調査:\n  - 調査\n要件調査:\n  - 分析\n", "categoryが重複"),
            ("要件調査:\n  - 調査\n  - 調査\n", "category内で重複"),
            ("要件調査:\n  - 調査\n設計:\n  - 調査\n", "複数category"),
            ("要件調査:\n  - ''\n", "空または不正"),
            ("要件調査: [\n", None),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "phase_dictionary.txt"
            for content, expected_message in cases:
                with self.subTest(content=content):
                    path.write_text(content, encoding="utf-8")
                    with self.assertRaises(Exception) as context:
                        validate_phase_dictionary(str(path))
                    if expected_message:
                        self.assertIn(expected_message, str(context.exception))

    def test_category_assignment_order_dedupe_and_null_policy(self):
        phase_dict = load_phase_dictionary(str(PHASE_DICTIONARY_PATH))
        phases, raw = extract_phases(
            "運用フローの設計、業務要件の確認、運用試験、運用試験", phase_dict
        )
        self.assertEqual(phases, ["運用保守", "要件調査", "テスト"])
        self.assertIsNotNone(raw)
        self.assertEqual(len(phases), len(set(phases)))
        self.assertEqual(extract_phases("工程の記載なし", phase_dict), ([], None))
        self.assertEqual(extract_phases(None, phase_dict), ([], None))

    def test_all_175_keywords_assign_to_their_category(self):
        validated = validate_phase_dictionary(str(PHASE_DICTIONARY_PATH))
        phase_dict = load_phase_dictionary(str(PHASE_DICTIONARY_PATH))
        for expected_phase, keywords in validated.items():
            for keyword in keywords:
                with self.subTest(phase=expected_phase, keyword=keyword):
                    phases, _raw = extract_phases(f"対象工程: {keyword}", phase_dict)
                    self.assertIn(expected_phase, phases)

    def test_ascii_boundary_semantics_are_preserved(self):
        phase_dict = load_phase_dictionary(str(PHASE_DICTIONARY_PATH))
        phases, _raw = extract_phases("PMO業務", phase_dict)
        self.assertIn("PMO", phases)
        phases, _raw = extract_phases("XPMOY", phase_dict)
        self.assertNotIn("PMO", phases)

    def test_record_construction_and_validity(self):
        phase_dict = load_phase_dictionary(str(PHASE_DICTIONARY_PATH))
        self.assertEqual(
            build_passthrough_record("m1"),
            {"message_id": "m1", "phases": [], "phases_raw": None},
        )
        record = build_extracted_record("m2", "運用試験", phase_dict)
        self.assertEqual(record["phases"], ["テスト"])
        self.assertTrue(is_valid_phase_record(record))
        self.assertFalse(
            is_valid_phase_record({"message_id": "m3", "phases": None})
        )

    def test_project_resource_steps_share_ssot_and_extractor_with_parity(self):
        project = _load_step_module(
            "phase_project_step",
            "03-9_extract_project_phase_category/00_tool/extract_project_phase_category.py",
        )
        resource = _load_step_module(
            "phase_resource_step",
            "05-9_extract_resource_phase_category/00_tool/extract_resource_phase_category.py",
        )
        expected_path = str(PHASE_DICTIONARY_PATH)
        self.assertEqual(project.PHASE_DICT_PATH, expected_path)
        self.assertEqual(resource.PHASE_DICT_PATH, expected_path)
        self.assertIs(project.extract_phases, extract_phases)
        self.assertIs(resource.extract_phases, extract_phases)
        self.assertIs(project.load_phase_dictionary, load_phase_dictionary)
        self.assertIs(resource.load_phase_dictionary, load_phase_dictionary)

        phase_dict = load_phase_dictionary(expected_path)
        body = "業務要件の確認から運用試験、運用フローの設計まで"
        expected = build_extracted_record("same", body, phase_dict)
        self.assertEqual(
            project.build_extracted_record("same", body, phase_dict), expected
        )
        self.assertEqual(
            resource.build_extracted_record("same", body, phase_dict), expected
        )
        self.assertEqual(
            project.build_extracted_record("same", body, phase_dict), expected
        )


if __name__ == "__main__":
    unittest.main()

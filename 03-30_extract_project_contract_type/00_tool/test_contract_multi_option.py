#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""03-30 explicit multi-option contract grammarのfocused test。"""

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent / "extract_project_contract_type.py"
SPEC = importlib.util.spec_from_file_location("contract_multi_option_under_test", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
target = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(target)


class ContractMultiOptionTest(unittest.TestCase):
    def assert_contract_type(self, text: str, expected: str) -> None:
        actual, _source, _raw = target.detect_contract_type(text)
        self.assertEqual(expected, actual, text)

    def test_explicit_multi_option_is_quasi_mandate(self) -> None:
        cases = (
            "契約形態:準委任契約,派遣契約",
            "契約形態:準委任契約、派遣契約",
            "契約形態:準委任契約・派遣契約",
            "契約形態:派遣契約・準委任契約",
            "契約形態:派遣契約,準委任契約",
            "契約形態:準委任契約(派遣契約でも可)",
            "契約形態:準委任契約（派遣契約でも可）",
        )
        for text in cases:
            with self.subTest(text=text):
                self.assert_contract_type(text, "quasi_mandate")

    def test_existing_multi_option_grammar_is_preserved(self) -> None:
        for text in (
            "契約形態:派遣/準委任",
            "契約形態:準委任/派遣",
            "契約形態:派遣または準委任",
        ):
            with self.subTest(text=text):
                self.assert_contract_type(text, "quasi_mandate")

    def test_dispatch_and_ambiguous_expressions_keep_existing_result(self) -> None:
        cases = (
            ("契約形態:派遣契約", "dispatch"),
            ("契約形態:派遣のみ", "dispatch"),
            ("原則派遣", "outsourcing"),
            ("派遣優先", "outsourcing"),
            ("準委任不可", "outsourcing"),
            ("準委任は応相談", "outsourcing"),
            ("準委任検討可", "outsourcing"),
            ("準委任の可能性あり", "outsourcing"),
            ("契約形態:派遣契約・準委任不可", "dispatch"),
            ("契約形態:派遣契約・準委任契約 不可", "dispatch"),
            ("契約形態:派遣契約・準委任契約 は応相談", "dispatch"),
            ("契約形態:派遣契約・準委任契約 検討可", "dispatch"),
            ("契約形態:派遣契約・準委任契約 の可能性あり", "dispatch"),
            ("契約形態:原則派遣契約・準委任契約", "dispatch"),
            ("契約形態:派遣優先/準委任", "dispatch"),
        )
        for text, expected in cases:
            with self.subTest(text=text):
                self.assert_contract_type(text, expected)

    def test_separate_sections_do_not_form_multi_option(self) -> None:
        self.assert_contract_type(
            "過去契約:準委任契約\n今回契約:派遣契約",
            "dispatch",
        )


if __name__ == "__main__":
    unittest.main()

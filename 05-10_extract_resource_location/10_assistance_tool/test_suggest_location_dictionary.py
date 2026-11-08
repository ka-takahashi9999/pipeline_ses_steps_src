"""05-10 resource location辞書候補suggestのfocused test。"""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml


SCRIPT = Path(__file__).with_name("suggest_location_dictionary.py")


def _load_module():
    spec = importlib.util.spec_from_file_location("suggest_location_dictionary_05_10_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _items(count, snippet="x"):
    return [
        {"message_id": "m{:03d}".format(index), "snippet": snippet}
        for index in range(count)
    ]


def _write_jsonl(path, rows):
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


class SuggestLocationDictionaryTest(unittest.TestCase):
    def setUp(self):
        self.module = _load_module()

    def test_record_count_boundaries(self):
        expected = {
            49: [49],
            50: [50],
            51: [50, 1],
            123: [50, 50, 23],
        }
        for count, batch_sizes in expected.items():
            with self.subTest(count=count):
                batches = self.module.build_batches(_items(count))
                self.assertEqual(batch_sizes, [len(batch) for batch in batches])

    def test_character_limit_is_kept_and_first_reached_limit_splits(self):
        exactly_limit = _items(2, "x" * 3000)
        self.assertEqual([2], [len(batch) for batch in self.module.build_batches(exactly_limit)])

        over_limit = exactly_limit + _items(1, "y")
        self.assertEqual([2, 1], [len(batch) for batch in self.module.build_batches(over_limit)])

        character_limit_first = _items(2, "x" * 3001)
        self.assertEqual(
            [1, 1],
            [len(batch) for batch in self.module.build_batches(character_limit_first)],
        )

    def test_llm_contract_keeps_ids_snippets_schema_and_uses_4096_tokens(self):
        captured = {}

        def fake_call_llm(**kwargs):
            captured.update(kwargs)
            return {"candidates": {"関東地方": ["新宿"]}}

        batch = [
            {"message_id": "m001", "snippet": "最寄駅: 新宿"},
            {"message_id": "m002", "snippet": "居住地: 横浜"},
        ]
        with mock.patch.object(self.module, "call_llm", side_effect=fake_call_llm):
            result = self.module.extract_candidates_via_llm(batch, {"東京"})

        self.assertEqual({"candidates": {"関東地方": ["新宿"]}}, result)
        self.assertEqual(4096, captured["max_tokens"])
        self.assertEqual({"candidates": {}}, captured["response_schema"])
        self.assertIn("[m001]\n最寄駅: 新宿", captured["user_prompt"])
        self.assertIn("[m002]\n居住地: 横浜", captured["user_prompt"])

    def test_main_merges_and_deduplicates_candidates_across_batches(self):
        with tempfile.TemporaryDirectory(prefix="suggest_05_10_") as temp_name:
            temp = Path(temp_name)
            resources = temp / "resources.jsonl"
            cleaned = temp / "cleaned.jsonl"
            dictionary = temp / "location_dictionary.txt"
            output = temp / "location_dictionary_candidates.txt"

            resource_rows = [{"message_id": "m{:03d}".format(i)} for i in range(51)]
            cleaned_rows = [
                {
                    "message_id": "m{:03d}".format(i),
                    "body_text": "氏名: A{}\n最寄駅: 駅{}\n所属: テスト".format(i, i),
                }
                for i in range(51)
            ]
            _write_jsonl(resources, resource_rows)
            _write_jsonl(cleaned, cleaned_rows)
            dictionary.write_text("関東地方:\n  - 既存駅\n", encoding="utf-8")

            calls = []

            def fake_call_llm(**kwargs):
                calls.append(kwargs)
                if len(calls) == 1:
                    return {"candidates": {"関東地方": ["新宿", "重複", "既存駅"]}}
                return {
                    "candidates": {
                        "関東地方": ["重複", "渋谷"],
                        "近畿地方": ["梅田"],
                    }
                }

            with mock.patch.object(self.module, "INPUT_RESOURCES", str(resources)), mock.patch.object(
                self.module, "INPUT_CLEANED", str(cleaned)
            ), mock.patch.object(self.module, "DICT_PATH", str(dictionary)), mock.patch.object(
                self.module, "OUTPUT_CANDIDATES", str(output)
            ), mock.patch.object(self.module, "call_llm", side_effect=fake_call_llm):
                self.module.main()

            self.assertEqual(2, len(calls))
            self.assertIn("【対象スニペット（50件）】", calls[0]["user_prompt"])
            self.assertIn("【対象スニペット（1件）】", calls[1]["user_prompt"])
            self.assertIn("[m000]", calls[0]["user_prompt"])
            self.assertIn("最寄駅: 駅0", calls[0]["user_prompt"])
            self.assertIn("[m050]", calls[1]["user_prompt"])
            self.assertTrue(all(call["max_tokens"] == 4096 for call in calls))
            self.assertTrue(all(call["response_schema"] == {"candidates": {}} for call in calls))

            candidates = yaml.safe_load(output.read_text(encoding="utf-8"))
            self.assertEqual(["新宿", "重複", "渋谷"], candidates["関東地方"])
            self.assertEqual(["梅田"], candidates["近畿地方"])
            self.assertNotIn("既存駅", candidates["関東地方"])


if __name__ == "__main__":
    unittest.main()

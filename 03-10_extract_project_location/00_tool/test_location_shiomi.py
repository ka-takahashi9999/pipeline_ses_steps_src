#!/usr/bin/env python3
"""最新757案件をメモリ上で辞書差分評価し、隔離出力でconfirmする。"""
import hashlib
import importlib.util
import sys
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

STEP = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(STEP / "00_tool"))
import extract_project_location as target
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl


class ShiomiFocusedTest(unittest.TestCase):
    def test_latest_757_and_confirm(self):
        entries = target.load_location_dictionary(target.DICT_PATH)
        self.assertEqual([(region, word) for region, word, _ in entries if word == "潮見"], [("関東地方", "潮見")])
        self.assertIn("同名地名あり。通勤圏の暫定値として関東扱い。2026-09-17案件では他地方の明示例なし。最終勤務地は営業確認。",
                      Path(target.DICT_PATH).read_text())
        before_entries = [e for e in entries if e[1] != "潮見"]
        projects = read_jsonl_as_list(target.INPUT_PROJECTS)
        self.assertEqual(len(projects), 757)
        cleaned = read_jsonl_as_dict(target.INPUT_CLEANED)
        before, after, changes = [], [], []
        shiomi_ids = {r["message_id"] for r in projects if "潮見" in cleaned[r["message_id"]]["body_text"]}
        for row in projects:
            mid = row["message_id"]
            body = cleaned[mid]["body_text"]
            old = target.build_extracted_record(mid, body, before_entries)
            new = target.build_extracted_record(mid, body, entries)
            before.append(old)
            after.append(new)
            if old != new:
                self.assertIn(mid, shiomi_ids)
                self.assertEqual(new["location"], "関東地方")
                self.assertEqual(new["location_source"], "label")
                self.assertTrue(new["location_raw"].startswith("潮見"))
                if old["location"] == "remote":
                    self.assertIn("リモート頻度：フル出社", body)
                    self.assertIn("場所：潮見駅", body)
                changes.append({"message_id": mid, "before": old, "after": new})
        self.assertEqual(len(changes), 9)
        self.assertEqual({r["message_id"] for r in changes}, shiomi_ids)
        self.assertEqual(Counter(r["before"]["location"] for r in changes), {None: 5, "remote": 1, "近畿地方": 1, "関東地方": 2})
        canonical = [STEP / "01_result" / f for f in (target.OUTPUT_EXTRACTED, target.OUTPUT_NULL)]
        hashes = [hashlib.sha256(p.read_bytes()).hexdigest() for p in canonical]
        focused = STEP / "02_confirm" / "focused_shiomi"
        write_jsonl(str(focused / "changes.jsonl"), changes)
        write_jsonl(str(focused / "01_result" / target.OUTPUT_EXTRACTED), [r for r in after if r["location"] is not None])
        write_jsonl(str(focused / "01_result" / target.OUTPUT_NULL), [r for r in after if r["location"] is None])
        spec = importlib.util.spec_from_file_location("confirm_shiomi", STEP / "02_confirm" / "confirm_extract_project_location.py")
        confirm = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(confirm)
        before_suspicious = {
            r["message_id"]: r for r in before if r["location_source"] != "remote_fallback"
            and confirm._SIGNATURE_WORD_RE.search(r["location_raw"] or "")
        }
        after_suspicious = {
            r["message_id"]: r for r in after if r["location_source"] != "remote_fallback"
            and confirm._SIGNATURE_WORD_RE.search(r["location_raw"] or "")
        }
        # 今回の辞書追加とは無関係な既存2件のNGを記録し、正常とは報告しない。
        self.assertEqual(set(after_suspicious), {"1a0ae5b0d63d9ae3", "1a0ad51eb2c57639"})
        for mid, rec in after_suspicious.items():
            self.assertEqual(rec, before_suspicious[mid])
        self.assertFalse(set(after_suspicious) & shiomi_ids)
        self.assertTrue(confirm.check_count_consistency(757, sum(r["location"] is not None for r in after),
                                                       sum(r["location"] is None for r in after)))
        self.assertTrue(confirm.check_location_values([r for r in after if r["location"] is not None],
                                                     [r for r in after if r["location"] is None]))
        self.assertTrue(confirm.check_signature_contamination([r["after"] for r in changes]))
        write_jsonl(str(focused / "preexisting_confirm_ng.jsonl"), [
            {"message_id": mid, "before": before_suspicious[mid], "after": rec}
            for mid, rec in after_suspicious.items()
        ])
        confirm_exit = 0
        with patch.object(confirm, "_STEP_DIR", focused):
            try:
                confirm.main()
            except SystemExit as exc:
                confirm_exit = exc.code
        self.assertEqual(confirm_exit, 1)
        self.assertEqual(hashes, [hashlib.sha256(p.read_bytes()).hexdigest() for p in canonical])
        write_jsonl(str(focused / "summary.jsonl"), [{
            "input": 757, "changed": 9, "null_to_kanto": 5, "remote_to_kanto": 1,
            "kinki_to_kanto": 1, "kanto_evidence_changed": 2,
            "non_shiomi_differences": 0, "new_signature_hits": 0,
            "all_changes_label_matches": True, "changed_records_confirm": "OK",
            "full_confirm": "NG", "full_confirm_exit": confirm_exit,
            "preexisting_signature_hits_before": len(before_suspicious),
            "preexisting_signature_hits_after": len(after_suspicious),
        }])


if __name__ == "__main__":
    unittest.main()

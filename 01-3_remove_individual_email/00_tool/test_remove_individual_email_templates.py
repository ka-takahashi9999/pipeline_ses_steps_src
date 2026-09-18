#!/usr/bin/env python3
"""2026-09-17入力の限定退避Focused Test。通常成果物は上書きしない。"""
import copy
import hashlib
import importlib.util
import sys
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

STEP = Path(__file__).resolve().parent.parent
ROOT = STEP.parent
sys.path.insert(0, str(STEP / "00_tool"))
import remove_individual_email as target
from common.file_utils import ensure_result_dirs
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl

EXPECTED = {
    "1a0ad9150dc1f6dd": ("multi_item_mail", "hyperlink_three_resource_list_v1"),
    "1a0ad4dad3d31df2": ("multi_item_mail", "aliplaza_three_resource_blocks_v1"),
    "1a0acf03475efda6": ("list_or_portal_notice", "identity_python_list_portal_v1"),
    "1a0aeccf899375aa": ("service_notification", "chotatsu_connection_invitation_v1"),
    "1a0ae6e35e8ed3a6": ("service_notification", "chotatsu_connection_invitation_v1"),
    "1a0ad952ac225881": ("service_notification", "chotatsu_connection_invitation_v1"),
    "1a0acf6790c511f7": ("service_notification", "chotatsu_connection_invitation_v1"),
    "1a0ad1b2fa385eaa": ("service_notification", "chotatsu_boost_digest_v1"),
}


class TemplateFocusedTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.master = read_jsonl_as_dict(target.INPUT_MASTER)
        cls.prev = read_jsonl_as_list(target.INPUT_PREV)
        cls.rules = target.load_exclude_list(target.EXCLUDE_LIST_PATH)

    def test_latest_input_and_confirm(self):
        canonical = [STEP / "01_result" / name for name in
                     (target.OUTPUT_FILTERED, target.OUTPUT_REMOVED)]
        before_hashes = [hashlib.sha256(p.read_bytes()).hexdigest() for p in canonical]
        old_filtered, old_removed = [], []
        new_filtered, new_removed, details = [], [], []
        for row in self.prev:
            mid = row["message_id"]
            rec = self.master[mid]
            legacy = (target.is_excluded(rec, *self.rules)
                      or target.detect_p1_exclusion_reason(rec["subject"]))
            (old_removed if legacy else old_filtered).append({"message_id": mid})
            reason = target.determine_exclusion_reason(rec, *self.rules)
            (new_removed if reason else new_filtered).append({"message_id": mid})
            if reason in target.TEMPLATE_REASONS:
                decision = target.detect_template_exclusion(rec)
                self.assertEqual(decision, EXPECTED[mid])
                details.append(target.build_detail_record(rec, *decision))
        self.assertEqual(len(self.prev), 2757)
        self.assertEqual(old_filtered, read_jsonl_as_list(str(canonical[0])))
        self.assertEqual(old_removed, read_jsonl_as_list(str(canonical[1])))
        self.assertEqual((len(old_filtered), len(old_removed)), (2654, 103))
        self.assertEqual((len(new_filtered), len(new_removed)), (2646, 111))
        self.assertEqual({r["message_id"] for r in details}, set(EXPECTED))
        self.assertEqual(Counter(r["reason"] for r in details),
                         {"multi_item_mail": 2, "list_or_portal_notice": 1, "service_notification": 5})
        removed_ids = {r["message_id"] for r in new_removed}
        new_ids = {r["message_id"] for r in new_filtered}
        self.assertEqual(removed_ids - {r["message_id"] for r in old_removed}, set(EXPECTED))
        self.assertFalse(new_ids & set(EXPECTED))  # 3名分の混在を生む両メールも後続へ渡さない
        for filename, before, after in (("projects.jsonl", 757, 757), ("resources.jsonl", 1891, 1883)):
            rows = read_jsonl_as_list(str(ROOT / "02-2_classify_output_file_project_resource" / "01_result" / filename))
            self.assertEqual(len(rows), before)
            self.assertEqual(sum(r["message_id"] in new_ids for r in rows), after)

        focused = STEP / "02_confirm" / "focused_template_exclusion"
        dirs = ensure_result_dirs(str(focused))
        # 実際のmainの書き込み・confirmも隔離した出力で実行する。
        with patch.object(target, "ensure_result_dirs", return_value=dirs):
            target.main()
        saved = read_jsonl_as_list(str(dirs["result"] / target.OUTPUT_DETAIL))
        self.assertEqual(saved, details)
        for detail in saved:
            original = self.master[detail["message_id"]]
            self.assertEqual({k: v for k, v in detail.items() if k not in ("reason", "rule_id")}, original)
            self.assertTrue({"message_id", "subject", "from", "date", "body_text", "attachments", "html_links"} <= detail.keys())
        spec = importlib.util.spec_from_file_location("confirm_templates", STEP / "02_confirm" / "confirm_remove_individual_email.py")
        confirm = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(confirm)
        with patch.object(confirm, "_STEP_DIR", focused):
            confirm.main()
        # 再実行しても詳細JSONLの重複・古いレコードが残らない。
        detail_path = dirs["result"] / target.OUTPUT_DETAIL
        digest = hashlib.sha256(detail_path.read_bytes()).hexdigest()
        with patch.object(target, "ensure_result_dirs", return_value=dirs):
            target.main()
        self.assertEqual(hashlib.sha256(detail_path.read_bytes()).hexdigest(), digest)
        self.assertEqual(before_hashes, [hashlib.sha256(p.read_bytes()).hexdigest() for p in canonical])
        write_jsonl(str(focused / "summary.jsonl"), [{
            "input": 2757, "passed_before": 2654, "passed_after": 2646,
            "removed_before": 103, "removed_after": 111,
            "reasons": dict(Counter(r["reason"] for r in saved)),
            "project_before": 757, "project_after": 757,
            "resource_before": 1891, "resource_after": 1883,
            "unexpected_exclusions": 0, "legacy_differences": 0,
            "original_record_preserved": True, "confirm": "OK",
        }])

    def test_sender_subject_structure_guards(self):
        for mid in EXPECTED:
            rec = self.master[mid]
            for key, value in (("from", "other@example.com"), ("subject", "人材 一覧 新着"),
                               ("body_text", "人材 一覧 新着"), ("body_text", "")):
                with self.subTest(mid=mid, key=key):
                    self.assertIsNone(target.detect_template_exclusion(dict(rec, **{key: value})))
            for key in ("from", "subject", "body_text", "attachments", "html_links"):
                changed = dict(rec)
                changed.pop(key)
                self.assertIsNone(target.detect_template_exclusion(changed))

    def test_notices_with_individual_or_attachment_are_kept(self):
        for mid, (reason, _) in EXPECTED.items():
            if reason == "multi_item_mail":
                continue
            rec = self.master[mid]
            self.assertIsNone(target.detect_template_exclusion(dict(rec, attachments=[{"filename": "skill.xlsx"}])))
            for text in ("\n【氏 名】A.T\n【単価】70万", "\n案件名: Java開発\n必須スキル: Java"):
                self.assertIsNone(target.detect_template_exclusion(dict(rec, body_text=rec["body_text"] + text)))

    def test_multiple_structure_and_legacy_precedence(self):
        rec = self.master["1a0ad4dad3d31df2"]
        self.assertIsNone(target.detect_template_exclusion(dict(rec, body_text=rec["body_text"].split("要員情報２")[0])))
        rec = self.master["1a0ad9150dc1f6dd"]
        body = "\n".join(line for line in rec["body_text"].splitlines() if not line.startswith("・VBA/RPA/SQL"))
        self.assertIsNone(target.detect_template_exclusion(dict(rec, body_text=body)))
        self.assertEqual(target.determine_exclusion_reason(rec, {target.extract_email(rec["from"])}, []), "manual_exclude_list")
        self.assertEqual(target.determine_exclusion_reason(dict(rec, subject="要員3名"), set(), []), "multiple_resource_explicit_subject_count")

    def test_audit_collision_and_confirm_corruption(self):
        original = self.master["1a0ad4dad3d31df2"]
        with self.assertRaises(ValueError):
            target.build_detail_record(dict(original, reason="original"), *EXPECTED[original["message_id"]])
        detail = target.build_detail_record(original, *EXPECTED[original["message_id"]])
        spec = importlib.util.spec_from_file_location("confirm_corruption", STEP / "02_confirm" / "confirm_remove_individual_email.py")
        confirm = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(confirm)
        ids = [{"message_id": original["message_id"]}]
        for key in ("body_text", "attachments", "html_links", "reason", "rule_id"):
            corrupted = copy.deepcopy(detail)
            corrupted[key] = "corrupted"
            self.assertFalse(confirm.check_detail_records(ids, [], ids, [corrupted], self.master))
        self.assertFalse(confirm.check_detail_records(ids, [], ids, [], self.master))


if __name__ == "__main__":
    unittest.main()

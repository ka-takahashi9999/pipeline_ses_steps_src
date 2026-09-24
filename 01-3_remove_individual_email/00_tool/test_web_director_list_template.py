"""Webディレクター一覧テンプレートの最新入力Focused Test（本番出力は書かない）。"""
import copy
import importlib.util
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

import remove_individual_email as target
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list


class WebDirectorListTemplateTest(unittest.TestCase):
    REPRESENTATIVE = '1a0cbd3700ed3089'
    DECISION = ('list_or_portal_notice', 'identity_web_director_list_portal_v1')

    @classmethod
    def setUpClass(cls):
        cls.master = read_jsonl_as_dict(target.INPUT_MASTER)
        cls.original = cls.master[cls.REPRESENTATIVE]
        cls.rules = target.load_exclude_list(target.EXCLUDE_LIST_PATH)

    def test_reusable_id_and_delivery_url(self):
        changed = copy.deepcopy(self.original)
        changed['message_id'] = 'future-message-id'
        changed['date'] = 'future delivery'
        url = changed['html_links'][0]['href']
        future_url = 'https://info.techcareer.jp/e/998201/newSheet-edit-gid-555/newDelivery/123456/h/newToken'
        changed['body_text'] = changed['body_text'].replace(url, future_url).replace('\r\n', '\n\n')
        changed['html_links'][0]['href'] = future_url
        changed['html_links'][0]['text'] = future_url
        self.assertEqual(target.detect_template_exclusion(changed), self.DECISION)
        changed.pop('message_id')
        self.assertEqual(target.detect_template_exclusion(changed), self.DECISION)

    def test_all_guards_and_individual_profiles(self):
        modifications = [
            ('from', 'other@id-entity.jp'), ('from', 'bp@other.example'),
            ('subject', 'Webディレクターの個別ご紹介'),
            ('body_text', 'Webディレクター 営業中 ご紹介 人材 一覧 案件'),
            ('html_links', []), ('html_links', [{'href': 'https://example.com/list', 'source': 'text/html'}]),
            ('html_links', [{'href': self.original['html_links'][0]['href'], 'source': 'text/plain'}]),
            ('attachments', [{'filename': 'profile.xlsx'}]),
        ]
        for key, value in modifications:
            with self.subTest(key=key, value=value):
                self.assertIsNone(target.detect_template_exclusion(dict(self.original, **{key: value})))
        for marker in ['氏名：A.T', '年齢：35歳', '最寄駅：新宿', '単価：70万円', 'Web制作ディレクション経験10年']:
            body = self.original['body_text'].replace('■人材一覧リスト', marker + '\n■人材一覧リスト')
            self.assertIsNone(target.detect_template_exclusion(dict(self.original, body_text=body)))
        for key in ['from', 'subject', 'body_text', 'html_links', 'attachments']:
            missing = dict(self.original)
            missing.pop(key)
            self.assertIsNone(target.detect_template_exclusion(missing))
        # 単語単独による除外は追加しない。
        for word in ['Webディレクター', '営業中', 'ご紹介', '人材', '一覧', '案件']:
            self.assertIsNone(target.determine_exclusion_reason(dict(self.original, subject=word), set(), []))

    def test_original_record_preserved(self):
        original = copy.deepcopy(self.original)
        self.assertEqual(target.detect_template_exclusion(original), self.DECISION)
        detail = target.build_detail_record(original, *self.DECISION)
        self.assertEqual({k: v for k, v in detail.items() if k not in ('reason', 'rule_id')}, self.original)
        self.assertEqual(original, self.original)
        self.assertTrue({'message_id', 'subject', 'from', 'date', 'body_text', 'attachments', 'html_links', 'reason', 'rule_id'} <= detail.keys())

    def test_latest_312_memory_replay_and_confirm(self):
        prev = read_jsonl_as_list(target.INPUT_PREV)
        self.assertEqual(len(prev), 312)
        result_dir = target._STEP_DIR / '01_result'
        old_pass = read_jsonl_as_list(str(result_dir / target.OUTPUT_FILTERED))
        old_removed = read_jsonl_as_list(str(result_dir / target.OUTPUT_REMOVED))
        old_details = read_jsonl_as_list(str(result_dir / target.OUTPUT_DETAIL))
        original_master = copy.deepcopy(self.master)
        captured = {}
        # 通常mainを実行するが、出力と実行時間の書き込みだけメモリへ差し替える。
        with patch.object(target, 'ensure_result_dirs', return_value={'result': result_dir, 'execution_time': Path('/tmp')}), \
             patch.object(target, 'write_jsonl', side_effect=lambda p, rows: captured.update({Path(p).name: copy.deepcopy(rows)})), \
             patch.object(target, 'write_execution_time'), patch.object(target, 'logger'):
            target.main()
        filtered = captured[target.OUTPUT_FILTERED]
        removed = captured[target.OUTPUT_REMOVED]
        details = captured[target.OUTPUT_DETAIL]
        self.assertEqual(filtered, [r for r in old_pass if r['message_id'] != self.REPRESENTATIVE])
        self.assertEqual([r for r in removed if r['message_id'] != self.REPRESENTATIVE], old_removed)
        self.assertEqual([r for r in details if r['message_id'] != self.REPRESENTATIVE], old_details)
        self.assertEqual((len(old_pass), len(filtered), len(old_removed), len(removed)), (259, 258, 53, 54))
        reasons = Counter(target.determine_exclusion_reason(self.master[r['message_id']], *self.rules) for r in prev)
        self.assertEqual(reasons, {None: 258, 'manual_exclude_list': 52, 'service_notification': 1, 'list_or_portal_notice': 1})
        new_ids = {r['message_id'] for r in filtered}
        self.assertIn('1a0cc6ca3e86ff6f', new_ids)  # Salesforce案件
        self.assertIn('1a0cbce0b08ac2b2', new_ids)  # 正常な個別Webディレクター紹介
        self.assertIsNone(target.determine_exclusion_reason(self.master['1a0cbce0b08ac2b2'], *self.rules))
        resources = read_jsonl_as_list(str(target._PROJECT_ROOT / '02-2_classify_output_file_project_resource/01_result/resources.jsonl'))
        self.assertEqual(len(resources), 229)
        self.assertEqual(sum(r['message_id'] in new_ids for r in resources), 228)
        self.assertTrue(all(set(r) == {'message_id'} for r in filtered + removed))
        self.assertEqual(next(r for r in details if r['message_id'] == self.REPRESENTATIVE), target.build_detail_record(self.original, *self.DECISION))
        self.assertEqual(self.master, original_master)
        spec = importlib.util.spec_from_file_location('confirm_web_list', target._STEP_DIR / '02_confirm/confirm_remove_individual_email.py')
        confirm = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(confirm)
        paths = {str(result_dir / name): rows for name, rows in captured.items()}
        paths[confirm.INPUT_PREV] = prev
        with patch.object(confirm, 'read_jsonl_as_list', side_effect=lambda p: paths[str(p)]), patch.object(confirm, 'logger') as logger:
            confirm.main()
            self.assertTrue(any('confirm OK' in str(call) for call in logger.ok.call_args_list))


if __name__ == '__main__':
    unittest.main()

"""
Success Cache方式の focused test（06-80 / 08-1）

本番成果物には触れず、一時ディレクトリ上のfixtureに対して
06-80 と 08-1 の main() を実行して検証する。
LLM呼び出し・full Pipeline実行は行わない。

【20260814データを使ったreplayの位置づけ】
実施済みのreplayは「Cache MISS 979件のうち893件が07-1で成功した場合」を仮定した
08-1ロジック検証であり、実際に893件をLLM再評価した結果ではない。
そこで得られた merged=2,566 は本番期待値ではない。

次回本番runの正しい期待値（S=07-1 success / E=07-1 error）:
    run前 Success Cache      : 1,673
    06-80                    : HIT=1,673 / MISS=979
    07-1                     : input=979 / success=S / error=E / S+E=979
    08-1                     : cache restore=1,673 / new success=S /
                               merged=1,673+S / error=E
    run後 Success Cache      : 1,673+S

実行:
  python3 08-1_restore_and_merge_requirement_skill_ai_matching/00_tool/test_success_cache_flow.py
"""

import importlib.util
import json
import logging
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from common.success_cache import (  # noqa: E402
    SuccessCacheError,
    build_cache_entry,
    build_comparison_key,
    load_success_cache,
    upsert_success_cache,
)

DUP_TOOL = PROJECT_ROOT / "06-80_duplicate_proposal_check/00_tool/duplicate_proposal_check.py"
MERGE_TOOL = (
    PROJECT_ROOT
    / "08-1_restore_and_merge_requirement_skill_ai_matching/00_tool"
    / "restore_and_merge_requirement_skill_ai_matching.py"
)


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


dup_mod = load_module("dup_tool_under_test", DUP_TOOL)
merge_mod = load_module("merge_tool_under_test", MERGE_TOOL)


def write_jsonl(path: Path, records: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> List[dict]:
    if not path.exists():
        return []
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def mail(message_id: str, sender: str, subject: str) -> dict:
    return {"message_id": message_id, "from": sender, "subject": subject}


def pair(project_mid: str, resource_mid: str) -> dict:
    return {
        "project_info": {"message_id": project_mid},
        "resource_info": {"message_id": resource_mid},
        "match_info": {"match_budget": True},
    }


def skills(tag: str) -> List[dict]:
    return [{"skill": f"skill-{tag}", "match": True, "note": "ok"}]


def ai_result(project_mid: str, resource_mid: str, tag: str) -> dict:
    return {
        "project_info": {"message_id": project_mid},
        "resource_info": {"message_id": resource_mid},
        "required_skills": skills(tag),
        "optional_skills": [],
        "evaluation_meta": {"llm_model": "test-model"},
    }


def message_key(record: dict) -> Tuple[str, str]:
    return (
        record.get("project_info", {}).get("message_id", ""),
        record.get("resource_info", {}).get("message_id", ""),
    )


class SuccessCacheFlowTestCase(unittest.TestCase):
    def setUp(self) -> None:
        logging.disable(logging.CRITICAL)
        self.tmp = Path(tempfile.mkdtemp(prefix="success_cache_test_"))
        self.dup_dir = self.tmp / "06-80"
        self.merge_dir = self.tmp / "08-1"
        (self.dup_dir / "01_result").mkdir(parents=True, exist_ok=True)
        (self.merge_dir / "01_result").mkdir(parents=True, exist_ok=True)

        self.cache_file = self.merge_dir / "01_result/success_cache.jsonl"

        # 06-80
        self.pairs_file = self.tmp / "input_pairs.jsonl"
        self.mail_master_file = self.tmp / "mail_master.jsonl"
        self.new_file = self.dup_dir / "01_result/duplicate_proposal_check.jsonl"
        self.duplicate_file = self.dup_dir / "01_result/99_duplicate.jsonl"
        self.diff_file = self.dup_dir / "01_result/diff_file.jsonl"
        self.bk_diff_file = self.dup_dir / "01_result/bk_diff_file.jsonl"

        dup_mod.STEP_DIR = self.dup_dir
        dup_mod.INPUT_PAIRS = self.pairs_file
        dup_mod.INPUT_MAIL_MASTER = self.mail_master_file
        dup_mod.INPUT_SUCCESS_CACHE = self.cache_file
        dup_mod.OUTPUT_NEW = self.new_file
        dup_mod.OUTPUT_DUPLICATE = self.duplicate_file
        dup_mod.OUTPUT_DIFF_FILE = self.diff_file
        dup_mod.OUTPUT_BK_DIFF_FILE = self.bk_diff_file

        # 08-1
        self.ai_result_file = self.tmp / "requirement_skill_ai_matching.jsonl"
        self.merged_file = self.merge_dir / "01_result/merged.jsonl"
        self.restored_file = self.merge_dir / "01_result/restored.jsonl"
        self.error_file = self.merge_dir / "01_result/99_error.jsonl"

        merge_mod.STEP_DIR = self.merge_dir
        merge_mod.INPUT_NEW_PAIRS = self.new_file
        merge_mod.INPUT_DUPLICATE_PAIRS = self.duplicate_file
        merge_mod.INPUT_DIFF_FILE = self.diff_file
        merge_mod.INPUT_NEW_AI_RESULT = self.ai_result_file
        merge_mod.SUCCESS_CACHE_FILE = self.cache_file
        merge_mod.OUTPUT_MERGED = self.merged_file
        merge_mod.OUTPUT_RESTORED = self.restored_file
        merge_mod.OUTPUT_ERROR = self.error_file
        merge_mod.DIAGNOSTICS_OUTPUT = self.merge_dir / "02_confirm/diagnostics.txt"

    def tearDown(self) -> None:
        logging.disable(logging.NOTSET)
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ------------------------------------------------------------------ helper

    def setup_inputs(self, mails: List[dict], pairs: List[dict]) -> None:
        write_jsonl(self.mail_master_file, mails)
        write_jsonl(self.pairs_file, pairs)

    def run_06_80(self) -> None:
        dup_mod.main()

    def simulate_07_1(self, tag: str, skip_message_keys=None) -> None:
        """06-80のMISS出力に対する07-1正常結果を作る（LLM呼び出しはしない）。"""
        skip = set(skip_message_keys or [])
        results = []
        for record in read_jsonl(self.new_file):
            key = message_key(record)
            if key in skip:
                continue
            results.append(ai_result(key[0], key[1], tag))
        write_jsonl(self.ai_result_file, results)

    def run_08_1(self) -> None:
        merge_mod.main()

    def cache_map(self) -> Dict[tuple, dict]:
        return load_success_cache(str(self.cache_file))

    # ------------------------------------------------------------------- tests

    def test_1_same_from_subject_different_message_id_is_cache_hit(self):
        """1. 同じfrom/subject・別message_id → Cache HIT"""
        key = build_comparison_key("p@x.com", "案件A", "r@y.com", "要員B")
        write_jsonl(
            self.cache_file,
            [build_cache_entry(key, "AAA", "BBB", skills("cached"), [], {"llm_model": "old"})],
        )
        self.setup_inputs(
            [mail("CCC", "p@x.com", "案件A"), mail("DDD", "r@y.com", "要員B")],
            [pair("CCC", "DDD")],
        )
        self.run_06_80()

        duplicates = read_jsonl(self.duplicate_file)
        self.assertEqual(len(duplicates), 1)
        self.assertEqual(message_key(duplicates[0]), ("CCC", "DDD"))
        self.assertIs(duplicates[0]["duplicate_proposal_check"], True)

    def test_2_cache_hit_pair_is_not_sent_to_07_1(self):
        """2. Cache HIT pair → 07-1入力(06-80新規出力)に含まれない"""
        key = build_comparison_key("p@x.com", "案件A", "r@y.com", "要員B")
        write_jsonl(
            self.cache_file,
            [build_cache_entry(key, "AAA", "BBB", skills("cached"), [], {})],
        )
        self.setup_inputs(
            [
                mail("CCC", "p@x.com", "案件A"),
                mail("DDD", "r@y.com", "要員B"),
                mail("EEE", "p2@x.com", "案件C"),
                mail("FFF", "r2@y.com", "要員D"),
            ],
            [pair("CCC", "DDD"), pair("EEE", "FFF")],
        )
        self.run_06_80()

        new_keys = {message_key(r) for r in read_jsonl(self.new_file)}
        self.assertNotIn(("CCC", "DDD"), new_keys)
        self.assertIn(("EEE", "FFF"), new_keys)
        self.assertEqual(len(read_jsonl(self.diff_file)), 2)

    def test_3_cache_hit_result_is_rebound_to_current_message_id(self):
        """3. Cache HIT結果 → 今回message_idへrebind / cacheのsource_message_idsは保持"""
        key = build_comparison_key("p@x.com", "案件A", "r@y.com", "要員B")
        write_jsonl(
            self.cache_file,
            [build_cache_entry(key, "AAA", "BBB", skills("cached"), [], {"llm_model": "old"})],
        )
        self.setup_inputs(
            [mail("CCC", "p@x.com", "案件A"), mail("DDD", "r@y.com", "要員B")],
            [pair("CCC", "DDD")],
        )
        self.run_06_80()
        self.simulate_07_1("new")
        self.run_08_1()

        merged = read_jsonl(self.merged_file)
        self.assertEqual(len(merged), 1)
        self.assertEqual(message_key(merged[0]), ("CCC", "DDD"))
        self.assertIs(merged[0]["duplicate_proposal_check"], True)
        self.assertEqual(merged[0]["project_info"]["required_skills"], skills("cached"))
        self.assertEqual(merged[0]["evaluation_meta"], {"llm_model": "old"})

        entry = self.cache_map()[key]
        self.assertEqual(
            entry["source_message_ids"],
            {"project_message_id": "AAA", "resource_message_id": "BBB"},
        )

    def test_4_in_previous_diff_but_cache_miss_goes_to_07_1(self):
        """4. 前回diffには存在するがCache MISS → 07-1入力へ入る"""
        self.setup_inputs(
            [mail("CCC", "p@x.com", "案件A"), mail("DDD", "r@y.com", "要員B")],
            [pair("CCC", "DDD")],
        )
        # 前回diff（bk）に同じcomparison_keyが存在する状態を作る
        write_jsonl(
            self.diff_file,
            [
                {
                    "project_info": {"message_id": "AAA", "from": "p@x.com", "subject": "案件A"},
                    "resource_info": {"message_id": "BBB", "from": "r@y.com", "subject": "要員B"},
                }
            ],
        )
        self.assertFalse(self.cache_file.exists())

        self.run_06_80()

        self.assertEqual(len(read_jsonl(self.bk_diff_file)), 1)
        new_keys = {message_key(r) for r in read_jsonl(self.new_file)}
        self.assertIn(("CCC", "DDD"), new_keys)
        self.assertEqual(read_jsonl(self.duplicate_file), [])

    def test_5_partial_error_upserts_only_success(self):
        """5. 950 success / 50 error → success 950だけcache upsert"""
        mails = []
        pairs = []
        for i in range(1000):
            pmid = f"P{i:04d}"
            rmid = f"R{i:04d}"
            mails.append(mail(pmid, f"p{i}@x.com", f"案件{i}"))
            mails.append(mail(rmid, f"r{i}@y.com", f"要員{i}"))
            pairs.append(pair(pmid, rmid))
        self.setup_inputs(mails, pairs)
        self.run_06_80()

        # 末尾50件を07-1 error 相当（正常結果なし）にする
        error_keys = {(f"P{i:04d}", f"R{i:04d}") for i in range(950, 1000)}
        self.simulate_07_1("new", skip_message_keys=error_keys)
        self.assertEqual(len(read_jsonl(self.ai_result_file)), 950)

        self.run_08_1()

        merged = read_jsonl(self.merged_file)
        errors = read_jsonl(self.error_file)
        self.assertEqual(len(merged), 950)
        self.assertEqual(len(errors), 50)
        self.assertEqual({e["error_type"] for e in errors}, {"new_ai_result_not_found"})
        self.assertEqual(len(self.cache_map()), 950)

        # 8. merged + error = diff（message_idペア）
        diff_keys = {message_key(r) for r in read_jsonl(self.diff_file)}
        self.assertEqual(
            {message_key(r) for r in merged} | {message_key(r) for r in errors},
            diff_keys,
        )

        # 次runでは950件がCache HIT / 50件がCache MISS
        self.run_06_80()
        self.assertEqual(len(read_jsonl(self.duplicate_file)), 950)
        self.assertEqual(len(read_jsonl(self.new_file)), 50)

    def test_6_restore_works_when_source_message_id_missing_from_mail_master(self):
        """6. 古いsource_message_idが現行mail masterに無くてもcomparison_keyで復元可能"""
        key = build_comparison_key("p@x.com", "案件A", "r@y.com", "要員B")
        write_jsonl(
            self.cache_file,
            [build_cache_entry(key, "OLD_P", "OLD_R", skills("cached"), [], {})],
        )
        # mail master に OLD_P / OLD_R は存在しない
        self.setup_inputs(
            [mail("CCC", "p@x.com", "案件A"), mail("DDD", "r@y.com", "要員B")],
            [pair("CCC", "DDD")],
        )
        self.run_06_80()
        self.simulate_07_1("new")
        self.run_08_1()

        merged = read_jsonl(self.merged_file)
        self.assertEqual(len(merged), 1)
        self.assertEqual(message_key(merged[0]), ("CCC", "DDD"))
        self.assertEqual(merged[0]["project_info"]["required_skills"], skills("cached"))
        self.assertEqual(read_jsonl(self.error_file), [])

    def test_7_rerun_same_input_does_not_grow_cache_or_merged(self):
        """7. 同一入力再実行 → cache件数・merged件数・内容が増殖しない"""
        mails = []
        pairs = []
        for i in range(5):
            mails.append(mail(f"P{i}", f"p{i}@x.com", f"案件{i}"))
            mails.append(mail(f"R{i}", f"r{i}@y.com", f"要員{i}"))
            pairs.append(pair(f"P{i}", f"R{i}"))
        self.setup_inputs(mails, pairs)

        self.run_06_80()
        self.simulate_07_1("new")
        self.run_08_1()
        first_merged = read_jsonl(self.merged_file)
        first_cache_text = self.cache_file.read_text(encoding="utf-8")
        self.assertEqual(len(first_merged), 5)
        self.assertEqual(len(self.cache_map()), 5)

        # 2回目: 同一入力。全件Cache HITとなり07-1入力は0件
        self.run_06_80()
        self.assertEqual(len(read_jsonl(self.new_file)), 0)
        self.simulate_07_1("new")
        self.run_08_1()

        second_merged = read_jsonl(self.merged_file)
        self.assertEqual(len(second_merged), 5)
        self.assertEqual(len(self.cache_map()), 5)
        self.assertEqual(self.cache_file.read_text(encoding="utf-8"), first_cache_text)
        self.assertEqual(read_jsonl(self.error_file), [])
        self.assertEqual(
            [(message_key(r), r["project_info"]["required_skills"]) for r in second_merged],
            [(message_key(r), r["project_info"]["required_skills"]) for r in first_merged],
        )

    def test_8_merged_plus_error_equals_diff_by_both_keys(self):
        """8. merged集合 + error集合 = diff集合（message_idペア / comparison_key）"""
        key = build_comparison_key("p0@x.com", "案件0", "r0@y.com", "要員0")
        write_jsonl(
            self.cache_file,
            [build_cache_entry(key, "OLD_P", "OLD_R", skills("cached"), [], {})],
        )
        mails = []
        pairs = []
        for i in range(4):
            mails.append(mail(f"P{i}", f"p{i}@x.com", f"案件{i}"))
            mails.append(mail(f"R{i}", f"r{i}@y.com", f"要員{i}"))
            pairs.append(pair(f"P{i}", f"R{i}"))
        self.setup_inputs(mails, pairs)
        self.run_06_80()

        # 1件を07-1 error 相当にする
        self.simulate_07_1("new", skip_message_keys={("P3", "R3")})
        self.run_08_1()

        diff_records = read_jsonl(self.diff_file)
        merged = read_jsonl(self.merged_file)
        errors = read_jsonl(self.error_file)

        self.assertEqual(len(merged) + len(errors), len(diff_records))

        diff_message_keys = {message_key(r) for r in diff_records}
        self.assertEqual(
            {message_key(r) for r in merged} | {message_key(r) for r in errors},
            diff_message_keys,
        )
        self.assertEqual(
            {message_key(r) for r in merged} & {message_key(r) for r in errors},
            set(),
        )

        diff_key_map = {
            message_key(r): build_comparison_key(
                r["project_info"]["from"],
                r["project_info"]["subject"],
                r["resource_info"]["from"],
                r["resource_info"]["subject"],
            )
            for r in diff_records
        }
        merged_keys = {diff_key_map[message_key(r)] for r in merged}
        error_keys = {diff_key_map[message_key(r)] for r in errors}
        self.assertEqual(merged_keys | error_keys, set(diff_key_map.values()))


    def test_9_empty_comparison_key_fails_fast_in_06_80(self):
        """A. comparison_keyの1項目が空 → 06-80でfail-fast / 07-1入力へ入らない"""
        self.setup_inputs(
            [
                mail("P0", "p0@x.com", "案件0"),
                mail("R0", "r0@y.com", "要員0"),
                mail("P1", "p1@x.com", "案件1"),
                mail("R1", "r1@y.com", ""),  # resource_subject が空
            ],
            [pair("P0", "R0"), pair("P1", "R1")],
        )

        with self.assertRaises(SystemExit) as ctx:
            self.run_06_80()
        self.assertEqual(ctx.exception.code, 1)

        # 07-1入力（新規出力）も diff_file も書き進めない
        self.assertFalse(self.new_file.exists())
        self.assertFalse(self.duplicate_file.exists())
        self.assertFalse(self.diff_file.exists())

        # ロジック単体: 空keyは同一identity扱いしない
        self.assertFalse(
            dup_mod.is_complete_comparison_key(build_comparison_key("p", "s", "r", ""))
        )
        incomplete = dup_mod.find_incomplete_comparison_keys(
            [
                {
                    "project_info": {"message_id": "P1", "from": "p1@x.com", "subject": "案件1"},
                    "resource_info": {"message_id": "R1", "from": "r1@y.com", "subject": ""},
                }
            ]
        )
        self.assertEqual(len(incomplete), 1)
        self.assertEqual(incomplete[0]["empty_fields"], ["resource_subject"])


class SuccessCacheValidationTestCase(unittest.TestCase):
    """cache不整合はCache MISSにせず明示的エラーで停止する。"""

    def setUp(self) -> None:
        logging.disable(logging.CRITICAL)
        self.tmp = Path(tempfile.mkdtemp(prefix="success_cache_validate_"))
        self.cache_file = self.tmp / "cache.jsonl"
        self.key = build_comparison_key("p@x.com", "案件A", "r@y.com", "要員B")

    def tearDown(self) -> None:
        logging.disable(logging.NOTSET)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_missing_cache_file_is_empty_cache(self):
        self.assertEqual(load_success_cache(str(self.cache_file)), {})

    def test_empty_comparison_key_raises(self):
        entry = build_cache_entry(self.key, "AAA", "BBB", skills("x"), [], {})
        entry["comparison_key"]["resource_subject"] = ""
        write_jsonl(self.cache_file, [entry])
        with self.assertRaises(SuccessCacheError):
            load_success_cache(str(self.cache_file))

    def test_cache_version_mismatch_raises(self):
        entry = build_cache_entry(self.key, "AAA", "BBB", skills("x"), [], {})
        entry["cache_version"] = 2
        write_jsonl(self.cache_file, [entry])
        with self.assertRaises(SuccessCacheError):
            load_success_cache(str(self.cache_file))

    def test_duplicate_comparison_key_raises(self):
        entry = build_cache_entry(self.key, "AAA", "BBB", skills("x"), [], {})
        write_jsonl(self.cache_file, [entry, dict(entry)])
        with self.assertRaises(SuccessCacheError):
            load_success_cache(str(self.cache_file))

    def test_missing_evaluation_schema_raises(self):
        entry = build_cache_entry(self.key, "AAA", "BBB", skills("x"), [], {})
        del entry["required_skills"]
        write_jsonl(self.cache_file, [entry])
        with self.assertRaises(SuccessCacheError):
            load_success_cache(str(self.cache_file))

    def test_upsert_keeps_untouched_entries_and_is_atomic(self):
        key2 = build_comparison_key("p2@x.com", "案件C", "r2@y.com", "要員D")
        write_jsonl(
            self.cache_file,
            [
                build_cache_entry(self.key, "AAA", "BBB", skills("old"), [], {}),
                build_cache_entry(key2, "EEE", "FFF", skills("keep"), [], {}),
            ],
        )
        stats = upsert_success_cache(
            str(self.cache_file),
            [build_cache_entry(self.key, "CCC", "DDD", skills("new"), [], {})],
        )
        cache = load_success_cache(str(self.cache_file))

        self.assertEqual(stats["before_count"], 2)
        self.assertEqual(stats["after_count"], 2)
        self.assertEqual(stats["updated"], 1)
        self.assertEqual(stats["inserted"], 0)
        self.assertEqual(cache[self.key]["required_skills"], skills("new"))
        self.assertEqual(
            cache[self.key]["source_message_ids"],
            {"project_message_id": "CCC", "resource_message_id": "DDD"},
        )
        self.assertEqual(cache[key2]["required_skills"], skills("keep"))
        self.assertEqual(list(self.tmp.glob("*.tmp")), [])

    def test_duplicate_new_entries_raises_and_keeps_cache_unchanged(self):
        """B. 同一comparison_keyを2件upsert → SuccessCacheError / cacheファイル無変更"""
        write_jsonl(
            self.cache_file,
            [build_cache_entry(self.key, "AAA", "BBB", skills("old"), [], {})],
        )
        before = self.cache_file.read_text(encoding="utf-8")

        with self.assertRaises(SuccessCacheError):
            upsert_success_cache(
                str(self.cache_file),
                [
                    build_cache_entry(self.key, "CCC", "DDD", skills("new1"), [], {}),
                    build_cache_entry(self.key, "EEE", "FFF", skills("new2"), [], {}),
                ],
            )

        self.assertEqual(self.cache_file.read_text(encoding="utf-8"), before)
        self.assertEqual(list(self.tmp.glob("*.tmp")), [])

    def test_upsert_stats_match_unique_keys(self):
        """inserted / updated が一意キー単位の実数と一致する"""
        key2 = build_comparison_key("p2@x.com", "案件C", "r2@y.com", "要員D")
        key3 = build_comparison_key("p3@x.com", "案件E", "r3@y.com", "要員F")
        write_jsonl(
            self.cache_file,
            [build_cache_entry(self.key, "AAA", "BBB", skills("old"), [], {})],
        )
        stats = upsert_success_cache(
            str(self.cache_file),
            [
                build_cache_entry(self.key, "CCC", "DDD", skills("new"), [], {}),
                build_cache_entry(key2, "EEE", "FFF", skills("new"), [], {}),
                build_cache_entry(key3, "GGG", "HHH", skills("new"), [], {}),
            ],
        )
        self.assertEqual(stats, {"before_count": 1, "after_count": 3, "inserted": 2, "updated": 1})
        self.assertEqual(len(load_success_cache(str(self.cache_file))), 3)


class ConfirmScriptGitSyncTestCase(unittest.TestCase):
    """C. confirmスクリプトだけが正規sync経路の対象になること。"""

    SYNC_SCRIPT = PROJECT_ROOT / "tools/pipeline_sync_git.sh"
    CONFIRM_TARGETS = (
        "06-80_duplicate_proposal_check/02_confirm/confirm_duplicate_proposal_check.py",
        "07-1_requirement_skill_ai_matching/02_confirm/confirm_requirement_skill_ai_matching.py",
        "08-1_restore_and_merge_requirement_skill_ai_matching/02_confirm"
        "/confirm_restore_and_merge_requirement_skill_ai_matching.py",
    )
    EXCLUDED_TARGETS = (
        "06-80_duplicate_proposal_check/02_confirm/confirm_result_duplicate_proposal_check.txt",
        "08-1_restore_and_merge_requirement_skill_ai_matching/02_confirm"
        "/diagnostics_restore_and_merge_requirement_skill_ai_matching.txt",
        "07-1_requirement_skill_ai_matching/01_result/run_metadata.json",
        "08-1_restore_and_merge_requirement_skill_ai_matching/01_result"
        "/merged_requirement_skill_ai_matching.jsonl",
    )

    def run_dry_run(self, target: str):
        return subprocess.run(
            ["bash", str(self.SYNC_SCRIPT), "--dry-run", target],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(PROJECT_ROOT),
        )

    def test_confirm_scripts_are_sync_targets(self):
        for target in self.CONFIRM_TARGETS:
            if not (PROJECT_ROOT / target).exists():
                self.skipTest(f"対象が存在しない: {target}")
            out = self.run_dry_run(target).stdout.decode("utf-8", errors="replace")
            self.assertIn("--- 同期対象 (1件) ---", out, msg=target)
            self.assertIn(target, out, msg=target)
            self.assertNotIn("同期対象外", out, msg=target)

    def test_generated_files_are_not_sync_targets(self):
        for target in self.EXCLUDED_TARGETS:
            if not (PROJECT_ROOT / target).exists():
                continue
            proc = self.run_dry_run(target)
            out = proc.stdout.decode("utf-8", errors="replace")
            self.assertNotEqual(proc.returncode, 0, msg=target)
            self.assertIn("同期対象外", out, msg=target)

    def test_02_confirm_directory_itself_is_not_a_sync_target(self):
        proc = self.run_dry_run("06-80_duplicate_proposal_check/02_confirm")
        out = proc.stdout.decode("utf-8", errors="replace")
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("同期対象外", out)


if __name__ == "__main__":
    unittest.main(verbosity=2)

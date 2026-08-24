"""08-5 retention sidecar第三入力のoffline focused tests。"""

import copy
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parents[1]
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

import high_score_required_skill_recheck as direct
import high_score_required_skill_recheck_batch as batch
from common.json_utils import write_jsonl


def record(project, resource, retention=False):
    value = {
        "project_info": {
            "message_id": project,
            "required_skills": [
                {"skill": "Python開発経験", "match": False, "note": "1ヶ月のみ"}
            ],
            "optional_skills": [],
        },
        "resource_info": {"message_id": resource},
        "duplicate_proposal_check": False,
        "match_info": {"required_skills_match_rate": 0.5},
    }
    if retention:
        value["retention_guard"] = {
            "destination": "08-5_recheck_only",
            "proposal_ready_direct": False,
        }
    return value


class RetentionInputWiringTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="08_5_retention_input_")
        root = Path(self.temporary.name)
        self.normal_100 = root / "100.jsonl"
        self.normal_80 = root / "80.jsonl"
        self.sidecar = root / "retention.jsonl"
        write_jsonl(str(self.normal_100), [record("p1", "r1")])
        write_jsonl(str(self.normal_80), [record("p2", "r2")])
        write_jsonl(
            str(self.sidecar),
            [
                record("p1", "r1", retention=True),
                record("1a0230d86a22a79b", "1a0225959a74d90a", retention=True),
            ],
        )
        self.base = (
            ("100percent", self.normal_100),
            ("80to99percent", self.normal_80),
        )

    def tearDown(self):
        self.temporary.cleanup()

    def test_flag_zero_is_exact_existing_input_set_and_ignores_sidecar(self):
        with patch.object(direct, "INPUT_SCORE_FILES", self.base), patch.object(
            direct, "RETENTION_SIDECAR", self.sidecar
        ), patch.object(direct, "_concurrent_input_enabled", return_value=False):
            configured = direct.configured_input_score_files()
            rows = list(direct.iter_input_records())
        self.assertEqual(configured, self.base)
        self.assertEqual(
            [(band, direct._input_pair_key(row)) for band, row in rows],
            [
                ("100percent", ("p1", "r1")),
                ("80to99percent", ("p2", "r2")),
            ],
        )

    def test_flag_zero_preserves_existing_duplicates_exactly(self):
        duplicate_base = (
            ("100percent", self.normal_100),
            ("80to99percent", self.normal_100),
        )
        with patch.object(direct, "INPUT_SCORE_FILES", duplicate_base), patch.object(
            direct, "RETENTION_SIDECAR", self.sidecar
        ), patch.object(direct, "_concurrent_input_enabled", return_value=False):
            rows = list(direct.iter_input_records())
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][1], rows[1][1])

    def test_flag_one_adds_sidecar_last_and_deduplicates_normal_first(self):
        with patch.object(direct, "INPUT_SCORE_FILES", self.base), patch.object(
            direct, "RETENTION_SIDECAR", self.sidecar
        ), patch.object(direct, "_concurrent_input_enabled", return_value=True):
            rows = list(direct.iter_input_records())
        keys = [direct._input_pair_key(row) for _, row in rows]
        self.assertEqual(
            keys,
            [
                ("p1", "r1"),
                ("p2", "r2"),
                ("1a0230d86a22a79b", "1a0225959a74d90a"),
            ],
        )
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(rows[-1][0], "retention_guard")
        self.assertFalse(
            rows[-1][1]["retention_guard"]["proposal_ready_direct"]
        )

    def test_input_wiring_does_not_mutate_normal_or_retention_records(self):
        before_normal = list(direct.read_jsonl(str(self.normal_100)))
        before_sidecar = list(direct.read_jsonl(str(self.sidecar)))
        with patch.object(direct, "INPUT_SCORE_FILES", self.base), patch.object(
            direct, "RETENTION_SIDECAR", self.sidecar
        ), patch.object(direct, "_concurrent_input_enabled", return_value=True):
            list(direct.iter_input_records())
        self.assertEqual(
            before_normal, list(direct.read_jsonl(str(self.normal_100)))
        )
        self.assertEqual(
            before_sidecar, list(direct.read_jsonl(str(self.sidecar)))
        )

    def test_batch_context_loader_uses_same_deduplicated_iterator(self):
        rows = [
            ("100percent", record("p1", "r1")),
            ("retention_guard", record("p3", "r3", retention=True)),
        ]
        skillsheets = {
            "r1": {"success": True, "skillsheet": "Python実務"},
            "r3": {"success": True, "skillsheet": "Python実務"},
        }
        emails = {"p1": "Python案件", "p3": "Python案件"}
        with patch.object(direct, "iter_input_records", return_value=iter(rows)), patch.object(
            direct, "_load_skillsheet_map", return_value=skillsheets
        ), patch.object(direct, "_load_cleaned_email_map", return_value=emails):
            contexts = batch.load_production_contexts()
        self.assertEqual(len(contexts), 2)
        self.assertEqual(contexts[1]["score_band"], "retention_guard")
        self.assertEqual(contexts[1]["dispatch"], "batch")


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""Publication authority / phase / tail guard focused tests (AWS・S3変更なし)."""

import argparse
import copy
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import portal_s3_backup_rotation as target


PLAN_SPEC = importlib.util.spec_from_file_location(
    "tail_recovery_plan_for_test",
    Path(__file__).resolve().parent / "tail_recovery_plan.py",
)
plan = importlib.util.module_from_spec(PLAN_SPEC)
PLAN_SPEC.loader.exec_module(plan)


def inventory(size=10):
    return {
        "08-5_high_score_required_skill_recheck/01_result/production_commit.json": {
            "size": size,
            "etag": '"etag"',
            "last_modified": "2026-11-10T23:40:00+00:00",
        }
    }


def managed(when="2026-11-10T01:00:00+00:00"):
    return {
        "publication_kind": "managed",
        "source": {"run_date": "20261109", "run_id": "sfn-managed"},
        "verified_at": when,
        "manifest_sha256": "a" * 64,
        "manifest_source_path": "/fixture/manifest.jsonl",
        "summary_source_path": "/fixture/summary.json",
        "receipt_source_path": None,
        "manifest_inventory": {
            path: item["size"] for path, item in inventory().items()
        },
        "evidence_verified": True,
    }


def manual(when="2026-11-10T23:40:47+00:00", verified=True):
    return {
        "publication_kind": "manual_recovery",
        "source": {"run_date": "20261110", "run_id": "sfn-failed-recovered"},
        "verified_at": when,
        "manifest_sha256": "b" * 64,
        "manifest_source_path": "/fixture/receipt.jsonl#manifest_snapshot",
        "summary_source_path": "/fixture/receipt.jsonl#summary_snapshot",
        "receipt_source_path": "/fixture/receipt.jsonl",
        "current_fingerprint": inventory(),
        "bk1_fingerprint": inventory(9),
        "evidence_verified": verified,
    }


class TestPublicationAuthoritySelection(unittest.TestCase):
    def test_case_a_managed_publication_is_selected(self):
        result = target.select_latest_publication_authority(
            managed(), [], inventory(), inventory(9),
            {"run_date": "20261109", "run_id": "sfn-managed"},
            "2026-11-10T01:00:00+00:00",
        )
        self.assertEqual(result["publication_kind"], "managed")

    def test_case_b_failed_managed_then_verified_manual_is_selected(self):
        result = target.select_latest_publication_authority(
            managed(), [manual()], inventory(), inventory(9),
            {"run_date": "20261110", "run_id": "sfn-failed-recovered"},
            "2026-11-10T01:00:00+00:00",
        )
        self.assertEqual(result["publication_kind"], "manual_recovery")
        self.assertEqual(result["source"]["run_date"], "20261110")

    def test_case_c_manual_current_fingerprint_mismatch_fails_closed(self):
        with self.assertRaises(target.RotationError):
            target.select_latest_publication_authority(
                managed(), [manual()], inventory(11), inventory(9),
                {"run_date": "20261110", "run_id": "sfn-failed-recovered"},
                "2026-11-10T01:00:00+00:00",
            )

    def test_case_d_unverified_manual_is_not_adopted(self):
        result = target.select_latest_publication_authority(
            managed(), [manual(verified=False)], inventory(), inventory(9),
            {"run_date": "20261109", "run_id": "sfn-managed"},
            "2026-11-10T01:00:00+00:00",
        )
        self.assertEqual(result["publication_kind"], "managed")

    def test_case_e_newer_managed_publication_wins(self):
        newer = managed("2026-11-11T01:00:00+00:00")
        result = target.select_latest_publication_authority(
            newer, [manual("2026-11-10T23:40:47+00:00")], inventory(), inventory(9),
            {"run_date": "20261109", "run_id": "sfn-managed"},
            "2026-11-11T01:00:00+00:00",
        )
        self.assertEqual(result["publication_kind"], "managed")

    def test_manual_authority_does_not_use_old_managed_last_modified_cutoff(self):
        result = target.select_latest_publication_authority(
            managed(), [manual()], inventory(), inventory(9),
            {"run_date": "20261110", "run_id": "sfn-failed-recovered"},
            "2026-11-10T01:00:00+00:00",
        )
        self.assertEqual(result["current_fingerprint"], target._fingerprint_summary(inventory()))

    def test_manual_fingerprint_summary_change_is_rejected(self):
        authority = target.select_latest_publication_authority(
            managed(), [manual()], inventory(), inventory(9),
            {"run_date": "20261110", "run_id": "sfn-failed-recovered"},
            "2026-11-10T01:00:00+00:00",
        )
        with patch.object(target, "complete_fingerprints", side_effect=[inventory(11), inventory(9)]):
            with self.assertRaises(target.RotationError):
                target.verify_publication_authority_live(authority, object())

    def test_e2e_failed_scheduled_manual_publish_next_scheduled_handoff(self):
        """scheduled FAILED→manual publish→CURRENT更新→次scheduledの明示handoff。"""
        authority = target.select_latest_publication_authority(
            managed(), [manual()], inventory(), inventory(9),
            {"run_date": "20261110", "run_id": "sfn-failed-recovered"},
            "2026-11-10T01:00:00+00:00",
        )
        with tempfile.TemporaryDirectory() as temp:
            authority_path = Path(temp) / "authority.json"
            receipt_path = Path(temp) / "receipt.jsonl"
            receipt_path.write_text("{}\n", encoding="utf-8")
            authority["receipt_source_path"] = str(receipt_path)
            authority_path.write_text(json.dumps(authority), encoding="utf-8")
            args = argparse.Namespace(
                publication_authority_file=authority_path,
                manual_recovery_receipt=None,
                create_manual_receipt=False,
                manual_recovery_id=None,
                manual_source_run_id=None,
                manual_source_run_date=None,
                manual_source_execution_arn=None,
            )
            with patch.object(target, "run_manual_rotation", return_value={"ok": True}) as call:
                self.assertEqual(target.run(args, object()), {"ok": True})
            self.assertEqual(call.call_args.args[0].manual_recovery_receipt, receipt_path)


class TestRotationPhases(unittest.TestCase):
    def test_validating_rotation_started_rotation_verified_are_recorded(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "summary.json"
            args = argparse.Namespace(
                bootstrap=False, dry_run=False, _summary_path=path
            )
            for phase in (
                target.PHASE_VALIDATING,
                target.PHASE_ROTATION_STARTED,
                target.PHASE_ROTATION_VERIFIED,
            ):
                target._record_rotation_phase(args, phase)
                self.assertEqual(json.loads(path.read_text())["side_effect_phase"], phase)


def preflight_guard():
    return {
        "backup_status": "SUCCEEDED",
        "mode": "dry-run",
        "manual_recovery": {
            "verified": True,
            "source_guard": {
                "at_validating_boundary": True,
                "before_publication_boundary": False,
                "side_effect_phase": "VALIDATING",
                "rotation_started": False,
                "authority_fingerprint_verified": True,
                "downstream_80_8_started": False,
                "downstream_80_9_started": False,
                "partial_publish_detected": False,
                "publication_boundary_reached": False,
                "source_execution_redrive_count": 0,
            },
        },
    }


class TestTailRecoveryGuard(unittest.TestCase):
    def test_validating_failure_can_skip_completed_80_7(self):
        self.assertEqual(plan.decide(preflight_guard()), "SKIP_80_7")

    def test_rotation_started_failure_is_rejected(self):
        value = preflight_guard()
        value["manual_recovery"]["source_guard"].update(
            side_effect_phase="ROTATION_STARTED", rotation_started=True
        )
        with self.assertRaises(ValueError):
            plan.decide(value)

    def test_current_or_bk1_authority_change_is_rejected(self):
        value = preflight_guard()
        value["manual_recovery"]["source_guard"]["authority_fingerprint_verified"] = False
        with self.assertRaises(ValueError):
            plan.decide(value)

    def test_partial_publish_is_rejected(self):
        value = preflight_guard()
        value["manual_recovery"]["source_guard"]["partial_publish_detected"] = True
        with self.assertRaises(ValueError):
            plan.decide(value)


class TestBoundaryExecutionHistory(unittest.TestCase):
    def setUp(self):
        self.context = {
            "recovery_id": "recovery-next",
            "run_date": "20261111",
            "run_id": "sfn-failed-next",
            "execution_arn": "arn:fixture",
        }
        self.document = {"current_step": target.PUBLICATION_BOUNDARY_STEP_NAME}
        self.events = [{
            "id": 1,
            "type": "TaskStateEntered",
            "stateEnteredEventDetails": {"name": target.PUBLICATION_BOUNDARY_STATE},
        }]

    def test_phase_b_launcher_is_allowed_for_exact_validating_boundary(self):
        evidence = {
            "side_effect_phase": target.PHASE_VALIDATING,
            "rotation_started": False,
            "authority_fingerprint_verified": True,
            "downstream_80_8_started": False,
            "downstream_80_9_started": False,
            "partial_publish_detected": False,
        }
        with patch.object(
            target, "verify_manual_source", return_value=("status-key", self.document)
        ), patch.object(
            target, "_validating_boundary_evidence", return_value=evidence
        ), patch.object(
            target, "_get_execution_history", return_value=(self.events, 1)
        ):
            _, _, result = target.verify_manual_prepublication_source(
                self.context, object(), object(), authority_fingerprint_verified=True
            )
        self.assertTrue(result["launcher_state_reached"])
        self.assertFalse(result["publication_side_effect_boundary_reached"])

    def test_phase_b_launcher_remains_rejected_for_earlier_failure_claim(self):
        earlier = {"current_step": "80-7_manage_09_result_retention"}
        with patch.object(
            target, "verify_manual_source", return_value=("status-key", earlier)
        ), patch.object(
            target, "_get_execution_history", return_value=(self.events, 1)
        ):
            with self.assertRaises(target.RotationError):
                target.verify_manual_prepublication_source(
                    self.context, object(), object(), authority_fingerprint_verified=False
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)

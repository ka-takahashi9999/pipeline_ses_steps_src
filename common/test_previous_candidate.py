"""previous candidate 4-field joinのfocused test。"""

import tempfile
import unittest
from pathlib import Path

from common.previous_candidate import (
    PreviousCandidateError,
    mark_candidate_records,
    resolve_previous_candidate_path,
)


def candidate(
    project_from="project@example.com",
    project_subject="Project subject",
    resource_from="resource@example.com",
    resource_subject="Resource subject",
    project_message_id="project-current",
    resource_message_id="resource-current",
    duplicate_proposal_check=False,
):
    return {
        "project_sender_email": project_from,
        "project_subject": project_subject,
        "resource_sender_email": resource_from,
        "resource_subject": resource_subject,
        "project_message_id": project_message_id,
        "resource_message_id": resource_message_id,
        "duplicate_proposal_check": duplicate_proposal_check,
    }


class PreviousCandidateTest(unittest.TestCase):
    def marked(self, current, previous):
        return mark_candidate_records([current], [previous], "20260819")[0]

    def test_four_fields_match_is_previous_candidate(self):
        self.assertTrue(self.marked(candidate(), candidate())["previous_candidate"])

    def test_cache_hit_without_previous_final_is_not_previous_candidate(self):
        current = candidate(duplicate_proposal_check=True)
        self.assertFalse(mark_candidate_records([current], [], "20260819")[0]["previous_candidate"])

    def test_message_ids_are_not_identity(self):
        previous = candidate(project_message_id="project-old", resource_message_id="resource-old")
        self.assertTrue(self.marked(candidate(), previous)["previous_candidate"])

    def test_project_subject_difference_is_not_previous_candidate(self):
        self.assertFalse(self.marked(candidate(), candidate(project_subject="different"))["previous_candidate"])

    def test_resource_subject_difference_is_not_previous_candidate(self):
        self.assertFalse(self.marked(candidate(), candidate(resource_subject="different"))["previous_candidate"])

    def test_from_difference_is_not_previous_candidate(self):
        cases = [
            candidate(project_from="different@example.com"),
            candidate(resource_from="different@example.com"),
        ]
        for previous in cases:
            with self.subTest(previous=previous):
                self.assertFalse(self.marked(candidate(), previous)["previous_candidate"])

    def test_incomplete_identity_stops_instead_of_becoming_unknown(self):
        with self.assertRaises(PreviousCandidateError):
            mark_candidate_records([candidate(project_subject="")], [], "20260819")

    def test_previous_artifact_uses_latest_date_before_target(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            for date in ("20260814", "20260817", "20260820", "20260821"):
                (base / f"sales_proposal_candidates_{date}.jsonl").touch()
            path, date = resolve_previous_candidate_path(base, "20260820")
            self.assertEqual("20260817", date)
            self.assertEqual("sales_proposal_candidates_20260817.jsonl", path.name)


if __name__ == "__main__":
    unittest.main()

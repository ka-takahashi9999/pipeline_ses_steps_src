"""previous candidate 4-field joinのfocused test。"""

import tempfile
import unittest
from pathlib import Path

from common.previous_candidate import (
    PreviousCandidateError,
    candidate_identity,
    mark_candidate_records,
    pair_identity,
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
        self.assertEqual(
            candidate_identity(candidate()),
            ("project@example.com", "Project subject", "resource@example.com", "Resource subject"),
        )

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

    def test_empty_subject_uses_only_corresponding_message_id(self):
        for fields, expected in (
            ({"resource_subject": ""}, ("Project subject", "gmail_mid:resource-current")),
            ({"project_subject": ""}, ("gmail_mid:project-current", "Resource subject")),
            ({"project_subject": "", "resource_subject": ""},
             ("gmail_mid:project-current", "gmail_mid:resource-current")),
        ):
            with self.subTest(fields=fields):
                record = candidate(**fields)
                events = []
                identity = candidate_identity(record, events)
                self.assertEqual((identity[1], identity[3]), expected)
                self.assertEqual(record["project_subject"], fields.get("project_subject", "Project subject"))
                self.assertEqual(record["resource_subject"], fields.get("resource_subject", "Resource subject"))
                self.assertEqual(len(events), sum(not value for value in fields.values()))
                self.assertTrue(all(event[2:] == ("project-current", "resource-current") for event in events))

    def test_empty_subject_identity_is_stable_and_distinct_by_message_id(self):
        current = candidate(resource_subject="")
        self.assertEqual(candidate_identity(current), candidate_identity(candidate(resource_subject="")))
        different = candidate(resource_subject="", resource_message_id="resource-other")
        self.assertNotEqual(candidate_identity(current), candidate_identity(different))
        self.assertTrue(self.marked(current, candidate(resource_subject=""))["previous_candidate"])
        self.assertFalse(self.marked(current, different)["previous_candidate"])

    def test_previous_candidate_with_empty_subject_uses_same_fallback(self):
        previous = candidate(project_subject="", resource_subject="")
        current = candidate(project_subject="", resource_subject="")
        events = []
        marked = mark_candidate_records([current], [previous], "20260819", events)
        self.assertTrue(marked[0]["previous_candidate"])
        self.assertEqual(len(events), 4)
        self.assertEqual(previous["project_subject"], "")
        self.assertEqual(current["resource_subject"], "")

    def test_pair_and_candidate_identity_share_subject_fallback(self):
        pair = {
            "project_info": {"message_id": "project-current"},
            "resource_info": {"message_id": "resource-current"},
        }
        mail_master = {
            "project-current": {"from": "project@example.com", "subject": ""},
            "resource-current": {"from": "resource@example.com", "subject": ""},
        }
        events = []
        self.assertEqual(
            pair_identity(pair, mail_master, events),
            candidate_identity(candidate(project_subject="", resource_subject="")),
        )
        self.assertEqual(events, [
            ("project", "project-current", "project-current", "resource-current"),
            ("resource", "resource-current", "project-current", "resource-current"),
        ])
        self.assertEqual(mail_master["project-current"]["subject"], "")

    def test_non_subject_empty_identity_still_stops(self):
        with self.assertRaises(PreviousCandidateError):
            mark_candidate_records([candidate(project_from="")], [], "20260819")
        with self.assertRaises(PreviousCandidateError):
            candidate_identity(candidate(resource_subject="", resource_message_id=""))

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

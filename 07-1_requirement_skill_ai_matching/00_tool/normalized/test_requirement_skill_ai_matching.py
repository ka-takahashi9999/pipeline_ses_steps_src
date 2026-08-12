import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parents[2]
sys.path.insert(0, str(TOOL_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

import requirement_skill_ai_matching as target
from common.llm_client import LLMOutputTruncatedError


def make_skills(count):
    return [{"skill": f"skill-{index}", "match": None, "note": None} for index in range(count)]


class ProcessPairTest(unittest.TestCase):
    pair = {
        "project_info": {"message_id": "project-1"},
        "resource_info": {"message_id": "resource-1"},
    }
    skillsheet_map = {
        "resource-1": {
            "message_id": "resource-1",
            "success": True,
            "skillsheet": "Python経験あり",
            "source": "test",
        }
    }

    def test_67_skills_are_skipped_without_llm_call(self):
        project_skills_map = {
            "project-1": {"required_skills": make_skills(67), "optional_skills": []}
        }

        with patch.object(target, "call_llm") as call_llm_mock:
            result, error = target.process_pair(
                self.pair, project_skills_map, self.skillsheet_map, Mock()
            )

        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "project_skill_count_exceeded")
        self.assertIn("total=67", error["error_message"])
        call_llm_mock.assert_not_called()

    def test_required_and_optional_skills_are_counted_together(self):
        project_skills_map = {
            "project-1": {
                "required_skills": make_skills(30),
                "optional_skills": make_skills(11),
            }
        }

        with patch.object(target, "call_llm") as call_llm_mock:
            result, error = target.process_pair(
                self.pair, project_skills_map, self.skillsheet_map, Mock()
            )

        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "project_skill_count_exceeded")
        self.assertIn("required=30 optional=11 total=41", error["error_message"])
        call_llm_mock.assert_not_called()

    def test_40_skills_continue_and_success_schema_is_unchanged(self):
        project_skills_map = {
            "project-1": {"required_skills": make_skills(40), "optional_skills": []}
        }
        llm_response = {
            "required_skills": [
                {"skill": f"skill-{index}", "match": False, "note": "該当経験の記載なし"}
                for index in range(40)
            ],
            "optional_skills": [],
        }

        with patch.object(target, "call_llm", return_value=llm_response) as call_llm_mock:
            result, error = target.process_pair(
                self.pair, project_skills_map, self.skillsheet_map, Mock()
            )

        self.assertIsNone(error)
        self.assertEqual(
            set(result.keys()),
            {"project_info", "resource_info", "required_skills", "optional_skills", "evaluation_meta"},
        )
        call_llm_mock.assert_called_once()

    def test_truncated_output_is_recorded_separately(self):
        project_skills_map = {
            "project-1": {"required_skills": make_skills(1), "optional_skills": []}
        }

        with patch.object(
            target,
            "call_llm",
            side_effect=LLMOutputTruncatedError("finish_reason=length"),
        ):
            result, error = target.process_pair(
                self.pair, project_skills_map, self.skillsheet_map, Mock()
            )

        self.assertIsNone(result)
        self.assertEqual(error["error_type"], "llm_output_truncated")


if __name__ == "__main__":
    unittest.main()

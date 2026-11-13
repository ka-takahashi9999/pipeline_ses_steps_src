import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

from common.skill_canonical_map import SKILL_CANONICAL_MAP
from common.skill_registry import build_effective_matcher, build_registry_matcher
from common.skill_registry_runtime import create_runtime_context, load_runtime_context


ROOT = Path(__file__).resolve().parents[2]


def load_step_module(name: str, path: Path):
    old_config = sys.modules.pop("config", None)
    old_path = list(sys.path)
    try:
        sys.path.insert(0, str(path.parent))
        spec = importlib.util.spec_from_file_location(name, str(path))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path[:] = old_path
        sys.modules.pop("config", None)
        if old_config is not None:
            sys.modules["config"] = old_config


class SkillRegistryProductionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        path = create_runtime_context(
            ROOT,
            Path(cls.temp.name),
            "20261104",
            "production-focused-test",
            mode="registry_overlay",
        )
        cls.runtime = load_runtime_context(path, ROOT, expected_mode="registry_overlay")
        cls.project_module = load_step_module(
            "production_project_skill",
            ROOT / "03-8_extract_project_skill_category/00_tool/extract_project_skill_category.py",
        )
        cls.resource_module = load_step_module(
            "production_resource_skill",
            ROOT / "05-8_extract_resource_skill_category/00_tool/extract_resource_skill_category.py",
        )
        cls.skill_gate_module = load_step_module(
            "production_skill_gate",
            ROOT / "06-8_match_skill_category/00_tool/match_skill_category.py",
        )
        cls.required_gate_module = load_step_module(
            "production_required_gate",
            ROOT / "06-11_match_required_skills_list/00_tool/match_required_skills_list.py",
        )
        cls.matcher, _ = build_effective_matcher(
            cls.runtime.snapshot,
            ROOT / "03-8_extract_project_skill_category/00_tool/skill_dictionary.txt",
            SKILL_CANONICAL_MAP,
            {"production"},
        )
        cls.required_matcher = build_registry_matcher(
            cls.runtime.snapshot, {"production"}
        )

    def test_group_ownership_is_declarative(self):
        self.assertEqual(len(self.runtime.snapshot.production_canonical_names), 62)
        self.assertEqual(
            self.runtime.snapshot.production_canonical_names,
            tuple(
                name
                for name in self.runtime.snapshot.canonical_names
                if name not in {"Go", "Scrum", "組み込み"}
            ),
        )
        self.assertEqual(
            self.runtime.snapshot.shadow_canonical_names,
            ("Go", "Scrum", "組み込み"),
        )

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_project_extraction_and_primary_use_registry_overlay(self):
        body = "Springboot NodeJS SQLServer Aurora PowerBI Power-BI"
        required = {
            "p": {
                "required_skills": [
                    {"skill": "Spring Boot"},
                    {"skill": "PowerBI"},
                ]
            }
        }
        record, hits = self.project_module.build_registry_overlay_record(
            "p",
            body,
            self.matcher,
            self.required_matcher,
            self.runtime.snapshot,
            required,
        )
        self.assertEqual(
            record["skills"],
            ["Spring Boot", "Node.js", "SQL Server", "Amazon Aurora", "Power BI"],
        )
        self.assertEqual(record["primary_skills"], ["Spring Boot", "Power BI"])
        self.assertEqual(record["skills_by_category"]["DB"], ["SQL Server", "Amazon Aurora"])
        self.assertTrue(any(hit["matched_surface"] == "PowerBI" for hit in hits))
        self.assertTrue(all(len(hit["span"]) == 2 for hit in hits))

    def test_resource_extraction_deduplicates_aliases(self):
        record, hits = self.resource_module.build_registry_overlay_record(
            "r", "Power BI / PowerBI / Power-BI", self.matcher
        )
        self.assertEqual(record["skills"], ["Power BI"])
        self.assertEqual(record["skills_by_category"], {"データ分析": ["Power BI"]})
        self.assertEqual(len([hit for hit in hits if hit["canonical"] == "Power BI"]), 1)

    def test_06_8_matches_registry_canonicals(self):
        project, resource, primary = self.skill_gate_module.normalize_registry_owned_skills(
            ["PowerBI"], ["Power-BI"], ["Power BI"], self.runtime
        )
        result = self.skill_gate_module.judge_skill_category_match(
            project, resource, primary
        )
        self.assertTrue(result[0])
        self.assertEqual(result[1], ["Power BI"])

    def test_original_registry_groups_remain_canonicalized_in_production(self):
        body = "Springboot NodeJS SQLServer Aurora"
        dictionary = self.project_module.load_skill_dictionary(
            str(ROOT / "03-8_extract_project_skill_category/00_tool/skill_dictionary.txt")
        )
        required = {"p": {"required_skills": [{"skill": "Spring Boot"}]}}
        legacy = self.project_module.build_extracted_record(
            "p", body, dictionary, required
        )
        overlay, hits = self.project_module.build_registry_overlay_record(
            "p",
            body,
            self.matcher,
            self.required_matcher,
            self.runtime.snapshot,
            required,
        )
        self.assertEqual(
            overlay["skills"],
            ["Spring Boot", "Node.js", "SQL Server", "Amazon Aurora"],
        )
        self.assertEqual(overlay["primary_skills"], ["Spring Boot"])
        self.assertTrue(hits)

        values = ["Springboot", "NodeJS", "SQLServer", "Aurora"]
        normalized = self.skill_gate_module.normalize_registry_owned_skills(
            values, values, values, self.runtime
        )
        canonical = ["Spring Boot", "Node.js", "SQL Server", "Amazon Aurora"]
        self.assertEqual(normalized, (canonical, canonical, canonical))

        legacy_required = self.required_gate_module.judge_match(
            ["Springboot"], [], [], "Spring Boot", ""
        )
        overlay_required = self.required_gate_module.judge_match_with_runtime(
            ["Springboot"], [], [], "Spring Boot", "", self.runtime
        )
        self.assertFalse(legacy_required[0])
        self.assertTrue(overlay_required[0])
        self.assertEqual(overlay_required[1], ["Spring Boot"])

    def test_06_11_matches_three_power_bi_surfaces_and_keeps_substring(self):
        for required, evidence in (
            ("PowerBI", "Power BI"),
            ("Power BI", "Power-BI"),
            ("Power-BI", "PowerBI"),
        ):
            result = self.required_gate_module.judge_match_with_runtime(
                [required], [], [], evidence, "", self.runtime
            )
            self.assertTrue(result[0])
            self.assertEqual(result[1], ["Power BI"])
        substring = self.required_gate_module.judge_match_with_runtime(
            ["BI"], [], [], "PowerBI", "", self.runtime
        )
        self.assertTrue(substring[0])

    def test_legacy_rollback_keeps_legacy_power_bi_surface(self):
        dictionary = self.project_module.load_skill_dictionary(
            str(ROOT / "03-8_extract_project_skill_category/00_tool/skill_dictionary.txt")
        )
        record = self.project_module.build_extracted_record(
            "p", "PowerBI", dictionary, {"p": {"required_skills": [{"skill": "PowerBI"}]}}
        )
        self.assertEqual(record["skills"], ["PowerBI"])
        self.assertEqual(record["primary_skills"], ["PowerBI"])

    def test_power_bi_legacy_ownership_restores_legacy_extraction(self):
        data = yaml.safe_load(self.runtime.snapshot.raw_bytes)
        for entry in data["skills"]:
            if entry["canonical"] == "Power BI":
                entry["ownership"] = "legacy"
                break
        raw = yaml.safe_dump(data, allow_unicode=True, sort_keys=False)
        from common.skill_registry import parse_registry_bytes

        rollback_snapshot = parse_registry_bytes(raw.encode("utf-8"))
        matcher, _ = build_effective_matcher(
            rollback_snapshot,
            ROOT / "03-8_extract_project_skill_category/00_tool/skill_dictionary.txt",
            SKILL_CANONICAL_MAP,
            {"production"},
        )
        skills = [hit.canonical for hit in matcher.find_hits("PowerBI")]
        self.assertEqual(skills, ["PowerBI"])


if __name__ == "__main__":
    unittest.main()

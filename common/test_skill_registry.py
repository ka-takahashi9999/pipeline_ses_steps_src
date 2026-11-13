import copy
import tempfile
import unittest
from pathlib import Path

import yaml

from common.skill_canonical_map import SKILL_ALIAS_MAP, SKILL_CANONICAL_MAP
from common.skill_registry import (
    SkillRegistryError,
    build_effective_matcher,
    build_registry_matcher,
    execution_fingerprint,
    hits_to_skill_output,
    hits_to_skills_raw,
    load_registry,
    load_registry_for_mode,
    parse_registry_bytes,
    shadow_primary_skills,
)
from common.skill_registry_shadow import (
    judge_required_gate_legacy_substring,
    judge_skill_gate,
)


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "common/skill_registry.yaml"
DICTIONARY = ROOT / "03-8_extract_project_skill_category/00_tool/skill_dictionary.txt"


class SkillRegistryValidationTest(unittest.TestCase):
    def setUp(self):
        self.data = yaml.safe_load(REGISTRY.read_text(encoding="utf-8"))

    def parse(self, data):
        raw = yaml.safe_dump(data, allow_unicode=True, sort_keys=False).encode("utf-8")
        return parse_registry_bytes(raw)

    def assert_invalid(self, data, text):
        with self.assertRaisesRegex(SkillRegistryError, text):
            self.parse(data)

    def test_approved_registry_has_exactly_sixty_five_groups(self):
        snapshot = load_registry(REGISTRY)
        self.assertEqual(
            snapshot.canonical_names,
            (
                "Spring Boot", "Node.js", "SQL Server", "Amazon Aurora", "Power BI",
                "Web API", "PL/SQL", "Azure Functions", "React", "AngularJS",
                "Tailwind CSS", "Google Cloud", "Palo Alto", "Fargate",
                "Azure Monitor", "Active Directory", "React Native",
                "Google Analytics", "Redshift", "SOC 2", "Deep Security",
                "As-Is/To-Be", "S/4HANA", "Unreal Engine", "Excel VBA",
                "Access VBA", "Power Apps", "Google Workspace", "テストケース作成",
                "Go", "Ruby on Rails", "VB6", "Express", "Vue.js",
                "Material UI", "Nuxt", "Vue.js 3", "Kubernetes", "RHEL",
                "Windowsバッチ", "KornShell", "vSphere", "ESXi", "BIG-IP",
                "AWS Bedrock", "Adobe XD", "Adobe Photoshop", "Intune",
                "Microsoft 365", "Oracle Database", "UiPath",
                "Automation Anywhere", "Dataverse",
                "Windows Forms", "Firewall", "Machine Learning",
                "Deep Learning", "Agile", "Scrum", "組み込み", "PL/I",
                "Group Policy Object", "IBM License Metric Tool",
                "HPE Serviceguard", "Crystal Reports",
            ),
        )
        self.assertEqual(
            snapshot.production_canonical_names,
            tuple(
                name
                for name in snapshot.canonical_names
                if name not in {"Go", "Scrum", "組み込み"}
            ),
        )
        self.assertEqual(snapshot.shadow_canonical_names, ("Go", "Scrum", "組み込み"))

    def test_invalid_ownership_is_rejected(self):
        data = copy.deepcopy(self.data)
        data["skills"][0]["ownership"] = "unknown"
        self.assert_invalid(data, "invalid ownership")

    def test_duplicate_ownership_key_is_rejected(self):
        raw = REGISTRY.read_text(encoding="utf-8").replace(
            "    ownership: production\n",
            "    ownership: production\n    ownership: shadow\n",
            1,
        )
        with self.assertRaisesRegex(SkillRegistryError, "duplicate YAML key"):
            parse_registry_bytes(raw.encode("utf-8"))

    def test_unknown_ownership_filter_is_rejected(self):
        snapshot = load_registry(REGISTRY)
        with self.assertRaisesRegex(SkillRegistryError, "invalid ownership mode"):
            build_registry_matcher(snapshot, {"unknown"})

    def test_canonical_duplicate_is_rejected(self):
        data = copy.deepcopy(self.data)
        data["skills"].append(copy.deepcopy(data["skills"][0]))
        self.assert_invalid(data, "duplicate canonical")

    def test_alias_duplicate_is_rejected(self):
        data = copy.deepcopy(self.data)
        data["skills"][0]["aliases"].append("Springboot")
        self.assert_invalid(data, "duplicate/canonical alias")

    def test_alias_multiple_ownership_is_rejected(self):
        data = copy.deepcopy(self.data)
        data["skills"][1]["aliases"].append("Springboot")
        self.assert_invalid(data, "multiple canonicals")

    def test_invalid_category_is_rejected(self):
        data = copy.deepcopy(self.data)
        data["skills"][0]["category"] = "不存在"
        self.assert_invalid(data, "invalid category")

    def test_unknown_field_is_rejected(self):
        data = copy.deepcopy(self.data)
        data["skills"][0]["unexpected"] = True
        self.assert_invalid(data, "unknown fields")

    def test_casefold_collision_is_rejected(self):
        data = copy.deepcopy(self.data)
        data["skills"][1]["aliases"].append("SPRINGBOOT")
        self.assert_invalid(data, "multiple canonicals")

    def test_case_sensitive_entry_is_valid_and_matches_exact_case(self):
        data = copy.deepcopy(self.data)
        data["skills"] = [
            {
                "canonical": "CaseExactTool",
                "ownership": "production",
                "category": "その他",
                "aliases": [],
                "match_mode": "boundary",
                "case_sensitive": True,
                "enabled": True,
            }
        ]
        snapshot = self.parse(data)
        matcher = build_registry_matcher(snapshot, {"production"})
        self.assertEqual(
            [hit.canonical for hit in matcher.find_hits("CaseExactTool")],
            ["CaseExactTool"],
        )
        self.assertEqual(matcher.find_hits("caseexacttool"), [])

    def test_case_sensitive_alias_still_rejects_casefold_ownership_collision(self):
        data = copy.deepcopy(self.data)
        data["skills"] = [
            {
                "canonical": "FirstTool",
                "ownership": "production",
                "category": "その他",
                "aliases": ["Collision"],
                "match_mode": "boundary",
                "case_sensitive": True,
                "enabled": True,
            },
            {
                "canonical": "SecondTool",
                "ownership": "shadow",
                "category": "その他",
                "aliases": ["collision"],
                "match_mode": "boundary",
                "case_sensitive": True,
                "enabled": True,
            },
        ]
        self.assert_invalid(data, "multiple canonicals")

    def test_yaml_duplicate_key_is_rejected(self):
        raw = b"schema_version: 2\nschema_version: 2\nallowed_categories: [DB]\nskills: []\n"
        with self.assertRaisesRegex(SkillRegistryError, "duplicate YAML key"):
            parse_registry_bytes(raw)


class SkillRegistryBehaviorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.snapshot = load_registry(REGISTRY)
        cls.matcher, cls.overrides = build_effective_matcher(
            cls.snapshot,
            DICTIONARY,
            SKILL_CANONICAL_MAP,
            {"production", "shadow"},
        )
        cls.registry_matcher = build_registry_matcher(
            cls.snapshot, {"production", "shadow"}
        )

    def test_extraction_surface_canonical_category_dedup_and_span(self):
        text = (
            "Springboot / Spring Boot / NodeJS / SQLServer / Aurora / "
            "PowerBI / Power-BI"
        )
        hits = self.matcher.find_hits(text)
        skills, by_category = hits_to_skill_output(hits)
        self.assertEqual(
            skills,
            ["Spring Boot", "Node.js", "SQL Server", "Amazon Aurora", "Power BI"],
        )
        self.assertEqual(by_category["バックエンド"], ["Spring Boot", "Node.js"])
        self.assertEqual(by_category["DB"], ["SQL Server", "Amazon Aurora"])
        self.assertEqual(by_category["データ分析"], ["Power BI"])
        self.assertEqual([hit.matched_surface for hit in hits], [
            "Springboot", "NodeJS", "SQLServer", "Aurora", "PowerBI"
        ])
        for hit in hits:
            self.assertEqual(text[hit.start:hit.end], hit.matched_surface)

    def test_primary_keeps_canonical_when_alias_surface_matched(self):
        text = "Springboot NodeJS SQLServer Aurora PowerBI"
        body_hits = self.matcher.find_hits(text)
        primary, required_hits = shadow_primary_skills(
            body_hits,
            [text],
            self.registry_matcher,
            ["Power BI"],
            ["Spring Boot", "Node.js", "SQL Server", "Amazon Aurora"],
        )
        self.assertEqual(
            primary,
            ["Spring Boot", "Node.js", "SQL Server", "Amazon Aurora", "Power BI"],
        )
        self.assertEqual(required_hits["Power BI"][0]["matched_surface"], "PowerBI")

    def test_batch1_aliases_canonicalize_and_preserve_raw_surface(self):
        text = (
            "WebAPI PLSQL AzureFunction React.js Angular.js TailwindCSS "
            "GoogleCloud PaloAlto fargate AzureMonitor ActiveDirectory "
            "ReactNative GoogleAnalytics redshift SOC2 DeepSecurity AsIs/ToBe "
            "S4HANA UnrealEngine ExcelVBA AccessVBA PowerApps GoogleWorkspace"
        )
        hits = self.matcher.find_hits(text)
        self.assertEqual(
            [hit.canonical for hit in hits],
            [
                "Web API", "PL/SQL", "Azure Functions", "React", "AngularJS",
                "Tailwind CSS", "Google Cloud", "Palo Alto", "Fargate",
                "Azure Monitor", "Active Directory", "React Native",
                "Google Analytics", "Redshift", "SOC 2", "Deep Security",
                "As-Is/To-Be", "S/4HANA", "Unreal Engine", "Excel VBA",
                "Access VBA", "Power Apps", "Google Workspace",
            ],
        )
        self.assertEqual(
            [hit.matched_surface for hit in hits],
            text.split(),
        )
        raw_text = "prefix React.js ExcelVBA suffix"
        raw = hits_to_skills_raw(raw_text, self.matcher.find_hits(raw_text))
        self.assertIn("React.js", raw)
        self.assertIn("ExcelVBA", raw)

    def test_production_does_not_claim_deferred_groups(self):
        production = {"production"}
        for surface in (
            "Scrum", "スクラム", "組み込み", "組込", "Go", "Go言語",
            "vCenter", "オンプレ", "Entra ID", "NSG", "AVD", "OCI",
        ):
            self.assertIsNone(self.snapshot.resolve_surface(surface, production))

    def test_batch3_aliases_canonicalize_and_preserve_raw_surface(self):
        text = (
            "WinForm ファイアウォール 機械学習 深層学習 アジャイル "
            "PL/1 GPO ILMT ServiceGuard Crystal Report"
        )
        hits = self.matcher.find_hits(text)
        self.assertEqual(
            [hit.canonical for hit in hits],
            [
                "Windows Forms", "Firewall", "Machine Learning",
                "Deep Learning", "Agile", "PL/I", "Group Policy Object",
                "IBM License Metric Tool", "HPE Serviceguard", "Crystal Reports",
            ],
        )
        self.assertEqual(
            [hit.matched_surface for hit in hits],
            [
                "WinForm", "ファイアウォール", "機械学習", "深層学習",
                "アジャイル", "PL/1", "GPO", "ILMT", "ServiceGuard",
                "Crystal Report",
            ],
        )
        raw_text = "SAP Crystal Reports / ServiceGuard / GPO"
        raw = hits_to_skills_raw(raw_text, self.matcher.find_hits(raw_text))
        self.assertIn("SAP Crystal Reports", raw)
        self.assertIn("ServiceGuard", raw)
        self.assertIn("GPO", raw)

    def test_batch3_boundaries_and_parent_skills(self):
        production = {"production"}
        self.assertIsNone(self.snapshot.resolve_surface("Group Policy", production))
        self.assertIsNone(self.snapshot.resolve_surface("NSG", production))
        self.assertIsNone(self.snapshot.resolve_surface("vCenter", production))
        self.assertIsNone(self.snapshot.resolve_surface("Go言語", production))

        hits = self.matcher.find_hits(
            "SAP platform with SAP Crystal Reports; PL/1 and PL/I"
        )
        values = [hit.canonical for hit in hits]
        self.assertIn("SAP", values)
        self.assertIn("Crystal Reports", values)
        self.assertEqual(values.count("PL/I"), 1)

        go_collision = judge_skill_gate(["Go"], ["Go言語"], ["Go"], [])
        self.assertFalse(go_collision.passed)

    def test_batch3_case_insensitive_serviceguard_needs_one_alias(self):
        entry = next(
            entry
            for entry in self.snapshot.entries
            if entry.canonical == "HPE Serviceguard"
        )
        self.assertEqual(entry.aliases, ("ServiceGuard",))
        self.assertEqual(
            [hit.canonical for hit in self.matcher.find_hits("Serviceguard")],
            ["HPE Serviceguard"],
        )

    def test_batch1_longest_match_keeps_parent_child_distinct(self):
        hits = self.matcher.find_hits("React Native and React")
        self.assertEqual(
            [hit.canonical for hit in hits],
            ["React Native", "React"],
        )

    def test_batch2_aliases_canonicalize_and_preserve_raw_surface(self):
        text = (
            "Rails Visual Basic6.0 Express-JS Vue MUI Nuxt.js Vue3.js "
            "k8s Red Hat Enterprise Linux Winバッチ ksh VMware vSphere "
            "VMware ESXi F5 BIG-IP Amazon Bedrock XD Photoshop Microsoft Intune "
            "M365 OracleDB RPA UiPath Autometion Anywhere Microsoft Dataverse"
        )
        hits = self.matcher.find_hits(text)
        self.assertEqual(
            [hit.canonical for hit in hits],
            [
                "Ruby on Rails", "VB6", "Express", "Vue.js",
                "Material UI", "Nuxt", "Vue.js 3", "Kubernetes", "RHEL",
                "Windowsバッチ", "KornShell", "vSphere", "ESXi", "BIG-IP",
                "AWS Bedrock", "Adobe XD", "Adobe Photoshop", "Intune",
                "Microsoft 365", "Oracle Database", "UiPath",
                "Automation Anywhere", "Dataverse",
            ],
        )
        self.assertEqual(
            [hit.matched_surface for hit in hits],
            [
                "Rails", "Visual Basic6.0", "Express-JS", "Vue",
                "MUI", "Nuxt.js", "Vue3.js", "k8s",
                "Red Hat Enterprise Linux", "Winバッチ", "ksh",
                "VMware vSphere", "VMware ESXi", "F5 BIG-IP",
                "Amazon Bedrock", "XD", "Photoshop", "Microsoft Intune",
                "M365", "OracleDB", "RPA UiPath", "Autometion Anywhere",
                "Microsoft Dataverse",
            ],
        )
        raw_text = "prefix VMware vSphere and M365 suffix"
        raw = hits_to_skills_raw(raw_text, self.matcher.find_hits(raw_text))
        self.assertIn("VMware vSphere", raw)
        self.assertIn("M365", raw)

    def test_batch2_vendor_parent_and_product_stay_distinct(self):
        text = (
            "VMware platform with VMware vSphere and VMware ESXi; "
            "SAP platform with SAP S/4HANA; F5 platform with F5 BIG-IP; "
            "AWS platform with Amazon Bedrock; Oracle platform with Oracle Database"
        )
        hits = self.matcher.find_hits(text)
        values = [hit.canonical for hit in hits]
        for parent, product in (
            ("VMware", "vSphere"), ("VMware", "ESXi"),
            ("SAP", "SAP S/4HANA"), ("F5", "BIG-IP"),
            ("AWS", "AWS Bedrock"), ("Oracle", "Oracle Database"),
        ):
            self.assertIn(parent, values)
            self.assertIn(product, values)

    def test_06_8_legacy_groups_are_stable_and_power_bi_improves(self):
        excluded = []
        legacy_surfaces = {
            "Springboot": "Spring Boot",
            "NodeJS": "Node.js",
            "SQLServer": "SQL Server",
            "Aurora": "Amazon Aurora",
        }
        for surface, canonical in legacy_surfaces.items():
            legacy_canonical = SKILL_CANONICAL_MAP.get(surface, surface)
            registry_canonical = self.registry_matcher.find_hits(surface)[0].canonical
            legacy = judge_skill_gate(
                [legacy_canonical], [legacy_canonical], [legacy_canonical], excluded
            )
            shadow = judge_skill_gate(
                [registry_canonical], [registry_canonical], [registry_canonical], excluded
            )
            self.assertEqual(canonical, registry_canonical)
            self.assertEqual(legacy, shadow)
        legacy = judge_skill_gate(["PowerBI"], ["Power BI"], ["PowerBI"], excluded)
        shadow = judge_skill_gate(["Power BI"], ["Power BI"], ["Power BI"], excluded)
        self.assertFalse(legacy.passed)
        self.assertTrue(shadow.passed)

    def test_06_11_alias_and_legacy_substring_semantics(self):
        result = judge_required_gate_legacy_substring(
            ["Power BI", "Spring Boot"],
            [],
            [],
            "PowerBI and Springboot delivery",
            "",
            self.snapshot,
            SKILL_ALIAS_MAP,
        )
        self.assertTrue(result.passed)
        self.assertEqual(result.matched_keywords, ("Power BI", "Spring Boot"))
        substring = judge_required_gate_legacy_substring(
            ["BI"], [], [], "PowerBI", "", self.snapshot, SKILL_ALIAS_MAP
        )
        self.assertTrue(substring.passed)

        batch1 = judge_required_gate_legacy_substring(
            ["React.js", "ActiveDirectory"],
            [],
            [],
            "React and Active Directory delivery",
            "",
            self.snapshot,
            SKILL_ALIAS_MAP,
            self.snapshot.production_canonical_names,
        )
        self.assertTrue(batch1.passed)
        self.assertEqual(batch1.matched_keywords, ("React", "Active Directory"))

        batch2 = judge_required_gate_legacy_substring(
            ["M365", "OracleDB", "VMware vSphere"],
            [],
            [],
            "Microsoft 365, Oracle Database, and vSphere delivery",
            "",
            self.snapshot,
            SKILL_ALIAS_MAP,
            self.snapshot.production_canonical_names,
        )
        self.assertTrue(batch2.passed)
        self.assertEqual(
            batch2.matched_keywords,
            ("Microsoft 365", "Oracle Database", "vSphere"),
        )

        batch3 = judge_required_gate_legacy_substring(
            ["GPO", "ILMT", "ServiceGuard", "Crystal Report"],
            [],
            [],
            (
                "Group Policy Object, IBM License Metric Tool, "
                "HPE Serviceguard, and Crystal Reports"
            ),
            "",
            self.snapshot,
            SKILL_ALIAS_MAP,
            self.snapshot.production_canonical_names,
        )
        self.assertTrue(batch3.passed)
        self.assertEqual(
            batch3.matched_keywords,
            (
                "Group Policy Object", "IBM License Metric Tool",
                "HPE Serviceguard", "Crystal Reports",
            ),
        )

    def test_06_11_m0_preserves_existing_group_keyword_surface(self):
        result = judge_required_gate_legacy_substring(
            ["Springboot", "PowerBI"],
            [],
            [],
            "Spring Boot and Power BI delivery",
            "",
            self.snapshot,
            SKILL_ALIAS_MAP,
            ["Power BI"],
        )
        self.assertTrue(result.passed)
        self.assertEqual(result.matched_keywords, ("Power BI",))

        canonical_key = judge_required_gate_legacy_substring(
            ["Spring Boot"],
            [],
            [],
            "Springboot delivery",
            "",
            self.snapshot,
            SKILL_ALIAS_MAP,
            ["Power BI"],
        )
        self.assertTrue(canonical_key.passed)
        self.assertEqual(canonical_key.matched_keywords, ("Spring Boot",))

    def test_legacy_mode_does_not_load_registry(self):
        missing = Path(tempfile.gettempdir()) / "definitely-missing-skill-registry.yaml"
        snapshot = load_registry_for_mode("legacy", missing)
        self.assertIsNone(snapshot)
        fingerprint = execution_fingerprint(snapshot, "legacy", "legacy_substring", "impl")
        self.assertEqual(len(fingerprint), 64)


if __name__ == "__main__":
    unittest.main()

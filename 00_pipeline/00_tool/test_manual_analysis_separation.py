"""本番runnerと2つの手動系統の分離を確認するfocused test。"""

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TOOL_DIR = ROOT / "00_pipeline" / "00_tool"
ASSISTANCE_DIR = ROOT / "00_pipeline" / "10_assistance_tool"
RUNNERS = (
    TOOL_DIR / "run_full_pipeline.sh",
    TOOL_DIR / "run_full_pipeline_master.sh",
    TOOL_DIR / "run_full_pipeline_phase_b.sh",
)


class ManualAnalysisSeparationTest(unittest.TestCase):
    def test_production_runners_have_no_manual_entrypoint(self):
        forbidden = (
            "run_suggest_and_cleanup",
            "run_pair_quality_analysis",
            "quality_dictionary_analysis",
            "QUALITY_DICTIONARY_ANALYSIS_RUNNER",
            "ENABLE_QUALITY_DICTIONARY_ANALYSIS",
        )
        for runner in RUNNERS:
            text = runner.read_text(encoding="utf-8")
            self.assertFalse([word for word in forbidden if word in text], runner.name)

    def test_production_config_has_no_quality_analysis_flag(self):
        text = (TOOL_DIR / "pipeline_s3_config.env").read_text(encoding="utf-8")
        self.assertNotIn("ENABLE_QUALITY_DICTIONARY_ANALYSIS", text)
        self.assertNotIn("QUALITY_DICTIONARY_ANALYSIS_RUNNER", text)
        self.assertIn('STEP_08_5_EXECUTION_MODE:=legacy', text)

    def test_manual_entrypoints_do_not_call_each_other(self):
        suggest = (ASSISTANCE_DIR / "run_suggest_and_cleanup.sh").read_text(encoding="utf-8")
        pair = (ASSISTANCE_DIR / "run_pair_quality_analysis.sh").read_text(encoding="utf-8")
        self.assertNotIn("pair_quality", suggest)
        self.assertNotIn("quality_dictionary", suggest)
        self.assertNotIn("suggest", pair)
        self.assertNotIn("cleanup", pair)

    def test_pair_tools_have_no_dictionary_candidate_processing(self):
        for name in (
            "run_pair_quality_analysis.sh",
            "analyze_pair_quality.py",
            "confirm_pair_quality_analysis.py",
        ):
            text = (ASSISTANCE_DIR / name).read_text(encoding="utf-8")
            self.assertNotIn("dictionary_improvement", text, name)
            self.assertNotIn("new_skill", text, name)
            self.assertNotIn("synonym_candidate", text, name)
            self.assertNotIn("category_add_candidate", text, name)


if __name__ == "__main__":
    unittest.main()

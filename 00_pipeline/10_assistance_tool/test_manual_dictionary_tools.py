"""手動辞書候補の一括生成・cleanup・atomic公開を確認する。"""

import importlib.util
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml


ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = ROOT / "00_pipeline" / "10_assistance_tool"
RUNNER = SCRIPT_DIR / "run_suggest_and_cleanup.sh"
CASES = (
    (
        "03-8_extract_project_skill_category",
        "skill_dictionary.txt",
        "skill_dictionary_candidates.txt",
        "03-8_skill_dictionary_candidates",
    ),
    (
        "03-9_extract_project_phase_category",
        "phase_dictionary.txt",
        "phase_dictionary_candidates.txt",
        "03-9_phase_dictionary_candidates",
    ),
    (
        "03-10_extract_project_location",
        "location_dictionary.txt",
        "location_dictionary_candidates.txt",
        "03-10_location_dictionary_candidates",
    ),
    (
        "05-8_extract_resource_skill_category",
        "skill_dictionary.txt",
        "skill_dictionary_candidates.txt",
        "05-8_skill_dictionary_candidates",
    ),
    (
        "05-9_extract_resource_phase_category",
        "phase_dictionary.txt",
        "phase_dictionary_candidates.txt",
        "05-9_phase_dictionary_candidates",
    ),
    (
        "05-10_extract_resource_location",
        "location_dictionary.txt",
        "location_dictionary_candidates.txt",
        "05-10_location_dictionary_candidates",
    ),
)

DICTIONARY_PATHS = (
    Path("03-8_extract_project_skill_category/00_tool/skill_dictionary.txt"),
    Path("common/phase_dictionary.txt"),
    Path("03-10_extract_project_location/00_tool/location_dictionary.txt"),
    Path("05-8_extract_resource_skill_category/00_tool/skill_dictionary.txt"),
    Path("05-10_extract_resource_location/00_tool/location_dictionary.txt"),
)


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fake_python(path: Path, fail_token: str = "") -> None:
    path.write_text(
        "#!/usr/bin/env bash\n"
        "set -eu\n"
        + ("if [[ \"${1:-}\" == *\"" + fail_token + "\"* ]]; then exit 17; fi\n" if fail_token else "")
        + "script_name=$(basename \"${1:-}\")\n"
        "if [[ \"$script_name\" == suggest_* ]]; then\n"
        "  printf 'raw:%s:%s\\n' \"${FAKE_CANDIDATE_VERSION:-v1}\" "
        "\"$(basename \"$DICTIONARY_CANDIDATES_FILE\")\" > \"$DICTIONARY_CANDIDATES_FILE\"\n"
        "elif [[ \"$script_name\" == cleanup_* ]]; then\n"
        "  printf 'cleaned:%s:%s\\n' \"${FAKE_CANDIDATE_VERSION:-v1}\" "
        "\"$(basename \"$DICTIONARY_CANDIDATES_FILE\")\" > \"$DICTIONARY_CANDIDATES_FILE\"\n"
        "fi\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _create_runner_root(temp: Path) -> Path:
    pipeline_root = temp / "pipeline_root"
    for step, dictionary_name, candidate_name, history_prefix in CASES:
        candidate_dir = pipeline_root / step / "10_assistance_tool"
        candidate_dir.mkdir(parents=True)
        (candidate_dir / candidate_name).write_text(f"original:{history_prefix}\n", encoding="utf-8")
    for dictionary_path in DICTIONARY_PATHS:
        dictionary = "development:\n  - implementation\n"
        if "skill_dictionary" in dictionary_path.name:
            dictionary = "backend:\n  - Python\n"
        elif "location_dictionary" in dictionary_path.name:
            dictionary = f"location:\n  - {dictionary_path.parts[0]}\n"
        destination = pipeline_root / dictionary_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(dictionary, encoding="utf-8")
    return pipeline_root


def _fixed_candidates(pipeline_root: Path):
    return [pipeline_root / step / "10_assistance_tool" / candidate for step, _, candidate, _ in CASES]


def _snapshot(paths):
    return {path: path.read_bytes() for path in paths}


class ManualDictionaryToolsTest(unittest.TestCase):
    def _environment(self, temp: Path, fake_python: Path, pipeline_root: Path = ROOT):
        environment = dict(os.environ)
        environment.update({
            "PIPELINE_ROOT": str(pipeline_root),
            "PIPELINE_PYTHON_BIN": str(fake_python),
            "DICTIONARY_SUGGEST_OUTPUT_ROOT": str(temp / "suggest_result"),
            "DICTIONARY_SUGGEST_LOG": str(temp / "suggest.log"),
        })
        return environment

    def test_dry_run_executes_nothing_and_creates_no_result(self):
        with tempfile.TemporaryDirectory(prefix="suggest_dry_run_") as temp_name:
            temp = Path(temp_name)
            environment = self._environment(temp, Path("/does/not/exist"))
            completed = subprocess.run(
                ["/usr/bin/bash", str(RUNNER), "--run-date", "20260911", "--dry-run"],
                env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False,
            )
            self.assertEqual(0, completed.returncode, completed.stdout)
            self.assertIn("no candidate, dictionary, pair-analysis, or pipeline command executed", completed.stdout)
            self.assertFalse((temp / "suggest_result").exists())

    def test_success_publishes_six_cleaned_fixed_candidates_and_matching_histories(self):
        with tempfile.TemporaryDirectory(prefix="suggest_success_") as temp_name:
            temp = Path(temp_name)
            pipeline_root = _create_runner_root(temp)
            fake_python = temp / "fake-python"
            _fake_python(fake_python)
            environment = self._environment(temp, fake_python, pipeline_root)
            dictionaries = [pipeline_root / path for path in DICTIONARY_PATHS]
            dictionaries_before = _snapshot(dictionaries)
            completed = subprocess.run(
                ["/usr/bin/bash", str(RUNNER), "--run-date", "20260911"],
                env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False,
            )
            self.assertEqual(0, completed.returncode, completed.stdout)
            for fixed, (_, _, _, history_prefix) in zip(_fixed_candidates(pipeline_root), CASES):
                history = temp / "suggest_result" / f"{history_prefix}_20260911.txt"
                self.assertEqual(fixed.read_bytes(), history.read_bytes())
                self.assertIn("cleaned:v1:", fixed.read_text(encoding="utf-8"))
                self.assertNotIn("raw:", fixed.read_text(encoding="utf-8"))
            self.assertEqual(dictionaries_before, _snapshot(dictionaries))
            self.assertIn("skill dictionary exact-match / phase dictionary SSOT: OK", completed.stdout)
            self.assertIn("dictionary SHA-256 unchanged: OK", completed.stdout)
            self.assertEqual(6, len(list((temp / "suggest_result").glob("*.txt"))))
            self.assertFalse((temp / "suggest_result" / ".tmp").exists())
            self.assertFalse(list(pipeline_root.glob("**/.dictionary_candidate_*")))
            self.assertFalse(list((temp / "suggest_result").glob(".dictionary_candidate_*")))
            self.assertFalse(list((temp / "suggest_result").glob("**/run_manifest.json")))

    def test_partial_failure_changes_neither_fixed_candidates_nor_histories(self):
        with tempfile.TemporaryDirectory(prefix="suggest_failure_") as temp_name:
            temp = Path(temp_name)
            pipeline_root = _create_runner_root(temp)
            output_root = temp / "suggest_result"
            output_root.mkdir()
            for _, _, _, history_prefix in CASES:
                (output_root / f"{history_prefix}_20260911.txt").write_text(
                    f"previous:{history_prefix}\n", encoding="utf-8"
                )
            fixed_before = _snapshot(_fixed_candidates(pipeline_root))
            histories = sorted(output_root.glob("*.txt"))
            histories_before = _snapshot(histories)
            fake_python = temp / "fake-python"
            _fake_python(
                fake_python,
                "05-9_extract_resource_phase_category/10_assistance_tool/cleanup_phase_candidates.py",
            )
            environment = self._environment(temp, fake_python, pipeline_root)
            completed = subprocess.run(
                ["/usr/bin/bash", str(RUNNER), "--run-date", "20260911"],
                env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False,
            )
            self.assertEqual(17, completed.returncode, completed.stdout)
            self.assertEqual(fixed_before, _snapshot(_fixed_candidates(pipeline_root)))
            self.assertEqual(histories_before, _snapshot(histories))
            self.assertEqual(histories, sorted(output_root.glob("*.txt")))
            self.assertFalse((output_root / ".tmp").exists())

    def test_same_day_rerun_overwrites_fixed_candidates_and_one_dated_history(self):
        with tempfile.TemporaryDirectory(prefix="suggest_rerun_") as temp_name:
            temp = Path(temp_name)
            pipeline_root = _create_runner_root(temp)
            fake_python = temp / "fake-python"
            _fake_python(fake_python)
            environment = self._environment(temp, fake_python, pipeline_root)
            command = ["/usr/bin/bash", str(RUNNER), "--run-date", "20260911"]
            first = subprocess.run(command, env=environment, text=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, check=False)
            self.assertEqual(0, first.returncode, first.stdout)
            environment["FAKE_CANDIDATE_VERSION"] = "v2"
            second = subprocess.run(command, env=environment, text=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, check=False)
            self.assertEqual(0, second.returncode, second.stdout)
            for fixed, (_, _, _, history_prefix) in zip(_fixed_candidates(pipeline_root), CASES):
                history_files = list((temp / "suggest_result").glob(f"{history_prefix}_*.txt"))
                self.assertEqual(1, len(history_files))
                self.assertEqual(fixed.read_bytes(), history_files[0].read_bytes())
                self.assertIn("cleaned:v2:", fixed.read_text(encoding="utf-8"))
            self.assertFalse((temp / "suggest_result" / ".tmp").exists())

    def test_history_retains_fourteen_generations_per_series(self):
        with tempfile.TemporaryDirectory(prefix="suggest_retention_") as temp_name:
            temp = Path(temp_name)
            pipeline_root = _create_runner_root(temp)
            output_root = temp / "suggest_result"
            output_root.mkdir()
            for _, _, _, history_prefix in CASES:
                for day in range(1, 15):
                    (output_root / f"{history_prefix}_202608{day:02d}.txt").write_text(
                        f"old:{day}\n", encoding="utf-8"
                    )
            fake_python = temp / "fake-python"
            _fake_python(fake_python)
            completed = subprocess.run(
                ["/usr/bin/bash", str(RUNNER), "--run-date", "20260815"],
                env=self._environment(temp, fake_python, pipeline_root), text=True,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False,
            )
            self.assertEqual(0, completed.returncode, completed.stdout)
            for _, _, _, history_prefix in CASES:
                history_files = sorted(output_root.glob(f"{history_prefix}_*.txt"))
                self.assertEqual(14, len(history_files))
                self.assertFalse((output_root / f"{history_prefix}_20260801.txt").exists())
                self.assertTrue((output_root / f"{history_prefix}_20260815.txt").exists())

    def test_cleanup_preserves_hash_in_quoted_skill_and_recounts(self):
        module = _load(
            ROOT / "03-8_extract_project_skill_category/10_assistance_tool/cleanup_skill_candidates.py",
            "cleanup_skill_candidates_focused",
        )
        self.assertEqual("C#", module.extract_skill_name('  - "C#"  # registered\n'))
        with tempfile.TemporaryDirectory(prefix="cleanup_candidates_") as temp_name:
            candidate = Path(temp_name) / "candidates.txt"
            candidate.write_text("# 件数: 99件\nバックエンド:\n  - \"C#\"\n  - FocusedNewSkill\n", encoding="utf-8")
            with mock.patch.object(module, "CANDIDATES_FILE", candidate), mock.patch.object(sys, "argv", ["cleanup"]):
                module.main()
            output = candidate.read_text(encoding="utf-8")
            self.assertIn("# 件数: 1件", output)
            self.assertIn("FocusedNewSkill", output)
            self.assertNotIn('  - "C#"', output)

    def test_location_zero_input_writes_current_zero_result(self):
        module = _load(
            ROOT / "03-10_extract_project_location/10_assistance_tool/suggest_location_dictionary.py",
            "suggest_location_dictionary_focused",
        )
        with tempfile.TemporaryDirectory(prefix="location_zero_") as temp_name:
            temp = Path(temp_name)
            projects = temp / "projects.jsonl"
            cleaned = temp / "cleaned.jsonl"
            output = temp / "candidates.txt"
            projects.write_text("", encoding="utf-8")
            cleaned.write_text("", encoding="utf-8")
            module.INPUT_PROJECTS = str(projects)
            module.INPUT_CLEANED = str(cleaned)
            module.OUTPUT_CANDIDATES = str(output)
            module.main()
            self.assertIn("今回実行結果: ラベルヒット0件 / 候補数: 0件", output.read_text(encoding="utf-8"))

    def test_prompts_match_current_dictionary_categories_and_llm_failure_is_not_fallback(self):
        cases = (
            ("03-8_extract_project_skill_category", "suggest_skill_dictionary.py",
             Path("03-8_extract_project_skill_category/00_tool/skill_dictionary.txt")),
            ("05-8_extract_resource_skill_category", "suggest_skill_dictionary.py",
             Path("05-8_extract_resource_skill_category/00_tool/skill_dictionary.txt")),
            ("03-9_extract_project_phase_category", "suggest_phase_dictionary.py",
             Path("common/phase_dictionary.txt")),
            ("05-9_extract_resource_phase_category", "suggest_phase_dictionary.py",
             Path("common/phase_dictionary.txt")),
            ("03-10_extract_project_location", "suggest_location_dictionary.py",
             Path("03-10_extract_project_location/00_tool/location_dictionary.txt")),
            ("05-10_extract_resource_location", "suggest_location_dictionary.py",
             Path("05-10_extract_resource_location/00_tool/location_dictionary.txt")),
        )
        for step, script_name, dictionary_path in cases:
            source = (ROOT / step / "10_assistance_tool" / script_name).read_text(encoding="utf-8")
            categories = (yaml.safe_load((ROOT / dictionary_path).read_text(encoding="utf-8")) or {}).keys()
            self.assertFalse([category for category in categories if category not in source], step)
            self.assertIn("from common.llm_client import call_llm", source)
            self.assertNotIn("call_llm_with_fallback", source)


if __name__ == "__main__":
    unittest.main()

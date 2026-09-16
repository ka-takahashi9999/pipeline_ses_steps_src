"""Focused dry-run tests for canonical 08-5 mode propagation."""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
TOOL_DIR = ROOT / "00_pipeline" / "00_tool"
MODE_ENV = "STEP_08_5_EXECUTION_MODE"
LEGACY_FLAG_ENV = "ENABLE_08_5_BATCH_ORCHESTRATION"
CONFIG_FILE = TOOL_DIR / "pipeline_s3_config.env"
LAUNCHER = TOOL_DIR / "launch_full_pipeline_async.sh"
MANAGED = TOOL_DIR / "run_full_pipeline_managed.sh"
RUNNERS = (
    TOOL_DIR / "run_full_pipeline.sh",
    TOOL_DIR / "run_full_pipeline_master.sh",
)
ENTRYPOINT_PATH = (
    ROOT
    / "08-5_high_score_required_skill_recheck"
    / "00_tool"
    / "run_high_score_required_skill_recheck.py"
)
MODE_CONFIG = ENTRYPOINT_PATH.parent / "config.py"

sys.path.insert(0, str(ENTRYPOINT_PATH.parent))
import run_high_score_required_skill_recheck as entrypoint  # noqa: E402


def _write_executable(path, text):
    path.write_text(text, encoding="utf-8")
    path.chmod(0o755)


def _base_environment():
    environment = dict(os.environ)
    for name in (
        MODE_ENV,
        LEGACY_FLAG_ENV,
        "PIPELINE_S3_CONFIG_FILE",
        "PIPELINE_CURRENT_STEP_FILE",
        "PIPELINE_STATUS_WRITER",
        "PIPELINE_SUSPEND_CONTRACT_FILE",
        "PIPELINE_SUSPEND_EXIT_CODE",
    ):
        environment.pop(name, None)
    return environment


def _launcher_probe(temp, supplied_mode=None, config_file=CONFIG_FILE):
    trace = temp / "systemd_args.txt"
    systemctl = temp / "systemctl"
    systemd_run = temp / "systemd-run"
    _write_executable(
        systemctl,
        "#!/usr/bin/env bash\n"
        "if [[ \"${1:-}\" == \"is-active\" ]]; then exit 1; fi\n"
        "exit 0\n",
    )
    _write_executable(
        systemd_run,
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$@\" > \"$TRACE\"\n"
        "exit 0\n",
    )
    environment = _base_environment()
    environment.update(
        {
            "RUN_ID": "mode-propagation-launcher",
            "RUN_DATE": "20260911",
            "PIPELINE_S3_CONFIG_FILE": str(config_file),
            "PIPELINE_SYSTEMD_RUN_BIN": str(systemd_run),
            "PIPELINE_SYSTEMCTL_BIN": str(systemctl),
            "TRACE": str(trace),
        }
    )
    if supplied_mode is not None:
        environment[MODE_ENV] = supplied_mode
    completed = subprocess.run(
        ["/usr/bin/bash", str(LAUNCHER)],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=30,
        check=False,
    )
    arguments = trace.read_text(encoding="utf-8").splitlines() if trace.exists() else []
    systemd_environment = {
        argument[len("--setenv=") :].split("=", 1)[0]: argument[len("--setenv=") :].split("=", 1)[1]
        for argument in arguments
        if argument.startswith("--setenv=") and "=" in argument[len("--setenv=") :]
    }
    return completed, systemd_environment


def _managed_probe(temp, mode, config_file=CONFIG_FILE):
    trace = temp / f"managed_{mode}.txt"
    child = temp / f"managed_child_{mode}.sh"
    writer = temp / f"status_writer_{mode}.py"
    fake_aws = temp / f"aws_{mode}"
    _write_executable(
        child,
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"mode=$STEP_08_5_EXECUTION_MODE\" "
        "\"legacy_flag=${ENABLE_08_5_BATCH_ORCHESTRATION+set}\" > \"$TRACE\"\n"
        "exit 0\n",
    )
    writer.write_text("raise SystemExit(0)\n", encoding="utf-8")
    _write_executable(fake_aws, "#!/usr/bin/env bash\nexit 0\n")

    run_id = f"mode-propagation-managed-{mode}"
    managed_state = ROOT / "00_pipeline" / "01_result" / "managed" / "19700101" / run_id
    environment = _base_environment()
    environment.update(
        {
            "RUN_ID": run_id,
            "RUN_DATE": "19700101",
            MODE_ENV: mode,
            "PIPELINE_S3_CONFIG_FILE": str(config_file),
            "PIPELINE_SCRIPT": str(child),
            "PIPELINE_STATUS_WRITER": str(writer),
            "PIPELINE_PYTHON_BIN": sys.executable,
            "PIPELINE_AWS_BIN": str(fake_aws),
            "PIPELINE_LOCK_FILE": str(temp / f"managed_{mode}.lock"),
            "TRACE": str(trace),
        }
    )
    try:
        completed = subprocess.run(
            ["/usr/bin/bash", str(MANAGED)],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=30,
            check=False,
        )
        observed = trace.read_text(encoding="utf-8").splitlines() if trace.exists() else []
    finally:
        if managed_state.exists():
            shutil.rmtree(managed_state)
    return completed, observed


def _runner_probe(temp, runner, mode):
    trace = temp / f"{runner.stem}_{mode}.txt"
    fake_python = temp / "python3"
    _write_executable(
        fake_python,
        "#!/usr/bin/env bash\n"
        f"if [[ \"${{1:-}}\" == \"{MODE_CONFIG}\" ]]; then exec /usr/bin/python3 \"$@\"; fi\n"
        f"if [[ \"${{1:-}}\" == \"{ENTRYPOINT_PATH}\" ]]; then "
        "printf '%s\\n' \"mode=$STEP_08_5_EXECUTION_MODE\" "
        "\"legacy_flag=${ENABLE_08_5_BATCH_ORCHESTRATION+set}\" > \"$TRACE\"; fi\n"
        "exit 0\n",
    )
    environment = _base_environment()
    environment.update(
        {
            "RUN_ID": f"mode-propagation-{runner.stem}",
            "RUN_DATE": "20260911",
            MODE_ENV: mode,
            "PIPELINE_LOG": str(temp / f"{runner.stem}_{mode}.log"),
            "PATH": str(temp) + os.pathsep + environment.get("PATH", ""),
            "TRACE": str(trace),
        }
    )
    completed = subprocess.run(
        ["/usr/bin/bash", str(runner)],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=30,
        check=False,
    )
    observed = trace.read_text(encoding="utf-8").splitlines() if trace.exists() else []
    return completed, observed


def _entrypoint_probe(mode):
    environment = {MODE_ENV: mode}
    with patch.dict(os.environ, environment, clear=True), patch.object(
        entrypoint, "run_phase_a", return_value=0
    ) as phase_a, patch.object(
        sys,
        "argv",
        [
            "run_high_score_required_skill_recheck.py",
            "--pipeline-run-id",
            "mode-propagation-entrypoint",
            "--run-date",
            "20260911",
        ],
    ):
        exit_code = entrypoint.main()
    return exit_code, phase_a.call_args


class ExecutionModePropagationTest(unittest.TestCase):
    def _assert_mode_across_path(self, supplied_mode, expected_mode):
        with tempfile.TemporaryDirectory(prefix="mode_propagation_") as temp_name:
            temp = Path(temp_name)
            launcher, systemd_environment = _launcher_probe(temp, supplied_mode)
            self.assertEqual(0, launcher.returncode, launcher.stdout)
            self.assertIn(f"mode={expected_mode}", launcher.stdout)
            self.assertEqual(expected_mode, systemd_environment.get(MODE_ENV))
            self.assertNotIn(LEGACY_FLAG_ENV, systemd_environment)

            managed, managed_observed = _managed_probe(temp, expected_mode)
            self.assertEqual(0, managed.returncode, managed.stdout)
            self.assertIn(f"mode={expected_mode}", managed.stdout)
            self.assertEqual(
                [f"mode={expected_mode}", "legacy_flag="], managed_observed
            )

            for runner in RUNNERS:
                completed, observed = _runner_probe(temp, runner, expected_mode)
                self.assertEqual(0, completed.returncode, completed.stdout)
                self.assertIn(
                    f"STEP_08_5_EXECUTION_MODE={expected_mode}", completed.stdout
                )
                self.assertEqual([f"mode={expected_mode}", "legacy_flag="], observed)

        exit_code, call_args = _entrypoint_probe(expected_mode)
        self.assertEqual(0, exit_code)
        self.assertEqual(expected_mode, call_args.args[2])

    def test_legacy_from_ec2_config_propagates_across_canonical_path(self):
        self._assert_mode_across_path(None, "legacy")

    def test_explicit_batch_preserves_existing_route(self):
        self._assert_mode_across_path("batch", "batch")

    def test_launcher_fails_closed_when_canonical_config_omits_mode(self):
        with tempfile.TemporaryDirectory(prefix="mode_missing_launcher_") as temp_name:
            temp = Path(temp_name)
            config = temp / "config.env"
            config.write_text(
                'PIPELINE_S3_BUCKET="technoverse"\n'
                'PIPELINE_S3_BASE_PREFIX="pipeline_ses_steps"\n'
                'PIPELINE_STATUS_PREFIX="pipeline-status"\n'
                'PIPELINE_LOG_PREFIX="pipeline-logs"\n'
                'PIPELINE_AWS_REGION="ap-northeast-1"\n'
                'PIPELINE_SYSTEMD_USER=""\n',
                encoding="utf-8",
            )
            completed, systemd_environment = _launcher_probe(
                temp, supplied_mode=None, config_file=config
            )
        self.assertEqual(2, completed.returncode)
        self.assertIn("must be set by pipeline S3 config", completed.stdout)
        self.assertEqual({}, systemd_environment)

    def test_managed_fails_closed_before_child_when_mode_is_missing(self):
        with tempfile.TemporaryDirectory(prefix="mode_missing_managed_") as temp_name:
            temp = Path(temp_name)
            config = temp / "config.env"
            config.write_text(
                'PIPELINE_S3_BUCKET="technoverse"\n'
                'PIPELINE_S3_BASE_PREFIX="pipeline_ses_steps"\n'
                'PIPELINE_STATUS_PREFIX="pipeline-status"\n'
                'PIPELINE_LOG_PREFIX="pipeline-logs"\n'
                'PIPELINE_AWS_REGION="ap-northeast-1"\n'
                'PIPELINE_SYSTEMD_USER=""\n',
                encoding="utf-8",
            )
            completed, observed = _managed_probe(temp, "", config_file=config)
        self.assertEqual(2, completed.returncode)
        self.assertIn("must be set by pipeline S3 config or launcher", completed.stdout)
        self.assertEqual([], observed)


if __name__ == "__main__":
    unittest.main(verbosity=2)

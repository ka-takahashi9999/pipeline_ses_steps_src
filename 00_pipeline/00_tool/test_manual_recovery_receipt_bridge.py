"""Focused stub-only tests ensuring the retired receipt bridge stays removed."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TOOL_DIR = ROOT / "00_pipeline" / "00_tool"
RUNNER = TOOL_DIR / "run_full_pipeline.sh"
MODE_CONFIG = ROOT / "08-5_high_score_required_skill_recheck" / "00_tool" / "config.py"
ROTATION_TOOL = str(
    ROOT
    / "80-75_portal_s3_backup_rotation"
    / "00_tool"
    / "portal_s3_backup_rotation.py"
)
RECHECK_TOOL = str(
    ROOT
    / "08-5_high_score_required_skill_recheck"
    / "00_tool"
    / "run_high_score_required_skill_recheck.py"
)
BRIDGE_RUN_DATE = "20260915"


def _write_fake_python(path: Path) -> None:
    path.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"if [[ \"${{1:-}}\" == \"{MODE_CONFIG}\" ]]; then\n"
        "  printf '%s\\n' legacy\n"
        "  exit 0\n"
        "fi\n"
        "{\n"
        "  printf '%s\\t%s' \"${RUN_ID:-}\" \"${RUN_DATE:-}\"\n"
        "  printf '\\t%s' \"$@\"\n"
        "  printf '\\n'\n"
        "} >> \"$TRACE\"\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _runner_probe(temp: Path, run_date: str):
    trace = temp / f"trace_{run_date}.tsv"
    fake_python = temp / "python3"
    _write_fake_python(fake_python)
    environment = dict(os.environ)
    for name in (
        "PIPELINE_CURRENT_STEP_FILE",
        "PIPELINE_STATUS_WRITER",
        "PIPELINE_SUSPEND_CONTRACT_FILE",
        "PIPELINE_SUSPEND_EXIT_CODE",
    ):
        environment.pop(name, None)
    environment.update(
        {
            "RUN_ID": "managed-identity-probe",
            "RUN_DATE": run_date,
            "PIPELINE_LOG": str(temp / f"runner_{run_date}.log"),
            "PATH": str(temp) + os.pathsep + environment.get("PATH", ""),
            "TRACE": str(trace),
        }
    )
    completed = subprocess.run(
        ["/usr/bin/bash", str(RUNNER)],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=30,
        check=False,
    )
    calls = []
    if trace.exists():
        for line in trace.read_text(encoding="utf-8").splitlines():
            run_id, observed_date, *arguments = line.split("\t")
            calls.append(
                {
                    "run_id": run_id,
                    "run_date": observed_date,
                    "arguments": arguments,
                }
            )
    return completed, calls


def _find_call(calls, tool):
    matches = [call for call in calls if call["arguments"] and call["arguments"][0] == tool]
    if len(matches) != 1:
        raise AssertionError(f"expected one call for {tool}, got {len(matches)}")
    return matches[0]


class ManualRecoveryReceiptBridgeTest(unittest.TestCase):
    def test_runner_has_no_one_time_bridge(self):
        source = RUNNER.read_text(encoding="utf-8")
        for marker in (
            "20260915", "MANUAL_RECOVERY_BRIDGE", "manual-20260914-tail-20260915-01",
            "--manual-recovery-receipt",
        ):
            self.assertNotIn(marker, source)

    def test_managed_rotation_never_adds_receipt(self):
        with tempfile.TemporaryDirectory(prefix="manual_receipt_bridge_") as temp_name:
            temp = Path(temp_name)
            bridge_run, bridge_calls = _runner_probe(temp, BRIDGE_RUN_DATE)
            other_run, other_calls = _runner_probe(temp, "20260916")

        self.assertEqual(0, bridge_run.returncode, bridge_run.stdout)
        self.assertEqual(0, other_run.returncode, other_run.stdout)

        bridge_rotation = _find_call(bridge_calls, ROTATION_TOOL)
        other_rotation = _find_call(other_calls, ROTATION_TOOL)
        self.assertEqual([ROTATION_TOOL], bridge_rotation["arguments"])
        self.assertEqual([ROTATION_TOOL], other_rotation["arguments"])

        self.assertEqual(
            [call["arguments"][0] for call in bridge_calls],
            [call["arguments"][0] for call in other_calls],
        )
        self.assertTrue(
            all(
                not argument.startswith("--manual-recovery-receipt")
                for call in bridge_calls + other_calls
                for argument in call["arguments"]
            )
        )

    def test_managed_identity_is_preserved(self):
        with tempfile.TemporaryDirectory(prefix="manual_receipt_identity_") as temp_name:
            completed, calls = _runner_probe(Path(temp_name), BRIDGE_RUN_DATE)

        self.assertEqual(0, completed.returncode, completed.stdout)
        rotation = _find_call(calls, ROTATION_TOOL)
        self.assertEqual("managed-identity-probe", rotation["run_id"])
        self.assertEqual(BRIDGE_RUN_DATE, rotation["run_date"])

        recheck = _find_call(calls, RECHECK_TOOL)
        self.assertIn("--pipeline-run-id", recheck["arguments"])
        run_id_index = recheck["arguments"].index("--pipeline-run-id")
        self.assertEqual("managed-identity-probe", recheck["arguments"][run_id_index + 1])
        run_date_index = recheck["arguments"].index("--run-date")
        self.assertEqual(BRIDGE_RUN_DATE, recheck["arguments"][run_date_index + 1])


if __name__ == "__main__":
    unittest.main()

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from common.skill_registry import SkillRegistryError
from common.skill_registry_runtime import (
    create_runtime_context,
    load_runtime_context,
    propagate_step_fingerprint,
    require_step_fingerprints,
    write_step_fingerprint,
)


ROOT = Path(__file__).resolve().parents[1]


class SkillRegistryRuntimeTest(unittest.TestCase):
    def test_overlay_context_is_immutable_and_reusable(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(
            os.environ, {}, clear=False
        ):
            output_root = Path(directory)
            path = create_runtime_context(
                ROOT, output_root, "20261104", "runtime-overlay", mode="registry_overlay"
            )
            runtime = load_runtime_context(path, ROOT, expected_mode="registry_overlay")
            self.assertTrue(runtime.overlay_enabled)
            self.assertEqual(
                len(runtime.snapshot.production_canonical_names),
                62,
            )
            self.assertEqual(
                runtime.snapshot.shadow_canonical_names,
                ("Go", "Scrum", "組み込み"),
            )
            self.assertIn("Power BI", runtime.snapshot.production_canonical_names)
            self.assertIn("Google Workspace", runtime.snapshot.production_canonical_names)
            self.assertIn("Amazon Aurora", runtime.snapshot.production_canonical_names)
            self.assertIn("Microsoft 365", runtime.snapshot.production_canonical_names)
            self.assertEqual(
                path,
                create_runtime_context(
                    ROOT,
                    output_root,
                    "20261104",
                    "runtime-overlay",
                    mode="registry_overlay",
                ),
            )

    def test_snapshot_tamper_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = create_runtime_context(
                ROOT, Path(directory), "20261104", "runtime-tamper", mode="registry_overlay"
            )
            snapshot = path.parent / "registry_snapshot.yaml"
            snapshot.write_text(snapshot.read_text(encoding="utf-8") + "\n", encoding="utf-8")
            with self.assertRaisesRegex(SkillRegistryError, "snapshot hash mismatch"):
                load_runtime_context(path, ROOT, expected_mode="registry_overlay")

    def test_context_ownership_tamper_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = create_runtime_context(
                ROOT, Path(directory), "20261104", "runtime-ownership", mode="registry_overlay"
            )
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["registry_ownership"]["Power BI"] = "shadow"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(SkillRegistryError, "ownership mismatch"):
                load_runtime_context(path, ROOT, expected_mode="registry_overlay")

    def test_legacy_context_does_not_load_registry(self):
        with tempfile.TemporaryDirectory() as directory:
            path = create_runtime_context(
                ROOT, Path(directory), "20261104", "runtime-legacy", mode="legacy"
            )
            runtime = load_runtime_context(path, ROOT, expected_mode="legacy")
            self.assertFalse(runtime.overlay_enabled)
            self.assertIsNone(runtime.snapshot)
            self.assertFalse((path.parent / "registry_snapshot.yaml").exists())

    def test_sidecar_mismatch_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            temp_root = Path(directory)
            context_path = create_runtime_context(
                ROOT,
                temp_root / "contexts",
                "20261104",
                "runtime-sidecar",
                mode="registry_overlay",
            )
            runtime = load_runtime_context(
                context_path, ROOT, expected_mode="registry_overlay"
            )
            step_dir = temp_root / "step-a"
            write_step_fingerprint(step_dir, "step-a", runtime)
            require_step_fingerprints(temp_root, ("step-a",), runtime)
            sidecar = step_dir / "02_confirm/skill_registry_fingerprint.json"
            payload = json.loads(sidecar.read_text(encoding="utf-8"))
            payload["registry_fingerprint"] = "wrong"
            sidecar.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(SkillRegistryError, "sidecar mismatch"):
                require_step_fingerprints(temp_root, ("step-a",), runtime)

    def test_sidecar_chain_propagates_only_after_direct_producer_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            temp_root = Path(directory)
            context_path = create_runtime_context(
                ROOT,
                temp_root / "contexts",
                "20261104",
                "runtime-chain",
                mode="registry_overlay",
            )
            runtime = load_runtime_context(
                context_path, ROOT, expected_mode="registry_overlay"
            )
            write_step_fingerprint(temp_root / "06-8", "06-8", runtime)
            propagate_step_fingerprint(
                temp_root, "06-8", temp_root / "06-9", "06-9", runtime
            )
            propagate_step_fingerprint(
                temp_root, "06-9", temp_root / "06-10", "06-10", runtime
            )
            require_step_fingerprints(temp_root, ("06-10",), runtime)

            sidecar = temp_root / "06-9/02_confirm/skill_registry_fingerprint.json"
            payload = json.loads(sidecar.read_text(encoding="utf-8"))
            payload["run_id"] = "stale"
            sidecar.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(SkillRegistryError, "sidecar mismatch"):
                propagate_step_fingerprint(
                    temp_root, "06-9", temp_root / "06-10b", "06-10b", runtime
                )
            self.assertFalse(
                (temp_root / "06-10b/02_confirm/skill_registry_fingerprint.json").exists()
            )


if __name__ == "__main__":
    unittest.main()

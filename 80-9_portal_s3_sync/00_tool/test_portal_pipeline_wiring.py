"""
80-7 / 80-8 / 80-9 のPipeline組込み確認（focused test）

確認内容:
① run_full_pipeline.sh / run_full_pipeline_master.sh が完全一致していること
② assistant処理（run_suggest_and_cleanup）の後に 80-7 → 80-8 → 80-9 の順で並ぶこと
③ 80-7 / 80-8 / 80-9 が run_step 経由（失敗時にPipelineをexitさせる経路）であること
④ 09-1〜09-5本体 / 06-80 / 07-1 / 08-1 / Success Cache関連に差分がないこと
   （workspace と _src のcheckoutをsha256比較する）
⑤ 設定が pipeline_s3_config.env に統合され、bucketを二重管理していないこと

full Pipeline実行・AWSアクセスは行わない。

実行:
  python3 80-9_portal_s3_sync/00_tool/test_portal_pipeline_wiring.py
"""

import hashlib
import re
import sys
import unittest
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.pipeline_s3_env import load_pipeline_s3_config  # noqa: E402

SRC_ROOT = project_root
GIT_ROOT = Path("/home/ec2-user/pipeline_ses_steps_src")

RUNNER = SRC_ROOT / "00_pipeline/00_tool/run_full_pipeline.sh"
RUNNER_MASTER = SRC_ROOT / "00_pipeline/00_tool/run_full_pipeline_master.sh"
CONFIG_ENV = SRC_ROOT / "00_pipeline/00_tool/pipeline_s3_config.env"

NEW_STEPS = (
    "80-7_manage_09_result_retention",
    "80-8_portal_s3_prepare",
    "80-9_portal_s3_sync",
)

# 今回変更してはいけない領域
FROZEN_GLOBS = (
    "common/success_cache.py",
    "06-80_duplicate_proposal_check/00_tool/*.py",
    "07-1_requirement_skill_ai_matching/00_tool/*.py",
    "07-1_requirement_skill_ai_matching/00_tool/normalized/*.py",
    "08-1_restore_and_merge_requirement_skill_ai_matching/00_tool/*.py",
    "09-1_mail_display_format/00_tool/*.py",
    "09-2_extract_high_score_mail_display/00_tool/*.py",
    "09-3_prepare_sales_proposal_input/00_tool/*.py",
    "09-3_prepare_sales_mail_context/00_tool/*.py",
    "09-4_remove_category_mismatch_sales_candidates/00_tool/*.py",
    "09-5_generate_sales_reply_draft/00_tool/*.py",
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class TestRunnerWiring(unittest.TestCase):
    def setUp(self):
        self.text = RUNNER.read_text(encoding="utf-8")

    def test_runners_are_identical(self):
        self.assertEqual(sha256(RUNNER), sha256(RUNNER_MASTER))

    def test_new_steps_run_after_assistant_in_order(self):
        markers = [
            "run_suggest_and_cleanup.sh",
            "80-7_manage_09_result_retention/00_tool/manage_09_result_retention.py",
            "80-8_portal_s3_prepare/00_tool/portal_s3_prepare.py",
            "80-9_portal_s3_sync/00_tool/portal_s3_sync.py",
        ]
        positions = []
        for marker in markers:
            index = self.text.find(marker)
            self.assertNotEqual(index, -1, msg=f"runnerに {marker} がありません")
            positions.append(index)
        self.assertEqual(positions, sorted(positions), msg=f"順序が不正です: {positions}")

    def test_new_steps_use_run_step(self):
        for step in NEW_STEPS:
            pattern = re.compile(rf'^run_step "{re.escape(step)}.*\$ROOT/{re.escape(step)}/00_tool/', re.M)
            self.assertRegex(self.text, pattern, msg=step)

    def test_80_7_is_invoked_with_apply(self):
        line = [l for l in self.text.splitlines() if "manage_09_result_retention.py" in l]
        self.assertEqual(len(line), 1)
        self.assertIn("--apply", line[0])
        self.assertIn('--run-date "$RUN_DATE"', line[0])

    def test_pipeline_end_marker_is_after_new_steps(self):
        self.assertGreater(
            self.text.find("pipeline end"),
            self.text.find("portal_s3_sync.py"),
        )

    def test_unrelated_runners_are_not_referenced(self):
        for name in ("run_full_pipeline_managed.sh", "launch_full_pipeline_async.sh"):
            self.assertNotIn(name, self.text)


class TestConfig(unittest.TestCase):
    def test_portal_settings_are_in_pipeline_s3_config(self):
        text = CONFIG_ENV.read_text(encoding="utf-8")
        self.assertIn("PORTAL_S3_PREFIX", text)
        self.assertIn("PORTAL_S3_VERIFY_WAIT_SEC", text)

    def test_bucket_is_not_duplicated(self):
        text = CONFIG_ENV.read_text(encoding="utf-8")
        self.assertEqual(text.count('PIPELINE_S3_BUCKET:='), 1)
        # Portal prefix は既存のbase prefixを再利用する
        self.assertIn("${PIPELINE_S3_BASE_PREFIX}/pipeline_ses_steps", text)

    def test_resolved_values(self):
        config = load_pipeline_s3_config()
        self.assertEqual(config["PIPELINE_S3_BUCKET"], "technoverse")
        self.assertEqual(config["PORTAL_S3_PREFIX"], "pipeline_ses_steps/pipeline_ses_steps")
        self.assertEqual(config["PORTAL_S3_VERIFY_WAIT_SEC"], "30")


class TestFrozenAreasHaveNoDiff(unittest.TestCase):
    def test_success_cache_and_09_bodies_are_unchanged(self):
        if not (GIT_ROOT / ".git").is_dir():
            self.skipTest(f"_src が存在しないため比較をスキップ: {GIT_ROOT}")

        checked = 0
        diffs = []
        for pattern in FROZEN_GLOBS:
            for path in sorted(SRC_ROOT.glob(pattern)):
                relative = path.relative_to(SRC_ROOT)
                counterpart = GIT_ROOT / relative
                if not counterpart.is_file():
                    diffs.append(f"_srcに存在しない: {relative}")
                    continue
                checked += 1
                if sha256(path) != sha256(counterpart):
                    diffs.append(f"差分あり: {relative}")
        self.assertGreater(checked, 0, msg="比較対象が0件です")
        self.assertEqual(diffs, [], msg=f"凍結領域に差分があります: {diffs[:3]}")


if __name__ == "__main__":
    unittest.main(verbosity=2)

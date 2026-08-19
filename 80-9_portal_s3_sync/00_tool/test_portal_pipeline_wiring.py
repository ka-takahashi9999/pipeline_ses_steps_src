"""
80-7 / 80-8 / 80-9 のPipeline組込み確認（focused test）

確認内容:
① run_full_pipeline.sh / run_full_pipeline_master.sh が完全一致していること
② assistant処理（run_suggest_and_cleanup）の後に 80-7 → 80-8 → 80-9 の順で並ぶこと
③ 80-7 / 80-8 / 80-9 が run_step 経由（失敗時にPipelineをexitさせる経路）であること
④ 80-7のproduction code path・local retention対象・summary contractを直接検証すること
⑤ 設定が pipeline_s3_config.env に統合され、bucketを二重管理していないこと

production contract regression（(33)-(38)）:
⑥ 80-7 のlocal retentionが未変更であること
⑦ CURRENT destination / pipeline-status / pipeline-logs prefixが未変更であること
⑧ private mail master uploader が production wiring 上でまだ有効であること
⑨ 09-2 root ZIP writer が外部配布用の正式contractとしてcutover後も有効であること
⑩ active runner が 80-75 rotation をまだ呼び出していないこと（cutover未実施）

full Pipeline実行・AWSアクセスは行わない。

実行:
  python3 80-9_portal_s3_sync/00_tool/test_portal_pipeline_wiring.py
"""

import ast
import hashlib
import re
import sys
import unittest
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.pipeline_s3_env import load_pipeline_s3_config  # noqa: E402

SRC_ROOT = project_root

RUNNER = SRC_ROOT / "00_pipeline/00_tool/run_full_pipeline.sh"
RUNNER_MASTER = SRC_ROOT / "00_pipeline/00_tool/run_full_pipeline_master.sh"
CONFIG_ENV = SRC_ROOT / "00_pipeline/00_tool/pipeline_s3_config.env"
RETENTION_SCRIPT = (
    SRC_ROOT / "80-7_manage_09_result_retention/00_tool/manage_09_result_retention.py"
)

NEW_STEPS = (
    "80-7_manage_09_result_retention",
    "80-8_portal_s3_prepare",
    "80-9_portal_s3_sync",
)

ROTATION_STEP = "80-75_portal_s3_backup_rotation"
ROTATION_SCRIPT = f"{ROTATION_STEP}/00_tool/portal_s3_backup_rotation.py"
PRIVATE_UPLOADER = "01-1_fetch_gmail/00_tool/upload_mail_master_private_s3.py"
ZIP_WRITER = SRC_ROOT / "09-2_extract_high_score_mail_display/00_tool/extract_high_score_mail_display.py"

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
        self.assertIn("PORTAL_S3_BACKUP_PREFIX", text)

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

    # ---- (34) CURRENT destination unchanged -----------------------------
    def test_34_current_destination_is_unchanged(self):
        config = load_pipeline_s3_config()
        self.assertEqual(
            f"s3://{config['PIPELINE_S3_BUCKET']}/{config['PORTAL_S3_PREFIX']}/",
            "s3://technoverse/pipeline_ses_steps/pipeline_ses_steps/",
        )
        self.assertEqual(
            f"s3://{config['PIPELINE_S3_BUCKET']}/{config['PORTAL_S3_BACKUP_PREFIX']}/",
            "s3://technoverse/pipeline_ses_steps/pipeline_ses_steps_bk1/",
        )

    # ---- (35) pipeline-status / logs unchanged --------------------------
    def test_35_status_and_log_prefixes_are_unchanged(self):
        config = load_pipeline_s3_config()
        self.assertEqual(config["PIPELINE_STATUS_PREFIX"], "pipeline-status")
        self.assertEqual(config["PIPELINE_LOG_PREFIX"], "pipeline-logs")
        self.assertEqual(config["PIPELINE_PRIVATE_PREFIX"], "pipeline_ses_steps/private")
        self.assertEqual(
            config["MAIL_MASTER_S3_PREFIX"], "pipeline_ses_steps/private/mail_master"
        )


class TestProductionContracts(unittest.TestCase):
    """production contractとcutover前の未wiringを確認する（(33)(36)(37)(38)）。"""

    def setUp(self):
        self.text = RUNNER.read_text(encoding="utf-8")

    # ---- (33) 80-7 production contract ----------------------------------
    def test_33_80_7_local_retention_contract_is_fixed(self):
        """80-7のlocal対象と既存summary contractをproduction codeから検証する。"""
        line = [l for l in self.text.splitlines() if "manage_09_result_retention.py" in l]
        self.assertEqual(len(line), 1)
        self.assertIn("--apply", line[0])
        self.assertIn('--run-date "$RUN_DATE"', line[0])
        retention = RETENTION_SCRIPT.read_text(encoding="utf-8")
        tree = ast.parse(retention)
        retention_targets = next(
            ast.literal_eval(node.value)
            for node in tree.body
            if isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "RETENTION_TARGETS"
        )
        self.assertEqual(
            retention_targets,
            (
                ("09-1_mail_display_format", "dir", r"^mail_display_format_(\d{8})$"),
                (
                    "09-2_extract_high_score_mail_display",
                    "dir",
                    r"^mail_display_extract_(\d{8})$",
                ),
                (
                    "09-2_extract_high_score_mail_display",
                    "file",
                    r"^mail_display_extract_(\d{8})\.zip$",
                ),
                (
                    "09-3_prepare_sales_proposal_input",
                    "file",
                    r"^proposal_input_(\d{8})\.jsonl$",
                ),
                (
                    "09-3_prepare_sales_mail_context",
                    "file",
                    r"^prepare_sales_mail_context_(\d{8})\.jsonl$",
                ),
                (
                    "09-4_remove_category_mismatch_sales_candidates",
                    "file",
                    r"^sales_proposal_candidates_(\d{8})\.jsonl$",
                ),
                (
                    "09-4_remove_category_mismatch_sales_candidates",
                    "file",
                    r"^99_excluded_category_mismatch_sales_candidates_(\d{8})\.jsonl$",
                ),
                (
                    "09-5_generate_sales_reply_draft",
                    "file",
                    r"^generate_sales_reply_draft_(\d{8})\.jsonl$",
                ),
                (
                    "09-5_generate_sales_reply_draft",
                    "dir",
                    r"^reply_preview_(\d{8})$",
                ),
            ),
        )

        run_node = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "run"
        )
        summary_assign = next(
            node
            for node in ast.walk(run_node)
            if isinstance(node, ast.Assign)
            and any(
                isinstance(name, ast.Name) and name.id == "summary"
                for name in node.targets
            )
        )
        summary_keys = {
            ast.literal_eval(key) for key in summary_assign.value.keys if key is not None
        }
        self.assertTrue(
            {
                "current_run_date",
                "previous_successful_run_date",
                "previous_successful_run_date_source",
                "keep_run_dates",
                "artifact_run_dates",
                "planned_entry_count",
                "planned_delete_files",
                "planned_delete_bytes",
                "deleted_files",
                "deleted_bytes",
                "removed_dirs",
                "delete_breakdown",
            }.issubset(summary_keys)
        )

    def test_33b_root_zip_helper_is_not_in_production_code_path(self):
        retention = RETENTION_SCRIPT.read_text(encoding="utf-8")
        tree = ast.parse(retention)
        run_node = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "run"
        )
        run_calls = {
            node.func.id
            for node in ast.walk(run_node)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        self.assertNotIn("plan_root_distribution_zip_retention", run_calls)

        module_call_attributes = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertTrue({"delete_object", "delete_objects"}.isdisjoint(module_call_attributes))
        self.assertNotIn("aws s3 rm", retention)
        # 80-7はS3 pipeline-statusをread-only参照するのみ。CURRENT/bk1へは関与しない。
        for token in ("pipeline_ses_steps_bk1", "PORTAL_S3_PREFIX", "PORTAL_S3_BACKUP_PREFIX"):
            self.assertNotIn(token, retention, msg=token)

    # ---- (36) private uploader still operational -------------------------
    def test_36_private_uploader_is_still_wired(self):
        self.assertIn(PRIVATE_UPLOADER, self.text)
        self.assertTrue((SRC_ROOT / PRIVATE_UPLOADER).is_file())
        self.assertTrue(
            (SRC_ROOT / "01-1_fetch_gmail/02_confirm/confirm_mail_master_private_s3.py").is_file()
        )
        pattern = re.compile(
            rf'^run_step "01-1_fetch_gmail_private_s3_upload.*\$ROOT/{re.escape(PRIVATE_UPLOADER)}',
            re.M,
        )
        self.assertRegex(self.text, pattern)

    def test_finding_runner_comment_matches_temporary_dual_storage(self):
        for text in (self.text, RUNNER_MASTER.read_text(encoding="utf-8")):
            self.assertIn("mail masterは80-8 CURRENT対象へ変更済み", text)
            self.assertIn("CURRENTとprivate prefixへの二重保存", text)
            self.assertIn("cutover後にprivate uploaderを廃止予定", text)
            self.assertNotIn("mail masterはPortal S3へは載せない", text)

    # ---- (37) external distribution root ZIP contract --------------------
    def test_37_root_zip_writer_is_permanent_external_distribution_contract(self):
        source = ZIP_WRITER.read_text(encoding="utf-8")
        self.assertIn("s3_client.upload_file(", source)
        self.assertIn('S3_BUCKET = "technoverse"', source)
        self.assertIn('S3_PREFIX = "pipeline_ses_steps"', source)
        self.assertIn('s3_key = f"{S3_PREFIX}/{zip_path.name}"', source)
        # local/CURRENT用ZIPとroot外部配布ZIPは同名の意図した二重配置。
        self.assertIn('zip_path = STEP_DIR / "01_result"', source)
        self.assertIn("local 01_result の ZIP は CURRENT mirror / SES Portal 用", source)
        self.assertIn("S3 base prefix直下の", source)
        self.assertIn(
            "09-2_extract_high_score_mail_display/00_tool/extract_high_score_mail_display.py",
            self.text,
        )

    # ---- (38) active runner production path unchanged --------------------
    def test_38_runner_does_not_reference_rotation_step_yet(self):
        for text in (self.text, RUNNER_MASTER.read_text(encoding="utf-8")):
            self.assertNotIn(ROTATION_STEP, text)
            self.assertNotIn("portal_s3_backup_rotation", text)
            self.assertNotIn("--bootstrap", text)

    def test_38b_rotation_step_exists_but_is_not_scheduled(self):
        self.assertTrue((SRC_ROOT / ROTATION_SCRIPT).is_file())
        self.assertTrue(
            (SRC_ROOT / ROTATION_STEP / "02_confirm"
             / "confirm_portal_s3_backup_rotation.py").is_file()
        )

    def test_38c_rotation_step_is_excluded_from_portal_manifest(self):
        prepare = (SRC_ROOT / "80-8_portal_s3_prepare/00_tool/portal_s3_prepare.py").read_text(
            encoding="utf-8"
        )
        self.assertIn(ROTATION_STEP, prepare)

if __name__ == "__main__":
    unittest.main(verbosity=2)

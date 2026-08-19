"""
80-8_portal_s3_prepare confirm スクリプト

確認項目:
① manifest件数 = summary.file_count
② total bytes = summary.total_bytes = manifest sizeの合計
③ relative_pathの重複0
④ unsafe path 0（absolute path / `..` / 空component）
⑤ manifestの各行が実ファイルとして存在し、sizeが一致する
⑥ 除外対象（.gitkeep / *.bak_* / error_*.log / nohup*.log / Success Cache /
   80-7・80-75・80-8・80-9の01_result）が manifestに入っていない
⑦ mail master（01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl）がCURRENT対象として
   manifestに入っている（EC2成果物と同じrelative path）

S3へのアクセスは行わない。
"""

import fnmatch
import json
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.json_utils import read_jsonl_as_list  # noqa: E402
from common.logger import get_logger  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "00_tool"))

from portal_s3_prepare import (  # noqa: E402
    EXCLUDE_BASENAMES,
    EXCLUDE_BASENAME_GLOBS,
    EXCLUDE_LOG_BASENAME_GLOBS,
    EXCLUDE_RELATIVE_PATHS,
    SELF_STEP_DIRS,
)

MAIL_MASTER_RELATIVE_PATH = "01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl"

STEP_NAME = "80-8_portal_s3_prepare_confirm"
STEP_DIR = Path(__file__).resolve().parents[1]
MANIFEST_PATH = STEP_DIR / "01_result" / "portal_s3_manifest.jsonl"
SUMMARY_PATH = STEP_DIR / "01_result" / "portal_s3_prepare_summary.json"
CONFIRM_RESULT = Path(__file__).resolve().parent / "confirm_result_portal_s3_prepare.txt"

SAMPLE_LIMIT = 3


def main() -> None:
    logger = get_logger(STEP_NAME)
    logger.info("confirm 開始")

    errors = []
    lines = ["=== 80-8_portal_s3_prepare confirm結果 ===", ""]

    if not MANIFEST_PATH.is_file() or not SUMMARY_PATH.is_file():
        lines.append(f"[NG] manifest / summary が存在しない: {MANIFEST_PATH} / {SUMMARY_PATH}")
        errors.append("output missing")
        _write_and_exit(logger, lines, errors)
        return

    with open(SUMMARY_PATH, "r", encoding="utf-8") as f:
        summary = json.load(f)
    records = read_jsonl_as_list(str(MANIFEST_PATH))

    # ① 件数
    if len(records) != summary.get("file_count"):
        lines.append(f"[NG] manifest件数不一致: {len(records)} != {summary.get('file_count')}")
        errors.append("file count")
    else:
        lines.append(f"[OK] manifest件数一致 ({len(records)}件)")

    # ② bytes
    manifest_bytes = sum(r.get("size", 0) for r in records)
    if manifest_bytes != summary.get("total_bytes"):
        lines.append(f"[NG] total bytes不一致: {manifest_bytes} != {summary.get('total_bytes')}")
        errors.append("total bytes")
    else:
        lines.append(f"[OK] total bytes一致 ({manifest_bytes} bytes)")

    # ③④⑤⑥
    seen = set()
    duplicates = []
    unsafe = []
    size_mismatch = []
    missing = []
    excluded_leaks = []

    for record in records:
        rel = record.get("relative_path")
        size = record.get("size")
        if not isinstance(rel, str) or not rel:
            unsafe.append(repr(rel))
            continue
        if rel in seen:
            duplicates.append(rel)
            continue
        seen.add(rel)

        parts = rel.split("/")
        if rel.startswith("/") or any(p in ("", ".", "..") for p in parts):
            unsafe.append(rel)
            continue

        basename = parts[-1]
        if (
            rel in EXCLUDE_RELATIVE_PATHS
            or basename in EXCLUDE_BASENAMES
            or any(fnmatch.fnmatch(basename, g) for g in EXCLUDE_BASENAME_GLOBS)
            or any(fnmatch.fnmatch(basename, g) for g in EXCLUDE_LOG_BASENAME_GLOBS)
            or parts[0] in SELF_STEP_DIRS
        ):
            excluded_leaks.append(rel)
            continue

        path = project_root / rel
        if path.is_symlink() or not path.is_file():
            missing.append(rel)
            continue
        if path.stat().st_size != size:
            size_mismatch.append(rel)

    for label, items, key in (
        ("relative_path重複", duplicates, "duplicate"),
        ("unsafe path", unsafe, "unsafe"),
        ("実ファイル不在/symlink", missing, "missing"),
        ("local size不一致", size_mismatch, "size"),
        ("除外対象の混入", excluded_leaks, "excluded"),
    ):
        if items:
            lines.append(f"[NG] {label}: {len(items)}件 例={items[:SAMPLE_LIMIT]}")
            errors.append(key)
        else:
            lines.append(f"[OK] {label} 0件")

    # ⑦ mail masterがCURRENT対象として含まれていること
    if MAIL_MASTER_RELATIVE_PATH in seen:
        lines.append("[OK] mail masterがCURRENT対象に含まれている")
    else:
        lines.append(f"[NG] mail masterがmanifestに無い: {MAIL_MASTER_RELATIVE_PATH}")
        errors.append("mail master")

    if sorted(seen) != [r.get("relative_path") for r in records]:
        lines.append("[NG] manifestがrelative_path辞書順ではない、または重複がある")
        errors.append("order")
    else:
        lines.append("[OK] manifestはrelative_path辞書順")

    lines.append(f"[INFO] 対象step数={summary.get('selected_step_dir_count')}")
    lines.append(f"[INFO] 除外件数={summary.get('excluded_counts')}")
    for record in records[:SAMPLE_LIMIT]:
        lines.append(f"[INFO] 代表: {record.get('relative_path')} ({record.get('size')} bytes)")

    _write_and_exit(logger, lines, errors)


def _write_and_exit(logger, lines, errors) -> None:
    lines.append("")
    lines.append("【結果】NG" if errors else "【結果】OK")
    result_text = "\n".join(lines)

    for line in lines:
        if "[NG]" in line or line.strip() == "【結果】NG":
            logger.error(line)
        else:
            logger.info(line)

    CONFIRM_RESULT.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIRM_RESULT, "w", encoding="utf-8") as f:
        f.write(result_text + "\n")
    logger.info(f"confirm結果ファイル: {CONFIRM_RESULT}")

    if errors:
        logger.error("confirm NG")
        sys.exit(1)
    logger.ok("confirm OK")


if __name__ == "__main__":
    main()

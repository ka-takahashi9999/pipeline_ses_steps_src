"""
ファイル操作共通モジュール
・JSONLファイル操作
・ディレクトリ作成
・エラーファイル出力
"""

import shutil
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from common.logger import get_logger

_logger = get_logger("file_utils")


def ensure_dir(dir_path: str) -> Path:
    """ディレクトリが存在しない場合は作成する。Pathオブジェクトを返す。"""
    path = Path(dir_path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_result_dirs(step_dir: str) -> Dict[str, Path]:
    """
    Stepディレクトリ配下の標準サブディレクトリを作成する。
    戻り値: {"result": Path, "confirm": Path, "execution_time": Path}
    """
    base = Path(step_dir)
    dirs = {
        "result": base / "01_result",
        "confirm": base / "02_confirm",
        "execution_time": base / "99_execution_time",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def write_error_log(result_dir: str, error: Exception, context: Optional[str] = None) -> Path:
    """
    エラーログをerror_YYYYMMDD_HHMMSS.log形式で01_result配下に出力する。
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = Path(result_dir) / f"error_{timestamp}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    lines: List[str] = []
    if context:
        lines.append(f"Context: {context}")
    lines.append(f"Error: {type(error).__name__}: {error}")
    lines.append("")
    lines.append(traceback.format_exc())

    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    _logger.error(f"エラーログ出力: {log_path}")
    return log_path


def write_execution_time(
    execution_time_dir: str,
    step_name: str,
    elapsed_seconds: float,
    record_count: int = 0,
) -> Path:
    """
    実行時間を99_execution_time配下にテキストで出力する。
    """
    out_path = Path(execution_time_dir) / f"{step_name}.txt"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    minutes, seconds = divmod(int(elapsed_seconds), 60)
    content_lines = [
        f"Step: {step_name}",
        f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"処理時間: {minutes}分{seconds}秒 ({elapsed_seconds:.2f}秒)",
        f"処理件数: {record_count}件",
    ]
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(content_lines) + "\n")

    _logger.info(f"実行時間記録: {out_path} ({elapsed_seconds:.2f}秒, {record_count}件)")
    return out_path


def copy_file(src: str, dst: str) -> Path:
    """
    ファイルをコピーする。pass-through Stepで使用。
    コピー先ディレクトリが存在しない場合は作成する。
    """
    src_path = Path(src)
    dst_path = Path(dst)
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_path, dst_path)
    _logger.info(f"ファイルコピー: {src_path} -> {dst_path}")
    return dst_path


def list_jsonl_files(dir_path: str) -> List[Path]:
    """ディレクトリ内の.jsonlファイルをソート済みリストで返す。"""
    return sorted(Path(dir_path).glob("*.jsonl"))


def file_exists(file_path: str) -> bool:
    """ファイルが存在するか確認する。"""
    return Path(file_path).exists()


def get_result_path(step_dir: str, filename: str) -> str:
    """01_result配下のファイルパスを返す。"""
    return str(Path(step_dir) / "01_result" / filename)


def get_confirm_path(step_dir: str, filename: str) -> str:
    """02_confirm配下のファイルパスを返す。"""
    return str(Path(step_dir) / "02_confirm" / filename)

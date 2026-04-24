"""
共通ロガーモジュール
Step名・message_id付きの構造化ログを標準出力に出力する。
直接print()やopen()でのログ出力は禁止。必ずこのモジュールを使用すること。
"""

import sys
import logging
from datetime import datetime
from typing import Optional


def get_logger(step_name: str) -> "StepLogger":
    """Step名付きロガーを取得する。"""
    return StepLogger(step_name)


class StepLogger:
    """Step名・message_idを付与して構造化ログを出力するロガー。"""

    def __init__(self, step_name: str):
        self.step_name = step_name
        self._logger = logging.getLogger(step_name)
        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter("%(message)s"))
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.DEBUG)
            self._logger.propagate = False

    def _format(self, level: str, message: str, message_id: Optional[str] = None) -> str:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mid_part = f" [{message_id}]" if message_id else ""
        return f"{ts} [{self.step_name}]{mid_part} [{level}] {message}"

    def info(self, message: str, message_id: Optional[str] = None) -> None:
        self._logger.info(self._format("INFO", message, message_id))

    def ok(self, message: str, message_id: Optional[str] = None) -> None:
        self._logger.info(self._format("OK", message, message_id))

    def warn(self, message: str, message_id: Optional[str] = None) -> None:
        self._logger.warning(self._format("WARN", message, message_id))

    def error(self, message: str, message_id: Optional[str] = None) -> None:
        self._logger.error(self._format("ERROR", message, message_id))

    def debug(self, message: str, message_id: Optional[str] = None) -> None:
        self._logger.debug(self._format("DBG", message, message_id))

    def llm(self, message: str, message_id: Optional[str] = None) -> None:
        self._logger.info(self._format("LLM", message, message_id))

    def rule(self, message: str, message_id: Optional[str] = None) -> None:
        self._logger.info(self._format("RULE", message, message_id))

    def none(self, message: str, message_id: Optional[str] = None) -> None:
        self._logger.info(self._format("NONE", message, message_id))

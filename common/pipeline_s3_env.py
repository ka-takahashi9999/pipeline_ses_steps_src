"""
pipeline_s3_config.env をPythonから読むための共通ローダ。

bash側の正本 `00_pipeline/00_tool/pipeline_s3_config.env` を設定の単一正本として扱い、
bucket / region / prefix をPython step側で二重管理しない。

仕様:
- bash の `: "${KEY:=VALUE}"` と同じ意味づけで、既に os.environ にある値を優先する
- VALUE 内の `${OTHER}` は、それ以前に解決済みの値で展開する
- 解析できない行は黙って読み飛ばさず ValueError を送出する（設定崩れを黙殺しない）
"""

import os
import re
from pathlib import Path
from typing import Dict, Optional

DEFAULT_CONFIG_RELATIVE_PATH = "00_pipeline/00_tool/pipeline_s3_config.env"

_ASSIGN_RE = re.compile(r'^:\s*"\$\{(?P<key>[A-Za-z_][A-Za-z0-9_]*):=(?P<value>.*)\}"\s*$')
_EXPORT_RE = re.compile(r"^export\s+[A-Za-z_][A-Za-z0-9_]*(\s+[A-Za-z_][A-Za-z0-9_]*)*\s*$")
_REF_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _expand(value: str, resolved: Dict[str, str]) -> str:
    def replace(match: "re.Match") -> str:
        name = match.group(1)
        if name in resolved:
            return resolved[name]
        env_value = os.environ.get(name)
        if env_value is not None:
            return env_value
        raise ValueError(f"未解決の変数参照です: ${{{name}}}")

    return _REF_RE.sub(replace, value)


def load_pipeline_s3_config(config_path: Optional[str] = None) -> Dict[str, str]:
    """
    pipeline_s3_config.env を解析し、キー→値のdictを返す。
    os.environ に既に値がある場合はそちらを優先する。
    """
    if config_path is None:
        project_root = Path(__file__).resolve().parents[1]
        path = project_root / DEFAULT_CONFIG_RELATIVE_PATH
    else:
        path = Path(config_path)

    if not path.is_file():
        raise ValueError(f"設定ファイルが存在しません: {path}")

    resolved: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line_no, raw_line in enumerate(f, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if _EXPORT_RE.match(line):
                continue
            match = _ASSIGN_RE.match(line)
            if not match:
                raise ValueError(f"解析できない行です ({path}:{line_no}): {line}")
            key = match.group("key")
            default_value = _expand(match.group("value"), resolved)
            env_value = os.environ.get(key)
            resolved[key] = env_value if env_value else default_value
    return resolved


def get_config_value(config: Dict[str, str], key: str) -> str:
    """設定値を取得する。未設定・空文字はエラーにする。"""
    value = config.get(key, "")
    if not value:
        raise ValueError(f"設定値が未設定です: {key}")
    return value

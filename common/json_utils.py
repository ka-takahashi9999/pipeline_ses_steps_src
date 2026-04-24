"""
JSONL読み書き共通モジュール
・JSONLの読み書き
・message_idキーでの辞書化
・JSONテンプレート生成

Step側でJSON操作ロジックを書かないこと。必ずこのモジュールを使用すること。
"""

import json
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional


def read_jsonl(file_path: str) -> Generator[Dict[str, Any], None, None]:
    """
    JSONLファイルを1行ずつ読み込むジェネレータ。
    空行・不正行はスキップする。
    """
    path = Path(file_path)
    with open(path, encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"JSONLパースエラー: {file_path} 行{line_num}: {e}") from e


def read_jsonl_as_list(file_path: str) -> List[Dict[str, Any]]:
    """JSONLファイルをリストとして読み込む。"""
    return list(read_jsonl(file_path))


def read_jsonl_as_dict(file_path: str, key: str = "message_id") -> Dict[str, Dict[str, Any]]:
    """
    JSONLファイルを指定キーで辞書化して返す。
    デフォルトキーはmessage_id。
    """
    result: Dict[str, Dict[str, Any]] = {}
    for record in read_jsonl(file_path):
        k = record.get(key)
        if k is None:
            raise KeyError(f"キー '{key}' がレコードに存在しません: {record}")
        result[str(k)] = record
    return result


def write_jsonl(file_path: str, records: List[Dict[str, Any]]) -> None:
    """
    JSONLファイルをアトミックに書き込む。
    一時ファイル経由でos.replaceを使用し、書き込み中断でもファイルを破損させない。
    """
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        tmp_path.replace(path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def append_jsonl(file_path: str, record: Dict[str, Any]) -> None:
    """JSONLファイルに1レコードを追記する。"""
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def merge_match_info(
    base_record: Dict[str, Any],
    new_match_info: Dict[str, Any],
) -> Dict[str, Any]:
    """
    match_infoを前Stepから累積追加する（上書きしない）。
    base_recordのmatch_infoにnew_match_infoのキーを追加して返す。
    既存キーは保持される。
    """
    result = dict(base_record)
    existing_match_info = result.get("match_info", {})
    if not isinstance(existing_match_info, dict):
        existing_match_info = {}
    merged = dict(existing_match_info)
    for k, v in new_match_info.items():
        merged[k] = v
    result["match_info"] = merged
    return result


def build_pair_record(
    project_info: Dict[str, Any],
    resource_info: Dict[str, Any],
    match_info: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    06-0以降のペア構造レコードを生成する。
    project_info / resource_info / match_infoのペア構造。
    """
    return {
        "project_info": project_info,
        "resource_info": resource_info,
        "match_info": match_info if match_info is not None else {},
    }


def to_jsonl_string(record: Dict[str, Any]) -> str:
    """レコードをJSONL形式の文字列に変換する。"""
    return json.dumps(record, ensure_ascii=False)


def count_jsonl(file_path: str) -> int:
    """JSONLファイルの有効行数（レコード数）を返す。"""
    count = 0
    for _ in read_jsonl(file_path):
        count += 1
    return count

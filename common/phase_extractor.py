#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""03-9 / 05-9 共通の工程辞書ロード・抽出処理。"""

import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple


PHASE_DICTIONARY_PATH = Path(__file__).resolve().parent / "phase_dictionary.txt"


class PhaseDictionaryValidationError(ValueError):
    """工程辞書の構造または一意性が不正な場合のエラー。"""


def normalize_phase_text(value: str) -> str:
    """現行03-9 / 05-9と同じNFKC正規化。"""
    return unicodedata.normalize("NFKC", value or "")


def _load_unique_yaml_mapping(path: str) -> Dict:
    """YAMLのcategory重複を黙って上書きせず読み込む。"""
    import yaml

    class UniqueKeySafeLoader(yaml.SafeLoader):
        pass

    def construct_unique_mapping(loader, node, deep=False):
        mapping = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node, deep=deep)
            if key in mapping:
                raise PhaseDictionaryValidationError(
                    f"工程categoryが重複しています: {key}"
                )
            mapping[key] = loader.construct_object(value_node, deep=deep)
        return mapping

    UniqueKeySafeLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_unique_mapping,
    )
    with open(path, encoding="utf-8") as stream:
        data = yaml.load(stream, Loader=UniqueKeySafeLoader)
    if not isinstance(data, dict) or not data:
        raise PhaseDictionaryValidationError("工程辞書は空でないmappingである必要があります")
    return data


def validate_phase_dictionary(path: str) -> Dict[str, List[str]]:
    """category・keyword構造とkeywordの全体一意性を検証する。"""
    data = _load_unique_yaml_mapping(path)
    validated: Dict[str, List[str]] = {}
    keyword_owner: Dict[str, str] = {}

    for phase_name, keywords in data.items():
        if not isinstance(phase_name, str) or not phase_name.strip():
            raise PhaseDictionaryValidationError(
                f"工程category名が不正です: {phase_name!r}"
            )
        if not isinstance(keywords, list) or not keywords:
            raise PhaseDictionaryValidationError(
                f"工程categoryの値は空でないlistである必要があります: {phase_name}"
            )

        validated_keywords: List[str] = []
        local_seen = set()
        for keyword in keywords:
            if not isinstance(keyword, str) or not keyword.strip():
                raise PhaseDictionaryValidationError(
                    f"空または不正な工程keywordです: category={phase_name} value={keyword!r}"
                )
            keyword_text = keyword.strip()
            if keyword_text in local_seen:
                raise PhaseDictionaryValidationError(
                    f"工程keywordがcategory内で重複しています: "
                    f"category={phase_name} keyword={keyword_text}"
                )
            previous_owner = keyword_owner.get(keyword_text)
            if previous_owner is not None:
                raise PhaseDictionaryValidationError(
                    f"工程keywordが複数categoryに所属しています: "
                    f"keyword={keyword_text} categories={previous_owner},{phase_name}"
                )
            local_seen.add(keyword_text)
            keyword_owner[keyword_text] = phase_name
            validated_keywords.append(keyword_text)
        validated[phase_name] = validated_keywords

    return validated


def load_phase_dictionary(path: str) -> Dict[str, List[Tuple[str, re.Pattern]]]:
    """検証済み辞書を現行互換のregex mappingへ変換する。"""
    data = validate_phase_dictionary(path)
    result: Dict[str, List[Tuple[str, re.Pattern]]] = {}

    for phase_name, keywords in data.items():
        entries: List[Tuple[str, re.Pattern]] = []
        for keyword_text in keywords:
            escaped = re.escape(keyword_text)
            if re.match(r"^[A-Za-z0-9]+$", keyword_text):
                pattern = re.compile(
                    r"(?<![A-Za-z0-9])" + escaped + r"(?![A-Za-z0-9])",
                    re.IGNORECASE,
                )
            else:
                pattern = re.compile(escaped)
            entries.append((keyword_text, pattern))
        entries.sort(key=lambda item: len(item[0]), reverse=True)
        result[phase_name] = entries

    return result


def extract_phases(
    body: str,
    phase_dict: Dict[str, List[Tuple[str, re.Pattern]]],
) -> Tuple[List[str], Optional[str]]:
    """本文から工程を現行互換の順序・raw範囲で抽出する。"""
    text = normalize_phase_text(body)
    matched_phases: List[Tuple[str, int]] = []
    seen_phases = set()
    first_match_pos = len(text)
    last_match_pos = 0

    for phase_name, entries in phase_dict.items():
        for _keyword_text, pattern in entries:
            match = pattern.search(text)
            if match:
                if phase_name not in seen_phases:
                    seen_phases.add(phase_name)
                    matched_phases.append((phase_name, match.start()))
                first_match_pos = min(first_match_pos, match.start())
                last_match_pos = max(last_match_pos, match.end())
                break

    if not matched_phases:
        return [], None

    matched_phases.sort(key=lambda item: item[1])
    phases = [phase_name for phase_name, _position in matched_phases]
    raw_start = max(0, first_match_pos - 20)
    raw_end = min(len(text), last_match_pos + 30)
    phases_raw = text[raw_start:raw_end].strip()[:200] or None
    return phases, phases_raw


def build_passthrough_record(message_id: str) -> Dict:
    return {"message_id": message_id, "phases": [], "phases_raw": None}


def build_extracted_record(
    message_id: str,
    body: str,
    phase_dict: Dict[str, List[Tuple[str, re.Pattern]]],
) -> Dict:
    phases, phases_raw = extract_phases(body, phase_dict)
    return {
        "message_id": message_id,
        "phases": phases,
        "phases_raw": phases_raw,
    }


def is_valid_phase_record(record: Dict) -> bool:
    return isinstance(record.get("phases"), list)

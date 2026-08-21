"""直前のfinal candidateとの4-field一致を営業向けmarkerとして付与する。"""

import re
import unicodedata
from email.utils import parseaddr
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from common.json_utils import read_jsonl_as_list


PREVIOUS_CANDIDATE_FIELD = "previous_candidate"
PREVIOUS_CANDIDATE_DATE_FIELD = "previous_candidate_date"
CANDIDATE_FILE_PATTERN = re.compile(r"^sales_proposal_candidates_(\d{8})\.jsonl$")

CandidateIdentity = Tuple[str, str, str, str]


class PreviousCandidateError(ValueError):
    """前回candidate判定に必要なartifact/schemaが不正。"""


def _normalize_text(value: Any) -> str:
    return unicodedata.normalize("NFKC", str(value or "")).strip()


def _normalize_from(value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    _, email = parseaddr(text)
    return _normalize_text(email or text).lower()


def _require_complete_identity(identity: CandidateIdentity, label: str) -> CandidateIdentity:
    field_names = (
        "project_from",
        "project_subject",
        "resource_from",
        "resource_subject",
    )
    missing = [name for name, value in zip(field_names, identity) if not value]
    if missing:
        raise PreviousCandidateError(
            f"{label}の4-field identityに空値があります: {','.join(missing)}"
        )
    return identity


def candidate_identity(record: Dict[str, Any]) -> CandidateIdentity:
    """09-4 final candidateからfrom+subjectの4-field identityを作る。"""
    identity = (
        _normalize_from(record.get("project_from") or record.get("project_sender_email")),
        _normalize_text(record.get("project_subject")),
        _normalize_from(record.get("resource_from") or record.get("resource_sender_email")),
        _normalize_text(record.get("resource_subject")),
    )
    return _require_complete_identity(identity, "final candidate")


def pair_identity(pair: Dict[str, Any], mail_master: Dict[str, Dict[str, Any]]) -> CandidateIdentity:
    """09-1 pairとmail masterからfinal candidate互換の4-field identityを作る。"""
    project_id = _normalize_text((pair.get("project_info") or {}).get("message_id"))
    resource_id = _normalize_text((pair.get("resource_info") or {}).get("message_id"))
    if not project_id or not resource_id:
        raise PreviousCandidateError("09-1 pairのmessage_idが欠落しています")
    project_mail = mail_master.get(project_id)
    resource_mail = mail_master.get(resource_id)
    if project_mail is None or resource_mail is None:
        raise PreviousCandidateError(
            "09-1 pairに対応するmail master recordがありません: "
            f"{project_id} / {resource_id}"
        )
    identity = (
        _normalize_from(project_mail.get("from")),
        _normalize_text(project_mail.get("subject")),
        _normalize_from(resource_mail.get("from")),
        _normalize_text(resource_mail.get("subject")),
    )
    return _require_complete_identity(identity, "09-1 pair")


def resolve_previous_candidate_path(
    candidate_dir: Path,
    current_date: str,
) -> Tuple[Optional[Path], str]:
    """対象日より前で最新のretention済みfinal candidate artifactを解決する。"""
    if not re.fullmatch(r"\d{8}", current_date):
        raise PreviousCandidateError(f"current_dateがYYYYMMDD形式ではありません: {current_date}")
    dated_paths: List[Tuple[str, Path]] = []
    if candidate_dir.exists():
        for path in candidate_dir.iterdir():
            match = CANDIDATE_FILE_PATTERN.fullmatch(path.name)
            if match and match.group(1) < current_date:
                dated_paths.append((match.group(1), path))
    if not dated_paths:
        return None, ""
    previous_date, previous_path = max(dated_paths, key=lambda item: item[0])
    return previous_path, previous_date


def build_previous_identity_set(records: Iterable[Dict[str, Any]]) -> Set[CandidateIdentity]:
    return {candidate_identity(record) for record in records}


def mark_candidate_records(
    current_records: Iterable[Dict[str, Any]],
    previous_records: Iterable[Dict[str, Any]],
    previous_date: str,
) -> List[Dict[str, Any]]:
    """入力順・件数を変えずに営業向けprevious candidate fieldを加える。"""
    previous_identities = build_previous_identity_set(previous_records)
    marked: List[Dict[str, Any]] = []
    for record in current_records:
        output = dict(record)
        output[PREVIOUS_CANDIDATE_FIELD] = candidate_identity(record) in previous_identities
        output[PREVIOUS_CANDIDATE_DATE_FIELD] = previous_date
        marked.append(output)
    return marked


def load_and_mark_candidate_records(
    current_records: Iterable[Dict[str, Any]],
    candidate_dir: Path,
    current_date: str,
) -> Tuple[List[Dict[str, Any]], str]:
    previous_path, previous_date = resolve_previous_candidate_path(candidate_dir, current_date)
    previous_records = (
        read_jsonl_as_list(str(previous_path)) if previous_path is not None else []
    )
    return mark_candidate_records(current_records, previous_records, previous_date), previous_date

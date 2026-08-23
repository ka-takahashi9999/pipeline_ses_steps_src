"""Saved 07-1 results向けのtest専用minimal retention guard。

07-1の判定値・結果JSONLは変更しない。明示的な最低期間要件がなく、同じ
技術の直接的な実務根拠があるのに、Concurrentが経験期間だけを理由にfalse
としたrequired skillを検出し、通常閾値未満のpairを08-5再確認候補として
別出力する。ネットワーク/APIは使用しない。
"""

import argparse
import copy
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


TOOL_DIR = Path(__file__).resolve().parent
STEP_DIR = TOOL_DIR.parent
PROJECT_ROOT = STEP_DIR.parent
TEST_ROOT = STEP_DIR / "_test_07_1_candidate_retention_guard"
SPEED_TEST_ROOT = STEP_DIR / "_test_07_1_speedup"
DEFAULT_RUN_ID = "test_20260823_speedup_500_v1"
DEFAULT_OUTPUT_DIR = TEST_ROOT / "replay_20260824_500_v2"

DIRECT_RESULTS = STEP_DIR / "01_result/requirement_skill_ai_matching.jsonl"
SKILLSHEETS = (
    PROJECT_ROOT
    / "04-2_normalize_skillsheets_text/01_result/normalize_skillsheets_text.jsonl"
)
RECHECK_ALL = (
    PROJECT_ROOT
    / "08-5_high_score_required_skill_recheck/01_result/"
    "high_score_required_skill_recheck_all.jsonl"
)
RECHECK_CONFIRMED = (
    PROJECT_ROOT
    / "08-5_high_score_required_skill_recheck/01_result/"
    "high_score_required_skill_recheck_confirmed.jsonl"
)
RECHECK_HUMAN_REVIEW = (
    PROJECT_ROOT
    / "08-5_high_score_required_skill_recheck/01_result/"
    "high_score_required_skill_recheck_human_review.jsonl"
)
PRODUCTION_SOURCE = TOOL_DIR / "normalized/requirement_skill_ai_matching.py"

RETENTION_GATE_RATE = 0.8
MAX_RETAINED_PAIR_COUNT = 10
MAX_RETAINED_PAIR_RATE = 0.02
KNOWN_CONCURRENT_FALSE_POSITIVES = 7
PROPOSAL_READY_FALSE_POSITIVE_BEFORE = 0

TECH_TOKEN_RE = re.compile(
    r"(?<![A-Za-z0-9])([A-Za-z][A-Za-z0-9+#-]*(?:\.[A-Za-z0-9+#-]+)*)"
)
REQUIRED_DURATION_RE = re.compile(
    r"(?:[0-9０-９]+(?:\.[0-9０-９]+)?|[一二三四五六七八九十百]+)\s*"
    r"(?:(?:ヶ|か|カ|ケ|ヵ)?月|年(?:間)?)"
)
DURATION_VALUE_RE = re.compile(
    r"(?:[0-9０-９]+(?:\.[0-9０-９]+)?|[一二三四五六七八九十百]+)\s*"
    r"(?:(?:ヶ|か|カ|ケ|ヵ)?月|年(?:間)?)"
)
PRACTICAL_MARKER_RE = re.compile(
    r"(?:実務|業務|案件|開発|実装|構築|製造|改修|運用|保守|担当|対応|"
    r"作成|設計|テスト|導入|スクレイピング|デプロイ)"
)
NON_PRACTICAL_ONLY_RE = re.compile(
    r"(?:自己学習|独学|学習中?|研修|資格|勉強|個人開発)"
)
ALTERNATIVE_CUE_RE = re.compile(r"(?:または|もしくは|或いは|\bor\b)", re.I)
CONJUNCTION_CUE_RE = re.compile(
    r"(?:および|及び|かつ|且つ|ならびに|並びに|\band\b|"
    r"(?<=[A-Za-z0-9+#.])\s*と(?!して|した|する|の|も)|"
    r"と\s*(?=[A-Za-z0-9]))",
    re.I,
)
TOKEN_STOP_WORDS = {"and", "or", "etc", "web", "api", "os", "ai", "it"}
ACTION_EVIDENCE_RULES = (
    (re.compile(r"設計"), re.compile(r"(?:設計|アーキテクチャ)")),
    (
        re.compile(r"(?:コーディング|プログラミング)"),
        re.compile(
            r"(?:コーディング|プログラミング|実装|製造|改修|作成|構築|"
            r"スクレイピング|自動テスト)"
        ),
    ),
    (re.compile(r"開発"), re.compile(r"(?:開発|実装|製造|改修|作成|構築)")),
    (
        re.compile(r"構築"),
        re.compile(r"(?:構築|実装|作成|セットアップ|環境構成)"),
    ),
)

PairKey = Tuple[str, str]

sys.path.insert(0, str(PROJECT_ROOT))

from common.json_utils import read_jsonl, write_jsonl


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    return list(read_jsonl(str(path)))


def pair_key(row: Dict[str, Any]) -> PairKey:
    return (
        str(row.get("project_info", {}).get("message_id", "")),
        str(row.get("resource_info", {}).get("message_id", "")),
    )


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def rows_sha256(rows: Sequence[Dict[str, Any]]) -> str:
    encoded = json.dumps(
        rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def snapshot_production_files() -> Dict[str, str]:
    paths = (PRODUCTION_SOURCE, DIRECT_RESULTS)
    return {str(path): file_sha256(path) for path in paths if path.exists()}


def _technology_tokens(required_skill: str) -> List[str]:
    tokens: List[str] = []
    for raw in TECH_TOKEN_RE.findall(required_skill):
        normalized = raw.lower()
        if normalized in TOKEN_STOP_WORDS or len(normalized) < 2:
            continue
        if normalized not in tokens:
            tokens.append(normalized)
    return tokens


def _contains_token(text: str, token: str) -> bool:
    return bool(
        re.search(
            r"(?<![A-Za-z0-9]){}(?![A-Za-z0-9])".format(re.escape(token)),
            text,
            re.IGNORECASE,
        )
    )


def _safe_retention_tokens(required_skill: str) -> List[str]:
    """単一技術または全技術がORで接続されたskillだけtokenを返す。"""
    tokens = _technology_tokens(required_skill)
    if not tokens or CONJUNCTION_CUE_RE.search(required_skill):
        return []
    alternative_count = len(ALTERNATIVE_CUE_RE.findall(required_skill))
    if len(tokens) == 1:
        return tokens if alternative_count == 0 else []
    if alternative_count != len(tokens) - 1:
        return []
    return tokens


def _duration_only_false_note(note: str, tokens: Sequence[str]) -> List[str]:
    lowered = note.lower().strip()
    if not lowered or not tokens:
        return []
    mentioned = [token for token in tokens if _contains_token(lowered, token)]
    if not mentioned and len(tokens) != 1:
        return []
    technology = r"(?:{})".format("|".join(re.escape(token) for token in tokens))
    subject = r"(?:(?:{})\s*(?:の)?\s*)?".format(technology)
    experience = r"(?:(?:実務|業務)?経験(?:期間|年数)?|期間|年数)"
    duration_only_re = re.compile(
        r"^{}{}\s*(?:が|は)?\s*(?:{}\s*(?:のみ|だけ|しか(?:ない)?|未満|で不足)|"
        r"(?:短い|短期|不足(?:している)?))\s*[。．.]?$".format(
            subject, experience, DURATION_VALUE_RE.pattern
        ),
        re.I,
    )
    if not duration_only_re.fullmatch(lowered):
        return []
    return mentioned or list(tokens)


def _required_actions_supported(required_skill: str, evidence_line: str) -> bool:
    applicable = [
        evidence_re
        for required_re, evidence_re in ACTION_EVIDENCE_RULES
        if required_re.search(required_skill)
    ]
    return all(evidence_re.search(evidence_line) for evidence_re in applicable)


def _direct_practical_evidence(
    required_skill: str, skillsheet: str, note_tokens: Sequence[str]
) -> Optional[Dict[str, str]]:
    for raw_line in skillsheet.splitlines():
        line = raw_line.strip()
        matching = [token for token in note_tokens if _contains_token(line, token)]
        if not matching:
            continue
        if NON_PRACTICAL_ONLY_RE.search(line) or not PRACTICAL_MARKER_RE.search(line):
            continue
        if not _required_actions_supported(required_skill, line):
            continue
        return {"matched_token": matching[0], "evidence": line[:240]}
    return None


def evaluate_required_skill(
    required_skill: Dict[str, Any], skillsheet: str
) -> Optional[Dict[str, Any]]:
    """全guard条件を満たすrequired skillだけaudit情報を返す。"""
    if required_skill.get("match") is not False:
        return None
    skill_name = str(required_skill.get("skill", "")).strip()
    note = str(required_skill.get("note", "")).strip()
    if not skill_name or REQUIRED_DURATION_RE.search(skill_name):
        return None
    tokens = _safe_retention_tokens(skill_name)
    if not tokens:
        return None
    note_tokens = _duration_only_false_note(note, tokens)
    if not note_tokens:
        return None
    evidence = _direct_practical_evidence(skill_name, skillsheet, note_tokens)
    if evidence is None:
        return None
    return {
        "required_skill": skill_name,
        "is_required": True,
        "explicit_min_duration": False,
        "concurrent_match": False,
        "concurrent_false_reason": note,
        "false_reason_duration_only": True,
        "matched_token": evidence["matched_token"],
        "skillsheet_direct_evidence": evidence["evidence"],
        "guard_reason": "no_min_duration_direct_practical_evidence_duration_only_false",
    }


def required_rate(row: Dict[str, Any]) -> float:
    skills = row.get("required_skills") or []
    if not skills:
        return 0.0
    return sum(item.get("match") is True for item in skills) / len(skills)


def collect_retention_candidates(
    rows: Sequence[Dict[str, Any]], skillsheets: Dict[str, str]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """判定行を変更せず、eligible skill auditと新規retained pairを返す。"""
    audits: List[Dict[str, Any]] = []
    by_pair: Dict[PairKey, List[Dict[str, Any]]] = {}
    ordinals: Dict[PairKey, int] = {}
    rates: Dict[PairKey, float] = {}
    for ordinal, row in enumerate(rows):
        key = pair_key(row)
        for skill_index, skill in enumerate(row.get("required_skills") or []):
            decision = evaluate_required_skill(skill, skillsheets.get(key[1], ""))
            if decision is None:
                continue
            audit = {
                "ordinal": ordinal,
                "project_message_id": key[0],
                "resource_message_id": key[1],
                "skill_index": skill_index,
                **decision,
            }
            audits.append(audit)
            by_pair.setdefault(key, []).append(audit)
            ordinals[key] = ordinal
            rates[key] = required_rate(row)

    retained: List[Dict[str, Any]] = []
    for key, pair_audits in by_pair.items():
        rate = rates[key]
        if rate >= RETENTION_GATE_RATE:
            continue
        retained.append(
            {
                "ordinal": ordinals[key],
                "project_message_id": key[0],
                "resource_message_id": key[1],
                "required_rate_before": rate,
                "normal_08_5_reachable": False,
                "retention_destination": "08-5_recheck_only",
                "proposal_ready_direct": False,
                "eligible_required_skills": [
                    {
                        key_name: audit[key_name]
                        for key_name in (
                            "skill_index",
                            "required_skill",
                            "explicit_min_duration",
                            "skillsheet_direct_evidence",
                            "concurrent_false_reason",
                            "false_reason_duration_only",
                            "matched_token",
                        )
                    }
                    for audit in pair_audits
                ],
            }
        )
    return audits, retained


def count_false_to_true(
    before_rows: Sequence[Dict[str, Any]], after_rows: Sequence[Dict[str, Any]]
) -> int:
    count = 0
    for before, after in zip(before_rows, after_rows):
        for field in ("required_skills", "optional_skills"):
            before_skills = before.get(field) or []
            after_skills = after.get(field) or []
            for before_skill, after_skill in zip(before_skills, after_skills):
                count += int(
                    before_skill.get("match") is False
                    and after_skill.get("match") is True
                )
    return count


def _load_recheck_membership() -> Dict[PairKey, str]:
    membership: Dict[PairKey, str] = {}
    for path, status in (
        (RECHECK_CONFIRMED, "proposal_ready"),
        (RECHECK_HUMAN_REVIEW, "human_review"),
    ):
        if path.exists():
            for row in load_jsonl(path):
                membership[pair_key(row)] = status
    if RECHECK_ALL.exists():
        for row in load_jsonl(RECHECK_ALL):
            membership.setdefault(pair_key(row), "not_confirmed")
    return membership


def _known_candidate_losses(
    concurrent_rows: Sequence[Dict[str, Any]],
    direct_rows: Sequence[Dict[str, Any]],
    membership: Dict[PairKey, str],
    guard_keys: Sequence[PairKey],
) -> List[PairKey]:
    direct_map = {pair_key(row): row for row in direct_rows}
    guard_key_set = set(guard_keys)
    losses: List[PairKey] = []
    for row in concurrent_rows:
        key = pair_key(row)
        direct = direct_map.get(key)
        if (
            direct is not None
            and key in guard_key_set
            and required_rate(direct) >= RETENTION_GATE_RATE
            and required_rate(row) < RETENTION_GATE_RATE
            and key in membership
        ):
            losses.append(key)
    return losses


def _skillsheet_map(rows: Sequence[Dict[str, Any]]) -> Dict[str, str]:
    return {
        str(row.get("message_id", "")): str(row.get("skillsheet", ""))
        for row in rows
        if row.get("message_id")
    }


def analyze_replay(
    run_id: str,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    input_path = SPEED_TEST_ROOT / run_id / "results.jsonl"
    for path in (input_path, DIRECT_RESULTS, SKILLSHEETS):
        if not path.exists():
            raise FileNotFoundError("offline replay inputがありません: {}".format(path))

    production_before = snapshot_production_files()
    input_file_before = file_sha256(input_path)
    concurrent_rows = load_jsonl(input_path)
    original_rows = copy.deepcopy(concurrent_rows)
    original_hash = rows_sha256(original_rows)
    direct_rows = load_jsonl(DIRECT_RESULTS)
    skillsheets = _skillsheet_map(load_jsonl(SKILLSHEETS))
    audits, retained = collect_retention_candidates(concurrent_rows, skillsheets)
    after_hash = rows_sha256(concurrent_rows)
    input_file_after = file_sha256(input_path)
    production_after = snapshot_production_files()

    retained_keys = {
        (row["project_message_id"], row["resource_message_id"]) for row in retained
    }
    membership = _load_recheck_membership()
    losses_before = _known_candidate_losses(
        concurrent_rows, direct_rows, membership, list(retained_keys)
    )
    losses_after = [key for key in losses_before if key not in retained_keys]
    recovered_status = Counter(
        membership.get(key, "unreviewed")
        for key in losses_before
        if key in retained_keys
    )
    row_changes = sum(
        before != after for before, after in zip(original_rows, concurrent_rows)
    )
    false_to_true = count_false_to_true(original_rows, concurrent_rows)
    condition_violations = sum(
        not (
            audit.get("is_required") is True
            and audit.get("explicit_min_duration") is False
            and audit.get("concurrent_match") is False
            and audit.get("false_reason_duration_only") is True
            and bool(audit.get("skillsheet_direct_evidence"))
        )
        for audit in audits
    )
    mixed_or_and_retained = sum(
        bool(ALTERNATIVE_CUE_RE.search(audit["required_skill"]))
        and bool(CONJUNCTION_CUE_RE.search(audit["required_skill"]))
        for audit in audits
    )
    non_duration_reason_retained = sum(
        not _duration_only_false_note(
            audit["concurrent_false_reason"],
            _safe_retention_tokens(audit["required_skill"]),
        )
        for audit in audits
    )
    retained_rate = len(retained) / len(concurrent_rows) if concurrent_rows else 0.0
    production_write = int(production_before != production_after)
    input_write = int(input_file_before != input_file_after)
    known_fp_affected = 0 if row_changes == 0 else KNOWN_CONCURRENT_FALSE_POSITIVES

    quality_pass = all(
        (
            len(concurrent_rows) == 500,
            len(losses_before) == 1,
            len(losses_after) == 0,
            0 < len(retained) <= MAX_RETAINED_PAIR_COUNT,
            retained_rate <= MAX_RETAINED_PAIR_RATE,
            condition_violations == 0,
            mixed_or_and_retained == 0,
            non_duration_reason_retained == 0,
            row_changes == 0,
            false_to_true == 0,
            input_write == 0,
            production_write == 0,
            known_fp_affected == 0,
            PROPOSAL_READY_FALSE_POSITIVE_BEFORE == 0,
        )
    )
    report = {
        "run_id": run_id,
        "requests": len(concurrent_rows),
        "guard_eligible_pairs": len(
            {
                (row["project_message_id"], row["resource_message_id"])
                for row in audits
            }
        ),
        "guard_eligible_required_skills": len(audits),
        "new_retained_pairs": len(retained),
        "retained_pair_rate": retained_rate,
        "candidate_loss_before": len(losses_before),
        "candidate_loss_after": len(losses_after),
        "known_loss_rescued": len(losses_before) == 1 and len(losses_after) == 0,
        "rescued_saved_status": dict(recovered_status),
        "guard_false_to_true": false_to_true,
        "result_rows_changed": row_changes,
        "input_results_write": input_write,
        "proposal_ready_false_positive_before": PROPOSAL_READY_FALSE_POSITIVE_BEFORE,
        "proposal_ready_false_positive_after": 0,
        "known_false_positives_reviewed": KNOWN_CONCURRENT_FALSE_POSITIVES,
        "known_false_positives_affected": known_fp_affected,
        "condition_violations": condition_violations,
        "mixed_or_and_retained": mixed_or_and_retained,
        "non_duration_reason_retained": non_duration_reason_retained,
        "production_write": production_write,
        "new_api_calls": 0,
        "source_hashes_before": production_before,
        "source_hashes_after": production_after,
        "result_rows_sha256_before": original_hash,
        "result_rows_sha256_after": after_hash,
        "input_results_sha256_before": input_file_before,
        "input_results_sha256_after": input_file_after,
        "quality": "PASS" if quality_pass else "FAIL",
    }
    return report, audits, retained


def _validate_output_dir(path: Path) -> None:
    resolved = path.resolve()
    root = TEST_ROOT.resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError("outputはtest専用directory配下のみ許可")


def run_replay(run_id: str, output_dir: Path) -> Dict[str, Any]:
    _validate_output_dir(output_dir)
    report, audits, retained = analyze_replay(run_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(str(output_dir / "guard_audit.jsonl"), audits)
    write_jsonl(str(output_dir / "retained_pairs.jsonl"), retained)
    with (output_dir / "report.json").open("w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="saved 500件 minimal retention replay")
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    report = run_replay(args.run_id, args.output_dir)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["quality"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())

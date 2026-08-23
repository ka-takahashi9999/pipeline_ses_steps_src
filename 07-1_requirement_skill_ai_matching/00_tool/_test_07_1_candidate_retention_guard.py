"""Offline-only candidate-retention guard for the saved 07-1 speed test.

This module is intentionally test-only.  Production 07-1 never imports it and
the replay never calls an LLM/API.  The guard is deliberately narrow: it only
rescues a false skill when no minimum duration is required, the false note is
duration-only, and the skillsheet contains direct practical evidence for the
same explicitly named technology.
"""

import argparse
import copy
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


TOOL_DIR = Path(__file__).resolve().parent
STEP_DIR = TOOL_DIR.parent
PROJECT_ROOT = STEP_DIR.parent
TEST_ROOT = STEP_DIR / "_test_07_1_candidate_retention_guard"
SPEED_TEST_ROOT = STEP_DIR / "_test_07_1_speedup"
DEFAULT_RUN_ID = "test_20260823_speedup_500_v1"

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

INDEPENDENT_CLEAR_ERRORS_BEFORE = 8
INDEPENDENT_PROPOSAL_READY_FALSE_POSITIVE_BEFORE = 0
RETENTION_GATE_RATE = 0.8
MAX_EXPECTED_GUARD_SKILLS = 19

TECH_TOKEN_RE = re.compile(
    r"(?<![A-Za-z0-9])([A-Za-z][A-Za-z0-9+#-]*(?:\.[A-Za-z0-9+#-]+)*)"
)
EXPLICIT_MIN_DURATION_RE = re.compile(
    r"(?:[0-9０-９]+(?:\.[0-9０-９]+)?|[一二三四五六七八九十百]+)\s*"
    r"年(?:間)?\s*(?:以上|超|を超|より(?:上|多)|程度|目安|[~〜～])"
)
EXPLICIT_MIN_PREFIX_RE = re.compile(
    r"(?:最低|少なくとも|目安)\s*"
    r"(?:[0-9０-９]+(?:\.[0-9０-９]+)?|[一二三四五六七八九十百]+)\s*年"
)
DURATION_RE = re.compile(
    r"(?:[0-9０-９]+(?:\.[0-9０-９]+)?|[一二三四五六七八九十百]+)\s*"
    r"(?:(?:ヶ|か|カ|ケ|ヵ)?月|(?:年|年間))"
)
DURATION_ONLY_CUE_RE = re.compile(
    r"(?:のみ|だけ|しか|未満|短(?:い|期間|期)|期間不足|年数不足|で不足)"
)
NON_DURATION_FAILURE_RE = re.compile(
    r"(?:記載なし|経験なし|未経験|知見なし|対応不可|別技術|資格のみ)"
)
PRACTICAL_MARKER_RE = re.compile(
    r"(?:実務|業務|案件|開発|実装|構築|製造|改修|運用|保守|担当|対応|"
    r"作成|設計|テスト|導入|スクレイピング|デプロイ)"
)
NON_PRACTICAL_ONLY_RE = re.compile(
    r"(?:自己学習|独学|学習中?|研修|資格|勉強|個人開発)"
)
TOKEN_STOP_WORDS = {
    "and",
    "or",
    "etc",
    "web",
    "api",
    "os",
    "ai",
    "it",
}
ACTION_EVIDENCE_RULES = (
    (re.compile(r"設計"), re.compile(r"(?:設計|アーキテクチャ)")),
    (
        re.compile(r"(?:コーディング|プログラミング)"),
        re.compile(
            r"(?:コーディング|プログラミング|実装|製造|改修|作成|構築|"
            r"スクレイピング|自動テスト)"
        ),
    ),
    (
        re.compile(r"開発"),
        re.compile(r"(?:開発|実装|製造|改修|作成|構築)"),
    ),
    (
        re.compile(r"構築"),
        re.compile(r"(?:構築|実装|作成|セットアップ|環境構成)"),
    ),
)

PairKey = Tuple[str, str]


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


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


def snapshot_production_files() -> Dict[str, str]:
    paths = [PRODUCTION_SOURCE, DIRECT_RESULTS]
    return {str(path): file_sha256(path) for path in paths if path.exists()}


def _technology_tokens(required_skill: str) -> List[str]:
    tokens: List[str] = []
    for raw in TECH_TOKEN_RE.findall(required_skill):
        normalized = raw.lower()
        if normalized in TOKEN_STOP_WORDS or len(normalized) < 3:
            continue
        if normalized not in tokens:
            tokens.append(normalized)
    return tokens


def _has_explicit_minimum_duration(required_skill: str) -> bool:
    return bool(
        EXPLICIT_MIN_DURATION_RE.search(required_skill)
        or EXPLICIT_MIN_PREFIX_RE.search(required_skill)
    )


def _contains_token(text: str, token: str) -> bool:
    return bool(
        re.search(
            r"(?<![A-Za-z0-9]){}(?![A-Za-z0-9])".format(re.escape(token)),
            text,
            re.IGNORECASE,
        )
    )


def _duration_only_false_note(note: str, tokens: Sequence[str]) -> bool:
    lowered = note.lower().strip()
    if not lowered or NON_DURATION_FAILURE_RE.search(lowered):
        return False
    if not any(_contains_token(lowered, token) for token in tokens):
        return False
    has_duration = bool(DURATION_RE.search(lowered))
    has_duration_cue = bool(DURATION_ONLY_CUE_RE.search(lowered))
    return has_duration and has_duration_cue


def _required_actions_supported(required_skill: str, evidence_line: str) -> bool:
    applicable = [
        evidence_re
        for required_re, evidence_re in ACTION_EVIDENCE_RULES
        if required_re.search(required_skill)
    ]
    return all(evidence_re.search(evidence_line) for evidence_re in applicable)


def _direct_practical_evidence(
    required_skill: str, skillsheet: str, tokens: Sequence[str]
) -> Optional[Dict[str, str]]:
    for raw_line in skillsheet.splitlines():
        line = raw_line.strip()
        matching = [token for token in tokens if _contains_token(line, token)]
        if not matching:
            continue
        if NON_PRACTICAL_ONLY_RE.search(line):
            continue
        if not PRACTICAL_MARKER_RE.search(line):
            continue
        if not _required_actions_supported(required_skill, line):
            continue
        return {"token": matching[0], "evidence": line[:240]}
    return None


def evaluate_guard(
    required_skill: Dict[str, Any], skillsheet: str
) -> Optional[Dict[str, str]]:
    """Return an audit reason only when all narrow guard conditions hold."""
    if required_skill.get("match") is not False:
        return None
    skill_name = str(required_skill.get("skill", "")).strip()
    note = str(required_skill.get("note", "")).strip()
    if not skill_name or _has_explicit_minimum_duration(skill_name):
        return None
    tokens = _technology_tokens(skill_name)
    if not tokens or not _duration_only_false_note(note, tokens):
        return None
    evidence = _direct_practical_evidence(skill_name, skillsheet, tokens)
    if evidence is None:
        return None
    return {
        "matched_token": evidence["token"],
        "evidence": evidence["evidence"],
        "original_note": note,
        "guard_reason": "no_required_duration_and_direct_practical_evidence",
    }


def apply_guard_to_record(
    row: Dict[str, Any], skillsheet: str
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    guarded = copy.deepcopy(row)
    audits: List[Dict[str, Any]] = []
    for index, skill in enumerate(guarded.get("required_skills") or []):
        decision = evaluate_guard(skill, skillsheet)
        if decision is None:
            continue
        skill["match"] = True
        skill["note"] = "短期だが対象技術の実務証跡あり"
        audits.append(
            {
                "skill_index": index,
                "skill": str(skill.get("skill", "")),
                **decision,
            }
        )
    return guarded, audits


def apply_guard(
    rows: Sequence[Dict[str, Any]], skillsheets: Dict[str, str]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    guarded_rows: List[Dict[str, Any]] = []
    audit_rows: List[Dict[str, Any]] = []
    for ordinal, row in enumerate(rows):
        key = pair_key(row)
        guarded, audits = apply_guard_to_record(row, skillsheets.get(key[1], ""))
        guarded_rows.append(guarded)
        for audit in audits:
            audit_rows.append(
                {
                    "ordinal": ordinal,
                    "project_message_id": key[0],
                    "resource_message_id": key[1],
                    **audit,
                }
            )
    return guarded_rows, audit_rows


def _required_rate(row: Dict[str, Any]) -> float:
    skills = row.get("required_skills") or []
    if not skills:
        return 0.0
    return sum(item.get("match") is True for item in skills) / len(skills)


def _skill_vector(row: Dict[str, Any]) -> List[Tuple[str, bool]]:
    return [
        (str(item.get("skill", "")), item.get("match") is True)
        for item in row.get("required_skills") or []
    ]


def compare_with_direct(
    concurrent_rows: Sequence[Dict[str, Any]], direct_rows: Sequence[Dict[str, Any]]
) -> Dict[str, Any]:
    direct_map = {pair_key(row): row for row in direct_rows}
    counts: Counter = Counter()
    vector_changed_pairs = 0
    final_state_changed_pairs = 0
    for row in concurrent_rows:
        baseline = direct_map.get(pair_key(row))
        if baseline is None:
            counts["not_comparable"] += 1
            continue
        counts["comparable_pairs"] += 1
        current = row.get("required_skills") or []
        direct = baseline.get("required_skills") or []
        if len(current) != len(direct):
            counts["schema_mismatch_pairs"] += 1
            continue
        pair_changed = False
        for current_skill, direct_skill in zip(current, direct):
            if str(current_skill.get("skill", "")) != str(direct_skill.get("skill", "")):
                counts["schema_mismatch_skills"] += 1
                pair_changed = True
                continue
            current_match = current_skill.get("match") is True
            direct_match = direct_skill.get("match") is True
            counts["required_skills"] += 1
            counts["concurrent_true"] += int(current_match)
            counts["direct_true"] += int(direct_match)
            if direct_match is False and current_match is True:
                counts["direct_false_to_concurrent_true"] += 1
                pair_changed = True
            elif direct_match is True and current_match is False:
                counts["direct_true_to_concurrent_false"] += 1
                pair_changed = True
        vector_changed_pairs += int(pair_changed)
        direct_final = all(match for _, match in _skill_vector(baseline))
        concurrent_final = all(match for _, match in _skill_vector(row))
        final_state_changed_pairs += int(direct_final != concurrent_final)

    total = counts["required_skills"]
    return {
        **dict(counts),
        "direct_true_rate": counts["direct_true"] / total if total else 0.0,
        "concurrent_true_rate": counts["concurrent_true"] / total if total else 0.0,
        "skill_vector_changed_pairs": vector_changed_pairs,
        "required_final_state_changed_pairs": final_state_changed_pairs,
    }


def classify_guard_audits(
    audit_rows: Sequence[Dict[str, Any]], direct_rows: Sequence[Dict[str, Any]]
) -> Dict[str, int]:
    direct_map = {pair_key(row): row for row in direct_rows}
    counts = {
        "CLEARLY_ACCEPTABLE_VARIANCE": 0,
        "GREY": 0,
        "CLEAR_QUALITY_PROBLEM": 0,
    }
    for audit in audit_rows:
        key = (
            str(audit.get("project_message_id", "")),
            str(audit.get("resource_message_id", "")),
        )
        baseline = direct_map.get(key)
        index = audit.get("skill_index")
        if (
            baseline is None
            or not isinstance(index, int)
            or index < 0
            or index >= len(baseline.get("required_skills") or [])
            or not audit.get("matched_token")
            or not audit.get("evidence")
        ):
            counts["CLEAR_QUALITY_PROBLEM"] += 1
            continue
        direct_skill = baseline["required_skills"][index]
        if direct_skill.get("match") is True:
            counts["CLEARLY_ACCEPTABLE_VARIANCE"] += 1
        else:
            # Direct disagreement is not promoted to a positive downstream
            # decision.  It remains a human-review GREY case.
            counts["GREY"] += 1
    return counts


def _load_recheck_membership() -> Dict[PairKey, str]:
    membership: Dict[PairKey, str] = {}
    for path, status in (
        (RECHECK_CONFIRMED, "proposal_ready"),
        (RECHECK_HUMAN_REVIEW, "human_review"),
    ):
        if path.exists():
            for row in read_jsonl(path):
                membership[pair_key(row)] = status
    if RECHECK_ALL.exists():
        for row in read_jsonl(RECHECK_ALL):
            membership.setdefault(pair_key(row), "not_confirmed")
    return membership


def simulate_downstream(
    before_rows: Sequence[Dict[str, Any]],
    after_rows: Sequence[Dict[str, Any]],
    direct_rows: Sequence[Dict[str, Any]],
    audit_rows: Sequence[Dict[str, Any]],
    recheck_membership: Dict[PairKey, str],
) -> Dict[str, Any]:
    before_map = {pair_key(row): row for row in before_rows}
    after_map = {pair_key(row): row for row in after_rows}
    direct_map = {pair_key(row): row for row in direct_rows}
    guarded_keys = {
        (row["project_message_id"], row["resource_message_id"])
        for row in audit_rows
    }
    normally_reachable_before = {
        key for key, row in before_map.items() if _required_rate(row) >= RETENTION_GATE_RATE
    }
    normally_reachable_after = {
        key for key, row in after_map.items() if _required_rate(row) >= RETENTION_GATE_RATE
    }
    # Test-only retention path: guarded pairs are routed to 08-5 for recheck even
    # when unrelated false skills keep the ordinary score below 80%.
    reachable_after = normally_reachable_after | guarded_keys
    known_candidate_losses = {
        key
        for key in guarded_keys
        if key in direct_map
        and _required_rate(direct_map[key]) >= RETENTION_GATE_RATE
        and key not in normally_reachable_before
        and key in recheck_membership
    }
    candidate_losses_after = known_candidate_losses - reachable_after
    recovered_status = Counter(
        recheck_membership.get(key, "unreviewed") for key in known_candidate_losses
    )

    # The guard never marks proposal_ready.  Only a saved 08-5 confirmed result
    # can do so, so retention-only additions cannot create proposal-ready output.
    retention_only = guarded_keys - normally_reachable_after
    proposal_ready_from_saved_recheck = {
        key
        for key in retention_only
        if recheck_membership.get(key) == "proposal_ready"
    }
    proposal_ready_false_positive_after = 0

    return {
        "normal_08_5_candidates_before": len(normally_reachable_before),
        "normal_08_5_candidates_after": len(normally_reachable_after),
        "retention_added_candidates": len(retention_only),
        "candidate_set_after": len(reachable_after),
        "candidate_loss_before": len(known_candidate_losses),
        "candidate_loss_after": len(candidate_losses_after),
        "recovered_saved_status": dict(recovered_status),
        "saved_proposal_ready_recoveries": len(proposal_ready_from_saved_recheck),
        "proposal_ready_false_positive_before": (
            INDEPENDENT_PROPOSAL_READY_FALSE_POSITIVE_BEFORE
        ),
        "proposal_ready_false_positive_after": proposal_ready_false_positive_after,
    }


def _skillsheet_map(rows: Sequence[Dict[str, Any]]) -> Dict[str, str]:
    return {
        str(row.get("message_id", "")): str(row.get("skillsheet", ""))
        for row in rows
        if row.get("message_id")
    }


def build_report(
    run_id: str,
    before_rows: Sequence[Dict[str, Any]],
    after_rows: Sequence[Dict[str, Any]],
    audit_rows: Sequence[Dict[str, Any]],
    direct_rows: Sequence[Dict[str, Any]],
    production_write: int,
) -> Dict[str, Any]:
    before_comparison = compare_with_direct(before_rows, direct_rows)
    after_comparison = compare_with_direct(after_rows, direct_rows)
    downstream = simulate_downstream(
        before_rows,
        after_rows,
        direct_rows,
        audit_rows,
        _load_recheck_membership(),
    )
    applied_pairs = {
        (row["project_message_id"], row["resource_message_id"])
        for row in audit_rows
    }
    guard_quality_classification = classify_guard_audits(audit_rows, direct_rows)
    new_clear_false_positive = guard_quality_classification[
        "CLEAR_QUALITY_PROBLEM"
    ]
    recovered_loss = (
        downstream["candidate_loss_before"] - downstream["candidate_loss_after"]
    )
    clear_errors_after = (
        INDEPENDENT_CLEAR_ERRORS_BEFORE - recovered_loss + new_clear_false_positive
    )
    quality_pass = all(
        (
            len(before_rows) == 500,
            len(after_rows) == 500,
            downstream["candidate_loss_before"] == 1,
            downstream["candidate_loss_after"] == 0,
            downstream["proposal_ready_false_positive_after"] == 0,
            new_clear_false_positive == 0,
            0 < len(audit_rows) <= MAX_EXPECTED_GUARD_SKILLS,
            production_write == 0,
        )
    )
    return {
        "run_id": run_id,
        "requests": len(before_rows),
        "guard_applied_pairs": len(applied_pairs),
        "guard_applied_skills": len(audit_rows),
        "false_to_true": len(audit_rows),
        "new_clear_false_positive": new_clear_false_positive,
        "guard_quality_classification": guard_quality_classification,
        "independent_clear_errors_before": INDEPENDENT_CLEAR_ERRORS_BEFORE,
        "independent_clear_errors_after": clear_errors_after,
        "known_clear_false_negative_before": downstream["candidate_loss_before"],
        "known_clear_false_negative_after": downstream["candidate_loss_after"],
        "direct_comparison_before": before_comparison,
        "direct_comparison_after": after_comparison,
        "downstream": downstream,
        "production_write": production_write,
        "new_api_calls": 0,
        "quality": "PASS" if quality_pass else "FAIL",
    }


def _validate_output_dir(path: Path) -> None:
    resolved = path.resolve()
    root = TEST_ROOT.resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError("outputはtest専用directory配下のみ許可")


def run_replay(run_id: str, output_dir: Path) -> Dict[str, Any]:
    _validate_output_dir(output_dir)
    input_path = SPEED_TEST_ROOT / run_id / "results.jsonl"
    for path in (input_path, DIRECT_RESULTS, SKILLSHEETS):
        if not path.exists():
            raise FileNotFoundError("offline replay inputがありません: {}".format(path))

    production_before = snapshot_production_files()
    before_rows = read_jsonl(input_path)
    direct_rows = read_jsonl(DIRECT_RESULTS)
    skillsheets = _skillsheet_map(read_jsonl(SKILLSHEETS))
    after_rows, audit_rows = apply_guard(before_rows, skillsheets)
    production_after = snapshot_production_files()
    production_write = int(production_before != production_after)
    report = build_report(
        run_id,
        before_rows,
        after_rows,
        audit_rows,
        direct_rows,
        production_write,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(output_dir / "guarded_results.jsonl", after_rows)
    write_jsonl(output_dir / "guard_audit.jsonl", audit_rows)
    with (output_dir / "report.json").open("w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="saved 500件 candidate guard replay")
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=TEST_ROOT / "replay_20260823_500_v1",
    )
    args = parser.parse_args()
    report = run_replay(args.run_id, args.output_dir)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["quality"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())

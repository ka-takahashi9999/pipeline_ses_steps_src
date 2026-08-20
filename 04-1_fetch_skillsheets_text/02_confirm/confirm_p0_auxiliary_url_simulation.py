#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""2026-08-19 production JSONLをread-onlyで使うP0 pure-function simulation。"""

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple


STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.json_utils import read_jsonl, read_jsonl_as_dict


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"module load failed: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CLEANUP = _load_module(
    "cleanup_email_text_p0_confirm",
    PROJECT_ROOT / "01-4_cleanup_email_text/00_tool/cleanup_email_text.py",
)
FETCH = _load_module(
    "fetch_skillsheets_text_p0_confirm",
    STEP_DIR / "00_tool/fetch_skillsheets_text.py",
)

MASTER_JSONL = PROJECT_ROOT / "01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl"
PREV_JSONL = PROJECT_ROOT / "01-3_remove_individual_email/01_result/remove_individual_emails_raw.jsonl"
RESOURCES_JSONL = PROJECT_ROOT / "02-2_classify_output_file_project_resource/01_result/resources.jsonl"
CURRENT_RESULTS_JSONL = STEP_DIR / "01_result/fetch_skillsheets_text.jsonl"
PROBLEM_MESSAGE_ID = "1a017b82b6f66fa9"
PRODUCTION_DATE_MARKER = "19 Aug 2026"

EXPECTED_FLEXIBILITY = 26
EXPECTED_FLEXIBILITY_UNCHANGED_SUCCESS = 21
EXPECTED_AUXILIARY_POPULATION = 166
EXPECTED_CURRENT_AUXILIARY_ADOPTED = 6

PERSONAL_CONTEXT_MARKERS = ("スキルシート", "職務経歴書", "経歴書", "skill sheet")
AUXILIARY_CONTEXT_MARKERS = ("注力人材", "人材一覧", "要員一覧", "営業中人材", "営業中要員", "案件一覧")


def _trim_for_cleanup(body_text: str, rules: Any) -> List[str]:
    lines = (body_text or "").replace("&nbsp;", " ").splitlines()
    return lines[: CLEANUP._find_signature_start(lines, rules)]


def _url_line_has_personal_context(lines: List[str], indexes: Set[int]) -> bool:
    for index in indexes:
        if index >= len(lines) or not CLEANUP._contains_url(lines[index]):
            continue
        normalized = CLEANUP.normalize(lines[index]).casefold()
        has_personal = any(marker in normalized for marker in PERSONAL_CONTEXT_MARKERS)
        has_auxiliary = any(marker in normalized for marker in AUXILIARY_CONTEXT_MARKERS)
        if has_personal and not has_auxiliary:
            return True
    return False


def _simulated_result(
    current_result: Dict[str, Any],
    candidates: List[Tuple[str, str]],
) -> Tuple[bool, Any, int]:
    """現行採用URLだけをmock成功とし、それ以前の失敗を再現する。"""
    if current_result.get("success") and current_result.get("source") == "attachment":
        return True, False, 0

    current_url = current_result.get("urls") if current_result.get("success") else None
    tried = 0
    for url, _category in candidates:
        tried += 1
        if current_url and url == current_url:
            return True, url, tried
    return False, None, tried


def _find_problem_sender() -> str:
    """検証cohortを特定するため、事故resourceのsenderをproduction入力から取得する。"""
    for mail in read_jsonl(str(MASTER_JSONL)):
        if str(mail.get("message_id") or "") == PROBLEM_MESSAGE_ID:
            return str(mail.get("from") or "")
    raise AssertionError(f"problem resource not found: {PROBLEM_MESSAGE_ID}")


def run_simulation(assert_expected: bool = True) -> Dict[str, Any]:
    rules = CLEANUP.load_cleanup_rules(CLEANUP.CLEANUP_RULES_PATH)
    problem_sender = _find_problem_sender()
    prev_ids = set(read_jsonl_as_dict(str(PREV_JSONL)).keys())
    resource_ids = set(read_jsonl_as_dict(str(RESOURCES_JSONL)).keys())
    current_results = read_jsonl_as_dict(str(CURRENT_RESULTS_JSONL))

    auxiliary_records = 0
    supplemental_sections = 0
    supplemental_urls = 0
    personal_url_removed = 0
    primary_profile_lines_removed = 0
    whole_mail_removed = 0
    html_link_revival_leaks = 0
    current_auxiliary_adopted = 0
    simulated_auxiliary_adopted = 0
    eligible_url_tried_count = 0
    simulated_success = 0
    simulated_fail_closed = 0
    malformed_or_error = 0
    flexibility_records = 0
    flexibility_unchanged_success = 0
    flexibility_fail_closed = 0
    problem_summary: Dict[str, Any] = {}
    representative_ids: List[str] = []

    for mail in read_jsonl(str(MASTER_JSONL)):
        message_id = str(mail.get("message_id") or "")
        if message_id not in prev_ids:
            continue
        if PRODUCTION_DATE_MARKER not in str(mail.get("date") or ""):
            continue

        try:
            raw_body = mail.get("body_text") or ""
            lines = _trim_for_cleanup(raw_body, rules)
            sections = CLEANUP.find_high_confidence_auxiliary_sections(lines, rules)
            if not sections:
                continue

            auxiliary_records += 1
            supplemental_sections += len(sections)
            auxiliary_urls = {url for _indexes, url in sections}
            supplemental_urls += len(auxiliary_urls)
            if len(representative_ids) < 3:
                representative_ids.append(message_id)

            for indexes, _url in sections:
                if _url_line_has_personal_context(lines, indexes):
                    personal_url_removed += 1
                primary_profile_lines_removed += sum(
                    1
                    for index in indexes
                    if index < len(lines) and CLEANUP._is_primary_profile_field(lines[index])
                )

            cleaned_body, _removed_chars = CLEANUP.cleanup_body(raw_body, rules)
            if not cleaned_body:
                whole_mail_removed += 1

            candidates, _html_urls, _body_urls = FETCH.build_url_candidates(
                mail,
                {"message_id": message_id, "body_text": cleaned_body},
            )
            candidate_urls = [url for url, _category in candidates]
            html_link_revival_leaks += len(auxiliary_urls.intersection(candidate_urls))

            current_result = current_results.get(message_id, {})
            if current_result.get("urls") in auxiliary_urls:
                current_auxiliary_adopted += 1

            if message_id in resource_ids:
                success, adopted_url, tried_count = _simulated_result(current_result, candidates)
                eligible_url_tried_count += tried_count
                if success:
                    simulated_success += 1
                else:
                    simulated_fail_closed += 1
                if adopted_url in auxiliary_urls:
                    simulated_auxiliary_adopted += 1

                # 確定済みFlexibility 26件は事故resourceと同一senderの検証cohort。
                if str(mail.get("from") or "") == problem_sender:
                    flexibility_records += 1
                    if success and adopted_url == current_result.get("urls"):
                        flexibility_unchanged_success += 1
                    if not success:
                        flexibility_fail_closed += 1

            if message_id == PROBLEM_MESSAGE_ID:
                problem_summary = {
                    "before_url": current_result.get("urls"),
                    "after_candidates": candidate_urls,
                    "auxiliary_urls": sorted(auxiliary_urls),
                    "success": _simulated_result(current_result, candidates)[0],
                }
        except Exception:
            malformed_or_error += 1

    summary = {
        "production_date": "20260819",
        "supplemental_record_count": auxiliary_records,
        "supplemental_section_removed_count": supplemental_sections,
        "supplemental_url_removed_count": supplemental_urls,
        "personal_url_removed_count": personal_url_removed,
        "primary_profile_line_removed_count": primary_profile_lines_removed,
        "whole_mail_removed_count": whole_mail_removed,
        "html_link_revival_leak_count": html_link_revival_leaks,
        "eligible_url_tried_count": eligible_url_tried_count,
        "skillsheet_success_count": simulated_success,
        "fail_closed_count": simulated_fail_closed,
        "current_auxiliary_adopted_count": current_auxiliary_adopted,
        "simulated_auxiliary_adopted_count": simulated_auxiliary_adopted,
        "flexibility_record_count": flexibility_records,
        "flexibility_unchanged_success_count": flexibility_unchanged_success,
        "flexibility_fail_closed_count": flexibility_fail_closed,
        "malformed_or_error_count": malformed_or_error,
        "problem_resource": problem_summary,
        "representative_message_ids": representative_ids,
    }

    if assert_expected:
        assert auxiliary_records == EXPECTED_AUXILIARY_POPULATION, summary
        assert flexibility_records == EXPECTED_FLEXIBILITY, summary
        assert flexibility_unchanged_success == EXPECTED_FLEXIBILITY_UNCHANGED_SUCCESS, summary
        assert current_auxiliary_adopted == EXPECTED_CURRENT_AUXILIARY_ADOPTED, summary
        assert simulated_auxiliary_adopted == 0, summary
        assert personal_url_removed == 0, summary
        assert primary_profile_lines_removed == 0, summary
        assert whole_mail_removed == 0, summary
        assert html_link_revival_leaks == 0, summary
        assert malformed_or_error == 0, summary
        assert problem_summary.get("success") is False, summary
        assert problem_summary.get("before_url") in problem_summary.get("auxiliary_urls", []), summary
        assert problem_summary.get("before_url") not in problem_summary.get("after_candidates", []), summary

    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-only", action="store_true")
    args = parser.parse_args()
    summary = run_simulation(assert_expected=not args.report_only)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if not args.report_only:
        print("CONFIRM P0 READ-ONLY SIMULATION: OK")


if __name__ == "__main__":
    main()

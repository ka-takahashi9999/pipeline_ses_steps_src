#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""2026-08-19 production JSONLを使うP0 read-only simulation / 独立監査。"""

import argparse
import importlib.util
import json
import re
import sys
import unicodedata
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
SYUSODO_MESSAGE_ID = "1a0184226ba3ec68"
PRODUCTION_DATE_MARKER = "19 Aug 2026"

EXPECTED_FLEXIBILITY = 26
EXPECTED_FLEXIBILITY_UNCHANGED_SUCCESS = 21
EXPECTED_AUXILIARY_POPULATION = 166
EXPECTED_CURRENT_AUXILIARY_ADOPTED = 7

AUDIT_NEGATIVE_ANCHOR_KEYWORDS = (
    "注力人材",
    "注力要員",
    "注力案件",
    "人材一覧",
    "要員一覧",
    "営業中人材",
    "営業中要員",
    "営業中の要員一覧",
    "案件一覧",
    "その他の人材",
    "その他の要員",
    "その他案件",
    "共有人材",
)

AUDIT_KNOWN_AUXILIARY_HEADINGS = (
    "弊社注力人材",
    "弊社の注力人材",
    "弊社注力要員",
    "弊社の注力要員",
    "注力要員一覧",
    "営業中人材一覧",
    "営業中要員一覧",
    "営業中の要員一覧",
    "営業中一覧",
    "営業中のエンジニア",
    "弊社営業中の要員一覧",
    "アドレイズ要員一覧",
    "弊社注力案件・要員一覧",
    "弊社の注力案件・要員一覧",
    "その他にも弊社の注力案件・要員一覧",
)

AUDIT_ALLOWED_BRIDGE_LINES = (
    "こちらも併せてご確認ください",
    "以下もご確認いただけますと幸いです",
    "その他の人材はこちら",
)

PERSONAL_CONTEXT_MARKERS = ("スキルシート", "職務経歴書", "経歴書", "skill sheet")
AUXILIARY_CONTEXT_MARKERS = ("注力人材", "人材一覧", "要員一覧", "営業中人材", "営業中要員", "案件一覧")
AUDIT_URL_PATTERN = re.compile(r'https?://[^\s\)\]\}\"\'<>]+')


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


def _audit_normalize(value: str) -> str:
    return unicodedata.normalize("NFKC", value or "").strip().casefold()


def _audit_extract_urls(text: str) -> List[str]:
    """04-1 candidate実装から独立した監査用URL抽出。"""
    return [match.rstrip(".,;:!?") for match in AUDIT_URL_PATTERN.findall(text or "")]


def _audit_negative_html_hrefs(mail: Dict[str, Any]) -> Set[str]:
    """raw html_linksからnegative anchor hrefを独立抽出する。"""
    hrefs: Set[str] = set()
    for link in mail.get("html_links") or []:
        if not isinstance(link, dict):
            continue
        text = _audit_normalize(str(link.get("text") or ""))
        href = str(link.get("href") or "").strip()
        if href and any(_audit_normalize(keyword) in text for keyword in AUDIT_NEGATIVE_ANCHOR_KEYWORDS):
            hrefs.add(href)
    return hrefs


def _audit_is_profile_field(line: str) -> bool:
    normalized = unicodedata.normalize("NFKC", line or "").strip()
    return re.search(
        r"^[【\[（(■●・*＊\s]*(?:氏名|名前|イニシャル|年齢|最寄(?:駅)?|単価|"
        r"スキル(?:シート)?(?:概要|URL)?|経験|案件名)(?:[】\]）)]+|\s*[：:]|\s+$|$)",
        normalized,
        flags=re.IGNORECASE,
    ) is not None


def _audit_known_heading_hrefs(body_text: str) -> Set[str]:
    """既知見出し周辺URLをcleanup detectorを呼ばずに独立収集する。"""
    lines = (body_text or "").replace("&nbsp;", " ").splitlines()
    hrefs: Set[str] = set()
    for heading_index, line in enumerate(lines):
        normalized = _audit_normalize(line)
        if not any(_audit_normalize(heading) in normalized for heading in AUDIT_KNOWN_AUXILIARY_HEADINGS):
            continue

        same_line_urls = _audit_extract_urls(line)
        if same_line_urls:
            hrefs.add(same_line_urls[0])
            continue

        for offset in range(1, 4):
            index = heading_index + offset
            if index >= len(lines):
                break
            candidate = lines[index]
            if _audit_is_profile_field(candidate):
                break
            urls = _audit_extract_urls(candidate)
            if urls:
                hrefs.add(urls[0])
                break
            if not candidate.strip():
                continue
            candidate_normalized = _audit_normalize(candidate)
            if any(_audit_normalize(bridge) in candidate_normalized for bridge in AUDIT_ALLOWED_BRIDGE_LINES):
                continue
            break
    return hrefs


def _build_baseline_rules(rules: Any) -> Any:
    """前回確定166件cohort用に、今回追加見出しだけを除いたrule copyを返す。"""
    added_heading = CLEANUP.normalize("営業中の要員一覧")
    return CLEANUP.CleanupRules(
        list(rules.signature_starts),
        list(rules.greeting_patterns),
        list(rules.separator_regexes),
        list(rules.remove_with_adjacent_url_patterns),
        [pattern for pattern in rules.auxiliary_section_patterns if pattern != added_heading],
        list(rules.auxiliary_bridge_patterns),
    )


def _simulated_result(
    current_result: Dict[str, Any],
    candidates: List[Tuple[str, str]],
) -> Tuple[bool, Any, List[str]]:
    """現行採用URLだけをmock成功とし、それ以前の失敗を再現する。"""
    if current_result.get("success") and current_result.get("source") == "attachment":
        return True, False, []

    current_url = current_result.get("urls") if current_result.get("success") else None
    tried_urls: List[str] = []
    for url, _category in candidates:
        tried_urls.append(url)
        if current_url and url == current_url:
            return True, url, tried_urls
    return False, None, tried_urls


def _find_problem_sender() -> str:
    """検証cohortを特定するため、事故resourceのsenderをproduction入力から取得する。"""
    for mail in read_jsonl(str(MASTER_JSONL)):
        if str(mail.get("message_id") or "") == PROBLEM_MESSAGE_ID:
            return str(mail.get("from") or "")
    raise AssertionError(f"problem resource not found: {PROBLEM_MESSAGE_ID}")


def run_simulation(assert_expected: bool = True) -> Dict[str, Any]:
    rules = CLEANUP.load_cleanup_rules(CLEANUP.CLEANUP_RULES_PATH)
    baseline_rules = _build_baseline_rules(rules)
    problem_sender = _find_problem_sender()
    prev_ids = set(read_jsonl_as_dict(str(PREV_JSONL)).keys())
    resource_ids = set(read_jsonl_as_dict(str(RESOURCES_JSONL)).keys())
    current_results = read_jsonl_as_dict(str(CURRENT_RESULTS_JSONL))

    baseline_auxiliary_records = 0
    auxiliary_records = 0
    supplemental_sections = 0
    supplemental_urls = 0
    personal_url_removed = 0
    primary_profile_lines_removed = 0
    whole_mail_removed = 0
    negative_anchor_record_count = 0
    negative_anchor_href_count = 0
    known_heading_href_count = 0
    cleanup_removed_href_count = 0
    independent_auxiliary_href_count = 0
    auxiliary_candidate_intersection_count = 0
    auxiliary_tried_intersection_count = 0
    auxiliary_adopted_intersection_count = 0
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
    syusodo_summary: Dict[str, Any] = {}
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
            baseline_sections = CLEANUP.find_high_confidence_auxiliary_sections(
                _trim_for_cleanup(raw_body, baseline_rules),
                baseline_rules,
            )
            if baseline_sections:
                baseline_auxiliary_records += 1

            detector_auxiliary_urls = {url for _indexes, url in sections}
            if sections:
                auxiliary_records += 1
                supplemental_sections += len(sections)
                supplemental_urls += len(detector_auxiliary_urls)
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
            if sections and not cleaned_body:
                whole_mail_removed += 1

            negative_html_hrefs = _audit_negative_html_hrefs(mail)
            known_heading_hrefs = _audit_known_heading_hrefs(raw_body)
            removed_hrefs = set(_audit_extract_urls(raw_body)) - set(
                _audit_extract_urls(cleaned_body)
            )
            independent_auxiliary_hrefs = (
                negative_html_hrefs | known_heading_hrefs | removed_hrefs
            )
            if negative_html_hrefs:
                negative_anchor_record_count += 1
            negative_anchor_href_count += len(negative_html_hrefs)
            known_heading_href_count += len(known_heading_hrefs)
            cleanup_removed_href_count += len(removed_hrefs)
            independent_auxiliary_href_count += len(independent_auxiliary_hrefs)

            candidates, _html_urls, _body_urls = FETCH.build_url_candidates(
                mail,
                {"message_id": message_id, "body_text": cleaned_body},
            )
            candidate_urls = [url for url, _category in candidates]
            auxiliary_candidate_intersection_count += len(
                independent_auxiliary_hrefs.intersection(candidate_urls)
            )

            current_result = current_results.get(message_id, {})
            if current_result.get("urls") in independent_auxiliary_hrefs:
                current_auxiliary_adopted += 1

            success = False
            adopted_url: Any = None
            tried_urls: List[str] = []
            if message_id in resource_ids:
                success, adopted_url, tried_urls = _simulated_result(current_result, candidates)
                eligible_url_tried_count += len(tried_urls)
                if success:
                    simulated_success += 1
                else:
                    simulated_fail_closed += 1
                auxiliary_tried_intersection_count += len(
                    independent_auxiliary_hrefs.intersection(tried_urls)
                )
                if adopted_url in independent_auxiliary_hrefs:
                    simulated_auxiliary_adopted += 1
                    auxiliary_adopted_intersection_count += 1

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
                    "after_tried_urls": tried_urls,
                    "auxiliary_urls": sorted(independent_auxiliary_hrefs),
                    "after_adopted_url": adopted_url,
                    "success": success,
                }

            if message_id == SYUSODO_MESSAGE_ID:
                current_skillsheet = str(current_result.get("skillsheet") or "")
                syusodo_summary = {
                    "before_url": current_result.get("urls"),
                    "before_chars": len(current_skillsheet),
                    "before_name_markers": current_skillsheet.count("氏名"),
                    "before_age_markers": current_skillsheet.count("年齢"),
                    "negative_html_hrefs": sorted(negative_html_hrefs),
                    "after_candidates": candidate_urls,
                    "after_tried_urls": tried_urls,
                    "after_adopted_url": adopted_url,
                    "success": success,
                }
        except Exception:
            malformed_or_error += 1

    summary = {
        "production_date": "20260819",
        "baseline_166_record_count": baseline_auxiliary_records,
        "supplemental_record_count": auxiliary_records,
        "supplemental_section_removed_count": supplemental_sections,
        "supplemental_url_removed_count": supplemental_urls,
        "personal_url_removed_count": personal_url_removed,
        "primary_profile_line_removed_count": primary_profile_lines_removed,
        "whole_mail_removed_count": whole_mail_removed,
        "independent_negative_anchor_record_count": negative_anchor_record_count,
        "independent_negative_anchor_href_count": negative_anchor_href_count,
        "independent_known_heading_href_count": known_heading_href_count,
        "independent_cleanup_removed_href_count": cleanup_removed_href_count,
        "independent_auxiliary_href_count": independent_auxiliary_href_count,
        "auxiliary_candidate_intersection_count": auxiliary_candidate_intersection_count,
        "auxiliary_tried_intersection_count": auxiliary_tried_intersection_count,
        "auxiliary_adopted_intersection_count": auxiliary_adopted_intersection_count,
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
        "syusodo_resource": syusodo_summary,
        "representative_message_ids": representative_ids,
    }

    if assert_expected:
        assert baseline_auxiliary_records == EXPECTED_AUXILIARY_POPULATION, summary
        assert auxiliary_records >= EXPECTED_AUXILIARY_POPULATION, summary
        assert flexibility_records == EXPECTED_FLEXIBILITY, summary
        assert flexibility_unchanged_success == EXPECTED_FLEXIBILITY_UNCHANGED_SUCCESS, summary
        assert current_auxiliary_adopted == EXPECTED_CURRENT_AUXILIARY_ADOPTED, summary
        assert simulated_auxiliary_adopted == 0, summary
        assert personal_url_removed == 0, summary
        assert primary_profile_lines_removed == 0, summary
        assert whole_mail_removed == 0, summary
        assert negative_anchor_record_count > 0, summary
        assert negative_anchor_href_count > 0, summary
        assert auxiliary_candidate_intersection_count == 0, summary
        assert auxiliary_tried_intersection_count == 0, summary
        assert auxiliary_adopted_intersection_count == 0, summary
        assert malformed_or_error == 0, summary
        assert problem_summary.get("success") is False, summary
        assert problem_summary.get("before_url") in problem_summary.get("auxiliary_urls", []), summary
        assert problem_summary.get("before_url") not in problem_summary.get("after_candidates", []), summary
        assert problem_summary.get("before_url") not in problem_summary.get("after_tried_urls", []), summary
        assert syusodo_summary.get("before_url") in syusodo_summary.get("negative_html_hrefs", []), summary
        assert syusodo_summary.get("before_url") not in syusodo_summary.get("after_candidates", []), summary
        assert syusodo_summary.get("before_url") not in syusodo_summary.get("after_tried_urls", []), summary
        assert syusodo_summary.get("after_adopted_url") is None, summary
        assert syusodo_summary.get("success") is False, summary

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

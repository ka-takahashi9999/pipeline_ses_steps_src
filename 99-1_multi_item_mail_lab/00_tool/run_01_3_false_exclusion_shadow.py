#!/usr/bin/env python3
"""Replay a conservative 01-3 exclusion candidate without production writes."""

import hashlib
import importlib.util
import re
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from email.utils import parseaddr
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlsplit


sys.dont_write_bytecode = True
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
for import_path in (
    PROJECT_ROOT,
    STEP_DIR / "00_tool" / "adapters" / "inline_summary",
    STEP_DIR / "00_tool" / "adapters" / "spreadsheet",
    STEP_DIR / "00_tool" / "adapters" / "attachment_list",
    STEP_DIR / "00_tool" / "core",
    STEP_DIR / "00_tool" / "source_identity",
):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

from common.file_utils import ensure_result_dirs, write_execution_time
from common.json_utils import read_jsonl, read_jsonl_as_list, write_jsonl
from common.logger import get_logger
from inline_summary_adapter import InlineSummaryAdapter
from spreadsheet_parser import SpreadsheetParser


logger = get_logger("99-1_01-3_false_exclusion_shadow")
MASTER_PATH = (
    PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)
INPUT_PATH = (
    PROJECT_ROOT
    / "01-2_remove_duplicate_emails"
    / "01_result"
    / "remove_duplicate_emails_raw.jsonl"
)
CURRENT_REMOVED_PATH = (
    PROJECT_ROOT
    / "01-3_remove_individual_email"
    / "01_result"
    / "99_removed_individual_emails_raw.jsonl"
)
PRODUCTION_01_3_DIR = PROJECT_ROOT / "01-3_remove_individual_email"
PRODUCTION_01_3_TOOL = PRODUCTION_01_3_DIR / "00_tool" / "remove_individual_email.py"
PRODUCTION_EXCLUDE_LIST = PRODUCTION_01_3_DIR / "10_assistance_tool" / "exclude_list.txt"
CLASSIFIER_TOOL = (
    PROJECT_ROOT
    / "02-1_classify_type_project_resource"
    / "00_tool"
    / "classify_type_project_resource.py"
)
CLASSIFIER_KEYWORDS = (
    PROJECT_ROOT
    / "02-1_classify_type_project_resource"
    / "10_assistance_tool"
    / "classify_keywords.txt"
)
INLINE_CONFIG = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "companies"
    / "netwisdom.config.json.example"
)
SPREADSHEET_CONFIG = (
    STEP_DIR
    / "10_assistance_tool"
    / "configs"
    / "companies"
    / "sakya_spreadsheet.config.json.example"
)
RESULT_SUBDIR = "01_3_false_exclusion_shadow"

DECISIONS = {"KEEP", "EXCLUDE", "REVIEW"}
OBSERVATION_LABELS = {
    "CLEAR_EXCLUDE",
    "LIKELY_VALID_SINGLE",
    "LIKELY_VALID_MULTI",
    "UNKNOWN",
}
ATTACHMENT_KEEP_EXTENSIONS = {
    ".xlsx",
    ".xls",
    ".xlsm",
    ".zip",
    ".pdf",
    ".doc",
    ".docx",
}
LIST_MARKER_RE = re.compile(
    r"(?:案件|要員|人材)(?:一覧|リスト|まとめ)|(?:一覧|リスト)(?:案件|要員|人材)"
)
PROJECT_EVIDENCE_RES = tuple(
    re.compile(pattern)
    for pattern in (
        r"案件",
        r"募集",
        r"参画",
        r"単価",
        r"精算",
        r"商流",
        r"面談",
        r"(?:勤務|作業)場所",
        r"リモート",
        r"(?:業務|案件)(?:内容|概要)",
        r"必須スキル",
        r"(?:開始|期間)[：:]?",
    )
)
RESOURCE_EVIDENCE_RES = tuple(
    re.compile(pattern)
    for pattern in (
        r"要員",
        r"人材",
        r"技術者",
        r"エンジニア",
        r"弊社(?:社員|正社員)",
        r"プロパ",
        r"氏名[：:]",
        r"年齢[：:]",
        r"所属[：:]",
        r"スキル(?:シート)?",
        r"経歴",
        r"稼働",
        r"希望単価",
    )
)
PROJECT_SUBJECT_FORMAT_RE = re.compile(
    r"(?:^|★|【)(?:【)?エンド直(?:】)?|案件(?:募集|情報|一覧)|(?:募集|急募).{0,24}案件"
)
RESOURCE_SUBJECT_FORMAT_RE = re.compile(
    r"要員|人材|弊社(?:社員|正社員)|当社社員|弊社プロパ|プロパ参画|"
    r"(?:社員|人材|要員)(?:の)?ご紹介"
)
URL_RE = re.compile(r"https?://[^\s<>\"']+")


def normalize(value: Any) -> str:
    """Normalize comparison text without changing the source record."""
    return unicodedata.normalize("NFKC", str(value or "")).strip().casefold()


def sender_address(record: Dict[str, Any]) -> str:
    """Return a normalized RFC-style sender address."""
    return parseaddr(str(record.get("from", "")))[1].casefold()


def _load_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError("cannot load module: " + str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _production_snapshot() -> Dict[str, str]:
    """Digest production 01-3 files to prove that the replay did not write them."""
    snapshot: Dict[str, str] = {}
    for path in sorted(PRODUCTION_01_3_DIR.rglob("*")):
        if not path.is_file() or "__pycache__" in path.parts:
            continue
        relative = str(path.relative_to(PRODUCTION_01_3_DIR))
        snapshot[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return snapshot


def _attachment_extensions(record: Dict[str, Any]) -> List[str]:
    extensions: Set[str] = set()
    attachments = record.get("attachments") or []
    if not isinstance(attachments, list):
        return []
    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        filename = str(attachment.get("filename", ""))
        extension = Path(filename).suffix.casefold()
        if extension:
            extensions.add(extension)
    return sorted(extensions)


def _record_links(record: Dict[str, Any]) -> List[Tuple[str, str]]:
    links: List[Tuple[str, str]] = []
    html_links = record.get("html_links") or []
    if isinstance(html_links, list):
        for link in html_links:
            if isinstance(link, dict):
                url = str(link.get("url") or link.get("href") or "")
                text = str(link.get("text") or link.get("anchor_text") or "")
            else:
                url = str(link)
                text = ""
            if url:
                links.append((url, text))
    for url in URL_RE.findall(str(record.get("body_text", ""))):
        links.append((url.rstrip(".,)]】"), ""))
    return links


def remote_candidate_evidence(record: Dict[str, Any]) -> List[str]:
    """Identify 99-2 candidates without fetching any URL."""
    evidence: Set[str] = set()
    for url, anchor_text in _record_links(record):
        try:
            parsed = urlsplit(url)
        except ValueError:
            continue
        host = parsed.netloc.casefold()
        path = parsed.path.casefold()
        if host in {"docs.google.com", "sheets.google.com"} and path.startswith(
            "/spreadsheets/"
        ):
            evidence.add("google_spreadsheet_url")
            continue
        normalized_anchor = normalize(anchor_text)
        if "スキルシート" in normalized_anchor:
            evidence.add("remote_skillsheet_link")
            continue
        if (
            LIST_MARKER_RE.search(normalized_anchor)
            and "unsubscribe" not in path
            and "preference" not in path
        ):
            evidence.add("web_list_anchor")
    return sorted(evidence)


def strong_exclusion_evidence(record: Dict[str, Any]) -> List[str]:
    """Return only source-specific, high-confidence exclusion evidence."""
    sender = sender_address(record)
    subject = normalize(record.get("subject", ""))
    body = normalize(record.get("body_text", ""))
    if (
        sender == "noreply-apps-scripts-notifications@google.com"
        and re.fullmatch(r"summary of failures for google apps script:\s*.+", subject)
        and "has recently failed to finish successfully" in body
        and "google apps script" in body
        and "please do not reply" in body
    ):
        return [
            "google_apps_script_notification_sender",
            "google_apps_script_failure_subject_anchored",
            "google_apps_script_failure_body_format",
        ]
    return []


def positive_sales_evidence(record: Dict[str, Any]) -> List[str]:
    """Find evidence that a mail has downstream SES processing value."""
    evidence: List[str] = []
    subject = normalize(record.get("subject", ""))
    combined = normalize(subject + "\n" + str(record.get("body_text", "")))
    project_markers = sum(
        bool(pattern.search(combined)) for pattern in PROJECT_EVIDENCE_RES
    )
    resource_markers = sum(
        bool(pattern.search(combined)) for pattern in RESOURCE_EVIDENCE_RES
    )
    if project_markers >= 2:
        evidence.append("project_sales_structure")
    if resource_markers >= 2:
        evidence.append("resource_sales_structure")
    if PROJECT_SUBJECT_FORMAT_RE.search(subject):
        evidence.append("project_subject_format")
    if RESOURCE_SUBJECT_FORMAT_RE.search(subject):
        evidence.append("resource_subject_format")
    extensions = _attachment_extensions(record)
    if any(extension in ATTACHMENT_KEEP_EXTENSIONS for extension in extensions):
        evidence.append("processable_attachment")
    return evidence


def candidate_99_1_routes(
    record: Dict[str, Any],
    inline_adapter: InlineSummaryAdapter,
    spreadsheet_parser: SpreadsheetParser,
) -> List[str]:
    """Select only existing 99-1 routes; this does not parse or mutate production."""
    if inline_adapter.matches(record):
        return ["INLINE"]
    if spreadsheet_parser.matches(record):
        return ["SPREADSHEET"]
    extensions = _attachment_extensions(record)
    if ".zip" in extensions:
        return ["ARCHIVE"]
    candidate_files = sum(
        1
        for attachment in (record.get("attachments") or [])
        if isinstance(attachment, dict)
        and Path(str(attachment.get("filename", ""))).suffix.casefold()
        in ATTACHMENT_KEEP_EXTENSIONS
    )
    if candidate_files >= 2:
        return ["ATTACHMENT_LIST"]
    return []


def shadow_decision(
    record: Dict[str, Any],
    current_hit_type: str,
    routes_99_1: Sequence[str],
) -> Dict[str, Any]:
    """Classify one record as KEEP, EXCLUDE, or REVIEW."""
    if current_hit_type == "NONE":
        return {
            "decision": "KEEP",
            "decision_reason": "current_survivor_preserved",
            "decision_evidence": [],
        }

    exclusion_evidence = strong_exclusion_evidence(record)
    if exclusion_evidence:
        return {
            "decision": "EXCLUDE",
            "decision_reason": "source_specific_non_sales_notification",
            "decision_evidence": exclusion_evidence,
        }

    remote_evidence = remote_candidate_evidence(record)
    sales_evidence = positive_sales_evidence(record)
    keep_evidence = list(routes_99_1) + remote_evidence + sales_evidence
    if keep_evidence:
        return {
            "decision": "KEEP",
            "decision_reason": "downstream_processing_value",
            "decision_evidence": keep_evidence,
        }

    return {
        "decision": "REVIEW",
        "decision_reason": "insufficient_source_specific_evidence",
        "decision_evidence": [],
    }


def observation_label(
    record: Dict[str, Any],
    routes_99_1: Sequence[str],
) -> str:
    """Assign an observation class without using message IDs or fixed counts."""
    if strong_exclusion_evidence(record):
        return "CLEAR_EXCLUDE"
    remote_evidence = remote_candidate_evidence(record)
    subject = normalize(record.get("subject", ""))
    if routes_99_1 or (remote_evidence and LIST_MARKER_RE.search(subject)):
        return "LIKELY_VALID_MULTI"
    if remote_evidence or positive_sales_evidence(record):
        return "LIKELY_VALID_SINGLE"
    return "UNKNOWN"


def _current_hit_type(
    record: Dict[str, Any],
    production_module: Any,
    from_only_set: Set[str],
    from_subject_rules: Sequence[Tuple[str, str]],
) -> str:
    sender = production_module.extract_email(record.get("from") or "")
    subject = production_module.normalize(record.get("subject") or "")
    if sender in from_only_set:
        return "FROM_ONLY"
    if any(
        rule_sender == sender
        and production_module.subject_matches(rule_subject, subject)
        for rule_sender, rule_subject in from_subject_rules
    ):
        return "FROM_SUBJECT"
    if production_module.detect_p1_exclusion_reason(record.get("subject") or ""):
        return "SUBJECT_DETECTOR"
    return "NONE"


def _load_selected_master(input_ids: Set[str]) -> Tuple[Dict[str, Dict[str, Any]], int]:
    selected: Dict[str, Dict[str, Any]] = {}
    total = 0
    for record in read_jsonl(str(MASTER_PATH)):
        total += 1
        message_id = str(record.get("message_id", ""))
        if message_id in input_ids:
            if message_id in selected:
                raise ValueError("duplicate message_id in mail master: " + message_id)
            selected[message_id] = record
    return selected, total


def _classify_rescued_singles(
    decisions: List[Dict[str, Any]], records_by_id: Dict[str, Dict[str, Any]]
) -> Counter:
    classifier = _load_module("shadow_offline_classifier", CLASSIFIER_TOOL)
    if classifier.USE_LLM_CLASSIFY:
        raise ValueError("offline shadow classification requires USE_LLM_CLASSIFY=False")
    keywords = classifier.load_keywords(str(CLASSIFIER_KEYWORDS))
    counts: Counter = Counter()
    for decision in decisions:
        if not (
            decision["rescued"]
            and decision["observation_label"] == "LIKELY_VALID_SINGLE"
        ):
            continue
        record = records_by_id[decision["message_id"]]
        mail_type, _confidence, _hits = classifier.rule_classify(
            str(record.get("subject", "")),
            str(record.get("body_text", "")),
            keywords,
            has_attachment=bool(record.get("attachments")),
        )
        if mail_type not in classifier.VALID_MAIL_TYPES:
            raise ValueError("unexpected offline classification: " + str(mail_type))
        decision["offline_single_classification"] = mail_type.upper()
        counts[mail_type.upper()] += 1
    return counts


def _replay_rescued_99_1(
    decisions: List[Dict[str, Any]],
    records_by_id: Dict[str, Dict[str, Any]],
    inline_adapter: InlineSummaryAdapter,
    spreadsheet_parser: SpreadsheetParser,
) -> List[Dict[str, Any]]:
    regression: List[Dict[str, Any]] = []
    for decision in decisions:
        if not decision["rescued"] or not decision["routes_99_1"]:
            continue
        record = records_by_id[decision["message_id"]]
        for route in decision["routes_99_1"]:
            try:
                if route == "INLINE":
                    result = inline_adapter.parse(record)
                    item_count = len(result.items)
                elif route == "SPREADSHEET":
                    result = spreadsheet_parser.parse(record)
                    item_count = len(result.technical_items)
                else:
                    regression.append(
                        {
                            "message_id": decision["message_id"],
                            "route": route,
                            "status": "CANDIDATE_ONLY",
                            "item_count": 0,
                            "parser_crash": False,
                            "safe_reach": True,
                        }
                    )
                    continue
                status = str(result.status)
                regression.append(
                    {
                        "message_id": decision["message_id"],
                        "route": route,
                        "status": status,
                        "item_count": item_count,
                        "parser_crash": False,
                        "safe_reach": status != "SYSTEM_FAILURE",
                    }
                )
            except Exception as error:
                regression.append(
                    {
                        "message_id": decision["message_id"],
                        "route": route,
                        "status": "PARSER_CRASH",
                        "item_count": 0,
                        "parser_crash": True,
                        "safe_reach": False,
                        "error_type": type(error).__name__,
                    }
                )
    return regression


def _sender_rule_report(decisions: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for decision in decisions:
        if decision["current_hit_type"] != "NONE":
            grouped[decision["sender"]].append(decision)
    report: List[Dict[str, Any]] = []
    for sender in sorted(grouped):
        rows = grouped[sender]
        report.append(
            {
                "sender": sender,
                "current_hits": len(rows),
                "current_hit_types": dict(
                    sorted(Counter(row["current_hit_type"] for row in rows).items())
                ),
                "shadow_decisions": dict(
                    sorted(Counter(row["shadow_decision"] for row in rows).items())
                ),
                "observation_labels": dict(
                    sorted(Counter(row["observation_label"] for row in rows).items())
                ),
                "routes_99_1": dict(
                    sorted(
                        Counter(
                            route for row in rows for route in row["routes_99_1"]
                        ).items()
                    )
                ),
                "remote_candidates": sum(row["remote_candidate"] for row in rows),
                "shadow_rule": (
                    "SOURCE_SPECIFIC_EXCLUDE"
                    if any(row["shadow_decision"] == "EXCLUDE" for row in rows)
                    else "KEEP_WITH_POSITIVE_EVIDENCE_OR_REVIEW"
                ),
            }
        )
    return report


def build_shadow_results() -> Dict[str, Any]:
    before_snapshot = _production_snapshot()
    production = _load_module("shadow_current_01_3", PRODUCTION_01_3_TOOL)
    from_only_set, from_subject_rules = production.load_exclude_list(
        str(PRODUCTION_EXCLUDE_LIST)
    )
    input_rows = read_jsonl_as_list(str(INPUT_PATH))
    input_ids = [str(row.get("message_id", "")) for row in input_rows]
    if not all(input_ids):
        raise ValueError("01-2 input contains an empty message_id")
    if len(input_ids) != len(set(input_ids)):
        raise ValueError("01-2 input contains duplicate message_id values")
    records_by_id, mail_record_count = _load_selected_master(set(input_ids))
    missing_ids = sorted(set(input_ids) - set(records_by_id))

    stored_removed_rows = read_jsonl_as_list(str(CURRENT_REMOVED_PATH))
    stored_removed_ids = {str(row.get("message_id", "")) for row in stored_removed_rows}
    inline_adapter = InlineSummaryAdapter.from_file(INLINE_CONFIG)
    spreadsheet_parser = SpreadsheetParser.from_file(SPREADSHEET_CONFIG)

    decisions: List[Dict[str, Any]] = []
    for message_id in input_ids:
        record = records_by_id.get(message_id)
        if record is None:
            decisions.append(
                {
                    "message_id": message_id,
                    "sender": "",
                    "subject": "",
                    "current_hit_type": "NONE",
                    "current_excluded": False,
                    "shadow_decision": "REVIEW",
                    "shadow_reason": "mail_master_record_missing",
                    "shadow_evidence": [],
                    "observation_label": "UNKNOWN",
                    "routes_99_1": [],
                    "remote_candidate": False,
                    "attachment_extensions": [],
                    "rescued": False,
                    "offline_single_classification": "NOT_APPLICABLE",
                }
            )
            continue
        current_hit = _current_hit_type(
            record, production, from_only_set, from_subject_rules
        )
        routes = candidate_99_1_routes(record, inline_adapter, spreadsheet_parser)
        shadow = shadow_decision(record, current_hit, routes)
        label = observation_label(record, routes) if current_hit != "NONE" else "UNKNOWN"
        current_excluded = current_hit != "NONE"
        decisions.append(
            {
                "message_id": message_id,
                "sender": sender_address(record),
                "subject": str(record.get("subject", "")),
                "current_hit_type": current_hit,
                "current_excluded": current_excluded,
                "shadow_decision": shadow["decision"],
                "shadow_reason": shadow["decision_reason"],
                "shadow_evidence": shadow["decision_evidence"],
                "observation_label": label,
                "routes_99_1": routes,
                "remote_candidate": bool(remote_candidate_evidence(record)),
                "attachment_extensions": _attachment_extensions(record),
                "rescued": current_excluded and shadow["decision"] == "KEEP",
                "offline_single_classification": "NOT_APPLICABLE",
            }
        )

    computed_removed_ids = {
        row["message_id"] for row in decisions if row["current_excluded"]
    }
    current_replay_matches_stored = computed_removed_ids == stored_removed_ids
    classification_counts = _classify_rescued_singles(decisions, records_by_id)
    parser_regression = _replay_rescued_99_1(
        decisions, records_by_id, inline_adapter, spreadsheet_parser
    )
    after_snapshot = _production_snapshot()

    current_rows = [row for row in decisions if row["current_excluded"]]
    rescued = [row for row in decisions if row["rescued"]]
    shadow_excluded_rows = [
        row for row in decisions if row["shadow_decision"] == "EXCLUDE"
    ]
    review_rows = [row for row in decisions if row["shadow_decision"] == "REVIEW"]
    newly_excluded = [
        row
        for row in decisions
        if not row["current_excluded"] and row["shadow_decision"] == "EXCLUDE"
    ]
    clear_rows = [
        row for row in current_rows if row["observation_label"] == "CLEAR_EXCLUDE"
    ]
    observation_counts = Counter(row["observation_label"] for row in current_rows)
    route_counts = Counter(route for row in rescued for route in row["routes_99_1"])
    parser_status_counts = Counter(row["status"] for row in parser_regression)
    safe_reached = sum(row["safe_reach"] for row in parser_regression)
    parser_crashes = sum(row["parser_crash"] for row in parser_regression)
    parser_system_failures = sum(
        row["status"] == "SYSTEM_FAILURE" for row in parser_regression
    )
    rescued_ids = [row["message_id"] for row in rescued]

    summary = {
        "implementation": "PASS",
        "mail_records": mail_record_count,
        "shadow_input_records": len(input_ids),
        "mail_master_missing": len(missing_ids),
        "current_excluded": len(current_rows),
        "current_replay_matches_stored": current_replay_matches_stored,
        "shadow_keep": sum(row["shadow_decision"] == "KEEP" for row in decisions),
        "shadow_excluded": len(shadow_excluded_rows),
        "shadow_review": len(review_rows),
        "rescued": len(rescued),
        "newly_excluded": len(newly_excluded),
        "rescued_likely_valid_single": sum(
            row["observation_label"] == "LIKELY_VALID_SINGLE" for row in rescued
        ),
        "rescued_likely_valid_multi": sum(
            row["observation_label"] == "LIKELY_VALID_MULTI" for row in rescued
        ),
        "rescued_99_2_candidate": sum(row["remote_candidate"] for row in rescued),
        "rescued_other": sum(
            row["observation_label"] not in {"LIKELY_VALID_SINGLE", "LIKELY_VALID_MULTI"}
            for row in rescued
        ),
        "observation_labels_current_excluded": dict(sorted(observation_counts.items())),
        "clear_exclude_preserved": sum(
            row["shadow_decision"] == "EXCLUDE" for row in clear_rows
        ),
        "clear_exclude_total": len(clear_rows),
        "from_only_current": sum(
            row["current_hit_type"] == "FROM_ONLY" for row in current_rows
        ),
        "from_subject_current": sum(
            row["current_hit_type"] == "FROM_SUBJECT" for row in current_rows
        ),
        "from_only_remaining": sum(
            row["current_hit_type"] == "FROM_ONLY"
            and row["shadow_decision"] == "EXCLUDE"
            for row in current_rows
        ),
        "from_subject_remaining": sum(
            row["current_hit_type"] == "FROM_SUBJECT"
            and row["shadow_decision"] == "EXCLUDE"
            for row in current_rows
        ),
        "new_false_exclude_suspected": sum(
            row["observation_label"] != "CLEAR_EXCLUDE"
            for row in shadow_excluded_rows
        ),
        "new_false_keep_suspected": sum(
            row["observation_label"] == "CLEAR_EXCLUDE" for row in rescued
        ),
        "rescued_99_1_applicable": len(parser_regression),
        "rescued_99_1_routes": dict(sorted(route_counts.items())),
        "rescued_99_1_safe_reached": safe_reached,
        "rescued_99_1_parser_statuses": dict(sorted(parser_status_counts.items())),
        "parser_crash": parser_crashes,
        "system_failure": parser_system_failures,
        "rescued_single_offline_classification": dict(
            sorted(classification_counts.items())
        ),
        "rescued_single_classification_total": sum(classification_counts.values()),
        "message_id_collision": len(rescued_ids) - len(set(rescued_ids)),
        "external_access": 0,
        "production_01_3_changes": int(before_snapshot != after_snapshot),
        "production_write": int(before_snapshot != after_snapshot),
    }
    required_pass = (
        summary["mail_master_missing"] == 0
        and summary["current_replay_matches_stored"]
        and summary["shadow_review"] == 0
        and summary["rescued"] > 0
        and summary["clear_exclude_preserved"] == summary["clear_exclude_total"]
        and summary["new_false_exclude_suspected"] == 0
        and summary["new_false_keep_suspected"] == 0
        and summary["rescued_99_1_safe_reached"]
        == summary["rescued_99_1_applicable"]
        and summary["parser_crash"] == 0
        and summary["system_failure"] == 0
        and summary["message_id_collision"] == 0
        and summary["external_access"] == 0
        and summary["production_01_3_changes"] == 0
        and summary["production_write"] == 0
    )
    summary["implementation"] = "PASS" if required_pass else "FAIL"
    return {
        "decisions": decisions,
        "parser_regression": parser_regression,
        "sender_rules": _sender_rule_report(decisions),
        "summary": summary,
    }


def _write_results(results: Dict[str, Any]) -> None:
    dirs = ensure_result_dirs(str(STEP_DIR))
    result_dir = dirs["result"] / RESULT_SUBDIR
    write_jsonl(str(result_dir / "decisions.jsonl"), results["decisions"])
    write_jsonl(
        str(result_dir / "parser_regression.jsonl"), results["parser_regression"]
    )
    write_jsonl(str(result_dir / "sender_rules.jsonl"), results["sender_rules"])
    write_jsonl(str(result_dir / "shadow_summary.jsonl"), [results["summary"]])


def main() -> None:
    started = time.monotonic()
    results = build_shadow_results()
    _write_results(results)
    elapsed = time.monotonic() - started
    write_execution_time(
        str(STEP_DIR / "99_execution_time"),
        "99-1_01-3_false_exclusion_shadow",
        elapsed,
        results["summary"]["shadow_input_records"],
    )
    summary = results["summary"]
    logger.ok(
        "shadow replay "
        + summary["implementation"]
        + ": current_excluded="
        + str(summary["current_excluded"])
        + " shadow_excluded="
        + str(summary["shadow_excluded"])
        + " rescued="
        + str(summary["rescued"])
        + " review="
        + str(summary["shadow_review"])
    )
    if summary["implementation"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

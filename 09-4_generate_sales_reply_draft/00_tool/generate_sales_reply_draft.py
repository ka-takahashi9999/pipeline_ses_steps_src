"""
09-4_generate_sales_reply_draft
09-3 の構造化コンテキストから営業メールドラフトと preview を生成する。
"""

import math
import re
import shutil
import sys
import time
import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "09-4_generate_sales_reply_draft"
STEP_DIR = Path(__file__).resolve().parents[1]
INPUT_BASE_DIR = project_root / "09-3_prepare_sales_mail_context/01_result"
OUTPUT_FILE_TEMPLATE = "generate_sales_reply_draft_{date}.jsonl"
PREVIEW_DIR_TEMPLATE = "reply_preview_{date}"

MIN_PROFIT = 120000
ROUND_UNIT = 10000
DEFAULT_SENDER_NAME = "XXXXXX<名前>XXXXXX"
PAIR_TYPE = "project_resource"
MAIL_MODE_REPLY_PROJECT = "reply_to_project"
MAIL_MODE_REPLY_RESOURCE = "reply_to_resource"
RECHECK_HUMAN_REVIEW_NOTE = "必須スキルに人間確認項目があります"
RECHECK_NOT_CONFIRMED_NOTE = "必須スキル未確認項目があります"
START_DATE_INVALID_LABEL_PATTERN = re.compile(
    r"^(?:条件|備考|面談|精算|場所|勤務地|作業場所|単価|外国籍|商流|募集人数|再委託|作業期間)\s*$"
)
LABEL_DECORATION_CLASS = r"[■◇●・*\-◎○〇◆▼□]"


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def first_non_empty(*values: Any) -> Optional[Any]:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        if isinstance(value, list) and len(value) == 0:
            continue
        return value
    return None


def normalize_start_date_for_display(value: Any) -> Optional[str]:
    text = normalize_text(value).strip(" :：")
    if not text:
        return None
    compact = re.sub(r"[\s:：]", "", text)
    compact = re.sub(r"[■◇●・*\-◎○〇◆▼□]", "", compact)
    if not compact:
        return None
    if START_DATE_INVALID_LABEL_PATTERN.fullmatch(compact):
        return None
    return text


def strip_label_artifacts_for_display(value: Any, prefixes: List[str]) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    text = re.sub(rf"^(?:{LABEL_DECORATION_CLASS}\s*)+", "", text)
    for prefix in prefixes:
        text = re.sub(rf"^{re.escape(prefix)}\s*[:：]?\s*", "", text)
    text = re.sub(rf"(?:{LABEL_DECORATION_CLASS}\s*)+$", "", text)
    return text.strip(" :：")


def normalize_available_date_for_display(value: Any) -> Optional[str]:
    text = strip_label_artifacts_for_display(value, ["稼働開始日", "稼動開始日", "稼働", "稼動", "開始日", "参画可能日"])
    compact = re.sub(r"[\s:：]", "", text)
    compact = re.sub(LABEL_DECORATION_CLASS, "", compact)
    if not compact or compact in {"日", "開始日", "稼働", "稼動", "参画可能日", "開始"}:
        return None
    return text


def normalize_settlement_for_display(value: Any) -> Optional[str]:
    text = strip_label_artifacts_for_display(value, ["精算条件", "精算幅", "精算", "条件"])
    compact = re.sub(r"[\s:：]", "", text)
    compact = re.sub(LABEL_DECORATION_CLASS, "", compact)
    if not compact or compact in {"条件", "精算", "精算条件", "精算幅"}:
        return None
    return text


def normalize_foreign_restriction_for_display(value: Any) -> Optional[str]:
    text = strip_label_artifacts_for_display(value, ["外国籍条件", "外国籍"])
    compact = re.sub(r"[\s:：]", "", text)
    compact = re.sub(LABEL_DECORATION_CLASS, "", compact)
    if compact == "不可":
        return "外国籍不可"
    if compact == "可":
        return "外国籍可"
    if compact in {"外国籍不可", "外国籍可"}:
        return compact
    return None


def normalize_parallel_status_for_display(value: Any) -> Optional[str]:
    text = strip_label_artifacts_for_display(value, ["並行状況", "並行"])
    compact = re.sub(r"[\s:：]", "", text)
    compact = re.sub(LABEL_DECORATION_CLASS, "", compact)
    if compact in {"提案中", "面談調整中", "面談予定"}:
        return "並行" + compact
    if compact in {"並行提案中", "並行面談調整中", "並行面談予定"}:
        return compact
    return None


def count_missing_major_fields(values: List[Optional[Any]]) -> int:
    return sum(1 for value in values if value in (None, "", []))


def find_latest_input() -> Tuple[Path, str]:
    candidates = sorted(INPUT_BASE_DIR.glob("prepare_sales_mail_context_*.jsonl"))
    if not candidates:
        raise FileNotFoundError(f"09-3入力JSONLが存在しません: {INPUT_BASE_DIR}")
    latest = candidates[-1]
    date_part = latest.stem.replace("prepare_sales_mail_context_", "", 1)
    if not re.fullmatch(r"\d{8}", date_part):
        raise ValueError(f"入力日付を解釈できません: {latest.name}")
    return latest, date_part


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-date", help="処理対象日付 YYYYMMDD")
    return parser.parse_args()


def resolve_target_date(target_date: Optional[str]) -> Optional[str]:
    if target_date is None:
        return None
    if not re.fullmatch(r"\d{8}", target_date):
        raise ValueError(f"--target-date は YYYYMMDD 形式で指定してください: {target_date}")
    return target_date


def resolve_input(target_date: Optional[str]) -> Tuple[Path, str, str]:
    if target_date:
        input_path = INPUT_BASE_DIR / f"prepare_sales_mail_context_{target_date}.jsonl"
        if not input_path.exists():
            raise FileNotFoundError(f"対象日付の09-3入力JSONLが存在しません: {input_path}")
        return input_path, target_date, "target-date"
    latest, date_part = find_latest_input()
    return latest, date_part, "latest"


def round_down_to_unit(value: int, unit: int = ROUND_UNIT) -> int:
    return int(math.floor(value / unit) * unit)


def format_currency(value: Optional[int]) -> str:
    if value is None:
        return "未設定"
    return f"{int(value / 10000)}万円"


def format_body_currency(value: Optional[int]) -> str:
    if value is None:
        return "-"
    return format_currency(value)


def display_text(value: Any) -> str:
    return normalize_text(first_non_empty(value, "-"))


def parse_recipients(values: Any) -> List[str]:
    if values is None:
        return []
    items = values if isinstance(values, list) else [values]
    recipients: List[str] = []
    for item in items:
        text = normalize_text(item)
        if not text:
            continue
        recipients.append(text)
    return recipients


def dedupe_list(values: List[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values:
        key = normalize_text(value).lower()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(normalize_text(value))
    return result


def choose_mail_mode(record: Dict[str, Any]) -> str:
    return MAIL_MODE_REPLY_PROJECT


def build_to_cc(record: Dict[str, Any], mail_mode: str) -> Tuple[List[str], List[str], List[str]]:
    review_notes: List[str] = []
    if mail_mode == MAIL_MODE_REPLY_PROJECT:
        to_source = record.get("project_reply_target_source")
        to_list = dedupe_list(
            parse_recipients(
                first_non_empty(
                    record.get("project_reply_to_candidates"),
                    record.get("project_reply_to"),
                    record.get("project_sender_email"),
                )
            )
        )
        cc_list = dedupe_list(parse_recipients(first_non_empty(record.get("project_cc_candidates"), record.get("project_cc"))))
        if str(to_source).startswith("body") and not record.get("project_reply_to_candidates"):
            review_notes.append("案件側本文指示を優先すべきですがTO候補が空です")
        if record.get("project_cc_candidates") and not cc_list:
            review_notes.append("案件側本文CC指示がありますがCCが空です")
    else:
        to_source = record.get("resource_reply_target_source")
        to_list = dedupe_list(
            parse_recipients(
                first_non_empty(
                    record.get("resource_reply_to_candidates"),
                    record.get("resource_reply_to"),
                    record.get("resource_sender_email"),
                )
            )
        )
        cc_list = dedupe_list(parse_recipients(first_non_empty(record.get("resource_cc_candidates"), record.get("resource_cc"))))
        if str(to_source).startswith("body") and not record.get("resource_reply_to_candidates"):
            review_notes.append("要員側本文指示を優先すべきですがTO候補が空です")
        if record.get("resource_cc_candidates") and not cc_list:
            review_notes.append("要員側本文CC指示がありますがCCが空です")
    if not to_list:
        review_notes.append("宛先候補を特定できませんでした")
    return to_list, cc_list, review_notes


def build_salutation(company: Optional[str], person: Optional[str]) -> str:
    company_text = normalize_text(company)
    person_text = normalize_text(person)
    if person_text and company_text and person_text != company_text:
        return f"{company_text}\n{person_text}様"
    if company_text:
        return f"{company_text}\nご担当者様"
    if person_text:
        return f"{person_text}様"
    return "ご担当者様"


def compute_price_plan(record: Dict[str, Any], mail_mode: str) -> Tuple[Optional[int], Dict[str, Any], List[str]]:
    project_budget = record.get("project_budget")
    resource_price = record.get("resource_desired_unit_price")
    review_notes: List[str] = []
    if not isinstance(project_budget, int) or not isinstance(resource_price, int):
        review_notes.append("単価情報が不足しているため提案単価を算出できませんでした")
        return None, {
            "project_budget": project_budget,
            "resource_desired_unit_price": resource_price,
            "profit_amount": None,
            "basis": "単価情報不足",
        }, review_notes

    gap = project_budget - resource_price
    if gap <= 0:
        review_notes.append("案件単価と要員希望単価の差額が不足しています")
        return None, {
            "project_budget": project_budget,
            "resource_desired_unit_price": resource_price,
            "profit_amount": gap,
            "basis": "差額不足",
        }, review_notes

    target_profit = round_down_to_unit(int(gap * 0.9))
    if target_profit < MIN_PROFIT:
        target_profit = MIN_PROFIT
    if target_profit > gap:
        target_profit = gap

    if gap < MIN_PROFIT:
        review_notes.append("想定利益が12万円未満です")

    if mail_mode == MAIL_MODE_REPLY_PROJECT:
        suggested_price = resource_price + target_profit
        if suggested_price > project_budget:
            suggested_price = project_budget
            target_profit = project_budget - resource_price
            review_notes.append("案件上限単価に合わせて提案単価を調整しました")
    else:
        suggested_price = project_budget - target_profit
        if suggested_price < resource_price:
            suggested_price = resource_price
            target_profit = project_budget - resource_price
            review_notes.append("要員希望単価を下回らないように提案単価を調整しました")

    basis = (
        f"案件単価{format_currency(project_budget)} / 要員希望{format_currency(resource_price)} / "
        f"想定利益{format_currency(target_profit)}"
    )
    return suggested_price, {
        "project_budget": project_budget,
        "resource_desired_unit_price": resource_price,
        "profit_amount": target_profit,
        "basis": basis,
    }, review_notes


def build_skill_match_mark(skill_text: str, keywords: List[str], haystack: str) -> str:
    normalized_skill = skill_text.lower()
    for keyword in keywords:
        if keyword and keyword in normalized_skill and keyword in haystack:
            return "○"
    for token in re.split(r"[、,/()\s]+", normalized_skill):
        token = token.strip()
        if len(token) >= 2 and token in haystack:
            return "○"
    return "×"


def make_plain_skill_lines(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    lines: List[str] = []
    for value in values:
        skill_text = normalize_text(value.get("skill") if isinstance(value, dict) else value)
        if skill_text:
            lines.append(f"・{skill_text}")
    return lines


def make_skill_check_lines(record: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    lines.append("【必須】")
    required_skill_checks = record.get("required_skill_checks")
    if isinstance(required_skill_checks, list) and required_skill_checks:
        for check in required_skill_checks:
            if not isinstance(check, dict):
                continue
            skill_text = normalize_text(check.get("skill"))
            if not skill_text:
                continue
            confidence = check.get("confidence")
            if confidence == "confirmed":
                lines.append(f"・[○]{skill_text}")
            elif confidence == "not_confirmed":
                lines.append(f"・[×]{skill_text}")
            else:
                lines.append(f"・[△]{skill_text}（要確認）")
            if len(lines) >= 9:
                break
    else:
        required_skills = record.get("project_required_skills") or []
        keywords = [normalize_text(v).lower() for v in (record.get("project_required_skill_keywords") or []) if normalize_text(v)]
        resource_skills_text = " ".join([normalize_text(v) for v in (record.get("resource_skills") or [])]).lower()
        resource_summary_text = normalize_text(record.get("resource_skill_summary")).lower()
        haystack = resource_skills_text + " " + resource_summary_text
        for index, skill in enumerate(required_skills, 1):
            skill_text = normalize_text(skill)
            if not skill_text:
                continue
            mark = build_skill_match_mark(skill_text, keywords, haystack)
            lines.append(f"・[{mark}]{skill_text}")
            if index >= 8:
                break
    if len(lines) == 1:
        lines.append("・[△]詳細はご確認ベースでお願いします")

    preferred_skills = record.get("project_preferred_skills") or []
    optional_keywords = [
        normalize_text(v).lower()
        for v in (record.get("project_optional_skill_keywords") or [])
        if normalize_text(v)
    ]
    resource_skills_text = " ".join([normalize_text(v) for v in (record.get("resource_skills") or [])]).lower()
    resource_summary_text = normalize_text(record.get("resource_skill_summary")).lower()
    haystack = resource_skills_text + " " + resource_summary_text
    lines.extend(["", "【尚可】"])
    optional_count = 0
    for skill in preferred_skills:
        skill_text = normalize_text(skill)
        if not skill_text:
            continue
        mark = build_skill_match_mark(skill_text, optional_keywords, haystack)
        lines.append(f"・[{mark}]{skill_text}")
        optional_count += 1
        if optional_count >= 8:
            break
    if optional_count == 0:
        lines.append("・-")
    return lines


def format_remote_type(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return "-"
    mapping = {
        "onsite": "常駐",
        "fullremote": "フルリモート",
        "full_remote": "フルリモート",
        "remote": "フルリモート",
        "hybrid": "リモート併用",
    }
    return mapping.get(text.lower(), text)


def append_recheck_review_notes(record: Dict[str, Any], review_notes: List[str]) -> None:
    recheck_status = record.get("required_skill_recheck_status")
    required_skill_checks = record.get("required_skill_checks")
    has_human_review_check = False
    has_not_confirmed_check = False
    if isinstance(required_skill_checks, list):
        for check in required_skill_checks:
            if not isinstance(check, dict):
                has_human_review_check = True
                continue
            confidence = check.get("confidence")
            if confidence == "not_confirmed":
                has_not_confirmed_check = True
            elif confidence != "confirmed":
                has_human_review_check = True

    if recheck_status == "required_skill_not_confirmed" or has_not_confirmed_check:
        review_notes.append(RECHECK_NOT_CONFIRMED_NOTE)
    if recheck_status == "required_skill_human_review" or has_human_review_check:
        review_notes.append(RECHECK_HUMAN_REVIEW_NOTE)


def build_project_reply_subject(record: Dict[str, Any]) -> str:
    subject = normalize_text(record.get("project_subject"))
    resource_summary = first_non_empty(record.get("resource_skill_summary"), record.get("resource_subject"))
    if subject:
        return f"Re: {subject}"
    if resource_summary:
        return f"【要員提案】{normalize_text(resource_summary)[:50]}"
    return "【要員提案】ご紹介"


def build_resource_reply_subject(record: Dict[str, Any]) -> str:
    subject = normalize_text(record.get("resource_subject"))
    project_summary = first_non_empty(record.get("project_subject"), record.get("project_location"))
    if subject:
        return f"Re: {subject}"
    if project_summary:
        return f"【案件提案】{normalize_text(project_summary)[:50]}"
    return "【案件提案】ご紹介"


def render_project_reply(
    record: Dict[str, Any],
    salutation: str,
    suggested_price: Optional[int],
    skill_check_lines: List[str],
) -> str:
    available_date = normalize_available_date_for_display(record.get("resource_available_date"))
    parallel_status = normalize_parallel_status_for_display(record.get("suggested_parallel_status"))
    lines: List[str] = [
        salutation,
        "",
        f"お世話になっております。株式会社テクノヴァースの{DEFAULT_SENDER_NAME}です。",
        "",
        "本件に関して、以下要員を提案いたします。",
        "ご確認のうえ、問題なければエントリー可否をご教示ください。",
        "",
        "【スキルチェック】",
    ]
    lines.extend(skill_check_lines or ["【必須】", "・[△]詳細はご確認ベースでお願いします", "", "【尚可】", "・-"])
    lines.extend(
        [
        "",
        "【要員情報】",
        f"・所属: {display_text(record.get('suggested_affiliation_label'))}",
        f"・年齢: {display_text(str(record.get('resource_age')) + '歳' if record.get('resource_age') is not None else None)}",
        f"・最寄駅: {display_text(record.get('resource_nearest_station'))}",
        f"・稼働開始: {display_text(available_date)}",
        f"・単価: {format_body_currency(suggested_price)}",
        f"・並行状況: {display_text(first_non_empty(parallel_status, '並行提案中'))}",
        f"・スキル概要: {display_text(record.get('resource_skill_summary'))}",
        ]
    )
    if record.get("resource_notes"):
        lines.extend(["", "【備考】", normalize_text(record.get("resource_notes"))])
    lines.extend(
        [
            "",
            "以上、よろしくお願いいたします。",
            "",
            "----- 引用 -----",
            normalize_text(record.get("quoted_project_mail_body")),
        ]
    )
    return "\n".join(lines).strip()


def render_resource_reply(
    record: Dict[str, Any],
    salutation: str,
    suggested_price: Optional[int],
    skill_check_lines: List[str],
) -> str:
    settlement = normalize_settlement_for_display(record.get("project_settlement"))
    foreign_restriction = normalize_foreign_restriction_for_display(record.get("project_foreign_restriction"))
    required_skill_lines = make_plain_skill_lines(record.get("project_required_skills"))
    preferred_skill_lines = make_plain_skill_lines(record.get("project_preferred_skills"))
    lines: List[str] = [
        salutation,
        "",
        f"お世話になっております。株式会社テクノヴァースの{DEFAULT_SENDER_NAME}です。",
        "",
        "本件に関して、以下案件を提案いたします。",
        "ご興味がございましたら、エントリー希望可否と補足事項をご教示ください。",
        "",
        "弊社にてスキルチェックを実施しました。認識相違や補足があればご教示ください。",
        "【スキルチェック】",
    ]
    if skill_check_lines:
        lines.extend(skill_check_lines)
    else:
        lines.extend(["【必須】", "・[△]詳細はご確認ベースでお願いします", "", "【尚可】", "・-"])
    lines.extend(
        [
            "",
            "【案件情報】",
            f"・案件名: {display_text(record.get('project_subject'))}",
            f"・開始時期: {display_text(normalize_start_date_for_display(record.get('project_start_date')))}",
            f"・勤務地: {display_text(record.get('project_location'))}",
            f"・勤務形態: {format_remote_type(record.get('project_remote_type'))}",
            f"・単価: {format_body_currency(suggested_price)}",
            f"・面談: {display_text(record.get('project_interview_count'))}",
            f"・精算: {display_text(settlement)}",
            f"・外国籍条件: {display_text(foreign_restriction)}",
            "",
            "【必須スキル】",
        ]
    )
    lines.extend(required_skill_lines or ["・-"])
    lines.extend(["", "【尚可スキル】"])
    lines.extend(preferred_skill_lines or ["・-"])
    if record.get("project_notes"):
        lines.extend(["", "【備考】", normalize_text(record.get("project_notes"))])
    lines.extend(
        [
            "",
            "以上、よろしくお願いいたします。",
            "",
            "----- 引用 -----",
            normalize_text(record.get("quoted_resource_mail_body")),
        ]
    )
    return "\n".join(lines).strip()


def refine_mail_text(draft_text: str) -> Tuple[str, bool, List[str]]:
    refined_lines: List[str] = []
    prev_blank = False
    changed = False
    for line in draft_text.splitlines():
        stripped = line.rstrip()
        if stripped == "":
            if prev_blank:
                changed = True
                continue
            prev_blank = True
            refined_lines.append("")
            continue
        prev_blank = False
        refined_lines.append(stripped)
    refined_text = "\n".join(refined_lines).strip()
    if refined_text != draft_text.strip():
        return refined_text, False, ["改行の重複を整理"]
    return draft_text.strip(), False, []


def build_preview_text(
    record: Dict[str, Any],
    to_list: List[str],
    cc_list: List[str],
    reply_subject: str,
    refined_mail_text: str,
    ai_refined: bool,
    review_notes: List[str],
) -> str:
    lines = [
        f"TO: {', '.join(to_list)}",
        f"CC: {', '.join(cc_list)}",
        f"SUBJECT: {reply_subject}",
        "",
        refined_mail_text,
    ]
    return "\n".join(lines).strip() + "\n"


def build_note_text(
    record: Dict[str, Any],
    ai_refined: bool,
    review_notes: List[str],
) -> str:
    lines = [
        f"PAIR FILE: {record.get('pair_file_name')}",
        f"SCORE BAND: {record.get('score_band')}",
        f"PAIR TYPE: {record.get('pair_type')}",
        f"MAIL MODE: {record.get('mail_mode')}",
        f"CATEGORY MATCH: {record.get('category_match', 'unclear')}",
        f"CATEGORY NOTE: {record.get('category_note', '')}",
        f"AI REFINED: {ai_refined}",
        f"NEEDS HUMAN REVIEW: {record.get('needs_human_review')}",
        f"REVIEW NOTES: {' | '.join(review_notes)}",
        f"suggested_price_basis: {record.get('suggested_price_basis')}",
    ]
    return "\n".join(lines).strip() + "\n"


def generate_record(record: Dict[str, Any], mail_mode: str) -> Tuple[Dict[str, Any], str, str, str, str]:
    to_list, cc_list, recipient_review_notes = build_to_cc(record, mail_mode)
    suggested_price, price_plan, price_review_notes = compute_price_plan(record, mail_mode)

    review_notes = list(record.get("review_notes") or [])
    review_notes.extend(recipient_review_notes)
    review_notes.extend(price_review_notes)
    append_recheck_review_notes(record, review_notes)
    available_date = normalize_available_date_for_display(record.get("resource_available_date"))
    settlement = normalize_settlement_for_display(record.get("project_settlement"))
    foreign_restriction = normalize_foreign_restriction_for_display(record.get("project_foreign_restriction"))
    parallel_status = normalize_parallel_status_for_display(record.get("suggested_parallel_status"))

    if mail_mode == MAIL_MODE_REPLY_PROJECT:
        salutation = build_salutation(record.get("project_sender_company"), record.get("project_sender_name"))
        reply_subject = build_project_reply_subject(record)
        skill_check_lines = make_skill_check_lines(record)
        draft_text = render_project_reply(record, salutation, suggested_price, skill_check_lines)
    else:
        salutation = build_salutation(record.get("resource_sender_company"), record.get("resource_sender_name"))
        reply_subject = build_resource_reply_subject(record)
        skill_check_lines = make_skill_check_lines(record)
        draft_text = render_resource_reply(record, salutation, suggested_price, skill_check_lines)

    refined_text, ai_refined, refine_points = refine_mail_text(draft_text)

    if not to_list:
        review_notes.append("TOが空です")
    if not reply_subject:
        review_notes.append("件名が空です")
    if not refined_text:
        review_notes.append("本文が空です")
    if suggested_price is None:
        review_notes.append("提案単価が未設定です")
    if not record.get("suggested_affiliation_label"):
        review_notes.append("所属表現候補が未設定です")
    if record.get("resource_available_date") and not available_date:
        review_notes.append("稼働開始の崩れを検出しました")
    if record.get("project_settlement") and not settlement:
        review_notes.append("精算の崩れを検出しました")
    if record.get("project_foreign_restriction") and not foreign_restriction:
        review_notes.append("外国籍条件の崩れを検出しました")
    if record.get("suggested_parallel_status") and not parallel_status:
        review_notes.append("並行状況の崩れを検出しました")
    if mail_mode == MAIL_MODE_REPLY_PROJECT:
        missing_major = count_missing_major_fields(
            [
                record.get("resource_nearest_station"),
                available_date,
                record.get("resource_skill_summary"),
                record.get("resource_desired_unit_price"),
                suggested_price,
                parallel_status,
            ]
        )
        if missing_major >= 3:
            review_notes.append("要員情報の主要項目が未確認/未設定過多です")
    else:
        missing_major = count_missing_major_fields(
            [
                normalize_start_date_for_display(record.get("project_start_date")),
                record.get("project_location"),
                settlement,
                foreign_restriction,
                record.get("project_budget"),
                suggested_price,
            ]
        )
        if missing_major >= 3:
            review_notes.append("案件情報の主要項目が未確認/未設定過多です")

    review_notes = list(dict.fromkeys(review_notes))

    output_record = {
        "pair_file_name": record.get("pair_file_name"),
        "score_band": record.get("score_band"),
        "pair_type": record.get("pair_type", PAIR_TYPE),
        "draft_direction": mail_mode,
        "mail_mode": mail_mode,
        "reply_target_source": (
            record.get("project_reply_target_source")
            if mail_mode == MAIL_MODE_REPLY_PROJECT
            else record.get("resource_reply_target_source")
        ),
        "to_recipients": to_list,
        "cc_recipients": cc_list,
        "reply_to_candidates": (
            record.get("project_reply_to_candidates")
            if mail_mode == MAIL_MODE_REPLY_PROJECT
            else record.get("resource_reply_to_candidates")
        ),
        "cc_candidates": (
            record.get("project_cc_candidates")
            if mail_mode == MAIL_MODE_REPLY_PROJECT
            else record.get("resource_cc_candidates")
        ),
        "reply_subject": reply_subject,
        "draft_mail_text": draft_text,
        "refined_mail_text": refined_text,
        "ai_refined": ai_refined,
        "refine_points": refine_points,
        "suggested_price": suggested_price,
        "suggested_price_basis": {
            "project_budget": price_plan["project_budget"],
            "resource_desired_unit_price": price_plan["resource_desired_unit_price"],
            "profit_amount": price_plan["profit_amount"],
            "basis": price_plan["basis"],
        },
        "suggested_affiliation_label": record.get("suggested_affiliation_label"),
        "suggested_parallel_status": record.get("suggested_parallel_status"),
        "required_skill_recheck_status": record.get("required_skill_recheck_status"),
        "required_skill_recheck_info": record.get("required_skill_recheck_info"),
        "required_skill_checks": record.get("required_skill_checks"),
        "category_match": record.get("category_match", "unclear"),
        "category_note": record.get("category_note", ""),
        "needs_human_review": len(review_notes) > 0,
        "review_notes": review_notes,
        "project_message_id": record.get("project_message_id"),
        "resource_message_id": record.get("resource_message_id"),
    }
    preview_file_name = record.get("pair_file_name", "").replace(".txt", f"_{mail_mode}.txt")
    note_file_name = record.get("pair_file_name", "").replace(".txt", f"_{mail_mode}_note.txt")
    output_record["preview_file_name"] = preview_file_name
    output_record["note_file_name"] = note_file_name
    preview_text = build_preview_text(
        {**record, "mail_mode": mail_mode, "needs_human_review": output_record["needs_human_review"]},
        to_list,
        cc_list,
        reply_subject,
        refined_text,
        ai_refined,
        review_notes,
    )
    note_text = build_note_text(
        {
            **record,
            "mail_mode": mail_mode,
            "needs_human_review": output_record["needs_human_review"],
            "suggested_price_basis": output_record["suggested_price_basis"],
        },
        ai_refined,
        review_notes,
    )
    return output_record, preview_text, note_text, preview_file_name, note_file_name


def main() -> None:
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()
    args = parse_args()

    try:
        target_date = resolve_target_date(args.target_date)
        input_path, date_part, resolve_mode = resolve_input(target_date)
        output_path = STEP_DIR / "01_result" / OUTPUT_FILE_TEMPLATE.format(date=date_part)
        preview_dir = STEP_DIR / "01_result" / PREVIEW_DIR_TEMPLATE.format(date=date_part)
        logger.info(f"input resolve mode: {resolve_mode}")
        logger.info(f"target date: {date_part}")
        logger.info(f"09-3 input path: {input_path}")

        if preview_dir.exists():
            shutil.rmtree(preview_dir)
        preview_dir.mkdir(parents=True, exist_ok=True)

        input_records = read_jsonl_as_list(str(input_path))
        output_records: List[Dict[str, Any]] = []

        for record in input_records:
            for mail_mode in [MAIL_MODE_REPLY_PROJECT, MAIL_MODE_REPLY_RESOURCE]:
                output_record, preview_text, note_text, preview_file_name, note_file_name = generate_record(record, mail_mode)
                output_records.append(output_record)
                score_band = normalize_text(output_record.get("score_band")) or "unknown"
                band_preview_dir = preview_dir / score_band
                band_preview_dir.mkdir(parents=True, exist_ok=True)
                note_subdir = band_preview_dir / "note"
                note_subdir.mkdir(parents=True, exist_ok=True)
                preview_path = band_preview_dir / preview_file_name
                note_path = note_subdir / note_file_name
                output_record["preview_file_path"] = str(preview_path.relative_to(STEP_DIR / "01_result"))
                output_record["note_file_path"] = str(note_path.relative_to(STEP_DIR / "01_result"))
                preview_path.write_text(preview_text, encoding="utf-8")
                note_path.write_text(note_text, encoding="utf-8")

        write_jsonl(str(output_path), output_records)
        elapsed = time.time() - start_time
        write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, len(output_records))
        logger.ok(f"ドラフトJSONL出力完了: {output_path} ({len(output_records)}件)")
        logger.ok(f"preview出力完了: {preview_dir}")

    except Exception as error:
        write_error_log(str(dirs["result"]), error, context=STEP_NAME)
        logger.error(f"処理失敗: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()

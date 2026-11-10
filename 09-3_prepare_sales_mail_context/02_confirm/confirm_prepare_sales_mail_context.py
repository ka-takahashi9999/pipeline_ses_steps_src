"""
09-3_prepare_sales_mail_context confirm
高確度pair件数と出力JSONLの整合、および主要キー/sourceの有無を確認する。
"""

import argparse
import json
import re
import sys
from pathlib import Path
from collections import Counter
from typing import Dict, List, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.json_utils import read_jsonl_as_list
from common.logger import get_logger

STEP_NAME = "09-3_prepare_sales_mail_context_confirm"
STEP_DIR = Path(__file__).resolve().parents[1]
RECHECK_RESULT_DIR = project_root / "08-5_high_score_required_skill_recheck/01_result"
RECHECK_ALL_PATH = RECHECK_RESULT_DIR / "high_score_required_skill_recheck_all.jsonl"
RECHECK_COMMIT_PATH = RECHECK_RESULT_DIR / "production_commit.json"
OUTPUT_BASE_DIR = STEP_DIR / "01_result"
CLEANED_BODY_PATH = project_root / "01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl"
CONFIRM_RESULT = Path(__file__).resolve().parent / "confirm_result_prepare_sales_mail_context.txt"

REQUIRED_KEYS = [
    "pair_file_name",
    "project_message_id",
    "resource_message_id",
    "pair_type",
    "project_budget_source",
    "resource_desired_unit_price_source",
    "resource_vendor_flow_source",
    "resource_skillsheet_source",
    "suggested_affiliation_label_source",
    "suggested_parallel_status_source",
]
ALLOWED_PAIR_TYPES = {"project_resource"}
ALLOWED_SCORE_BANDS = {"100percent", "80to99percent", "retention_guard"}
ALLOWED_RECHECK_STATUSES = {
    "required_skill_confirmed",
    "required_skill_human_review",
}
REGION_LIKE_WORDS = ["北海道地方", "東北地方", "関東地方", "中部地方", "関西地方", "近畿地方", "中国地方", "四国地方", "九州地方", "地方", "remote", "リモート"]
NEAREST_STATION_REVIEW_NOTE = "要員本文に最寄駅シグナルがありますが駅名を抽出できませんでした"
NEAREST_STATION_REVIEW_NG_THRESHOLD = 20
START_DATE_REVIEW_NOTE = "案件本文に開始時期シグナルがありますがproject_start_dateを抽出できませんでした"
INTERVIEW_REVIEW_NOTE = "案件本文に面談シグナルがありますがproject_interview_countを抽出できませんでした"
START_DATE_REVIEW_NG_THRESHOLD = 20
INTERVIEW_REVIEW_NG_THRESHOLD = 10
EMAIL_PATTERN = re.compile(r"(?i)\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b")
START_DATE_INVALID_LABEL_PATTERN = re.compile(
    r"^(?:条件|備考|面談|精算|場所|勤務地|作業場所|単価|外国籍|商流|募集人数|再委託|作業期間)\s*$"
)


def normalize_text(value) -> str:
    return str(value or "").strip()


def normalize_email_set(values) -> set:
    items = values if isinstance(values, list) else [values]
    result = set()
    for item in items:
        text = normalize_text(item).lower()
        if text:
            result.add(text)
    return result


def extract_emails(text: str) -> List[str]:
    return [normalize_text(match.group(0)).lower() for match in EMAIL_PATTERN.finditer(text)]


def extract_explicit_reply_to_candidates(body: str) -> set:
    candidates: List[str] = []
    for raw_line in normalize_text(body).splitlines():
        line = normalize_text(raw_line)
        if not line:
            continue
        emails = extract_emails(line)

        if re.search(r"(?i)\b(mail\s*to|to)\s*[:：]", line) and emails:
            candidates.extend(emails)

        to_jp = re.search(r"Toを(.+?)宛", line, re.IGNORECASE)
        if to_jp:
            candidates.extend(extract_emails(to_jp.group(1)))

        if any(phrase in line for phrase in ["こちらにご連絡", "本メール宛にご返信", "本メール宛に返信", "返信はこの担当まで", "LINEまたはメール"]):
            candidates.extend(emails)

    return normalize_email_set(candidates)


def has_station_label_residue(value: str) -> bool:
    return bool(re.match(r"^(?:[：:]|り[：:]|寄[：:])", normalize_text(value)))


def has_invalid_start_date_value(value) -> bool:
    text = normalize_text(value).strip(" :：")
    if not text:
        return False
    compact = re.sub(r"[\s:：]", "", text)
    compact = re.sub(r"[■◇●・*\-◎○〇◆▼□]", "", compact)
    if not compact:
        return True
    if START_DATE_INVALID_LABEL_PATTERN.fullmatch(compact):
        return True
    return False


def has_invalid_label_residue(value, invalid_labels: List[str]) -> bool:
    text = normalize_text(value).strip(" :：")
    if not text:
        return False
    compact = re.sub(r"[\s:：]", "", text)
    compact = re.sub(r"[■◇●・*\-◎○〇◆▼□]", "", compact)
    if not compact:
        return True
    return compact in invalid_labels


def has_start_date_label_signal(text: str) -> bool:
    body = normalize_text(text)
    if not body:
        return False
    return any(
        token in body
        for token in ["[契約開始日]", "[開始時期]", "期間", "契約開始日", "開始日", "開始時期", "入場時期", "参画時期"]
    ) or any(token in body for token in ["即日", "4月~", "4月〜", "5月~", "5月〜", "/01~", "/01〜"])


def has_interview_label_signal(text: str) -> bool:
    body = normalize_text(text)
    if not body:
        return False
    return any(token in body for token in ["[面談回数]", "Web面談", "WEB面談", "面談回数", "面接回数", "面談:", "面談："])


def load_sales_recheck_records() -> Tuple[str, List[Dict]]:
    commit = json.loads(RECHECK_COMMIT_PATH.read_text(encoding="utf-8"))
    input_date = normalize_text(commit.get("run_date"))
    if not re.fullmatch(r"\d{8}", input_date):
        raise ValueError(f"08-5 production_commit run_dateが不正です: {input_date}")
    records = [
        record
        for record in read_jsonl_as_list(str(RECHECK_ALL_PATH))
        if (record.get("recheck_info") or {}).get("recheck_status") in ALLOWED_RECHECK_STATUSES
    ]
    return input_date, records


def parse_args():
    parser = argparse.ArgumentParser(description="09-3 sales mail context confirm")
    parser.add_argument("--target-date", help="対象日 YYYYMMDD。省略時は08-5 production_commitを使用")
    parser.add_argument("--output-path", type=Path, help="focused検証用の出力JSONL path")
    return parser.parse_args()


def main() -> None:
    logger = get_logger(STEP_NAME)
    args = parse_args()
    errors: List[str] = []
    lines = ["=== 09-3_prepare_sales_mail_context confirm結果 ===", ""]

    try:
        input_date, input_records = load_sales_recheck_records()
        if args.target_date and args.target_date != input_date:
            raise ValueError(
                f"--target-dateと08-5 production_commitが不一致です: "
                f"target={args.target_date} committed={input_date}"
            )
        output_path = args.output_path or OUTPUT_BASE_DIR / f"prepare_sales_mail_context_{input_date}.jsonl"

        if RECHECK_ALL_PATH.exists() and RECHECK_COMMIT_PATH.exists():
            lines.append(f"[OK] 08-5正本入力存在: {RECHECK_ALL_PATH}")
        else:
            raise FileNotFoundError("08-5正本入力またはproduction_commitが存在しません")

        if output_path.exists():
            lines.append(f"[OK] 出力JSONL存在: {output_path}")
        else:
            msg = f"[NG] 出力JSONLが存在しない: {output_path}"
            lines.append(msg)
            errors.append(msg)

        input_count = len(input_records)
        output_records = read_jsonl_as_list(str(output_path)) if output_path.exists() else []
        output_count = len(output_records)
        input_pair_keys = [
            (
                normalize_text((record.get("project_info") or {}).get("message_id")),
                normalize_text((record.get("resource_info") or {}).get("message_id")),
            )
            for record in input_records
        ]
        output_pair_keys = [
            (
                normalize_text(record.get("project_message_id")),
                normalize_text(record.get("resource_message_id")),
            )
            for record in output_records
        ]
        cleaned_body_map: Dict[str, str] = {}
        if CLEANED_BODY_PATH.exists():
            cleaned_body_map = {
                normalize_text(record.get("message_id")): normalize_text(record.get("body_text"))
                for record in read_jsonl_as_list(str(CLEANED_BODY_PATH))
                if normalize_text(record.get("message_id"))
            }
        lines.append(f"入力pair件数: {input_count}")
        lines.append(f"出力JSONL件数: {output_count}")
        if input_count == output_count:
            lines.append("[OK] 08-5 confirmed/human_review件数と出力件数が一致")
        else:
            msg = f"[NG] 件数不一致: 入力={input_count} 出力={output_count}"
            lines.append(msg)
            errors.append(msg)
        if (
            len(input_pair_keys) == len(set(input_pair_keys))
            and len(output_pair_keys) == len(set(output_pair_keys))
            and set(input_pair_keys) == set(output_pair_keys)
        ):
            lines.append("[OK] 08-5営業対象と09-3出力のpair key集合が重複なく一致")
        else:
            msg = "[NG] 08-5営業対象と09-3出力のpair key集合に欠落・余剰・重複あり"
            lines.append(msg)
            errors.append(msg)

        missing_required_key_count = 0
        invalid_pair_type_count = 0
        missing_source_count = 0
        human_review_count = 0
        empty_quote_count = 0
        non_preferred_source_count = 0
        reply_signal_but_empty_count = 0
        body_explicit_header_mixed_count = 0
        weird_station_count = 0
        station_label_residue_count = 0
        invalid_start_date_count = 0
        invalid_available_date_count = 0
        invalid_settlement_count = 0
        invalid_foreign_count = 0
        invalid_parallel_count = 0
        start_signal_missing_count = 0
        interview_signal_missing_count = 0
        start_review_count = 0
        interview_review_count = 0
        explicit_start_label_missing_count = 0
        explicit_interview_label_missing_count = 0
        nearest_station_signal_review_count = 0
        missing_recheck_status_count = 0
        invalid_recheck_status_count = 0
        invalid_required_skill_checks_count = 0
        not_confirmed_mixed_count = 0
        invalid_score_band_count = 0
        recheck_status_counter: Counter = Counter()
        review_reason_counter: Counter = Counter()

        for record in output_records:
            recheck_status = record.get("required_skill_recheck_status")
            if not recheck_status:
                missing_recheck_status_count += 1
            else:
                recheck_status_counter[recheck_status] += 1
                if recheck_status not in ALLOWED_RECHECK_STATUSES:
                    invalid_recheck_status_count += 1
                if recheck_status == "required_skill_not_confirmed":
                    not_confirmed_mixed_count += 1
            if not isinstance(record.get("required_skill_checks"), list):
                invalid_required_skill_checks_count += 1
            score_band = record.get("source_score_band", record.get("score_band"))
            if score_band not in ALLOWED_SCORE_BANDS:
                invalid_score_band_count += 1
            if record.get("needs_human_review") is True:
                human_review_count += 1
            for note in record.get("review_notes", []):
                review_reason_counter[note] += 1
                if note == NEAREST_STATION_REVIEW_NOTE:
                    nearest_station_signal_review_count += 1
                if note == START_DATE_REVIEW_NOTE:
                    start_review_count += 1
                if note == INTERVIEW_REVIEW_NOTE:
                    interview_review_count += 1
            if not record.get("quoted_project_mail_body") or not record.get("quoted_resource_mail_body"):
                empty_quote_count += 1
            if record.get("pair_type") not in ALLOWED_PAIR_TYPES:
                invalid_pair_type_count += 1
            for key in REQUIRED_KEYS:
                value = record.get(key)
                if value is None or value == "" or value == []:
                    if key.endswith("_source"):
                        missing_source_count += 1
                    else:
                        missing_required_key_count += 1
            if record.get("project_budget") is not None and record.get("project_budget_source") == "01-4_cleanup_email_text":
                non_preferred_source_count += 1
            if (
                record.get("resource_desired_unit_price") is not None
                and record.get("resource_desired_unit_price_source") == "01-4_cleanup_email_text"
            ):
                non_preferred_source_count += 1
            if (
                record.get("project_reply_directive_lines")
                and not record.get("project_reply_to_candidates")
                and not record.get("project_cc_candidates")
                and not (
                    "案件側本文に返信先指示シグナルがありますがTO候補を抽出できませんでした" in (record.get("review_notes") or [])
                    and "案件側の返信先指示が曖昧です" in (record.get("review_notes") or [])
                )
            ) or (
                record.get("resource_reply_directive_lines")
                and not record.get("resource_reply_to_candidates")
                and not record.get("resource_cc_candidates")
                and not (
                    "要員側本文に返信先指示シグナルがありますがTO候補を抽出できませんでした" in (record.get("review_notes") or [])
                    and "要員側の返信先指示が曖昧です" in (record.get("review_notes") or [])
                )
            ):
                reply_signal_but_empty_count += 1
            station = str(record.get("resource_nearest_station") or "")
            if station and any(word in station for word in REGION_LIKE_WORDS):
                weird_station_count += 1
            if station and has_station_label_residue(station):
                station_label_residue_count += 1
            if has_invalid_start_date_value(record.get("project_start_date")):
                invalid_start_date_count += 1
            if has_invalid_label_residue(record.get("resource_available_date"), ["日", "開始日", "稼働", "稼動", "参画可能日", "開始"]):
                invalid_available_date_count += 1
            if has_invalid_label_residue(record.get("project_settlement"), ["条件", "精算", "精算条件", "精算幅"]):
                invalid_settlement_count += 1
            if has_invalid_label_residue(record.get("project_foreign_restriction"), ["外国籍"]):
                invalid_foreign_count += 1
            if has_invalid_label_residue(record.get("suggested_parallel_status"), ["並行", "提案中", "面談調整中", "面談予定"]):
                if ":" in normalize_text(record.get("suggested_parallel_status")) or "：" in normalize_text(record.get("suggested_parallel_status")):
                    invalid_parallel_count += 1
            for prefix in ["project", "resource"]:
                if record.get(f"{prefix}_reply_target_source") != "body_explicit":
                    continue
                header_candidates = normalize_email_set(
                    [record.get(f"{prefix}_reply_to"), record.get(f"{prefix}_sender_email")]
                )
                stored_candidates = normalize_email_set(record.get(f"{prefix}_reply_to_candidates"))
                body_candidates = extract_explicit_reply_to_candidates(
                    cleaned_body_map.get(normalize_text(record.get(f"{prefix}_message_id")), record.get(f"quoted_{prefix}_mail_body"))
                )
                unexpected_header_candidates = (stored_candidates & header_candidates) - body_candidates
                if unexpected_header_candidates:
                    body_explicit_header_mixed_count += 1
            if record.get("project_start_date_source") == "unavailable" and has_start_date_label_signal(record.get("quoted_project_mail_body")):
                start_signal_missing_count += 1
            if record.get("project_interview_count_source") == "unavailable" and has_interview_label_signal(record.get("quoted_project_mail_body")):
                interview_signal_missing_count += 1
            if record.get("project_start_date_source") == "unavailable" and has_start_date_label_signal(record.get("quoted_project_mail_body")):
                explicit_start_label_missing_count += 1
            if record.get("project_interview_count_source") == "unavailable" and has_interview_label_signal(record.get("quoted_project_mail_body")):
                explicit_interview_label_missing_count += 1

        lines.append(f"needs_human_review=true 件数: {human_review_count}")
        lines.append(f"本文引用空件数: {empty_quote_count}")
        lines.append(f"返信先指示シグナルありだが候補空 件数: {reply_signal_but_empty_count}")
        lines.append(f"body_explicitでheader宛先混在 件数: {body_explicit_header_mixed_count}")
        lines.append(f"resource_nearest_station地域値混入 件数: {weird_station_count}")
        lines.append(f"resource_nearest_stationラベル残骸混入 件数: {station_label_residue_count}")
        lines.append(f"project_start_date開始時期不正値 件数: {invalid_start_date_count}")
        lines.append(f"resource_available_dateラベル残骸 件数: {invalid_available_date_count}")
        lines.append(f"project_settlementラベル残骸 件数: {invalid_settlement_count}")
        lines.append(f"project_foreign_restrictionラベル残骸 件数: {invalid_foreign_count}")
        lines.append(f"suggested_parallel_statusラベル残骸 件数: {invalid_parallel_count}")
        lines.append(f"最寄駅シグナル未抽出review 件数: {nearest_station_signal_review_count}")
        lines.append(f"開始時期未抽出review 件数: {start_review_count}")
        lines.append(f"面談回数未抽出review 件数: {interview_review_count}")
        lines.append(f"時期シグナルあり未抽出 件数: {start_signal_missing_count}")
        lines.append(f"面談シグナルあり未抽出 件数: {interview_signal_missing_count}")
        lines.append(f"開始時期ラベルあり未抽出 件数: {explicit_start_label_missing_count}")
        lines.append(f"面談回数ラベルあり未抽出 件数: {explicit_interview_label_missing_count}")
        lines.append("required_skill_recheck_status 件数:")
        if recheck_status_counter:
            for status, count in sorted(recheck_status_counter.items()):
                lines.append(f"- {status}: {count}")
        else:
            lines.append("- none: 0")
        lines.append(f"required_skill_recheck_status missing 件数: {missing_recheck_status_count}")
        lines.append(f"required_skill_recheck_status 不正 件数: {invalid_recheck_status_count}")
        lines.append(f"required_skill_checks list不正 件数: {invalid_required_skill_checks_count}")
        lines.append(f"required_skill_not_confirmed 混入 件数: {not_confirmed_mixed_count}")
        lines.append(f"score_band/source_score_band 不正 件数: {invalid_score_band_count}")
        if review_reason_counter:
            lines.append("主なreview理由:")
            for reason, count in review_reason_counter.most_common(8):
                lines.append(f"- {reason}: {count}")

        if missing_required_key_count == 0:
            lines.append("[OK] 各レコードに必須キーあり")
        else:
            msg = f"[NG] 必須キー欠落あり: {missing_required_key_count}"
            lines.append(msg)
            errors.append(msg)

        if invalid_pair_type_count == 0:
            lines.append("[OK] pair_typeは想定値のみ")
        else:
            msg = f"[NG] 想定外pair_typeあり: {invalid_pair_type_count}"
            lines.append(msg)
            errors.append(msg)

        if missing_source_count == 0:
            lines.append("[OK] 主要sourceキーあり")
        else:
            msg = f"[NG] 主要sourceキー欠落あり: {missing_source_count}"
            lines.append(msg)
            errors.append(msg)

        if non_preferred_source_count == 0:
            lines.append("[OK] 構造化値優先方針に反する主要sourceなし")
        else:
            msg = f"[NG] 構造化値優先方針に反する主要sourceあり: {non_preferred_source_count}"
            lines.append(msg)
            errors.append(msg)

        if empty_quote_count == 0:
            lines.append("[OK] 明らかに空の本文引用なし")
        else:
            msg = f"[NG] 空の本文引用あり: {empty_quote_count}"
            lines.append(msg)
            errors.append(msg)

        if reply_signal_but_empty_count == 0:
            lines.append("[OK] 想定外の本文返信先指示未反映なし")
        else:
            msg = f"[NG] 想定外の本文返信先指示未反映あり: {reply_signal_but_empty_count}"
            lines.append(msg)
            errors.append(msg)

        if body_explicit_header_mixed_count == 0:
            lines.append("[OK] body_explicitでheader宛先混在なし")
        else:
            msg = f"[NG] body_explicitでheader宛先混在あり: {body_explicit_header_mixed_count}"
            lines.append(msg)
            errors.append(msg)

        if weird_station_count == 0:
            lines.append("[OK] resource_nearest_stationに地域値混入なし")
        else:
            msg = f"[NG] resource_nearest_stationに地域値混入あり: {weird_station_count}"
            lines.append(msg)
            errors.append(msg)

        if station_label_residue_count == 0:
            lines.append("[OK] resource_nearest_stationにラベル残骸混入なし")
        else:
            msg = f"[NG] resource_nearest_stationにラベル残骸混入あり: {station_label_residue_count}"
            lines.append(msg)
            errors.append(msg)

        if invalid_start_date_count == 0:
            lines.append("[OK] project_start_dateに開始時期不正値なし")
        else:
            msg = f"[NG] project_start_dateに開始時期不正値あり: {invalid_start_date_count}"
            lines.append(msg)
            errors.append(msg)

        if invalid_available_date_count == 0:
            lines.append("[OK] resource_available_dateにラベル残骸なし")
        else:
            msg = f"[NG] resource_available_dateにラベル残骸あり: {invalid_available_date_count}"
            lines.append(msg)
            errors.append(msg)

        if invalid_settlement_count == 0:
            lines.append("[OK] project_settlementにラベル残骸なし")
        else:
            msg = f"[NG] project_settlementにラベル残骸あり: {invalid_settlement_count}"
            lines.append(msg)
            errors.append(msg)

        if invalid_foreign_count == 0:
            lines.append("[OK] project_foreign_restrictionにラベル残骸なし")
        else:
            msg = f"[NG] project_foreign_restrictionにラベル残骸あり: {invalid_foreign_count}"
            lines.append(msg)
            errors.append(msg)

        if invalid_parallel_count == 0:
            lines.append("[OK] suggested_parallel_statusにラベル残骸なし")
        else:
            msg = f"[NG] suggested_parallel_statusにラベル残骸あり: {invalid_parallel_count}"
            lines.append(msg)
            errors.append(msg)

        if nearest_station_signal_review_count < NEAREST_STATION_REVIEW_NG_THRESHOLD:
            lines.append(
                f"[OK] 最寄駅シグナル未抽出review件数は閾値未満: "
                f"{nearest_station_signal_review_count} < {NEAREST_STATION_REVIEW_NG_THRESHOLD}"
            )
        else:
            msg = (
                f"[NG] 最寄駅シグナル未抽出review件数が閾値以上: "
                f"{nearest_station_signal_review_count} >= {NEAREST_STATION_REVIEW_NG_THRESHOLD}"
            )
            lines.append(msg)
            errors.append(msg)

        if start_review_count < START_DATE_REVIEW_NG_THRESHOLD:
            lines.append(
                f"[OK] 開始時期未抽出review件数は閾値未満: "
                f"{start_review_count} < {START_DATE_REVIEW_NG_THRESHOLD}"
            )
        else:
            msg = f"[NG] 開始時期未抽出review件数が閾値以上: {start_review_count} >= {START_DATE_REVIEW_NG_THRESHOLD}"
            lines.append(msg)
            errors.append(msg)

        if interview_review_count < INTERVIEW_REVIEW_NG_THRESHOLD:
            lines.append(
                f"[OK] 面談回数未抽出review件数は閾値未満: "
                f"{interview_review_count} < {INTERVIEW_REVIEW_NG_THRESHOLD}"
            )
        else:
            msg = f"[NG] 面談回数未抽出review件数が閾値以上: {interview_review_count} >= {INTERVIEW_REVIEW_NG_THRESHOLD}"
            lines.append(msg)
            errors.append(msg)

        if explicit_start_label_missing_count == 0:
            lines.append("[OK] 期間/契約開始日等の開始時期ラベル未抽出なし")
        else:
            msg = f"[NG] 期間/契約開始日等を含む開始時期ラベル未抽出あり: {explicit_start_label_missing_count}"
            lines.append(msg)
            errors.append(msg)

        if explicit_interview_label_missing_count == 0:
            lines.append("[OK] []形式/Web面談等の面談回数ラベル未抽出なし")
        else:
            msg = f"[NG] []形式/Web面談等の面談回数ラベル未抽出あり: {explicit_interview_label_missing_count}"
            lines.append(msg)
            errors.append(msg)

        if missing_recheck_status_count == 0:
            lines.append("[OK] required_skill_recheck_status missingなし")
        else:
            msg = f"[NG] required_skill_recheck_status missingあり: {missing_recheck_status_count}"
            lines.append(msg)
            errors.append(msg)

        if invalid_recheck_status_count == 0:
            lines.append("[OK] required_skill_recheck_statusは採用対象のみ")
        else:
            msg = f"[NG] required_skill_recheck_status不正あり: {invalid_recheck_status_count}"
            lines.append(msg)
            errors.append(msg)

        if invalid_required_skill_checks_count == 0:
            lines.append("[OK] required_skill_checksはlist")
        else:
            msg = f"[NG] required_skill_checks list不正あり: {invalid_required_skill_checks_count}"
            lines.append(msg)
            errors.append(msg)

        if not_confirmed_mixed_count == 0:
            lines.append("[OK] required_skill_not_confirmed混入なし")
        else:
            msg = f"[NG] required_skill_not_confirmed混入あり: {not_confirmed_mixed_count}"
            lines.append(msg)
            errors.append(msg)

        if invalid_score_band_count == 0:
            lines.append("[OK] score_band/source_score_bandは想定値のみ")
        else:
            msg = f"[NG] score_band/source_score_band不正あり: {invalid_score_band_count}"
            lines.append(msg)
            errors.append(msg)

    except Exception as error:
        msg = f"[NG] confirm実行失敗: {error}"
        lines.append(msg)
        errors.append(msg)

    lines.append("")
    lines.append("【結果】NG" if errors else "【結果】OK")
    result_text = "\n".join(lines)

    for line in lines:
        if "[NG]" in line or line.strip() == "【結果】NG":
            logger.error(line)
        else:
            logger.info(line)

    CONFIRM_RESULT.parent.mkdir(parents=True, exist_ok=True)
    CONFIRM_RESULT.write_text(result_text + "\n", encoding="utf-8")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()

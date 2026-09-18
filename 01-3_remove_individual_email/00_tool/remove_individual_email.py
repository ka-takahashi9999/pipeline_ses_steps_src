#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 01-3: 個別除外処理スクリプト

複数要員メール・キャンペーンメール等を個別除外する。
手動除外条件は 10_assistance_tool/exclude_list.txt で管理する。

手動除外に該当しないメールだけ、SubjectからHIGH_CONFIDENCEに判定できる
明示的な複数要員・複数案件・一覧そのものをdeterministicに除外する。

除外リスト形式（1行につき）:
  from のみ    → そのアドレスからの全メールを除外
  from,subject → from + subject が一致するメールを除外
  # で始まる行・空行はスキップ

入力①（本文参照用）:
  01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl

入力②（処理対象のmessage_id）:
  01-2_remove_duplicate_emails/01_result/remove_duplicate_emails_raw.jsonl

出力①: 01_result/remove_individual_emails_raw.jsonl  （除外後の message_id）
出力②: 01_result/99_removed_individual_emails_raw.jsonl （除外された message_id）
出力③: 01_result/99_removed_individual_emails_detail.jsonl
       （限定テンプレートで追加除外したメールの元全項目 + reason / rule_id）
"""

import fnmatch
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# common モジュールのパス解決
_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "01-3_remove_individual_email"
logger = get_logger(STEP_NAME)

INPUT_MASTER = str(
    _PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)
INPUT_PREV = str(
    _PROJECT_ROOT / "01-2_remove_duplicate_emails" / "01_result" / "remove_duplicate_emails_raw.jsonl"
)
EXCLUDE_LIST_PATH = str(_STEP_DIR / "10_assistance_tool" / "exclude_list.txt")

OUTPUT_FILTERED = "remove_individual_emails_raw.jsonl"
OUTPUT_REMOVED = "99_removed_individual_emails_raw.jsonl"
OUTPUT_DETAIL = "99_removed_individual_emails_detail.jsonl"

# 2026-09-17レビューの実例に限定。汎用の複数要員・非SES判定には広げない。
TEMPLATE_REASONS = {"multi_item_mail", "list_or_portal_notice", "service_notification"}
_PROFILE_OR_PROJECT_RE = re.compile(
    r"氏\s*名|名\s*前|年\s*齢|最\s*寄|単\s*価|単\s*金|"
    r"案件名|案件概要|業務内容|作業内容|必須スキル|【案件】"
)
_CHO_LINK_URL = r"https://u29046571\.ct\.sendgrid\.net/ls/click\?\S+"
_CHO_INVITE_BODY_RE = re.compile(
    r"(?P<company>[^\n]+?)の(?P<person>[^\n]+?)様より、チョータツでの連携のご依頼です\n+"
    r"チョータツにご登録いただくと、メールでお送りしている案件に加えて、会員限定で公開している案件・人材もご覧いただけます。\n"
    r"さらに(?P=company)と連携いただくと、案件・人材の一覧がまとまり、お互いに提案できるようになります。\n+"
    r"チョータツは基本無料です。会社のメールアドレスがあれば1分で登録できます。\n+"
    r"▼ (?P=company)と連携する\n" + _CHO_LINK_URL + r"\n+"
    r"- - -\nチョータツでできること\n"
    r"・会員限定の案件・人材を検索・閲覧\n"
    r"・自社の案件・人材を、登録済みの配信先へワンクリックで一斉配信\n"
    r"・3000社以上の登録企業とつながる\n- - -\n+---\n"
    r"このメールアドレスは送信専用のため、ご返信いただけません。\n"
    r"ご不明な点・ご質問はお問い合わせください。\n" + _CHO_LINK_URL
)
_IDENTITY_LIST_MAIN_RE = re.compile(
    r"株式会社テクノヴァース\nご担当者様\n+お世話になっております。\n"
    r"アイデンティティーのビジネスパートナーチームです。\n+"
    r"本日時点で弊社で営業中のPython人材にマッチする案件を探しております。\n+"
    r"見合う案件がございましたら、ぜひご提案いただけますと幸いです。\n+"
    r"※スキルシートは、下記URL内のリンクからご確認くださいませ。\n"
    r"※ご提案いただく際は、商流をご教示いただくようお願い申し上げます。\n+"
    r"※ご提案いただきました案件はすべて確認させていただいておりますが、\n"
    r"弊社から2営業日以内に連絡がない場合は、お見送りとご判断くださいませ。\n+"
    r"■人材一覧リスト\n[—]+\n+https://info\.techcareer\.jp/e/998201/"
    r"MU-edit-gid-969571495-range-A1/\S+\n+[—]+\n+"
    r"以上、ご提案をお待ちしております。"
)


def detect_template_exclusion(record: Dict) -> Optional[Tuple[str, str]]:
    """送信元・件名・本文構造が揃う確認済みテンプレートのみ退避する。

    正規化は比較用コピーだけ。欠損・型不明は新規ルールで除外しない。
    一覧/通知は添付が一つでもあれば残す（処理対象添付を取りこぼさない）。
    """
    if not all(isinstance(record.get(k), str) for k in ("from", "subject", "body_text")):
        return None
    if not all(isinstance(record.get(k), list) for k in ("attachments", "html_links")):
        return None
    sender = extract_email(record["from"])
    subject = unicodedata.normalize("NFKC", record["subject"]).strip()
    body = unicodedata.normalize("NFKC", record["body_text"]).replace("\r\n", "\n").strip()

    if (sender == "feng-h@aliplaza.co.jp"
            and re.fullmatch(r"【[0-9]{2}/[0-9]{2}】【JAVA\s+SE3名】【要件定義~】.+", subject)):
        blocks = re.split(r"(?m)^要員情報([123])\s*$", body)
        if (len(blocks) == 7 and blocks[1::2] == ["1", "2", "3"]
                and all(re.search(r"■\s*スキル\s*:", b)
                        and re.search(r"■\s*所\s*属\s*:", b)
                        and re.search(r"■\s*稼\s*働\s*:", b) for b in blocks[2::2])):
            return "multi_item_mail", "aliplaza_three_resource_blocks_v1"

    if (sender == "masaya.hayashi@hyperlinksolution.co.jp"
            and subject.startswith("【人材情報一覧】開発経験者3名のご紹介/")
            and "弊社プロパー人材情報のリストをお送り致します。" in body
            and re.search(r"【営業中プロパーリスト】\nhttps://x\.bmd\.jp/70/2209/\d+/255", body)):
        three = re.search(r"下記の3名を営業中。\s*\n(?P<rows>(?:・[^\n]+\n){3})", body)
        if three and all(re.search(r"要員\s+[0-9]+月~", row)
                         for row in three.group("rows").splitlines()):
            return "multi_item_mail", "hyperlink_three_resource_list_v1"

    if record["attachments"] or _PROFILE_OR_PROJECT_RE.search(body):
        return None

    if sender == "noreply@cho-tatsu.com":
        invite = _CHO_INVITE_BODY_RE.fullmatch(body)
        if (invite and subject == "{}の{}様よりチョータツでの連携のご依頼".format(
                invite.group("company"), invite.group("person"))):
            return "service_notification", "chotatsu_connection_invitation_v1"

    if (sender == "noreply@driven-x.com"
            and re.fullmatch(r"【チョータツブースト】 新着案件・人材\([0-9]{2}月[0-9]{2}日\)", subject)
            and body == "チョータツブーストの新着案件・人材情報"):
        hrefs = {link.get("href") for link in record["html_links"]
                 if isinstance(link, dict) and isinstance(link.get("href"), str)}
        if {"https://cho-tatsu.com/boost/talents", "https://cho-tatsu.com/boost/projects"} <= hrefs:
            return "service_notification", "chotatsu_boost_digest_v1"

    if (sender == "bp@id-entity.jp"
            and subject == "ご提案可能な営業中Python人材のご紹介"
            and _IDENTITY_LIST_MAIN_RE.fullmatch(body.partition("━━")[0].strip())):
        return "list_or_portal_notice", "identity_python_list_portal_v1"
    return None


def build_detail_record(record: Dict, reason: str, rule_id: str) -> Dict:
    """元レコード全項目を保持する。監査キー衝突時は上書きせず停止する。"""
    if "reason" in record or "rule_id" in record:
        raise ValueError("mail master contains reserved audit keys: reason / rule_id")
    return dict(record, reason=reason, rule_id=rule_id)

RESOURCE_NOUN_PATTERN = r"(?:要員|人材|エンジニア|技術者|プロパー)"
RESOURCE_COUNT_PATTERNS = (
    re.compile(
        rf"(?P<count>[0-9]+)名の{RESOURCE_NOUN_PATTERN}"
        rf"|{RESOURCE_NOUN_PATTERN}(?:[ :])?(?P<count_after>[0-9]+)名"
    ),
    re.compile(r"(?P<count>[0-9]+)名で1人月"),
)
PROJECT_COUNT_PATTERNS = (
    re.compile(r"案件(?P<count>[0-9]+)件"),
    re.compile(r"(?P<count>[0-9]+)件の案件"),
)

RECRUITMENT_VETO_WORDS = (
    "募集",
    "募集枠",
    "増員",
    "必要人数",
    "急募",
    "求人",
    "採用",
)
MANAGEMENT_VETO_WORDS = (
    "マネジメント実績",
    "管理実績",
    "管理経験",
    "マネジメント経験",
)

_LEADING_REPLY_PREFIX_RE = re.compile(r"^(?:re|fw|fwd)\s*:\s*", re.IGNORECASE)
_LEADING_CATEGORY_TAG_RE = re.compile(r"^(?:【[^】]+】|\[[^\]]+\])\s*")
_LIST_DATE_PATTERN = (
    r"(?:(?:20[0-9]{2}[/-])?[0-9]{1,2}[/-][0-9]{1,2}"
    r"|(?:20[0-9]{2}年)?[0-9]{1,2}月[0-9]{1,2}日)"
)
_LIST_AS_PRIMARY_RE = re.compile(
    r"(?:弊社)?(?:営業中)?"
    r"(?:要員一覧|要員リスト|要員共有|人材一覧|人材リスト|人材共有|"
    r"案件一覧|案件リスト|案件・要員まとめ|案件/要員まとめ)"
    r"(?:の送付|送付|の共有|共有|のご案内|ご案内|案内|更新)?"
    rf"(?:\s*{_LIST_DATE_PATTERN})?"
)


def normalize(s: str) -> str:
    """比較用に正規化（NFKC + 小文字 + 前後空白除去）。"""
    return unicodedata.normalize("NFKC", s or "").strip().lower()


def normalize_detector_subject(subject: str) -> str:
    """P1 Subject判定用にNFKC正規化し、連続空白を1文字へ圧縮する。"""
    normalized = unicodedata.normalize("NFKC", subject or "").strip().lower()
    return re.sub(r"\s+", " ", normalized)


def _match_count(match: re.Match) -> int:
    """count/count_afterのうち一致したASCII数字をintで返す。"""
    value = match.groupdict().get("count") or match.groupdict().get("count_after")
    return int(value)


def _has_explicit_multiple_resource(subject: str) -> bool:
    """狭いSubject grammarで明示された複数要員提案だけを判定する。"""
    if any(word in subject for word in RECRUITMENT_VETO_WORDS):
        return False
    if any(word in subject for word in MANAGEMENT_VETO_WORDS):
        return False

    for pattern in RESOURCE_COUNT_PATTERNS:
        for match in pattern.finditer(subject):
            if _match_count(match) < 2:
                continue
            if subject[match.end() :].startswith("規模"):
                continue
            return True
    return False


def _strip_list_leading_markers(subject: str) -> str:
    """一覧判定時だけ、先頭に連続するreply prefix/category tagを除去する。"""
    stripped = subject
    while stripped:
        reply_match = _LEADING_REPLY_PREFIX_RE.match(stripped)
        if reply_match:
            stripped = stripped[reply_match.end() :].strip()
            continue

        tag_match = _LEADING_CATEGORY_TAG_RE.match(stripped)
        if tag_match:
            stripped = stripped[tag_match.end() :].strip()
            continue
        break
    return re.sub(r"\s+", " ", stripped)


def _list_as_primary_reason(subject: str) -> Optional[str]:
    """前処理後Subject全体が狭い一覧grammarへfullmatchした場合だけ理由を返す。"""
    list_subject = _strip_list_leading_markers(subject)
    if not _LIST_AS_PRIMARY_RE.fullmatch(list_subject):
        return None
    if "案件" in list_subject:
        return "project_list_as_primary_subject"
    return "resource_list_as_primary_subject"


def _has_explicit_multiple_project(subject: str) -> bool:
    """案件N件/N件の案件の2形式だけで明示的な複数案件を判定する。"""
    for pattern in PROJECT_COUNT_PATTERNS:
        for match in pattern.finditer(subject):
            if _match_count(match) >= 2:
                return True
    return False


def detect_p1_exclusion_reason(subject: str) -> Optional[str]:
    """Subjectだけを使うconservativeなP1 detector。非該当(TYPE D)はNone。"""
    normalized_subject = normalize_detector_subject(subject)

    if _has_explicit_multiple_resource(normalized_subject):
        return "multiple_resource_explicit_subject_count"

    list_reason = _list_as_primary_reason(normalized_subject)
    if list_reason:
        return list_reason

    if _has_explicit_multiple_project(normalized_subject):
        return "multiple_project_explicit_subject_count"

    return None


def extract_email(s: str) -> str:
    """
    'Display Name <email@example.com>' や '<email@example.com>' から
    メールアドレス部分だけを抽出して正規化する。
    '<>' が存在しない場合はそのまま normalize する。
    """
    m = re.search(r"<([^>]+)>", s or "")
    if m:
        return normalize(m.group(1))
    return normalize(s)


def subject_matches(pattern: str, subject: str) -> bool:
    """
    subject の一致判定。
    pattern に * が含まれる場合は fnmatch によるワイルドカード一致。
    * が含まれない場合は完全一致。
    どちらも正規化済みの文字列を受け取ること。
    """
    if "*" in pattern:
        return fnmatch.fnmatch(subject, pattern)
    return pattern == subject


def load_exclude_list(path: str) -> Tuple[Set[str], List[Tuple[str, str]]]:
    """
    除外リストを読み込む。
    戻り値:
      from_only_set   : from のみ指定（正規化済みアドレスのセット）
      from_subj_rules : (from, subject_pattern) のリスト（正規化済み）
                        subject_pattern は * を含む場合はワイルドカードとして扱う
    """
    from_only_set: Set[str] = set()
    from_subj_rules: List[Tuple[str, str]] = []

    exclude_path = Path(path)
    if not exclude_path.exists():
        logger.warn(f"除外リストファイルが存在しません: {path}")
        return from_only_set, from_subj_rules

    with open(exclude_path, encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",", 1)
            if len(parts) == 1:
                # from のみ
                from_only_set.add(normalize(parts[0]))
            else:
                # from,subject（subject は * ありの場合はワイルドカードパターン）
                from_subj_rules.append((normalize(parts[0]), normalize(parts[1])))

    logger.info(
        f"除外リスト読み込み完了: fromのみ={len(from_only_set)}件, "
        f"from+subject={len(from_subj_rules)}件"
    )
    return from_only_set, from_subj_rules


def is_excluded(
    record: Dict,
    from_only_set: Set[str],
    from_subj_rules: List[Tuple[str, str]],
) -> bool:
    """レコードが除外対象かどうかを判定する。"""
    norm_from = extract_email(record.get("from") or "")
    norm_subject = normalize(record.get("subject") or "")

    if norm_from in from_only_set:
        return True
    for rule_from, rule_subj in from_subj_rules:
        if rule_from == norm_from and subject_matches(rule_subj, norm_subject):
            return True
    return False


def determine_exclusion_reason(
    record: Dict,
    from_only_set: Set[str],
    from_subj_rules: List[Tuple[str, str]],
) -> Optional[str]:
    """manual → P1 → 限定テンプレートの順で評価し、既存除外理由を保つ。"""
    if is_excluded(record, from_only_set, from_subj_rules):
        return "manual_exclude_list"
    p1_reason = detect_p1_exclusion_reason(record.get("subject") or "")
    if p1_reason:
        return p1_reason
    template = detect_template_exclusion(record)
    return template[0] if template else None


def main() -> None:
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = str(dirs["result"])

    start_time = time.time()
    filtered_records: List[Dict] = []
    removed_records: List[Dict] = []
    detail_records: List[Dict] = []
    exclusion_reason_counts: Dict[str, int] = {}

    try:
        # 除外リスト読み込み
        from_only_set, from_subj_rules = load_exclude_list(EXCLUDE_LIST_PATH)

        # メールマスタ読み込み（message_id → レコード）
        logger.info(f"メールマスタ読み込み: {INPUT_MASTER}")
        master = read_jsonl_as_dict(INPUT_MASTER, key="message_id")
        logger.info(f"メールマスタ件数: {len(master)}件")

        # 01-2 の出力（処理対象 message_id）読み込み
        logger.info(f"01-2 出力読み込み: {INPUT_PREV}")
        prev_records = read_jsonl_as_list(INPUT_PREV)
        input_count = len(prev_records)
        logger.info(f"01-2 入力件数: {input_count}件")

        not_found_count = 0
        for rec in prev_records:
            mid = rec.get("message_id", "")
            master_rec = master.get(mid)
            if master_rec is None:
                logger.warn(f"メールマスタに存在しない message_id: {mid}")
                not_found_count += 1
                # マスタになくても除外せずに残す（安全側）
                filtered_records.append({"message_id": mid})
                continue

            exclusion_reason = determine_exclusion_reason(
                master_rec, from_only_set, from_subj_rules
            )
            if exclusion_reason:
                removed_records.append({"message_id": mid})
                if exclusion_reason in TEMPLATE_REASONS:
                    reason, rule_id = detect_template_exclusion(master_rec)
                    detail_records.append(build_detail_record(master_rec, reason, rule_id))
                exclusion_reason_counts[exclusion_reason] = (
                    exclusion_reason_counts.get(exclusion_reason, 0) + 1
                )
            else:
                filtered_records.append({"message_id": mid})

        if not_found_count > 0:
            logger.warn(f"メールマスタに存在しなかった件数: {not_found_count}件")

        logger.info(
            f"個別除外: {input_count}件 → {len(filtered_records)}件 "
            f"（除外: {len(removed_records)}件）"
        )
        logger.info(f"除外理由別件数: {exclusion_reason_counts}")

        # 出力①: 除外後 message_id
        out_filtered = str(dirs["result"] / OUTPUT_FILTERED)
        write_jsonl(out_filtered, filtered_records)
        logger.ok(f"出力①書き込み完了: {out_filtered} ({len(filtered_records)}件)")

        # 出力②: 除外された message_id
        out_removed = str(dirs["result"] / OUTPUT_REMOVED)
        write_jsonl(out_removed, removed_records)
        logger.ok(f"出力②書き込み完了: {out_removed} ({len(removed_records)}件)")

        out_detail = str(dirs["result"] / OUTPUT_DETAIL)
        write_jsonl(out_detail, detail_records)
        logger.ok(f"出力③書き込み完了: {out_detail} ({len(detail_records)}件)")

    except Exception as e:
        write_error_log(result_dir, e, context=f"input={INPUT_PREV}")
        logger.error(f"処理失敗: {e}")
        sys.exit(1)

    finally:
        elapsed = time.time() - start_time
        write_execution_time(
            str(dirs["execution_time"]),
            STEP_NAME,
            elapsed,
            record_count=len(filtered_records),
        )

    logger.ok(
        f"Step完了: 入力={input_count}件 / 出力={len(filtered_records)}件 / "
        f"除外={len(removed_records)}件"
    )


if __name__ == "__main__":
    main()

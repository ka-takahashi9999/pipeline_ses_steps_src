"""
09-3_prepare_sales_proposal_input
高確度候補ペアを営業提案文生成向けの JSONL に整形する。
"""

import re
import sys
import time
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "09-3_prepare_sales_proposal_input"
STEP_DIR = Path(__file__).resolve().parents[1]

INPUT_FILES = [
    ("100percent", project_root / "08-4_match_score_sort/01_result/match_score_sort_100percent.jsonl"),
    ("80to99percent", project_root / "08-4_match_score_sort/01_result/match_score_sort_80to99percent.jsonl"),
]
MAIL_MASTER_PATH = project_root / "01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl"
OUTPUT_DIR = STEP_DIR / "01_result"

EMAIL_RE = re.compile(r"(?i)\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b")
URL_RE = re.compile(r"(?i)https?://\S+|www\.\S+")
DOMAIN_RE = re.compile(r"(?i)\b[a-z0-9.\-]+\.(?:co\.jp|ne\.jp|or\.jp|com|jp|net|org)\b")
PHONE_RE = re.compile(r"(?<!\d)(?:\+81[-\s]?)?(?:0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{3,4})(?!\d)")
LINE_ID_RE = re.compile(r"(?i)\bline\s*id\b[:：]?\s*\S+")
SEPARATOR_RE = re.compile(r"^[=\-＿_＊*■□◆◇━─/＋+>＾~～]{6,}$")
NAME_ONLY_RE = re.compile(r"^[一-龥ぁ-んァ-ヶ]{1,8}(?:\s|　)+[一-龥ぁ-んァ-ヶ]{1,8}$")
NAME_ROMAJI_RE = re.compile(
    r"^[一-龥ぁ-んァ-ヶA-Za-z]+(?:(?:\s|　)+[一-龥ぁ-んァ-ヶA-Za-z]+)*\s*[\(（][A-Za-z][A-Za-z\s.\-]{2,}[\)）]$"
)
NAME_SLASH_ROMAJI_RE = re.compile(
    r"^[一-龥ぁ-んァ-ヶ]{1,8}(?:\s|　)+[一-龥ぁ-んァ-ヶ]{1,8}\s*/\s*[A-Za-z][A-Za-z\s.\-]{2,}$"
)
NAME_PIPE_ROMAJI_RE = re.compile(
    r"^[一-龥ぁ-んァ-ヶ]{1,8}(?:\s|　)*[|｜]\s*[A-Za-z][A-Za-z\s.\-]{2,}$"
)
NAME_KANA_PAREN_RE = re.compile(
    r"^[一-龥ぁ-んァ-ヶ]{1,8}(?:\s|　)*[\(（][ぁ-んァ-ヶー\s　]{2,}[\)）]$"
)
COMPANY_PERSON_GREETING_RE = re.compile(
    r"^[^【】\[\]：:]{1,30}の[一-龥ぁ-んァ-ヶA-Za-z]{1,12}(?:です|でございます)。?$"
)
BUILDING_FRAGMENT_RE = re.compile(
    r"(ビル|マンション|ハイツ|コーポ|ガーデン|タワー|レジデンス|コート|山王|ニューオータニ|FORECAST|F[0-9]{1,2}|[0-9]{1,2}F|[0-9]{3,4}号室|[0-9]{2,4})"
)
FACTUAL_PREFIX_RE = re.compile(
    r"^(【.+?】|■|・|◇|□|[①-⑳]|[0-9]+[.)]|(?:案件名|案件概要|概要|業務内容|内容|必須|尚可|スキル|単価|勤務地|場所|作業場所|最寄|稼働|開始|時期|面談|募集|人数|契約|工程|年齢|性別|所属|希望|備考|外国籍|出社条件|対応工程|フレームワーク|クラウド|その他|営業コメント|利用可能ツール|利用可能環境)[:：])"
)
GREETING_PATTERNS = [
    re.compile(r"ご担当者様"),
    re.compile(r"ご担当者 様"),
    re.compile(r"各位"),
    re.compile(r"協力会社様"),
    re.compile(r"株式会社テクノヴァース"),
    re.compile(r"^\s*様\s*$"),
    re.compile(r"^[一-龥ぁ-んァ-ヶA-Za-z0-9]+様様$"),
]
BOILERPLATE_PATTERNS = [
    re.compile(r"配信停止"),
    re.compile(r"メール停止"),
    re.compile(r"配信メール"),
    re.compile(r"以前交流会"),
    re.compile(r"ご挨拶させていただいた"),
    re.compile(r"ご紹介いただけますと幸い"),
    re.compile(r"ご提案いただけますと幸い"),
    re.compile(r"ご検討のほど"),
    re.compile(r"よろしくお願いいたします"),
    re.compile(r"よろしくお願い申し上げます"),
    re.compile(r"平素より"),
    re.compile(r"いつもお世話"),
    re.compile(r"お世話になっております"),
    re.compile(r"早速ですが"),
    re.compile(r"見合う案件"),
    re.compile(r"見合う人材"),
    re.compile(r"その他案件情報"),
    re.compile(r"営業中エンジニア一覧"),
    re.compile(r"スプレッドシート"),
    re.compile(r"SES事業者のための営業支援ツール"),
    re.compile(r"許可なく求人媒体"),
    re.compile(r"二次配信"),
    re.compile(r"ご紹介です"),
    re.compile(r"ご提案です"),
    re.compile(r"ご連絡いたしました"),
    re.compile(r"下記要員のご紹介"),
    re.compile(r"注力案件のご紹介"),
    re.compile(r"ご対応可能な要員様がいらっしゃいましたら"),
    re.compile(r"見合う方がいらっしゃいましたら"),
    re.compile(r"ご提案お待ちしております"),
    re.compile(r"何卒、よろしく"),
    re.compile(r"以上です"),
    re.compile(r"営業中の技術者"),
    re.compile(r"本日は.+ご紹介"),
    re.compile(r"この度は.+ご紹介"),
    re.compile(r"以前弊社"),
    re.compile(r"名刺交換"),
    re.compile(r"ご返信"),
    re.compile(r"ご確認いただけますと"),
    re.compile(r"リンク内よりダウンロード"),
    re.compile(r"心よりお待ちしております"),
    re.compile(r"幸甚"),
    re.compile(r"お気軽にLINE追加"),
    re.compile(r"弊社担当よりご対応"),
    re.compile(r"入社しました"),
    re.compile(r"お気軽に打ち合わせ"),
    re.compile(r"お気軽にお問い合わせ"),
    re.compile(r"お気軽にご相談"),
    re.compile(r"営業管理はこれひとつ"),
    re.compile(r"技術の力で自由に"),
    re.compile(r"時間は有限、あなたの業務をもっと楽に"),
    re.compile(r"LINEをやられているようでしたら"),
    re.compile(r"問い合わせは下記のメールでお願いします"),
    re.compile(r"表題の件でご連絡させていただきました"),
    re.compile(r"現在営業している(?:プロパー|要員)の情報をお送りさせていただきます"),
    re.compile(r"この度は、注力案件情報をお送りします"),
    re.compile(r"ご提案の際は必須尚可スキルの〇×記載をお願いいたします"),
    re.compile(r"ご確認の程、宜しくお願い申し上げます"),
    re.compile(r"案件スプレッド短縮URL"),
    re.compile(r"人材スプレッド短縮URL"),
    re.compile(r"資料ダウンロードはこちら"),
    re.compile(r"配信先変更、停止依頼はこちら"),
    re.compile(r"^詳しく$"),
    re.compile(r"^お取引先$"),
    re.compile(r"^高橋様$"),
    re.compile(r"署名のQRコードからご登録いただけますと幸いです"),
    re.compile(r"下記の要員以外にも営業中プロパー一覧をお送りしますので"),
    re.compile(r"他社へ配信、WEB公開などはご遠慮くださいませ"),
    re.compile(r"営業中プロパー一覧"),
    re.compile(r"情報交換などお打ち合わせのご連絡は以下のアドレスでお願いします"),
    re.compile(r"LINEも対応しておりますのでお気軽にご連絡ください"),
]
CONTACT_PATTERNS = [
    re.compile(r"(?i)\b(mail|email|e-mail)\b"),
    re.compile(r"(?i)\b(tel|fax|line|hp|url)\b"),
    re.compile(r"電話番号"),
    re.compile(r"携帯"),
    re.compile(r"個人メール"),
    re.compile(r"共通アドレス"),
    re.compile(r"担当営業"),
    re.compile(r"担当者"),
    re.compile(r"代表取締役"),
    re.compile(r"営業部"),
    re.compile(r"ご提案の際にはこちらにご連絡"),
    re.compile(r"ご質問"),
    re.compile(r"お問い合わせ"),
    re.compile(r"営業担当"),
    re.compile(r"スキルシート"),
    re.compile(r"LINE追加"),
]
ADDRESS_PATTERNS = [
    re.compile(r"〒\d{3}-\d{4}"),
    re.compile(r"(東京都|北海道|神奈川県|大阪府|福岡県|千葉県|埼玉県|愛知県|京都府|兵庫県).*\d"),
    re.compile(r"(区|市|町|村).*\d"),
    re.compile(r"[0-9]{1,4}(?:F|階|号室)$"),
]
COMPANY_LINE_PATTERNS = [
    re.compile(r"株式会社"),
    re.compile(r"有限会社"),
    re.compile(r"合同会社"),
    re.compile(r"Inc\.?", re.IGNORECASE),
]

REQUIRED_KEYS = [
    "pair_id",
    "score_band",
    "project_message_id",
    "resource_message_id",
    "project_subject",
    "resource_subject",
    "sanitized_project_text",
    "sanitized_resource_text",
    "sanitization_applied",
    "sanitization_notes",
]


def is_no_match_file(records: List[dict]) -> bool:
    return len(records) == 1 and records[0].get("status") == "no_match"


def require_input_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"入力ファイルが存在しません: {path}")


def normalize_text(text: str) -> str:
    text = (text or "").replace("&nbsp;", " ").replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.rstrip() for line in text.split("\n")]
    normalized: List[str] = []
    prev_blank = False
    for line in lines:
        stripped = line.strip()
        if not stripped:
            if not prev_blank:
                normalized.append("")
            prev_blank = True
            continue
        normalized.append(stripped)
        prev_blank = False
    return "\n".join(normalized).strip()


def should_remove_line(line: str) -> Tuple[bool, List[str]]:
    notes: List[str] = []
    stripped = line.strip()
    lower = stripped.lower()

    if not stripped:
        return False, notes

    if URL_RE.search(stripped):
        notes.append("url_removed")
    if DOMAIN_RE.search(stripped):
        notes.append("domain_removed")
    if EMAIL_RE.search(stripped):
        notes.append("email_removed")
    if PHONE_RE.search(stripped):
        notes.append("phone_removed")
    if LINE_ID_RE.search(stripped):
        notes.append("line_id_removed")
    if any(pattern.search(stripped) for pattern in BOILERPLATE_PATTERNS):
        notes.append("boilerplate_removed")
    if any(pattern.search(stripped) for pattern in GREETING_PATTERNS):
        notes.append("greeting_removed")
    if SEPARATOR_RE.match(stripped):
        notes.append("separator_removed")
    if any(pattern.search(stripped) for pattern in CONTACT_PATTERNS):
        notes.append("contact_removed")
    if any(pattern.search(stripped) for pattern in ADDRESS_PATTERNS):
        notes.append("address_removed")
    if BUILDING_FRAGMENT_RE.search(stripped) and not FACTUAL_PREFIX_RE.match(stripped):
        notes.append("address_removed")
    if (
        NAME_ONLY_RE.fullmatch(stripped)
        or NAME_ROMAJI_RE.fullmatch(stripped)
        or NAME_SLASH_ROMAJI_RE.fullmatch(stripped)
        or NAME_PIPE_ROMAJI_RE.fullmatch(stripped)
        or NAME_KANA_PAREN_RE.fullmatch(stripped)
    ):
        notes.append("signature_removed")
    if COMPANY_PERSON_GREETING_RE.fullmatch(stripped):
        notes.append("signature_removed")
    if stripped.startswith("担当：") or stripped.startswith("担当:"):
        notes.append("signature_removed")
    if (
        any(pattern.search(stripped) for pattern in COMPANY_LINE_PATTERNS)
        and len(stripped) <= 40
        and ("様" not in stripped)
    ):
        notes.append("signature_removed")
    if lower.startswith(("from:", "to:", "cc:", "bcc:", "reply-to:")):
        notes.append("header_removed")

    return bool(notes), sorted(set(notes))


def is_fact_like_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if FACTUAL_PREFIX_RE.match(stripped):
        return True
    fact_keywords = [
        "万円", "リモート", "常駐", "開発", "設計", "テスト", "要件定義",
        "Python", "Java", "PHP", "React", "AWS", "Node.js", "TypeScript",
        "月", "週", "名", "可", "不可", "年", "歳", "駅",
    ]
    return any(keyword in stripped for keyword in fact_keywords)


def should_remove_leading_line(line: str) -> Tuple[bool, List[str]]:
    remove_line, notes = should_remove_line(line)
    if remove_line:
        return True, notes

    stripped = line.strip()
    leading_patterns = [
        r"^[一-龥ぁ-んァ-ヶA-Za-z]{1,12}様$",
        r"^[一-龥ぁ-んァ-ヶA-Za-z0-9]+様様$",
        r"の.+です。?$",
        r"^[^【】\[\]：:]{1,30}の[一-龥ぁ-んァ-ヶA-Za-z]{1,12}(?:です|でございます)。?$",
        r"ご紹介",
        r"ご提案",
        r"ご連絡",
        r"営業中の技術者",
        r"本日は",
        r"この度は",
        r"この度は、注力案件情報をお送りします",
        r"以前弊社",
        r"名刺交換",
        r"お世話になります",
        r"お世話になっております",
        r"平素より",
        r"早速ですが",
        r"見合う",
        r"お気軽に",
        r"入社しました",
        r"打ち合わせ",
        r"お問い合わせ",
        r"ご相談",
        r"LINEをやられているようでしたら",
        r"表題の件でご連絡させていただきました",
        r"現在営業している(?:プロパー|要員)の情報をお送りさせていただきます",
        r"問い合わせは下記のメールでお願いします",
        r"お取引先$",
    ]
    if any(re.search(pattern, stripped) for pattern in leading_patterns):
        return True, ["boilerplate_removed"]
    return False, []


def should_remove_trailing_line(line: str) -> Tuple[bool, List[str]]:
    remove_line, notes = should_remove_line(line)
    if remove_line:
        return True, notes

    stripped = line.strip()
    if (
        NAME_ONLY_RE.fullmatch(stripped)
        or NAME_ROMAJI_RE.fullmatch(stripped)
        or NAME_SLASH_ROMAJI_RE.fullmatch(stripped)
        or NAME_PIPE_ROMAJI_RE.fullmatch(stripped)
        or NAME_KANA_PAREN_RE.fullmatch(stripped)
    ):
        return True, ["signature_removed"]
    if COMPANY_PERSON_GREETING_RE.fullmatch(stripped):
        return True, ["signature_removed"]
    if BUILDING_FRAGMENT_RE.search(stripped) and len(stripped) <= 40:
        return True, ["address_removed", "signature_removed"]
    trailing_patterns = [
        r"事業部$",
        r"営業$",
        r"部$",
        r"本社$",
        r"オフィス$",
        r"よろしく",
        r"お気軽に",
        r"ご質問",
        r"お問い合わせ",
        r"お待ちしております",
        r"幸甚",
        r"技術の力で自由に",
        r"営業共通",
        r"営業管理はこれひとつ",
        r"社内で情報の共有",
        r"詳しく$",
        r"^詳しく$",
        r"時間は有限、あなたの業務をもっと楽に",
        r"資料ダウンロードはこちら",
        r"配信先変更、停止依頼はこちら",
        r"案件スプレッド短縮URL",
        r"人材スプレッド短縮URL",
        r"ご提案の際は必須尚可スキルの〇×記載をお願いいたします",
        r"ご確認の程、宜しくお願い申し上げます",
        r"^\【東京本社】$",
        r"^\【大阪オフィス】$",
        r"^\【Tokyo Office】$",
        r"^\【Osaka Office】$",
        r"ブランド",
        r"スローガン",
    ]
    if any(re.search(pattern, stripped) for pattern in trailing_patterns):
        return True, ["signature_removed"]
    return False, []


def trim_edge_noise(lines: List[str]) -> Tuple[List[str], List[str]]:
    notes: List[str] = []
    trimmed = list(lines)

    while trimmed:
        remove_line, line_notes = should_remove_leading_line(trimmed[0])
        if remove_line and not is_fact_like_line(trimmed[0]):
            notes.extend(line_notes)
            trimmed.pop(0)
            continue
        break

    while trimmed:
        remove_line, line_notes = should_remove_trailing_line(trimmed[-1])
        short_name_like = bool(re.fullmatch(r"[一-龥ぁ-んァ-ヶA-Za-z\s　]{2,20}", trimmed[-1].strip()))
        if remove_line or short_name_like:
            notes.extend(line_notes)
            if short_name_like:
                notes.append("signature_removed")
            trimmed.pop()
            continue
        break

    # 末尾署名塊を追加で剥がす。短い署名系行が連続する場合のみ除去する。
    trailing_block: List[Tuple[str, List[str]]] = []
    idx = len(trimmed) - 1
    while idx >= 0:
        line = trimmed[idx].strip()
        if not line:
            idx -= 1
            continue
        remove_line, line_notes = should_remove_trailing_line(line)
        if remove_line and len(line) <= 50 and not is_fact_like_line(line):
            trailing_block.append((line, line_notes))
            idx -= 1
            continue
        break
    if len(trailing_block) >= 2:
        for _, line_notes in trailing_block:
            notes.extend(line_notes)
        trimmed = trimmed[: len(trimmed) - len(trailing_block)]

    return trimmed, sorted(set(notes))


def sanitize_mail_text(text: str) -> Tuple[str, List[str]]:
    normalized = normalize_text(text)
    if not normalized:
        return "", []

    output_lines: List[str] = []
    notes: List[str] = []

    for line in normalized.split("\n"):
        remove_line, line_notes = should_remove_line(line)
        if remove_line:
            notes.extend(line_notes)
            continue

        cleaned = line
        if DOMAIN_RE.search(cleaned):
            cleaned = DOMAIN_RE.sub("[domain removed]", cleaned)
            notes.append("domain_removed")
        if EMAIL_RE.search(cleaned):
            cleaned = EMAIL_RE.sub("[email removed]", cleaned)
            notes.append("email_removed")
        if URL_RE.search(cleaned):
            cleaned = URL_RE.sub("[url removed]", cleaned)
            notes.append("url_removed")
        if PHONE_RE.search(cleaned):
            cleaned = PHONE_RE.sub("[phone removed]", cleaned)
            notes.append("phone_removed")
        if LINE_ID_RE.search(cleaned):
            cleaned = LINE_ID_RE.sub("[line removed]", cleaned)
            notes.append("line_id_removed")

        cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
        if cleaned:
            output_lines.append(cleaned)

    output_lines, edge_notes = trim_edge_noise(output_lines)
    notes.extend(edge_notes)
    sanitized = normalize_text("\n".join(output_lines))

    if sanitized:
        return sanitized, sorted(set(notes))

    # 行単位除去で空になった場合は過剰除去を避けるため、最小限の置換版を返す
    fallback = normalized
    if DOMAIN_RE.search(fallback):
        fallback = DOMAIN_RE.sub("[domain removed]", fallback)
        notes.append("domain_removed")
    if EMAIL_RE.search(fallback):
        fallback = EMAIL_RE.sub("[email removed]", fallback)
        notes.append("email_removed")
    if URL_RE.search(fallback):
        fallback = URL_RE.sub("[url removed]", fallback)
        notes.append("url_removed")
    if PHONE_RE.search(fallback):
        fallback = PHONE_RE.sub("[phone removed]", fallback)
        notes.append("phone_removed")
    if LINE_ID_RE.search(fallback):
        fallback = LINE_ID_RE.sub("[line removed]", fallback)
        notes.append("line_id_removed")

    return normalize_text(fallback), sorted(set(notes + ["fallback_used"]))


def build_pair_id(score_band: str, project_message_id: str, resource_message_id: str) -> str:
    return f"{score_band}_{project_message_id}_{resource_message_id}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-date", help="出力対象日付 YYYYMMDD")
    return parser.parse_args()


def resolve_target_date(target_date: Optional[str]) -> str:
    if target_date:
        if not re.fullmatch(r"\d{8}", target_date):
            raise ValueError(f"--target-date は YYYYMMDD 形式で指定してください: {target_date}")
        return target_date
    return datetime.now().strftime("%Y%m%d")


def build_record(score_band: str, pair: dict, mail_master: Dict[str, dict]) -> dict:
    project_message_id = str(pair.get("project_info", {}).get("message_id", "")).strip()
    resource_message_id = str(pair.get("resource_info", {}).get("message_id", "")).strip()

    if not project_message_id or not resource_message_id:
        raise KeyError(f"message_id が不足しています: score_band={score_band} pair={pair}")

    if project_message_id not in mail_master:
        raise KeyError(
            f"mail master に案件 message_id が存在しません: score_band={score_band} "
            f"project_message_id={project_message_id} resource_message_id={resource_message_id}"
        )
    if resource_message_id not in mail_master:
        raise KeyError(
            f"mail master に要員 message_id が存在しません: score_band={score_band} "
            f"project_message_id={project_message_id} resource_message_id={resource_message_id}"
        )

    project_mail = mail_master[project_message_id]
    resource_mail = mail_master[resource_message_id]

    sanitized_project_text, project_notes = sanitize_mail_text(project_mail.get("body_text", ""))
    sanitized_resource_text, resource_notes = sanitize_mail_text(resource_mail.get("body_text", ""))

    if not sanitized_project_text:
        raise ValueError(
            f"sanitized_project_text が空です: project_message_id={project_message_id} "
            f"resource_message_id={resource_message_id}"
        )
    if not sanitized_resource_text:
        raise ValueError(
            f"sanitized_resource_text が空です: project_message_id={project_message_id} "
            f"resource_message_id={resource_message_id}"
        )

    notes = sorted(set(project_notes + resource_notes))

    record = {
        "pair_id": build_pair_id(score_band, project_message_id, resource_message_id),
        "score_band": score_band,
        "project_message_id": project_message_id,
        "resource_message_id": resource_message_id,
        "project_subject": project_mail.get("subject", ""),
        "resource_subject": resource_mail.get("subject", ""),
        "sanitized_project_text": sanitized_project_text,
        "sanitized_resource_text": sanitized_resource_text,
        "sanitization_applied": bool(notes),
        "sanitization_notes": notes,
    }

    for key in REQUIRED_KEYS:
        if key not in record:
            raise KeyError(f"必須キー不足: {key}")

    return record


def main() -> None:
    logger = get_logger(STEP_NAME)
    dirs = ensure_result_dirs(str(STEP_DIR))
    start_time = time.time()
    args = parse_args()

    try:
        for _, path in INPUT_FILES:
            require_input_file(path)
        require_input_file(MAIL_MASTER_PATH)

        today = resolve_target_date(args.target_date)
        output_path = OUTPUT_DIR / f"proposal_input_{today}.jsonl"
        logger.info(f"target date: {today}")

        mail_master = read_jsonl_as_dict(str(MAIL_MASTER_PATH), key="message_id")
        logger.info(f"メールマスタ読込件数={len(mail_master)}")

        all_records: List[dict] = []
        input_total = 0

        for score_band, input_path in INPUT_FILES:
            pairs = read_jsonl_as_list(str(input_path))
            if is_no_match_file(pairs):
                logger.info(f"{score_band}: 0件 (no_match)")
                continue

            input_total += len(pairs)
            for pair in pairs:
                record = build_record(score_band, pair, mail_master)
                all_records.append(record)

            logger.info(f"{score_band}: {len(pairs)}件整形")

        write_jsonl(str(output_path), all_records)
        logger.info(f"出力ファイル: {output_path}")
        logger.info(f"入力件数={input_total} 出力件数={len(all_records)}")

        elapsed = time.time() - start_time
        write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, len(all_records))
        logger.ok("処理完了")

    except Exception as e:
        write_error_log(str(dirs["result"]), e, context=STEP_NAME)
        logger.error(f"処理失敗: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

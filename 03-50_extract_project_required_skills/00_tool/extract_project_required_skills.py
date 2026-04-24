#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 03-50: 案件メールから必須スキル・尚可スキルをルールベースで抽出
（LLMはフォールバック限定）

入力①: 01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl
入力②: 02-2_classify_output_file_project_resource/01_result/projects.jsonl
出力①: 01_result/extract_project_required_skills.jsonl
出力②: 01_result/99_skill_null_extract_project_required_skills.jsonl
"""

import html
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Tuple

_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.llm_client import call_llm_with_fallback
from common.logger import get_logger

_TOOL_DIR = Path(__file__).resolve().parent
if str(_TOOL_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOL_DIR))
from config import USE_LLM_FALLBACK

STEP_NAME = "03-50_extract_project_required_skills"
logger = get_logger(STEP_NAME)

INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_PROJECTS = str(
    _PROJECT_ROOT / "02-2_classify_output_file_project_resource" / "01_result" / "projects.jsonl"
)
OUTPUT_EXTRACTED   = "extract_project_required_skills.jsonl"
OUTPUT_NULL        = "99_skill_null_extract_project_required_skills.jsonl"
OUTPUT_RULE_EMPTY  = "99_rule_empty_extract_project_required_skills.jsonl"

MAX_BODY_LEN_LLM = 3000


# ────────────────────────────────────────────────────────────────
# 正規化
# ────────────────────────────────────────────────────────────────

def _n(s: str) -> str:
    """NFKC正規化（全角→半角統一）"""
    return unicodedata.normalize("NFKC", s or "")


_HDR_STRIP_LEAD_RE = re.compile(r'^[\s【■□◆◇●○◎▶▼◾〈★☆~\[<「『《※*＊\-ー=－]+')
_HDR_STRIP_TAIL_RE = re.compile(r'[\s】■◆~\]>〉」』》:：*＊!！\-ー=－]+$')
_HDR_INNER_SPACE_RE = re.compile(r'[\s　\t_＿]+')  # 全角スペース・半角スペース・タブ・アンダースコア
# 絵文字バリエーションセレクタ（U+FE00-FE0F: ◾️ の ️ 部分等）を除去
_VARIATION_SELECTOR_RE = re.compile('[\ufe00-\ufe0f]')


def _normalize_hdr(line: str) -> str:
    """
    見出し判定専用の正規化。
    NFKC + バリエーションセレクタ除去 + 行頭行末装飾除去（○含む）+ 内部スペース（全角含む）除去。
    例: 「備　考」→「備考」, 「期 間」→「期間」, 「○求める人物像：」→「求める人物像」
    """
    s = _n(line.strip())
    s = _VARIATION_SELECTOR_RE.sub('', s)
    s = _HDR_STRIP_LEAD_RE.sub('', s)
    s = _HDR_STRIP_TAIL_RE.sub('', s)
    s = _HDR_INNER_SPACE_RE.sub('', s)
    return s


# 「【キー】値」形式からキーを抽出する（面 談 → 面談 のようにスペース除去）
_BRACKET_KEY_EXTRACT_RE = re.compile(r'^[【〔\[■◆]\s*([^】〕\]■◆]{1,20}?)\s*[】〕\]■◆]')


def _get_bracket_key(line: str) -> str:
    """「【キー】値」形式からキー部分を抽出し、内部スペースを除去して返す。"""
    m = _BRACKET_KEY_EXTRACT_RE.match(_n(line.strip()))
    if m:
        return _HDR_INNER_SPACE_RE.sub('', m.group(1))
    return ''


# ────────────────────────────────────────────────────────────────
# 区切り線・URLスキップ
# ────────────────────────────────────────────────────────────────

# 装飾文字のみで構成される区切り線（☆★◇― 等を含む）
_SEPARATOR_RE = re.compile(
    r'^[\s\-=*_＝＿〜~━─◇＊☆★◆■□|｜<>←→―ーヽ〰・]+$'
)
_URL_ONLY_RE = re.compile(r'^https?://\S+$')


def _should_skip(line: str) -> bool:
    """空行・区切り線・URLのみ行はスキップ。"""
    s = line.strip()
    if not s:
        return True
    if len(s) >= 4 and _SEPARATOR_RE.match(s):
        return True
    if _URL_ONLY_RE.match(s):
        return True
    return False


# ────────────────────────────────────────────────────────────────
# セクション停止条件
# ────────────────────────────────────────────────────────────────

# 正規化後コアテキストの完全一致で停止
_STOP_HDR_CORE_RE = re.compile(
    r'^(?:'
    r'期間|作業期間|契約期間|案件期間|参画期間|スタート|開始|開始日|開始時期|時期|参画開始|'
    r'勤務地|作業場所|就業場所|場所|最寄|最寄駅|勤務条件|勤務先|'
    r'単価|月額単価|希望単価|想定単価|報酬|給与|予算|'
    r'面談|面談回数|選考|選考フロー|'
    r'備考|補足|その他|注意|注意事項|'
    r'商流|商流規制|'
    r'外国籍|国籍|'
    r'年齢|年齢制限|'
    r'時間|勤務時間|就業時間|稼働時間|作業時間|所定労働時間|'
    r'精算|清算|精算幅|清算幅|精算方式|精算形式|'
    r'支払|支払サイト|支払い|'
    r'募集人数|募集|採用人数|人数|'
    r'契約形態|雇用形態|'
    r'服装|ドレスコード|'
    r'出社|出社頻度|'
    r'リモート|テレワーク|在宅|在宅勤務|'
    r'稼働|稼働率|稼働日数|稼働頻度|'
    r'所属|所属会社|'
    r'会社名|社名|'
    r'連絡先|お問合せ|お問い合わせ|'
    r'担当|担当者|営業担当|'
    r'署名|ご提案|'
    r'案件名|案件概要|案件内容|業務内容|業務概要|作業内容|'
    r'プロジェクト名|概要|'
    r'開発環境|フェーズ|工程|'
    r'求める人物像|人物像|'
    r'INFORMATION|'
    r'TEL|FAX|Email|E-mail|Mail'
    r')$',
    re.IGNORECASE
)

# 正規化後コアテキストの前方一致で停止（複合見出し対応）
_STOP_HDR_STARTS_RE = re.compile(
    r'^(?:求める人物像|人物像|希望要素|歓迎する人物|マインド|ソフトスキル|ヒューマンスキル|その他)',
    re.IGNORECASE
)

# 行全体パターン（装飾なしでも停止）
_STOP_FULL_LINE_RE = re.compile(
    r'(?:よろしくお願い|何卒よろしく|以上です|以上となります|'
    r'ご不明な点|お気軽にお問い合わせ)',
)

# 署名開始を示す行（行頭に会社形態名・連絡先フィールドが来る）
# 「株式会社XXX」は区切り文字不要。TEL/Email 等はコロン等のセパレータが必要
_SIGNATURE_STOP_RE = re.compile(
    r'^(?:株式会社|有限会社|合同会社)'
    r'|^(?:TEL|FAX|E-?[Mm]ail|Mail|Mobile|LINE|URL)\s*[：:（(【]',
    re.IGNORECASE
)

# キー：値形式の条件行（正規化後テキストに適用）
_CONDITION_KV_RE = re.compile(
    r'^(?:期間|勤務地|場所|単価|月額単価|希望単価|想定単価|単金|面談|面談回数|備考|商流|外国籍|年齢|時間|精算|清算|支払|'
    r'募集|開始|開始時期|参画時期|参画開始|リモート|稼働|出社|服装|担当|TEL|Email|Mail|会社|社名|所属|予算|'
    r'契約|就業|報酬|スタート|人数|フェーズ|工程|開発環境|作業期間|作業場所|商流制限|勤務条件)\s*[：:].+',
    re.IGNORECASE
)

# 箇条書き除去
_BULLET_STRIP_RE = re.compile(
    r'^[・\-ー●◆■□◎?※▶➤*]\s*'
    r'|^\d+[\.）)]\s*'
    r'|^[①②③④⑤⑥⑦⑧⑨⑩]\s*'
)
_HAS_BULLET_RE = re.compile(
    r'^[・\-●◆■□◎?※▶➤*]'
    r'|^\d+[\.）)]\s'
    r'|^[①②③④⑤⑥⑦⑧⑨⑩]'
)

_LEADING_INDENT_RE = re.compile(r'^[ \t　]+')


def _strip_bullet(line: str) -> str:
    return _BULLET_STRIP_RE.sub('', line).strip()


def _has_bullet(line: str) -> bool:
    return bool(_HAS_BULLET_RE.match(line.strip()))


def _has_leading_indent(raw_line: str) -> bool:
    """生の行頭に字下げ（半角/全角スペース・タブ）があるかを判定する。

    必須／尚可セクション内で、箇条書き記号なし・技術文脈語なしの
    継続行（字下げによる視覚的な列挙表現）を救済するために使用する。
    例:
        必須スキル：Java開発経験
              フロントエンドの開発経験(Javascript.HTML.CSS)
              コミュニケーションよく自立して作業を進められる方
    """
    if not raw_line:
        return False
    return bool(_LEADING_INDENT_RE.match(raw_line))


def _is_stop_section(line: str) -> bool:
    """
    この行がセクション終端かどうかを判定する。
    _normalize_hdr() で文字間スペース・装飾を除去してから判定する。
    「【キー】値」形式（例: 【単価】70万）もキーを抽出して判定する。
    """
    s = line.strip()
    if not s:
        return False
    # 行全体パターン
    if _STOP_FULL_LINE_RE.search(s):
        return True
    # 署名開始行（会社名・連絡先フィールド）
    if _SIGNATURE_STOP_RE.match(_n(s)):
        return True
    # 「【キー】値」形式: キー部分だけ取り出して判定
    bk = _get_bracket_key(s)
    if bk and (_STOP_HDR_CORE_RE.match(bk) or _STOP_HDR_STARTS_RE.match(bk)):
        return True
    # 正規化後の全体テキストで判定
    norm = _normalize_hdr(s)
    if not norm:
        return False
    if _CONDITION_KV_RE.match(norm):
        return True
    if _STOP_HDR_CORE_RE.match(norm):
        return True
    if _STOP_HDR_STARTS_RE.match(norm):
        return True
    # 「キー:値」形式で、キー部分がstopワードなら停止（例: 募集人数:2名）
    colon_pos = norm.find(':')
    if colon_pos > 0:
        key_part = norm[:colon_pos]
        if _STOP_HDR_CORE_RE.match(key_part) or _STOP_HDR_STARTS_RE.match(key_part):
            return True
    return False


# セクション内専用停止語（「期間 2026年4月〜」のようにコロンなしで値が続く場合に対応）
# _is_stop_section で捕捉できない「停止語+空白+値」形式を捕捉する
_SECTION_STOP_PREFIX_RE = re.compile(
    r'^(?:期間|勤務地|場所|単価|面談|備考|商流|外国籍|年齢|時間|精算|'
    r'募集人数|募集|会社名|社名|連絡先|担当|所属|求める人物像|人物像|環境)'
    r'(?:[\s　：:\d（(【]|$)',
    re.IGNORECASE
)


def _is_section_stop(line: str) -> bool:
    """
    STATE_REQUIRED / STATE_OPTIONAL セクション内専用の終端判定。
    _is_stop_section（グローバル判定）に加えて以下を追加でカバーする:
      - 「期間 2026年4月〜」のようなコロンなし停止語+値 形式
      - 【キー】値 形式でキーが停止語一覧に一致する場合
    """
    # グローバル停止判定（署名・ブラケット・KV・完全一致）を優先適用
    if _is_stop_section(line):
        return True
    s = _n(line.strip())
    if not s:
        return False
    # 先頭装飾を除去して停止語前方一致を確認
    s_stripped = _HDR_STRIP_LEAD_RE.sub('', s)
    if _SECTION_STOP_PREFIX_RE.match(s_stripped):
        return True
    # 【キー】値 形式でキーが停止語一覧に一致
    bk = _get_bracket_key(s)
    if bk and _SECTION_STOP_PREFIX_RE.match(bk):
        return True
    return False


# ────────────────────────────────────────────────────────────────
# required / optional 見出しパターン（正規化後に適用）
# ────────────────────────────────────────────────────────────────

_REQUIRED_HDR_NORM_RE = re.compile(
    r'^(?:'
    r'必須(?:スキル(?:[・/／]経験)?|条件|要件(?:[/／]スキル)?|資格)?'  # 必須スキル / 必須スキル・経験 / 必須要件/スキル
    r'|必要(?:スキル(?:[・/／]経験)?|条件)?'              # 必要スキル / 必要スキル・経験 / 必要(単体)
    r'|要求(?:スキル(?:[・/／]経験)?|条件|要件)?'          # 要求スキル / 要求条件
    r'|要望(?:スキル(?:[・/／]経験)?|条件|要件)?'          # 要望スキル / 要望条件
    r'|募集条件スキル|スキル必須要件|募集要件'
    r'|応募資格|求めるスキル|スキル要件|技術要件'
    r'|実務経験|現場要望'
    r'|スキル|スキルセット|スキル条件'
    r'|Required|MUST|Must'
    r')$',
    re.IGNORECASE
)

_OPTIONAL_HDR_NORM_RE = re.compile(
    r'^(?:'
    r'尚可(?:スキル(?:[・/／]経験)?|条件|要件)?'
    r'|歓迎(?:スキル(?:[/／]経験)?|条件|する経験)?'      # 歓迎スキル / 歓迎スキル/経験
    r'|優遇(?:スキル|条件)'                               # 優遇スキル 追加
    r'|あれば尚可|あると(?:望ましい|尚可|嬉しい(?:スキル(?:[/／]経験)?)?)'  # あると嬉しいスキル(/経験)
    r'|以下[、，]?あると(?:良い|望ましい|尚可|嬉しい)(?:スキル(?:[・/／]経験)?)?'  # 以下、あると嬉しいスキル
    r'|望ましい(?:スキル|条件|経験)?|プラス要素'
    r'|希望(?:スキル|条件|要件)?'
    r'|Preferred|WANT|Want'
    r')$',
    re.IGNORECASE
)

# 行末の「(must)」「(尚可)」等、スキル名に付随するインラインラベル（セクションヘッダーではなくラベル）
# 例: 「Prisma Accessの設計、運用経験(must)」→ skill="Prisma Accessの設計、運用経験"
_INLINE_SKILL_MUST_RE = re.compile(
    r'\s*(?:\(\s*must\s*\)|\(\s*必須\s*\))\s*$',
    re.IGNORECASE
)
_INLINE_SKILL_OPT_RE = re.compile(
    r'\s*(?:\(\s*optional\s*\)|\(\s*尚可\s*\)|\(\s*want\s*\))\s*$',
    re.IGNORECASE
)

# 行頭の「(必須) skill」「(尚可) skill」ラベル（one-shot スキル抽出）
# 例: 「(必須) IBMホスト上でのPL/Iの開発経験」→ required_skill
# 例: 「(尚可) JP1」→ optional_skill
# スキルテキストが後続する場合のみマッチ（ラベルのみは section header として扱う）
_INLINE_PREFIX_REQ_RE = re.compile(
    r'^\(\s*(?:必須|MUST|Must)\s*\)\s*(.*\S)',
    re.IGNORECASE
)
_INLINE_PREFIX_OPT_RE = re.compile(
    r'^\(\s*(?:尚可|WANT|Want|optional)\s*\)\s*(.*\S)',
    re.IGNORECASE
)

# 「必須：skill」「尚可：skill」行頭コロン形式（one-shot スキル抽出）
# 例: 「必須：AIX設計構築経験者」→ required_skill
# 例: 「尚可：Java,springboot」→ optional_skill
_COLON_REQ_RE = re.compile(r'^必\s*須\s*[：:]\s*(.*\S)', re.IGNORECASE)
_COLON_OPT_RE = re.compile(r'^尚\s*可\s*[：:]\s*(.*\S)', re.IGNORECASE)

# 擬似リストヘッダ（単独スキルではなく直下の箇条書きを束ねる導入句）
# 例: 「以下いずれかの設計・構築経験」「以下の経験」「下記いずれか」「次のいずれか」
# これらが「尚可： <header>」の inline 値になっている場合、one-shot スキルではなく
# セクションヘッダとして扱い、後続の箇条書きをそのセクション配下で拾えるようにする。
_PSEUDO_LIST_HEADER_RE = re.compile(
    r'^(?:以下|下記|次の)(?:いずれか|の)',
    re.IGNORECASE
)


def _is_pseudo_list_header(text: str) -> bool:
    """inline 値が擬似リストヘッダ（直下の箇条書きを束ねる導入句）かを判定する。"""
    if not text:
        return False
    return bool(_PSEUDO_LIST_HEADER_RE.match(text.strip()))

# 「必須条件: ...」「尚可条件: ...」「必須スキル: ...」形式（section header + optional inline）
# 例: 「必須条件:」→ required_header; 「必須条件: Python」→ required_header with inline
_COLON_COND_REQ_RE = re.compile(
    r'^必\s*須(?:条件|スキル|要件|スキル・経験|スキル/経験|スキル／経験)?\s*[：:]\s*(.*)',
    re.IGNORECASE
)
_COLON_COND_OPT_RE = re.compile(
    r'^尚\s*可(?:条件|スキル|要件|スキル・経験|スキル/経験|スキル／経験)?\s*[：:]\s*(.*)',
    re.IGNORECASE
)

# 「スキル・経験 content」行頭ラベル形式（required_header として処理）
# 例: 「スキル・経験 富士通Solarisのミドルウェア経験。」
_SKILL_LABEL_INLINE_RE = re.compile(r'^スキル・経験\s+(.*\S)')

# インライン Must/Want 検出（「経験・スキル 【Must】」「【Want】〜」等）
_INLINE_REQ_RE = re.compile(
    r'(?:【|■|◆|\[|（|\(|<|＜)\s*(?:必須(?:スキル|条件|要件)?|MUST|Must)\s*(?:】|\]|）|\)|>|＞|：|:|$)',
    re.IGNORECASE
)
_INLINE_OPT_RE = re.compile(
    r'(?:【|■|◆|\[|（|\(|<|＜)\s*(?:尚可(?:スキル|条件|要件)?|WANT|Want|希望(?:スキル|条件|要件)?)\s*(?:】|\]|）|\)|>|＞|：|:|$)',
    re.IGNORECASE
)

# 「スキル：・A・B・C」のような見出し+埋め込み列挙の検出（◇/■ は正規化で除去済み）
_SKILL_HDR_WITH_INLINE_RE = re.compile(
    r'^(?:スキル|スキルセット|スキル要件|スキル条件)[：:](.+)',
    re.IGNORECASE
)

# 埋め込み列挙の分割文字（「・A・B」→「A」「B」）
_INLINE_BULLET_SPLIT_RE = re.compile(r'[・○]')

# インラインヘッダー後のコンテンツ抽出
_INLINE_HDR_SPLIT_RE = re.compile(
    r'^.*?(?:【(?:必須(?:スキル|条件|要件)?|MUST|Must|尚可(?:スキル|条件|要件)?|WANT|Want|希望(?:スキル|条件|要件)?)】'
    r'|(?:必須(?:スキル|条件|要件)?|MUST|Must|尚可(?:スキル|条件|要件)?|WANT|Want|希望(?:スキル|条件|要件)?)\s*[：:])\s*(.*)',
    re.IGNORECASE
)


def _classify_line(line: str) -> Tuple[str, str]:
    """
    行を分類する。
    Returns: (kind, inline_content)
      kind: 'required_header' | 'optional_header' | 'required_skill' | 'optional_skill' | 'stop' | 'content'
      inline_content: 見出しと同一行にあるコンテンツ（通常は空文字）
      ※ required_skill / optional_skill は one-shot 抽出（STATE_NONE に戻る）
    """
    # 停止条件を最優先
    if _is_stop_section(line):
        return 'stop', ''

    norm = _normalize_hdr(line)

    # 正規化後の完全一致で required/optional 判定
    if norm and _REQUIRED_HDR_NORM_RE.match(norm):
        return 'required_header', ''
    if norm and _OPTIONAL_HDR_NORM_RE.match(norm):
        return 'optional_header', ''

    # 注釈・条件付け部分を除去して再照合（例: 「スキル ※すべてを満たすこと」→「スキル」）
    short_norm = re.split(r'[（(※]', norm)[0].rstrip()
    short_norm = re.sub(r'(?:等|など)$', '', short_norm)
    if short_norm and short_norm != norm:
        if _REQUIRED_HDR_NORM_RE.match(short_norm):
            return 'required_header', ''
        if _OPTIONAL_HDR_NORM_RE.match(short_norm):
            return 'optional_header', ''

    # ブラケット見出し+補足テキスト形式: 「【必要スキル】 以下の◎は...」
    # 「【スキル】必須：skill」「【スキル】尚可：skill」形式も対応
    # 後者は one-shot (STATE_NONE 維持) として処理し、後続の説明行を吸収しない
    bk = _get_bracket_key(line)
    if bk:
        bk_norm = _normalize_hdr(bk)
        # 「【尚可/共通】」のように / で区切られた複合キーは先頭部分で照合
        bk_short = re.split(r'[/／]', bk_norm)[0]
        _req_match = (bk_norm and _REQUIRED_HDR_NORM_RE.match(bk_norm)) or (bk_short and bk_short != bk_norm and _REQUIRED_HDR_NORM_RE.match(bk_short))
        if _req_match:
            bracket_end = line.find('】')
            after_bk = ''
            if bracket_end >= 0:
                after_bk = _n(line[bracket_end + 1:].strip())
                m_req = _COLON_REQ_RE.match(after_bk)
                if m_req:
                    inline = m_req.group(1).strip()
                    if _is_pseudo_list_header(inline):
                        return 'required_header', inline
                    return 'required_skill', inline
                m_opt = _COLON_OPT_RE.match(after_bk)
                if m_opt:
                    inline = m_opt.group(1).strip()
                    if _is_pseudo_list_header(inline):
                        return 'optional_header', inline
                    return 'optional_skill', inline
            return 'required_header', after_bk
        _opt_match = (bk_norm and _OPTIONAL_HDR_NORM_RE.match(bk_norm)) or (bk_short and bk_short != bk_norm and _OPTIONAL_HDR_NORM_RE.match(bk_short))
        if _opt_match:
            bracket_end = line.find('】')
            after_bk = ''
            if bracket_end >= 0:
                after_bk = _n(line[bracket_end + 1:].strip())
            return 'optional_header', after_bk

    # 「スキル：(必須) skill」「スキル：・A・B」のような見出し+埋め込み列挙
    m_ski = _SKILL_HDR_WITH_INLINE_RE.match(norm)
    if m_ski:
        inline_raw = m_ski.group(1).strip()
        # inline が「(必須) skill」「(尚可) skill」形式なら one-shot 抽出
        m_req = _INLINE_PREFIX_REQ_RE.match(inline_raw)
        if m_req:
            return 'required_skill', m_req.group(1).strip()
        m_opt = _INLINE_PREFIX_OPT_RE.match(inline_raw)
        if m_opt:
            return 'optional_skill', m_opt.group(1).strip()
        # inline が「必須：skill」「尚可：skill」コロン形式なら one-shot 抽出
        # 例: 「スキル：必　須： VB.NET（C/S）」→ required_skill
        m_colon_req = _COLON_REQ_RE.match(inline_raw)
        if m_colon_req:
            inline = m_colon_req.group(1).strip()
            if _is_pseudo_list_header(inline):
                return 'required_header', inline
            return 'required_skill', inline
        m_colon_opt = _COLON_OPT_RE.match(inline_raw)
        if m_colon_opt:
            inline = m_colon_opt.group(1).strip()
            if _is_pseudo_list_header(inline):
                return 'optional_header', inline
            return 'optional_skill', inline
        return 'required_header', inline_raw

    # インライン Must/Want 検出（「経験・スキル 【Must】」「【Must】〜内容〜」等）
    n_line = _n(line)

    # 行頭の「(必須) skill」「(尚可) skill」ラベル（one-shot）
    # _INLINE_REQ_RE より先に判定（(必須) が INLINE_REQ_RE にもマッチするため）
    m_prefix_req = _INLINE_PREFIX_REQ_RE.match(n_line)
    if m_prefix_req:
        return 'required_skill', m_prefix_req.group(1).strip()
    m_prefix_opt = _INLINE_PREFIX_OPT_RE.match(n_line)
    if m_prefix_opt:
        return 'optional_skill', m_prefix_opt.group(1).strip()

    # 「必須：skill」「尚可：skill」行頭コロン形式（one-shot スキル抽出）
    # inline が「以下いずれか〜」等の擬似リストヘッダなら header に降格し、
    # 後続の箇条書きをそのセクション配下で拾う。
    m_colon_req = _COLON_REQ_RE.match(n_line)
    if m_colon_req:
        inline = m_colon_req.group(1).strip()
        if _is_pseudo_list_header(inline):
            return 'required_header', inline
        return 'required_skill', inline
    m_colon_opt = _COLON_OPT_RE.match(n_line)
    if m_colon_opt:
        inline = m_colon_opt.group(1).strip()
        if _is_pseudo_list_header(inline):
            return 'optional_header', inline
        return 'optional_skill', inline

    # 「必須条件: ...」「尚可条件: ...」形式（section header, inline があれば渡す）
    # 「必須: skill」は _COLON_REQ_RE で処理済みのため、ここは複合語のみ対象
    m_cond_req = _COLON_COND_REQ_RE.match(n_line)
    if m_cond_req:
        inline = m_cond_req.group(1).strip()
        if _is_pseudo_list_header(inline):
            return 'required_header', inline
        return 'required_header', inline
    m_cond_opt = _COLON_COND_OPT_RE.match(n_line)
    if m_cond_opt:
        inline = m_cond_opt.group(1).strip()
        if _is_pseudo_list_header(inline):
            return 'optional_header', inline
        return 'optional_header', inline

    # 「スキル・経験 content」形式（required_header として処理）
    m_ski_label = _SKILL_LABEL_INLINE_RE.match(n_line)
    if m_ski_label:
        return 'required_header', m_ski_label.group(1).strip()

    # 行末ラベル「(must)」「(尚可)」はセクションヘッダーではなくスキルのラベル
    # → skill テキストとして inline_content に返してセクション変遷させる
    m_skill_must = _INLINE_SKILL_MUST_RE.search(n_line)
    if m_skill_must:
        skill_text = _INLINE_SKILL_MUST_RE.sub('', n_line).strip()
        return 'required_header', skill_text
    m_skill_opt = _INLINE_SKILL_OPT_RE.search(n_line)
    if m_skill_opt:
        skill_text = _INLINE_SKILL_OPT_RE.sub('', n_line).strip()
        return 'optional_header', skill_text

    if _INLINE_REQ_RE.search(n_line):
        m = _INLINE_HDR_SPLIT_RE.match(n_line)
        inline = m.group(1).strip() if m else ''
        return 'required_header', inline
    if _INLINE_OPT_RE.search(n_line):
        m = _INLINE_HDR_SPLIT_RE.match(n_line)
        inline = m.group(1).strip() if m else ''
        return 'optional_header', inline

    return 'content', ''


# ────────────────────────────────────────────────────────────────
# スキル候補行の採用条件
# ────────────────────────────────────────────────────────────────

# スキル文脈語（これを含む行のみ採用）
_SKILL_CONTEXT_RE = re.compile(
    r'経験|開発|設計|構築|運用|保守|テスト|実装|言語|フレームワーク|チューニング|'
    r'データベース|インフラ|サーバ|ネットワーク|クラウド|'
    r'VBA|マクロ|Excel|Word|PowerPoint|Outlook|'
    r'Java|Python|Go|Ruby|PHP|JavaScript|TypeScript|C\+\+|C#|Swift|Kotlin|'
    r'AWS|Azure|GCP|Docker|Kubernetes|Linux|Windows|Unix|'
    r'Spring|React|Vue|Angular|Django|Rails|Laravel|Node|'
    r'SQL|Oracle|MySQL|PostgreSQL|MongoDB|Redis|'
    r'Git|CI/CD|Agile|スクラム|上流|下流|基本設計|詳細設計|結合|単体|'
    r'折衝|マネジメント|リーダー|PL|PM|SE|エンジニア|プログラミング|コーディング|'
    r'要件定義|アーキテクチャ|API|REST',
    re.IGNORECASE
)

# 人物像・スタンス文脈語。
# required / optional セクション内の字下げ救済でのみ使い、条件面の行は別途 reject する。
_PERSON_SKILL_CONTEXT_RE = re.compile(
    r'コミュニケーション|コミュ力|協調|柔軟|主体|積極|報連相|報告連絡相談|'
    r'報告・連絡・相談|チームワーク|責任感|素直|自立|自走|一人称',
    re.IGNORECASE
)

# カンマ区切りのスキル列挙（短い行のみ）
_COMMA_SKILL_RE = re.compile(r'^[^、。\n]{1,50}[,，、][^、。\n]{1,50}$')

# 採用しない行のパターン
_REJECT_CONTENT_RE = re.compile(
    r'(?:駅近辺|お客様ビル|駅周辺にも出社|にて勤務可能|'
    r'\d+歳|40代|50代|60代|30代|\d+万|\d{1,2}:\d{2}|'
    r'コミュニケーション(?:良好|能力が高い)|協調性|責任感|勤怠良好|'
    r'平日勤務|代休|長期休暇|'
    r'個人事業主|貴社所属|並行営業|'
    r'ご提案いただく|ご紹介いただく|記載いただく|ご連絡お待ち|提案いただけますと|'
    r'株式会社|有限会社|合同会社|'
    r'(?:TEL|Email|Mail|担当)\s*[：:]|'
    r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}|'
    # 「→」「└」で始まる補足・説明・理由文
    # 「→Java/Python」のような技術列挙は explanation キーワードを含まないため通過する
    r'^[→└].*(?:だけでなく|ではなく|のため|ために|ながらの|があるため|取得相当)|'
    # ── 否定条件・禁止文 ──
    # 「のみは不可」は _is_non_skill_explanatory_line で条件付き判定（※や括弧内は除外しない）
    r'できません|'                           # 「確保できません」等の否定文
    r'未経験者|'                             # 「未経験者が〜」
    # ── 回答依頼・記入依頼 ──
    r'[〇○◯]\s*[×✕xX]|'                    # ○× チェック依頼
    r'お願い(?:いたし|致し|し)ます|'          # 依頼文全般
    # ── 説明文・見出し的フレーズ ──
    r'下記.*に当てはまる|'                    # 「下記経験に当てはまる項目〜」
    r'キャッチアップ.*(?:時間|期間))',         # 「キャッチアップする時間が〜」
    re.IGNORECASE
)

_MAX_SKILL_LINE_LEN = 80

# セクション見出し・説明文（スキル行として採用しない）
# 例: 「【リーダークラス】」「以下いずれかに該当する方」
_SECTION_HDR_REJECT_RE = re.compile(
    r'^(?:'
    r'[【\[（(＜<][^】\]\）)＞>\n]*[】\]\）)＞>]'           # 【〜】 形式の見出し全体
    r'|以下(?:いずれか|の(?:いずれか|条件|要件|スキル|うち))'  # 「以下いずれか〜」
    r'|下記(?:いずれか|の(?:いずれか|条件|要件|スキル))'       # 「下記いずれか〜」
    r')\s*(?:に該当|のいずれか|の(?:方|スキル)|$)',
    re.IGNORECASE
)

# 見出し単体の行を除外するための完全一致セット。
# _normalize_hdr 適用後の文字列と照合する（装飾記号・スペース除去済み）。
# ルール: substring ではなく完全一致のみ reject するため、
#         「インフラ構築経験」「CI/CD環境の構築経験」のような実スキル文は除外されない。
_STANDALONE_HEADING_NORMS: frozenset = frozenset([
    # ユーザー指定の最低限対象
    'ベーススキル', 'テクニカルスキル', '技術スタック', '対応工程',
    '歓迎スキル', '歓迎スキル・経験', '歓迎スキル/経験', '歓迎スキル／経験',
    '歓迎スキル経験', '優先スキル', '歓迎経験', '尚可スキル', '尚可条件',
    # 人物像セクション見出し単体（配下の箇条書きは残す）
    '人物想定', '求める人物', '求められる人物像',
    '条件',
    # 「以下いずれかを〜」系（_SECTION_HDR_REJECT_RE でカバーできない suffix パターン）
    '以下いずれかを満たすこと', '以下いずれかに該当すること',
    '以下のいずれかを満たすこと', '以下のいずれかに該当すること',
    '以下いずれかの経験があること',
    '下記いずれかを満たすこと', '下記いずれかに該当すること',
    # 条件・注記見出し単体
    '諸条件', '応募条件', '注意事項', '補足事項',
    # 技術スタック配下の分類見出し
    'フロント', 'フロントエンド', 'バックエンド', 'インフラ',
    'DB', 'AI', '管理', 'デザイン', '認証', '開発支援',
    'CI/CD',
])

_NON_SKILL_PREFIX_RE = re.compile(
    r'^(?:'
    r'募集ポジション|開催場所|会場|形式|交通費|宿泊費|勤務場所|勤務地|勤務時間|就業時間|'
    r'勤務形態|勤務制約|現地作業|出張|募集ID|地域|期間|日程|リモート|単価|金額|'
    r'支払いサイト|清算幅|商流制限|面談回数|その他|備考|条件'
    r')\s*[：:].*$',
    re.IGNORECASE
)

_NON_SKILL_CONSTRAINT_RE = re.compile(
    r'(?:出張|現地作業|交通費|宿泊費|勤務時間|就業時間|勤務場所|会場|'
    r'通勤可能|実費精算|地方だった場合|フルリモート|リモート開催|'
    r'外国籍|国籍|出社|リモート|テレワーク|在宅|勤務|稼働|長期参画|長期稼働|'
    r'年齢|歳まで|代まで|単価|月額|精算|清算|開始|参画時期|日程|時期|'
    r'ご経験が浅くても可|経験が浅くても可)',
    re.IGNORECASE
)

_NON_SKILL_ALWAYS_RE = re.compile(
    r'(?:'
    r'外国籍(?:不可|可|相談|NG|NG不可)?'
    r'|長期参画(?:いただける|可能|できる)?方?'
    r'|ご経験が浅くても可'
    r'|経験が浅くても可'
    r')',
    re.IGNORECASE
)

_ARROW_GUIDE_RE = re.compile(
    r'^[→└]\s*.*(?:'
    r'のため|ために|ため、|だった場合|となります|してください|ください|お願い|'
    r'ご安心|可能な方|発生します|発生する|対応可能|ご相談'
    r').*',
    re.IGNORECASE
)

_REQUEST_INSTRUCTION_RE = re.compile(
    r'(?:'
    r'スキル.*[○◯〇]\s*×.*(?:回答|コメント|チェック|記載)'
    r'|[○◯〇]\s*×.*(?:回答|コメント|チェック|記載)'
    r'|コメントお願いいたします'
    r'|チェックをお願いします'
    r'|ご教示いただけますと幸いです'
    r'|ご回答ください'
    r'|ご記載ください'
    r')',
    re.IGNORECASE
)

_CONDITION_EXPLANATION_RE = re.compile(
    r'(?:'
    r'必須の条件となります'
    r'|条件となります'
    r'|稼働条件[\(（].*[\)）]'
    r'|条件を満たせる方'
    r'|週\d+\s*日.*(?:稼働|出社)'
    r'|土日.*(?:稼働|出社)'
    r'|出社.*(?:満たせる方|可能な方)'
    r')',
    re.IGNORECASE
)


def _is_non_skill_explanatory_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False

    content = _strip_bullet(stripped) if _has_bullet(stripped) else stripped
    if not content:
        return False

    if _NON_SKILL_PREFIX_RE.match(content):
        return True

    if _REQUEST_INSTRUCTION_RE.search(content):
        return True

    if _NON_SKILL_ALWAYS_RE.search(content):
        return True

    if _CONDITION_EXPLANATION_RE.search(content) and not _SKILL_CONTEXT_RE.search(content):
        return True

    if _ARROW_GUIDE_RE.match(stripped) and not _SKILL_CONTEXT_RE.search(content):
        return True

    if _NON_SKILL_CONSTRAINT_RE.search(content) and not _SKILL_CONTEXT_RE.search(content):
        return True

    # 「〜のみは不可」「〜のみ不可」: 禁止文だがスキルの補足条件として使われる場合がある
    # ※付き（例: ※読み書きのみは不可）や括弧内（例: (個人ツール作成のみは不可)）は
    # スキル行の補足条件なので除外しない。それ以外は禁止文として除外する。
    if re.search(r'のみは?不可', content):
        if '※' not in content and not re.search(r'[（(][^）)]*のみは?不可', content):
            return True

    return False


def _is_skill_line(line: str, in_section: bool = False, indented: bool = False) -> bool:
    """
    スキル候補として採用してよい行かどうかを判定する。

    in_section=True のとき（required / optional セクション内）:
      箇条書き行は stop/reject でなければ採用（スキル文脈語不要）。
      「FTPの理解」「一人称で作業ができる方」など技術語が弱い要件行を拾うため。
      indented=True の場合でも、スキル文脈語または人物像文脈語を含む行だけ採用する。
      字下げは条件面の説明にも使われるため、字下げ単独では採用しない。
    in_section=False のとき（セクション外）:
      スキル文脈語を含む行 OR カンマ列挙行のみ採用。
    """
    s = line.strip()
    if not s or len(s) < 2:
        return False
    if _should_skip(s):
        return False
    if _is_stop_section(s):
        return False
    if _REJECT_CONTENT_RE.search(s):
        return False
    if _is_non_skill_explanatory_line(s):
        return False

    content = _strip_bullet(s) if _has_bullet(s) else s
    if not content:
        return False
    if _is_stop_section(content) or _REJECT_CONTENT_RE.search(content):
        return False
    if _is_non_skill_explanatory_line(content):
        return False

    # セクション見出し・説明文は採用しない（箇条書きでも）
    if _SECTION_HDR_REJECT_RE.search(content):
        return False

    # 見出し単体の完全一致チェック（装飾除去後の正規化テキストで判定）
    # 「インフラ構築経験」のような実スキル文は除外しない
    if _normalize_hdr(content) in _STANDALONE_HEADING_NORMS:
        return False

    # 擬似リストヘッダ（「以下いずれかの〜」等）は単独スキルではなく
    # 直下の箇条書きを束ねる導入句なので採用しない。
    if _is_pseudo_list_header(content):
        return False

    # required / optional セクション内は箇条書きを広く採用（stop/reject 以外）
    if in_section and _has_bullet(s):
        return True

    # required / optional セクション内で、字下げによる継続行を限定採用する。
    # 「必須スキル：Java開発経験\n      フロントエンドの開発経験(…)\n      コミュニケーションよく…」
    # のように bullet 記号がないが、技術/人物像文脈語を含む行だけ救済。
    # ただし「【時期】:…」のような装飾ラベル付き属性ヘッダは救済対象外にする
    # （本来 stop セクションだが stop 定義の取りこぼしで到達するケースのガード）。
    if in_section and indented and len(content) <= _MAX_SKILL_LINE_LEN:
        if not re.match(r'^[【\[＜<]', content) and (
            _SKILL_CONTEXT_RE.search(content)
            or _PERSON_SKILL_CONTEXT_RE.search(content)
        ):
            return True

    # スキル文脈語を含む短い行
    if len(content) <= _MAX_SKILL_LINE_LEN and _SKILL_CONTEXT_RE.search(content):
        return True

    # カンマ区切りスキル列挙
    if _COMMA_SKILL_RE.match(content):
        return True

    return False


# ────────────────────────────────────────────────────────────────
# 要員紹介メール判定
# ────────────────────────────────────────────────────────────────

# 案件メールと確定できるキーワード（1語でも含む場合は要員紹介と判定しない）
_PROJECT_INDICATOR_KWS: frozenset = frozenset([
    '案件名', '案件概要', '案件内容', '作業内容',
    '必須スキル', '必要スキル', '尚可スキル', '募集要件',
    '募集人数', '作業場所', '単価',
])

# 個人プロフィール特有のキーワード（案件メールには出にくいもの限定）
# 「要員」「ご紹介」「参画可能」のような案件メールにも出る汎用語は除外
_RESOURCE_INTRO_KEYWORDS = [
    'スキルシート', '経歴書', '単金', '稼働可能日', '着任可能日',
    '希望案件', '保有スキル', '希望単価', '経験歴', '並行営業',
    '業務経歴書', '個人参画可能', 'チーム参画可能',
    'エンジニア紹介', '提案可能', '要員情報', '性別:', '国籍:',
]
_RESOURCE_INTRO_THRESHOLD = 3


def _is_resource_introduction(body: str) -> bool:
    # 全角スペース「性　別」→NFKC→「性 別」のようなケースに対応するため
    # キーワード検索時はスペースを除去した本文で判定する
    normalized = _HDR_INNER_SPACE_RE.sub('', _n(body))
    # 個人プロフィール語が閾値以上ある場合は案件キーワード判定より優先して要員紹介と判定する。
    # 「希望単価」が単価を含むため案件扱いされるが、他に複数の要員語があるケースを救済する。
    count = sum(1 for kw in _RESOURCE_INTRO_KEYWORDS if kw in normalized)
    if count >= _RESOURCE_INTRO_THRESHOLD:
        return True
    # 案件キーワードが1語でもあれば案件メール優先（要員紹介ではない）
    for kw in _PROJECT_INDICATOR_KWS:
        if kw in normalized:
            return False
    # 個人プロフィール語が閾値以上ある場合のみ要員紹介と判定
    return count >= _RESOURCE_INTRO_THRESHOLD


# ────────────────────────────────────────────────────────────────
# インライン補助パターン（セクション外）
# ────────────────────────────────────────────────────────────────

_TECH_ELEMENT_RE = re.compile(r'^技術要素\s*[：:]\s*.+$')
_DESIRABLE_RE = re.compile(
    r'.{3,}(?:だと望ましい|が望ましい|であることが望ましい|であれば尚可|あると望ましい)'
)


def _make_skill(text: str) -> Dict[str, Any]:
    return {"skill": text, "match": None, "note": None}


_PSEUDO_CONTEXT_EXTRACT_RE = re.compile(
    r'^(?:以下|下記|次の)(?:の)?(?:いずれか|うち)?(?:の)?(.+)$',
    re.IGNORECASE
)

_PSEUDO_CONTEXT_REQUIRED_RE = re.compile(
    r'(?:'
    r'設計(?:・|/|／)?構築経験|設計・構築経験|設計/構築経験|設計／構築経験|'
    r'設計経験|構築経験|開発経験|運用経験|保守経験|導入経験|'
    r'ミドルウェア経験|インフラ経験'
    r')',
    re.IGNORECASE
)

_CHILD_HAS_PROCESS_CONTEXT_RE = re.compile(
    r'経験|設計|構築|開発|運用|保守|導入|実装|テスト|要件定義|基本設計|詳細設計',
    re.IGNORECASE
)

_PSEUDO_CHILD_TOKEN_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9#+./_\- ]{1,40}$')


def _extract_pseudo_parent_context(header: str) -> str:
    """擬似ヘッダから子スキルへ継承してよい文脈部だけを取り出す。"""
    if not header or not _is_pseudo_list_header(header):
        return ''
    text = _strip_bullet(header.strip())
    m = _PSEUDO_CONTEXT_EXTRACT_RE.match(text)
    context = m.group(1).strip() if m else ''
    if not context or not _PSEUDO_CONTEXT_REQUIRED_RE.search(context):
        return ''
    return context


def _complete_skill_with_pseudo_context(skill: str, pseudo_header: str) -> str:
    """擬似ヘッダ直下の短い子スキルに、親の工程文脈を補完する。"""
    child = skill.strip()
    if not child:
        return child
    context = _extract_pseudo_parent_context(pseudo_header)
    if not context:
        return child
    if _CHILD_HAS_PROCESS_CONTEXT_RE.search(child):
        return child
    if context in child:
        return child

    completed = f"{child}の{context}"
    if len(completed) > _MAX_SKILL_LINE_LEN:
        return child
    return completed


def _is_pseudo_context_child_line(line: str, pseudo_header: str) -> bool:
    """強い擬似ヘッダ直下に限り、短い製品名・技術名だけの行を子スキルとして扱う。"""
    if not _extract_pseudo_parent_context(pseudo_header):
        return False
    content = _strip_bullet(line.strip()) if _has_bullet(line) else line.strip()
    if not content or len(content) > 40:
        return False
    if _should_skip(content) or _is_stop_section(content):
        return False
    if _is_non_skill_explanatory_line(content):
        return False
    if _is_pseudo_list_header(content):
        return False
    return bool(_PSEUDO_CHILD_TOKEN_RE.match(content))


# ────────────────────────────────────────────────────────────────
# 状態定数
# ────────────────────────────────────────────────────────────────

STATE_NONE     = "none"
STATE_REQUIRED = "required"
STATE_OPTIONAL = "optional"
STATE_DONE     = "done"


# ────────────────────────────────────────────────────────────────
# ルールベース抽出（明示的状態機械）
# ────────────────────────────────────────────────────────────────

def _is_hard_stop(line: str) -> bool:
    """
    メール末尾の確実な終端行を判定する（署名・結語）。
    これらを検出したら STATE_DONE にしてループを終了する。
    セクション区切り（期間・備考等）はハード停止ではなく STATE_NONE に戻す。
    """
    s = _n(line.strip())
    return bool(_SIGNATURE_STOP_RE.match(s) or _STOP_FULL_LINE_RE.search(line))


def rule_extract_skills(body: str) -> Tuple[List[Dict], List[Dict]]:
    """
    状態遷移:
      NONE              + required_header → REQUIRED
      NONE              + optional_header → OPTIONAL
      REQUIRED          + optional_header → OPTIONAL
      OPTIONAL          + required_header → REQUIRED   ← Want → Must の逆順対応
      REQUIRED/OPTIONAL + ソフト停止      → NONE       ← 後続 Must/Want を処理可能にする
      REQUIRED/OPTIONAL + ハード停止      → DONE       ← 署名・結語でメール終端
      DONE                               → ループ終了

    ソフト停止: 期間・勤務地・備考など案件情報セクション見出し（後続に Must/Want が来る場合がある）
    ハード停止: 株式会社〜署名、「よろしくお願い」等の結語（以降にスキル情報はない）
    """
    # HTMLエンティティを解除してからNFKC正規化（&lt;必須&gt; → <必須> 等）
    text = _n(html.unescape(body))
    lines = text.splitlines()

    required: List[str] = []
    optional: List[str] = []
    state = STATE_NONE
    pseudo_parent_header = ''

    for raw_line in lines:
        if state == STATE_DONE:
            break

        line = raw_line.strip()
        if not line:
            continue

        kind, inline = _classify_line(line)

        # one-shot スキル抽出: (必須) skill / (尚可) skill 形式
        # ラベルで種別が確定しているため _is_skill_line の文脈チェックを省略し、
        # reject/stop/空文字チェックのみ実施。skill 確定後 STATE_NONE に戻る。
        if kind in ('required_skill', 'optional_skill'):
            pseudo_parent_header = ''
            if inline:
                skill_text = _strip_bullet(inline)
                if (skill_text and len(skill_text) > 1
                        and not _should_skip(skill_text)
                        and not _is_stop_section(skill_text)
                        and not _REJECT_CONTENT_RE.search(skill_text)
                        and not _is_non_skill_explanatory_line(skill_text)):
                    if kind == 'required_skill':
                        required.append(skill_text)
                    else:
                        optional.append(skill_text)
            state = STATE_NONE
            continue

        # Must/Want 見出し: いずれの状態からでも正しいセクションに遷移する
        if kind == 'required_header':
            state = STATE_REQUIRED
            if inline and _is_pseudo_list_header(inline):
                pseudo_parent_header = inline
                continue
            pseudo_parent_header = ''
            # 同一行のインラインコンテンツ（単一 or 「・A・B・C」形式の列挙）
            if inline:
                for item in _INLINE_BULLET_SPLIT_RE.split(inline):
                    item = item.strip()
                    if item and _is_skill_line(item, in_section=True):
                        required.append(_strip_bullet(item))
            continue

        if kind == 'optional_header':
            state = STATE_OPTIONAL
            if inline and _is_pseudo_list_header(inline):
                pseudo_parent_header = inline
                continue
            pseudo_parent_header = ''
            if inline:
                for item in _INLINE_BULLET_SPLIT_RE.split(inline):
                    item = item.strip()
                    if item and _is_skill_line(item, in_section=True):
                        optional.append(_strip_bullet(item))
            continue

        if kind == 'stop':
            if state in (STATE_REQUIRED, STATE_OPTIONAL):
                # ソフト停止 → STATE_NONE（後続の Must/Want 見出しを処理可能にする）
                # ハード停止 → STATE_DONE（メール末尾確定、ループ終了）
                state = STATE_DONE if _is_hard_stop(line) else STATE_NONE
                pseudo_parent_header = ''
            # STATE_NONE での stop は状態を変えない。
            # メール冒頭の会社名（送信元紹介）が署名判定されても問題ないよう、
            # STATE_NONE 時はハード停止でもループを終了させない。
            continue

        # content
        if state == STATE_NONE:
            # _REJECT_CONTENT_RE で年齢・単価等の非スキル行を除外してから判定する。
            # STATE_NONE はセクション前後どちらでも通過するため、拒否フィルタが必要。
            if _REJECT_CONTENT_RE.search(line):
                pass
            elif _DESIRABLE_RE.search(line):
                skill_text = _strip_bullet(line)
                if skill_text and len(skill_text) > 5:
                    optional.append(skill_text)
            elif _TECH_ELEMENT_RE.match(line):
                skill_text = _strip_bullet(line)
                if skill_text:
                    required.append(skill_text)

        elif state in (STATE_REQUIRED, STATE_OPTIONAL):
            if _should_skip(line):
                continue
            # ── セクション終端を skill 採用より前に明示的に判定 ──────────────
            # _classify_line が 'content' と判定した行にも停止条件を適用する。
            # 「期間 2026年4月〜」のようにコロンなしで _is_stop_section が
            # 取りこぼすパターンを _is_section_stop で追加カバーする。
            # ここに到達する行は署名ではないためソフト停止として STATE_NONE に戻す。
            if _is_section_stop(line):
                state = STATE_NONE
                pseudo_parent_header = ''
                continue
            # ──────────────────────────────────────────────────────────────
            indented = _has_leading_indent(raw_line)
            if not _is_skill_line(line, in_section=True, indented=indented):
                if not _is_pseudo_context_child_line(line, pseudo_parent_header):
                    pseudo_parent_header = ''
                    continue
            skill_text = _strip_bullet(line)
            if skill_text and len(skill_text) > 1:
                skill_text = _complete_skill_with_pseudo_context(
                    skill_text, pseudo_parent_header
                )
                if state == STATE_REQUIRED:
                    required.append(skill_text)
                else:
                    optional.append(skill_text)

    return [_make_skill(s) for s in required], [_make_skill(s) for s in optional]


# ────────────────────────────────────────────────────────────────
# LLMフォールバック
# ────────────────────────────────────────────────────────────────

_SKILL_INDICATOR_RE = re.compile(
    r'必須|尚可|スキル|Required|MUST|Preferred|WANT|経験者|開発経験'
)

SYSTEM_PROMPT = """\
あなたはSES案件メールから必須スキルと尚可スキルを抽出するアシスタントです。

【抽出ルール】
1. 必須スキルセクション（【必須スキル】等）の行を1行1スキルとして抽出する
2. 尚可スキルセクション（【尚可スキル】等）の行を1行1スキルとして抽出する
3. 箇条書き記号（・、※、●等）は除去してスキルテキストのみを格納する
4. skill フィールドには、メール本文の記載をそのまま格納する（要約禁止）
5. 空行・区切り線・見出し行はスキルに含めない
6. match と note は必ず null のまま

{"required_skills": [{"skill": "...", "match": null, "note": null}],
 "optional_skills": [{"skill": "...", "match": null, "note": null}]}

JSONのみ返すこと。マークダウン禁止。"""

FALLBACK_SCHEMA = {
    "required_skills": [{"skill": "", "match": None, "note": None}],
    "optional_skills": [{"skill": "", "match": None, "note": None}],
}


def _normalize_skill_list(raw_list: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_list, list):
        return []
    result = []
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        skill = item.get("skill")
        if not skill or not str(skill).strip():
            continue
        result.append({"skill": str(skill).strip(), "match": None, "note": None})
    return result


def llm_extract_skills(mid: str, body: str) -> Tuple[List[Dict], List[Dict]]:
    truncated = body[:MAX_BODY_LEN_LLM] if len(body) > MAX_BODY_LEN_LLM else body
    fallback = {"required_skills": [], "optional_skills": []}
    response = call_llm_with_fallback(
        system_prompt=SYSTEM_PROMPT,
        user_prompt=truncated,
        response_schema=FALLBACK_SCHEMA,
        fallback_value=fallback,
        step_name=STEP_NAME,
        max_tokens=2048,
        model="gpt-4o-mini",
    )
    return (
        _normalize_skill_list(response.get("required_skills")),
        _normalize_skill_list(response.get("optional_skills")),
    )


def extract_skills(mid: str, body: str) -> Tuple[List[Dict], List[Dict], str]:
    normalized = _n(body)

    if _is_resource_introduction(normalized):
        logger.info(f"要員紹介スキップ: {mid}", message_id=mid)
        return [], [], "skip"

    req, opt = rule_extract_skills(body)

    if not req and not opt:
        if _SKILL_INDICATOR_RE.search(normalized) and USE_LLM_FALLBACK:
            logger.info(f"LLMフォールバック: {mid}", message_id=mid)
            req, opt = llm_extract_skills(mid, normalized)
            return req, opt, "llm"
        logger.info(f"ルール抽出空: {mid}", message_id=mid)
        return [], [], "rule_empty"

    return req, opt, "rule"


def build_record(mid, required_skills, optional_skills, method: str = ""):
    # method はログ・内部カウント専用。JSONL 出力には含めない。
    return {
        "message_id": mid,
        "required_skills": required_skills,
        "optional_skills": optional_skills,
    }


def _all_skills_empty(rec: Dict[str, Any]) -> bool:
    req = rec.get("required_skills") or []
    opt = rec.get("optional_skills") or []
    if not req and not opt:
        return True
    return all(not item.get("skill") for item in req + opt)


# ────────────────────────────────────────────────────────────────
# メイン
# ────────────────────────────────────────────────────────────────

def main() -> None:
    start = time.time()
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = dirs["result"]

    for path in [INPUT_PROJECTS, INPUT_CLEANED]:
        if not Path(path).exists():
            logger.error(f"入力ファイルが存在しません: {path}")
            sys.exit(1)

    try:
        project_ids = [r["message_id"] for r in read_jsonl_as_list(INPUT_PROJECTS)]
        cleaned_map = read_jsonl_as_dict(INPUT_CLEANED)
    except Exception as e:
        write_error_log(str(result_dir), e, "入力ファイル読み込みエラー")
        logger.error(f"入力ファイル読み込みエラー: {e}")
        sys.exit(1)

    total = len(project_ids)
    logger.info(f"処理開始: {total}件")

    extracted: list = []
    null_records: list = []
    rule_empty_records: list = []
    counts = {"rule": 0, "llm": 0, "skip": 0, "rule_empty": 0, "llm_failed": 0}

    for mid in project_ids:
        body = (cleaned_map.get(mid) or {}).get("body_text", "")
        req, opt, method = extract_skills(mid, body)

        # llm_failed: LLM実行後もスキル空
        effective_method = method
        if method == "llm" and not req and not opt:
            effective_method = "llm_failed"

        counts[effective_method] = counts.get(effective_method, 0) + 1

        rec = build_record(mid, req, opt, method=effective_method)

        if _all_skills_empty(rec):
            null_records.append(rec)
            if effective_method == "rule_empty":
                rule_empty_records.append(rec)
        else:
            extracted.append(rec)

        logger.info(
            f"{mid} → req={len(req)} opt={len(opt)} [{effective_method}]",
            message_id=mid,
        )

    write_jsonl(str(result_dir / OUTPUT_EXTRACTED), extracted)
    write_jsonl(str(result_dir / OUTPUT_NULL), null_records)
    write_jsonl(str(result_dir / OUTPUT_RULE_EMPTY), rule_empty_records)

    elapsed = time.time() - start
    write_execution_time(str(dirs["execution_time"]), STEP_NAME, elapsed, total)

    logger.ok(
        f"Step完了: 入力={total} "
        f"/ 抽出成功(rule)={counts['rule']} "
        f"/ 抽出成功(llm)={counts['llm']} "
        f"/ 要員紹介skip={counts['skip']} "
        f"/ ルール抽出空(rule_empty)={counts['rule_empty']} "
        f"/ LLM失敗(llm_failed)={counts['llm_failed']} "
        f"/ USE_LLM_FALLBACK={USE_LLM_FALLBACK}"
    )


if __name__ == "__main__":
    main()

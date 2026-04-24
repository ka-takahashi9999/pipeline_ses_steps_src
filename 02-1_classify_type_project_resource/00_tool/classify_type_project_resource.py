#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 02-1: メール種別分類スクリプト（案件 / 要員 / あいまい / 不明）

分類ロジック:
  ① ルールベース（キーワード辞書 + 本文構造ラベル）で判定
     - 案件構造ラベル（【案件】/【概要】/【場所】等）が3種類以上 → project 確定
     - 要員構造ラベル（氏名：/年齢：/所属：等）が4種類以上 → resource 確定
     - 氏名：が3名分以上 → resource 確定（複数要員リスト）
     - 上記以外はキーワードスコアで判定
     - resource スコアが大きく上回る → resource
     - project スコアが大きく上回る  → project
     - スコア接戦                     → ambiguous
     - キーワード未ヒット             → unknown
     添付ファイルあり → resource 加点（+1.5）のみ（確定ではない）
  ② ルールで ambiguous/unknown の場合のみ LLM 補助
     （USE_LLM_CLASSIFY=True のときのみ有効）

入力①（本文・添付情報参照用）:
  01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl

入力②（クリーニング済み本文）:
  01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl

入力③（処理対象 message_id）:
  01-3_remove_individual_email/01_result/remove_individual_emails_raw.jsonl

出力①（project/resource/ambiguous）:
  01_result/classify_types_project_resource.jsonl
  {"message_id": "...", "mail_type": "project|resource|ambiguous"}

出力②（unknown のみ）:
  01_result/99_no_classify_types_project_resource.jsonl
  {"message_id": "...", "mail_type": "unknown"}
"""

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
from common.llm_client import call_llm_with_fallback
from common.logger import get_logger

# config は同ディレクトリ（00_tool）から import
_TOOL_DIR = Path(__file__).resolve().parent
if str(_TOOL_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOL_DIR))
from config import (
    USE_LLM_CLASSIFY,
    LLM_MODEL,
    LLM_MAX_TOKENS,
    RULE_MARGIN,
    RULE_MIN_CONFIDENCE,
)

STEP_NAME = "02-1_classify_type_project_resource"
logger = get_logger(STEP_NAME)

INPUT_MASTER = str(
    _PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)
INPUT_CLEANED = str(
    _PROJECT_ROOT / "01-4_cleanup_email_text" / "01_result" / "cleanup_email_text_emails_raw.jsonl"
)
INPUT_PREV = str(
    _PROJECT_ROOT / "01-3_remove_individual_email" / "01_result" / "remove_individual_emails_raw.jsonl"
)
KEYWORDS_PATH = str(_STEP_DIR / "10_assistance_tool" / "classify_keywords.txt")

OUTPUT_CLASSIFIED = "classify_types_project_resource.jsonl"
OUTPUT_UNKNOWN = "99_no_classify_types_project_resource.jsonl"

VALID_MAIL_TYPES = {"project", "resource", "ambiguous", "unknown"}


# ---------------------------------------------------------------------------
# キーワード辞書読み込み
# ---------------------------------------------------------------------------

class KeywordDict:
    """classify_keywords.txt から読み込んだキーワード辞書。"""

    def __init__(
        self,
        resource: Dict[str, float],
        project: Dict[str, float],
    ):
        self.resource = resource  # {normalized_keyword: weight}
        self.project = project


def _normalize(s: str) -> str:
    """Unicode NFKC 正規化 + 小文字化。"""
    return unicodedata.normalize("NFKC", s or "").lower()


def load_keywords(path: str) -> KeywordDict:
    """classify_keywords.txt を読み込んでキーワード辞書を返す。"""
    resource: Dict[str, float] = {}
    project: Dict[str, float] = {}

    kw_path = Path(path)
    if not kw_path.exists():
        logger.warn(f"キーワードファイルが存在しません: {path}")
        return KeywordDict(resource, project)

    current_section = None
    with open(kw_path, encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("[") and line.endswith("]"):
                current_section = line[1:-1].upper()
                continue
            if current_section not in ("RESOURCE", "PROJECT"):
                continue

            parts = line.split(",", 1)
            keyword = _normalize(parts[0].strip())
            if not keyword:
                continue
            try:
                weight = float(parts[1].strip()) if len(parts) > 1 else 1.0
            except ValueError:
                weight = 1.0

            if current_section == "RESOURCE":
                resource[keyword] = weight
            else:
                project[keyword] = weight

    logger.info(
        f"キーワード辞書読み込み完了: resource={len(resource)}語, project={len(project)}語"
    )
    return KeywordDict(resource, project)


# ---------------------------------------------------------------------------
# スコアリング
# ---------------------------------------------------------------------------

# 要員コンテキストパターン: 一致した場合、resource スコアに加点 + 「案件」重みを無効化
# インデックス 0-8: 強いパターン（スコア 5.0）/ 9以降: 中程度（スコア 3.0）
_RESOURCE_CONTEXT_PATTERNS = [
    # 強パターン (idx 0-8) → score 5.0
    # 「ご紹介いただ/ください/頂」は送り手の受動依頼なので除外（案件メールの誤検知防止）
    r"技術者をご紹介(?!いただ|ください|頂)",
    r"人材をご紹介(?!いただ|ください|頂)",
    r"要員をご紹介(?!いただ|ください|頂)",
    r"人材情報を送付",
    r"要員情報を送付",
    r"人材のご紹介",
    r"要員情報のご紹介",
    r"営業中の技術者(?!は[、,\s])",   # 「弊社の営業中の技術者は、下記シートから」フッターを除外
    r"営業中の要員",
    # --- 中程度パターン (idx 9-19) → score 3.5 ---
    r"エンジニアのご紹介",
    r"技術者のご紹介",
    r"インフラエンジニアのご紹介",
    r"フリーランスのご紹介",
    r"プロパーのご紹介",
    r"人材.*ご紹介(?!いただ|ください|頂)",
    r"要員.*ご紹介(?!いただ|ください|頂)",
    r"案件を探しております",
    r"案件を探しています",
    r"見合う案件",
    r"案件がございましたら",
    r"下記人材",
    r"(?<!案件・)人材情報",  # 「案件・人材情報」(CRE-CO等フッター) は除外
    r"要員情報",
    r"プロパー情報.*送付",   # 「弊社プロパー情報をご送付いたします」等
]

# 案件コンテキストパターン: 一致した場合、project スコアに加点
_PROJECT_CONTEXT_PATTERNS = [
    r"注力案件",                            # 「注力案件となります」等（単独言及も含む）
    r"案件名",
    r"案件の詳細",
    r"案件内容(?!に応じ|次第|によって)",   # 「案件内容に応じて」は要員メール文言なので除外
    r"案件概要",
    r"募集案件",
    r"常駐案件",
    r"常駐必須",
    # --- 追加: 案件メール定型文 ---
    r"見合う要員.*いらっしゃいましたら",
    r"見合う.*方がいらっしゃいましたら",
    r"ご対応可能な要員.*いらっしゃいましたら",
    r"技術者.*募集しております",
    r"エンジニア.*募集しております",
    r"案件情報.*お送り",
    r"注力案件のご紹介",
    r"注力案件.*ご紹介",
    r"下記.*案件.*技術者.*募集",
    r"下記案件",
    r"下記.*元請",
    r"元請.*直案件",
    # --- 追加: 候補者紹介依頼フレーズ (要員紹介お願いする案件メール定型文) ---
    r"見合う.*がおりましたら",        # 「見合う人材がおりましたら」(Y's等)
    r"見合う.*がいらっしゃれば",      # 「見合う方がいらっしゃれば」(sus4等)
    r"対応可能な.*ご紹介ください",    # 「対応可能なエンジニア様が…ご紹介ください」(CRE-CO等)
]

_RESOURCE_CONTEXT_RE = [re.compile(p) for p in _RESOURCE_CONTEXT_PATTERNS]
_PROJECT_CONTEXT_RE = [re.compile(p) for p in _PROJECT_CONTEXT_PATTERNS]

# subject の強力な判別パターン（本文が薄い場合も有効）
_SUBJ_RESOURCE_RE = [re.compile(p) for p in [
    r"直フリーランス",
    r"直個人",
    r"プロパー",
    r"弊社正社員",
    r"弊社要員",
    r"弊社人材",
    r"当社社員",
    r"当社プロパー",
    r"自社社員",
    r"人材情報",
    r"要員情報",
    r"人財情報",
    r"注力要員",     # 「注力要員リスト」等
    r"のご紹介です",
    r"をご紹介",
    r"人材.*ご紹介",
    r"人材.*ご提案",
    r"要員.*ご紹介",
    r"エンジニア.*ご紹介",
    r"経歴[\d.]+年",
]]
_SUBJ_PROJECT_RE = [re.compile(p) for p in [
    r"注力案件",
    r"急.{0,3}募",   # 「急募」「急　募」「急　　募」等に対応
    r"案件配信",
    r"案件情報",
    r"【案件",
    r"技術者.*募集",
    r"エンジニア.*募集",
    r"要員.*募集",
    r"長期案件",
    r"案件探してます",    # 「案件探してます」「AWS案件探してます」等
    r"案件探しております",
]]

# 「案件」キーワード（コンテキストに応じて重みを調整するため個別管理）
_KW_CASE = _normalize("案件")

# ---------------------------------------------------------------------------
# 本文構造ラベル（早期確定用）
# ---------------------------------------------------------------------------

# 案件構造ラベル: 3種類以上ヒット → project 確定
# 要員紹介でも使われやすい【スキル】【単金】【備考】は除外し、
# 案件専用度の高いラベルだけで早期確定する。
_PROJECT_STRUCT_LABEL_RES: List[re.Pattern] = [re.compile(p) for p in [
    r'【案件[】：]',
    r'【概要[】：]',
    r'【場所[】：]',
    r'【人数[】：]',
    r'【精算[】：]',
    r'【面談[】：]',
]]

# 要員単人構造ラベル: 4種類以上ヒット → resource 確定
# 個人プロフィール行（氏名:, 【氏名】, 年齢:, 【年齢】, 所属:等）が4つ以上
# = 要員紹介メール固有の構造
_RESOURCE_SINGLE_LABEL_RES: List[re.Pattern] = [re.compile(p) for p in [
    r'氏名[：:]',
    r'【氏名[】]',
    r'年齢[：:]',
    r'【年齢[】]',
    r'所属[：:]',
    r'【所属[】]',
    r'最寄.{0,1}駅[：:]',  # 「最寄駅：」「最寄り駅：」両方に対応
    r'【最寄.{0,2}[】]',
    r'入場日[：:]',
    r'【入場日[】]',
    r'単金[：:]',
    r'【単金[】]',
    r'単価[：:]',           # 「単価：80万」形式（括弧なし）
    r'【単価[】]',
    r'名前[：:]',           # 「名前：Y.T」形式（氏名の代替）
    r'【名前[】]',
    r'稼働[：:]',           # 「稼働：4月～」形式
    r'稼動[：:]',
    r'【稼働[】]',
    r'【稼動[】]',
]]


# CJK文字間に挟まれた半角スペース除去用パターン（全角スペースはNFKCで半角化済みのため）
# 例: 「案 件：」→「案件：」「単 価：」→「単価：」（ラベル表記の全角スペース整形アーティファクト）
_CJK_INNER_SPACE_RE = re.compile(
    r'(?<=[\u4e00-\u9fff\u3040-\u30ff\uff00-\uffef]) (?=[\u4e00-\u9fff\u3040-\u30ff\uff00-\uffef])'
)


def _remove_cjk_inner_spaces(s: str) -> str:
    """CJK文字間の半角スペースを除去する（NFKC正規化後に適用）。"""
    return _CJK_INNER_SPACE_RE.sub('', s)


def score_text(
    subject: str,
    body: str,
    kw_dict: KeywordDict,
    has_attachment: bool = False,
) -> Tuple[float, float, List[str], List[str], float, float]:
    """
    subject + body をスコアリングして
    (resource_score, project_score, res_hits, proj_hits, resource_context_score, project_context_score)
    を返す。
    """
    norm_subj = _normalize(subject or "")
    norm_body = _remove_cjk_inner_spaces(_normalize(body or ""))
    txt = norm_subj + "\n" + norm_body

    # ノイズ除去（URL・電話番号等）
    txt = re.sub(r"https?://\S+", " ", txt)
    txt = re.sub(r"[a-z0-9._-]+\.(?:co\.jp|jp|com|net)\S*", " ", txt)
    txt = re.sub(r"\d{3}-?\d{4}", " ", txt)   # 郵便番号
    txt = re.sub(r"0\d{1,3}-\d{1,4}-\d{3,4}", " ", txt)  # 電話番号

    # ---- コンテキスト判定（body 対象）----
    body_txt = norm_body  # CJK間スペース除去済みのものを使用
    resource_context_score = 0.0
    for i, pat in enumerate(_RESOURCE_CONTEXT_RE):
        if pat.search(body_txt):
            # インデックス 0-8: 強力パターン(5.0) / 9-19: 中パターン(3.5) / 20+: 弱パターン(3.0)
            # ※「案件を探しております/見合う案件/案件がございましたら」も中パターン扱い(idx 16-19)
            if i < 9:
                resource_context_score = 5.0
            elif i < 20:
                resource_context_score = 3.5
            else:
                resource_context_score = 3.0
            break

    project_context_score = 0.0
    for pat in _PROJECT_CONTEXT_RE:
        if pat.search(body_txt):
            project_context_score = 3.0
            break

    # ---- subject パターンボーナス ----
    for pat in _SUBJ_RESOURCE_RE:
        if pat.search(norm_subj):
            resource_context_score += 2.0
            break
    for pat in _SUBJ_PROJECT_RE:
        if pat.search(norm_subj):
            project_context_score += 2.0
            break

    # 「常駐可/検討可」 → 要員寄り
    if re.search(r"常駐検討可|常駐可", body_txt) and not re.search(r"常駐必須|常駐案件", body_txt):
        resource_context_score += 1.0
    elif re.search(r"常駐必須|常駐案件", body_txt):
        project_context_score += 1.0

    # 添付ファイルあり → 要員加点（確定ではなく加点のみ）
    if has_attachment:
        resource_context_score += 1.5

    res_score = 0.0
    proj_score = 0.0
    res_hits: List[str] = []
    proj_hits: List[str] = []

    for kw, weight in kw_dict.resource.items():
        cnt = txt.count(kw)
        if cnt > 0:
            res_score += cnt * weight
            res_hits.append(kw)

    for kw, weight in kw_dict.project.items():
        cnt = txt.count(kw)
        if cnt > 0:
            # 「案件」はコンテキストに応じて重みを大幅調整
            if kw == _KW_CASE:
                if resource_context_score > 0:
                    weight = weight * 0.01
                    cnt = min(cnt, 1)
                elif re.search(r"人材.*案件|要員.*案件|技術者.*案件|候補者.*案件", txt):
                    weight = weight * 0.1
                    cnt = min(cnt, 3)
                elif re.search(r"案件を探|見合う案件|案件がございましたら", txt):
                    weight = weight * 0.1
                    cnt = min(cnt, 3)
                elif project_context_score > 0:
                    weight = weight * 1.2
                else:
                    cnt = min(cnt, 5)
            # 要員コンテキストが強い場合、案件系キーワードの重みを下げる
            # （要員メールが「【商流】【面談回数】【精算幅】」を列挙するパターン対策）
            elif resource_context_score >= 3.0 and kw in (
                _normalize("商流"), _normalize("面談回数"), _normalize("精算"),
                _normalize("単価"), _normalize("契約形態"),
            ):
                weight = weight * 0.2
            # 要員コンテキストがある場合、工程系KW（スキル記述で多用）の重みを下げる
            # （PMO/アーキテクト系エンジニア紹介メールでの「要件定義/基本設計」過剰評価対策）
            elif resource_context_score >= 3.5 and kw in (
                _normalize("要件定義"), _normalize("基本設計"), _normalize("詳細設計"),
                _normalize("上流工程"), _normalize("下流工程"),
            ):
                weight = weight * 0.3
            elif kw == _normalize("単価") and resource_context_score > 0:
                weight = weight * 0.3
            proj_score += cnt * weight
            proj_hits.append(kw)

    # コンテキストスコアを加算
    res_score += resource_context_score
    proj_score += project_context_score

    return res_score, proj_score, res_hits, proj_hits, resource_context_score, project_context_score


# ---------------------------------------------------------------------------
# ルールベース分類
# ---------------------------------------------------------------------------

def rule_classify(
    subject: str,
    body: str,
    kw_dict: KeywordDict,
    has_attachment: bool = False,
) -> Tuple[str, float, List[str]]:
    """
    ルールベースで分類する。

    Returns:
        (mail_type, confidence, matched_keywords)
        mail_type: "project" | "resource" | "ambiguous" | "unknown"
    """
    # 本文構造ラベルによる早期確定（CJK間スペース除去後に判定）
    body_cjk = _remove_cjk_inner_spaces(_normalize(body or ""))

    # 複数要員リスト（氏名: / 【氏名】が3件以上 = resource確定）
    if len(re.findall(r'氏名[：:]|【氏名[】]', body_cjk)) >= 3:
        return "resource", 0.9, []

    # 要員単人構造ラベル（4種類以上 = resource確定）
    # 氏名：, 【氏名】, 年齢：, 【年齢】, 所属：, 【所属】 等が4つ以上 = 要員プロフィール
    res_struct = sum(1 for p in _RESOURCE_SINGLE_LABEL_RES if p.search(body_cjk))
    if res_struct >= 4:
        return "resource", 0.85, []

    res_score, proj_score, res_hits, proj_hits, res_ctx, proj_ctx = score_text(
        subject, body, kw_dict, has_attachment=has_attachment
    )
    total = res_score + proj_score

    # 案件構造ラベル（3種類以上 = project確定）
    # ただし要員紹介シグナルが強い場合は早期確定させず、通常スコア判定に流す。
    proj_struct = sum(1 for p in _PROJECT_STRUCT_LABEL_RES if p.search(body_cjk))
    if proj_struct >= 3 and res_ctx < 3.5:
        return "project", 0.9, []

    if total <= 0:
        # キーワードゼロ（HTML化失敗など）→ subject のみで再判定
        return _classify_by_subject_only(subject)

    # 動的マージン: 強いコンテキストパターンがある場合は閾値を下げる
    norm_subj = _normalize(subject or "")
    effective_margin = RULE_MARGIN
    margin_reduced = False

    # スコアベースの強度判定（インデックスではなくスコア値で判断）
    # res_ctx >= 3.5 → 強力/中程度パターン一致（インデックス 0-19）
    # proj_ctx >= 3.0 → 案件コンテキスト一致
    strong_res = res_ctx >= 3.5
    strong_proj = proj_ctx >= 3.0
    subj_res = any(pat.search(norm_subj) for pat in _SUBJ_RESOURCE_RE)
    subj_proj = any(pat.search(norm_subj) for pat in _SUBJ_PROJECT_RE)

    if (strong_res or subj_res) and res_score > proj_score:
        effective_margin = max(0.2, RULE_MARGIN * 0.25)
        margin_reduced = True
    elif (strong_res or subj_res) and res_score >= proj_score * 0.6:
        effective_margin = max(0.2, RULE_MARGIN * 0.35)
        margin_reduced = True
    elif (strong_proj or subj_proj) and proj_score > res_score:
        effective_margin = max(0.2, RULE_MARGIN * 0.25)
        margin_reduced = True
    elif (strong_proj or subj_proj) and proj_score >= res_score * 0.6:
        effective_margin = max(0.2, RULE_MARGIN * 0.35)
        margin_reduced = True

    conf = abs(res_score - proj_score) / max(1.0, total)
    # マージン縮小済みの場合は信頼度要件を大幅に下げる（コンテキストシグナルが既に判別済み）
    min_conf = 0.05 if margin_reduced else RULE_MIN_CONFIDENCE

    # 強力要員コンテキスト（body+subject両方で要員シグナル）で score が接近している場合のresource確定
    # 条件: rctx>=5.0 かつ subj_res=True かつ res >= proj * 0.8
    # 例: 「直個人」subject + 「エンジニアのご紹介」body + 技術スキル記述による案件系KW過剰評価への対処
    if res_ctx >= 5.0 and subj_res and res_score >= proj_score * 0.8:
        return "resource", min(1.0, conf), sorted(set(res_hits))

    # 中程度の要員コンテキストでも、件名が明確に要員紹介でスコア差が僅差なら resource を優先する。
    if res_ctx >= 3.5 and subj_res and res_score >= proj_score * 0.8:
        return "resource", min(1.0, conf), sorted(set(res_hits))

    # 信頼度が低くてもスコア差が effective_margin の 2 倍以上ある場合は確定
    # （高スコアメールでは total が大きいため conf が薄まる → conf 単独の閾値チェックを緩和）
    large_margin_res = res_score - proj_score >= effective_margin * 2
    large_margin_proj = proj_score - res_score >= effective_margin * 2

    if res_score - proj_score >= effective_margin and (conf >= min_conf or large_margin_res):
        return "resource", min(1.0, conf), sorted(set(res_hits))
    if proj_score - res_score >= effective_margin and (conf >= min_conf or large_margin_proj):
        return "project", min(1.0, conf), sorted(set(proj_hits))

    # スコア接戦 → ambiguous（キーワードヒットあり）
    return "ambiguous", conf, sorted(set(res_hits + proj_hits))


def _classify_by_subject_only(subject: str) -> Tuple[str, float, List[str]]:
    """
    本文キーワードがゼロの場合（HTMLメール等）に subject のみで分類するフォールバック。
    """
    norm_subj = _normalize(subject or "")
    for pat in _SUBJ_RESOURCE_RE:
        if pat.search(norm_subj):
            return "resource", 0.5, []
    for pat in _SUBJ_PROJECT_RE:
        if pat.search(norm_subj):
            return "project", 0.5, []
    return "unknown", 0.0, []


# ---------------------------------------------------------------------------
# LLM 補助分類
# ---------------------------------------------------------------------------

_LLM_SCHEMA = {
    "mail_type": "resource",
    "confidence": 0.0,
    "rationale": "",
}

_LLM_SYSTEM = (
    "あなたはSES営業メールを分類するアシスタントです。\n"
    "メールを以下の4種別のいずれかに分類してください。\n"
    "  resource  : 要員・人材紹介メール\n"
    "  project   : 案件・求人紹介メール\n"
    "  ambiguous : 要員と案件の両方が混在するメール\n"
    "  unknown   : 判断不能なメール\n"
)


def llm_classify(
    message_id: str,
    subject: str,
    body: str,
) -> Tuple[str, float]:
    """
    LLM で分類する。

    Returns:
        (mail_type, confidence)
    """
    user_prompt = (
        f"件名: {subject or ''}\n\n"
        f"本文（抜粋）:\n{(body or '')[:1500]}\n\n"
        "上記メールの mail_type を分類し、confidence（0.0〜1.0）と rationale（日本語）を返してください。"
    )

    fallback = {"mail_type": "unknown", "confidence": 0.0, "rationale": "llm-error"}
    result = call_llm_with_fallback(
        system_prompt=_LLM_SYSTEM,
        user_prompt=user_prompt,
        response_schema=_LLM_SCHEMA,
        fallback_value=fallback,
        step_name=STEP_NAME,
        message_id=message_id,
        model=LLM_MODEL,
        max_tokens=LLM_MAX_TOKENS,
    )

    mail_type = result.get("mail_type", "unknown")
    if mail_type not in VALID_MAIL_TYPES:
        mail_type = "unknown"
    confidence = float(result.get("confidence") or 0.0)
    confidence = max(0.0, min(1.0, confidence))
    return mail_type, confidence


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = str(dirs["result"])

    logger.info(f"LLM補助フラグ: USE_LLM_CLASSIFY={USE_LLM_CLASSIFY}")

    start_time = time.time()
    classified_records: List[Dict] = []
    unknown_records: List[Dict] = []
    input_count = 0

    try:
        # キーワード辞書読み込み
        kw_dict = load_keywords(KEYWORDS_PATH)

        # メールマスタ読み込み（subject / attachments 参照）
        logger.info(f"メールマスタ読み込み: {INPUT_MASTER}")
        master = read_jsonl_as_dict(INPUT_MASTER, key="message_id")
        logger.info(f"メールマスタ件数: {len(master)}件")

        # クリーニング済み本文読み込み
        logger.info(f"クリーニング済み本文読み込み: {INPUT_CLEANED}")
        cleaned_dict = read_jsonl_as_dict(INPUT_CLEANED, key="message_id")
        logger.info(f"クリーニング済み件数: {len(cleaned_dict)}件")

        # 処理対象 message_id 読み込み
        logger.info(f"処理対象読み込み: {INPUT_PREV}")
        prev_records = read_jsonl_as_list(INPUT_PREV)
        input_count = len(prev_records)
        logger.info(f"処理対象件数: {input_count}件")

        stats = {"attachment": 0, "rule": 0, "llm": 0, "ambiguous": 0, "unknown": 0}

        for rec in prev_records:
            mid = rec.get("message_id", "")
            master_rec = master.get(mid, {})
            cleaned_rec = cleaned_dict.get(mid, {})

            subject = master_rec.get("subject") or ""
            attachments = master_rec.get("attachments") or []
            body = cleaned_rec.get("body_text") or master_rec.get("body_text") or ""

            # ① ルールベース分類（添付あり場合も含む。添付はスコア加点のみ）
            if attachments:
                stats["attachment"] += 1
            mail_type, conf, hits = rule_classify(subject, body, kw_dict, has_attachment=bool(attachments))

            # ③ LLM補助（ambiguous/unknown かつ USE_LLM_CLASSIFY=True の場合のみ）
            if mail_type in ("ambiguous", "unknown") and USE_LLM_CLASSIFY:
                llm_type, llm_conf = llm_classify(mid, subject, body)
                logger.info(
                    f"[LLM] {mid}: {mail_type} → {llm_type} (conf={llm_conf:.2f})"
                )
                mail_type = llm_type
                stats["llm"] += 1
            else:
                stats["rule"] += 1

            if mail_type == "ambiguous":
                stats["ambiguous"] += 1
            if mail_type == "unknown":
                stats["unknown"] += 1

            if mail_type == "unknown":
                unknown_records.append({"message_id": mid, "mail_type": mail_type})
            else:
                classified_records.append({"message_id": mid, "mail_type": mail_type})

        total_classified = len(classified_records)
        total_unknown = len(unknown_records)
        logger.info(
            f"分類完了: 入力={input_count}件 / "
            f"classified={total_classified}件 (resource+project+ambiguous) / "
            f"unknown={total_unknown}件"
        )
        logger.info(
            f"内訳: 添付確定={stats['attachment']}件 / "
            f"ルール={stats['rule']}件 / LLM={stats['llm']}件 / "
            f"ambiguous={stats['ambiguous']}件 / unknown={stats['unknown']}件"
        )

        # 出力①: classified (project/resource/ambiguous)
        out_classified = str(dirs["result"] / OUTPUT_CLASSIFIED)
        write_jsonl(out_classified, classified_records)
        logger.ok(f"出力①書き込み完了: {out_classified} ({total_classified}件)")

        # 出力②: unknown
        out_unknown = str(dirs["result"] / OUTPUT_UNKNOWN)
        write_jsonl(out_unknown, unknown_records)
        logger.ok(f"出力②書き込み完了: {out_unknown} ({total_unknown}件)")

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
            record_count=input_count,
        )

    logger.ok(
        f"Step完了: 入力={input_count}件 / "
        f"classified={len(classified_records)}件 / unknown={len(unknown_records)}件"
    )


if __name__ == "__main__":
    main()

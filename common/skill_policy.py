"""
スキル判定の共通ポリシー。

- 07-1_requirement_skill_ai_matching と 08-5_high_score_required_skill_recheck で
  同じ「営業確認前提で○固定」ルールを適用するための共通関数を提供する。

判定方針:
  スキル文言に「コミュニケーション／協調／主体／積極／一人称／自立／報連相」等の
  人物像系キーワードが含まれ、かつ技術語（英数字・設計/開発/構築 等）を含まない場合、
  スキルシートからは判定不能とみなして confidence=confirmed / match=true を固定で付与する。
  技術語を含む場合は通常の判定（スキルシート根拠）に委ねる。
"""

import re
from typing import Iterable

# ○固定対象キーワード（substring 判定。人物像・コミュ系・一人称系）
AUTO_TRUE_KEYWORDS: tuple = (
    "コミュニケーション",
    "コミュ力",
    "協調",
    "柔軟",
    "主体",
    "積極",
    "報連相",
    "報告連絡相談",
    "報告・連絡・相談",
    "チームワーク",
    "責任感",
    "素直",
    "自立",
    "自走",
    "一人称",
)

# 技術語（英数字を含まないスキル文言でも、これらの語を含めば技術要件として扱う）
TECHNICAL_HINT_KEYWORDS: tuple = (
    "設計",
    "開発",
    "実装",
    "製造",
    "テスト",
    "保守",
    "運用",
    "構築",
    "導入",
    "移行",
    "推進",
    "環境",
    "経験",
    "知見",
    "基本設計",
    "詳細設計",
    "要件定義",
)

# ○固定時の note / reason 文言
AUTO_TRUE_NOTE: str = "営業確認前提で固定true"
AUTO_TRUE_RECHECK_REASON: str = "営業確認前提で固定"

_ALPHANUM_TECH_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9#+./_\-]*")


def has_technical_focus(skill: str) -> bool:
    """スキル文言が技術要件として扱うべき特徴を含むかを判定する。

    - 英数字（言語名・製品名等）を含む、または
    - 設計/開発/構築 等の工程語を含む
    """
    if not skill:
        return False
    if _ALPHANUM_TECH_RE.search(skill):
        return True
    return any(keyword in skill for keyword in TECHNICAL_HINT_KEYWORDS)


def is_auto_true_skill(skill: str) -> bool:
    """スキルシートで判定不能な人物像・コミュ系・一人称系スキルかを判定する。

    判定:
      1. 技術語（英数字・設計/開発/構築 等）を含まない
      2. かつ AUTO_TRUE_KEYWORDS のいずれかを substring として含む

    True を返したスキルは、営業確認前提で match=true / confidence=confirmed 固定とする。
    """
    if not skill:
        return False
    if has_technical_focus(skill):
        return False
    return any(keyword in skill for keyword in AUTO_TRUE_KEYWORDS)


def iter_auto_true_keywords() -> Iterable[str]:
    return iter(AUTO_TRUE_KEYWORDS)

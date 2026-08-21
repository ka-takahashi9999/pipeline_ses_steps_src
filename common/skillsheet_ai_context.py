"""04-2 normalized skillsheet から安全なAI入力用contextを作る。

canonical artifact は変更せず、LLMへ渡す直前の文字列だけを軽量化する。
曖昧なsheet判定、解析異常、cleanup後の空文字は入力全文へfallbackする。
"""

import re
from dataclasses import dataclass
from typing import List, Sequence, Tuple


SHEET_HEADING_RE = re.compile(r"^===\s*シート:\s*(.*?)\s*===\s*$", re.MULTILINE)
SAMPLE_SHEET_NAME_RE = re.compile(
    r"(?:サンプル|sample|見本|記入例|入力例|雛形|テンプレート|template)",
    re.IGNORECASE,
)

GENERIC_TITLE_LINES = {
    "スキルシート",
    "職務経歴書",
    "技術経歴書",
    "業務経歴書",
    "経歴書",
    "skill sheet",
    "skillsheet",
}
GENERIC_TITLE_KEYS = {re.sub(r"\s+", "", value).lower() for value in GENERIC_TITLE_LINES}

PROFILE_LABELS = {
    "氏名",
    "イニシャル",
    "年齢",
    "性別",
    "最寄駅",
    "住所",
    "電話番号",
    "電話",
    "tel",
    "email",
    "e-mail",
    "メールアドレス",
    "所属会社",
    "会社名",
    "営業担当",
    "担当営業",
}

BODY_STRUCTURE_KEYWORDS = ("期間", "業務内容", "案件", "プロジェクト", "職務経歴")
BODY_EVIDENCE_KEYWORDS = (
    "要件定義",
    "基本設計",
    "詳細設計",
    "設計",
    "開発",
    "実装",
    "製造",
    "構築",
    "運用",
    "保守",
    "テスト",
    "試験",
    "移行",
    "role",
    "役割",
    "工程",
    "資格",
)

EXPLICIT_AUXILIARY_SHEET_NAMES = {
    "書き方のポイント",
    "入力説明",
    "取扱説明",
    "スキルシート取説（営業部向け）",
}

SEPARATOR_ONLY_RE = re.compile(
    r"^[\s\-_=＝ー―─━│┃|｜+＋*＊/／\\＼.,，。:：;；()[\]（）【】<>＜＞]{8,}$"
)
FORMULA_ONLY_RE = re.compile(r"^\s*=\s*[A-Za-z_][A-Za-z0-9_.]*(?:\([^\n]*\))?\s*$")
PAGE_ONLY_RE = re.compile(
    r"^\s*(?:page|ページ)\s*\d+\s*(?:[/／]|of)\s*\d+\s*$",
    re.IGNORECASE,
)
DATE_ONLY_RE = re.compile(
    r"^\s*(?:作成日|更新日|最終更新日)\s*[:：|｜]?\s*"
    r"(?:\d{4}[年/.-]\d{1,2}(?:[月/.-]\d{1,2}日?)?)\s*$"
)
BARE_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
BARE_PHONE_RE = re.compile(r"^(?:\+?\d[\d()（）\-ー\s]{8,}\d)$")
PLACEHOLDER_RE = re.compile(
    r"(?:○○株式会社|〇〇株式会社|株式会社○○|株式会社〇〇|"
    r"\b(?:sample|example)@|\bX{2,}\b|記入例|入力例|例として|"
    r"テンプレートの説明|書き方)",
    re.IGNORECASE,
)
PLACEHOLDER_INITIAL_RE = re.compile(
    r"氏\s*名\s*[|｜:]\s*[A-ZＡ-Ｚ]\s*[・.．]\s*[A-ZＡ-Ｚ](?:\s*[|｜]|\s*$)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SkillsheetAIContextResult:
    text: str
    used_fallback: bool
    fallback_reason: str
    removed_line_count: int
    removed_sheet_names: Tuple[str, ...]


@dataclass(frozen=True)
class _SheetBlock:
    name: str
    body: str


def _compact_label(value: str) -> str:
    return re.sub(r"\s+", "", value).lower()


PROFILE_LABEL_KEYS = {_compact_label(label) for label in PROFILE_LABELS}


def _is_profile_only_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return True
    if BARE_EMAIL_RE.fullmatch(stripped) or BARE_PHONE_RE.fullmatch(stripped):
        return True

    cells = [cell.strip() for cell in re.split(r"[|｜]", stripped)]
    if len(cells) >= 2 and len(cells) % 2 == 0:
        labels = cells[0::2]
        if labels and all(_compact_label(label.rstrip(":：")) in PROFILE_LABEL_KEYS for label in labels):
            return True

    match = re.match(r"^(.+?)\s*[:：]\s*(.+)$", stripped)
    if match and _compact_label(match.group(1)) in PROFILE_LABEL_KEYS:
        return True

    if re.fullmatch(r"年\s*齢\s+\d{1,3}\s*歳", stripped):
        return True
    if re.fullmatch(r"性\s*別\s+(?:男|女|男性|女性)", stripped):
        return True
    return False


def _is_safe_noise_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return True
    if _compact_label(stripped) in GENERIC_TITLE_KEYS:
        return True
    if SHEET_HEADING_RE.fullmatch(stripped):
        return True
    if _is_profile_only_line(stripped):
        return True
    if SEPARATOR_ONLY_RE.fullmatch(stripped):
        return True
    if FORMULA_ONLY_RE.fullmatch(stripped):
        return True
    if PAGE_ONLY_RE.fullmatch(stripped):
        return True
    if DATE_ONLY_RE.fullmatch(stripped):
        return True
    return False


def _cleanup_safe_lines(text: str) -> Tuple[str, int]:
    kept: List[str] = []
    removed = 0
    for line in text.splitlines():
        if _is_safe_noise_line(line):
            removed += 1
        else:
            kept.append(line.rstrip())
    return "\n".join(kept).strip(), removed


def _parse_sheet_blocks(text: str) -> Tuple[str, List[_SheetBlock]]:
    matches = list(SHEET_HEADING_RE.finditer(text))
    if not matches:
        if "=== シート:" in text:
            raise ValueError("malformed sheet heading")
        return text, []

    prefix = text[: matches[0].start()]
    blocks: List[_SheetBlock] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        name = match.group(1).strip()
        if not name:
            raise ValueError("empty sheet name")
        body = text[match.end() : end]
        blocks.append(_SheetBlock(name=name, body=body))
    return prefix, blocks


def _is_sample_sheet_name(name: str) -> bool:
    return bool(SAMPLE_SHEET_NAME_RE.search(name))


def _is_explicit_auxiliary_name(name: str) -> bool:
    return name.strip() in EXPLICIT_AUXILIARY_SHEET_NAMES


def _meaningful_lines(body: str) -> Sequence[str]:
    return [line.strip() for line in body.splitlines() if line.strip() and not _is_safe_noise_line(line)]


def _has_clear_skillsheet_body(body: str) -> bool:
    lines = _meaningful_lines(body)
    if len(lines) < 5:
        return False
    joined = "\n".join(lines)
    has_structure = any(keyword in joined for keyword in BODY_STRUCTURE_KEYWORDS)
    evidence_hits = sum(1 for keyword in BODY_EVIDENCE_KEYWORDS if keyword.lower() in joined.lower())
    return has_structure and evidence_hits >= 2


def _has_high_confidence_sample_signal(body: str) -> bool:
    lines = _meaningful_lines(body)
    if len(lines) <= 2:
        return True
    # profile行は通常cleanup対象だが、例示イニシャル自体は補助sheet判定signalに使う。
    joined = body
    return bool(PLACEHOLDER_RE.search(joined) or PLACEHOLDER_INITIAL_RE.search(joined))


def _select_sheet_blocks(blocks: Sequence[_SheetBlock]) -> Tuple[List[_SheetBlock], Tuple[str, ...], str]:
    candidates = [
        block
        for block in blocks
        if _is_sample_sheet_name(block.name) or _is_explicit_auxiliary_name(block.name)
    ]
    if not candidates:
        return list(blocks), (), ""

    non_candidates = [block for block in blocks if block not in candidates]
    clear_real_blocks = [block for block in non_candidates if _has_clear_skillsheet_body(block.body)]
    if not clear_real_blocks:
        return list(blocks), (), "ambiguous_or_sample_only_sheets"

    removable: List[_SheetBlock] = []
    for block in candidates:
        if _is_explicit_auxiliary_name(block.name):
            removable.append(block)
        elif _has_high_confidence_sample_signal(block.body):
            removable.append(block)
        else:
            return list(blocks), (), "ambiguous_sample_sheet"

    kept = [block for block in blocks if block not in removable]
    if not kept or not any(_has_clear_skillsheet_body(block.body) for block in kept):
        return list(blocks), (), "substantive_sheet_unknown"
    return kept, tuple(block.name for block in removable), ""


def build_skillsheet_ai_context_result(normalized_text: str) -> SkillsheetAIContextResult:
    """SAFE cleanup結果とfallback情報を返す。入力は04-2 normalized本文。"""
    original = str(normalized_text or "")
    if not original.strip():
        return SkillsheetAIContextResult(original, True, "empty_input", 0, ())

    try:
        prefix, blocks = _parse_sheet_blocks(original)
        selected, removed_names, fallback_reason = _select_sheet_blocks(blocks)
        if fallback_reason:
            return SkillsheetAIContextResult(original, True, fallback_reason, 0, ())

        if blocks:
            parts = [prefix]
            parts.extend(block.body for block in selected)
            selected_text = "".join(parts)
        else:
            selected_text = original

        cleaned, removed_line_count = _cleanup_safe_lines(selected_text)
        if not cleaned:
            return SkillsheetAIContextResult(original, True, "cleanup_empty", 0, ())
        return SkillsheetAIContextResult(
            cleaned,
            False,
            "",
            removed_line_count,
            removed_names,
        )
    except Exception:
        return SkillsheetAIContextResult(original, True, "parse_or_cleanup_error", 0, ())


def build_skillsheet_ai_context(normalized_text: str) -> str:
    """04-2 normalized本文からAI入力用contextを返す。"""
    return build_skillsheet_ai_context_result(normalized_text).text

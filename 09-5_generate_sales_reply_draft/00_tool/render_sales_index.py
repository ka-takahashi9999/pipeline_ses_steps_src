"""09-5の既存queueとpreviewを営業確認用の静的index.htmlへまとめる。"""

import argparse
import html
import os
import re
import sys
import tempfile
from collections import Counter
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote, urlparse

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from common.json_utils import read_jsonl_as_list
from common.logger import get_logger

STEP_NAME = "09-5_render_sales_index"
STEP_DIR = Path(__file__).resolve().parents[1]
OUTPUT_BASE_DIR = STEP_DIR / "01_result"
INPUT_09_4_DIR = project_root / "09-4_remove_category_mismatch_sales_candidates/01_result"
INPUT_08_5_PATH = (
    project_root
    / "08-5_high_score_required_skill_recheck/01_result/high_score_required_skill_recheck_all.jsonl"
)

REVIEW_ITEM_LABELS = {
    "phase": "工程",
    "work_terms": "契約・精算条件",
    "technology_semantic": "技術内容",
    "years": "経験年数",
    "start_timing": "開始時期",
    "role": "役割・PL・顧客調整",
    "location": "勤務地",
    "price": "単価・価格",
    "sales_recipient": "返信先・宛先",
    "sales_field": "営業項目",
    "category": "技術カテゴリ",
    "error": "処理エラー",
    "skillsheet": "スキルシート",
}
LINK_LABELS = {
    ("reply_to_project", "preview_file_path"): "案件Draft・原文",
    ("reply_to_project", "note_file_path"): "案件確認メモ",
    ("reply_to_resource", "preview_file_path"): "要員Draft・原文",
    ("reply_to_resource", "note_file_path"): "要員確認メモ",
}

PairKey = Tuple[str, str]


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def pair_key(record: Dict[str, Any]) -> PairKey:
    key = (
        normalize_text(record.get("project_message_id")),
        normalize_text(record.get("resource_message_id")),
    )
    if not all(key):
        raise ValueError("index入力にpair ID欠落があります")
    return key


def recheck_pair_key(record: Dict[str, Any]) -> PairKey:
    return pair_key(
        {
            "project_message_id": (record.get("project_info") or {}).get("message_id"),
            "resource_message_id": (record.get("resource_info") or {}).get("message_id"),
        }
    )


def _index_unique(
    records: Iterable[Dict[str, Any]], key_function, label: str
) -> Dict[PairKey, Dict[str, Any]]:
    result: Dict[PairKey, Dict[str, Any]] = {}
    for record in records:
        key = key_function(record)
        if key in result:
            raise ValueError(f"{label}にpair重複があります: {key[0]} / {key[1]}")
        result[key] = record
    return result


def _format_percent(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return "記載なし"
    percent = float(value) * 100
    return f"{percent:.0f}%" if percent.is_integer() else f"{percent:.1f}%"


def _sender_display(record: Dict[str, Any], prefix: str) -> str:
    values: List[str] = []
    for key in (
        f"{prefix}_sender_company",
        f"{prefix}_sender_name",
        f"{prefix}_sender_email",
    ):
        value = normalize_text(record.get(key))
        if value and value not in values:
            values.append(value)
    return " / ".join(values) if values else "記載なし"


def _review_labels(record: Dict[str, Any]) -> List[str]:
    items = record.get("normalized_review_items")
    if not isinstance(items, list):
        return []
    return [REVIEW_ITEM_LABELS.get(normalize_text(item), "その他確認項目") for item in items]


def _validated_artifact_href(
    value: Any, preview_dir: Path, output_base_dir: Path
) -> str:
    path_text = normalize_text(value)
    if not path_text or "\\" in path_text:
        raise ValueError(f"draft_refs pathが不正です: {path_text or '(empty)'}")
    relative_path = PurePosixPath(path_text)
    if relative_path.is_absolute() or any(part in ("", ".", "..") for part in relative_path.parts):
        raise ValueError(f"draft_refs pathがrelative artifactではありません: {path_text}")
    if not relative_path.parts or relative_path.parts[0] != preview_dir.name:
        raise ValueError(f"draft_refs pathが対象preview directory外です: {path_text}")

    artifact_path = output_base_dir.joinpath(*relative_path.parts)
    resolved_preview = preview_dir.resolve()
    resolved_artifact = artifact_path.resolve()
    try:
        relative_to_preview = resolved_artifact.relative_to(resolved_preview)
    except ValueError as error:
        raise ValueError(f"draft_refs pathが対象preview directory外です: {path_text}") from error
    if artifact_path.is_symlink() or not artifact_path.is_file():
        raise FileNotFoundError(f"draft_refs artifactが存在しないregular fileです: {path_text}")
    return quote(relative_to_preview.as_posix(), safe="/-_.~")


def _render_links(record: Dict[str, Any], preview_dir: Path, output_base_dir: Path) -> Tuple[str, int]:
    refs = record.get("draft_refs")
    if not isinstance(refs, list) or not refs:
        raise ValueError(f"draft_refsがありません: {pair_key(record)}")
    links: List[str] = []
    seen: set = set()
    for ref in refs:
        if not isinstance(ref, dict):
            raise ValueError(f"draft_refs要素がobjectではありません: {pair_key(record)}")
        direction = normalize_text(ref.get("draft_direction"))
        for path_key, artifact_label in (
            ("preview_file_path", "preview"),
            ("note_file_path", "note"),
        ):
            href = _validated_artifact_href(ref.get(path_key), preview_dir, output_base_dir)
            if href in seen:
                raise ValueError(f"draft_refs linkが重複しています: {pair_key(record)} / {href}")
            seen.add(href)
            link_label = LINK_LABELS.get((direction, path_key), f"draft {artifact_label}")
            links.append(
                f'<a href="{html.escape(href, quote=True)}">'
                f"{html.escape(link_label)}</a>"
            )
    return " ".join(links), len(links)


def _required_check_kind(check: Dict[str, Any]) -> str:
    confidence = normalize_text(check.get("confidence"))
    evidence = normalize_text(check.get("evidence"))
    reason = normalize_text(check.get("reason"))
    if confidence == "confirmed" and evidence:
        return "grounded"
    if confidence == "confirmed" and "営業確認前提" in reason:
        return "sales_confirmation"
    if confidence == "confirmed":
        return "review"
    if confidence == "human_review":
        return "review"
    return "missing"


def _required_stats(record: Dict[str, Any], recheck: Dict[str, Any]) -> Dict[str, Any]:
    checks = record.get("required_skill_checks")
    if not isinstance(checks, list):
        checks = recheck.get("required_skill_checks")
    if not isinstance(checks, list):
        checks = []
    kinds = Counter(_required_check_kind(check) for check in checks if isinstance(check, dict))
    total = len(checks)
    matched = kinds["grounded"] + kinds["sales_confirmation"]
    rate = matched / total if total else 0.0
    return {
        "checks": checks,
        "total": total,
        "matched": matched,
        "rate": rate,
        "grounded": kinds["grounded"],
        "sales_confirmation": kinds["sales_confirmation"],
        "review": kinds["review"],
        "missing": kinds["missing"],
    }


def _optional_rate(recheck: Dict[str, Any]) -> float:
    value = (recheck.get("match_info") or {}).get("optional_skills_match_rate")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def _category_rank(candidate: Dict[str, Any]) -> int:
    return {"match": 0, "unclear": 1, "mismatch": 2}.get(
        normalize_text(candidate.get("category_match")), 1
    )


def _sales_sort_key(
    record: Dict[str, Any], candidate: Dict[str, Any], recheck: Dict[str, Any]
) -> Tuple[Any, ...]:
    stats = _required_stats(record, recheck)
    return (
        -stats["rate"],
        stats["review"],
        _category_rank(candidate),
        -stats["grounded"],
        stats["sales_confirmation"],
        -_optional_rate(recheck),
        normalize_text(record.get("project_message_id")),
        normalize_text(record.get("resource_message_id")),
    )


def _render_required_details(stats: Dict[str, Any]) -> str:
    rows: List[str] = []
    labels = {
        "grounded": "○ 根拠あり",
        "sales_confirmation": "○ 営業確認前提",
        "review": "△ 要確認",
        "missing": "× 不足",
    }
    for check in stats["checks"]:
        if not isinstance(check, dict):
            continue
        kind = _required_check_kind(check)
        skill = normalize_text(check.get("skill")) or "必須条件（名称なし）"
        evidence = normalize_text(check.get("evidence"))
        reason = normalize_text(check.get("reason"))
        detail = ""
        if evidence:
            detail += f'<div class="evidence">根拠: 「{html.escape(evidence)}」</div>'
        if reason and kind != "sales_confirmation":
            detail += f'<div class="reason">判定理由: {html.escape(reason)}</div>'
        rows.append(
            f'<li class="required-{kind}"><strong>{labels[kind]}</strong> '
            f'{html.escape(skill)}{detail}</li>'
        )
    return "<ul class=\"required-list\">" + "".join(rows) + "</ul>"


def _render_skillsheet_reference(candidate: Dict[str, Any]) -> str:
    url = normalize_text(candidate.get("resource_skillsheet_url"))
    parsed = urlparse(url)
    if (
        candidate.get("resource_skillsheet_available") is True
        and parsed.scheme in {"http", "https"}
        and parsed.netloc
        and not parsed.username
    ):
        return (
            f'<a href="{html.escape(url, quote=True)}" target="_blank" '
            'rel="noopener noreferrer">Skillsheet</a>'
        )
    if candidate.get("resource_skillsheet_available") is True:
        source = normalize_text(candidate.get("resource_skillsheet_source")) or "取得済み"
        return f'<span class="muted">Skillsheet: {html.escape(source)}から取得済み（リンクなし）</span>'
    return '<span class="muted">Skillsheet参照なし</span>'


def _render_candidate_row(
    record: Dict[str, Any],
    candidate: Dict[str, Any],
    recheck: Dict[str, Any],
    preview_dir: Path,
    output_base_dir: Path,
) -> Tuple[str, int]:
    project_id, resource_id = pair_key(record)
    project_subject = normalize_text(candidate.get("project_subject")) or "件名記載なし"
    resource_subject = normalize_text(candidate.get("resource_subject")) or "件名記載なし"
    priority = "proposal_ready" if record.get("queue") == "proposal_ready" else normalize_text(record.get("review_priority"))
    stats = _required_stats(record, recheck)
    required_score = _format_percent(stats["rate"])
    optional_score = _format_percent(_optional_rate(recheck))
    labels = _review_labels(record)
    reasons = "、".join(labels) if labels else "—"
    links_html, link_count = _render_links(record, preview_dir, output_base_dir)
    previous_html = ""
    if record.get("previous_candidate") is True:
        previous_date = normalize_text(record.get("previous_candidate_date"))
        badge_text = f"[前回も候補: {previous_date}]" if previous_date else "[前回も候補]"
        previous_html = f' <span class="badge previous">{html.escape(badge_text)}</span>'
    duplicate_html = ""
    if recheck.get("duplicate_proposal_check") is True:
        duplicate_html = ' <span class="badge cache">過去AI判定を再利用</span>'
    category = normalize_text(candidate.get("category_match")) or "unclear"
    category_label = {"match": "適合", "unclear": "要確認", "mismatch": "不適合"}.get(category, "要確認")
    required_details = _render_required_details(stats)
    skillsheet_html = _render_skillsheet_reference(candidate)

    return (
        f'<tr data-project-id="{html.escape(project_id, quote=True)}" '
        f'data-resource-id="{html.escape(resource_id, quote=True)}" '
        f'data-priority="{html.escape(priority, quote=True)}" '
        f'data-required-rate="{stats["rate"]:.6f}" '
        f'data-previous-candidate="{str(record.get("previous_candidate") is True).lower()}">'
        f'<td><div class="subject">{html.escape(project_subject)}</div>'
        f'<div class="muted">{html.escape(_sender_display(candidate, "project"))}</div></td>'
        f'<td><div class="subject">{html.escape(resource_subject)}</div>'
        f'<div class="muted">{html.escape(_sender_display(candidate, "resource"))}</div></td>'
        f'<td><span class="badge priority">{html.escape(priority)}</span>{previous_html}{duplicate_html}'
        f'<div class="muted">Category: {html.escape(category_label)} ／ Optional: {html.escape(optional_score)}</div>'
        f'<div class="required-summary">Required {html.escape(required_score)} '
        f'○{stats["matched"]} / △{stats["review"]} / ×{stats["missing"]}</div>'
        f'<details class="required-evidence"><summary>必須条件と根拠</summary>{required_details}</details>'
        f'<div class="muted">確認理由: {html.escape(reasons)}</div></td>'
        f'<td class="links">{skillsheet_html}<br>{links_html}</td>'
        "</tr>",
        link_count,
    )


def _render_candidate_table(
    records: List[Dict[str, Any]],
    candidate_index: Dict[PairKey, Dict[str, Any]],
    recheck_index: Dict[PairKey, Dict[str, Any]],
    preview_dir: Path,
    output_base_dir: Path,
) -> Tuple[str, int]:
    ordered = sorted(
        records,
        key=lambda record: _sales_sort_key(
            record, candidate_index[pair_key(record)], recheck_index[pair_key(record)]
        ),
    )
    parts = [
        '<table><thead><tr><th>案件</th><th>要員</th><th>Required・根拠</th>'
        '<th>原文・Draft</th></tr></thead><tbody>'
    ]
    total_links = 0
    for record in ordered:
        key = pair_key(record)
        row_html, link_count = _render_candidate_row(
            record, candidate_index[key], recheck_index[key], preview_dir, output_base_dir
        )
        parts.append(row_html)
        total_links += link_count
    parts.append("</tbody></table>")
    return "\n".join(parts), total_links


def render_sales_index(
    proposal_ready: List[Dict[str, Any]],
    human_review: List[Dict[str, Any]],
    candidates: List[Dict[str, Any]],
    rechecks: List[Dict[str, Any]],
    preview_dir: Path,
    output_base_dir: Path,
    date_part: str,
) -> Tuple[str, Dict[str, Any]]:
    """静的HTMLを構築する。入力JSONLやpreview artifactは変更しない。"""
    if not re.fullmatch(r"\d{8}", date_part):
        raise ValueError(f"対象日がYYYYMMDDではありません: {date_part}")
    if preview_dir.name != f"reply_preview_{date_part}":
        raise ValueError(f"preview directory名が対象日と一致しません: {preview_dir}")

    proposal_index = _index_unique(proposal_ready, pair_key, "proposal_ready")
    human_index = _index_unique(human_review, pair_key, "human_review")
    if set(proposal_index) & set(human_index):
        raise ValueError("proposal_ready / human_reviewにpair重複があります")
    queue_index = {**proposal_index, **human_index}
    candidate_index_all = _index_unique(candidates, pair_key, "09-4 candidate")
    if set(queue_index) != set(candidate_index_all):
        missing = len(set(candidate_index_all) - set(queue_index))
        extra = len(set(queue_index) - set(candidate_index_all))
        raise ValueError(f"index candidate union不一致です: loss={missing} extra={extra}")
    recheck_index_all = _index_unique(rechecks, recheck_pair_key, "08-5 recheck")
    missing_rechecks = set(queue_index) - set(recheck_index_all)
    if missing_rechecks:
        raise ValueError(f"required score参照元が不足しています: {len(missing_rechecks)}")

    high_initial = [
        record
        for record in human_review
        if record.get("review_priority") == "HIGH" and record.get("initial_review") is True
    ]
    high_additional = [
        record
        for record in human_review
        if record.get("review_priority") == "HIGH" and record.get("initial_review") is False
    ]
    other = [record for record in human_review if record.get("review_priority") == "OTHER"]
    if len(high_initial) + len(high_additional) + len(other) != len(human_review):
        raise ValueError("human_reviewのHIGH initial / 追加HIGH / OTHER partitionが不正です")

    all_records = proposal_ready + human_review
    all_keys = [pair_key(record) for record in all_records]
    if len(all_keys) != len(set(all_keys)):
        raise ValueError("index掲載pairに重複があります")

    new_ready = [record for record in proposal_ready if record.get("previous_candidate") is not True]
    new_sales_review = []
    for record in human_review:
        if record.get("previous_candidate") is True:
            continue
        stats = _required_stats(record, recheck_index_all[pair_key(record)])
        if (
            stats["total"] > 0
            and stats["matched"] == stats["total"]
            and stats["sales_confirmation"] > 0
        ):
            new_sales_review.append(record)
    new_sales_review_keys = {pair_key(record) for record in new_sales_review}
    new_additional = [
        record
        for record in human_review
        if record.get("previous_candidate") is not True and pair_key(record) not in new_sales_review_keys
    ]
    previous = [record for record in all_records if record.get("previous_candidate") is True]
    new_keys = [pair_key(record) for record in new_ready + new_sales_review + new_additional]
    if len(new_keys) + len(previous) != len(all_records):
        raise ValueError("NEW / PREVIOUS partitionが不正です")

    section_specs = (
        ("new_ready", "① NEW・すぐ提案", new_ready, False),
        ("new_sales_review", "② NEW・営業確認あり", new_sales_review, False),
        ("new_additional", "③ NEW・追加確認", new_additional, True),
        ("previous", "④ PREVIOUS / 過去候補", previous, True),
    )
    section_html: List[str] = []
    draft_link_count = 0
    for section_key, title, records, collapsed in section_specs:
        groups_html, section_links = _render_candidate_table(
            records,
            candidate_index_all,
            recheck_index_all,
            preview_dir,
            output_base_dir,
        )
        draft_link_count += section_links
        if collapsed:
            section_html.append(
                f'<details class="sales-section {html.escape(section_key, quote=True)}" '
                f'data-section="{html.escape(section_key, quote=True)}" data-count="{len(records)}">'
                f"<summary>{html.escape(title)}（{len(records)}件）</summary>{groups_html}</details>"
            )
        else:
            section_html.append(
                f'<section class="sales-section {html.escape(section_key, quote=True)}" '
                f'data-section="{html.escape(section_key, quote=True)}" data-count="{len(records)}">'
                f"<h2>{html.escape(title)}（{len(records)}件）</h2>{groups_html}</section>"
            )

    previous_proposal = sum(record.get("previous_candidate") is True for record in proposal_ready)
    previous_high_initial = sum(record.get("previous_candidate") is True for record in high_initial)
    previous_initial = previous_proposal + previous_high_initial
    previous_all = sum(record.get("previous_candidate") is True for record in queue_index.values())
    reason_counts = Counter(
        label
        for record in high_initial
        for label in _review_labels(record)
    )
    summary: Dict[str, Any] = {
        "proposal_ready": len(proposal_ready),
        "high_initial": len(high_initial),
        "high_additional": len(high_additional),
        "other": len(other),
        "new_ready": len(new_ready),
        "new_sales_review": len(new_sales_review),
        "new_additional": len(new_additional),
        "new_total": len(new_keys),
        "previous": len(previous),
        "initial": len(new_ready) + len(new_sales_review),
        "total": len(all_keys),
        "duplicate": len(all_keys) - len(set(all_keys)),
        "candidate_loss": len(set(candidate_index_all) - set(all_keys)),
        "draft_link_missing": 0,
        "draft_link_count": draft_link_count,
        "previous_proposal": previous_proposal,
        "previous_high_initial": previous_high_initial,
        "previous_initial": previous_initial,
        "previous_all": previous_all,
        "initial_reason_counts": dict(sorted(reason_counts.items())),
    }

    document = f"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>営業候補 {html.escape(date_part)}</title>
<style>
body {{ color:#222; background:#f6f6f6; font-family:Arial,"Noto Sans JP",sans-serif; margin:0; }}
main {{ max-width:1500px; margin:0 auto; padding:20px; }}
h1 {{ margin:0 0 8px; }}
.lead {{ background:#fff; border-left:6px solid #1769aa; padding:12px; margin:0 0 20px; }}
.sales-section {{ display:block; background:#fff; border:1px solid #ccc; margin:0 0 18px; padding:14px; }}
.new_ready {{ border-left:7px solid #157347; }}
.new_sales_review {{ border-left:7px solid #b26a00; }}
.sales-section.previous {{ border-left:7px solid #6c757d; }}
summary {{ cursor:pointer; font-size:1.25rem; font-weight:bold; }}
.muted {{ color:#555; font-size:.86rem; }}
table {{ border-collapse:collapse; width:100%; table-layout:fixed; }}
th,td {{ border:1px solid #ddd; padding:7px; text-align:left; vertical-align:top; overflow-wrap:anywhere; }}
th {{ background:#eee; }}
th:first-child,th:nth-child(2) {{ width:20%; }}
th:last-child {{ width:14%; }}
.subject {{ font-weight:bold; }}
.badge {{ border:1px solid #777; border-radius:3px; display:inline-block; margin:1px; padding:2px 5px; font-size:.78rem; }}
.badge.previous {{ background:#fff3cd; }}
.cache {{ background:#eef2f7; color:#555; }}
.required-summary {{ font-weight:bold; margin:6px 0; }}
.required-list {{ margin:8px 0; padding-left:20px; }}
.required-list li {{ margin-bottom:8px; }}
.evidence,.reason {{ color:#444; font-size:.86rem; margin-top:2px; }}
.links a {{ display:inline-block; margin:0 6px 4px 0; }}
</style>
</head>
<body><main>
<h1>営業候補 {html.escape(date_part)}</h1>
<p class="lead">通常営業対象はNEW <strong>{summary['new_total']}件</strong>です。各section内は08-5再確認後のRequired一致率が高い順です。PREVIOUS {summary['previous']}件は「以前にも候補として登場したpair」であり、実際の連絡状況とは別です。</p>
{os.linesep.join(section_html)}
</main></body>
</html>
"""
    return document, summary


def load_sales_index_inputs(date_part: str) -> Tuple[List[Dict[str, Any]], ...]:
    paths = (
        OUTPUT_BASE_DIR / f"proposal_ready_{date_part}.jsonl",
        OUTPUT_BASE_DIR / f"human_review_{date_part}.jsonl",
        INPUT_09_4_DIR / f"sales_proposal_candidates_{date_part}.jsonl",
        INPUT_08_5_PATH,
    )
    for path in paths:
        if not path.is_file():
            raise FileNotFoundError(f"sales index入力が存在しません: {path}")
    return tuple(read_jsonl_as_list(str(path)) for path in paths)  # type: ignore[return-value]


def generate_sales_index(date_part: str, output_path: Optional[Path] = None) -> Dict[str, Any]:
    proposal_ready, human_review, candidates, rechecks = load_sales_index_inputs(date_part)
    preview_dir = OUTPUT_BASE_DIR / f"reply_preview_{date_part}"
    if not preview_dir.is_dir() or preview_dir.is_symlink():
        raise FileNotFoundError(f"sales index出力先preview directoryが不正です: {preview_dir}")
    destination = Path(output_path) if output_path else preview_dir / "index.html"
    destination.parent.mkdir(parents=True, exist_ok=True)
    document, summary = render_sales_index(
        proposal_ready,
        human_review,
        candidates,
        rechecks,
        preview_dir,
        OUTPUT_BASE_DIR,
        date_part,
    )
    temporary_name = ""
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=str(destination.parent),
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary.write(document)
            temporary_name = temporary.name
        os.replace(temporary_name, str(destination))
    finally:
        if temporary_name and Path(temporary_name).exists():
            Path(temporary_name).unlink()
    return {**summary, "index_path": str(destination)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="09-5 static sales index renderer")
    parser.add_argument("--target-date", required=True, help="対象日 YYYYMMDD")
    parser.add_argument("--output-path", type=Path, help="診断用の出力先。省略時は既存preview directory内")
    return parser.parse_args()


def main() -> None:
    logger = get_logger(STEP_NAME)
    args = parse_args()
    try:
        summary = generate_sales_index(args.target_date, args.output_path)
        logger.ok(
            "sales index生成完了: "
            f"path={summary['index_path']} proposal={summary['proposal_ready']} "
            f"HIGH_initial={summary['high_initial']} additional_HIGH={summary['high_additional']} "
            f"OTHER={summary['other']} total={summary['total']}"
        )
    except Exception as error:
        logger.error(f"sales index生成失敗: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()

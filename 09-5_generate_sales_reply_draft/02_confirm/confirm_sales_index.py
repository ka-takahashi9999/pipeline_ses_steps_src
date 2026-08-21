"""09-5の静的営業index.htmlと既存queue / artifactの整合を確認する。"""

import argparse
import copy
import hashlib
import sys
from collections import Counter, defaultdict
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import unquote

project_root = Path(__file__).resolve().parents[2]
tool_dir = Path(__file__).resolve().parents[1] / "00_tool"
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(tool_dir))

from common.logger import get_logger
from render_sales_index import (
    OUTPUT_BASE_DIR,
    load_sales_index_inputs,
    render_sales_index,
)

STEP_NAME = "09-5_sales_index_confirm"
STEP_DIR = Path(__file__).resolve().parents[1]
CONFIRM_RESULT = Path(__file__).resolve().parent / "confirm_result_sales_index.txt"
KNOWN_20260820 = {
    "proposal_ready": 36,
    "high_initial": 55,
    "high_additional": 59,
    "other": 468,
    "initial": 91,
    "total": 618,
    "previous_proposal": 9,
    "previous_high_initial": 17,
    "previous_initial": 26,
}


class SalesIndexParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.sections: List[Tuple[str, str, bool]] = []
        self.links: List[str] = []
        self.rows: List[Dict[str, str]] = []
        self.external_assets: List[str] = []
        self.current_section = ""
        self.current_project = ""

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        section = attributes.get("data-section")
        if section:
            self.current_section = section
            self.sections.append((tag, section, "open" in attributes))
        if tag == "article" and attributes.get("data-project-group"):
            self.current_project = attributes["data-project-group"]
        if tag == "a" and attributes.get("href") is not None:
            self.links.append(attributes["href"])
        if tag == "tr" and attributes.get("data-resource-id"):
            attributes["_section"] = self.current_section
            attributes["_project_group"] = self.current_project
            self.rows.append(attributes)
        if tag in ("script", "link") or (tag in ("img", "iframe") and attributes.get("src")):
            self.external_assets.append(tag)

    def handle_endtag(self, tag):
        if tag == "article":
            self.current_project = ""
        if tag in ("section", "details"):
            self.current_section = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="09-5 static sales index confirm")
    parser.add_argument("--target-date", required=True, help="対象日 YYYYMMDD")
    parser.add_argument("--index-path", type=Path, help="省略時は既存preview directory内index.html")
    return parser.parse_args()


def append_check(lines: List[str], errors: List[str], condition: bool, ok: str, ng: str) -> None:
    if condition:
        lines.append(f"[OK] {ok}")
    else:
        message = f"[NG] {ng}"
        lines.append(message)
        errors.append(message)


def main() -> None:
    logger = get_logger(STEP_NAME)
    args = parse_args()
    errors: List[str] = []
    lines = ["=== 09-5 static sales index confirm結果 ===", ""]
    try:
        date_part = args.target_date
        preview_dir = OUTPUT_BASE_DIR / f"reply_preview_{date_part}"
        index_path = args.index_path or preview_dir / "index.html"
        proposal_path = OUTPUT_BASE_DIR / f"proposal_ready_{date_part}.jsonl"
        human_path = OUTPUT_BASE_DIR / f"human_review_{date_part}.jsonl"
        canonical_path = OUTPUT_BASE_DIR / f"generate_sales_reply_draft_{date_part}.jsonl"
        protected_paths = (proposal_path, human_path, canonical_path)
        for path in protected_paths + (index_path,):
            if not path.is_file():
                raise FileNotFoundError(f"confirm入力が存在しません: {path}")
        hashes_before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in protected_paths}

        proposal, human, candidates, rechecks = load_sales_index_inputs(date_part)
        proposal_before = copy.deepcopy(proposal)
        human_before = copy.deepcopy(human)
        expected_html, summary = render_sales_index(
            proposal,
            human,
            candidates,
            rechecks,
            preview_dir,
            OUTPUT_BASE_DIR,
            date_part,
        )
        actual_html = index_path.read_text(encoding="utf-8")
        parser = SalesIndexParser()
        parser.feed(actual_html)

        expected_sections = ["proposal_ready", "high_initial", "high_additional", "other"]
        actual_sections = [section for _tag, section, _open in parser.sections]
        collapsed = {section: (tag == "details" and not opened) for tag, section, opened in parser.sections}
        row_keys = [
            (row.get("data-project-id", ""), row.get("data-resource-id", ""))
            for row in parser.rows
        ]
        section_counts = Counter(row.get("_section", "") for row in parser.rows)
        high_ranks = defaultdict(list)
        grouping_valid = True
        for row in parser.rows:
            grouping_valid = grouping_valid and row.get("data-project-id") == row.get("_project_group")
            if row.get("_section") in ("high_initial", "high_additional"):
                try:
                    rank = int(row.get("data-high-rank", ""))
                except ValueError:
                    rank = 0
                high_ranks[(row.get("_section", ""), row.get("data-project-id", ""))].append(rank)
        high_rank_order_valid = all(ranks == sorted(ranks) and all(rank > 0 for rank in ranks) for ranks in high_ranks.values())
        previous_rows = [row for row in parser.rows if row.get("data-previous-candidate") == "true"]
        previous_initial_rows = [
            row for row in previous_rows if row.get("_section") in ("proposal_ready", "high_initial")
        ]
        missing_links = 0
        for href in parser.links:
            target = preview_dir / unquote(href)
            if target.is_symlink() or not target.is_file():
                missing_links += 1

        hashes_after = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in protected_paths}
        lines.extend(
            [
                f"対象日: {date_part}",
                f"index: {index_path}",
                f"proposal_ready: {summary['proposal_ready']}",
                f"HIGH initial: {summary['high_initial']}",
                f"追加HIGH: {summary['high_additional']}",
                f"OTHER: {summary['other']}",
                f"初回: {summary['initial']}",
                f"total: {summary['total']}",
                f"candidate loss: {summary['candidate_loss']}",
                f"duplicate: {summary['duplicate']}",
                f"draft link count: {len(parser.links)}",
                f"draft link missing: {missing_links}",
                f"previous badge proposal: {summary['previous_proposal']}",
                f"previous badge HIGH initial: {summary['previous_high_initial']}",
                f"previous badge initial: {summary['previous_initial']}",
                f"previous badge all: {summary['previous_all']}",
            ]
        )
        for label, count in summary["initial_reason_counts"].items():
            lines.append(f"HIGH initial reason {label}: {count}")

        append_check(lines, errors, actual_html == expected_html, "indexがrendererの決定論的出力と一致", "indexがrenderer出力と不一致")
        append_check(lines, errors, actual_sections == expected_sections, "section順序OK", f"section順序不正: {actual_sections}")
        append_check(
            lines,
            errors,
            section_counts
            == Counter(
                {
                    "proposal_ready": summary["proposal_ready"],
                    "high_initial": summary["high_initial"],
                    "high_additional": summary["high_additional"],
                    "other": summary["other"],
                }
            ),
            "section掲載件数がqueue partitionと一致",
            f"section掲載件数不一致: {dict(section_counts)}",
        )
        append_check(lines, errors, collapsed.get("high_additional") is True, "追加HIGHは初期折りたたみ", "追加HIGHが初期折りたたみではない")
        append_check(lines, errors, collapsed.get("other") is True, "OTHERは初期折りたたみ", "OTHERが初期折りたたみではない")
        append_check(lines, errors, grouping_valid, "全候補がproject_message_id単位でgrouping", "project grouping不整合あり")
        append_check(lines, errors, high_rank_order_valid, "HIGHは案件内high_project_rank昇順", "HIGH rank順不整合あり")
        append_check(lines, errors, len(row_keys) == len(set(row_keys)) == summary["total"], "全pairを重複なく掲載", "掲載pairの欠落または重複あり")
        append_check(
            lines,
            errors,
            len(previous_rows) == summary["previous_all"]
            and len(previous_initial_rows) == summary["previous_initial"],
            "previous candidate badgeがqueue structured fieldと一致",
            "previous candidate badge件数不一致",
        )
        append_check(lines, errors, len(parser.links) == summary["draft_link_count"] and missing_links == 0, "全draft_refs linkが存在", f"draft link不整合: missing={missing_links}")
        append_check(lines, errors, not parser.external_assets, "外部CSS/JavaScript/画像参照なし", f"外部asset要素あり: {parser.external_assets}")
        append_check(lines, errors, proposal == proposal_before and human == human_before and hashes_before == hashes_after, "queue JSONLとcanonical draftは不変", "queue JSONLまたはcanonical draftが変化")
        append_check(lines, errors, "前回提案済" not in actual_html and "確認済み" not in actual_html, "禁止previous表現なし", "禁止previous表現あり")

        if date_part == "20260820":
            for key, expected in KNOWN_20260820.items():
                append_check(
                    lines,
                    errors,
                    summary[key] == expected,
                    f"20260820 {key}={expected}",
                    f"20260820 {key}不一致: actual={summary[key]} expected={expected}",
                )
    except Exception as error:
        message = f"[NG] confirm実行失敗: {error}"
        lines.append(message)
        errors.append(message)

    lines.extend(["", "【結果】NG" if errors else "【結果】OK"])
    text = "\n".join(lines)
    CONFIRM_RESULT.write_text(text + "\n", encoding="utf-8")
    for line in lines:
        if line.startswith("[NG]") or line == "【結果】NG":
            logger.error(line)
        else:
            logger.info(line)
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""20260819 production dataを上書きしない01-3 P1独立confirm。"""

import sys
from collections import Counter
from pathlib import Path
from typing import Dict, List, Set


_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
_TOOL_DIR = _STEP_DIR / "00_tool"
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
if str(_TOOL_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOL_DIR))

from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list
from common.logger import get_logger
from remove_individual_email import (
    EXCLUDE_LIST_PATH,
    detect_p1_exclusion_reason,
    is_excluded,
    load_exclude_list,
)


logger = get_logger("confirm_01-3_p1_subject_detector")

INPUT_MASTER = str(
    _PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)
INPUT_PREV = str(
    _PROJECT_ROOT
    / "01-2_remove_duplicate_emails"
    / "01_result"
    / "remove_duplicate_emails_raw.jsonl"
)

KNOWN_GOLD_IDS = {
    "1a01840aa9d32a40",
    "1a0175c745dfc200",
    "1a01758dde9c3823",
    "1a0180a63e1471f8",
    "1a017762ae84a084",
    "1a01841a949fbc27",
    "1a0175d9b8dba162",
    "1a0175dd9787f10f",
    "1a018790602ccf24",
    "1a017c682a98e9a2",
    "1a0179d48d974be5",
    "1a0178954bb794bb",
    "1a01761552730cc4",
    "1a01752235e3d8b6",
}
TYPE_A_IDS = {
    "1a017af482854082",
    "1a017c607554085f",
    "1a017aef32567aad",
    "1a017eaa42a7533f",
    "1a017b82b6f66fa9",
}
RECRUITMENT_NEGATIVE_IDS = {
    "1a018efcf402683b",
    "1a018d961bc3f358",
    "1a017de5e4fc081b",
}
MANAGEMENT_SCALE_NEGATIVE_IDS = {"1a018a855311e080"}
M365_NEGATIVE_IDS = {"1a018a4f7b800fa6"}


def _validate_fixture_presence(master: Dict[str, Dict], survivor_ids: Set[str]) -> bool:
    fixture_ids = (
        KNOWN_GOLD_IDS
        | TYPE_A_IDS
        | RECRUITMENT_NEGATIVE_IDS
        | MANAGEMENT_SCALE_NEGATIVE_IDS
        | M365_NEGATIVE_IDS
    )
    missing_master = fixture_ids - set(master)
    missing_survivor = fixture_ids - survivor_ids
    if missing_master:
        logger.error(f"masterに存在しないfixture: {sorted(missing_master)}")
    if missing_survivor:
        logger.error(f"manual survivorに存在しないfixture: {sorted(missing_survivor)}")
    return not missing_master and not missing_survivor


def _schema_is_message_id_only(records: List[Dict]) -> bool:
    return all(
        set(record) == {"message_id"}
        and isinstance(record["message_id"], str)
        and bool(record["message_id"])
        for record in records
    )


def main() -> None:
    for path in (INPUT_MASTER, INPUT_PREV, EXCLUDE_LIST_PATH):
        if not Path(path).exists():
            logger.error(f"ファイルが存在しません: {path}")
            sys.exit(1)

    master = read_jsonl_as_dict(INPUT_MASTER, key="message_id")
    prev_records = read_jsonl_as_list(INPUT_PREV)
    from_only_set, from_subj_rules = load_exclude_list(EXCLUDE_LIST_PATH)

    manual_excluded_ids: Set[str] = set()
    survivor_ids: Set[str] = set()
    missing_master_ids: Set[str] = set()
    for record in prev_records:
        message_id = record.get("message_id", "")
        master_record = master.get(message_id)
        if master_record is None:
            missing_master_ids.add(message_id)
            survivor_ids.add(message_id)
        elif is_excluded(master_record, from_only_set, from_subj_rules):
            manual_excluded_ids.add(message_id)
        else:
            survivor_ids.add(message_id)

    detector_reasons: Dict[str, str] = {}
    for message_id in survivor_ids:
        master_record = master.get(message_id)
        if master_record is None:
            continue
        reason = detect_p1_exclusion_reason(master_record.get("subject") or "")
        if reason:
            detector_reasons[message_id] = reason

    detector_excluded_ids = set(detector_reasons)
    gold_detected = detector_excluded_ids & KNOWN_GOLD_IDS
    gold_missed = KNOWN_GOLD_IDS - detector_excluded_ids
    type_a_excluded = detector_excluded_ids & TYPE_A_IDS
    recruitment_excluded = detector_excluded_ids & RECRUITMENT_NEGATIVE_IDS
    management_excluded = detector_excluded_ids & MANAGEMENT_SCALE_NEGATIVE_IDS
    m365_excluded = detector_excluded_ids & M365_NEGATIVE_IDS
    non_gold_excluded = detector_excluded_ids - KNOWN_GOLD_IDS
    unexpected_excluded = non_gold_excluded
    final_allow_ids = survivor_ids - detector_excluded_ids

    simulated_filtered = [{"message_id": mid} for mid in sorted(final_allow_ids)]
    simulated_removed = [
        {"message_id": mid}
        for mid in sorted(manual_excluded_ids | detector_excluded_ids)
    ]

    logger.info(f"total evaluated: {len(survivor_ids)}")
    logger.info(f"manual excluded: {len(manual_excluded_ids)}")
    logger.info(f"detector excluded: {len(detector_excluded_ids)}")
    logger.info(f"gold detected: {len(gold_detected)} / {len(KNOWN_GOLD_IDS)}")
    logger.info(f"gold missed: {len(gold_missed)} / {len(KNOWN_GOLD_IDS)}")
    logger.info(f"TYPE A excluded: {len(type_a_excluded)} / {len(TYPE_A_IDS)}")
    logger.info(
        "recruitment excluded: "
        f"{len(recruitment_excluded)} / {len(RECRUITMENT_NEGATIVE_IDS)}"
    )
    logger.info(
        "management-scale excluded: "
        f"{len(management_excluded)} / {len(MANAGEMENT_SCALE_NEGATIVE_IDS)}"
    )
    logger.info(f"M365 excluded: {len(m365_excluded)} / {len(M365_NEGATIVE_IDS)}")
    logger.info(f"non-gold excluded: {len(non_gold_excluded)}")
    logger.info(f"unexpected excluded: {len(unexpected_excluded)}")
    logger.info(f"final allow: {len(final_allow_ids)}")
    reason_counts = dict(sorted(Counter(detector_reasons.values()).items()))
    logger.info(f"reason counts: {reason_counts}")

    ok = True
    if len(prev_records) != len(manual_excluded_ids) + len(survivor_ids):
        logger.error("01-2 != manual excluded + manual survivor")
        ok = False
    if len(survivor_ids) != len(final_allow_ids) + len(detector_excluded_ids):
        logger.error("manual survivor != final allow + detector excluded")
        ok = False
    if missing_master_ids:
        logger.error(f"master欠落: {sorted(missing_master_ids)}")
        ok = False
    if not _validate_fixture_presence(master, survivor_ids):
        ok = False
    if type_a_excluded or recruitment_excluded or management_excluded or m365_excluded:
        logger.error("negative fixtureをdetectorが除外しました")
        ok = False
    if unexpected_excluded:
        logger.error(f"unexpected exclude: {sorted(unexpected_excluded)}")
        ok = False
    if not _schema_is_message_id_only(simulated_filtered):
        logger.error("filtered simulation schemaがmessage_id-onlyではありません")
        ok = False
    if not _schema_is_message_id_only(simulated_removed):
        logger.error("removed simulation schemaがmessage_id-onlyではありません")
        ok = False
    if len(simulated_filtered) + len(simulated_removed) != len(prev_records):
        logger.error("primary出力simulationの件数が01-2入力と整合しません")
        ok = False

    for message_id in sorted(gold_detected)[:3]:
        logger.info(
            f"代表detected: {message_id} / {detector_reasons[message_id]} / "
            f"{master[message_id].get('subject', '')}"
        )

    if not ok:
        logger.error("independent confirm NG")
        sys.exit(1)

    logger.ok("independent confirm OK")


if __name__ == "__main__":
    main()

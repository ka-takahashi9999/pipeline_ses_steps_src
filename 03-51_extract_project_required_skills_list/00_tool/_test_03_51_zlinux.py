#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from extract_project_required_skills_list import (
    PHASE_DICT_PATH,
    SKILL_DICT_PATH,
    build_record,
    load_phase_map,
    load_skill_list,
)


SKILL_ENTRIES = load_skill_list(SKILL_DICT_PATH)
PHASE_ENTRIES = load_phase_map(PHASE_DICT_PATH)


def _build(required, optional=None):
    return build_record(
        "focused-test",
        [{"skill": text} for text in required],
        [{"skill": text} for text in (optional or [])],
        SKILL_ENTRIES,
        PHASE_ENTRIES,
    )


def main():
    rec = _build(["z/Linux環境構築経験"])
    assert rec["required_skill_keywords"] == ["z/Linux"], rec

    rec = _build(["z/Linux環境構築・テスト・移行経験者"])
    assert rec["required_skill_keywords"] == ["z/Linux"], rec

    rec = _build(["ｚ／Ｌｉｎｕｘ環境構築経験"])
    assert rec["required_skill_keywords"] == ["z/Linux"], rec

    rec = _build(["Linux環境構築経験"])
    assert rec["required_skill_keywords"] == ["Linux"], rec

    rec = _build(["必須: z/Linux経験"], ["尚可: Linux運用経験"])
    assert rec["required_skill_keywords"] == ["z/Linux"], rec
    assert rec["optional_skill_keywords"] == ["Linux"], rec

    rec = _build(["z/Linux経験", "Linux環境構築経験"])
    assert rec["required_skill_keywords"] == ["z/Linux", "Linux"], rec

    for literal in ("IDS/IPS", "CI/CD", "C++", "X++"):
        rec = _build([literal])
        assert rec["required_skill_keywords"] == [literal], (literal, rec)

    print("focused test OK: z/Linux + literal special-character regression")


if __name__ == "__main__":
    main()

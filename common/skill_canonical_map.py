"""
スキル名の代表表記・検索用 alias マップ。

SKILL_CANONICAL_MAP:
    辞書で検知済みの表記を代表表記へ寄せるための map。
    key は skill_dictionary.txt で検出される表記ゆれ、
    value は 03-8 / 05-8 の skills 出力、および 06-8 以降のマッチングで使う代表表記。

SKILL_ALIAS_MAP:
    代表表記を本文検索用の表記ゆれ候補へ展開するための map。
    06-11 では alias のどれかが本文に一致した場合、
    matched_keywords には代表表記を入れる想定。

運用:
    同じ意味のスキル表記を skill_dictionary.txt に追加した場合、
    後続の完全一致マッチや本文検索で揺れないよう、
    必要に応じて SKILL_CANONICAL_MAP と SKILL_ALIAS_MAP の両方を確認する。
"""

SKILL_CANONICAL_MAP = {
    "Springboot": "Spring Boot",
    "SpringBoot": "Spring Boot",
    "NodeJS": "Node.js",
    "SQLServer": "SQL Server",
    "SQL-Server": "SQL Server",
}

SKILL_ALIAS_MAP = {
    "Spring Boot": ["Spring Boot", "Springboot", "SpringBoot"],
    "Node.js": ["Node.js", "NodeJS"],
    "SQL Server": ["SQL Server", "SQLServer", "SQL-Server"],
}

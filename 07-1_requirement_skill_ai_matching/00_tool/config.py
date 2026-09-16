"""07-1 production feature flags。"""

# Falseの間は従来Direct serial pathだけを使用する。
ENABLE_07_1_CONCURRENT: bool = True

# 500件testで検証済みの上限。5以上へ変更しない。
CONCURRENT_INITIAL: int = 2
CONCURRENT_MAX: int = 4

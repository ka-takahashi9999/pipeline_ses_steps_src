"""
06-8_match_skill_category feature flag 設定

ENABLE_SKILL_CATEGORY_MATCH = False の場合:
  - 入力ペアをそのままpass-through出力（match_skill_categoryキーは追加しない）
  - パイプラインは継続（停止しない）

ENABLE_SKILL_CATEGORY_MATCH = True の場合:
  - 案件と要員のskillsを比較してマッチ判定を実行
  - 1つ以上一致すればtrue、0件はfalse
"""

# feature flag: スキルカテゴリマッチの有効/無効
ENABLE_SKILL_CATEGORY_MATCH: bool = True

SKILL_MATCH_EXCLUDED = [
    "クラウド",
    "バックエンド",
    "フロントエンド",
    "ネットワーク",
]

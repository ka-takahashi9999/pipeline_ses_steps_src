"""
03-8_extract_project_skill_category feature flag 設定

ENABLE_SKILL_CATEGORY = False の場合:
  - 全レコードを skills=[], skills_by_category={}, skills_raw=null でpass-through出力
  - パイプラインは継続（停止しない）

ENABLE_SKILL_CATEGORY = True の場合:
  - skill_dictionary.txt を使用してルールベース抽出を実行
"""

# feature flag: スキルカテゴリ抽出の有効/無効
ENABLE_SKILL_CATEGORY: bool = True

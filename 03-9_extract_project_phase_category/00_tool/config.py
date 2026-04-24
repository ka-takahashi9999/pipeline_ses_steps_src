"""
03-9_extract_project_phase_category feature flag 設定

ENABLE_PHASE_CATEGORY = False の場合:
  - 全レコードを phases=[], phases_raw=null でpass-through出力
  - パイプラインは継続（停止しない）

ENABLE_PHASE_CATEGORY = True の場合:
  - phase_dictionary.txt を使用してルールベース抽出を実行
"""

# feature flag: 工程カテゴリ抽出の有効/無効
ENABLE_PHASE_CATEGORY: bool = True

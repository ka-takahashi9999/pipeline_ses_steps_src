"""
03-10_extract_project_location feature flag 設定

ENABLE_LOCATION = False の場合:
  - 全レコードを location=null, location_raw=null でpass-through出力
  - パイプラインは継続（停止しない）

ENABLE_LOCATION = True の場合:
  - location_dictionary.txt を使用してルールベース抽出を実行
"""

# feature flag: ロケーション抽出の有効/無効
ENABLE_LOCATION: bool = True

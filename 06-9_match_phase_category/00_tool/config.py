"""
06-9_match_phase_category feature flag 設定

ENABLE_PHASE_CATEGORY_MATCH = False の場合:
  - 入力ペアをそのままpass-through出力（match_phase_categoryキーは追加しない）
  - パイプラインは継続（停止しない）

ENABLE_PHASE_CATEGORY_MATCH = True の場合:
  - 案件と要員のphasesを比較してマッチ判定を実行
  - 両方に値があり1つ以上一致すればtrue、1つも一致しなければfalse
  - どちらかが空/nullの場合はtrue（工程不明は通過）
"""

# feature flag: 工程カテゴリマッチの有効/無効
ENABLE_PHASE_CATEGORY_MATCH: bool = True

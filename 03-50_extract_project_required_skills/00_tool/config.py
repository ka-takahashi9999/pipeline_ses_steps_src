# 03-50_extract_project_required_skills 設定ファイル

# LLMフォールバックの有効/無効
# True  : ルール抽出が空のとき LLM (GPT-4o-mini) で補完する（本番向け）
# False : LLM は呼ばず、ルール抽出が空のケースを rule_empty として記録する（開発中の失敗ケース可視化用）
USE_LLM_FALLBACK = False

"""
Step 02-1 設定ファイル

USE_LLM_CLASSIFY:
  False（デフォルト）: ルールベース + 添付判定のみで分類
  True              : ルールで判定困難（ambiguous/unknown）の場合のみLLMを使用
"""

# LLM補助フラグ（デフォルトOFF）
USE_LLM_CLASSIFY: bool = False

# LLMで使用するモデル
LLM_MODEL: str = "gpt-4o-mini"

# ルール判定：resource/project どちらかが相手を上回るための最小スコア差
# （コンテキストパターン強化に伴い 0.7 → 0.5 に引き下げ）
RULE_MARGIN: float = 0.5

# ルール判定：この信頼度以上でルール確定とみなす（0〜1）
# （コンテキストシグナルなしの場合の閾値。コンテキストあり=0.05）
RULE_MIN_CONFIDENCE: float = 0.2

# LLM補助の対象にするスコア（ambiguous/unknown のとき使用）
LLM_MAX_TOKENS: int = 256

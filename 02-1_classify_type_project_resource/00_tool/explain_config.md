# explain_config

## 目的
- Step 02-1 設定ファイル

## 入力ファイルと参照方法
- 外部入力ファイルはこのモジュール単体では明示されていません。

## 出力ファイルと構造
- 外部ファイル出力はこのモジュール単体では明示されていません。

## 処理ロジックの詳細
- 主な設定項目: `USE_LLM_CLASSIFY`, `LLM_MODEL`, `RULE_MARGIN`, `RULE_MIN_CONFIDENCE`, `LLM_MAX_TOKENS`

## LLM使用有無と使用箇所
- LLM使用: 無（実装あり / feature flag OFF）
- `USE_LLM_CLASSIFY` の既定値が `False` のため、現行の本番実行ではLLMを呼び出しません。
- `LLM_MODEL="gpt-4o-mini"` / `LLM_MAX_TOKENS=256` は `USE_LLM_CLASSIFY=True` にした場合のみ使用されます。
- `USE_LLM_CLASSIFY` をONにする場合は、本ドキュメントと `PIPELINE_OVERVIEW.md` のLLM使用表記も更新すること。

## エラー時の挙動
- 明示的な異常系分岐は少なく、例外送出または呼び出し元処理に委ねます。

## 注意事項
- 設定値を参照する側の実装変更と整合させる必要があります。

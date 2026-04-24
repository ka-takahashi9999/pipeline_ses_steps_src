# explain_extract_resource_budget

## 目的
- Step 05-1: 要員メールから希望単価（月額）をルールベースで抽出

## 入力ファイルと参照方法
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- `02-2_classify_output_file_project_resource/01_result/resources.jsonl`
- `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`

## 出力ファイルと構造
- `extract_resource_budget.jsonl`
- `99_price_null_extract_resource_budget.jsonl`
- 主な辞書/レコードキー: `message_id`, `desired_unit_price`, `desired_unit_price_sub_infor`

## 処理ロジックの詳細
- `_n`: NFKC正規化（全角→半角、波ダッシュ等を統一）
- `_get_segments`: 価格キーワード周辺のテキストセグメントを返す（重複排除）
- `_hourly_daily`: 時給・日給を検出して月換算で返す
- `_man_patterns`: 万円パターンで価格を抽出。Returns: (price, min, max, reason, raw)
- `rule_extract`: ルールベースで希望単価を抽出。
- `build_record`: 1メール分の出力レコードを構築する。本文抽出不可の場合は件名をフォールバック使用。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

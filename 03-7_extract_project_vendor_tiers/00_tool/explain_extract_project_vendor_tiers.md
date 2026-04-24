# explain_extract_project_vendor_tiers

## 目的
- Step 03-7: 案件メールから商流制限をルールベースで抽出

## 入力ファイルと参照方法
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- `02-2_classify_output_file_project_resource/01_result/projects.jsonl`

## 出力ファイルと構造
- `extract_project_vendor_tiers.jsonl`
- `99_vendor_tiers_null_extract_project_vendor_tiers.jsonl`
- 主な辞書/レコードキー: `message_id`, `commercial_flow_level`, `commercial_flow_raw`, `commercial_flow_delegation_limit`, `commercial_flow_source`

## 処理ロジックの詳細
- `_normalize`: NFKC正規化 + 御社→貴社 + 迄→まで + 空白統一
- `_classify`: テキストから商流レベルとマッチ文字列を返す。
- `rule_extract_vendor_tiers`: ルールベースで商流レベルを抽出する。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

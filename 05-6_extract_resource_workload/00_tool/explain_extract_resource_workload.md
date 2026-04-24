# explain_extract_resource_workload

## 目的
- Step 05-6: 要員メールから稼働率をルールベースで抽出

## 入力ファイルと参照方法
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- `02-2_classify_output_file_project_resource/01_result/resources.jsonl`

## 出力ファイルと構造
- `extract_resource_workload.jsonl`
- `99_workload_null_extract_resource_workload.jsonl`
- 主な辞書/レコードキー: `message_id`, `workload_min`, `workload_max`, `workload_max_source`, `workload_raw`

## 処理ロジックの詳細
- `_n`: NFKC正規化（全角→半角、波ダッシュ等を統一）
- `_to_days`: 数字または漢数字を日数（int）に変換する。変換不可はNone。
- `_days_pct`: 週N日を%に変換する（週5日=100%）。
- `rule_extract_workload`: ルールベースで稼働率を抽出する。
- `_is_valid`: workload_min/max が 1〜100 の整数であること。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

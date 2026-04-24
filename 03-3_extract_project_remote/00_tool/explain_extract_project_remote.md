# explain_extract_project_remote

## 目的
- Step 03-3: 案件メールからリモート勤務条件をルールベースで抽出

## 入力ファイルと参照方法
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- `02-2_classify_output_file_project_resource/01_result/projects.jsonl`

## 出力ファイルと構造
- `extract_project_remote.jsonl`
- `99_remote_null_extract_project_remote.jsonl`
- 主な辞書/レコードキー: `message_id`, `remote_type`, `remote_type_source`, `remote_days_per_week`, `remote_raw`

## 処理ロジックの詳細
- `_n`: NFKC正規化（全角→半角、波ダッシュ等を統一）
- `_parse_weekly_days`: 週N日リモートのNを整数に変換する。
- `rule_extract_remote`: ルールベースでリモート種別を抽出する。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

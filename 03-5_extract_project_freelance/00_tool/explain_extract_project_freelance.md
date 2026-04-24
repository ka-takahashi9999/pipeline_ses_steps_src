# explain_extract_project_freelance

## 目的
- Step 03-5: 案件メールから個人事業主制限をルールベースで抽出

## 入力ファイルと参照方法
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- `02-2_classify_output_file_project_resource/01_result/projects.jsonl`

## 出力ファイルと構造
- `extract_project_freelance.jsonl`
- `99_freelance_null_extract_project_freelance.jsonl`
- 主な辞書/レコードキー: `message_id`, `freelance_ok`, `freelance_source`, `freelance_raw`

## 処理ロジックの詳細
- `_n`: NFKC正規化（全角→半角、波ダッシュ等を統一）
- `rule_extract_freelance`: ルールベースでフリーランス可否を抽出する。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

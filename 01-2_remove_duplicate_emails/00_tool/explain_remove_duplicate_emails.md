# explain_remove_duplicate_emails

## 目的
- Step 01-2: メール重複除去スクリプト

## 入力ファイルと参照方法
- `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`

## 出力ファイルと構造
- `remove_duplicate_emails_raw.jsonl`
- `99_duplicate_emails_raw.jsonl`
- `01_result/remove_duplicate_emails_raw.jsonl  （重複除去後の message_id）`
- `01_result/99_duplicate_emails_raw.jsonl      （除去された重複の message_id）`

## 処理ロジックの詳細
- `normalize_key`: 重複判定用キー正規化。
- `deduplicate`: (from, subject) キーで重複除去する。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

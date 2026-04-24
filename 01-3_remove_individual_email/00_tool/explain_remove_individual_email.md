# explain_remove_individual_email

## 目的
- Step 01-3: 個別除外処理スクリプト

## 入力ファイルと参照方法
- `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`
- `01-2_remove_duplicate_emails/01_result/remove_duplicate_emails_raw.jsonl`

## 出力ファイルと構造
- `remove_individual_emails_raw.jsonl`
- `99_removed_individual_emails_raw.jsonl`
- `01_result/remove_individual_emails_raw.jsonl  （除外後の message_id）`
- `01_result/99_removed_individual_emails_raw.jsonl （除外された message_id）`

## 処理ロジックの詳細
- `normalize`: 比較用に正規化（NFKC + 小文字 + 前後空白除去）。
- `extract_email`: 'Display Name <email@example.com>' や '<email@example.com>' から
- `subject_matches`: subject の一致判定。
- `load_exclude_list`: 除外リストを読み込む。
- `is_excluded`: レコードが除外対象かどうかを判定する。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。
- 個別異常は警告ログに記録します。
- レコード単位の失敗はスキップ継続する分岐があります。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

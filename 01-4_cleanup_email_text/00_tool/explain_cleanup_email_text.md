# explain_cleanup_email_text

## 目的
- Step 01-4: メール本文クリーニングスクリプト

## 入力ファイルと参照方法
- `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`
- `01-3_remove_individual_email/01_result/remove_individual_emails_raw.jsonl`

## 出力ファイルと構造
- `cleanup_email_text_emails_raw.jsonl`
- `01_result/cleanup_email_text_emails_raw.jsonl`

## 処理ロジックの詳細
- `normalize`: Unicode NFKC 正規化 + 前後空白除去。
- `load_cleanup_rules`: cleanup_rules.txt を読み込んでルールオブジェクトを返す。
- `_is_separator_line`: 区切り線パターンに一致するか確認する。
- `_is_greeting_line`: 挨拶文パターンを含むか確認する。
- `_find_signature_start`: 署名開始行のインデックスを返す。
- `_contains_url`: 行内にURLが含まれるか確認する。
- `_find_lines_to_remove_with_adjacent_url`: 特定文言を含む行と、その隣接URL行のインデックス集合を返す。
- `cleanup_body`: メール本文をクリーニングして返す。

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

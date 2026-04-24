# explain_mail_display_format

## 目的
- 09-1_mail_display_format
- マッチペアを人間可読形式で1ペア1ファイル出力し、S3に保存する。

## 入力ファイルと参照方法
- `08-4_match_score_sort/01_result/match_score_sort_100percent.jsonl`
- `08-4_match_score_sort/01_result/match_score_sort_80to99percent.jsonl`
- `08-4_match_score_sort/01_result/match_score_sort_60to79percent.jsonl`
- `08-4_match_score_sort/01_result/match_score_sort_40to59percent.jsonl`
- `08-4_match_score_sort/01_result/match_score_sort_20to39percent.jsonl`
- `08-4_match_score_sort/01_result/match_score_sort_1to19percent.jsonl`
- `08-4_match_score_sort/01_result/match_score_sort_0percent.jsonl`
- `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`

## 出力ファイルと構造
- `09-1_mail_display_format/01_result/mail_display_format_YYYYMMDD/` 配下のテキストファイル群
- `09-1_mail_display_format/01_result/mail_display_format_YYYYMMDD.zip`

## 処理ロジックの詳細
- `normalize_body`: メール本文を改行整形する。連続する空行を1行に圧縮。
- `format_pair`: 1ペアのテキスト出力を生成する。
- `delete_previous_local_dirs`: 前回日付のmail_display_format_YYYYMMDDディレクトリを削除する。
- `delete_previous_s3_zips`: 前回日付の圧縮ファイルをS3から削除する（当日分は残す）。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。
- 個別異常は警告ログに記録します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。
- 生成したZIPは S3 (`s3://technoverse/pipeline_ses_steps/...`) にアップロードします。

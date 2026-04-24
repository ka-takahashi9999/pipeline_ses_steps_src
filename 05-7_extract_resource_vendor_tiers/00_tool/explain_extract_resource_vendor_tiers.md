# explain_extract_resource_vendor_tiers

## 目的
- Step 05-7: 要員メールから商流情報（vendor_flow）をルールベースで抽出

## 入力ファイルと参照方法
- `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- `02-2_classify_output_file_project_resource/01_result/resources.jsonl`

## 出力ファイルと構造
- `extract_resource_vendor_tiers.jsonl`
- `99_vendor_tiers_null_extract_resource_vendor_tiers.jsonl`
- 主な辞書/レコードキー: `message_id`, `vendor_flow`, `vendor_flow_raw`

## 処理ロジックの詳細
- `_n`: NFKC正規化（全角→半角）
- `_extract_email`: 'Name <addr>' / '<addr>' / 'addr' 形式から
- `_is_technoverse`: 送信元が @technoverse.co.jp かどうか判定
- `_extract_depth_digit`: 本文から1の位（深さ: 0/1/2）を抽出する。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

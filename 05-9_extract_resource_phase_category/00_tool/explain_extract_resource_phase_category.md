# explain_extract_resource_phase_category

## 目的
- Step 05-9: 要員メールから工程をルールベースで抽出

## 入力ファイルと参照方法
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- `02-2_classify_output_file_project_resource/01_result/resources.jsonl`

## 出力ファイルと構造
- `extract_resource_phase_category.jsonl`
- `99_phase_null_extract_resource_phase_category.jsonl`
- 主な辞書/レコードキー: `message_id`, `phases`, `phases_raw`

## 処理ロジックの詳細
- `load_phase_dictionary`: YAML形式の工程辞書を読み込む。
- `extract_phases`: 本文から工程を抽出する。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。
- レコード単位の失敗はスキップ継続する分岐があります。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

# explain_extract_project_location

## 目的
- Step 03-10: 案件メールから作業場所（ロケーション）をルールベースで抽出し地方に分類する

## 入力ファイルと参照方法
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- `02-2_classify_output_file_project_resource/01_result/projects.jsonl`
- `03-10_extract_project_location/00_tool/location_dictionary.txt`

## 出力ファイルと構造
- `extract_project_location.jsonl`
- `99_location_null_extract_project_location.jsonl`
- `01_result/extract_project_location.jsonl`
- `01_result/99_location_null_extract_project_location.jsonl`
- 主な辞書/レコードキー: `message_id`, `location`, `location_raw`, `location_source`

## 処理ロジックの詳細
- `load_location_dictionary`: YAML形式のロケーション辞書を読み込む。
- `build_extracted_record`: 署名除去 → パーサー呼び出し → レコード構築

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

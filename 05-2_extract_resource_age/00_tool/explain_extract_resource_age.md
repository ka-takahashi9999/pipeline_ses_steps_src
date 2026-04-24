# explain_extract_resource_age

## 目的
- Step 05-2: 要員メールから現在年齢をルールベースで抽出

## 入力ファイルと参照方法
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- `02-2_classify_output_file_project_resource/01_result/resources.jsonl`

## 出力ファイルと構造
- `extract_resource_age.jsonl`
- `99_age_null_extract_resource_age.jsonl`
- 主な辞書/レコードキー: `message_id`, `current_age`, `current_age_source`, `current_age_raw`

## 処理ロジックの詳細
- `_n`: NFKC正規化（全角→半角、波ダッシュ等を統一）
- `_get_age_segments`: 年齢キーワード周辺のセグメントを返す（重複排除）
- `_decade_to_age`: XX代から代表年齢を返す。前半→+3、後半→+7、なし→+5（中央値）
- `_extract_from_segment`: セグメント内から current_age と current_age_raw を抽出する。
- `rule_extract_age`: ルールベースで現在年齢を抽出。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

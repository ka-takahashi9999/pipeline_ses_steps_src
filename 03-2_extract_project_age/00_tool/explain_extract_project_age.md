# explain_extract_project_age

## 目的
- Step 03-2: 案件メールから年齢制限をルールベースで抽出

## 入力ファイルと参照方法
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- `02-2_classify_output_file_project_resource/01_result/projects.jsonl`

## 出力ファイルと構造
- `extract_project_age.jsonl`
- `99_age_null_extract_project_age.jsonl`
- 主な辞書/レコードキー: `message_id`, `age_max`, `age_max_source`, `age_raw`

## 処理ロジックの詳細
- `_n`: NFKC正規化（全角→半角、波ダッシュ等を統一）
- `_decade_max`: XX代の最大年齢を返す。前半/半ば→+4、後半→+9、なし→+9
- `_get_age_segments`: 年齢キーワード周辺のセグメントを返す（重複排除）
- `_extract_from_segment`: セグメント内から age_max と age_raw を抽出する。
- `_extract_from_fulltext_fallback`: キーワードアンカーなしで全行を走査するfallback。
- `rule_extract_age`: ルールベースで年齢上限を抽出。

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

# explain_match_all_message_id

## 目的
- Step 06-0: 全案件×全要員の総当たりペアを生成（06系の起点）

## 入力ファイルと参照方法
- `02-2_classify_output_file_project_resource/01_result/projects.jsonl`
- `02-2_classify_output_file_project_resource/01_result/resources.jsonl`

## 出力ファイルと構造
- `matched_pairs_all.jsonl`

## 処理ロジックの詳細
- トップレベル定義を他モジュールから利用する前提です。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

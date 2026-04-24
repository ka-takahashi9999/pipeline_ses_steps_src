# explain_classify_output_file_project_resource

## 目的
- Step 02-2: 分類結果ファイル分割出力

## 入力ファイルと参照方法
- `02-1_classify_type_project_resource/01_result/classify_types_project_resource.jsonl`
- `02-1_classify_type_project_resource/01_result/99_no_classify_types_project_resource.jsonl`

## 出力ファイルと構造
- `projects.jsonl`
- `resources.jsonl`
- `ambiguous.jsonl`
- `unknown.jsonl`

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

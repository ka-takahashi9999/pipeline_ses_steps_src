# explain_match_foreign

## 目的
- 06-4_match_foreign
- 案件の外国籍制限と要員の国籍を比較してマッチ判定する。

## 入力ファイルと参照方法
- `06-3_match_remote/01_result/matched_pairs_remote.jsonl`
- `03-4_extract_project_foreign/01_result/extract_project_foreign.jsonl`
- `05-4_extract_resource_foreign/01_result/extract_resource_foreign.jsonl`

## 出力ファイルと構造
- `06-4_match_foreign/01_result/matched_pairs_foreign.jsonl`
- `06-4_match_foreign/01_result/99_no_matched_pairs_foreign.jsonl`

## 処理ロジックの詳細
- `judge_foreign_match`: 外国籍マッチ判定。

## LLM使用有無と使用箇所
- LLM使用: 無
- docstring上でLLM使用禁止が明記されています。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

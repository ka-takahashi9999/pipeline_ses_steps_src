# explain_match_workload

## 目的
- 06-6_match_workload
- 案件の稼働率制限と要員の稼働率を比較してマッチ判定する。

## 入力ファイルと参照方法
- `06-5_match_freelance/01_result/matched_pairs_freelance.jsonl`
- `03-6_extract_project_workload/01_result/extract_project_workload.jsonl`
- `05-6_extract_resource_workload/01_result/extract_resource_workload.jsonl`

## 出力ファイルと構造
- `06-6_match_workload/01_result/matched_pairs_workload.jsonl`
- `06-6_match_workload/01_result/99_no_matched_pairs_workload.jsonl`

## 処理ロジックの詳細
- `judge_workload_match`: 稼働率マッチ判定（区間オーバーラップ）。

## LLM使用有無と使用箇所
- LLM使用: 無
- docstring上でLLM使用禁止が明記されています。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

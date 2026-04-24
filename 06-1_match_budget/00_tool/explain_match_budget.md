# explain_match_budget

## 目的
- 06-1_match_budget
- 案件の単価と要員の希望単価を比較してマッチ判定する。

## 入力ファイルと参照方法
- `06-0_match_all_message_id/01_result/matched_pairs_all.jsonl`
- `03-1_extract_project_budget/01_result/extract_project_budget.jsonl`
- `05-1_extract_resource_budget/01_result/extract_resource_budget.jsonl`

## 出力ファイルと構造
- `06-1_match_budget/01_result/matched_pairs_budget.jsonl`
- `06-1_match_budget/01_result/99_no_matched_pairs_budget.jsonl`

## 処理ロジックの詳細
- `judge_budget_match`: 単価マッチ判定。

## LLM使用有無と使用箇所
- LLM使用: 無
- docstring上でLLM使用禁止が明記されています。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

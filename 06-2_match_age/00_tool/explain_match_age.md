# explain_match_age

## 目的
- 06-2_match_age
- 案件の年齢制限と要員の年齢を比較してマッチ判定する。

## 入力ファイルと参照方法
- `06-1_match_budget/01_result/matched_pairs_budget.jsonl`
- `03-2_extract_project_age/01_result/extract_project_age.jsonl`
- `05-2_extract_resource_age/01_result/extract_resource_age.jsonl`

## 出力ファイルと構造
- `06-2_match_age/01_result/matched_pairs_age.jsonl`
- `06-2_match_age/01_result/99_no_matched_pairs_age.jsonl`

## 処理ロジックの詳細
- `judge_age_match`: 年齢マッチ判定。

## LLM使用有無と使用箇所
- LLM使用: 無
- docstring上でLLM使用禁止が明記されています。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

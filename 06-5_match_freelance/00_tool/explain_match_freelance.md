# explain_match_freelance

## 目的
- 06-5_match_freelance
- 案件の個人事業主制限と要員の雇用形態を比較してマッチ判定する。

## 入力ファイルと参照方法
- `06-4_match_foreign/01_result/matched_pairs_foreign.jsonl`
- `03-5_extract_project_freelance/01_result/extract_project_freelance.jsonl`
- `05-5_extract_resource_freelance/01_result/extract_resource_freelance.jsonl`

## 出力ファイルと構造
- `06-5_match_freelance/01_result/matched_pairs_freelance.jsonl`
- `06-5_match_freelance/01_result/99_no_matched_pairs_freelance.jsonl`

## 処理ロジックの詳細
- `judge_freelance_match`: 個人事業主マッチ判定。

## LLM使用有無と使用箇所
- LLM使用: 無
- docstring上でLLM使用禁止が明記されています。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

# explain_match_vendor_tiers

## 目的
- 06-7_match_vendor_tiers
- 案件の商流制限と要員の商流を比較してマッチ判定する。

## 入力ファイルと参照方法
- `06-6_match_workload/01_result/matched_pairs_workload.jsonl`
- `03-7_extract_project_vendor_tiers/01_result/extract_project_vendor_tiers.jsonl`
- `05-7_extract_resource_vendor_tiers/01_result/extract_resource_vendor_tiers.jsonl`

## 出力ファイルと構造
- `06-7_match_vendor_tiers/01_result/matched_pairs_vendor_tiers.jsonl`
- `06-7_match_vendor_tiers/01_result/99_no_matched_pairs_vendor_tiers.jsonl`

## 処理ロジックの詳細
- `judge_vendor_tiers_match`: 商流マッチ判定。

## LLM使用有無と使用箇所
- LLM使用: 無
- docstring上でLLM使用禁止が明記されています。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

# explain_match_location

## 目的
- 06-10_match_location
- 案件と要員のlocation（勤務地）を比較してマッチ判定する。

## 入力ファイルと参照方法
- `06-9_match_phase_category/01_result/matched_pairs_phase_category.jsonl`
- `03-3_extract_project_remote/01_result/extract_project_remote.jsonl`
- `03-10_extract_project_location/01_result/extract_project_location.jsonl`
- `05-10_extract_resource_location/01_result/extract_resource_location.jsonl`

## 出力ファイルと構造
- `06-10_match_location/01_result/matched_pairs_location.jsonl`
- `06-10_match_location/01_result/99_not_matched_pairs_location.jsonl`

## 処理ロジックの詳細
- `judge_location_match`: locationマッチ判定。

## LLM使用有無と使用箇所
- LLM使用: 無
- docstring上でLLM使用禁止が明記されています。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

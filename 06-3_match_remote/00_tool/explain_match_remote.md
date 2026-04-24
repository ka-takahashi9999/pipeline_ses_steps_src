# explain_match_remote

## 目的
- 06-3_match_remote
- 案件のリモート条件と要員のリモート希望を比較してマッチ判定する。

## 入力ファイルと参照方法
- `06-2_match_age/01_result/matched_pairs_age.jsonl`
- `03-3_extract_project_remote/01_result/extract_project_remote.jsonl`
- `05-3_extract_resource_remote/01_result/extract_resource_remote.jsonl`

## 出力ファイルと構造
- `06-3_match_remote/01_result/matched_pairs_remote.jsonl`
- `06-3_match_remote/01_result/99_no_matched_pairs_remote.jsonl`

## 処理ロジックの詳細
- `judge_remote_match`: リモートマッチ判定。

## LLM使用有無と使用箇所
- LLM使用: 無
- docstring上でLLM使用禁止が明記されています。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

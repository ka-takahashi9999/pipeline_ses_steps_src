# explain_match_score_partition

## 目的
- 08-3_match_score_partition
- 必須スキル一致率でファイルを7分割する。

## 入力ファイルと参照方法
- `08-2_match_score_aggregation/01_result/match_score_aggregation.jsonl`

## 出力ファイルと構造
- `08-3_match_score_partition/01_result`

## 処理ロジックの詳細
- `init_output_files`: 出力7ファイルをno_matchステータスで初期化する。
- `classify`: required_skills_match_rateをパーティションインデックスに変換する。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。

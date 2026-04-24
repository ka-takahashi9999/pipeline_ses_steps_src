# explain_match_score_aggregation

## 目的
- 08-2_match_score_aggregation
- 必須スキル一致率・尚可スキル一致率・合計スコアを算出する。

## 入力ファイルと参照方法
- `08-1_restore_and_merge_requirement_skill_ai_matching/01_result/merged_requirement_skill_ai_matching.jsonl`

## 出力ファイルと構造
- `08-2_match_score_aggregation/01_result/match_score_aggregation.jsonl`

## 処理ロジックの詳細
- `calc_match_rate`: スキルリストのtrue一致率を返す。スキルが0件の場合は0.0。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。

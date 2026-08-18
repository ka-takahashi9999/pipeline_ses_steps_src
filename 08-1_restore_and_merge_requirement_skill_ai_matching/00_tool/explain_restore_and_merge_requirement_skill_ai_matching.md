# explain_restore_and_merge_requirement_skill_ai_matching

## 目的
- 08-1_restore_and_merge_requirement_skill_ai_matching
- Cache HIT ペアを **Success Cache** の評価結果から復元して current run の `message_id` へ rebind し、
  07-1 の新規成功結果とマージして全件完成版を作る。
- その後、今回の 07-1 正常結果だけを comparison_key 単位で Success Cache へ upsert する。

## 入力ファイルと参照方法
- `06-80_duplicate_proposal_check/01_result/duplicate_proposal_check.jsonl`（Cache MISS ペア）
- `06-80_duplicate_proposal_check/01_result/99_duplicate_duplicate_proposal_check.jsonl`（Cache HIT ペア）
- `06-80_duplicate_proposal_check/01_result/duplicate_proposal_check_diff_file.jsonl`（今回全ペアの comparison_key）
- `07-1_requirement_skill_ai_matching/01_result/requirement_skill_ai_matching.jsonl`（今回の新規成功結果）
- `08-1_restore_and_merge_requirement_skill_ai_matching/01_result/success_cache_requirement_skill_ai_matching.jsonl`（Success Cache）

## 出力ファイルと構造
- `08-1_restore_and_merge_requirement_skill_ai_matching/01_result/restored_requirement_skill_ai_matching.jsonl`
- `08-1_restore_and_merge_requirement_skill_ai_matching/01_result/merged_requirement_skill_ai_matching.jsonl`
- `08-1_restore_and_merge_requirement_skill_ai_matching/01_result/99_error_restore_requirement_skill_ai_matching.jsonl`
- `08-1_restore_and_merge_requirement_skill_ai_matching/01_result/success_cache_requirement_skill_ai_matching.jsonl`（upsert）
- 主な辞書/レコードキー: `project_info`, `resource_info`, `duplicate_proposal_check`, `compare_key`, `error_type`, `error_message`
- 監査用キー: `restore_key_type`（`success_cache_comparison_key`）, `restore_source_message_ids`

## 処理ロジックの詳細
- identity は **comparison_key の4フィールド**（`project_from` / `project_subject` / `resource_from` / `resource_subject`）。
  `message_id` は identity ではなく、run tracking と cache の `source_message_ids` として保持する。
- Cache HIT ペア: Success Cache の評価結果を current run の `message_id` へ rebind して merged へ入れる。
  該当 comparison_key が cache に無い場合は `cache_hit_source_not_found` として error に記録する。
- Cache MISS ペア: 07-1 の新規成功結果を merged へ入れ、その comparison_key を Success Cache へ upsert する。
  07-1 が error だったペアは成功結果が無いため `new_ai_result_not_found` として error に記録し、**cache へは登録しない**。
- そのため 07-1 error の comparison_key は次回 run で Cache MISS となり再評価される。
- cache の schema 不整合・version 不一致・重複 comparison_key、および同一 upsert 内の重複キーは
  Cache MISS へ逃がさず `SuccessCacheError` で停止する。
- トップレベル定義を他モジュールから利用する前提です。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。
- 個別異常は警告ログに記録します。
- レコード単位の失敗はスキップ継続する分岐があります。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- 後続stepへは `message_id` で受け渡すが、**Success Cache の identity は comparison_key** であり `message_id` ではない。
  同じ from / subject のメールが別日に再送され `message_id` だけ変わった場合も同一 comparison_key として扱う。
- `bk_merged_requirement_skill_ai_matching.jsonl` は旧 restore backup 方式の legacy / HOLD ファイルで、
  現行処理では読み込まない。現行入力として扱わないこと。

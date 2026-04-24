# explain_restore_and_merge_requirement_skill_ai_matching

## 目的
- 08-1_restore_and_merge_requirement_skill_ai_matching
- 重複ペアを前回完成版から復元し、07-1 の新規評価結果とマージして全件完成版を作る。

## 入力ファイルと参照方法
- `06-80_duplicate_proposal_check/01_result/duplicate_proposal_check.jsonl`
- `06-80_duplicate_proposal_check/01_result/99_duplicate_duplicate_proposal_check.jsonl`
- `06-80_duplicate_proposal_check/01_result/duplicate_proposal_check_diff_file.jsonl`
- `07-1_requirement_skill_ai_matching/01_result/requirement_skill_ai_matching.jsonl`
- `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`
- `08-1_restore_and_merge_requirement_skill_ai_matching/01_result/bk_merged_requirement_skill_ai_matching.jsonl`

## 出力ファイルと構造
- `08-1_restore_and_merge_requirement_skill_ai_matching/01_result/restored_requirement_skill_ai_matching.jsonl`
- `08-1_restore_and_merge_requirement_skill_ai_matching/01_result/merged_requirement_skill_ai_matching.jsonl`
- `08-1_restore_and_merge_requirement_skill_ai_matching/01_result/99_error_restore_requirement_skill_ai_matching.jsonl`
- 主な辞書/レコードキー: `project_info`, `resource_info`, `duplicate_proposal_check`, `compare_key`, `error_type`, `error_message`

## 処理ロジックの詳細
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
- `message_id` を主キーとする処理との整合が必要です。

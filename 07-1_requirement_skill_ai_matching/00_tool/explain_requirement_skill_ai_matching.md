# explain_requirement_skill_ai_matching

## 目的
- 07-1_requirement_skill_ai_matching
- 06-12 通過ペアに対し、案件の required_skills / optional_skills を

## 入力ファイルと参照方法
- `06-80_duplicate_proposal_check/01_result/duplicate_proposal_check.jsonl`
- `03-50_extract_project_required_skills/01_result/extract_project_required_skills.jsonl`
- `04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl`

## 出力ファイルと構造
- `07-1_requirement_skill_ai_matching/01_result/requirement_skill_ai_matching.jsonl`
- `07-1_requirement_skill_ai_matching/01_result/99_error_requirement_skill_ai_matching.jsonl`
- `07-1_requirement_skill_ai_matching/01_result/run_metadata.json`
- 主な辞書/レコードキー: `project_info`, `resource_info`, `error_type`, `error_message`

## 処理ロジックの詳細
- `_truncate_skillsheet`: 改行単位で切り詰める。精度を落とす粗い切り捨ては避ける。
- `_validate_skills`: スキルリストの出力スキーマを検証。エラー文字列を返す（問題なしはNone）。
- `process_pair`: 1ペアを処理。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 致命的な異常時は `sys.exit(1)` で停止します。
- 個別異常は警告ログに記録します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

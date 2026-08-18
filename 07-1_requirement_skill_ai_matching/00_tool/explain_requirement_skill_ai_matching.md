# explain_requirement_skill_ai_matching

## 目的
- 07-1_requirement_skill_ai_matching
- 06-80 の Cache MISS ペアに対し、03-50 が抽出した案件の required_skills / optional_skills を
  04-2 normalized の要員スキルシート本文を根拠にLLMで適合判定し、
  skill単位の `match` / `note` を出力する。

## 実装（正本）
- 本番active実装は `07-1_requirement_skill_ai_matching/00_tool/normalized/requirement_skill_ai_matching.py` のみ。
- 04-1 raw skillsheet を入力にしていた旧実装（`00_tool/requirement_skill_ai_matching.py`）は削除済み。

## 入力ファイルと参照方法
- `06-80_duplicate_proposal_check/01_result/duplicate_proposal_check.jsonl`
- `03-50_extract_project_required_skills/01_result/extract_project_required_skills.jsonl`
- `04-2_normalize_skillsheets_text/01_result/normalize_skillsheets_text.jsonl`

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
- LLM使用: 有（`common.llm_client.call_llm` / モデル `gpt-4o-mini`）
- 使用箇所: `process_pair` 内。案件要件（required_skills / optional_skills）と要員スキルシートの
  適合判定にLLMを使用する。
- 判定結果はJSON形式で受け取り、`_validate_skills` でschema validationする。
  parse失敗は `llm_parse_error`、parse成功後のschema不正は `invalid_output_schema` として分離する。

## エラー時の挙動
- 致命的な異常時は `sys.exit(1)` で停止します。
- 個別異常は警告ログに記録します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

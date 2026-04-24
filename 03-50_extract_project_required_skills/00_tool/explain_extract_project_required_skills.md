# explain_extract_project_required_skills

## 目的
- Step 03-50: 案件メールから必須スキル・尚可スキルをルールベースで抽出
- （LLMはフォールバック限定）

## 入力ファイルと参照方法
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- `02-2_classify_output_file_project_resource/01_result/projects.jsonl`

## 出力ファイルと構造
- `extract_project_required_skills.jsonl`
- `99_skill_null_extract_project_required_skills.jsonl`
- `99_rule_empty_extract_project_required_skills.jsonl`
- `01_result/extract_project_required_skills.jsonl`
- `01_result/99_skill_null_extract_project_required_skills.jsonl`
- 主な辞書/レコードキー: `skill`, `match`, `note`

## 処理ロジックの詳細
- `_n`: NFKC正規化（全角→半角統一）
- `_normalize_hdr`: 見出し判定専用の正規化。
- `_get_bracket_key`: 「【キー】値」形式からキー部分を抽出し、内部スペースを除去して返す。
- `_should_skip`: 空行・区切り線・URLのみ行はスキップ。
- `_is_stop_section`: この行がセクション終端かどうかを判定する。
- `_is_section_stop`: STATE_REQUIRED / STATE_OPTIONAL セクション内専用の終端判定。
- `_classify_line`: 行を分類する。
- `_is_skill_line`: スキル候補として採用してよい行かどうかを判定する。
- `_is_hard_stop`: メール末尾の確実な終端行を判定する（署名・結語）。
- `rule_extract_skills`: 状態遷移:

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。
- レコード単位の失敗はスキップ継続する分岐があります。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

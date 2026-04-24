# explain_filter_required_skills_noise

## 目的
- 06-12_filter_required_skills_noise
- 06-11 通過ペアに対して、広く一致しやすい語・短語・文脈依存語を追加で除外する。

## 入力ファイルと参照方法
- `06-11_match_required_skills_list/01_result/matched_pairs_required_skills_list.jsonl`
- `03-51_extract_project_required_skills_list/01_result/extract_project_required_skills_list.jsonl`
- `04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl`
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`

## 出力ファイルと構造
- `06-12_filter_required_skills_noise/01_result/matched_pairs_required_skills_noise_filtered.jsonl`
- `06-12_filter_required_skills_noise/01_result/99_not_matched_pairs_required_skills_noise_filtered.jsonl`

## 処理ロジックの詳細
- トップレベル定義を他モジュールから利用する前提です。

## LLM使用有無と使用箇所
- LLM使用: 無
- docstring上でLLM使用禁止が明記されています。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。
- レコード単位の失敗はスキップ継続する分岐があります。

## 注意事項
- 補助モジュールのため、単体では外部入出力を持たない場合があります。
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

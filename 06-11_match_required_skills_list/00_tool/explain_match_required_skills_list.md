# explain_match_required_skills_list

## 目的
- 06-11_match_required_skills_list
- 03-51 で抽出した required_skill_keywords を主軸とし、

## 入力ファイルと参照方法
- `06-10_match_location/01_result/matched_pairs_location.jsonl`
- `03-51_extract_project_required_skills_list/01_result/extract_project_required_skills_list.jsonl`
- `04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl`
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`

## 出力ファイルと構造
- `06-11_match_required_skills_list/01_result/matched_pairs_required_skills_list.jsonl`
- `06-11_match_required_skills_list/01_result/99_not_matched_pairs_required_skills_list.jsonl`

## 処理ロジックの詳細
- `build_required_skill_keyword_set`: required_skill_keywords を重複排除・順序保持で返す。
- `build_required_phase_keywords`: required_phase_keywords から補強対象の工程語だけを返す。
- `search_keywords_in_text`: textにkeywordsのうち含まれるものを返す（1件でもヒットすればよい）。
- `judge_match`: マッチ判定。

## LLM使用有無と使用箇所
- LLM使用: 無
- docstring上でLLM使用禁止が明記されています。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。
- レコード単位の失敗はスキップ継続する分岐があります。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

# explain_duplicate_proposal_check

## 目的
- 06-80_duplicate_proposal_check
- 06-12 通過ペアを前回比較キーと照合し、新規/重複に仕分けする。

## 入力ファイルと参照方法
- `06-12_filter_required_skills_noise/01_result/matched_pairs_required_skills_noise_filtered.jsonl`
- `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`
- `06-80_duplicate_proposal_check/01_result/bk_duplicate_proposal_check_diff_file.jsonl`

## 出力ファイルと構造
- `06-80_duplicate_proposal_check/01_result/duplicate_proposal_check.jsonl`
- `06-80_duplicate_proposal_check/01_result/99_duplicate_duplicate_proposal_check.jsonl`
- `06-80_duplicate_proposal_check/01_result/duplicate_proposal_check_diff_file.jsonl`
- `06-80_duplicate_proposal_check/01_result/bk_duplicate_proposal_check_diff_file.jsonl`
- 主な辞書/レコードキー: `project_info`, `resource_info`

## 処理ロジックの詳細
- トップレベル定義を他モジュールから利用する前提です。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

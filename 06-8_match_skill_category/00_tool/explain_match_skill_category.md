# explain_match_skill_category

## 目的
- 06-8_match_skill_category
- 案件と要員のスキルカテゴリを比較してマッチ判定する。

## 入力ファイルと参照方法
- `06-7_match_vendor_tiers/01_result/matched_pairs_vendor_tiers.jsonl`
- `03-8_extract_project_skill_category/01_result/extract_project_skill_category.jsonl`
- `05-8_extract_resource_skill_category/01_result/extract_resource_skill_category.jsonl`

## 出力ファイルと構造
- `06-8_match_skill_category/01_result/matched_pairs_skill_category.jsonl`
- `06-8_match_skill_category/01_result/99_no_matched_pairs_skill_category.jsonl`

## 処理ロジックの詳細
- `judge_skill_category_match`: スキルカテゴリマッチ判定。

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

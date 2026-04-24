# explain_extract_resource_skill_category

## 目的
- Step 05-8: 要員メールからスキル・カテゴリをルールベースで抽出

## 入力ファイルと参照方法
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- `02-2_classify_output_file_project_resource/01_result/resources.jsonl`

## 出力ファイルと構造
- `extract_resource_skill_category.jsonl`
- `99_skill_category_null_extract_resource_skill_category.jsonl`
- 主な辞書/レコードキー: `message_id`, `skills`, `skills_by_category`, `skills_raw`

## 処理ロジックの詳細
- `canonicalize_skill_name`: 辞書表記ゆれを canonical 名に寄せる。
- `load_skill_dictionary`: YAML形式のスキル辞書を読み込む。
- `extract_skills`: 本文からスキルを抽出してカテゴリ分類する。
- `build_passthrough_record`: Feature flag OFF時のpass-throughレコード。
- `build_extracted_record`: Feature flag ON時の抽出レコード。
- `_is_valid`: skills/skills_by_category が list/dict 型であること。

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

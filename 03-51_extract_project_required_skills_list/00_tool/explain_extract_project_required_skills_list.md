# explain_extract_project_required_skills_list

## 目的
- Step 03-51: 案件の必須/尚可スキル文から辞書ベースでスキル語・工程語を抽出する
- LLM使用禁止。辞書ベース・文字列処理のみ。

## 入力ファイルと参照方法
- `03-50_extract_project_required_skills/01_result/extract_project_required_skills.jsonl`
- `03-8_extract_project_skill_category/00_tool/skill_dictionary.txt`
- `03-9_extract_project_phase_category/00_tool/phase_dictionary.txt`
- `01_result/extract_project_required_skills_list.jsonl`
- `01_result/99_required_skills_list_null.jsonl`

## 出力ファイルと構造
- `extract_project_required_skills_list.jsonl`
- `99_required_skills_list_null.jsonl`
- 主な辞書/レコードキー: `message_id`, `required_skill_keywords`, `required_phase_keywords`, `optional_skill_keywords`, `optional_phase_keywords`

## 処理ロジックの詳細
- `load_skill_list`: YAML形式のスキル辞書から全スキル名を読み込む。
- `load_phase_map`: YAML形式の工程辞書を読み込む。
- `extract_skill_keywords`: テキストからスキル語を抽出する（重複排除・初出順）。
- `extract_phase_keywords`: テキストから工程語を抽出する（phase_nameを返す、重複排除・初出順）。
- `extract_from_skill_list`: skill_items（required_skills or optional_skills）のリストを処理し、
- `is_null_record`: 4つ全て空配列ならTrue（nullレコード扱い）

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

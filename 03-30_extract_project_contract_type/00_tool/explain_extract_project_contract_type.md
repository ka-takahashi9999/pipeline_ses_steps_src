# explain_extract_project_contract_type

## 目的
- Step 03-30: 案件メール本文から契約形態をルールベースで抽出

## 入力ファイルと参照方法
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- `02-2_classify_output_file_project_resource/01_result/projects.jsonl`

## 出力ファイルと構造
- `contract_type.jsonl`
- 主な辞書/レコードキー: `message_id`, `contract_type`, `contract_type_source`, `contract_type_raw`

## 処理ロジックの詳細
- `message_id` をキーに projects と本文メールを突合する。
- `派遣または準委任` のように準委任で契約可能な文面は `quasi_mandate` を優先する。
- `派遣契約`, `契約形態:派遣のみ`, `契約:派遣` などは `dispatch` とする。
- `直接ご契約いただきます` のような商流説明だけでは `dispatch` にしない。
- `労働者派遣事業許可番号` など免許情報は契約判定から除外する。
- 根拠なしの場合は `outsourcing` + `contract_type_source=default` + `contract_type_raw=null` を出力する。

## LLM使用有無と使用箇所
- LLM使用: 無

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- `contract_type_raw=null` はユーザー指定によるもので、`contract_type_source=default` とセットで根拠なしの既定値採用を示します。

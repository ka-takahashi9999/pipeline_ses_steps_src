# explain_classify_type_project_resource

## 目的
- Step 02-1: メール種別分類スクリプト（案件 / 要員 / あいまい / 不明）

## 入力ファイルと参照方法
- `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- `01-3_remove_individual_email/01_result/remove_individual_emails_raw.jsonl`

## 出力ファイルと構造
- `classify_types_project_resource.jsonl`
- `99_no_classify_types_project_resource.jsonl`

## 処理ロジックの詳細
- `_normalize`: Unicode NFKC 正規化 + 小文字化。
- `load_keywords`: classify_keywords.txt を読み込んでキーワード辞書を返す。
- `_remove_cjk_inner_spaces`: CJK文字間の半角スペースを除去する（NFKC正規化後に適用）。
- `score_text`: subject + body をスコアリングして
- `rule_classify`: ルールベースで分類する。
- `_classify_by_subject_only`: 本文キーワードがゼロの場合（HTMLメール等）に subject のみで分類するフォールバック。
- `llm_classify`: LLM で分類する。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。
- 個別異常は警告ログに記録します。
- レコード単位の失敗はスキップ継続する分岐があります。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

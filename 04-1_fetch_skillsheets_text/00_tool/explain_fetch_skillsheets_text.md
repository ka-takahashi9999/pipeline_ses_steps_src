# explain_fetch_skillsheets_text

## 目的
- 04-1_fetch_skillsheets_text: 要員メールからスキルシートテキストを取得

## 入力ファイルと参照方法
- `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- `02-2_classify_output_file_project_resource/01_result/resources.jsonl`
- `- 01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl  (添付ファイル参照)`
- `- 01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl  (本文URL抽出)`
- `- 02-2_classify_output_file_project_resource/01_result/resources.jsonl  (処理対象)`

## 出力ファイルと構造
- `04-1_fetch_skillsheets_text/01_result`
- `04-1_fetch_skillsheets_text/01_result/99_no_fetch_skillsheets_text.jsonl`
- `04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl`
- `- 04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl`
- `- 04-1_fetch_skillsheets_text/01_result/99_no_fetch_skillsheets_text.jsonl  (success=falseのみ)`
- 主な辞書/レコードキー: `message_id`, `skillsheet`, `source`, `success`, `urls`

## 処理ロジックの詳細
- `_base64url_encode`: URL-safe base64 をパディングなしで返す。
- `extract_text_from_pdf`: PDFバイナリからテキストを抽出する。
- `extract_text_from_excel`: Excel (.xlsx / .xls) バイナリからテキストを抽出する。
- `extract_text_from_word`: Word (.docx) バイナリからテキストを抽出する。
- `_list_ole_streams`: OLE2 コンテナのストリーム名一覧を返す。
- `detect_ole_office_type`: OLE2 Office バイナリを Word / Excel に判定する。
- `_run_legacy_word_command`: 旧Word抽出コマンドを一時ファイル経由で実行する。
- `extract_text_from_legacy_word`: 旧Word (.doc) バイナリからテキストを抽出する。
- `extract_text_from_bytes`: ファイル形式を自動判定してテキストを抽出する。
- `extract_from_attachment`: Gmail添付ファイル辞書からテキストを抽出する。
- `extract_urls_from_text`: テキスト中の https:// を含む URL をすべて抽出する。
- `classify_url`: URL を source 種別に分類する。
- `host_matches`: ホストがドメインと一致またはサブドメインかを判定する。
- `get_url_extension`: URLパスからファイル拡張子を抽出する。
- `is_google_docs_url`: Google Drive / Docs のURLかを判定する。

## LLM使用有無と使用箇所
- LLM使用: 無
- docstring上でLLM使用禁止が明記されています。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。
- 個別異常は警告ログに記録します。
- レコード単位の失敗はスキップ継続する分岐があります。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

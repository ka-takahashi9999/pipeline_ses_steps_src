# explain_fetch_gmail

## 目的
- Step 01-1: Gmail取得スクリプト

## 入力ファイルと参照方法
- 外部入力ファイルはこのモジュール単体では明示されていません。

## 出力ファイルと構造
- `fetch_gmail_mail_master.jsonl`
- `01_result/fetch_gmail_mail_master.jsonl`
- 主な辞書/レコードキー: `message_id`, `thread_id`, `subject`, `from`, `to`, `cc`, `reply_to`, `date`, `body_text`, `attachments`

## 処理ロジックの詳細
- `initialize_gmail_service`: SSMからGmail認証情報を取得し、Gmail APIクライアントを初期化する。
- `b64url_decode`: Base64URLデコード（パディング補完付き）。
- `html_to_text`: HTMLを最低限のプレーンテキストに変換する。
- `extract_headers`: メールヘッダーから必要フィールドを抽出する。null禁止のためデフォルト値を設定。
- `walk_parts`: MIMEパートを再帰的に走査してテキスト・添付情報を収集する。
- `extract_body_and_attachments`: メールペイロードから本文テキストと添付ファイル情報を抽出する。
- `download_attachment_data`: 添付ファイルのBase64データをダウンロードして返す。失敗時は空文字。
- `build_query`: コマンド引数からGmail検索クエリを構築する。
- `fetch_all_messages`: 指定クエリでメール一覧を取得し、全件のメッセージIDリストを返す。
- `build_record`: メッセージIDから完全なメールレコードを構築する。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。
- 個別異常は警告ログに記録します。

## 注意事項
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。

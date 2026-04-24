# explain_match_score_sort

## 目的
- 08-4_match_score_sort
- 各パーティションファイルをtotal_skills_match_rateの降順でソートする。

## 入力ファイルと参照方法
- `08-3_match_score_partition/01_result`

## 出力ファイルと構造
- `08-4_match_score_sort/01_result`

## 処理ロジックの詳細
- `init_output_files`: 出力7ファイルをno_matchステータスで初期化する。
- `is_no_match_file`: レコードリストがno_matchステータスのみかどうかを判定する。
- `sort_key`: ソートキー: total降順, required降順（negateで降順化）。

## LLM使用有無と使用箇所
- LLM使用: 無
- コード上にLLMクライアント呼び出しは見当たりません。

## エラー時の挙動
- 入力ファイル欠落時は warning を出して当該ファイルをスキップします。
- `no_match` ファイルはスキップ継続します。
- 例外時は `write_error_log` でエラーログを残します。
- 致命的な異常時は `sys.exit(1)` で停止します。

## 注意事項
- 実装根拠はこのPythonファイル本体を優先してください。

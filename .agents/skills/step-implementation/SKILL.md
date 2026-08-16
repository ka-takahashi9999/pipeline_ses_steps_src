---
name: step-implementation
description: >
  対象stepの調査 → 最小実装 → focused/selective test → confirm → 結果確認 を行う。
  「実装して」「修正して」「追加して」「このstepを直して」と言われたときに使用する。
  変更範囲は対象stepと直近関連ファイルに限定する。
---

対象stepに閉じた実装ワークフロー。
不変ルールは `AGENTS.md` が正本。ここでは手順のみ定義する。

## 手順

### 1. 調査
- 対象stepのディレクトリ構成（`00_tool/` `01_result/` `02_confirm/` `99_execution_time/`）
- 入力JSONLのパスとキー構造 / 出力JSONLの期待スキーマ
- 前stepとの `message_id` 接続
- 既存confirmの有無
- `common/` の利用状況（json_utils / file_utils / logger / llm_client）
- `explain_.md` と `PIPELINE_OVERVIEW.md` は明示指示がない限り読まない

### 2. 最小実装
- 変更は対象stepと直近関連ファイルに限定する
- 既存JSONLスキーマを維持する（キーを変えず値のみ更新）
- 不明値は default 値 + `*_source` で表す
- 解析不能データは除外せず、判定不能と分かる形で残す
- 例外を握りつぶさない。停止 / スキップ / 記録して継続 を明示する
- 目的に不要なリファクタ・リネームはしない
- `common/` を使う（独自ユーティリティを新設しない）

### 3. focused / selective test
- `python3 -m py_compile` で構文チェック
- 対象stepのみ実行する。full pipeline 実行はしない
- 大量入力が必要な場合は件数を絞って実行する
- pytest がある場合は対象範囲だけ選択実行する

### 4. confirm
- 対象stepのconfirmスクリプトを実行する
- confirmが無い場合は作成する（件数整合チェックを必ず含める）

### 5. 結果確認
- 入力件数 / 出力件数 / エラー件数の整合
- 必須キー欠落・型崩れ・1行1JSON崩れの有無
- default値と `*_source` の整合
- 代表3件のみ目視確認する

## 報告フォーマット
1. 変更ファイル（1〜3行）
2. 実行結果（成功件数 / エラー件数）
3. confirm結果（OK / NG + 理由）
4. 代表ケース（最大3件）
5. 残課題（あれば一言）

## やってはいけないこと
- JSONL全文・ログ全文・コード全文を貼る
- 無関係なstepを触る
- 既存スキーマを無断変更する
- full pipeline を実行する
- いきなり全stepを探索する

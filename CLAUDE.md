# pipeline_ses_steps

SESメールの案件・要員マッチングパイプライン。
Gmail取得 → 分類 → 属性抽出 → マッチング → 出力の順にstepが連なるDAG。

詳細ルール → `.claude/rules/00-core.md` / `.claude/rules/10-response-budget.md`

---

## ディレクトリ構成（概要）

```
00_pipeline/        # pipeline.sh（全step順次実行）
01-1 〜 01-4/       # Gmail取得・前処理
02-1 〜 02-2/       # 分類
03-1 〜 03-51/      # 案件属性抽出（03-50: 必須スキル, LLM使用）
04-1/               # スキルシート取得
05-1 〜 05-10/      # 要員属性抽出
06-0 〜 06-20/      # ペアマッチング（06-0がDAG起点）
07-1/               # スキルAIマッチング（LLM使用）
08-1 〜 08-4/       # スコア集計・ソート
09-1/               # 出力整形・S3保存
99_reference/       # 既存実装参照用（コピー禁止）
```

各stepの共通サブ構成: `00_tool/` `01_result/` `02_confirm/` `99_execution_time/`

---

## 重要制約（抜粋）

- **LLM使用可能**: 02-1補助 / 03-50 / 07-1 / 10_assistance_tool のみ
- **LLM使用stepは手動実行**（nohup推奨）、confirmは完了報告後に自動実行
- **確認必須**: 各stepにconfirmスクリプトを作成し、件数整合チェックを含める
- **feature flag**: 03-8/05-8/06-8/03-9/05-9/06-9 は設定ファイルで有効/無効切替
- **09-1**: 1ペア1ファイル出力 → S3圧縮保存 → 前回分削除
- **explain_.mdの扱い**: `explain_.md` は人間確認用ドキュメント。実装・修正・デバッグ時に自動で読む対象ではなく、**明示的に指示された場合のみ参照すること**

## 環境

- Python 3.9（match文禁止）/ Amazon Linux 2 / ap-northeast-1
- APIキーはAWS SSM Parameter Storeから取得（ハードコード禁止）
  - Gmail: `/gmail/credentials` / OpenAI: `/openai/api_key`
- S3: `s3://technoverse/pipeline_ses_steps/`
- LLMはOpenAI（anthropicライブラリ使用禁止）

---

## Git運用

- `/home/ec2-user/pipeline_ses_steps` は実行・作業用ディレクトリであり、git 管理対象ではない
- ソースコード管理用リポジトリは `/home/ec2-user/pipeline_ses_steps_src`
- git diff / git status / git commit / git push が必要な場合は、原則 `/home/ec2-user/pipeline_ses_steps_src` で実施する
- 実装をソース管理用リポジトリへ反映する場合は、先に `/home/ec2-user/pipeline_ses_steps_src/sync.sh` で同期する
- まとめて同期・commit・push する場合は `~/sync_and_push_pipeline_ses_steps_src.sh` を利用してよい
- `01_result/` `02_confirm/` `99_execution_time/` `*.json` `*.jsonl` などの生成物・成果物は git 管理しない

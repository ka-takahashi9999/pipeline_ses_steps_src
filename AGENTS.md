# AGENTS.md

SESマッチングパイプラインにおける **Coding Agent共通の不変ルールの正本**。
Claude Code / Codex / その他のAgentは、まずこのファイルに従う。

Claude固有事項は `CLAUDE.md`、
Agent設計思想は `docs/agent-design.md`、
使い方は `docs/agent-usage-guide.md` を参照する。

---

## 1. リポジトリの目的

SESメールの案件・要員マッチングパイプライン。
Gmail取得 → 分類 → 属性抽出 → マッチング → 出力 の順にstepが連なるDAG。

基本方針: シンプル優先 / 単一責務 / JSONL整合性重視。

---

## 2. workspace と Git領域の分離

| パス | 役割 | Agentの権限 |
|---|---|---|
| `/home/ec2-user/pipeline_ses_steps` | 実装・実行・テスト領域 | 調査・編集・実行を自走してよい |
| `/home/ec2-user/pipeline_ses_steps_src` | GitHubへpushするクリーンなソース管理領域 | **直接の実装・修正は禁止** |

`_src` で許可されるのは以下のみ:

- 正規同期スクリプトによる選択コピー
- `git status` / `git diff` / `git diff --check`
- 対象限定の `git add`
- `git commit`
- 通常の `git push`

**禁止（人間確認が必要）**: `git push --force` / `git reset --hard` / `git clean` / `git add .` などの一括add・破壊操作。

正規経路: `/home/ec2-user/bin/pipeline_sync_git.sh`（詳細は `.agents/skills/pipeline-sync-git/SKILL.md`）

生成物（`01_result/` `02_confirm/` `99_execution_time/` `*.json` `*.jsonl` `nohup.out`）はgit管理しない。

---

## 3. プロジェクト構造

- step単位で構成されたDAGパイプライン。**1 step 1責務**
- 各stepのスクリプトは `<step>/00_tool/*.py`
- 各stepは `00_tool/` `01_result/` `02_confirm/` `99_execution_time/` の定型構成を前提とする
- step間のデータ受け渡しは **JSONL のみ**
- 各stepは前stepの結果を入力として扱う
- **後続stepから前stepを逆参照する設計は禁止**
- `message_id` をキーとして後続stepへ受け渡す
- `common/` を必ずimportして使う（独自ユーティリティの新設禁止）

---

## 4. JSONLルール

- **1行1JSON** を厳守する
- UTF-8を使用する
- キー名は固定。**既存スキーマを無断変更しない**（キーは変えず値だけ更新）
- スキーマ変更が必要な場合は、変更対象・理由・影響範囲を明示する
- 不明値は安易に null にせず、**既定値 + `*_source` の組み合わせ**で管理する
  （例: `*_source="default"` など根拠が分かる形にする）
- **解析不能データを黙って捨てない**。除外せず、判定不能と分かる形で保持する

---

## 5. step設計ルール

- 各stepは単一責務。分類・抽出・マッチングを1stepに混在させない
- step名と処理内容を一致させる
- あるstepの責務不足を、別step内の場当たり的な補正で吸収しない
- **冪等性**: 同一入力で再実行しても結果が変わらないこと
- **再実行耐性**: 途中失敗後の再実行で破綻しないこと

---

## 6. 実装ルール

- **変更範囲は依頼対象stepとその直近関連ファイルに限定する**
- 関係ないstepには手を入れない
- リファクタ目的のみの変更は禁止
- 既存仕様を変える場合は、変更理由と影響範囲を必ず明示する
- ハードコードは最小限にし、既存の設定・定数定義を優先利用する
- エラー時の挙動は「停止」「スキップ」「記録して継続」を明示する
- 例外を握りつぶさない
- ログ・出力JSONL・確認ファイルのいずれかで追跡可能にする
- 各stepに **confirmスクリプトを必ず作る**（件数整合チェックを含める）

---

## 7. テスト方針

- **focused / selective test を優先する**
- 対象stepのみ実行する。**full pipeline実行は原則しない**
- 大量入力が必要な場合は件数を絞る
- pytestは対象範囲だけ選択実行する
- `python3 -m py_compile` による構文チェックを最低限行う
- 実行確認なしの断定をしない

---

## 8. 出力・品質ルール

- 各stepの成果物は定められたディレクトリに出力する
- 中間ファイルは原則上書き、必要な成果物のみ世代管理する
- 出力ファイル名は既存運用との整合を維持する
- 人手確認が必要なものは `02_confirm/` に寄せる
- 処理時間や件数など再実行確認に有用な情報は残す
- 空データ・欠損データ・異常系入力を考慮する
- 必須キー欠落、型崩れ、空文字混入を防ぐ
- 数値項目は数値として扱う

---

## 9. ドキュメント参照ルール

- `explain_.md` は人間確認用ドキュメント。
  実装・修正・デバッグ・レビュー時に**自動で参照してはならない**。明示指示時のみ参照する。
- `PIPELINE_OVERVIEW.md` は全体概要ドキュメント。
  個別stepの作業時に**不要な自動参照をしない**。全体設計の確認が必要な場合のみ参照する。

---

## 10. LLM利用制限

LLMを使用してよいstepは以下のみ:

```
02-1補助
03-50
07-1
10_assistance_tool
```

- 上記以外のstepでLLMを組み込まない
- LLMはOpenAIを使用する（anthropicライブラリの使用禁止）
- これらのstepは夜間自動Pipelineでも実行される。
  手動で単発実行する場合はコスト・実行時間に留意し、必要なら件数を絞る

---

## 11. 環境

- Python 3.9（`match` 文禁止） / Amazon Linux 2 / ap-northeast-1
- APIキーはAWS SSM Parameter Storeから取得（**ハードコード禁止**）
  - Gmail: `/gmail/credentials` / OpenAI: `/openai/api_key`
- S3: `s3://technoverse/pipeline_ses_steps/`
- feature flag: 03-8 / 05-8 / 06-8 / 03-9 / 05-9 / 06-9 は設定ファイルで有効/無効切替

---

## 12. Skill

Skill実体は `.agents/skills/` に置く。Claude Code側 `.claude/skills/` はsymlink。

| Skill | 用途 |
|---|---|
| `step-implementation` | 調査 → 最小実装 → focused test → confirm → 結果確認 |
| `pipeline-review` | 通常レビュー / `strict` で厳しめレビュー |
| `pipeline-sync-git` | 選択同期 → diff → add → commit → push |

---

## 13. 指示生成・報告ルール

- 指摘や修正提案は、そのまま実装に移せる粒度で書く。抽象表現は禁止
- 「対象ファイル」「問題内容」「修正内容」「理由」を分けて示す
- 1指摘 = 1修正単位を原則とする
- 結論を先に3〜5行で出す
- ログ全文・JSONL全文・コード全文を貼らない。代表3件で報告する

---

## 14. 禁止事項

- 無関係なstepの巻き込み修正
- 根拠のないスキーマ変更 / JSONLを崩す変更
- 解析不能データの黙殺
- 実行確認なしの断定
- 依頼されていない大規模リネームや構成変更
- `_src` での直接実装
- force push / reset --hard / git clean 等の破壊操作
- AWS変更操作・production設定変更・秘密情報取得（人間確認が必要）

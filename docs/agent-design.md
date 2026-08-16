# Agent設計書

SES Pipeline における Coding Agent（Claude Code / Codex）の設計方針。
実務的な使い方は `docs/agent-usage-guide.md`、不変ルールは `AGENTS.md` を参照。

---

## 1. 設計原則

### Autonomy First
安全な通常操作でAgentを止めない。
workspace内の調査・編集、Python実行、focused test、pytest、confirm、
正規sync/git処理、Git read-only、通常commit/push は自走対象とする。
Yes/Noを細かく挟むほど作業品質は下がるため、
**止めるのは復旧困難な操作だけ**に絞る。

### Lean Context
Agentに読ませるものを最小化する。

- 不変ルールの正本は `AGENTS.md` 一本。CLAUDE.md はClaude固有事項のみ
- `explain_.md` は自動参照禁止
- `PIPELINE_OVERVIEW.md` は個別step作業では自動参照しない
- 依頼は対象stepを明示し、全step探索をさせない
- 応答も同様に絞る（`.claude/rules/10-response-budget.md`）

### Workflow as Tool
繰り返す手順だけをSkillとして固定する。
役割（persona）ではなく**作業手順**を単位にする。
「実装担当」「レビュー担当」のような固定ロールAgentは作らない。

### Multi-Agent by Exception
Subagentは大規模探索・独立した並列作業が必要なときだけ使う。
通常のstep実装・レビューは単一セッションで完結させる。

### 正式Agentを作らない
`Primary Coding Agent` のような常設Agentは定義しない。
通常作業はClaude Code / Codexの通常セッションが自然文の依頼で担当する。

---

## 2. workspace / _src 分離

| パス | 役割 | Agent |
|---|---|---|
| `/home/ec2-user/pipeline_ses_steps` | 実装・実行・テスト | 可能な限り自走 |
| `/home/ec2-user/pipeline_ses_steps_src` | GitHub push用のクリーンなソース管理 | **直接実装・修正禁止** |

`_src` で許可されるのは、正規同期による選択コピーと
`git status` / `git diff` / `git diff --check` / 対象限定 `git add` / `git commit` / 通常 `git push` のみ。

理由: 実行成果物・試行錯誤の痕跡がGit履歴へ混入するのを構造的に防ぐため。
「同期しないと反映されない」状態を保つことが、そのままクリーンさの担保になる。

---

## 3. Skill構成

実体は `.agents/skills/`。Claude Code側 `.claude/skills/` は同一実体へのsymlink。
Claude / Codex で内容が分岐しないことを構造で保証する。

```
.agents/skills/
  step-implementation/SKILL.md
  pipeline-review/SKILL.md
  pipeline-sync-git/SKILL.md

.claude/skills/
  step-implementation -> ../../.agents/skills/step-implementation
  pipeline-review     -> ../../.agents/skills/pipeline-review
  pipeline-sync-git   -> ../../.agents/skills/pipeline-sync-git
```

| Skill | 責務 |
|---|---|
| `step-implementation` | 調査 → 最小実装 → focused/selective test → confirm → 結果確認 |
| `pipeline-review` | レビュー。引数 `strict` で厳しめ（設計逸脱・将来事故まで） |
| `pipeline-sync-git` | 選択同期 → diff/check → 対象限定add → commit → 通常push |

旧構成（`implement` / `review` / `adversarial_review` / `arch-review-claude` / `arch-review-codex`）は
本3本へ統合済み。レビュー観点は `pipeline-review` の通常 / strict へ移設した。

---

## 4. Permission / Sandbox方針

### Claude Code

`.claude/settings.local.json` で制御。

- **allow（自走）**: workspace内 Read/Edit/Write、`python3` / `python`、`pytest`、
  `python3 -m py_compile`、`ls` / `cat` / `grep` / `find` / `head` / `tail` / `wc` / `diff`、
  `bash -n`、confirmスクリプト実行、`/home/ec2-user/bin/pipeline_sync_git.sh`、
  Git read-only（`git status` / `git diff` / `git log` / `git show` / `git ls-files`）、
  通常 `git add` / `git commit` / `git push`
- **ask / deny**: `git push --force*`、`git reset --hard`、`git clean`、`rm -rf`、
  `aws` 変更操作、SSMからの秘密情報取得、`sudo`

`defaultMode` は `acceptEdits`。Claude `auto` mode / Claude Sandbox は今回は導入しない（Phase 2）。

### Codex

`.codex/config.toml` は 0.147.0 で実際に有効な項目だけに絞る。

```toml
model = "gpt-5.6-sol"
model_reasoning_effort = "high"
sandbox_mode = "workspace-write"
approval_policy = "on-request"
```

旧構成の `profile` / `profiles.*` / `instructions` は削除。
役割分岐はprofileではなくSkillで行い、プロジェクト指示は `AGENTS.md` に一本化した。

実測済みのsandbox挙動（Codex CLI 0.147.0）:

- workspace（`pipeline_ses_steps`）: 書込可能
- `pipeline_ses_steps_src`: workspace sandboxからの直接書込はblocked
- network: `:workspace` ではblocked

`_src` への直接書込がsandboxレベルでblockされることは、
「_src で直接実装しない」という設計と一致する。同期はsandbox外の正規経路で行う。

---

## 5. 正規sync/git経路

```
/home/ec2-user/bin/pipeline_sync_git.sh [--dry-run] [--no-push] [--prune] [-m MSG] <相対パス>...
```

`--prune` は**指定したディレクトリ配下に限り**、SRC側に存在しないファイルを `_src` から削除する
（ファイル削除・リネームを反映したい場合のみ使う。指定しなければ削除は一切行わない）。

本スクリプトは `~/bin` 配下にあり、Git管理対象外。
バックアップが必要な場合は別途保全すること（Phase 2で管理方法を検討）。

流れ:

```
対象パスの明示
→ 検証（絶対パス / .. / SRC外脱出 / 生成物 を拒否）
→ 同期対象ファイル一覧の提示
→ _src の想定外Git変更チェック（あれば停止・無変更で終了）
→ 選択コピー
→ git status / git diff --cached --stat / git diff --check
→ 対象限定 git add
→ git commit
→ 通常 git push
```

安全設計:

- **全同期をデフォルトにしない**（対象パス必須）
- 生成物除外（`01_result/` `02_confirm/` `99_execution_time/` `__pycache__/`
  `*.jsonl` `*.json` `*.log` `nohup.out` `settings.local.json`）
- `..` / 絶対パス / SRC外を指すsymlinkを拒否
- `_src` に想定外の変更があれば何も変更せず停止
- `git add` は対象限定（`git add .` はしない）
- force push機能を持たない
- `--dry-run` 対応

既存の `pipeline_ses_steps_src/sync.sh` と `~/sync_and_push_pipeline_ses_steps_src.sh` は
全同期 + `git add .` 前提のため、日常運用の既定経路からは外し、
一括同期が必要なときの手動手段として残す。

---

## 6. Phase 2候補 / 未確認事項

| 項目 | 状態 |
|---|---|
| Claude `auto` mode | 今回未導入。自走範囲を運用で確認してから検討 |
| Claude Sandbox | 今回未導入。workspace限定実行の追加防御として検討 |
| Codex `approvals_reviewer = "auto_review"` | **未確認 / 今回は採用しない**。`codex --help` では確認できず、公式仕様が確認できた場合のみ採用 |
| Hooks（PostToolUse 等） | 未導入。編集後の自動 `py_compile` / JSONL検証などが候補 |
| `.codex/rules` 相当の分割 | 未導入。AGENTS.md が肥大化した場合に検討 |

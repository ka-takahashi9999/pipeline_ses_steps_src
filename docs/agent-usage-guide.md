# Agent利用ガイド

SES Pipeline で Claude Code / Codex をどう使うかの実務ガイド。
不変ルールは `AGENTS.md`、設計思想は `docs/agent-design.md` を参照。

---

## 1. 普通の作業は自然文でそのまま依頼する

専用Agentを呼び分ける必要はない。通常のClaude Code / Codexセッションへ、
やりたいことをそのまま書く。

```
07-1を調査し、必要なら修正してfocused testまで実施してください。
```

```
06-11の出力件数が合わないので原因を調べて直してください。
```

```
03-50のconfirmを追加してください。件数整合チェックを含めてください。
```

ポイント:

- **対象stepを明示する**（変更範囲が限定される）
- どこまでやってほしいか書く（調査だけ / 修正まで / confirmまで）
- full pipeline実行は依頼しない。focused / selective test が原則

---

## 2. Skill（3本）

同じ手順を毎回きちんと踏ませたいときはSkillを使う。実体は `.agents/skills/`。

| Skill | 内容 |
|---|---|
| `step-implementation` | 調査 → 最小実装 → focused/selective test → confirm → 結果確認 |
| `pipeline-review` | レビュー（`strict` で厳しめ） |
| `pipeline-sync-git` | 選択同期 → diff → add → commit → push |

### Codex

```
$step-implementation 06-11の突合ロジックを修正
$pipeline-review 03-50
$pipeline-sync-git AGENTS.md CLAUDE.md .agents/skills
```

### Claude Code

```
/step-implementation 06-11の突合ロジックを修正
/pipeline-review 03-50
/pipeline-review strict 03-50
/pipeline-sync-git AGENTS.md CLAUDE.md .agents/skills
```

### pipeline-review の strict

引数なしは通常レビュー（達成条件・出力整合・実装妥当性）。
`strict` を付けると、step責務逸脱 / JSONL・schema / message_id /
default・`*_source` / 副作用 / 冪等性 / 再実行耐性 / 将来事故 まで確認する。

---

## 3. 障害調査

専用の障害調査Agentは用意していない。read-onlyであることを自然文で明示する。

```
今回の障害をread-onlyで原因調査してください。
変更はしないでください。
```

必要なら範囲も添える。

```
昨夜のPipeline失敗について、08-4以降のログと出力だけをread-onlyで確認して原因を報告してください。
```

---

## 4. 同期 / commit / push

作業は `/home/ec2-user/pipeline_ses_steps` で行い、
`/home/ec2-user/pipeline_ses_steps_src` へは**正規経路でのみ**反映する。

```bash
# 対象を確認（dry-run）
/home/ec2-user/bin/pipeline_sync_git.sh --dry-run AGENTS.md .agents/skills

# 同期 + stage + diff確認 + commit + push（通常はこの1回で完結）
/home/ec2-user/bin/pipeline_sync_git.sh -m "Update skills" AGENTS.md .agents/skills

# commit で止めたい例外時のみ
/home/ec2-user/bin/pipeline_sync_git.sh --no-push -m "Update skills" AGENTS.md .agents/skills

# ファイル削除も反映する（指定ディレクトリ配下限定）
/home/ec2-user/bin/pipeline_sync_git.sh --prune -m "Reorganize skills" .agents/skills

# 正本と実行用コピーの一致確認
/home/ec2-user/bin/pipeline_sync_git.sh --self-check
```

標準フローは **dry-run → 通常実行の2段階**。通常実行1回で push まで完結する。

スクリプトの配置:

| 役割 | パス | 備考 |
|---|---|---|
| 正本（Git管理対象） | `pipeline_ses_steps/tools/pipeline_sync_git.sh` | 編集用。**実行経路にはしない** |
| 実行用コピー | `/home/ec2-user/bin/pipeline_sync_git.sh` | 正規実行経路 |

スクリプトを直したいときは**正本を編集** → `cp -p` で実行用へ反映 → `--self-check` で確認 →
正規経路で `tools` を `_src` へ同期、の順で行う。

正本と実行用コピーが不一致のまま実行すると、**同期もcommitもpushも開始せず異常終了**する。

- 対象パスの明示が必須（全同期はしない）
- 生成物（`01_result/` `02_confirm/` `99_execution_time/` `*.jsonl` 等）は自動除外
- 同期対象外の変更が `_src` にあれば停止する
- force push機能は持たない

依頼するときは自然文でよい:

```
今回変更したAgent関連ファイルだけ_srcへ同期してcommit/pushしてください。
```

---

## 5. Subagent

大規模探索・独立した並列作業が必要なときのみ使う。
通常のstep実装・レビューは1セッションで完結させる。

---

## 6. 人間確認が必要なこと

Agentは以下を自走しない。依頼者が判断する。

- `rm` などの削除操作（`mkdir` / `touch` / `cp` / `mv` のような可逆操作は自走する）
- force push / `reset --hard` / `git clean` などの破壊操作
- AWS変更操作、production設定変更
- 秘密情報の取得
- 復旧困難な操作
- `_src` でのコード直接編集（そもそも禁止）

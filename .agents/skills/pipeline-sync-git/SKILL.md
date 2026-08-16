---
name: pipeline-sync-git
description: >
  pipeline_ses_steps の変更を pipeline_ses_steps_src へ選択同期し、
  diff確認 → add → commit → 通常push まで行う。
  「同期して」「pushして」「commitして」「_srcに反映して」と言われたときに使用。
---

作業ディレクトリ `/home/ec2-user/pipeline_ses_steps` の変更を、
Git管理リポジトリ `/home/ec2-user/pipeline_ses_steps_src` へ反映する定型手順。

**`_src` 側でコードを直接実装・修正してはならない。**
`_src` への書き込みは正規同期スクリプト経由のみ。

## 正規経路

```bash
/home/ec2-user/bin/pipeline_sync_git.sh [--dry-run] [--no-push] [--prune] [-m "message"] <対象パス>...
```

スクリプトの配置:

| 役割 | パス | sandbox外allow |
|---|---|---|
| 正本（Git管理対象） | `pipeline_ses_steps/tools/pipeline_sync_git.sh` | **しない**（workspace内でAIが編集可能なため） |
| 実行用コピー | `/home/ec2-user/bin/pipeline_sync_git.sh` | する（Codex rulesでallow / 正規実行経路） |

- **実行は必ず実行用コピー** `/home/ec2-user/bin/pipeline_sync_git.sh` を使う
- スクリプトを修正するときは**正本を編集**し、`cp -p` で実行用コピーへ反映する
- 正本と実行用コピーが不一致なら**実行禁止**（スクリプトが何もせず異常終了する / fail-closed）
- 一致確認: `/home/ec2-user/bin/pipeline_sync_git.sh --self-check`
- 対象パスは `pipeline_ses_steps` からの相対パスで**明示列挙**する。全同期はデフォルトにしない
- ディレクトリ指定は可。ただし内部で具体的ファイルへ展開され、以降はすべてファイル単位で処理される
- `..` / 絶対パス / SRC外・DST外へ抜けるsymlink経路は拒否される
- 生成物（`01_result/` `02_confirm/` `99_execution_time/` `*.jsonl` `nohup.out` 等）は除外される
- 同期対象ファイル以外の変更・untrackedが `_src` に1件でもあれば停止する
- `--prune` は指定した**ディレクトリ配下限定**で、SRCに無いファイルを `_src` から削除する

## 手順（2段階）

1. **変更ファイルを明示する**
   今回変更したファイルを列挙する。推測で広げない。

2. **dry-run で内容確認**
   ```bash
   /home/ec2-user/bin/pipeline_sync_git.sh --dry-run <対象パス>...
   ```
   同期対象 / 削除対象 / 除外を確認する。ここでは一切変更しない。

3. **通常実行（同期 → stage → diff → commit → push まで1回で完結）**
   ```bash
   /home/ec2-user/bin/pipeline_sync_git.sh -m "..." <対象パス>...
   ```
   スクリプト内で以下を実施する。
   ```
   対象ファイルだけ同期
   → 対象ファイルだけ git add（ディレクトリ / . / -A は使わない）
   → git status / git diff --cached --stat / git diff --cached / git diff --check
   → commit
   → 通常 push
   ```

`--no-push` は commit で止めたい例外時のみ使う（標準フローでは使わない）。

## 禁止操作
- `git push --force` / `--force-with-lease`
- `git reset --hard`
- `git clean`
- `git add .` / `git add -A` / ディレクトリ指定の `git add`（対象ファイル限定のみ）
- 正本 `tools/pipeline_sync_git.sh` の直接実行によるGit反映
- `_src` 側でのコード直接編集

これらが必要な場合は実行せず、人間に確認する。

## 報告
1. 同期したファイル一覧
2. `git diff --check` 結果
3. commit hash
4. push結果

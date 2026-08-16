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
/home/ec2-user/bin/pipeline_sync_git.sh [--dry-run] [--no-push] [-m "message"] <対象パス>...
```

- 対象パスは `pipeline_ses_steps` からの相対パスで**明示列挙**する
- 全同期はデフォルトにしない
- `..` / 絶対パス / シンボリックリンク越えは拒否される
- 生成物（`01_result/` `02_confirm/` `99_execution_time/` `*.jsonl` `nohup.out` 等）は除外される

## 手順

1. **変更ファイルを明示する**
   今回変更したファイルを列挙する。推測で広げない。

2. **dry-run**
   ```bash
   /home/ec2-user/bin/pipeline_sync_git.sh --dry-run <対象パス>...
   ```
   同期対象と除外対象を確認する。

3. **選択同期 + diff確認**
   ```bash
   /home/ec2-user/bin/pipeline_sync_git.sh --no-push -m "..." <対象パス>...
   ```
   スクリプト内で `git status` / `git diff` / `git diff --check` / 対象限定 `git add` / `commit` を行う。
   同期対象外の想定外変更が `_src` にあれば停止する。

4. **push**
   問題がなければ `--no-push` を外して実行する（通常pushのみ）。

## 禁止操作
- `git push --force` / `--force-with-lease`
- `git reset --hard`
- `git clean`
- `git add .` / `git add -A`（対象限定のみ）
- `_src` 側でのコード直接編集

これらが必要な場合は実行せず、人間に確認する。

## 報告
1. 同期したファイル一覧
2. `git diff --check` 結果
3. commit hash
4. push結果

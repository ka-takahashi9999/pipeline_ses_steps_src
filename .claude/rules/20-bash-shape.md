# Bash Shape

Bash実行形の制御ルール。permission prompt削減と監査性向上のため、
Bashは「1 tool call = 1 simple command」を原則とする。

## 原則1: 調査は専用tool優先

- file内容確認・検索・列挙は `Read` / `Grep` / `Glob` を優先する
- 単純な調査のために `grep` / `find` / `cat` をBashで組み合わせない

## 原則2: 1 Bash call = 1 simple command

原則禁止:

- `cmd1 ; cmd2`
- `cmd1 && cmd2`
- `cmd1 | cmd2`
- 複数行Bash
- `for` / `while`
- `if` block
- heredoc（`<<EOF`）
- `$(...)` / backtick
- `bash -c` / `bash -lc` / `sh -c`

## 原則3: cdを挟まない

- `cd /path && git status` を避ける
- `git -C /path status` を使う、または絶対pathで直接実行する

## 原則4: shell redirectでファイル編集しない

- `echo ... > file` / `cat <<EOF > file` / `sed -i ...` を避ける
- ファイル編集は `Edit` / `Write` を使う
- `sed *` `awk *` `echo *` `printf *` は意図的にallow対象外。
  permission回避目的でこれらに退避しない

## 原則5: Git mutationは正規wrapper

- workspace → `_src` → commit/push は `/home/ec2-user/bin/pipeline_sync_git.sh` を優先する
- `git add` / `git commit` / `git push` を複合Bashで組まない
- `_src` の確認は `git -C /home/ec2-user/pipeline_ses_steps_src <read-only>` を使う

## 原則6: AWSはwrapper優先

- read-only AWS調査は `/home/ec2-user/bin/pipeline_aws_readonly.sh` を直接使う
- raw `aws` CLIを複合commandへ埋め込まない
- 複数URI・複数ARNは高レベルsubcommandで1回にまとめる

## 原則7: 複雑処理はwrapperへ寄せる

- loop / heredoc / 長いshell集計が必要になったら、まず専用toolで代替できないか判断する
- 繰り返し必要な処理は `pipeline_*.sh` wrapperへ高レベルsubcommandを追加する設計を優先する
- 一過性の長いshellをその場で組まない

## 原則8: permission promptの扱い

- UI permission promptの総数はsession transcriptから正確に取得できない。
  件数を推測で報告しない（「0回」等と断定しない）
- 一過性commandで「Always allow / 今後聞かない」を前提にしない。
  再利用可能なruleが必要なら `.claude/settings.json` のpermission設計として扱う
- 共通permissionを `.claude/settings.local.json` へ蓄積しない

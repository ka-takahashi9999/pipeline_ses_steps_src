# 00_pipeline/00_tool

SESマッチングPipelineの実行系スクリプトと共通設定の置き場。
「どれが本番で動くのか」を最初に判断するためのディレクトリREADME。

---

## ファイル一覧

| ファイル | 用途 | 本番利用 | 備考 |
|---|---|---|---|
| `run_full_pipeline.sh` | **本番で実際に実行されるactive runner**。各stepを順次実行 | ○（実行対象） | 検証時に一時変更することがある。本番投入時は master と一致させる |
| `run_full_pipeline_master.sh` | **本番runnerの正本（マスター）**。比較・復元の基準 | ○（正本） | 通常のPipeline実行では直接実行しない |
| `run_full_pipeline_managed.sh` | active runnerの実行管理wrapper。ロック / status発行 / ログS3アップロード / 終了コード確定 | ○（実行経路） | 非同期launcherから起動される |
| `launch_full_pipeline_async.sh` | Step Functions / SSM からの非同期起動入口。`systemd-run` でPipeline本体を切り離す | ○（起動入口） | SSM command lifecycleと長時間実行を分離する |
| `pipeline_s3_config.env` | Pipeline共通のS3 / region / Portal設定の正本 | ○（設定正本） | 秘密値は保持しない。値の変更は影響範囲を確認のうえ実施 |
| `README.md` | このディレクトリの用途と運用ルール | －（ドキュメント） | 実ファイル構成に合わせて更新する |

Step Functions の状態機械定義 `stepfunctions_pipeline_orchestration.asl.json` は
Git管理領域（`pipeline_ses_steps_src`）側に置かれており、実行workspaceには配置しない。

---

## 各ファイルの説明

### run_full_pipeline.sh

本番で実際に実行される **active runner**。
`ROOT` 配下の各stepを `run_step` で順番に実行し、
`PIPELINE_CURRENT_STEP_FILE` / `PIPELINE_STATUS_WRITER` 経由で進捗（RUNNING / current_step）を発行する。
ログ出力先は `PIPELINE_LOG`（未指定時は `00_pipeline/01_result/pipeline_script_exec.log`）。

テスト・検証時に一時的に変更する場合があるが、
**本番利用時は `run_full_pipeline_master.sh` との一致を必ず確認する**。

### run_full_pipeline_master.sh

本番用runnerの **正本 / マスター**。
`run_full_pipeline.sh` の本番投入時の比較・復元基準として保持する。
通常の本番Pipelineから直接実行されることはない（実行対象は `run_full_pipeline.sh`）。

### run_full_pipeline_managed.sh

active runner (`run_full_pipeline.sh`) を管理下で実行する **wrapper**。

- `RUN_ID` / `RUN_DATE` の形式検証（不正なら起動前に停止）
- `flock` による二重起動防止（他runが実行中なら status を残して終了）
- 状態ディレクトリ `00_pipeline/01_result/managed/<RUN_DATE>/<RUN_ID>/` にログ・status・current_step を保持
- `99-9_publish_pipeline_status` により RUNNING / SUCCEEDED / FAILED を発行（終了系はリトライ付き）
- 実行ログをS3へアップロードし、Pipelineの終了コードを確定させる

実行対象は `PIPELINE_SCRIPT`（既定 `run_full_pipeline.sh`）。

### launch_full_pipeline_async.sh

Step Functions / SSM から長時間Pipelineを **非同期起動する入口**。
`systemd-run` で `pipeline-ses-<RUN_DATE>-<RUN_ID>.service` を作成して
`run_full_pipeline_managed.sh` を起動し、SSM command lifecycleからPipeline本体を切り離す。
同一unitが既に active な場合は「受理済み」として正常終了する（SSM再送に対する冪等性）。

### pipeline_s3_config.env

Pipeline共通のS3関連設定の正本。両runner系・launcherが `source` して使用する。
保持しているのは以下の設定項目（値は本ファイルを参照。秘密情報は保持しない）。

- `PIPELINE_S3_BUCKET` / `PIPELINE_S3_BASE_PREFIX`
- `PIPELINE_STATUS_PREFIX` / `PIPELINE_LOG_PREFIX`
- `PIPELINE_AWS_REGION`
- `PIPELINE_SYSTEMD_USER`
- `PORTAL_S3_PREFIX` / `PORTAL_S3_VERIFY_WAIT_SEC`

APIキー等の秘密情報はここではなくAWS SSM Parameter Storeで管理する。

---

## 運用ルール

### 本番runner

- active実行対象は `run_full_pipeline.sh` のみ
- 本番投入時は `run_full_pipeline_master.sh` と内容を一致させる（差分がある状態で本番実行しない）
- runnerを変更したら両者の整合を確認する（`diff run_full_pipeline.sh run_full_pipeline_master.sh`）
- 日付付きhistorical runner（`run_full_pipeline_<YYYYMMDD>.sh`）をactive用途に使わない
- 検証目的で一時変更した場合は、本番前に master 基準へ戻す

### 実行経路

```
Step Functions (stepfunctions_pipeline_orchestration.asl.json)
  ↓ SSM RunCommand
launch_full_pipeline_async.sh        # systemd-run で切り離し
  ↓ systemd unit: pipeline-ses-<RUN_DATE>-<RUN_ID>.service
run_full_pipeline_managed.sh         # lock / status発行 / ログS3 / 終了コード確定
  ↓ PIPELINE_SCRIPT
run_full_pipeline.sh                 # active runner
  ↓ run_step
各Pipeline step (01-x … 09-x, 80-7/80-8/80-9, 99-9)
```

手動実行が必要な場合も、status・ロック・ログ管理を伴う `run_full_pipeline_managed.sh`
経由を基本とする（`RUN_ID` / `RUN_DATE` が必須）。

### 変更時の注意

- `pipeline_s3_config.env` の値変更はPipeline全体（status / ログ / Portal同期）に影響する
- Git管理は `pipeline_ses_steps_src` 側。workspaceで編集し、正規同期経路で反映する

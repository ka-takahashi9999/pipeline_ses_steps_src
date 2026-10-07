# Gmail → S3 取込切替 引継ぎ記録（2026-10-01）

## 結論

- 2026-10-01 11:26 JST、現行EC2 `i-06c075528d0039c3a` へ指定4ファイルだけを反映した。
- 変更前AMI、停止窓、入力S3権限、package完全性、変更前後hash、限定テストはすべて合格した。
- 手動full Pipeline、AI、メール送信、通常master更新、AWS/IAM/Scheduler/Step Functions変更、`pipeline_ses_steps_src`変更は実施していない。
- 動作確認は今夜の既存定期実行で行う。

## 適用前ゲート

- 対象EC2: `i-06c075528d0039c3a`
- private DNS: `ip-172-31-10-222.ap-northeast-1.compute.internal`（現行ホストと一致）
- EC2状態: `running`、Instance/System/AttachedEBS status はすべて `ok`
- 変更前AMI: `ami-02e5693a70a251e71`
  - Name: `auto-match-llm-classifier-ec2-before-s3-20261001`
  - Description: `Before Gmail to S3 migration`
  - State: `available`
  - CreationDate: `2026-10-01T01:52:55.000Z`
- Pipeline: 関連processなし、`00_pipeline/01_result/run_full_pipeline.lock` 未保持。
- 直近Step Functions実行: 2026-10-01 02:17:34 JSTに `SUCCEEDED` で終了。RUNNING実行なし。
- 入力S3: 現行認証で `s3://technoverse-ses-mail/raw/` のListと原本1件のGetに成功。
  - 原本は `/tmp/gmail_s3_migration_20261001.gNrP0b/input_s3_get_check_original.bin` へ別保存。
  - 本文・認証情報は表示していない。保存原本SHA-256: `6c76d6b9d6ffe50a374be15a0024781ea2a8cca042d8f81d45e7c9b63af18196`

## 移行package

- Archive: `s3://technoverse-test/migration-handoff/20261001/gmail_s3_migration_handoff_20261001.tar.gz`
- Sidecar: `s3://technoverse-test/migration-handoff/20261001/gmail_s3_migration_handoff_20261001.tar.gz.sha256`
- SHA-256: `767336cd3f8ccb3be711540a39b967d99729cc2d8a0b9dcb30b6544563842078`（期待値・sidecar・実測が一致）
- 展開先: `/tmp/gmail_s3_migration_20261001.gNrP0b/extracted/gmail_s3_migration_handoff_20261001`
- 内部 `SHA256SUMS`: 11/11 OK。
- `README.md`、`APPLY_ROLLBACK.md`、`VERIFICATION.txt`、`CHANGES.patch` を確認済み。
- 既存3ファイルは `BEFORE_SHA256SUMS` と3/3一致、新規 `fetch_s3_mail.py` は適用前に不存在。

## 変更した4ファイル

| ファイル | 変更後SHA-256 | owner:group | mode | 確認 |
|---|---|---|---|---|
| `01-1_fetch_gmail/00_tool/fetch_s3_mail.py` | `cc4e400dc191b0bebc12ab212539bac17073c3eb7d8ef35906c00b4081c60b3d` | `ec2-user:ec2-user` | `644` | package一致 |
| `01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py` | `e875c954cfb539dc6350c5fbc131af2bb75a677f97682a82e6ac9f7a500a32f2` | `ec2-user:ec2-user` | `644` | package一致 |
| `00_pipeline/00_tool/run_full_pipeline.sh` | `fc8d116c53e17b38e248099b62c81bdf99b9b9e0b350e0ce6f5888070dc7aa65` | `ec2-user:ec2-user` | `755` | package一致 |
| `00_pipeline/00_tool/run_full_pipeline_master.sh` | `fc8d116c53e17b38e248099b62c81bdf99b9b9e0b350e0ce6f5888070dc7aa65` | `ec2-user:ec2-user` | `755` | package一致 |

## 個別バックアップ

| 絶対パス | SHA-256 | owner:group | mode |
|---|---|---|---|
| `/home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/02_confirm/confirm_fetch_gmail_old_20261001.py` | `ca2ce6266b31e91c59b8dd90cc935f969690afe1ade544c3121ef686a893f5e4` | `ec2-user:ec2-user` | `644` |
| `/home/ec2-user/pipeline_ses_steps/00_pipeline/00_tool/run_full_pipeline_old_20261001.sh` | `d444dc9c76c49ba4925deba67e0fcfb68fb349526bd9d1fe1691b63d70c74d0e` | `ec2-user:ec2-user` | `755` |
| `/home/ec2-user/pipeline_ses_steps/00_pipeline/00_tool/run_full_pipeline_master_old_20261001.sh` | `d444dc9c76c49ba4925deba67e0fcfb68fb349526bd9d1fe1691b63d70c74d0e` | `ec2-user:ec2-user` | `755` |

3本ともコピー前後のbyte比較とSHA-256一致を確認した。backupは実行対象へ追加していない。

## 限定確認結果

- Python 3.9.23: `fetch_s3_mail.py` / `confirm_fetch_gmail.py` の `py_compile` OK。
- Shell: 2 launcherの `bash -n` OK。
- 依存: boto3 1.39.4 / botocore 1.39.4、および既存Gmail parser補助のimport OK。
- 合成MIME 1件: JST当日00:00以上・取得開始時刻未満の選択、既存JSONL schema、parse、confirm成功を確認。
- 不完全manifest: confirmが非0終了することを確認。
- 2 launcherはbyte一致。同じ `RUN_DATE_ISO` から `FETCH_AFTER` / `FETCH_BEFORE` を生成し、fetch → confirm → 01-2の順であることを確認。
- `run_step` はfetch/confirmの非0を受けてexitするため、失敗時は01-2へ進まない。
- 通常master `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl` は未更新（mtime `2026-09-30 23:21:18 JST`、SHA-256 `57593e7c7404a5e752d89eea7c184d01efffafe5faf84abab9e625e9c73ffd06`）。
- `PIPELINE_S3_BUCKET=technoverse`、既存prefix・80系安全lock関連10ファイルは適用前後でSHA-256不変。
- 対象4ファイルに `technoverse-test` 参照なし。Gmail取得コード・認証情報は未変更。
- 手動full Pipeline、500件比較、AI、メール送信は未実行。

## 切戻しコマンド

最初にRUNNING実行がないことを確認する。

```bash
/home/ec2-user/bin/pipeline_aws_readonly.sh sfn-list-executions --max-items 5 --status-filter RUNNING
```

出力の `executions` が空であることを確認後、以下をそのまま実行する。共有lockを取得できない場合は何も変更せず停止する。

```bash
cd /home/ec2-user/pipeline_ses_steps
exec 9>>00_pipeline/01_result/run_full_pipeline.lock
flock -n 9 || { echo 'Pipeline lock is held; rollback aborted.' >&2; exit 1; }

ROLLBACK_DIR=/home/ec2-user/pipeline_ses_steps/00_pipeline/02_confirm/gmail_s3_migration_rollback_20261001
mkdir -p "$ROLLBACK_DIR"
chmod 700 "$ROLLBACK_DIR"
test ! -e "$ROLLBACK_DIR/fetch_s3_mail.py.rolled_back"

cp --preserve=all 00_pipeline/00_tool/run_full_pipeline_old_20261001.sh 00_pipeline/00_tool/run_full_pipeline.sh.rollback_tmp
mv 00_pipeline/00_tool/run_full_pipeline.sh.rollback_tmp 00_pipeline/00_tool/run_full_pipeline.sh
cp --preserve=all 00_pipeline/00_tool/run_full_pipeline_master_old_20261001.sh 00_pipeline/00_tool/run_full_pipeline_master.sh.rollback_tmp
mv 00_pipeline/00_tool/run_full_pipeline_master.sh.rollback_tmp 00_pipeline/00_tool/run_full_pipeline_master.sh
cp --preserve=all 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail_old_20261001.py 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py.rollback_tmp
mv 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py.rollback_tmp 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py
mv 01-1_fetch_gmail/00_tool/fetch_s3_mail.py "$ROLLBACK_DIR/fetch_s3_mail.py.rolled_back"

printf '%s  %s\n' ca2ce6266b31e91c59b8dd90cc935f969690afe1ade544c3121ef686a893f5e4 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py | sha256sum -c -
printf '%s  %s\n' d444dc9c76c49ba4925deba67e0fcfb68fb349526bd9d1fe1691b63d70c74d0e 00_pipeline/00_tool/run_full_pipeline.sh | sha256sum -c -
printf '%s  %s\n' d444dc9c76c49ba4925deba67e0fcfb68fb349526bd9d1fe1691b63d70c74d0e 00_pipeline/00_tool/run_full_pipeline_master.sh | sha256sum -c -
test ! -e 01-1_fetch_gmail/00_tool/fetch_s3_mail.py
bash -n 00_pipeline/00_tool/run_full_pipeline.sh 00_pipeline/00_tool/run_full_pipeline_master.sh
```

既存業務データと出力S3は切戻し対象外。

## 今夜の定期実行後に明日確認する場所

`RUN_DATE` と `RUN_ID` は実行ログに記録された値を正とする。

1. 実行状態
   - Step Functions: `/home/ec2-user/bin/pipeline_aws_readonly.sh sfn-list-executions --max-items 5`
   - ローカル状態: `/home/ec2-user/pipeline_ses_steps/00_pipeline/01_result/managed/<RUN_DATE>/<RUN_ID>/`
2. Pipelineログ
   - Local: 上記managed directory内のlog、および `/home/ec2-user/pipeline_ses_steps/00_pipeline/01_result/pipeline_script_exec.log`
   - S3: `s3://technoverse/pipeline_ses_steps/pipeline-logs/<RUN_DATE>/<RUN_ID>/pipeline.log`
3. S3取得件数とmanifest
   - `/home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/02_confirm/s3_daily/<YYYY-MM-DD>/pipeline/selection_manifest.jsonl`
   - logの `selection=<N> success=<N> failed=0 pending=0 attachments=<N>`
4. confirm
   - 同じPipeline logの `S3 confirm OK: selected=<N> output=<N> failed=0 pending=0`
5. 01-1通常出力
   - `/home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`
6. 最終出力
   - Local: `/home/ec2-user/pipeline_ses_steps/09-2_extract_high_score_mail_display/01_result/mail_display_extract_<RUN_DATE>.zip`
   - S3正式成果物: `s3://technoverse/pipeline_ses_steps/mail_display_extract_<RUN_DATE>.zip`
   - Portal CURRENT: `s3://technoverse/pipeline_ses_steps/pipeline_ses_steps/`
   - Portal同期summary: `/home/ec2-user/pipeline_ses_steps/80-9_portal_s3_sync/01_result/portal_s3_sync_summary.json`

## 2026-10-01定期実行の失敗原因調査（2026-10-02追記）

### 結論

- 確定原因は、S3選択候補 `3484件` がlauncherの固定上限 `3000件` を超え、`--require-complete` の完全性チェックが意図どおり非0終了したこと。
- Gmail→S3移行との関係は直接的。旧Gmail取得の `--max 3000` をS3経路へ引き継いだ一方、新S3経路では上限超過を黙って切り捨てずfail-closeするため、本番件数で初めて停止した。
- S3 Get、MIME解析、confirm、01-2以降には到達していない。IAM/S3 Get権限、MIME、confirmは今回の根本原因ではない。
- 通常masterと正式成果物は更新されておらず、切戻しは現時点では不要。次回実行前に件数上限を修正する前進修正を優先する。

### 対象実行と最初の失敗

- Step Functions execution: `526abe67-60aa-4897-aada-822d7731b311`
- RUN_ID: `sfn-9ee41b04-0dd5-4217-a285-e20835a482a7`
- RUN_DATE: `20261001`
- Step Functions: 2026-10-01 23:00:36.075 JST開始、23:02:19.022 JSTに `FAILED`。
- Pipeline本体: 23:02:08 JST開始、23:02:09 JSTに `01-1_fetch_gmail` 開始、23:02:11 JSTにexit `1`。
- 最初に失敗したコマンド:

```text
python3 /home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/00_tool/fetch_s3_mail.py --after 2026-10-01 --before 2026-10-02 --max 3000 --selection-limit 3000 --sample-dir /home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/02_confirm/s3_daily/2026-10-01/pipeline --out /home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl --canonical-output --require-complete
```

- ログに出た必要部分は `fetch_s3_mail NG: ValueError`、runner記録は `FAILED 01-1_fetch_gmail (exit=1, elapsed=2s)`。
- manifestが作成済みで全件 `pending` になった後、`fetch_s3_mail.py` の完全性条件により `ValueError("complete Pipeline selection exceeds max or is not certified")` が発生した。top-level handlerが例外型だけを出すため、メッセージ本文とtracebackはログに残っていない。
- 後続停止位置は `01-1_fetch_gmail`。`01-1_confirm_fetch_s3_mail` と `01-2_remove_duplicate_emails` は未開始。

### 件数・取得範囲・manifest

- launcher指定範囲: 2026-10-01 00:00 JST以上、2026-10-02 00:00 JST未満。
- 実選択範囲: 2026-10-01 00:00:00 JST以上、Pipeline開始時刻の同日23:02:09.620858 JST未満。実行当日のため上端は開始時刻へclampされた。
- S3 List候補: `3484件`。
- manifest記録: `3000件`、`selection_limit=3000`、`selection_complete=false`。
- status: `pending=3000`、`failed=0`。raw cacheなし、S3 Get `0件`、MIME parse `0件`、通常masterへの出力 `0件`。
- `failed=0` なのは個別オブジェクト処理前の全体件数チェックで停止したためで、成功を意味しない。

### 実行環境と手動確認との差

- 実行ユーザー: `ec2-user`。`pipeline_s3_config.env` の `PIPELINE_SYSTEMD_USER=ec2-user` をlauncherがsystemd `--uid` に渡し、生成ログ・manifestのownerも `ec2-user:ec2-user`。
- 作業ディレクトリ: `/home/ec2-user/pipeline_ses_steps`。launcherのsystemd `--working-directory` 指定と一致。
- Python: `/usr/bin/python3` → Python 3.9.23。runnerの `python3` も現在 `/usr/bin/python3` を解決。
- AWS認証主体: `arn:aws:sts::166714029268:assumed-role/auto-match-llm-classifier-ec2-Role/i-06c075528d0039c3a`。EC2 instance profile経由で、静的認証情報は使用していない。
- SSM command `77ad05bb-3765-4dbd-8a5a-cd9eef96c003` はResponseCode `0` / `Success`。これは非同期systemd unitの受付成功であり、Pipeline本体の成功ではない。
- 適用前の手動確認は合成MIME `1件` だったため、実データ `3484件` が固定上限を超える条件を通していなかった。実行ユーザー、Python、cwd、AWS主体に原因となる差は確認されなかった。

### 移行差分との照合

- `fetch_s3_mail.py` は移行時の新規ファイルで、現行SHA-256 `cc4e400dc191b0bebc12ab212539bac17073c3eb7d8ef35906c00b4081c60b3d` は移行package記録と一致。
- `confirm_fetch_gmail.py` はS3 manifest検証を追加したが、今回confirm自体が未開始のため失敗原因ではない。
- 2本のlauncherはbyte一致し、現行SHA-256 `fc8d116c53e17b38e248099b62c81bdf99b9e0b350e0ce6f5888070dc7aa65` は移行package記録と一致。
- backupとの差分では、旧 `fetch_gmail.py --max 3000` が新 `fetch_s3_mail.py --max 3000 --selection-limit 3000 --canonical-output --require-complete` へ置換されている。失敗に関係する変更はこの固定上限と完全性チェックの組合せ。
- launcherは同一日付のmanifestが存在すると自動再利用する。今回の未完了manifestを残したまま同じRUN_DATEを再試行すると、上限だけ変更しても旧 `selection_limit=3000` / `selection_complete=false` を読み、再失敗する。

### 根本原因と二次処理の区別

- 根本原因: 入口の選択件数上限超過。
- S3 Listは成功している。S3 Getは未実行のため、この実行だけからGet権限成否は評価できないが、適用前ゲートでは同じ入力bucketの原本1件Getに成功している。権限エラーを示す証跡はない。
- MIME解析とconfirmは未実行で、エラー根拠はない。
- managed wrapperによる失敗ログuploadとFAILED status発行は成功。
- Step FunctionsのEC2停止とSNS失敗通知はいずれも成功。通知・後処理の二次エラーは確認されなかった。

### 出力への影響

- 通常master `fetch_gmail_mail_master.jsonl`: mtime `2026-09-30 23:21:18 JST`、2619行、SHA-256 `57593e7c7404a5e752d89eea7c184d01efffafe5faf84abab9e625e9c73ffd06` のまま。移行前確認値と一致。
- 01-2成果物もmtime `2026-09-30 23:21:19 JST` のままで後続更新なし。
- ローカル `mail_display_extract_20261001.zip` なし。正式S3 `s3://technoverse/pipeline_ses_steps/mail_display_extract_20261001.zip` なし。
- Portal同期summaryは前回成功時の `2026-10-01 02:12:47 JST` のまま。80系未到達のためPortal CURRENTへの同期なし。
- `technoverse` に新規作成されたのは対象runのpipeline log（516 bytes）とstatus（597 bytes）のみ。既存正式成果物への上書き影響はない。

### 最小修正案と切戻し判断

1. 2本のlauncherに単一の `S3_FETCH_LIMIT` を定義し、fetchの `--max` / `--selection-limit` とconfirmの `--selection-limit` へ同じ値を渡す。対象実行を通す最低値は3484、当面の案は5000。設定値は直近の実日次件数を読み取り確認して決める。
2. active/master両launcherを同一内容に保つ。`fetch_s3_mail.py` と `confirm_fetch_gmail.py` の選択・parse・schemaロジック変更は今回不要。
3. 別修正単位として、メール本文を含まない安全なvalidation messageをログへ残し、`ValueError` 型だけで原因が失われないようにする。

切戻しは不要。fail-closeにより不完全な3000件がmasterへ公開されず、既存成果物も保全された。次回定期実行までに前進修正できない場合だけ、運用判断として既存の切戻し手順を選択肢にする。

### 修正後に失敗箇所だけ確認する方法

1. `python3 -m py_compile` と2 launcherの `bash -n`、active/masterのbyte一致を確認する。
2. fake paginatorへ対象時点と同じ3484件を与えるfocused testで、上限5000時に `len(entries)=3484`、`selection_complete=true`、`selection_candidate_count=3484` となり、完全性条件を通ることを確認する。S3 Get、MIME、後続stepは動かさない。
3. launcher引数をstubで捕捉し、fetchの `--max` / `--selection-limit` とconfirmの `--selection-limit` がすべて同じ5000であることを2本とも確認する。
4. 実S3で01-1だけ確認する必要がある場合は、既存 `.../2026-10-01/pipeline/selection_manifest.jsonl` を削除・上書きせず、`.../2026-10-01/limit5000-check/` のような新しい隔離sample-dirを使用する。canonical master、Pipeline、AI、後続stepは実行しない。

本調査ではコード、IAM、AWS設定、通常masterを変更せず、Pipeline／AIの再実行、切戻しも実施していない。

## S3取得上限10,000件への限定修正（2026-10-02追記）

### 結論

- 現行EC2 `i-06c075528d0039c3a` で、2本のlauncherが共通の `S3_FETCH_LIMIT=10000` をfetchの `--max` / `--selection-limit` とconfirmの `--selection-limit`へ渡すよう修正した。
- 10,000件ちょうどのfetch/confirm契約は通過し、10,001件はS3 Get前に停止する限定テストに合格した。通常経路の3,000件指定は除去済み。
- 昨日分を新しい隔離先で取得した結果は候補3,488件、raw取得3,488件、failed 0、pending 0。既存MIME品質判定により5件が`needs_review`となったため、certified outputは公開されずconfirmもNG停止した。
- 昨日の3,484件との差4件は、昨日のclamp終端（23:02:09 JST）以降、日次終端（24:00 JST）未満に追加された4件で説明できる。通常master、旧未完了manifest、本番出力は不変。
- 今夜の`RUN_DATE=20261002`が参照する`2026-10-02/pipeline/`と今回の`2026-10-01/limit10000-check-20261002/`は別パスであり、検証manifestを誤って再利用しない。

### 作業前ゲート

- 2026-10-02、`/home/ec2-user/bin/pipeline_aws_readonly.sh sfn-list-executions --max-items 20 --status-filter RUNNING`は`executions=[]`。
- 対象process（`run_full_pipeline*` / `fetch_s3_mail.py` / `confirm_fetch_gmail.py`）なし。
- `00_pipeline/01_result/run_full_pipeline.lock`は非blocking取得可能。
- local hostnameは`ip-172-31-10-222.ap-northeast-1.compute.internal`。AWS describe結果の対象instance IDは`i-06c075528d0039c3a`、同private DNS、state=`running`。

### 変更とバックアップ

実装変更は以下3ファイルと本引き継ぎだけ。`confirm_fetch_gmail.py`の実装は変更せず、launcherから同じ上限値を渡す。

| ファイル | 変更後SHA-256 | 内容 |
|---|---|---|
| `01-1_fetch_gmail/00_tool/fetch_s3_mail.py` | `2ef444b197ec50b80bb304e9601372f39017cbdc3f63a9c244a38fc3b1a3ce1b` | 完全選択チェックを関数化。停止ログへ安全な候補件数・上限・停止理由・error typeを追加 |
| `00_pipeline/00_tool/run_full_pipeline.sh` | `f3568d6fd3d5aba2f6dd63385d735824e20a09a11757ff2d545f6ac8e2b4cbb6` | `S3_FETCH_LIMIT=10000`をfetch/confirmへ共通渡し |
| `00_pipeline/00_tool/run_full_pipeline_master.sh` | `f3568d6fd3d5aba2f6dd63385d735824e20a09a11757ff2d545f6ac8e2b4cbb6` | active launcherとbyte一致 |

変更前に以下を作成し、元ファイルとのbyte一致を確認した。既存の`*_old_20261001`は上書きせず、SHA-256も前記記録から不変。

| バックアップ | SHA-256 |
|---|---|
| `01-1_fetch_gmail/00_tool/fetch_s3_mail_old_20261002.py` | `cc4e400dc191b0bebc12ab212539bac17073c3eb7d8ef35906c00b4081c60b3d` |
| `01-1_fetch_gmail/02_confirm/confirm_fetch_gmail_old_20261002.py` | `e875c954cfb539dc6350c5fbc131af2bb75a677f97682a82e6ac9f7a500a32f2` |
| `00_pipeline/00_tool/run_full_pipeline_old_20261002.sh` | `fc8d116c53e17b38e248099b62c81bdf99b9b9e0b350e0ce6f5888070dc7aa65` |
| `00_pipeline/00_tool/run_full_pipeline_master_old_20261002.sh` | `fc8d116c53e17b38e248099b62c81bdf99b9e0b350e0ce6f5888070dc7aa65` |
| `00_pipeline/02_confirm/gmail_s3_migration_handoff_20261001_before_limit_fix_20261002.md` | `537fed909707d3f974cdacecadd1e514e6d238a47efa74ddd6354c7220bfe586` |

### 限定テスト

- Python 3.9: `fetch_s3_mail.py` / `confirm_fetch_gmail.py`の`py_compile` OK。
- Shell: 2 launcherの`bash -n` OK、2本はbyte一致。
- fake paginator 10,000件: `selection_complete=true`、fetchの完全選択チェック通過。
- 合成10,000 record/manifest: confirmが`maximum=10000`かつ`require_complete=true`で通過。
- fake paginator 10,001件: 10,000件を一部処理せず、S3 Get前に`SelectionStoppedError`で停止。安全ログは`candidate_count=10001 limit=10000 stop_reason=selection_limit_exceeded`。
- 2 launcherともfetchの`--max` / `--selection-limit`とconfirmの`--selection-limit`が同じ`S3_FETCH_LIMIT`を参照し、active filesに`--max 3000` / `--selection-limit 3000`なし。
- 本文、件名、S3 key、認証情報は停止ログへ出さない。選択前の失敗では候補件数を`unknown`として安全に記録する。

### 昨日分の隔離取得・confirm

- 指定範囲: `after=2026-10-01`、`before=2026-10-02`（昨日のlauncher指定と同じ）。
- 新規隔離先: `01-1_fetch_gmail/02_confirm/s3_daily/2026-10-01/limit10000-check-20261002/`
- manifest: 3,488行、`selection_candidate_count=3488`、`selection_limit=10000`、`selection_complete=true`。
- selection range: 2026-10-01 00:00:00 JST以上、2026-10-02 00:00:00 JST未満。
- raw取得: 3,488件。parsed candidate: 3,488件。statusは`success=3483`、`needs_review=5`、`failed=0`、`pending=0`。
- 5件には既存parserが合計10個の`UndecodableBytesDefect`を記録した。メール内容はログ・本記録へ出していない。
- fetchは全取得後のstrict validationで非0停止。補正後replayログは`candidate_count=3488 limit=10000 stop_reason=validation_failed error_type=ValueError`。
- confirmは上記`parsed_candidate.jsonl`とmanifestへ`--selection-limit 10000 --require-complete`で実行し、`S3 confirm NG: ValueError`、exit 1。fail-closeを維持した。
- certified output `fetch_gmail_mail_master.jsonl`は隔離先にも作成されていない。検証manifestとraw、parsed candidateは証跡として保持する。
- 検証manifest SHA-256: `433e9b50929bc5e4adc9bf710f3071655489e45b59a24d493a270b50bfb2250c`。
- parsed candidate SHA-256: `65bf6452b460b35f1d6d5243a1f95d8b0230b827e91eabca0332e623cdce2cda`。

昨日の失敗manifestの実選択終端は`2026-10-01T14:02:09.620858Z`（23:02:09 JST）、今回の完成日選択終端は`2026-10-01T15:00:00.000000Z`（24:00 JST）。今回manifestを時刻だけで集計すると旧終端未満が3,484件、旧終端以上が4件であり、件数差と一致する。

### 不変確認と実行しなかったもの

- 旧未完了manifest `2026-10-01/pipeline/selection_manifest.jsonl`は3,000行、pending 3,000のまま保持。SHA-256 `081ae5c9b7f5a56e90a1321af4e0ebd28ce453b7d24af4e1d4ed4f4abd7540b8`。
- 通常masterはmtime `2026-09-30 23:21:18 JST`、SHA-256 `57593e7c7404a5e752d89eea7c184d01efffafe5faf84abab9e625e9c73ffd06`で不変。
- `PIPELINE_S3_BUCKET=technoverse`、既存prefix、安全lock、辞書、`common/`、IAM、Step Functions、Scheduler、`pipeline_ses_steps_src`は変更していない。
- S3への書込み、正式成果物更新、01-2以降、AI、full Pipeline、メール送信は実行していない。

### 2026-10-02限定修正の切戻し

切戻すと3,000件上限へ戻り、同規模の日は再び入口停止する。必要時のみ、最初にRUNNING実行がないことを確認してから実施する。

```bash
/home/ec2-user/bin/pipeline_aws_readonly.sh sfn-list-executions --max-items 5 --status-filter RUNNING
```

`executions=[]`を確認後、共有lockを取得できた場合だけ、2026-10-02バックアップを戻す。

```bash
cd /home/ec2-user/pipeline_ses_steps
exec 9>>00_pipeline/01_result/run_full_pipeline.lock
flock -n 9 || { echo 'Pipeline lock is held; rollback aborted.' >&2; exit 1; }

cp --preserve=all 01-1_fetch_gmail/00_tool/fetch_s3_mail_old_20261002.py 01-1_fetch_gmail/00_tool/fetch_s3_mail.py.rollback_tmp
mv 01-1_fetch_gmail/00_tool/fetch_s3_mail.py.rollback_tmp 01-1_fetch_gmail/00_tool/fetch_s3_mail.py
cp --preserve=all 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail_old_20261002.py 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py.rollback_tmp
mv 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py.rollback_tmp 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py
cp --preserve=all 00_pipeline/00_tool/run_full_pipeline_old_20261002.sh 00_pipeline/00_tool/run_full_pipeline.sh.rollback_tmp
mv 00_pipeline/00_tool/run_full_pipeline.sh.rollback_tmp 00_pipeline/00_tool/run_full_pipeline.sh
cp --preserve=all 00_pipeline/00_tool/run_full_pipeline_master_old_20261002.sh 00_pipeline/00_tool/run_full_pipeline_master.sh.rollback_tmp
mv 00_pipeline/00_tool/run_full_pipeline_master.sh.rollback_tmp 00_pipeline/00_tool/run_full_pipeline_master.sh

printf '%s  %s\n' cc4e400dc191b0bebc12ab212539bac17073c3eb7d8ef35906c00b4081c60b3d 01-1_fetch_gmail/00_tool/fetch_s3_mail.py | sha256sum -c -
printf '%s  %s\n' e875c954cfb539dc6350c5fbc131af2bb75a677f97682a82e6ac9f7a500a32f2 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py | sha256sum -c -
printf '%s  %s\n' fc8d116c53e17b38e248099b62c81bdf99b9b9e0b350e0ce6f5888070dc7aa65 00_pipeline/00_tool/run_full_pipeline.sh | sha256sum -c -
printf '%s  %s\n' fc8d116c53e17b38e248099b62c81bdf99b9e0b350e0ce6f5888070dc7aa65 00_pipeline/00_tool/run_full_pipeline_master.sh | sha256sum -c -
python3 -m py_compile 01-1_fetch_gmail/00_tool/fetch_s3_mail.py 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py
bash -n 00_pipeline/00_tool/run_full_pipeline.sh 00_pipeline/00_tool/run_full_pipeline_master.sh
```

Gmail経路まで戻す場合は、この限定修正の切戻しではなく、前節の`*_old_20261001`を使う移行全体の切戻し手順を使用する。

### 今夜の定期実行に向けた残件

- 上限修正に関する残作業はない。今夜の通常runで`S3_FETCH_LIMIT=10000`が3引数へ渡り、候補件数、取得件数、failed/pending、confirm結果を監視する。
- launcherが今夜確認するmanifestは`01-1_fetch_gmail/02_confirm/s3_daily/2026-10-02/pipeline/selection_manifest.jsonl`だけ。追記時点で同ファイルは存在せず、今回の検証manifestは日付もrun-nameも異なるため再利用されない。
- 旧`2026-10-01/pipeline/selection_manifest.jsonl`は証跡保持中。同じ`RUN_DATE=20261001`を通常launcherで再実行すると旧3,000件manifestを再利用して上限不一致で停止するため、削除・上書きしないまま通常再実行しない。
- 昨日分5件の`needs_review`は上限とは別の既存MIME品質問題。今回の限定修正ではparser/validationを緩和していない。昨日分をcertifyする必要がある場合は別修正単位で調査する。

## `UndecodableBytesDefect` 5件の限定調査（2026-10-02追記）

### 結論

- 原因は5件共通で、添付partの`Content-Disposition: filename=`と`Content-Type: name=`に、RFC 2231等のcharset表記を使わずUTF-8 byteを直接置いた原本側のMIME headerである。各headerに1個、1メール合計2個の`UndecodableBytesDefect`が付いた。
- 5件とも本文は宣言`charset=utf-8`、`quoted-printable`をstrict復号でき、件名・本文・添付ファイル名に置換文字はない。添付payloadは`base64`から復号でき、添付ファイル名は生byteのUTF-8 strict復号結果と一致した。内容の欠損・誤変換を示す証拠はないため、5件すべて「警告のみで内容を保持できている」と判定する。
- Gmail対照は原本`Message-ID`の完全一致検索だけを5件に限定して実施し、4件を取得、1件は対象不在だった。取得できた4件は、添付件数・ファイル名・MIME type・size・添付byteが全一致し、本文も完全一致または空白正規化後一致、件名も両端空白除去後一致した。対象不在の1件は別メール・別期間へ探索を広げず打ち切った。
- MIME復号処理そのものは修正不要。一方、回復可能なこの既知パターンまで`issues`へ入れ、全3,488件の公開と通常運用を止める現行品質判定は過剰である。結論は原因単位で「限定修正が必要」。一律解除ではなく、下記条件をすべて満たす添付ファイル名headerだけを`header_warnings`へ移す。
- 本調査ではコード、設定、通常master、保存済みmanifest/raw/parsed candidateを変更していない。3,488件の再取得・全件再解析、AI、後続step、full Pipelineは実行していない。

### 5件の分類

個人情報を避けるため、保存原本SHA-256の先頭12桁だけを識別子とする。

| 原本ID | 問題part | 本文 | 添付 | Gmail対照 | 分類 |
|---|---|---|---|---|---|
| `f391ad153a97` | `0_1` PDF | plain / UTF-8 strict成功 | 1件、byte・ファイル名一致 | 取得。本文は空白正規化後一致、件名は両端空白除去後一致 | 警告のみ、内容保持 |
| `a3d183e59937` | `0_1` PDF | HTML / UTF-8 strict成功 | 1件、byte・ファイル名一致 | 取得。本文完全一致、件名は両端空白除去後一致 | 警告のみ、内容保持 |
| `661c5cdcf165` | `0_1` XLSM | plain / UTF-8 strict成功 | 1件、byte・ファイル名一致 | 取得。本文は空白正規化後一致、件名は両端空白除去後一致 | 警告のみ、内容保持 |
| `ae3b121a7425` | `0_1` PDF | HTML / UTF-8 strict成功 | 1件、原本から完全復号、ファイル名もUTF-8 byteと一致 | 完全一致検索で対象不在。そこで打切り | 警告のみ、内容保持（Gmail実対照なし） |
| `61b37a719899` | `0_1` PDF | HTML / UTF-8 strict成功 | 1件、byte・ファイル名一致 | 取得。本文完全一致、件名は両端空白除去後一致 | 警告のみ、内容保持 |

「実際に欠損・誤変換がある」は0件、「判断できない」は0件。Gmail対象不在の1件も、保存原本について本文strict復号、添付payload完全復号、ファイル名のraw UTF-8 byteとの一致を独立に確認できたため、内容保持の判定は可能とした。ただしGmail API出力との実測比較だけは未実施である。

### 問題が発生したMIME部分と実際の復号

- 5件ともrootは`multipart/mixed`、本文はpart `0_0`、問題headerは添付part `0_1`だけ。root、本文part、Subject headerには`UndecodableBytesDefect`がない。
- 添付partの`Content-Disposition`の`filename`と`Content-Type`の`name`は非extended parameterで、charset/language宣言がない。そこへ6〜18 byteの非ASCII UTF-8を直接置いている。これは原本側の共通する非標準表現である。
- Python 3.9の`BytesParser(policy=policy.default)`はraw headerの非ASCII byteを`surrogateescape`で保持し、parameter解析時に各headerへ`UndecodableBytesDefect`を付ける。その後`email.utils._sanitize`が元byteをUTF-8として復号する。5件は全byte列がUTF-8 strictで有効なため置換なしで復元され、`part.get_filename()`はraw parameterのUTF-8 strict復号結果と全件一致した。
- 本文partは5件とも`charset=utf-8`を明示し、transfer encodingは`quoted-printable`。現行取得器は`get_payload(decode=True)`後に宣言charsetでstrict復号し、全件成功した。`decode_utf8_replace_part_*`は発生していない。
- 添付partは4件がPDF、1件がXLSMでcharsetは適用外。現行取得器は`base64` payloadをbyteへ復号して保持し、`attachment_original_bytes_unverified_*`は発生していない。Gmail対照4件では復号後byteのSHA-256も一致した。
- 件名のGmail差は4件ともS3側に外側のUnicode空白が1文字多いだけで、`strip()`後は一致した。plain本文2件の差も空白正規化後に一致し、HTML本文2件は完全一致した。いずれも今回の添付header defectとは別で、業務に必要な文字内容の欠損ではない。

### 原本側の問題とS3取得器側の差

- 原本側: 添付ファイル名parameterがRFC 2231の`filename*=`/`name*=`等ではなく、charset宣言なしの生UTF-8になっている。このため標準libraryが形式上のdefectを正しく記録する。
- S3取得器の復号: 生byteが有効なUTF-8である5件について、ファイル名・本文・添付payloadを欠損なく復元している。復号ロジックの変更は不要。
- S3取得器の品質判定: `fetch_s3_mail.py`のheader defect収集が、回復結果を検証せず全`UndecodableBytesDefect`を`issues`へ追加する。その結果、内容保持済みでも`status=needs_review`となり、strict validationとconfirmが全体停止する。Gmail経路はこの原本形式違反を利用者へ見せず、同じファイル名と添付byteを渡していた。

### 最小変更と5件を使う合格条件

対象は`01-1_fetch_gmail/00_tool/fetch_s3_mail.py`のheader defect分類（現行198〜208行付近）だけ。launcher、上限、`confirm_fetch_gmail.py`、schema、通常master、他stepは変更しない。

最小変更は、次の条件をすべて満たす`UndecodableBytesDefect`だけを`issues`ではなく`header_warnings`へ記録すること。

1. defectの所属先が添付partの`Content-Disposition`または`Content-Type`である。
2. 対象がそれぞれ`filename`または`name` parameterで、raw parameterに非ASCII byteがある。
3. raw parameter byteがUTF-8 strictで復号でき、その結果が当該headerの解析済みparameter値および最終`get_filename()`と一致する。
4. 解析済みファイル名に置換文字・surrogateがなく、添付payloadを原本byteとして取得できている。
5. 同じpart/headerに別種defectがなく、メール全体にも別の`issues`がない。

`UndecodableBytesDefect`の一律無視、メール除外、invalid UTF-8へのreplace許容は行わない。条件外の同defect、本文・件名の同defect、置換文字、添付byte未検証は従来どおり`needs_review`にする。

保存済み5原本だけを使うfocused testの合格条件:

- 5/5で本文・件名・添付ファイル名の置換文字が0、本文とfilenameの現在値および添付byteのSHA-256が変更前candidateから不変。
- 5/5で`issues=[]`、`status=success`となり、各件の`header_warnings`に対象header由来の回復警告が2件残る。
- Gmail取得済み4件は今回と同じく、添付件数・filename・MIME type・size・byteが一致し、本文・件名が完全一致または空白正規化後一致する。対象不在の1件はGmail再探索を合格条件にしない。
- negative caseとして、UTF-8 strict不成立、解析値不一致、置換文字あり、添付payload未検証、対象外headerのいずれかは`needs_review`のままになる。
- 5件用の一時manifest/outputでS3 confirm相当の件数・schema・raw hash整合を通す。3,488件の再取得・全件再解析やfull Pipelineは不要。

### 通常運用停止の妥当性と今夜への影響

- 未知のMIME破損や実データ欠損をfail-closeする設計自体は妥当。ただし、上記5条件でUTF-8 byteと復元結果の一致を証明できたこの原因に限り、3,483件を含む日次全体を止めるのは妥当ではない。
- 現行コードのまま今夜同形式のメールが1件でも入ると、全件取得後に`validation_failed`となり、通常masterを更新せず01-2以降を停止する。同形式が入らなければ今回5件は`2026-10-01/limit10000-check-20261002/`に隔離されているため、今夜の`2026-10-02/pipeline/`へ直接影響しない。
- 今夜の停止リスクを除くには上記限定修正が必要。昨日分3,488件を通常経路で再実行する必要はなく、修正確認は保存済み5原本だけで行う。

### 調査範囲と不変確認

- 保存済みmanifestの`needs_review`行は2016、2222、2232、2316、2317。原本hashと保存rawは5/5一致した。
- 保存済み旧Gmail masterには件名・本文・添付byte hashが一致する対照なし。Gmail APIは上記5個の原本`Message-ID`完全一致検索だけを行い、別メール・別期間へ広げていない。
- 調査終了時も通常master SHA-256は`57593e7c7404a5e752d89eea7c184d01efffafe5faf84abab9e625e9c73ffd06`。`fetch_s3_mail.py`、`confirm_fetch_gmail.py`、2本のlauncherも本節追記前の記録値から不変。
- 今回はこの1回で調査を区切る。追加のMIME一般対応や共通処理改善は対象外とする。

## 2026-10-02定期実行の失敗原因調査（2026-10-03追記）

### 結論

- 確定原因は、添付ファイル名headerの回復可能な既知の`UndecodableBytesDefect`を現行`fetch_s3_mail.py`が`issues`へ入れ、7件を`needs_review`としたため、全件取得後のstrict validationが非0終了したこと。
- 前日の3,000件上限超過とは別原因だが、前節の「`UndecodableBytesDefect` 5件」と同じ原因。10,000件上限は実際に適用され、候補3,700件を全件取得できた。
- 前節で提案した回復可能な添付ファイル名headerの限定分類修正は未適用。現行コードのSHA-256は上限修正直後の`2ef444b197ec50b80bb304e9601372f39017cbdc3f63a9c244a38fc3b1a3ce1b`のまま。
- 通常master、01-2以降、正式ZIP、Portal CURRENTは更新されていない。本調査ではコード、AWS設定、masterを変更せず、再取得・Pipeline再実行・Gmail再比較を行っていない。

### 対象実行・SSM・最初の失敗

- Step Functions execution: `526abfb8-e0aa-4897-aada-822d7731b311`
- RUN_ID: `sfn-bbea765f-63d5-43eb-9fd7-6eb4a8613a95`
- RUN_DATE: `20261002`
- Step Functions: 2026-10-02 23:00:36.084 JST開始、23:12:20.002 JSTに`FAILED`。
- SSM command: `3a65ada7-d9e9-4b21-8e8b-ff592f260329`。23:02:08.060 JST開始、23:02:11.060 JST終了、`ResponseCode=0` / `Success`。これはmanaged systemd unitの受付成功であり、Pipeline本体の成功ではない。
- Pipeline本体: 23:02:08 JST開始、23:02:09 JSTに`01-1_fetch_gmail`開始、23:07:29 JSTにexit `1`。
- launcherとRUN_DATEから解決される最初の失敗コマンド:

```text
python3 /home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/00_tool/fetch_s3_mail.py --after 2026-10-02 --before 2026-10-03 --max 10000 --selection-limit 10000 --sample-dir /home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/02_confirm/s3_daily/2026-10-02/pipeline --out /home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl --canonical-output --require-complete
```

- S3/ローカルのPipeline logは`FetchStoppedError candidate_count=3700 limit=10000 stop_reason=validation_failed error_type=ValueError`と`FAILED 01-1_fetch_gmail (exit=1, elapsed=320s)`で一致する。
- `fetch_s3_mail.py`はheader defectを収集する現行198〜208行で、限定済みの`InvalidHeaderDefect`以外をすべて`issues`へ追加する。今回の`UndecodableBytesDefect`も該当し、現行320〜321行の`status == success` / `issues=[]`条件によりvalidationが失敗した。
- `01-1_confirm_fetch_s3_mail`と`01-2_remove_duplicate_emails`は開始していない。

### 件数と既知原因との照合

- manifest: 3,700行、`selection_candidate_count=3700`、`selection_limit=10000`、`selection_complete=true`。
- raw取得3,700件、parsed candidate 3,700件。statusは`success=3693`、`needs_review=7`、`failed=0`、`pending=0`。添付総数は1,753件。
- 7件の`issues`は`UndecodableBytesDefect`だけで計14個。安全な位置情報だけを確認すると、7件すべて同じ添付partで`Content-Disposition`と`Content-Type`に各1個あり、filenameは取得済み、置換文字・surrogateなし、添付payloadもbyte取得済みだった。
- この位置・件数構造と未変更の判定コードは前節の既知5件と同じ。前回の調査結果を再利用し、Gmail比較、別期間探索、3,700件の再解析は行っていない。
- certified outputは作成されず、confirmも未実行。したがって取得自体は完了したが、通常masterへ公開された件数は0件。

### 通常master・本番出力・Portalへの影響

- 通常master `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`はmtime `2026-09-30 23:21:18 JST`、SHA-256 `57593e7c7404a5e752d89eea7c184d01efffafe5faf84abab9e625e9c73ffd06`で不変。
- 01-2成果物もmtime `2026-09-30 23:21:19 JST`のまま。対象runのlogに01-2以降のSTARTはない。
- ローカル`mail_display_extract_20261002.zip`なし、正式S3 `s3://technoverse/pipeline_ses_steps/mail_display_extract_20261002.zip`なし。
- 対象runが`technoverse`へ作成した運用証跡はpipeline log（610 bytes）とstatus（597 bytes）。既存の正式成果物への上書きはない。
- Portal同期summaryは2026-09-30 runの成功記録（executed_at `2026-10-01 02:11:05 JST`）のまま。80系未到達のためPortal CURRENTへの同期・rotationはない。

### 必要な最小修正

- 前節の提案どおり、`01-1_fetch_gmail/00_tool/fetch_s3_mail.py`のheader defect分類だけを修正する。
- 添付partの`Content-Disposition: filename` / `Content-Type: name`について、raw parameterがUTF-8 strictで復号でき、解析値・最終filenameと一致し、置換文字・surrogateがなく、添付payloadを原本byteとして取得でき、他のdefect/issuesがない場合に限り、`UndecodableBytesDefect`を`issues`ではなく`header_warnings`へ移す。
- `UndecodableBytesDefect`の一律無視、invalid UTF-8のreplace許容、launcher・10,000件上限・confirm・schema・他stepの変更は不要。

### 修正後の限定確認方法

1. 前節で保存済みの5原本を使うfocused positive testで、本文・filename・添付byteを不変に保ち、`issues=[]`、`status=success`、回復警告が`header_warnings`へ残ることを確認する。
2. UTF-8 strict不成立、解析値不一致、置換文字またはsurrogateあり、添付payload未検証、対象外header、別種defectのnegative caseは従来どおり`needs_review`となることを確認する。
3. 一時manifest/outputだけで件数・schema・raw hash整合とconfirm相当を確認する。通常master、今回の3,700件manifest/raw、S3、Gmail、AI、後続step、full Pipelineは動かさない。

原因は既知の添付ファイル名header判定に確定したため、ここで調査を終了する。

## 2026-10-03 テスト版完走・限定修正の本番反映

### 結論

- 現行取得器を `fetch_s3_mail_test_20261003.py` へbyte一致でコピーしてから、添付ファイル名の回復可能な生UTF-8警告だけを限定分類する修正を実装した。
- 2026-10-02分を新規隔離先へS3から再取得し、3,704件すべてを解析した。本文復元不能、添付復元不能、その他失敗は0件で、既存confirmも3,704/3,704件で合格した。
- 保存原本と異常系のfocused test、実データ全件、confirmが合格したため、日時付きバックアップ後に同じ差分を本番 `fetch_s3_mail.py` へ反映した。confirmの変更は不要だった。
- 通常master、既存Pipeline manifest、launcher、01-2以降、AWS設定、正式S3成果物、`pipeline_ses_steps_src`は変更していない。本日の許可範囲は完了した。

### 調査・変更範囲

- 対象: `01-1_fetch_gmail/00_tool/fetch_s3_mail.py` のMIME defect分類と個別parse失敗の継続処理だけ。
- 回復可能扱いは、添付partの `Content-Disposition: filename` / `Content-Type: name` にある単独の `UndecodableBytesDefect` について、raw parameterがUTF-8 strict復号可能、解析parameter・最終filenameと一致、置換文字・surrogateなし、添付payload bytes取得済み、メールに別issueなし、をすべて満たす場合だけ。警告は `header_warnings` に残す。
- 条件外の同defect、不正UTF-8、本文の置換復号、添付原本bytes未検証、別issue併存は `issues` / `needs_review` のまま。個別parse例外はmessage_idのあるmanifest行へ問題部分と例外型を記録して残りを継続する。
- S3 List/Get、接続、権限、source変更、raw byte数、manifest/output書込み等の全体成立に関わる障害は従来どおり停止する。
- MIME一般改修、schema変更、共通処理変更は行っていない。

難易度は中程度。警告を一律解除せず、添付bytes保持の証明条件と実復元不能のfail-closeを両立する必要があった。

### テスト版と隔離出力

- テスト版: `01-1_fetch_gmail/00_tool/fetch_s3_mail_test_20261003.py`
- 隔離先: `01-1_fetch_gmail/02_confirm/s3_daily/2026-10-02/test-20261003/`
  - `selection_manifest.jsonl`
  - `raw/`
  - `parsed_candidate.jsonl`
  - `fetch_gmail_mail_master.jsonl`
- 通常masterおよび通常実行の `2026-10-02/pipeline/selection_manifest.jsonl` は入出力先に使用していない。

### 取得範囲と件数差

- launcher指定と同じ日付範囲: 2026-10-02 00:00 JST以上、2026-10-03 00:00 JST未満。
- 失敗実行manifestの実終端: `2026-10-02T14:02:09.516619Z`（23:02:09 JST）。実行当日だったため開始時刻へclampされ、候補3,700件だった。
- 今回の完了日manifest終端: `2026-10-02T15:00:00.000000Z`（24:00 JST）。候補・raw取得・解析・出力はいずれも3,704件。
- 差分4件はすべて旧manifest終端以上、新manifest終端未満。旧3,700件の欠落は0件。したがって差は昨夜のclamp終端後に同日内へ追加された4件で説明できる。
- 上限10,000件以内、`selection_complete=true`、failed 0、pending 0。raw cacheは3,704ファイル、添付は合計1,753件。

### 分類・重複・内容保持

分類は1メール1区分で、重複分類0件。

| 分類 | 件数 | 判定 |
|---|---:|---|
| 正常（警告なし） | 2,123 | success |
| 警告のみ | 1,581 | success、`header_warnings`へ記録 |
| 本文復元不能 | 0 | 該当なし |
| 添付復元不能 | 0 | 該当なし |
| その他失敗 | 0 | 該当なし |

- 警告のみ1,581件の内訳は、既存の回復可能header構文警告1,574件と、今回対象の生UTF-8添付ファイル名警告7件。後者は7メール・14 header。
- manifest 3,704件、出力JSONL 3,704件、message_idはいずれも3,704件一意。manifestと出力のmessage_id差分0件。
- confirmが全rawのsize・SHA-256、出力schema、本文またはattachment-only理由、添付base64復号後sizeを検証した。
- 生UTF-8警告7件は旧失敗実行にも存在し、再取得raw SHA-256が不変。件名・本文・添付ファイル名・MIME type・size・添付bytesも変更前candidateから不変で、置換文字・surrogateは0件。
- manifest SHA-256: `f934468887607238f2adf42fb9ad01cfc55b8fabdb1bafdf0acda0c12eeca23b`
- test output SHA-256: `e73cf7d98e0dca63adb083df901c6d90eeb5647f0a2acba8f95e49dff102b56c`

### focused test・confirm

- Python 3.9 `py_compile`: テスト版、本番取得器、既存confirmすべてOK。
- 保存原本7件: 7/7で `issues=[]`、`status=success`、対象警告は各2件保持。本文・件名・添付値・添付bytesは変更前から不変。
- 合成negative: 不正UTF-8ファイル名、不正UTF-8本文、回復可能警告と別issueの併存、添付原本bytes未検証はすべて `needs_review` のまま。本文・添付の問題partと理由は `issues` に残った。
- テスト版実データ: `selection=3704 success=3704 failed=0 pending=0 attachments=1753`。
- 反映前の既存confirm: `S3 confirm OK: selected=3704 output=3704 failed=0 pending=0`。
- 反映後の本番confirmを同じ隔離出力へ再実行し、同じ結果でOK。全件S3再取得は繰り返していない。
- 本番取得器とテスト版はbyte一致、SHA-256はいずれも `6e6ac3fc38f59ec2ec2f2004a86ae559fabcb2bdb2b29ee6708f52f115083c85`。変更前後diffに空白エラーなし。

### 本番反映・不変確認

本番へ反映したファイルは1件だけ。

| ファイル | 結果 |
|---|---|
| `01-1_fetch_gmail/00_tool/fetch_s3_mail.py` | 上記限定差分を反映 |

- 変更前バックアップ: `01-1_fetch_gmail/00_tool/fetch_s3_mail_old_20261003_100125.py`
  - SHA-256: `2ef444b197ec50b80bb304e9601372f39017cbdc3f63a9c244a38fc3b1a3ce1b`
- `confirm_fetch_gmail.py` は変更不要。SHA-256 `e875c954cfb539dc6350c5fbc131af2bb75a677f97682a82e6ac9f7a500a32f2`、mtimeも不変。
- 反映前にStep Functions RUNNING実行なし、共有lock取得可能を確認した。
- 通常masterは未変更。mtime `2026-09-30 23:21:18 JST`、SHA-256 `57593e7c7404a5e752d89eea7c184d01efffafe5faf84abab9e625e9c73ffd06`。
- 失敗実行の通常manifestはmtime `2026-10-02 23:07:26 JST`のまま。今回のテスト版は別ディレクトリだけを使用した。
- 2 launcherは変更せずbyte一致、SHA-256 `f3568d6fd3d5aba2f6dd63385d735824e20a09a11757ff2d545f6ac8e2b4cbb6`。
- `technoverse`本番出力先・prefix、安全lock、取得上限10,000、辞書、IAM、Step Functions、Scheduler、`pipeline_ses_steps_src`は変更していない。
- 通常master更新、01-2以降、AI、full Pipeline、メール送信、S3書込みは実行していない。

### 残件・切戻し

- 本依頼の合格条件に対する残件はない。次回通常runの運用監視は必要だが、本日の限定作業では追加改善・網羅テストへ広げない。
- 切戻す場合は、最初にRUNNING実行なしと共有lock取得可能を確認し、次の1ファイルだけを戻す。通常master・test出力・AWS設定は切戻し対象外。

```bash
cd /home/ec2-user/pipeline_ses_steps
exec 9>>00_pipeline/01_result/run_full_pipeline.lock
flock -n 9 || { echo 'Pipeline lock is held; rollback aborted.' >&2; exit 1; }

cp --preserve=all 01-1_fetch_gmail/00_tool/fetch_s3_mail_old_20261003_100125.py 01-1_fetch_gmail/00_tool/fetch_s3_mail.py.rollback_tmp
mv 01-1_fetch_gmail/00_tool/fetch_s3_mail.py.rollback_tmp 01-1_fetch_gmail/00_tool/fetch_s3_mail.py
printf '%s  %s\n' 2ef444b197ec50b80bb304e9601372f39017cbdc3f63a9c244a38fc3b1a3ce1b 01-1_fetch_gmail/00_tool/fetch_s3_mail.py | sha256sum -c -
python3 -m py_compile 01-1_fetch_gmail/00_tool/fetch_s3_mail.py 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py
```

## 2026-10-05定期実行の失敗原因調査（2026-10-06追記）

### 結論

- 最初の失敗は `01-1_fetch_gmail` の全件取得・解析後に行うstrict validation。原本 `Message-ID` headerの限定的な形式不正4件が `needs_review` となり、`ValueError` → `FetchStoppedError`、exit `1` で停止した。
- S3候補4,112件は上限10,000件以内で、raw取得4,112件、parsed candidate 4,112件、success 4,108件、needs_review 4件、failed 0、pending 0。接続・IAM・S3 List/Get・件数上限・出力書込みの障害ではない。
- 2026-10-03反映済み取得器のSHA-256は引き継ぎ記録どおりで、添付ファイル名の回復可能警告5件はsuccessで通過した。前回修正の未反映・再発ではなく、未対応だった別header種別の品質判定である。
- 通常master、01-2以降、AI、正式ZIP、Portal CURRENTは更新されていない。失敗後のEC2停止とSNS失敗通知は成功し、実行中のPipeline・AI処理は残っていない。

### 対象実行・SSM受付・Pipeline本体

- Step Functions execution: `526ac3ad-60aa-4897-aada-822d7731b311`
- execution ARN: `arn:aws:states:ap-northeast-1:166714029268:execution:auto-match-llm-classifier-pipeline-orchestration:526ac3ad-60aa-4897-aada-822d7731b311`
- RUN_ID: `sfn-04e21f33-3f06-4bd3-9c0b-8a71aa0a86d7`
- RUN_DATE: `20261005`
- Step Functions: 2026-10-05 23:00:36.087 JST開始、23:12:19.940 JSTに`FAILED`。
- 対象EC2は `i-06c075528d0039c3a`。履歴上、同instanceを起動し、SSM Online確認後にlauncherを送信した。
- SSM command ID: `8cb1a3e9-5a3a-45fe-b669-0959e826a629`
  - 実行開始 `23:02:08.049 JST`、終了 `23:02:11.049 JST`
  - `ResponseCode=0`、`Status=Success`、stderr空
  - managed systemd unitの起動受付成功を示す。非同期Pipeline本体の成功を示すものではない。
- Pipeline本体は23:02:08 JST開始、23:02:09 JSTに `01-1_fetch_gmail` を開始し、23:08:37 JSTにexit `1`（elapsed 388秒）。local log/statusとS3 log/statusは同じRUN_ID・時刻・失敗step・exit codeで一致した。

### 最初の失敗工程と具体的エラー

RUN_DATEと現行launcherから解決されるコマンド:

```text
python3 /home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/00_tool/fetch_s3_mail.py --after 2026-10-05 --before 2026-10-06 --max 10000 --selection-limit 10000 --sample-dir /home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/02_confirm/s3_daily/2026-10-05/pipeline --out /home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl --canonical-output --require-complete
```

- logの具体的エラー: `FetchStoppedError candidate_count=4112 limit=10000 stop_reason=validation_failed error_type=ValueError`
- manifestは `selection_complete=true`、候補件数・selection limitは全行で4,112 / 10,000。選択範囲は2026-10-05 00:00 JST以上、Pipeline開始時刻23:02:09.642291 JST未満。
- manifest / parsed candidate / raw cacheは各4,112件。message_id重複0、manifestとparsed candidateのID差分0、添付総数2,068件。
- status内訳:
  - success（警告なし）2,207件
  - success（警告あり）1,901件
  - needs_review 4件
  - failed 0件
  - pending 0件
- 警告あり1,901件のうち、既存の回復可能header構文警告は1,896件、2026-10-03修正対象の生UTF-8添付ファイル名警告は5件・10 header。後者は停止せずsuccessになっている。
- needs_review 4件のissueはすべて `header_InvalidHeaderDefect_message-id_part_0` のみで、各メール2 defect、合計8 defect。confirmは開始前であり、結果は存在しない。`01-2_remove_duplicate_emails`も未開始。

### `Message-ID` defectの限定確認

- 4件ともrootに `Message-ID` headerが1つだけあり、ASCIIのみ、`<`で始まり`>`で終わるが、header内の `@` が2個ある同一形状だった。
- Python 3.9 email parserは各headerへ `missing trailing '>' on msg-id` と `Unexpected ...` の2つの `InvalidHeaderDefect` を付けた。後者の内容値は個人情報回避のため記録しない。
- 4件とも本文は保持され、件名・本文に置換文字／surrogateなし、添付なし、別issueなし。出力用message_idは原本 `Message-ID` ではなくS3 bucket/keyのSHA-256由来で、4件とも一意かつmanifestとparsed candidateで一致した。
- `fetch_s3_mail.py` 264〜269行付近は既知の回復可能warning以外のheader defectを一律 `issues` へ入れ、386〜387行付近のvalidationが `status=success` / `issues=[]` を要求する。この組合せで4件が日次全体を停止した。
- manifest SHA-256: `ee5684fe25f784edf3b896ab575db69bf3757ae6ffe2aab58b1c0964ab2a9d73`
- parsed candidate SHA-256: `c8b4267c21f7f5caa563cfd9be2e83980aa1111b9ec1c7243faccf2532573f1c`

### 原因分類と前回修正との関係

- 原本側には外部送信元由来の不正な `Message-ID` syntaxがある。ただしS3障害・IAM障害・外部サービス停止ではない。
- Pipeline停止の直接原因は、S3移行取得器が業務出力に使わない原本 `Message-ID` の形式警告まで一律fatalにする品質判定。したがって分類は「外部原本の形式不正を入力条件として顕在化した、S3移行固有のvalidation不足」。既存共通処理や後続stepの問題ではない。
- 現行 `fetch_s3_mail.py` SHA-256は `6e6ac3fc38f59ec2ec2f2004a86ae559fabcb2bdb2b29ee6708f52f115083c85` で、2026-10-03反映記録と一致。launcher 2本も記録どおり `f3568d6fd3d5aba2f6dd63385d735824e20a09a11757ff2d545f6ac8e2b4cbb6` でbyte一致。
- 既知の添付ファイル名警告は実起動経路で通過しており、Gmail比較や同原因調査は繰り返していない。

### データ・成果物・実行残存への影響

- 通常master `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl` はmtime `2026-09-30 23:21:18 JST`、SHA-256 `57593e7c7404a5e752d89eea7c184d01efffafe5faf84abab9e625e9c73ffd06` のまま。certified outputは公開されていない。
- 完了したのは01-1内部のS3選択・raw取得・解析・candidate保存まで。stepとしての01-1、confirm、01-2以降は未完了。
- AI stepは開始していない。調査時点で関連processなし、`pipeline-ses-*` systemd unit 0、Step Functions RUNNING実行0。
- `s3://technoverse/pipeline_ses_steps/mail_display_extract_20261005.zip` は不存在。対象runがtechnoverseへ残したものはpipeline log（610 bytes）とstatus（597 bytes）の運用証跡。
- Portal同期summaryは前回成功run `20260930`、executed_at `2026-10-01 02:11:05 JST` のまま。80系未到達のためPortal CURRENT更新・rotationなし。
- Step Functionsの `StopEC2AfterFailure` とSNS `PublishFailureNotification` はともに成功。通知・終了処理の二次エラーはない。

### 必要な最小修正と限定確認方法

本調査では修正を実施しない。必要な最小修正は `01-1_fetch_gmail/00_tool/fetch_s3_mail.py` のheader defect分類だけ。

1. rootの原本 `Message-ID` は出力identityに使用しないことを前提に、今回と同じ限定条件（headerが1つ、ASCII、置換文字／surrogateなし、defectが観測済み2種だけ、別issueなし）を満たす場合のみ、内容を含まない安全な警告名で `header_warnings` へ移す。
2. `InvalidHeaderDefect` の一律無視、原本Message-IDの出力採用、schema変更、confirm・launcher・後続stepの変更は行わない。条件外のMessage-ID defectや他header defectは従来どおり `needs_review` とする。
3. 保存済み4原本だけをテスト版で再parseし、本文・出力message_idが不変、`issues=[]`、warning保持、置換文字0を確認する。追加の不正Message-ID合成negativeは `needs_review` のままになることを確認する。
4. 保存済み4,112件のmanifest・parsed candidate・raw cacheをread-only入力として、変更対象4件だけをテスト版結果へ差し替えたin-memory validationを行う。件数4,112、ID一意性、raw hash、schema、添付sizeを確認し、S3再取得やfull Pipelineは行わない。

原因は既存ログ・manifest・raw・parsed candidateで確定したため、ここで読み取り調査を終了する。

## 2026-10-06 原本Message-ID形式警告の限定修正

### 結論

- `01-1_fetch_gmail/00_tool/fetch_s3_mail.py` だけを最小修正した。原本 `Message-ID` の値や今回4件のdefect文言を許可リスト化せず、後続で使うデータへの影響に基づいて警告と停止を分離した。
- rootに1個だけある原本 `Message-ID` の構文defectが下記の回復可能条件をすべて満たす場合、内容を含まない固定名 `unused_original_message_id_syntax_part_0` を `header_warnings` へ記録して処理を継続する。原本headerと解析値は書き換えない。
- 保存済み4原本は、変更前の出力recordと完全一致する値を保ったまま `issues=[]` / `status=success` となった。保存済み4,112件をread-only入力にしたin-memory validationと、隔離4件fixtureに対する既存confirmも合格した。
- 2026-10-05分のS3一括再取得、通常master更新、後続step、AI、full Pipeline、S3書込み、AWS設定、launcher、`pipeline_ses_steps_src` は変更・実行していない。昨日分の専用検証は差分レビュー合格後に1回だけ実行する。

### 変更ファイルとバックアップ

実装変更は取得器1ファイルと本引き継ぎだけ。confirmの実装変更は不要だった。

| ファイル | 内容 |
|---|---|
| `01-1_fetch_gmail/00_tool/fetch_s3_mail.py` | 未使用の原本Message-ID構文警告を影響ベースで限定分類 |
| `00_pipeline/02_confirm/gmail_s3_migration_handoff_20261001.md` | 判断基準、テスト、切戻し、次回検証コマンドを記録 |

変更前バックアップ:

- `01-1_fetch_gmail/00_tool/fetch_s3_mail_old_20261006_080752.py`
- SHA-256: `6e6ac3fc38f59ec2ec2f2004a86ae559fabcb2bdb2b29ee6708f52f115083c85`
- 作成直後に変更前ファイルとのbyte一致を `cmp` で確認済み。
- 変更後 `fetch_s3_mail.py` SHA-256: `5fd533273017cd3a112205c2daf63281db4556f322d4d08d7a4dd2d56c704220`

### 警告として継続する条件

次の条件をすべて満たす場合だけ回復可能な形式警告とする。

1. 対象はroot partの原本 `Message-ID` headerであり、raw headerが1個だけ存在する。
2. header defectは `InvalidHeaderDefect`、すなわちheader値の構文警告だけである。defectの文言や今回4件の値には依存しない。
3. Pipeline出力の `message_id` が従来どおりS3 bucket/keyのSHA-256由来であり、原本 `Message-ID` をidentityに使用していない。
4. raw header値がASCIIで、置換文字・surrogate・不正制御文字を含まず、折返し行も継続行の形を保っている。

上記を満たす場合も原本headerは保持し、警告だけを安全な固定名でmanifestへ記録する。

### 従来どおり停止する条件

- root以外、複数の原本 `Message-ID`、非ASCIIまたはlossy markerを含む値、構文警告以外のdefect、S3由来identityとの不一致は `issues` / `needs_review` のまま。
- MIME part defect、本文strict復号失敗、空本文かつ添付なし、添付原本bytes未検証、添付size不一致など、本文・添付の復元に影響する問題は従来どおり停止する。
- S3選択/取得失敗、raw byte数・SHA-256不一致、source変更、manifest/output件数不一致、pending/failed、S3由来message_id重複、schema不整合は従来どおり `validate_records()` または取得処理で停止する。
- 他の実害issueが併存する場合、原本Message-ID警告自体は警告として記録されるが、その実害issueによって日次は停止する。すべてのdefectを無条件に無視する変更ではない。

### 限定テスト結果

- `python3 -m py_compile`: 変更後取得器、既存confirm、限定テストscriptがすべてOK。
- 保存済みpositive 4件: 4/4で変更前recordと完全一致、S3由来message_id不変、本文・件名に置換文字/surrogateなし、`issues=[]`、`status=success`、安全な警告名を各1件保持。
- 合成recoverable 1件: 値や観測済みdefect文言に依存せず、単一root Message-IDの構文警告として通過。
- fatal negative 9条件: S3 identity不一致、原本Message-ID重複、非ASCII不正Message-ID、本文復号不能、添付原本bytes未検証、selection ID重複、manifest/output件数不一致、raw hash不一致、pendingをすべて非合格として検出。
- 保存済み4,112件: 元manifest・parsed candidate・raw cacheを変更せず、対象4件だけをメモリ上で再parse結果へ差し替えて `validate_records()` 合格。件数4,112、S3由来ID一意、manifest/output ID一致、全raw size/SHA-256、schema、添付base64復号後size、failed 0、pending 0を確認。
- 既存confirmを隔離4件fixtureへ実行し、`S3 confirm OK: selected=4 output=4 failed=0 pending=0`。現行confirmが件数、raw証跡、schema、S3由来ID、添付sizeを検証できるため、confirm変更は不要。

### 切戻し

RUNNING Pipelineがないことを確認し、共有lockを取得できた場合だけ次を実行する。切戻し対象は取得器1ファイルだけ。

```bash
/home/ec2-user/bin/pipeline_aws_readonly.sh sfn-list-executions --max-items 5 --status-filter RUNNING

cd /home/ec2-user/pipeline_ses_steps
exec 9>>00_pipeline/01_result/run_full_pipeline.lock
flock -n 9 || { echo 'Pipeline lock is held; rollback aborted.' >&2; exit 1; }

cp --preserve=all 01-1_fetch_gmail/00_tool/fetch_s3_mail_old_20261006_080752.py 01-1_fetch_gmail/00_tool/fetch_s3_mail.py.rollback_tmp
mv 01-1_fetch_gmail/00_tool/fetch_s3_mail.py.rollback_tmp 01-1_fetch_gmail/00_tool/fetch_s3_mail.py
printf '%s  %s\n' 6e6ac3fc38f59ec2ec2f2004a86ae559fabcb2bdb2b29ee6708f52f115083c85 01-1_fetch_gmail/00_tool/fetch_s3_mail.py | sha256sum -c -
python3 -m py_compile 01-1_fetch_gmail/00_tool/fetch_s3_mail.py 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py
```

### 差分レビューとレビュー後の昨日分検証

レビュー対象の実装差分:

```bash
cd /home/ec2-user/pipeline_ses_steps
diff -u 01-1_fetch_gmail/00_tool/fetch_s3_mail_old_20261006_080752.py 01-1_fetch_gmail/00_tool/fetch_s3_mail.py
```

レビュー合格後、2026-10-05 JST分を通常masterへ触れない専用先で1回だけ取得し、続けてその出力だけをconfirmする。次のコマンドは本修正作業では未実行。

```bash
cd /home/ec2-user/pipeline_ses_steps

python3 01-1_fetch_gmail/00_tool/fetch_s3_mail.py \
  --after 2026-10-05 \
  --before 2026-10-06 \
  --max 10000 \
  --selection-limit 10000 \
  --sample-dir 01-1_fetch_gmail/02_confirm/s3_daily/2026-10-05/message-id-fix-validation-20261006 \
  --out 01-1_fetch_gmail/02_confirm/s3_daily/2026-10-05/message-id-fix-validation-20261006/fetch_gmail_mail_master.jsonl \
  --require-complete

python3 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py \
  --input 01-1_fetch_gmail/02_confirm/s3_daily/2026-10-05/message-id-fix-validation-20261006/fetch_gmail_mail_master.jsonl \
  --manifest 01-1_fetch_gmail/02_confirm/s3_daily/2026-10-05/message-id-fix-validation-20261006/selection_manifest.jsonl \
  --after 2026-10-05 \
  --before 2026-10-06 \
  --selection-limit 10000 \
  --require-complete
```

検証時は、旧失敗manifestの4,112件を集合として新manifestに全件含むこと、旧selection終端 `2026-10-05T14:02:09.642291Z` 以上かつ日次終端未満の追加分を「23時台の取得終了後に届いた件数」として分離する。正常、警告のみ、復元不能、failed、pending、S3由来message_id重複を重複なしで集計し、取得完了とconfirm合格をクローズ条件とする。

## 2026-10-06 差分レビュー結果

### 結論

- 修正必須の指摘なし。`fetch_s3_mail_old_20261006_080752.py` との差分は、未使用の原本RFC `Message-ID` を限定判定する関数とheader分類からの呼び出しだけで、本文・添付・出力schema・業務用`message_id`生成は変更していない。
- 警告化はrootの単一`Message-ID`、`InvalidHeaderDefect`のみ、ASCIIかつlossy marker・不正制御文字なし、正しい折返し、S3由来identity一致をすべて要求する。特定メールの値・defect文言への例外ではなく、他header defect、複数原本header、非ASCII、本文・添付・MIMEの実害issueは従来どおり停止するため、defectの無条件無視ではない。
- 保存済み結果（実4件のrecord完全一致、回復可能1件、fatal negative 9条件、4,112件in-memory validation、隔離4件confirm）を採用した。全件取得・confirmと追加テストは実行していない。
- 昨日分の専用取得・confirmへ進めてよい。記載コマンドは`--canonical-output`を付けず、取得出力・manifest・rawを`02_confirm/s3_daily/2026-10-05/message-id-fix-validation-20261006/`配下へ限定し、confirmも同じ専用出力だけを参照するため、通常masterを更新しない。

### ID重複の意味と維持される停止条件

- 報告・`validate_records()`・confirmの「`message_id`重複」は、原本RFC `Message-ID`値のメール間重複ではなく、S3 bucket/keyのSHA-256から生成する業務用`message_id`の重複を指す。今回の4,112件で0件だったのはこちらである。
- fatal negativeの「原本Message-ID重複」は、1通のroot MIMEに原本`Message-ID` headerが複数存在する条件を指す。値が同じメールが複数あるかを表す集計ではない。この条件は警告化されず`needs_review`のまま。
- S3選択/取得失敗、source変更、raw byte数・SHA-256不一致、空・上限超過・不完全selection、manifest/output件数・ID不一致、failed/pending、業務用ID重複、schema・必須型、添付sizeの検出経路は差分外で維持されている。

## 2026-10-06 2026-10-05分の専用取得・confirm完了

### 結論

- 差分レビュー合格後、引き継ぎ記載コマンドをそのまま使用し、2026-10-05 00:00 JST以上、2026-10-06 00:00 JST未満のS3メールを上限10,000件で専用先へ取得した。fetchと既存confirmはいずれも終了コード0。
- 選択・raw・出力はいずれも4,116件。正常2,211件、警告のみ1,905件、`needs_review=0`、`failed=0`、`pending=0`、`issues`あり0件、業務用message_id重複0件。
- 昨夜の失敗run 4,112件は全件含まれ、欠落0件。追加4件はすべて旧取得終端23:02:09 JSTより後の23:06:37〜23:08:31 JSTに到着しており、完成日終端までに増えたメールで説明できる。
- `S3 confirm OK: selected=4116 output=4116 failed=0 pending=0`。全件取得・出力整合とconfirm合格を確認したため、原本Message-ID形式警告の修正・検証は完了とする。

### 専用検証先と取得条件

- 専用先: `01-1_fetch_gmail/02_confirm/s3_daily/2026-10-05/message-id-fix-validation-20261006/`
- 選択範囲: `2026-10-04T15:00:00.000000Z` 以上、`2026-10-05T15:00:00.000000Z` 未満。
- `selection_candidate_count=4116`、`selection_limit=10000`、`selection_complete=true`。
- fetch結果: `selection=4116 success=4116 failed=0 pending=0 attachments=2068`。
- raw cache 4,116件、manifest 4,116行、専用output 4,116行。
- 通常masterを指定する `--canonical-output` は使用していない。

### 分類と警告内訳

分類は1メール1区分で、正常と警告のみの合計が4,116件と一致する。

| 分類 | 件数 | 結果 |
|---|---:|---|
| 正常（header warningなし） | 2,211 | success |
| 警告のみ | 1,905 | success、`header_warnings`へ記録 |
| needs_review | 0 | 該当なし |
| failed | 0 | 該当なし |
| pending | 0 | 該当なし |
| issuesあり | 0 | 該当なし |

警告のみ1,905件の内訳は、既存の回復可能header構文警告1,896件、生UTF-8添付ファイル名警告5件、今回対象の原本Message-ID構文警告4件。分類間の重複はない。今回の4件は内容を含まない固定警告名 `unused_original_message_id_syntax_part_0` を保持し、停止せずsuccessとなった。

### 昨夜4,112件との照合

- 旧manifest 4,112件、新manifest 4,116件。旧ID集合の新manifest欠落は0件、追加は4件。
- 旧selection終端: `2026-10-05T14:02:09.642291Z`（23:02:09.642291 JST）。新selection終端: `2026-10-05T15:00:00.000000Z`（24:00 JST）。
- 追加4件のS3 LastModifiedは23:06:37 JST〜23:08:31 JSTで、全件が旧終端以上かつ新終端未満。昨夜の取得終了後に同日内へ届いた増加分である。
- 共通4,112件のraw SHA-256変更0件、出力record変更0件。昨夜の原本・解析値を警告解消目的で書き換えていない。
- manifestとoutputの業務用message_id差分0件。manifest側重複0件、output側重複0件。

### confirm・成果物hash・通常master不変

- 既存confirm終了コード: 0。
- confirm結果: `S3 confirm OK: selected=4116 output=4116 failed=0 pending=0`。
- 専用manifest SHA-256: `b21dbf332ad6053f1c457bd8c5d71bcafa85cf5727d1529a3f11390771b5325a`。
- 専用output SHA-256: `2b0a1accb0043f5c514d1f1c65dd664fbef0b3da4b7d702cd53d57298a7eea1a`。
- 通常master `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl` はmtime `2026-09-30 23:21:18.392033990 JST`、size `269764671`、SHA-256 `57593e7c7404a5e752d89eea7c184d01efffafe5faf84abab9e625e9c73ffd06` で不変。

### 本番コード適用状態と未実行範囲

- 実行に使用した本番 `fetch_s3_mail.py` SHA-256はレビュー済みの `5fd533273017cd3a112205c2daf63281db4556f322d4d08d7a4dd2d56c704220` と一致。
- 既存 `confirm_fetch_gmail.py` SHA-256は `e875c954cfb539dc6350c5fbc131af2bb75a677f97682a82e6ac9f7a500a32f2` で変更なし。
- 修正済み取得器は本番コードパスへ適用済み。追加修正、判定緩和、launcher変更、通常master更新、後続step、AI、full Pipeline、メール送信、S3書込み、AWS/IAM/Step Functions/Scheduler変更、`pipeline_ses_steps_src`変更は実施していない。
- 本件の残作業はない。追加レビュー・全体テストは不要。

## 2026-10-07 2026-10-06定期実行失敗の原因レビュー

### 結論

- 最新Executionは `526ac4fe-e0aa-4897-aada-822d7731b311`、RUN_IDは `sfn-f1c1127e-3af8-4f03-b923-8b748ff0a729`、RUN_DATEは `20261006`。最初の失敗は `01-1_fetch_gmail` で、`FetchStoppedError candidate_count=3903 limit=10000 stop_reason=validation_failed error_type=ValueError`、exit 1。
- S3選択・取得・raw保存・MIME parseは3,903件すべて完了し、`success=3901`、`needs_review=2`、`failed=0`、`pending=0`。2件の添付ファイル名header品質判定だけで日次全体が停止し、confirm・01-2以降は開始していない。
- 2件とも本文は復元済み、添付payloadも原本byte取得済みで出力sizeと一致する。1件はファイル名もlossy markerなし、もう1件はファイル名だけ置換文字を含む。添付内容・本文・S3由来message_id・manifest整合性の障害ではない。
- 前日検証と取得器・confirm・launcher・Python・実行ユーザーは同じ。旧コードへの巻戻りや別ファイル実行、manifest再利用はない。失敗理由は、10月6日分に前日にはなかった「ASCIIのRFC2047風添付名へ複数defectが付く形式」が2件入り、現行の原因別例外条件を外れたため。
- 根本的な不足は、観測済みdefectを個別にwarning化する一方、残った1件の`needs_review`でも `validate_records()` が日次全体をNGにする設計。今後はdefect名ではなく、利用データへの影響と障害範囲で「警告継続」「対象メールだけ要確認」「全体停止」を分ける必要がある。

### 最新実行の事実

| 項目 | 確定値 |
|---|---|
| Execution | `526ac4fe-e0aa-4897-aada-822d7731b311` |
| RUN_ID | `sfn-f1c1127e-3af8-4f03-b923-8b748ff0a729` |
| RUN_DATE | `20261006` |
| Step Functions | 2026-10-06 23:00:36.089 JST開始、23:12:19.902 JSTにFAILED |
| Pipeline本体 | 23:02:08開始、01-1を23:02:09開始、23:07:47にexit 1 |
| 最初の失敗 | `01-1_fetch_gmail` |
| 候補 / raw / parsed candidate | 3,903 / 3,903 / 3,903 |
| status | success 3,901 / needs_review 2 / failed 0 / pending 0 |
| 添付 | 1,677件 |
| 業務用message_id | manifest・candidateとも重複0、集合差0 |
| confirm・後続 | 未開始 |

launcherと新規run directoryから一意に復元できる実行コマンドは次のとおり。`2026-10-06/pipeline` directoryは当該runの23:02:09 JSTに新規作成され、selection endも当該開始時刻 `2026-10-06T14:02:09.549462Z` であるため、既存manifestの再利用はない。

```text
python3 /home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/00_tool/fetch_s3_mail.py --after 2026-10-06 --before 2026-10-07 --max 10000 --selection-limit 10000 --sample-dir /home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/02_confirm/s3_daily/2026-10-06/pipeline --out /home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl --canonical-output --require-complete
```

Step Functionsは対象EC2起動、SSM launcher受付、Pipeline status監視、失敗後EC2停止、SNS失敗通知まで成功した。Pipeline log・status・manifest・candidateのownerは `ec2-user:ec2-user`。実行環境は `/usr/bin/python3`、Python 3.9.23。接続・IAM・S3 List/Get・EC2・SSM・保存容量・後続工程の障害を示す証跡はない。

### 2件の安全な構造確認

本文・アドレス・添付名・添付内容は表示せず、MIME構造と復号可否だけを確認した。

1. 1件目: 添付part `0_1` の `Content-Type name` と `Content-Disposition filename` がASCIIのencoded-word形式。両headerに `InvalidHeaderDefect`、`CharsetError`、`UndecodableBytesDefect` があるが、解析後filenameに置換文字・surrogateはない。本文は非空・lossy markerなし、添付payload bytes取得済み、candidate sizeと一致。
2. 2件目: 同じ2headerがASCIIのencoded-word形式で、`InvalidHeaderDefect`、`InvalidBase64CharactersDefect`、`UndecodableBytesDefect` がある。解析filenameだけに置換文字があるが、本文は非空・lossy markerなし、添付payload bytes取得済み、candidate sizeと一致。

現行 `recoverable_attachment_filename_warning()` は「raw非ASCIIをUTF-8 strict復号できる」「defectが単独の`UndecodableBytesDefect`」を同時に要求する。今回2件はrawがASCII encoded-wordかつ複数defectなので、実データへの影響が添付名metadataだけでも対象外になる。その後の一般処理が全defectを`issues`へ入れ、`status=needs_review`となった。

### 前日成功検証との差

| 比較項目 | 2026-10-06の前日検証 | 今回の定期実行 |
|---|---|---|
| 対象日 | 2026-10-05完成日、4,116件 | 2026-10-06当日開始時刻まで、3,903件 |
| fetch hash | `5fd533273017cd3a112205c2daf63281db4556f322d4d08d7a4dd2d56c704220` | 同一 |
| confirm hash | `e875c954cfb539dc6350c5fbc131af2bb75a677f97682a82e6ac9f7a500a32f2` | 同一 |
| launcher hash | `f3568d6fd3d5aba2f6dd63385d735824e20a09a11757ff2d545f6ac8e2b4cbb6` | active/masterとも同一 |
| limit | max / selection-limitとも10,000 | 同一 |
| Python / user | Python 3.9.23 / ec2-user | 同一 |
| manifest | 専用先で新規 | 通常日付先で新規。再利用なし |
| 出力先 | 専用検証先、非canonical | 通常master、canonical指定 |
| 入力特徴 | observed警告は既存分類内 | 複数defectのRFC2047風添付名2件が新規 |
| 結果 | 4,116件・confirm OK | 3,901件が利用可能でもfetch validationで全体NG |

通常出力先の違いは原因ではない。fetchはstrict validation後にだけcanonical masterを書き換えるため、今回は書込み前に停止した。通常masterはmtime `2026-09-30 23:21:18.392033990 JST`、SHA-256 `57593e7c7404a5e752d89eea7c184d01efffafe5faf84abab9e625e9c73ffd06` のまま。launcherは本番パスの修正済み `fetch_s3_mail.py` を直接実行しており、旧版・test版の実行はない。

### 対症療法が続いた理由

1. `RECOVERABLE_HEADER_WARNINGS` はdefect文言3個のallow listで、同等に復元済みでも別文言はfatalになる。
2. 添付名修正は単独 `UndecodableBytesDefect` とraw UTF-8という観測済み原因へ限定され、添付payloadとfilenameの利用可否を一般判定していない。
3. Message-ID修正は「原本Message-IDは未使用」という影響ベースへ改善したが、header defect全体の分類方式と日次all-or-nothing契約は対象外のまま残した。
4. `parse_mail()` は残りのMIME/header defectをすべて `issues` にし、1件でも `needs_review` にする。`validate_records()` は全entryが `success` かつ `issues=[]` であることを要求する。
5. fetchはreview candidateを保存するが、1件でもreviewがあるとcanonical masterを一切更新しない。confirmも同じ `validate_records()` を呼ぶため区別できない。launcherはfetchの非0で即終了し、confirm・01-2へ進まない。
6. 過去テストはその時点で観測したdefectとnegativeを十分確認していたが、「別形式のheader不正」「1通だけ利用不能」「日次の正常部分を認証して進める」という影響別matrixを合格条件にしていなかった。

3,000件上限停止は入力完全性を守る別種の正当なfail-closeだった。一方、添付名・Message-ID・今回のRFC2047添付名は、メール単位またはmetadata単位の品質問題を日次全体障害と同じexit 1へ畳み込む共通設計が再発要因である。

### Gmail経路との実装差

- Gmail経路は `messages.get(format="full")` のparsed payloadを使用し、raw RFC headerを解析しない。headerはGmail APIが返したname/value、添付名はpayloadの`filename`をそのまま採用する。原本Message-IDは抽出せず、出力IDはGmail内部IDだった。したがって原本header defectは取得scriptから観測不能で、script自身がwarning/fatal判定する経路はない。
- Gmail APIが原本の各不正headerを「修復」「正規化」「破棄」のどれで処理したかは、このコードだけでは判定できない。parsed payloadを受け取っていた事実と、Gmail側変換の詳細は区別する。
- Gmail経路の本文はbase64url decode後、常にUTF-8 `errors="replace"` でdecodeし、置換発生を記録しない。本文破損をfail-closeしていたわけではない。
- 添付download例外はwarningログ後に空文字を返し、そのrecordを出力する。旧confirmは必須key・型・message_id重複だけを確認し、添付dataの実byte数や本文復号品質、一覧件数との完全性を確認しない。個別message取得の`HttpError`はログしてskipする一方、非`HttpError`のdecode例外等はtop-levelへ伝播し得る。
- よってGmail時代の挙動をそのまま再現すべきではない。S3経路のraw/hash/完全性検証は維持し、Gmail経路で隠れていた個別不良は明示的なreviewとして残すのが妥当。

### 推奨する一まとまりの修正案

defect classや文言の追加allow listではなく、次の影響区分を `parse_mail()`、`validate_records()`、confirmで共通契約にする。

| 影響 | 判定と処理 | 日次 |
|---|---|---|
| 形式警告のみ。未使用header、または利用値に置換・surrogate・欠落がなく本文/添付bytesも検証済み | `header_warnings`へ安全な原因分類を記録。原本は変更せず継続 | 継続 |
| 添付名metadataだけ不正、添付payload bytesは検証済み | 不正な解析filenameを信用せず `part_<番号>.bin` 等の決定的fallback名を使用し、`attachment_filename_sources`へ理由を記録 | 継続 |
| 特定添付のpayload bytesが取得・復号不能 | 当該メールを `needs_review` としてraw・manifest・理由を保持。通常masterへは入れず、他のsuccessメールを認証 | 対象メールのみ要確認、日次継続 |
| 本文など必須データが復号不能、空で説明不能、mail parse不能 | 同じく当該メールをquarantineし、raw/hash/理由を保持。破損recordを後続へ渡さない | 対象メールのみ要確認、日次継続 |
| S3 List/Get・権限、ETag/LastModified変更、raw size/hash、保存失敗、selection不完全、manifest不整合、業務用ID重複、success output schema不整合 | 現行どおり非0終了。既存masterを置換しない | 全体停止 |

最小変更範囲:

1. `01-1_fetch_gmail/00_tool/fetch_s3_mail.py`
   - header defectを「出力で利用する値」「lossy marker」「本文/添付byte検証結果」で分類する共通関数へ集約する。
   - 添付名headerにdefectがある場合、payloadが検証済みなら原名を無理に修復せず決定的fallback名にする。今回の2値やdefect文言を特例化しない。
   - fetch/system整合失敗は `failed`、raw取得済みだが当該メールだけ利用不能なら `needs_review`、実害なしなら `success + warnings` とする。
   - canonical masterは `success` recordだけを書き、全selectionのraw・manifest・parsed candidateとreview理由を保持する。既存master schemaは変えない。
2. `01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py`
   - `selection = success + needs_review`、`master IDs = success IDs`、`failed=0`、`pending=0`、全selectionのraw size/hash、success側schema/添付size、review側raw/reason保持を集合で検証する。
   - `needs_review`件数と原因区分を安全なsummaryとして出力し、欠落を成功扱いで隠さない。
3. `00_pipeline/00_tool/run_full_pipeline.sh` と `run_full_pipeline_master.sh`
   - active/masterを同一に保ち、canonical routeだけが上記partition契約を明示的に有効化する引数をfetch/confirmへ共通渡しする。confirm合格後だけ01-2へ進む。

`common/`、辞書、03系、後続schema、AWS設定は変更不要。個別reviewメールを通常masterから除くことは仕様変更なので、実装時にmanifest/status契約と件数式を明示する。review rawと理由を保存し、Pipeline logへ件数を必ず残すことで黙殺を防ぐ。

### 合格条件と限定テスト

1. 最新3,903件を保存rawからreplayし、3,903件すべてterminal、failed/pending 0。今回2件は本文・添付payload bytes不変で、lossless名は警告継続、lossy名は決定的fallback名＋source記録となる。fetch単体とconfirmが合格する。
2. 2026-10-05保存データの対象ケースだけを再利用し、生UTF-8添付名5件、原本Message-ID 4件が従来どおり警告継続し、本文・添付bytes・S3由来IDが不変であることを確認する。全成功テストの一括再実行は不要。
3. 異なる形式不正の代表として、ASCII encoded-word複数defect、未知charset、invalid base64、unused header不正、使用headerのlossy marker、malformed Date fallback、MIME boundary defectを合成する。defect文言ではなく影響結果で分類されることを確認する。
4. 実際の復元不能として、本文strict decode失敗、添付payload `decode=True`不能、mail parse例外を与え、対象メールだけ `needs_review`、raw/hash/reason保持、success masterから除外となることを確認する。
5. システム障害として、S3 AccessDenied/NoSuchKey、ETag変更、byte数/hash不一致、selection上限超過・不完全、manifest/output ID不整合、業務用ID重複をfake S3または隔離fixtureで確認し、全体非0・既存master不変を維持する。
6. 取得器単体は隔離先で上記分類と件数式を確認する。通常接続は別にlauncher引数captureでfetch/confirmが同じpartition契約を受け、fetch成功→confirm成功→01-2入口markerへ到達することを確認する。後続step本体、AI、full Pipelineは実行しない。

### 調査時の不変事項

本レビューではコード、AWS設定、通常master、`pipeline_ses_steps_src`を変更せず、Pipeline・AI・メール送信・S3取得を再実行していない。本番 `technoverse`、prefix、安全lockも変更していない。保存済みrawはread-onlyで構造と復号可否だけを確認し、本文・アドレス・添付名・添付内容を報告へ露出していない。

## 2026-10-07 個別メール不良の影響分離修正・限定テスト

### 結論

- 取得器・confirm・active/master両launcherを一まとまりで修正し、個別メールの本文・添付復元不能またはparse例外を`needs_review`へ隔離しつつ、正常メールだけを通常schemaのmasterへ渡せる契約へ変更した。
- 集合契約は `selection IDs = success IDs ∪ needs_review IDs`、両集合は重複なし、`master IDs = success IDs`。`failed/pending`、説明不能欠落、raw hash/size不一致、業務用ID不整合・重複、保存失敗、正常0件は従来どおり非0停止する。
- 保存済み2026-10-06対象2件は限定replayで2/2とも正常出力へ入り、業務用`message_id`、添付件数・size・bytes hashは不変。復元不能な添付名だけはpart番号とMIME typeから決める安全なfallback名へ置換した。
- 全件取得・通常master更新・full Pipeline・AI・メール送信・AWS変更・`pipeline_ses_steps_src`変更・commit/pushは実施していない。2026-10-06 JST全件の専用取得・confirmは本差分レビュー後に実施する。

### 変更ファイルと差分要旨

| ファイル | 変更内容 | 変更後SHA-256 |
|---|---|---|
| `01-1_fetch_gmail/00_tool/fetch_s3_mail.py` | headerを実利用影響で分類。添付payloadをtransfer encodingまで検証し、添付名だけ不良なら決定的fallback。個別復元不能・parse例外を`needs_review`へ隔離し、正常recordだけをmasterへ保存。`needs_review.jsonl`と`fetch_summary.jsonl`を追加 | `39a63812cf8ca7d0db483a68d0ffad48263cbb3193d8c9e3bc78c1e2beadda6f` |
| `01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py` | success/review集合、review理由・raw参照、summary、正常master schema/添付bytesを同じ契約で確認 | `0cb8e14be8853df4ed0c0ffef08ae2812e64bb7fea1bd09428f6d9e31d27e838` |
| `00_pipeline/00_tool/run_full_pipeline.sh` | fetch/confirmへ`--allow-needs-review`を共通指定 | `3badb28c2adfe3d3909a06bf74ffb5dba10f6b01ea8bb0965ae728cf0f217107` |
| `00_pipeline/00_tool/run_full_pipeline_master.sh` | activeと同一変更・byte一致 | `3badb28c2adfe3d3909a06bf74ffb5dba10f6b01ea8bb0965ae728cf0f217107` |

通常masterのschemaは変更していない。review情報は後続へ渡すmasterではなく、run directoryの`selection_manifest.jsonl`、`needs_review.jsonl`、`raw/<message_id>.eml`、`fetch_summary.jsonl`へ保存する。通常masterは作業前のSHA-256 `57593e7c7404a5e752d89eea7c184d01efffafe5faf84abab9e625e9c73ffd06`、mtime `2026-09-30 23:21:18 JST`のまま。

### 継続・隔離・停止の判断基準

| 判定 | 条件 | 出力と日次動作 |
|---|---|---|
| 警告付き正常 | header形式差だけで、利用値に置換文字・surrogateがなく、本文または添付bytesを検証できる。未使用の原本Message-ID警告もここに含む | `status=success`、`header_warnings`へ内容を含まない分類を記録しmasterへ渡す |
| 警告付き正常＋添付名fallback | 添付payload bytesは検証済みだが添付名だけ安全・losslessに復元できない | `part_<part番号><MIME由来拡張子>`を使用する。generic MIMEだけは元名末尾の安全な業務拡張子（pdf/xls/xlsx/doc/docx/csv/txt/zip/eml）を引き継ぎ、それ以外は`.bin`。`attachment_filename_sources=deterministic_fallback_header_defect`を記録し、原本・添付bytes・業務用IDは不変 |
| 個別隔離 | 本文strict decode不能、添付payload復号/検証不能、空本文かつ添付なし、MIME実害issue、個別parse例外 | `status=needs_review`。通常masterから除外し、message_id、固定理由、raw hash、絶対`raw_ref`をmanifestと`needs_review.jsonl`へ残して残りを継続 |
| 全体停止 | S3 List/Get/権限・ETag/LastModified変更、raw size/hash・ディスク・出力保存失敗、selection不完全、failed/pending残存、集合/summary/schema不整合、業務用ID重複・不一致、正常0件 | fetchまたはconfirmを非0終了し、01-2へ進めない |

成功ログと`fetch_summary.jsonl`には`selection/success/warning_success/needs_review/failed/pending/attachments`とreview記録先を残す。reviewが1件以上なら`result=completed_with_review`、confirmは`要確認ありで完了`と表示し、`全件正常`と区別する。

### 限定テスト結果

- `python3 -m py_compile`（取得器・confirm）: OK。
- `bash -n`（両launcher）: OK。両launcherはbyte一致。
- 保存済み2026-10-06対象2件: 限定replayの正常masterは2件、`needs_review=0`、`warning_success=2`。両方の添付bytes SHA-256・sizeは修正前candidateと一致し、業務用IDも不変。複数defect/未知charset系を含む2形式とも正常化し、lossyな添付名だけMIME由来拡張子付きfallbackとなった。同じ隔離成果物をconfirmし、`全件正常`でexit 0。
- 直近添付処理影響: `04-1`は拡張子・MIME typeを使い、generic `.bin` のOOXMLはbytes内部構造で限定判定する。対象2件について修正前後の`is_eligible_attachment()`判定が2/2一致することをread-onlyで確認した。
- 既存警告回帰: 2026-10-05保存rawから生UTF-8添付名1件と原本Message-ID形式警告1件を再parseし、いずれも`success`、従来のwarning分類と業務用IDを維持。
- 代表MIME: malformed DateはS3時刻fallbackで正常、invalid base64添付と終端boundary欠落はそれぞれ個別`needs_review`。
- 混在3ケース: `正常＋本文復元不能`、`正常＋添付復元不能`、`正常＋parse例外`はいずれも selection 2 / success 1 / needs_review 1 / failed 0 / pending 0。正常1件だけをmasterへ出し、reviewのID・理由・raw参照・summary件数が一致。各ケースを実confirmへ渡し、3/3とも`要確認ありで完了`でexit 0。
- 整合性・障害negative: 業務用ID重複、pending、master説明不能欠落、selection不完全、raw hash/size不一致、S3 Get失敗、正常0件、出力保存失敗をすべて非合格として検出。既存master相当のsentinelはS3 Get/出力保存失敗時に不変。
- launcher接続: stubで両launcherの`fetch → confirm → 01-2入口`を確認。fetch/confirmへ同じ日付・limit・manifest・`--require-complete --allow-needs-review`が渡り、01-2入口markerで意図的にexit 86。後続本体は実行していない。active/masterのcaptureは同一。

focused test本体は`/tmp/test_fetch_s3_partition_20261007.py`。最終出力は `saved=2 prior_warnings=2 header_mime=3 partition=3 confirm=4 integrity=8`。

### 変更前バックアップと切戻し

変更前に以下を隣接backupし、各原本とのSHA-256一致を確認した。

| backup | SHA-256 | mode |
|---|---|---|
| `01-1_fetch_gmail/00_tool/fetch_s3_mail_old_20261007_075052.py` | `5fd533273017cd3a112205c2daf63281db4556f322d4d08d7a4dd2d56c704220` | 644 |
| `01-1_fetch_gmail/02_confirm/confirm_fetch_gmail_old_20261007_075052.py` | `e875c954cfb539dc6350c5fbc131af2bb75a677f97682a82e6ac9f7a500a32f2` | 644 |
| `00_pipeline/00_tool/run_full_pipeline_old_20261007_075052.sh` | `f3568d6fd3d5aba2f6dd63385d735824e20a09a11757ff2d545f6ac8e2b4cbb6` | 755 |
| `00_pipeline/00_tool/run_full_pipeline_master_old_20261007_075052.sh` | `f3568d6fd3d5aba2f6dd63385d735824e20a09a11757ff2d545f6ac8e2b4cbb6` | 755 |

レビューで切戻し判断となった場合はPipeline非実行中・共有lock非保持を確認後、次の4コピーを一時名経由で置換する。通常master、既存raw、run成果物は切戻し対象外。

```bash
cd /home/ec2-user/pipeline_ses_steps
cp --preserve=all 01-1_fetch_gmail/00_tool/fetch_s3_mail_old_20261007_075052.py 01-1_fetch_gmail/00_tool/fetch_s3_mail.py.rollback_tmp
mv 01-1_fetch_gmail/00_tool/fetch_s3_mail.py.rollback_tmp 01-1_fetch_gmail/00_tool/fetch_s3_mail.py
cp --preserve=all 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail_old_20261007_075052.py 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py.rollback_tmp
mv 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py.rollback_tmp 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py
cp --preserve=all 00_pipeline/00_tool/run_full_pipeline_old_20261007_075052.sh 00_pipeline/00_tool/run_full_pipeline.sh.rollback_tmp
mv 00_pipeline/00_tool/run_full_pipeline.sh.rollback_tmp 00_pipeline/00_tool/run_full_pipeline.sh
cp --preserve=all 00_pipeline/00_tool/run_full_pipeline_master_old_20261007_075052.sh 00_pipeline/00_tool/run_full_pipeline_master.sh.rollback_tmp
mv 00_pipeline/00_tool/run_full_pipeline_master.sh.rollback_tmp 00_pipeline/00_tool/run_full_pipeline_master.sh
python3 -m py_compile 01-1_fetch_gmail/00_tool/fetch_s3_mail.py 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py
bash -n 00_pipeline/00_tool/run_full_pipeline.sh 00_pipeline/00_tool/run_full_pipeline_master.sh
cmp -s 00_pipeline/00_tool/run_full_pipeline.sh 00_pipeline/00_tool/run_full_pipeline_master.sh
```

### レビュー後に実行する2026-10-06 JST全件の専用取得・confirm

通常masterを更新しない新規run directoryを使う。`--canonical-output`は付けず、fetch出力・manifest・raw・review・summaryを専用先へ閉じる。次のコマンドは本修正作業では未実行。

```bash
cd /home/ec2-user/pipeline_ses_steps
S3_REVIEW_DIR=/home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/02_confirm/s3_daily/2026-10-06/impact-partition-validation-20261007-review
test ! -e "$S3_REVIEW_DIR"

python3 01-1_fetch_gmail/00_tool/fetch_s3_mail.py \
  --after 2026-10-06 \
  --before 2026-10-07 \
  --max 10000 \
  --selection-limit 10000 \
  --sample-dir "$S3_REVIEW_DIR" \
  --out "$S3_REVIEW_DIR/fetch_gmail_mail_master.jsonl" \
  --require-complete \
  --allow-needs-review

python3 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py \
  --input "$S3_REVIEW_DIR/fetch_gmail_mail_master.jsonl" \
  --manifest "$S3_REVIEW_DIR/selection_manifest.jsonl" \
  --after 2026-10-06 \
  --before 2026-10-07 \
  --selection-limit 10000 \
  --require-complete \
  --allow-needs-review
```

レビュー後のクローズ条件は、fetch/confirmともexit 0、`failed=0`、`pending=0`、selection集合式とmaster集合式が成立し、正常件数が1件以上であること。reviewが残る場合は`needs_review.jsonl`件数・ID・理由・`raw_ref`とsummaryが一致し、ログが`要確認ありで完了`であることを確認する。

## 2026-10-07 個別メール影響分離の差分レビュー結果

### 結論

- 修正必須の指摘なし。通常レビューを完了した。
- 変更前backupと現行4ファイルの差分は、個別メール影響分離、confirm集合検証、両launcherへの同一契約追加に限定され、依頼対象外の変更はない。
- 保存済み限定テスト結果は実装差分と対応しており、追加テストが必要な具体的疑義はなかった。
- 2026-10-06 JST分の通常master非更新・専用先取得とconfirmへ進めてよい。実行コマンドとクローズ条件は直前節の記載どおりとする。

### 確認根拠

1. 個別不良と正常処理
   - `parse_mail()`は本文・添付の復元不能を`issues`付き`needs_review`とし、個別parse例外もrun loopで`mail_parse_exception_part_unknown`付き`needs_review`へ変換して残りを継続する。
   - S3 Get、source変更、raw byte数、raw保存などの例外は別のfetch処理で`failed`を記録後に`FetchStoppedError`を送出し、個別reviewとして握りつぶさない。
2. 対象2件と添付名fallback
   - 添付payloadはtransfer encodingを含めて検証し、bytes検証済みの場合だけheader形式不良を警告継続する。
   - losslessに復元できない添付名はpart番号とMIME/安全な業務拡張子から決定的に生成する。通常recordの`message_id`と添付`data/size/mime_type`は変更しない。
   - 保存済みテストの対象2件2/2正常、添付bytes hash/size不変、`04-1`形式判定2/2不変という結果とコード経路が一致する。
3. 集合・隔離記録・正常0件
   - `validate_records()`はselection ID重複、status集合、success/reviewの重複なし和、master IDsとsuccess IDsの一致を検証する。
   - 全selectionのraw size/hashを検証し、reviewは非空の固定理由、`raw_ref`、`needs_review.jsonl`との完全一致を要求するため、黙った欠落を成功扱いにしない。
   - success集合が空なら`no successful mail to publish`で非0となり、既存masterを書き換えず後続へ進まない。
4. confirm・launcher
   - confirmはfetchと同じ`validate_records()`を使用し、review fileとsummaryの一致も検証する。reviewが存在しても整合していれば`要確認ありで完了`としてexit 0となる。
   - active/master両launcherの差分はfetch/confirmへの`--allow-needs-review`追加だけで、現行2ファイルはbyte一致。順序はfetch → confirm → 01-2のまま。
5. 不変範囲
   - 通常master recordのキー集合検証は従来どおりでschema変更なし。通常master SHA-256は`57593e7c7404a5e752d89eea7c184d01efffafe5faf84abab9e625e9c73ffd06`で不変。
   - 入力bucket `technoverse-ses-mail`、prefix `raw/`、本番出力`technoverse`、既存安全lockの変更は4差分にない。取得器はS3へ書き込まない。

### レビュー時の実施範囲

- 実施: 4本の変更前backupとのdiff、現行hash、launcher byte一致、引き継ぎの限定テスト証跡の照合。
- 未実施: コード変更、追加テスト、全件取得、通常master更新、AI・後続本体・全体Pipeline、AWS変更、`pipeline_ses_steps_src`同期、commit、push。
- レビュー後の任意改善・追加レビュー往復は不要。

## 2026-10-07 2026-10-06 JST分の専用取得・confirm完了

### 結論

- 差分レビュー完了後、直前節のコマンドどおり、通常masterを更新しない専用先へ2026-10-06 00:00 JST以上、2026-10-07 00:00 JST未満を上限10,000件で1回取得し、同じ成果物をconfirmした。fetch・confirmはいずれもexit 0。
- 選択3,907件は正常3,840件と要確認67件の重複なしの和。正常は警告なし2,364件、警告あり1,476件。取得失敗0、未処理0、selection/master/reviewの業務用ID重複0。
- confirmは`S3 confirm OK (要確認ありで完了)`。要確認67件は理由・raw hash・`raw_ref`・専用review記録が一致し、正常分だけがmasterへ通過したため合格とする。
- 昨夜停止原因の2件は両方とも正常出力へ入り、業務用ID・添付件数・添付size・添付bytes SHA-256は修正前candidateと一致した。1件は元のlossless名を保持、1件は決定的fallback名を使用した。
- 通常master、実行コード、AWS設定、本番出力`technoverse`、prefix、安全lockは不変。AI・後続step・全体Pipeline・メール送信・`_src`同期・commit・pushは実施していない。今回の修正・検証は完了。

### 実行結果

| 項目 | 結果 |
|---|---|
| 対象範囲 | `2026-10-05T15:00:00.000000Z`以上、`2026-10-06T15:00:00.000000Z`未満（2026-10-06 JST完成日） |
| selection | 3,907 |
| success（警告なし） | 2,364 |
| success（警告あり） | 1,476 |
| needs_review | 67 |
| failed / pending | 0 / 0 |
| 正常添付 | 1,645 |
| raw保存 | 3,907ファイル |
| ID重複 | selection 0 / master 0 / review 0 |
| 集合 | `selection = success ∪ needs_review`、重複なし、`master = success`、review file = review、すべて成立 |
| master schema shape | 1種類。既存schema検証合格 |
| confirm | exit 0、`要確認ありで完了` |

専用先:

`01-1_fetch_gmail/02_confirm/s3_daily/2026-10-06/impact-partition-validation-20261007-review/`

成果物SHA-256:

| 成果物 | SHA-256 |
|---|---|
| `selection_manifest.jsonl` | `caadd83d6e21719ebf79ae76b2eb9c6f8b7552268dc6865221ff034e3129aec3` |
| `fetch_gmail_mail_master.jsonl` | `38dfa229aa84be5d9049f1919fecac3f8b2f2ce3ad2c6185e13cd2191f715a0f` |
| `needs_review.jsonl` | `5c54e9e800acd13352da2e1999bb082832a23cd832edab932c020e8ce2467252` |
| `fetch_summary.jsonl` | `0571b7c872dfe93cbe74b50629fbf3784c32d0ad5c583abf3ead365b0601b700` |

### 昨夜3,903件との集合比較

- 昨夜のselection 3,903 IDは新selectionに3,903/3,903存在し、欠落0。
- 昨夜分の新分類はsuccess 3,836、needs_review 67。昨夜の旧successから67件がreviewへ移り、旧needs_review 2件は両方successへ移った。
- 追加は4件で、全4件がsuccess、review追加0。S3 LastModifiedは2026-10-06 23:07:09 JST〜23:09:15 JSTで、昨夜のselection終端23:02:09.549462 JST以降、日次終端24:00 JST未満に到着した分として説明できる。

### 昨夜停止原因2件

| 業務用message_id | 新status | 添付 | 確認 |
|---|---|---|---|
| `s3_3bdf5742f1e71977f9867c74318663df5a411e9ab1b7efc8739b9e9b8f37e43d` | success | 1件、`mime_filename` | ID不変、size・bytes SHA-256不変 |
| `s3_ca099706b7b5fc00375cb28d1f5b9d5dd3a65a1cd5fefdffc7deb488da8ab1cc` | success | 1件、`deterministic_fallback_header_defect` | ID不変、size・bytes SHA-256不変 |

保存済み限定テストで確認済みの`04-1`ファイル形式判定2/2不変と合わせ、添付名fallbackによる後続形式判定・添付bytesへの破壊はない。

### needs_review 67件

- 全67件は`8bit`の本文partで、解析後payloadは取得できるが原本bytesとの厳密な再構成検証ができないため、`body_payload_decode_unverified_*`として隔離されたもの。
- 影響partは70個。`text/plain` 45、`text/html` 25。理由分布はroot 32件、part `0_0`のみ32件、part `0_0`と`0_1`の両方3件。
- 全件でmanifest・`needs_review.jsonl`・保存raw・raw hash・`raw_ref`がconfirm済み。通常masterへは含めず、黙って除外していない。

### 適用状態・不変確認・残存プロセス

| ファイル | SHA-256 |
|---|---|
| `01-1_fetch_gmail/00_tool/fetch_s3_mail.py` | `39a63812cf8ca7d0db483a68d0ffad48263cbb3193d8c9e3bc78c1e2beadda6f` |
| `01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py` | `0cb8e14be8853df4ed0c0ffef08ae2812e64bb7fea1bd09428f6d9e31d27e838` |
| `00_pipeline/00_tool/run_full_pipeline.sh` | `3badb28c2adfe3d3909a06bf74ffb5dba10f6b01ea8bb0965ae728cf0f217107` |
| `00_pipeline/00_tool/run_full_pipeline_master.sh` | `3badb28c2adfe3d3909a06bf74ffb5dba10f6b01ea8bb0965ae728cf0f217107` |

- 両launcherはbyte一致。
- 通常masterはsize `269764671`、mtime `2026-09-30 23:21:18.392033990 JST`、SHA-256 `57593e7c7404a5e752d89eea7c184d01efffafe5faf84abab9e625e9c73ffd06`で不変。
- `fetch_s3_mail.py`、`confirm_fetch_gmail.py`、`run_full_pipeline.sh`、`run_full_pipeline_master.sh`の残存プロセスなし。
- 専用検証先の使用量は約1.2GB。通常masterおよび本番S3出力への書込みなし。

## 2026-10-07 needs_review 67件の本文利用可否調査

### 結論

- 分類は**利用可能67件、実際に利用不能0件、根拠不足0件**。今回の67件隔離は妥当ではなく、本文破損ではなく8bit本文の検証方法による過剰隔離である。
- 67件はすべて昨夜のsuccessから今回needs_reviewへ移った同一ID。保存rawは前回と今回で67/67 byte一致し、両runのmanifest記録SHA-256とも67/67一致したため、原本の破損・欠落はない。
- 影響70 partはすべて原本payload bytesを復元でき、2種類のparser policyで70/70一致した。宣言charsetでのstrict復号も70/70成功し、本文出力は67/67非空、前回出力とbyte相当で一致し、置換文字・surrogateは0件だった。
- 修正必須は1件。個別メール例外ではなく、`transfer_payload_verified()`のidentity transfer encoding検証を、表示用Unicodeではなくparserが保持する原payload bytesとの比較へ直す。今回は調査のみでコード変更・再取得・後続実行はしていない。

### 修正必須の指摘

**[P1] 8bit本文の表示用Unicodeを原payloadとして再エンコードし、利用可能な67件を隔離している**

- 対象: `01-1_fetch_gmail/00_tool/fetch_s3_mail.py`の`transfer_payload_verified()`（現行80〜100行、特に85〜91行）と本文判定呼出し（318〜327行）。
- 問題: `part.get_payload(decode=False)`はPython 3.9の`email.message.Message.get_payload()`で、BytesParserが保持するsurrogateescaped bytesを宣言charsetの表示用Unicodeへ変換して返す場合がある。その非ASCII Unicodeを`ascii` + `surrogateescape`で再エンコードするため、今回の8bit partは70/70で`UnicodeEncodeError`となる。
- 影響: `get_payload(decode=True)`で原payload bytesを取得でき、strict charset復号も成功しているにもかかわらず、`body_payload_decode_unverified_*`が付与される。67件はcandidateには生成されるが正常masterから除外され、利用可能なメールが後続へ渡らない。
- 根拠: Python 3.9標準ライブラリは内部payloadのsurrogateを`ascii/surrogateescape`でbytesへ戻した後、`decode=False`の場合だけcharset + `replace`で表示用文字列へ変換する（`/usr/lib64/python3.9/email/message.py` 262〜280行）。検証器はこの変換後の値を原payloadと誤認している。

### 67件・70 partの調査結果

| 項目 | 結果 |
|---|---:|
| 前回selectionとのID一致 | 67 / 67 |
| 前回success → 今回needs_review | 67 / 67 |
| 前回・今回の保存raw byte一致 | 67 / 67 |
| 実ファイルSHA-256と両manifest一致 | 67 / 67 |
| 対象part | 70（root 32、`0_0` 35、`0_1` 3） |
| MIME | `text/plain` 45、`text/html` 25 |
| charset | `utf-8` 61、`shift_jis` 9 |
| Content-Transfer-Encoding | `8bit` 70 |
| `decode=True` bytes取得 | 70 / 70 |
| default/compat32 policyのbytes一致 | 70 / 70 |
| parser内部の原payload bytesと`decode=True`一致 | 両policyとも70 / 70 |
| 宣言charsetでstrict復号成功 | 70 / 70 |
| 前回・今回の通常record全体一致 | 67 / 67 |
| 本文非空・前回本文一致 | 67 / 67 |
| 本文の置換文字・surrogateなし | 67 / 67 |
| 添付record一致 | 67 / 67（添付あり32件、計32添付） |

原因分類:

| 分類 | 件数 | 根拠 |
|---|---:|---|
| 原本の破損・欠落 | 0 | raw byte・hash・前回rawが全件一致 |
| 解析実装の制約で復元不能 | 0 | 原payload bytes取得、独立policy一致、strict charset復号が全part成功 |
| 検証条件・検証方法が厳しすぎる | 67 | 表示用UnicodeのASCII再エンコードだけが70/70失敗 |

67件中32件には既存の添付filename header形式警告があり、全32添付のfilename sourceは`mime_filename`。前回と今回で添付の名前・MIME・size・dataが一致しており、この警告による本文・添付bytes・後続利用内容への実害はない。今回のneeds_review理由はheader警告ではなく、上記本文payload検証の誤判定だけである。

本文・個人情報を出力しない代表経路確認では、IDをSHA-256先頭12桁へ置換した次の3形状を確認した。

| 匿名ID | 形状 | 結果 |
|---|---|---|
| `18d1562429e0` | root `text/plain; charset=utf-8; 8bit` | 原bytes比較一致、strict復号成功、本文一致 |
| `d44f1d860d30` | multipartの`0_0 text/plain; charset=utf-8; 8bit` | 原bytes比較一致、strict復号成功、本文・添付一致 |
| `cef64a39c918` | `0_0 text/plain` + `0_1 text/html`（ともにUTF-8/8bit） | 2 partとも原bytes比較一致、strict復号成功、本文一致 |

### 共通原因への最小修正案

1. `transfer_payload_verified()`で`8bit`、`binary`、`7bit`、encodingなしのidentity transfer encodingを検証するときは、`get_payload(decode=False)`の表示用Unicodeを使わない。
2. BytesParserがleaf partに保持する原payloadのsurrogateescaped表現からbytesを復元し、`get_payload(decode=True)`の`data`と完全一致した場合だけverifiedとする。原payloadが文字列でない、bytes復元不能、または不一致なら従来どおりneeds_reviewとする。
3. base64とquoted-printableの厳密な再decode比較、本文のstrict charset復号、raw hash、ID・集合整合、保存失敗・システム障害の停止条件は変更しない。8bitであること自体は警告または正常判定の材料に留め、復元不能の根拠にはしない。
4. Python内部表現への依存は小さなhelperへ閉じ、Python 3.9 BytesParserのsurrogateescape往復をテストで固定する。個別IDや今回のpart番号を条件にしない。

### 修正後の限定検証条件

- 保存済み67 rawだけを同じparser経路で再生し、67件すべてが正常出力へ入り、この理由によるneeds_reviewが0件になることを確認する。
- 修正前後で67件の通常recordを比較し、`message_id`、本文、添付filename/MIME/size/data、通常schemaが67/67一致することを確認する。
- root本文、multipart plain、plain+html、`utf-8`、`shift_jis`の各代表を含め、70 partすべてで原payload bytes = `decode=True` bytes、strict charset復号成功を確認する。
- identity payloadのbytes不一致・原payload欠落・charset復号不能を作ったfocused fixtureはneeds_reviewを維持する。既存の不正base64/quoted-printable、添付復元不能、raw hash不一致、ID重複、保存失敗、正常0件の停止条件も対象testだけ再確認する。
- subsetのfetch/confirmで`selection = success ∪ needs_review`（重複なし）、`master = success`を確認する。S3再取得、全件取得、後続step、通常master更新、全テスト一括実行は不要。

### 調査範囲

- 使用: 保存済みの前回`pipeline/`結果、今回`impact-partition-validation-20261007-review/`のraw・manifest・candidate・review・master。
- 実施: 67 ID照合、実raw hash照合、Python 3.9 default/compat32 policyでのpayload bytes比較、strict charset復号、前回/今回record比較、現行実装と標準ライブラリ経路の確認。
- 未実施: コード修正、S3再取得、Gmail探索、AI、後続step、全体Pipeline、通常master更新、AWS変更、`_src`同期、commit、push。

## 2026-10-07 正常な8bit本文67件の誤隔離修正・限定検証

### 結論

- `transfer_payload_verified()`のidentity transfer encoding検証を修正し、Python 3.9 BytesParserが保持する原payloadのsurrogateescaped表現からbytesを復元して、`decode=True`結果と完全一致する場合だけverifiedとするようにした。
- 保存済み67件だけを専用先へlocal replayした結果、success 67、warning_success 32、needs_review / failed / pendingはすべて0。前回正常時の通常record・本文・添付・業務用message_idは67/67一致した。
- 影響70 partはUTF-8 61、Shift_JIS 9で全件検証成功。focusedな負例では、復号不能本文・添付復元不能・解析例外を当該メールだけ隔離し、同時入力した正常メールは通過した。
- fetch本体と関連test以外の実行コードは変更していない。3,907件全体の再解析は未実施で、レビュー後に下記専用replay・confirmを1回実行する。

### 原因と変更内容

変更前は、identity transfer encodingでも`part.get_payload(decode=False)`が返す表示用UnicodeをASCIIへ再エンコードしていた。Python 3.9ではBytesParserの原payloadが宣言charsetで表示用Unicodeへ変換されるため、正常な非ASCII 8bit本文で`UnicodeEncodeError`となり、67件を誤ってneeds_reviewへ分類していた。

変更箇所:

| ファイル | 変更 |
|---|---|
| `01-1_fetch_gmail/00_tool/fetch_s3_mail.py` | `transfer_payload_verified()`の`8bit` / `7bit` / `binary` / encodingなしを共通分岐にし、leaf partの原payload表現を`ascii` + `surrogateescape`でbytesへ戻して`data`と完全比較。原payloadが文字列でない、復元不能、bytes不一致はFalseを維持 |
| `01-1_fetch_gmail/00_tool/test_fetch_s3_mail_transfer.py` | identity encoding、UTF-8 / Shift_JIS、bytes不一致、原payload復元不能、base64 / quoted-printable、未知encoding、strict charset復号不能のfocused testを追加 |

base64とquoted-printableの再decode比較、未知encodingの拒否、本文charset strict復号、raw hash、業務用ID・集合整合、個別review継続の処理は変更していない。特定message ID、part番号、charsetによる例外は追加しておらず、identity encodingを無条件で成功扱いにもしていない。

### バックアップ・hash・切戻し

変更前バックアップ:

`01-1_fetch_gmail/00_tool/fetch_s3_mail_old_20261007_083534.py`

| ファイル | SHA-256 |
|---|---|
| 変更前backup | `39a63812cf8ca7d0db483a68d0ffad48263cbb3193d8c9e3bc78c1e2beadda6f` |
| 現行`fetch_s3_mail.py` | `66194aec508c07242925248ce6ba054f43be88964a83601c52dadb978c78eac1` |
| 追加test | `c3eb6b46b2f7cff4aa382f7a7fd5510638763e679a89669db90a1d0491ba4955` |

本体だけを切り戻す場合:

```bash
cd /home/ec2-user/pipeline_ses_steps
cp --preserve=mode,timestamps \
  01-1_fetch_gmail/00_tool/fetch_s3_mail_old_20261007_083534.py \
  01-1_fetch_gmail/00_tool/fetch_s3_mail.py
python3 -m py_compile 01-1_fetch_gmail/00_tool/fetch_s3_mail.py
```

作業差分全体を切り戻す場合は、上記に加えて新規`test_fetch_s3_mail_transfer.py`と本節の追記を対象から外す。通常master・専用runのsource rawは変更していないため、データ切戻しは不要。

### 限定検証結果

1. 構文・共通経路test
   - Python 3.9.23でfetch本体と追加testの`py_compile`成功。
   - 追加test 4 test成功。identity経路はUTF-8/8bit、Shift_JIS/8bit、7bit、binary、encodingなしの5代表を確認し、原payload bytesとの一致時だけTrue、bytes不一致・原payload非文字列・surrogateescape復元不能はFalseを確認した。
   - base64 / quoted-printableの正常・bytes不一致、壊れたbase64、未知encodingを確認し、既存の厳密判定を維持した。
2. 保存済み67件の専用local replay
   - 専用先: `01-1_fetch_gmail/02_confirm/s3_daily/2026-10-06/identity-transfer-focused-20261007-review/`
   - selection 67、success 67、warning_success 32、needs_review 0、failed 0、pending 0、添付32。`needs_review.jsonl`は空。
   - 影響70 partは`8bit` 70、UTF-8 61、Shift_JIS 9。全partで原payload bytes検証とstrict charset復号が成功した。
   - 前回正常時との比較は通常record 67/67、本文67/67、添付67/67、業務用message_id 67/67一致。masterとcandidateもbyte一致。
   - confirm exit 0: `S3 confirm OK (全件正常): selected=67 success=67 warning_success=32 needs_review=0 failed=0 pending=0`。
3. 個別隔離・破損回帰
   - 「正常 + 本文復号不能」「正常 + 添付復元不能」「正常 + 解析例外」の3ケースで各selection 2、success 1、needs_review 1。正常IDとreview IDは重複せず、各confirm成功。
   - strict charset不能は`decode_utf8_replace_part_0`として隔離を維持し、identity payload検証失敗とは混同しない。
   - 壊れたbase64添付とMIME boundary欠落もneeds_reviewを維持。合格済みの無関係な全テストは再実行していない。

限定replay成果物SHA-256:

| 成果物 | SHA-256 |
|---|---|
| `selection_manifest.jsonl` | `84a2e850893be959672d0dbd46cf82a3940d5c6917562736c8e5624ba33f77d7` |
| `fetch_gmail_mail_master.jsonl` | `cd3b085066b3ff852e508c2366d8941d12c4e2f5aa726051cb69ed2302aa16cf` |
| `needs_review.jsonl`（空） | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `fetch_summary.jsonl` | `6e59012ff06d627e9daef71c8742347de55091a0156ec133d77823b8d0cbe87f` |

変更していない関連ファイルのSHA-256:

| ファイル | SHA-256 |
|---|---|
| `01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py` | `0cb8e14be8853df4ed0c0ffef08ae2812e64bb7fea1bd09428f6d9e31d27e838` |
| `00_pipeline/00_tool/run_full_pipeline.sh` | `3badb28c2adfe3d3909a06bf74ffb5dba10f6b01ea8bb0965ae728cf0f217107` |
| `00_pipeline/00_tool/run_full_pipeline_master.sh` | `3badb28c2adfe3d3909a06bf74ffb5dba10f6b01ea8bb0965ae728cf0f217107` |

### レビュー対象

- 変更前backupと現行`fetch_s3_mail.py`の差分は`transfer_payload_verified()`内のidentity encoding分岐だけ。
- 新規`test_fetch_s3_mail_transfer.py`は上記共通経路と負例だけを対象とする。
- 本引き継ぎ節は原因・差分・検証・切戻し・レビュー後コマンドの記録。confirm、launcher、後続、辞書、AWS設定、本番出力、通常master、`_src`は変更なし。

### レビュー後に実行する保存済み3,907件の専用再解析・confirm

S3へ接続せず、前回専用runに保存済みのmanifestと3,907 rawを新しい専用先へコピーして`--replay`する。`--canonical-output`は指定しないため通常masterは更新しない。次のコマンドは今回未実行。

```bash
cd /home/ec2-user/pipeline_ses_steps
SOURCE=/home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/02_confirm/s3_daily/2026-10-06/impact-partition-validation-20261007-review
TARGET=/home/ec2-user/pipeline_ses_steps/01-1_fetch_gmail/02_confirm/s3_daily/2026-10-06/identity-transfer-full-replay-20261007-review

test ! -e "$TARGET"
mkdir -m 700 -p "$TARGET/raw"
cp --preserve=mode,timestamps "$SOURCE/selection_manifest.jsonl" "$TARGET/selection_manifest.jsonl"
cp -a "$SOURCE/raw/." "$TARGET/raw/"

python3 01-1_fetch_gmail/00_tool/fetch_s3_mail.py \
  --after 2026-10-06 \
  --before 2026-10-07 \
  --max 10000 \
  --selection-limit 10000 \
  --sample-dir "$TARGET" \
  --out "$TARGET/fetch_gmail_mail_master.jsonl" \
  --manifest "$TARGET/selection_manifest.jsonl" \
  --replay \
  --require-complete \
  --allow-needs-review

python3 01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py \
  --input "$TARGET/fetch_gmail_mail_master.jsonl" \
  --manifest "$TARGET/selection_manifest.jsonl" \
  --after 2026-10-06 \
  --before 2026-10-07 \
  --selection-limit 10000 \
  --require-complete \
  --allow-needs-review
```

今回の限定結果からの期待値はselection 3,907、success 3,907、warning_success 1,508、needs_review / failed / pending 0、添付1,677、confirm「全件正常」。実行時は期待値だけで合格とせず、summary・集合式・master ID重複・通常record schema・raw hashをconfirm結果で確認する。

### 今回未実施

- 3,907件全体の再解析、S3再取得、通常master更新、AI、後続step、全体Pipeline、AWS変更、`pipeline_ses_steps_src`同期、commit、push。

## 2026-10-07 正常な8bit本文67件の誤隔離修正レビュー

### 結論

- **修正必須の指摘なし。通常レビューを完了した。**
- 変更前backupとの差分は`transfer_payload_verified()`のidentity transfer encoding検証だけで、Python 3.9 BytesParserが保持する原payload bytesとの完全一致を要求する実装として妥当。
- 保存済み67件のrecord一致、追加test 4件、個別隔離回帰3件の結果はコード経路と整合しており、追加確認が必要な具体的疑義はなかった。
- 引き継ぎ記載の保存済み3,907件専用replay・confirmへ進めてよい。通常master、S3、後続処理には書き込まないコマンドである。

### 確認結果

1. Python 3.9での原payload bytes復元
   - 実行環境はPython 3.9.23。
   - Python 3.9標準ライブラリは、BytesParserから渡されたbytesを`ascii` + `surrogateescape`で`Message._payload`へ保持する（`email/message.py` 303〜319行）。`get_payload(decode=False)`はsurrogateを原bytesへ戻した後、charset + `replace`で表示用Unicodeへ変換する場合がある（同262〜280行）。
   - 現行実装はidentity encodingだけ`_payload`の保持表現を`ascii` + `surrogateescape`でbytesへ戻し、`decode=True`の`data`と完全一致した場合のみTrueとする。変更前の表示用Unicode再エンコード誤判定を解消しつつ、無条件の検証省略にはなっていない。
2. identity encodingの共通性
   - 分岐はencodingなし、`7bit`、`8bit`、`binary`の4種類を対象にし、message ID・charset・part番号への個別条件はない。
   - 原payloadが文字列でない、surrogateescapeで復元できない、またはbytes不一致の場合はFalseとなる。追加testはUTF-8/8bit、Shift_JIS/8bit、7bit、binary、encodingなしと各負例を確認している。
3. 内容不変と個別隔離
   - parse後recordの組み立て、業務用`message_id`、本文charset strict復号、添付data生成には差分がない。
   - 保存済み限定結果は通常record・本文・添付・業務用IDが67/67一致。selection 67、success 67、needs_review / failed / pending 0、confirm「全件正常」で件数も整合している。
   - strict charset復号不能は`decode_utf8_replace_part_*`、payload検証不能は`*_payload_decode_unverified_*`として当該メールだけneeds_reviewとなる経路を維持。正常+不良の3回帰ケースも各success 1 / needs_review 1で整合している。
4. 既存検証の維持
   - backupとの差分上、base64のvalidate付き再decode比較とquoted-printable比較は変更なし。未知encodingはFalseのまま。
   - raw size/hash、selection ID重複、`selection = success ∪ needs_review`、`master = success`、review記録、正常0件停止の検証部分に差分なし。
   - 現行fetch SHA-256は指定値`66194aec508c07242925248ce6ba054f43be88964a83601c52dadb978c78eac1`と一致。通常master SHA-256は`57593e7c7404a5e752d89eea7c184d01efffafe5faf84abab9e625e9c73ffd06`で不変。
5. 3,907件replay・confirmコマンド
   - sourceは既存専用runのmanifestと3,907 raw、targetは新規`identity-transfer-full-replay-20261007-review/`。`test ! -e`で既存targetへの上書きを防ぐ。
   - rawとmanifestをtargetへコピーした後、fetchは`--manifest` + `--replay`を指定するためS3 clientを生成せず、各保存rawのSHA-256をmanifestと照合する。
   - `--sample-dir`と`--out`はtarget配下で、`--canonical-output`を指定していない。通常master`01_result/fetch_gmail_mail_master.jsonl`は更新しない。
   - confirmもtargetのmaster・manifestだけを入力とし、通常master・後続stepを参照しない。

### レビュー時の実施範囲

- 実施: 変更前backupとの差分、現行hash、Python 3.9標準ライブラリのpayload保持経路、追加test内容、保存済み67件成果物のhash・件数、通常master不変、3,907件replayコマンドの入出力経路を確認。
- 保存済み検証結果を採用し、追加テストは実施していない。
- 未実施: コード変更、S3再取得、3,907件replay、通常master更新、AI、後続処理、全体Pipeline、AWS変更、`_src`同期、commit、push。

## 2026-10-07 保存済み3,907件の専用replay・confirm完了

### 結論

- レビュー完了後、引き継ぎ実行節のコマンドどおり、保存済み3,907 rawを新規専用先へコピーし、レビュー済みfetchによる`--replay`とconfirmを各1回実行した。fetch・confirmはいずれもexit 0。
- selection 3,907、success 3,907、warning_success 1,508、needs_review / failed / pending 0、添付1,677。confirmは`S3 confirm OK (全件正常)`。
- 誤隔離されていた67件はすべてsuccessとなり、前回正常時recordと67/67一致。残り3,840件も直前の検証candidateと3,840/3,840一致し、全3,907 recordが直前candidateと一致した。
- selection/masterのID重複・欠落0、`selection = success`、`master = success`、raw hash・通常schema・保存成果物の整合が成立。通常master hashは実行前後で不変、関連残存プロセス0。
- S3再取得、通常master更新、後続step、AI、全体Pipeline、AWS変更、`_src`同期、commit、pushは実施していない。8bit本文誤隔離の修正・限定検証・全保存raw replayは完了。

### 実行結果

専用成果物:

`01-1_fetch_gmail/02_confirm/s3_daily/2026-10-06/identity-transfer-full-replay-20261007-review/`

| 項目 | 結果 |
|---|---:|
| selection | 3,907 |
| success（警告なし） | 2,399 |
| success（警告あり） | 1,508 |
| needs_review | 0 |
| failed / pending | 0 / 0 |
| 添付 | 1,677 |
| 保存raw | 3,907 |
| selection ID重複 | 0 |
| master ID重複 | 0 |
| selection/master ID欠落 | 0 |
| confirm | exit 0、全件正常 |

fetch出力:

`selection=3907 success=3907 warning_success=1508 needs_review=0 failed=0 pending=0 attachments=1677 result=all_success`

confirm出力:

`S3 confirm OK (全件正常): selected=3907 success=3907 warning_success=1508 needs_review=0 failed=0 pending=0`

### record・集合・raw整合

- 今回masterと直前`impact-partition-validation-20261007-review/parsed_candidate.jsonl`はSHA-256が同一で、全3,907 recordが順序を含めて一致した。
- 直前successだった3,840件: record一致3,840 / 3,840。
- 直前needs_reviewだった67件: 今回record一致67 / 67。さらに昨夜の前回正常時`pipeline/parsed_candidate.jsonl`とも67 / 67一致した。
- 今回manifestはsuccess 3,907だけで、master ID集合とselection ID集合が一致。selection/masterとも重複0。
- fetch replayは各rawのsizeとmanifest記録SHA-256を照合し、confirmは3,907 rawを再hashして合格した。needs_review記録は空ファイルでmanifestのreview集合0と一致。
- 通常record schemaの検証、業務用message_idとS3 key由来identityの一致もconfirmで合格した。

### 実行コード・通常master・プロセス

| 対象 | SHA-256 |
|---|---|
| `01-1_fetch_gmail/00_tool/fetch_s3_mail.py` | `66194aec508c07242925248ce6ba054f43be88964a83601c52dadb978c78eac1` |
| `01-1_fetch_gmail/02_confirm/confirm_fetch_gmail.py` | `0cb8e14be8853df4ed0c0ffef08ae2812e64bb7fea1bd09428f6d9e31d27e838` |
| 通常master（実行前） | `57593e7c7404a5e752d89eea7c184d01efffafe5faf84abab9e625e9c73ffd06` |
| 通常master（実行後） | `57593e7c7404a5e752d89eea7c184d01efffafe5faf84abab9e625e9c73ffd06` |

- 通常master size `269764671`、mtime `2026-09-30 23:21:18.392033990 JST`も不変。
- `fetch_s3_mail.py`、`confirm_fetch_gmail.py`、両`run_full_pipeline`の残存プロセスは0。

### 専用成果物SHA-256

| 成果物 | SHA-256 |
|---|---|
| `selection_manifest.jsonl` | `379d76a356f9ab459344596cb0f679a70c51ca09a0ba8d8a65789e092d79c852` |
| `fetch_gmail_mail_master.jsonl` | `a5e651bff20611102961aecc673031f6edf0ef664c2996063d693c15ddc8946c` |
| `parsed_candidate.jsonl` | `a5e651bff20611102961aecc673031f6edf0ef664c2996063d693c15ddc8946c` |
| `needs_review.jsonl`（空） | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `fetch_summary.jsonl` | `73df4e81f27b1b81696fcd2d36e304803928522c5dd0b61be95043685f23584f` |

### 実施範囲

- 実施: 保存済みmanifest/rawの専用先コピー、レビュー済みfetchのlocal replay 1回、confirm 1回、67/3,840 partition別record比較、ID・集合・成果物hash・通常master不変・残存プロセス確認。
- 未実施: S3再取得、通常master更新、追加修正、追加レビュー、無関係テスト、AI、後続step、全体Pipeline、AWS変更、`_src`同期、commit、push。

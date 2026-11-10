# Manual recovery receipt contract

## Scope

通常managedのvalidator、FAILED拒否、env provenance、execution/redrive検証は維持する。
manual recoveryはCLI引数で明示した場合だけ有効。receipt自動探索・自動fallbackはしない。
元runのstatus、過去80-9 summary、過去80-8 manifestは書き換えない。

業務JSONLの既存スキーマは変更しない。以下の独立した運用証跡を追加する。

## Receipt

保存先: `80-9_portal_s3_sync/01_result/manual_recovery/<recovery_id>/receipt.jsonl`

1行1JSON / UTF-8。ID単位のdirectoryを排他的に作成し、common.json_utilsのatomic writeで確定する。
既存directoryがあるIDは再利用・上書きしない。保存失敗時も同じIDを自動再利用しない。

| Field | 内容 |
| --- | --- |
| `schema_version` | 1 |
| `publication_kind` | `manual_recovery` |
| `recovery_id` | 元run IDと異なる復旧ID |
| `source` | 元execution ARN、run_id、run_date、FAILED、recovery_id |
| `summary_snapshot` | 公開成功80-9 summaryのJSON文字列。cli/env出自を保持 |
| `manifest_snapshot` | 公開に使った80-8 manifestのUTF-8文字列 |
| `failed_status_snapshot` | 元FAILED statusのJSON文字列 |
| `*_sha256` | 上記3snapshotのUTF-8文字列のSHA-256 |
| `current_verification` | 全path/size一致、件数、bytes、差分0、LIST完了 |
| `current_fingerprint`, `bk1_fingerprint` | 全相対pathに対するsize/ETag/UTC LastModified |
| `destination`, `backup_destination` | 固定CURRENT/BK1 URI |
| `verified_at`, `verification_method` | UTC時刻と検証方式v1 |

summary/statusのsnapshotはJSONを再serializeした文字列、manifestは元ファイルの文字列を保存する。
hashはsnapshot改変の検出用で、署名ではない。receiptの保存領域は他の運用証跡と同様に信頼する前提。

検証時はsnapshotのhashと内部整合を検査し、元FAILED statusとexecution履歴のPrepareRunContextを
再取得してrun identityを照合する。redriveは拒否する。
CURRENT/BK1は完全LISTして全fingerprintを再照合する。
空集合、重複key、metadata欠損、継続token欠損/循環、最終ページ未到達は拒否する。

## 9月11日のCLI公開を証跡化する経路

80-75の `--create-manual-receipt` と以下4引数で、S3変更なしの検証・local receipt保存を行う。

- `--manual-recovery-id`: 新規ID
- `--manual-source-run-id`: `sfn-c70bf50a-d519-4121-b204-62ae46097235`
- `--manual-source-run-date`: `20260911`
- `--manual-source-execution-arn`: 当該元executionのARN（履歴と照合必須）

既定の80-9 summary / 80-8 manifestを読み、共有lockとactive runを確認する。
この経路は既存80-75 summaryも変更しない。
receiptには現CURRENT/BK1を検証した結果を保存するため、実際の作成にはその時点のread-only AWS照合が必要。
本実装作業では本番receiptを作成していない。

## Rotation / publication

80-75は `--manual-recovery-receipt <path>` 明示時だけprevious CURRENTとしてreceiptを検証する。
通常managed callerの場合もcurrent env/status/execution guardを必須とする。
receipt確定後に別runのstatusが更新されていた場合は自動判断せず停止する。

tail callerは前述4引数も渡す。継承fd9の共有lock、復旧ID、active execution/status、
元FAILED executionを照合する。全検証後にだけCURRENT → BK1の既存sync/verifyを行う。

80-9は前述4引数の明示時、公開前後のlock/元FAILED検証とBK1不変確認を行い、
公開verify成功後に新しいmanifestとsummaryからreceiptを確定する。
sync失敗、verify失敗、receipt検証・保存失敗は異常終了し、元FAILED statusを変更しない。
公開開始後の失敗にはS3の自動rollbackはない。新しい復旧証跡での再検証が必要。

次の通常managed runでは、この新receiptを80-75へ明示指定できる。
既存runnerに自動検出や一時的なenv偽装を追加していない。

## Pre-publication tail entrypoint

`00_pipeline/00_tool/run_tail_recovery.sh` はmanaged wrapperを使わず、独立して共有lockを取得する。
status writerを継承した起動は拒否する。実行には明示的な `--apply` が必要。

必要な環境変数:

- `RECOVERY_ID`: 新規の復旧run ID
- `RECOVERY_SOURCE_RUN_DATE`: 元FAILED runのRUN_DATE
- `RECOVERY_SOURCE_RUN_ID`: 元FAILED runのRUN_ID
- `RECOVERY_SOURCE_EXECUTION_ARN`: 元FAILED execution ARN

直前CURRENTが通常managed公開なら、検証済み80-9 summary / 80-8 manifestを
`--manual-previous-managed`契約でbaselineにする。直前CURRENTがmanual recovery公開の場合だけ、
`PREVIOUS_MANUAL_RECOVERY_RECEIPT`に検証済みreceipt pathを明示する。

実行順は80-7 → 80-75 → 80-8 → 80-9のみ。08-5、09系、80-7以前は実行しない。
新しい80-8 manifest / summary / ログは
`00_pipeline/01_result/manual_recovery/<recovery_id>/` 配下で分離する。
previous manifestはprevious CURRENTの証跡だけに使い、復旧対象runの公開manifestには流用しない。
既存成果物自体は再生成しないため、復旧対象RUN_DATEの成果物が保持されていることが前提。

## Focused verification

`test_manual_recovery.py` はAWS read-only clientとsyncをfakeに置換し、正常系・改変・欠損・
不完全LIST・通常managedからの明示receipt利用・confirm正常/異常を検証する。
tailテストは `/tmp` のコピーと偽python3を使い、実共有lockの競合とstep選択/停止を確認する。
本番tail、production apply、CURRENT/BK1変更、Step Functions起動は実施しない。

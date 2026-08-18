# explain_high_score_required_skill_recheck

## 目的
- 08-5_high_score_required_skill_recheck
- 08-4 の高スコア帯（`100percent` / `80to99percent`）のペアについて、
  **必須スキル充足の確度をLLMで再判定**する。案件本文（01-4）も参照し、技術領域の一致度も評価する。
- **07-1 の判定結果そのものは書き換えない。** 元レコードを `deepcopy` した上に
  `recheck_info` / `required_skill_checks` / `category_match` / `category_note` / `source_score_band` を付加するだけであり、
  営業可否・単価・商流は判定しない。

## active script
- `08-5_high_score_required_skill_recheck/00_tool/high_score_required_skill_recheck.py`
- 現行runner（`00_pipeline/00_tool/run_full_pipeline.sh` / `run_full_pipeline_master.sh`）から
  引数なしで実行される。`--limit N` は小規模テスト用の任意オプション。

## 入力ファイルと参照方法
- `08-4_match_score_sort/01_result/match_score_sort_100percent.jsonl`（`source_score_band="100percent"`）
- `08-4_match_score_sort/01_result/match_score_sort_80to99percent.jsonl`（`source_score_band="80to99percent"`）
- `04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl`（要員スキルシート本文。`resource_info.message_id` で引く）
- `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`（案件本文。`project_info.message_id` で引く。無くてもエラーにしない）
- 上記いずれかが存在しない場合は `sys.exit(1)` で停止する。

## 出力ファイルと構造
- `01_result/high_score_required_skill_recheck_all.jsonl`（全処理レコード）
- `01_result/high_score_required_skill_recheck_confirmed.jsonl`
- `01_result/high_score_required_skill_recheck_human_review.jsonl`
- `01_result/high_score_required_skill_recheck_not_confirmed.jsonl`
- `01_result/99_error_high_score_required_skill_recheck.jsonl`
- 付加キー: `source_score_band`, `recheck_info`, `required_skill_checks`, `category_match`, `category_note`
- `recheck_info` の内容: `recheck_status` / `model` / `skillsheet_chars_used` / `required_skill_count` /
  `confirmed_count` / `human_review_count` / `not_confirmed_count`
- `recheck_status` は `required_skill_confirmed` / `required_skill_human_review` / `required_skill_not_confirmed` の3値。
  `_decide_recheck_status` が checks の `confidence` から決定し、その値に対応する分類ファイルへ振り分ける。
- `category_match` は `match` / `mismatch` / `unclear` の3値。

## 処理ロジックの詳細
- 入力2ファイルを band 順に読み、1レコードずつ処理する。
- 案件必須スキルは `project_info.required_skills` から取得する（`_required_skills_from_record`）。
- 入力長は上限で切り詰める: スキルシート `RECHECK_SKILLSHEET_MAX_CHARS=10000` / 案件本文 `RECHECK_PROJECT_BODY_MAX_CHARS=3000`。
  実際に使った文字数は `skillsheet_chars_used` に記録する。
- LLM正常応答かつschema検証済みの経路に限り、以下の上書きを適用する（`apply_auto_true_override=True`）。
  - `_apply_auto_true_override`: `common.skill_policy.is_auto_true_skill` に該当するコミュ系・一人称系など
    営業確認前提の非技術スキルを `confirmed` 固定にする（理由は `AUTO_TRUE_RECHECK_REASON`）。07-1 と同じポリシー。
  - `_apply_or_example_override`: OR条件・例示条件のスキルを緩和判定する。
- fallback経路（LLM未呼出・LLM失敗・検証NG）では上記overrideを**適用しない**。

## LLM使用有無と使用箇所
- LLM使用: **有**
- call site: `_process_record` 内の `common.llm_client.call_llm`
- model: **`gpt-4o`**（`RECHECK_LLM_MODEL`）。他stepの `gpt-4o-mini` とは異なる
- feature flag: **なし**。無効化手段は存在せず、条件を満たすレコードは必ずLLMを呼ぶ
- 呼び出しパラメータ: `temperature=0.0` / `max_tokens=4096` / `max_retries=3` /
  `response_schema=_build_schema(required_skills)`（必須スキルごとの判定枠を動的生成）

### LLMを呼ぶ条件
1レコード1回。ただし以下は**LLMを呼ばずに**確定させる。

1. `status == "no_match"` のレコード → `skipped_no_match` としてカウントし、出力もしない（`_is_no_match_record`）
2. 04-1 に該当 `resource_info.message_id` が無い → `missing_resource_skillsheet`
3. スキルシートの `success=false` または本文が空 → `missing_resource_skillsheet`

2・3 は `_fallback_checks` で全スキャンを `human_review` にして結果を出力し、同時にエラーレコードを残す。

### response validation
`_validate_required_skill_checks` が LLM応答の `required_skill_checks` を検証する。
以下のいずれかを満たさなければ `invalid_output_schema` として扱い、1件でもNGならレコード全体をfallbackにする。

- `required_skill_checks` が list であること
- 件数が入力の必須スキル件数と一致すること
- 各要素が dict であること
- 各要素の `skill` が入力スキル文字列（`_skill_text`）と**完全一致**すること（キーを変えない前提の担保）
- `confidence` が `VALID_CONFIDENCES`（`confirmed` / `human_review` / `not_confirmed`）のいずれかであること
- `reason` が空文字・null でない文字列であること

検証通過時は以下のキー構造へ正規化する（`evidence` は null を空文字に寄せる）:
`skill` / `original_match`（入力の `match is True`）/ `recheck_match`（`confidence != "not_confirmed"`）/
`confidence` / `reason` / `evidence`。

`_extract_category_fields` は `category_match` を小文字化して `VALID_CATEGORY_MATCHES`
（`match` / `mismatch` / `unclear`）で正規化し、範囲外は `unclear` に寄せる。
`category_note` が空の場合は既定値 `"判定不明"` を入れ、nullで潰さない。

### retry
- `call_llm` 側で `max_retries=3`、リトライ間隔は既定 `retry_wait_seconds=5.0`。
- 全リトライ失敗時は `RuntimeError`、`finish_reason=length` の途中終了は `LLMOutputTruncatedError`（`ValueError` 系）。

### error handling
例外は握りつぶさない。record単位で捕捉される想定エラーは
**error記録 → `human_review` 等へfallback → 他recordの処理を継続**する。
一方、record単位の `try` の外側で発生する異常（入出力ファイルのI/O失敗、JSONL読込時のstep-level異常、
出力初期化失敗、結果書込み失敗など）では**step自体が停止し得る**。
「すべてのエラーで必ず処理継続する」わけではない。

| error_type | 発生条件 |
|---|---|
| `missing_resource_skillsheet` | 04-1にデータなし / `success=false` / 本文空（LLM未呼出） |
| `llm_parse_error` | `call_llm` が `ValueError`（出力途中終了・JSON不正など） |
| `llm_call_error` | `call_llm` がその他例外（API失敗・全リトライ失敗など） |
| `invalid_output_schema` | LLM応答が `_validate_required_skill_checks` を通らない |
| `unexpected_error` | `_process_record` 全体の想定外例外（`main` で捕捉） |

### human_reviewへのフォールバック
上記いずれのエラーでも `_fallback_checks` で全必須スキルを `confidence=human_review` にし、
理由文（「スキルシート欠落のため人間確認」「LLM出力不正のため人間確認」等）を入れて出力する。
**判定不能データを捨てず、人間確認が必要と分かる形で残す**設計。

## downstreamとの関係
- `09-3_prepare_sales_mail_context` が `confirmed` / `human_review` の2ファイルのみを採用候補として読む。
  `not_confirmed` および 08-5 の結果に存在しないペアは営業候補から除外される。
- `category_match=mismatch` は `09-4_remove_category_mismatch_sales_candidates` の除外判断に使われる。

## エラー時の挙動
- 入力ファイル欠落時は `logger.error` の上 `sys.exit(1)` で**停止**。
- record単位で捕捉される想定エラー（`missing_resource_skillsheet` / `llm_parse_error` /
  `llm_call_error` / `invalid_output_schema` / `unexpected_error`）は上表の `error_type` で
  `99_error_*.jsonl` に**記録し、当該recordをfallbackで出力して次のrecordへ継続**。
- record単位の `try` の外側（I/O・JSONL読込・出力初期化・結果書込みなど）で発生した例外は
  捕捉されないため、**step自体が停止し得る**。
- 実行時間と件数は `write_execution_time` で `99_execution_time/` に残す。

## 注意事項
- **冪等性**: 出力5ファイルは `_init_output_files` で実行開始時に初期化してから追記するため、
  同一入力での再実行結果は同じファイル構成になる。ただしLLM応答は完全な決定性が保証されないため
  （`temperature=0.0` でも同一とは限らない）、`confidence` 単位で前回と完全一致する保証はない。
- **再実行耐性**: 途中失敗時は途中までの出力が残る。再実行すれば先頭から作り直されるため、
  部分結果を手で継ぎ足さないこと。件数を絞る場合は `--limit` を使う。
- **コスト・実行時間**: flagがなく全高スコアペアに対して `gpt-4o` を1件1回呼ぶため、本stepは
  Pipeline中でも重い。件数増加時は実行時間とコストが線形に増える点に留意する。
- JSONL前提の後続処理との整合に注意が必要です。
- `message_id` を主キーとする処理との整合が必要です。
- 実装根拠はこのPythonファイル本体を優先してください。

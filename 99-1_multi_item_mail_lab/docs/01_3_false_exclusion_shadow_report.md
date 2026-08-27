# 99-1 01-3 False Exclusion Shadow Report

Observation date: 2026-08-27 (latest saved actual, offline only)

## Result

- implementation: PASS
- mail master records: 2,913
- 01-3 shadow input records: 2,667
- current excluded: 80
- shadow excluded: 1
- rescued: 79
- newly excluded: 0
- shadow REVIEW: 0
- production 01-3 changes/write: 0 / 0
- external access: 0

The observed counts are replay results, not fixed-count test oracles. The confirm checks
partition integrity, replay equivalence with the saved current result, evidence-based
decision invariants, parser safety, and non-mutation.

## 1. Current 01-3 root cause

All 80 current exclusions were reproduced from the saved 01-3 input and result. There
were no mail-master misses and the computed excluded ID set exactly matched the saved
removed-ID set.

- From-only: 73
- From+Subject: 7
- Subject detector: 0

The root cause is the manual selector being used as the exclusion decision. It removes
single PROJECT/RESOURCE mails as well as multi-item and remote-artifact mails before
their processing value can be evaluated.

## 2. Shadow decision model

The shadow candidate has three decisions.

- `EXCLUDE`: sender, anchored Subject, and body format jointly prove a known non-sales
  notification.
- `KEEP`: an existing 99-1 route, a remote artifact, a processable attachment, or
  PROJECT/RESOURCE sales structure gives downstream processing value.
- `REVIEW`: a current exclusion hit has neither strong exclusion evidence nor positive
  processing evidence.

Current 01-3 survivors are preserved. Unknown current exclusion hits are never defaulted
to `EXCLUDE`.

## 3. From-only handling

The 73 From-only hits were re-evaluated from their mail content and artifacts. All 73
had positive downstream evidence and were rescued; From-only exclusions remaining in
the shadow result are 0.

The From value remains usable as a source selector, but it is not sufficient exclusion
evidence.

## 4. From+Subject handling

Of seven From+Subject hits, five NetWisdom two-resource mails and one Identity web-list
mail were rescued. The remaining one is a Google Apps Script failure notification that
matched a source-specific sender, anchored Subject, and failure-notification body format.

- From+Subject current: 7
- From+Subject shadow exclusions: 1
- From+Subject rescued: 6

## 5. Sender-specific observation

| Sender | Current hits | Observed content | Shadow result |
|---|---:|---|---|
| `dss@d-standing.co.jp` | 48 | 47 single PROJECT formats, 1 Google Sheet project list | KEEP 48 |
| `partner@lfield.co.jp` | 10 | single PROJECT/RESOURCE; six mails have XLSX | KEEP 10 |
| `r-hattori@legarea.jp` | 6 | single RESOURCE with remote skill-sheet link | KEEP 6 |
| `inoue@netwisdom.co.jp` | 5 | two-resource INLINE format with two mapped XLSX files | KEEP 5 |
| `y.ogawa@1-r.co.jp` | 5 | single PROJECT/RESOURCE plus Google Sheet URL | KEEP 5 |
| `ayana.yamamoto@sakya.jp` | 3 | two single PROJECT mails and one multi-record XLSX | KEEP 3 |
| `yk-nishimura@unite-neo.co.jp` | 1 | single RESOURCE with XLSX | KEEP 1 |
| `bp@id-entity.jp` | 1 | remote project-list web link | KEEP 1 |
| `noreply-apps-scripts-notifications@google.com` | 1 | Apps Script failure notification | EXCLUDE 1 |

## 6. Rescued singles

The shadow observation classified 71 rescued records as `LIKELY_VALID_SINGLE`. The
saved 02-1 rule function was called directly in offline mode; the 02-1 production step
was not executed and the LLM feature flag was required to be OFF.

- PROJECT: 58
- RESOURCE: 9
- AMBIGUOUS: 4
- UNKNOWN: 0
- classification output shape: 71 / 71

AMBIGUOUS remains a downstream classification result and is not silently discarded.

## 7. Rescued multis and 99-1 reachability

Eight rescued records were observed as `LIKELY_VALID_MULTI`.

- NetWisdom INLINE: 5 mails; all `PARSED`, two items per mail
- Sakya SPREADSHEET: 1 mail; safely reached the parser as `HUMAN_REVIEW`, with 108
  technical candidates and no automatic eligibility
- D-Standing Google Sheet project list: 1 mail; retained as a 99-2 remote candidate
- Identity web project list: 1 mail; retained as a 99-2 remote candidate

For the six rescued records applicable to existing 99-1 parsers, safe reach was 6 / 6,
parser crash was 0, and `SYSTEM_FAILURE` was 0. The Sakya `HUMAN_REVIEW` is the existing
safe status for incomplete source-owned manifest/acquisition evidence, not a parser
regression.

## 8. 99-2 remote candidates

Thirteen rescued records had explicit remote-artifact evidence and were retained without
accessing the URLs.

- remote skill-sheet link: 6
- Google Sheet RESOURCE/PROJECT link: 6
- project-list web link: 1
- external access: 0
- 99-2 processing: 0

## 9. False-keep and false-exclude check

- `CLEAR_EXCLUDE` preserved: 1 / 1
- newly excluded: 0
- new false-exclude suspected: 0
- new false-keep suspected: 0
- REVIEW: 0

The only clear non-sales notification remains excluded. The shadow rule does not turn
unknown evidence into exclusion.

## 10. Regression and integrity

- new focused tests: 11 / 11
- existing 99-1 stable tests: 209 / 209
- actual shadow confirm: PASS
- current replay equals saved 01-3 removed IDs: true
- rescued original message ID collision: 0
- mail-master missing records: 0
- parser crash / SYSTEM_FAILURE: 0 / 0

The Google Sheet acquisition Prototype tests and files were not changed or executed as
part of this issue.

## 11. Production non-change

No production code, exclude list, or result was changed. The replay digested the
production 01-3 tree before and after offline evaluation and observed no change. The
shadow writes only generated Lab artifacts under
`99-1_multi_item_mail_lab/01_result/01_3_false_exclusion_shadow/` and Lab execution time.

This report is evidence for review only. It does not apply the shadow candidate to
production 01-3.

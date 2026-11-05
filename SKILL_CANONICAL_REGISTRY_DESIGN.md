# Skill Canonical Registry Design

Status: Revised after independent review
Scope: 03-8 / 05-8 skill extraction, primary skill classification, 06-8, 06-11
Non-scope: skill候補の追加判断、既存61 groupの一括移行、matching条件の緩和

## 1. 目的と設計原則

skillのcanonical名、category、surface alias、match方法を宣言的registryへ集約する。
目標状態ではregistryをskill表記管理のsingle source of truthとし、Pythonコードへ製品名ごとのmap、regex、if分岐を追加しない。

設計原則は以下とする。

- canonicalごとにcategoryは1つだけ持つ。
- canonical自身も検索surfaceとして自動的に扱い、`aliases`へ重複記載しない。
- extraction、primary判定、06-8、06-11は同じregistry loaderと正規化規則を使う。
- `matched_surface`は内部情報として保持し、canonical化後も原文上の根拠を失わない。
- 既存JSONL schemaは変更しない。内部hit情報や比較結果はconfirm用途に限定する。
- regexそのものはregistryへ記述させず、小さな`match_mode` enumだけを提供する。
- 61 groupは一括移行しない。最初は既存4 canonicalとPower BIだけを対象にする。
- 移行はshadow比較、限定有効化、rollbackの順に小さく行う。

## 2. 現状と解消対象

現状は次の責務が分散している。

- 03-8 / 05-8の`skill_dictionary.txt`: surfaceとcategory
- `common/skill_canonical_map.py`の`SKILL_CANONICAL_MAP`: extraction後のcanonical変換
- 同ファイルの`SKILL_ALIAS_MAP`: 06-11本文検索用alias
- 03-8 `classify_primary_skills`: canonical文字列をrequired原文へ再検索
- 06-8: extraction結果の完全一致比較
- 06-11: 独自alias展開によるrequired本文検索

特に、`PowerBI`を`Power BI`へcanonical化するだけでは、primary判定がrequired原文中の`PowerBI`を見失う。
また`PowerPlatform`、`PowerAutomate`、`Entra ID`等はsurfaceによってcategoryが変わり得る。

## 3. 正本と配置

### 3.1 目標状態

正本候補は次とする。

`common/skill_registry.yaml`

このファイルが以下の唯一の正本になる。

- canonical skill名
- canonical category
- 検索対象alias
- match mode
- case sensitivity
- enabled状態
- group単位のownership（`production` / `shadow` / `legacy`）
- 任意の運用note/source

03-8 / 05-8は同じ絶対パスへ解決されたregistryを読み、別コピーを持たない。

### 3.2 移行期間

初回migrationでは全928 entryを移さないため、一時的に次のbridgeを許容する。

- registry登録済みであることはproduction適用を意味しない。
- `ownership: production`のcanonicalとsurfaceだけ、production判定でregistryが権威を持つ。
- `ownership: shadow`のcanonicalとsurfaceは比較・監査だけに使い、production出力はlegacyを維持する。
- `ownership: legacy`はregistry登録を残したままproduction/shadow判定へ使わない。
- registryに存在しないskill: legacy `skill_dictionary.txt`が権威を持つ。
- production ownership surfaceとlegacy辞書の同一surface重複は、移行に必要な`expected override`として許可する。
- production ownership surfaceだけをlegacy matcher側から除外し、二重matchさせない。shadow/legacy ownership surfaceはproduction上のlegacy matcherへ残す。
- 03-8 / 05-8のlegacy辞書は移行期間中もbyte-identicalを必須とする。
- 同じ正規化surfaceを複数registry canonicalが所有する場合、またはregistryが所有していないlegacy surfaceが別canonical/categoryと衝突する場合は起動時validation errorとする。

matcherはregistry用とlegacy用に分けて本文を二回走査しない。
production ownership surfaceと、それ以外のlegacy production surfaceを1つのeffective surface tableへ統合し、全体を同じ優先順位でsortして一度だけmatchする。shadow matcherは比較用に分離し、production outputへ混入させない。
expected overrideのsurface、旧canonical、旧category、新canonical、新categoryはshadow confirmへ記録する。

bridgeは移行専用であり、全entry移行後にlegacy dictionaryとPython mapを廃止する。
移行完了前に既存方式を削除してはならない。

### 3.3 Run単位のmodeとregistry snapshot

modeの正本は共通execution config 1か所だけに置く。03-8 / 05-8 / 06-8 / 06-11が個別configでmodeを持つことを禁止する。

Pipelineまたはselective run開始時に、次を含むimmutable execution contextを1回だけ確定する。

- `skill_registry_mode`
- `required_search_mode`
- `schema_version`
- registry bytesのSHA-256
- immutable registry snapshotのpath
- context生成時刻とrun identity
- canonicalごとのownership snapshot

各stepは元のregistry fileを個別に再読込せず、execution contextが指すimmutable snapshotを読む。
producer stepは利用したfingerprintをsidecar/confirmへ記録し、consumer stepは入力成果物のfingerprintが現在のexecution contextと一致することを確認する。
JSONL本体のschemaは変更しない。

fingerprintの欠落・不一致、run途中のmode変更、snapshot hash不一致はfail closedとする。
手動selective testでも明示的なexecution contextを必須とし、暗黙defaultでstepごとに異なるmodeを選ばない。

`legacy` modeはregistry fileの存在・parse・validationに依存せず、legacy専用fingerprintを用いる。

## 4. Registry schema

初期schemaは次を想定する。

```yaml
schema_version: 2

allowed_categories:
  - バックエンド
  - DB
  - データ分析

skills:
  - canonical: Power BI
    ownership: production
    category: データ分析
    aliases:
      - PowerBI
      - Power-BI
    match_mode: boundary
    case_sensitive: false
    enabled: true
    source: DICT-02 / CHECK-07
    note: Microsoft Power BIの表記差
```

実装時は現行32 categoryを`allowed_categories`へすべて列挙する。
初回migration対象外のcategoryも、将来の追加時にPython変更が不要となるよう最初から登録する。

### 4.1 必須field

| field | type | 制約 |
|---|---|---|
| `canonical` | string | 非空。NFKC後の前後空白なし。registry内で一意 |
| `ownership` | enum | `production` / `shadow` / `legacy`。canonicalごとに1値だけ必須 |
| `category` | string | `allowed_categories`のいずれか。canonical単位で一意 |
| `aliases` | list[string] | 空list可。canonical自身を含めない。重複不可 |
| `match_mode` | enum | `boundary` / `phrase` / `exact` |
| `case_sensitive` | bool | 原則false。省略不可 |
| `enabled` | bool | 省略不可。falseでもvalidation対象 |

### 4.2 任意field

| field | type | 用途 |
|---|---|---|
| `source` | string | 採用根拠、確認unit、issue等 |
| `note` | string | 衝突注意、製品rename等の短い説明 |

aliasごとのregex、優先度、任意Python関数はschemaへ入れない。
同一canonical内でaliasごとにmatch方法を変える必要が生じた場合は、実例とimpactを確認してschema versionを見直す。

`source`は採用根拠を短く示す識別情報とし、判定ロジックには使わない。
`note`も説明専用であり、matcherの分岐条件にしてはならない。
後方互換性のないfield・意味変更では`schema_version`を上げ、loaderは対応version以外をfail closedにする。

### 4.3 match_mode

registry loaderは最初にNFKC正規化し、`case_sensitive=false`の場合はcasefoldして比較する。

- `boundary`: 現行英数字skill相当。surface前後が`[A-Za-z0-9.-]`でない場合だけmatchする。
- `phrase`: escape済みliteral phraseを本文内検索する。日本語・混在語・空白を含む句に使う。
- `exact`: CRLF/LFを統一して行分割し、各行の前後空白をtrimした非空行全体がsurfaceと一致する場合だけmatchする。空行はmatch対象外。短く衝突しやすい語向けに予約する。

`case-insensitive`はmatch modeではなく`case_sensitive: false`で表す。
registryにraw regexを保存しない。

### 4.4 06-11移行互換

現行06-11はaliasの単純substring検索であり、extractionのboundary検索と意味が異なる。
この差をcanonical registry導入と同時に変更しない。

移行contextは次を持つ。

- `required_search_mode: legacy_substring`: alias listはregistryから取得するが、06-11の検索意味は現行substringを維持する。
- `required_search_mode: registry`: registryの`match_mode`を06-11にも適用する目標状態。

初回M1は`legacy_substring`を使用する。
既存4 groupで06-11差分0を確認後、boundary等への変更は別の仕様変更・impact reviewとして扱う。
`Spring Boot`と本文`SpringBootX`のようなsubstring/boundary差をpositive/negative testへ含める。
どちらのmodeでもalias表記自体はregistryから取得し、06-11に個別alias定義を残さない。

## 5. 共通Python責務

共通moduleは個別製品名を持たず、次だけを担当する。

1. registry loadとschema version確認
2. validation
3. NFKC / case normalization
4. match modeに応じたliteral surface pattern生成
5. 全canonical横断の長いsurface優先match
6. `SkillHit`生成
7. canonical単位のduplicate suppression
8. category付与
9. legacy bridge中のsurface ownership確認
10. group ownershipに基づくproduction/shadow matcher分離

内部表現の例:

```text
SkillHit(
    canonical="Power BI",
    category="データ分析",
    matched_surface="PowerBI",
    start=120,
    end=127,
    match_mode="boundary",
)
```

重複抑止規則:

- 全surfaceを正規化後の文字数降順でmatchする。
- 長いsurfaceの完全包含範囲を短いsurfaceで再採用しない。
- 同一canonicalが複数箇所でhitした場合、出力`skills`には最初の出現位置の1件だけを残す。
- `matched_surface`とspanは内部hit一覧では保持する。
- 出力順はcanonicalの最初の原文出現位置順とする。
- categoryは原文surfaceや辞書順ではなくregistryのcanonical categoryを必ず使う。

## 6. 03-8 / 05-8

03-8 / 05-8は共通registry loaderとsurface matcherを使う。
project/resourceで別registryや別alias listを持たない。
production outputにはproduction ownership groupだけをoverlayし、shadow ownership groupはlegacy outputとregistry比較を分離して保持する。

共通処理:

- bodyから`SkillHit`一覧を抽出
- canonical順序保持dedup
- `skills`をcanonical名listとして生成
- `skills_by_category`をregistry categoryから生成
- `skills_raw`は既存schema・既存生成規則を維持

project固有処理:

- 03-50のrequired skill text群に同じsurface matcherを適用
- primary canonical集合を作る
- body抽出canonicalとの積集合をbody出現順で`primary_skills`へ出力

resource固有処理は現状どおりprimary分類を持たない。

03-8 / 05-8の入出力JSONL keyは変更しない。

## 7. Primary skill判定

canonical文字列をrequired原文へsubstring検索する現方式は廃止する。

新方式:

1. body抽出で`canonical`と`matched_surface`を持つ`SkillHit`を生成する。
2. 03-50 `required_skills[].skill`の各textにも同じregistry matcherを適用する。
3. required側hitをcanonical集合へ変換する。
4. body側canonicalとの積集合をbody出現順で`primary_skills`にする。
5. required textが空の場合だけ、既存仕様どおり全skillsをprimaryへfallbackする。

例:

```text
body hit:     canonical=Power BI, matched_surface=PowerBI
required hit: canonical=Power BI, matched_surface=PowerBI
primary:      Power BI
```

この処理では`matched_surface`を内部監査情報として保持するが、既存JSONL schemaへ新keyは追加しない。
confirmでは対象recordについてcanonical、body surface、required surfaceを表示できるようにする。

## 8. 06-8 / 06-11

### 8.1 06-8

06-8はcanonical skill同士だけを比較する。

- 03-8 / 05-8がregistry canonicalを出力することを基本契約とする。
- migration bridge中は入力値を共通registryの`canonicalize_surface()`へ通してidempotentに正規化する。
- 06-8独自alias、製品名if、category補正を持たない。
- `primary_skills`がある場合の既存優先規則は変えない。

### 8.2 06-11

06-11は`SKILL_ALIAS_MAP`を直接参照しない。

- 03-51のrequired keywordを共通registryでcanonical解決する。
- 目標状態ではresource email / canonical skillsheet本文を同じregistry surface matcherで検索する。
- match結果にはrequired側canonical名を返す。
- strong/weak phase、same-source、04-2 canonical input、multi-person veto等の既存規則は変えない。
- migration bridge中はlegacy required keywordもregistryまたはlegacy辞書で解決できることを必須とする。

初回M1だけは4.4の`required_search_mode: legacy_substring`を使い、surface一覧の正本だけをregistryへ移す。
検索意味をregistry `match_mode`へ統一する変更は、既存4 groupの互換確認後に別unitで行う。

目標状態ではextraction、primary、06-8、06-11のいずれにもstep固有alias定義を残さない。

## 9. Validation

registry load時とFocused Testで、最低限次をfail closedで検査する。

1. `schema_version`不正
2. canonical空文字・型不正
3. canonical重複
4. alias空文字・型不正
5. 同一entry内alias重複
6. canonical自身のaliases内重複
7. aliasが複数canonicalに所属
8. canonicalが別canonicalのaliasとして登録される競合
9. NFKC + casefold後のcase-insensitive collision
10. category欠落
11. `allowed_categories`外category
12. 不正`match_mode`
13. `enabled` / `case_sensitive`の型不正
14. production ownership surfaceとlegacy dictionaryのownership分類。expected overrideは許可し、未宣言競合だけ拒否
15. 03-8 / 05-8が同一registry path・同一hashを参照していること
16. 同一canonicalが複数categoryへ現れないこと
17. YAML duplicate key
18. `allowed_categories`の空値・重複・型不正
19. 任意fieldを含む全fieldの型不正・未知field
20. execution contextとsnapshotのmode/schema/hash不一致
21. 不正ownership mode、ownership field重複、ownership欠落
22. execution contextとsnapshotのownership map不一致

aliasはcanonical参照ではなく単なるsurface stringなので、alias cycleはschema上作れない。
canonicalを他entryのaliasへ登録するケースをcycle相当の競合として禁止する。
`case_sensitive: false`のentryでは、同一canonical内であってもNFKC + casefold後に同じになるaliasを複数登録してはならない。
case-equivalentなlegacy表記はaliasを重複登録せず、1つのregistry surfaceがexpected overrideとしてすべて吸収する。

validation error時はstepを開始せず、対象canonical/surface/categoryを明示して停止する。
黙ってaliasを無視したりlegacyへfallbackしてはならない。
ただし`legacy` modeはregistry validationを実行せず、registry破損時にもrollback経路を維持する。

YAML loaderはduplicate keyを拒否するloaderを使用する。
load完了後はentry順とalias順を保持したimmutable snapshotを作り、そのbytesからhashを計算する。
surface sortは正規化後の長さ降順、registry/legacy ownership、entry宣言順、alias宣言順の順でstableに決定する。
同じ入力とsnapshotから必ず同じpattern順・出力順になることをtestする。

## 10. Focused Test設計

### 10.1 Registry単体

- valid registry load
- 各validation failure
- NFKC、case sensitivity、各match mode
- case-insensitiveな同一entry内alias collisionを拒否
- 長いsurface優先とspan containment
- canonical単位dedupと出現順
- category一意性
- disabled entryがmatchされないこと
- YAML duplicate key、allowed category重複、未知fieldを拒否
- snapshot/hashとstable tie-breakの決定性

### 10.2 Extraction共通性

- 同一本文に対する03-8 / 05-8のskill/category結果が同じ
- `PowerBI`、`Power BI`、`Power-BI`が`Power BI`になる
- `Aurora PostgreSQL`が`Amazon Aurora + PostgreSQL`になり重複しない
- alias表記によってcategoryが変わらない

### 10.3 Primary

- required `PowerBI`でbody `PowerBI`をprimary `Power BI`に保持
- required `Power BI`でbody `PowerBI`をprimary `Power BI`に保持
- optionalだけのsurfaceはprimaryへ入らない
- required空時の全skill fallbackを維持

### 10.4 06-8 / 06-11

- canonical表記差だけのpairが06-8で一致
- 06-11がemailと04-2 skillsheet双方でalias一致
- strong phaseのsame-source条件を維持
- 既存noise filter、skillsheet vetoを迂回しない
- `legacy_substring`では既存4 groupの06-11結果が変わらない
- `registry` modeではboundary差を明示したpositive/negative testが通る
- `exact` modeで前後空白trim、CRLF正規化、空行非matchの扱いが固定されている

## 11. 最小migration unit

初回registry entryは次の5 canonicalに限定する。

| canonical | category | aliases | match_mode | case_sensitive |
|---|---|---|---|---|
| Spring Boot | バックエンド | Springboot | boundary | false |
| Node.js | バックエンド | NodeJS | boundary | false |
| SQL Server | DB | SQLServer, SQL-Server | boundary | false |
| Amazon Aurora | DB | Aurora | boundary | false |
| Power BI | データ分析 | PowerBI, Power-BI | boundary | false |

現行4 groupは`SKILL_CANONICAL_MAP`と`SKILL_ALIAS_MAP`の検索意味を維持して移す。
casefold後に重複するaliasはregistryへ1表記だけ登録し、例えばlegacyの`SpringBoot`は`Springboot`とcase-equivalentなexpected overrideとして吸収する。
既存4 groupの挙動差分0を必須とする。
Power BIだけを新規改善対象とする。

61 groupの残りは初回migrationへ含めない。
追加groupは1テーマ単位で実メールimpactとFocused Testを確認して移す。

## 12. Migration手順

### M0: validation / shadow

- registry schema、共通loader、validator、Focused Testを追加する。
- production出力はlegacyのままにする。
- 5 groupだけregistryでshadow計算し、legacyとの差をconfirmへ出す。
- canonical artifactは変更しない。
- run開始時にimmutable snapshotとfingerprintを生成し、全対象stepで同一値を確認する。
- legacyとregistryのcompile時間・全件処理時間・peak memoryを比較する。matcher compileはprocessあたり1回とし、recordごとの再compileを禁止する。
- 性能差が運用許容範囲を超えた場合はM1へ進まず、原因と基準値をreviewする。
- Power BIの許容差分をrecord ID、pair ID、旧値、新値、差分理由でconfirm manifestへ固定する。
- `PowerBI`由来と`Power-BI`由来を別理由として集計する。

### M1: registry overlay

- feature flagまたは明示modeでregistry overlayを有効化する。
- 初回production ownershipはPower BIだけとし、Spring Boot / Node.js / SQL Server / Amazon Auroraはshadow ownershipを維持する。
- production ownership surfaceだけをlegacy matcherから除外する。
- その他skillはlegacy辞書を使用する。
- 03-8 / 05-8 / primary / 06-8 / 06-11を同じmodeで切り替える。stepごとの部分切替は禁止する。
- `required_search_mode`は`legacy_substring`とし、06-11検索意味の変更を混ぜない。
- consumer stepはproducer成果物のfingerprint一致を検証してから処理する。
- 06-8から06-30までの中継step（06-9、06-10、06-12を含む）は、pair JSONLを変えずにsidecarへ同じfingerprintを伝播する。各consumerは直前producer sidecarとrun contextの双方へ一致することを確認する。
- confirm manifestの許容ID以外に差分が1件でもあれば停止する。件数だけの一致では通過させない。
- group昇格はgroupごとのconfirmation unitで承認し、一括昇格を禁止する。

### M2: 安定確認

- 固定run全件をメモリ上で比較する。
- Focused Test、confirm、pair差分、complete-unmatchを確認する。
- 問題がなければ次のcanonical groupを別unitで提案する。

全928 entryの一括変換や、legacy mapの即時削除は行わない。

## 13. Compatibility / acceptance criteria

固定runで旧方式とregistry方式を同じ入力に対して比較する。

比較項目:

- 03-8 / 05-8 input/output件数、message_id、schema
- `skills`、`skills_by_category`、`primary_skills`、order
- null/empty件数
- 06-8 pass/rejectとmatched skill
- 06-11 pass/reject、matched keyword、matched source
- 06-30最終candidate
- complete-unmatch project/resource
- pairのreject→pass / pass→reject

初回migrationのacceptance:

- Spring Boot / Node.js / SQL Server / Amazon Auroraの差分0
- Power BI以外のcanonical/category/primary差分0
- Power BI対象でprimary skill消失0
- 06-8 pass→reject 0
- 06-11 pass→reject 0
- 意図しないfinal candidate差分0
- false positive疑義0
- 03-8 / 05-8は同じregistry hashを使用
- 03-8 / 05-8、および06-8→06-9→06-10→06-11→06-12→06-30の全stepでmode/schema/hash fingerprintが一致
- Power BIのrecord/pair差分が承認済みconfirm manifestと完全一致
- production ownership surfaceとlegacy expected override以外のownership競合0

CHECK-07事前probeのPower BI差分（06-8 reject→pass 5、06-11 reject→pass 1、最終candidate 1）は比較基準とするが、実装後の固定run実測を正とする。

## 14. Rollback

移行modeは次の3値とする。

- `legacy`: registryを一切load/validateせず、現行dictionary + Python mapを使用
- `shadow`: 出力はlegacy、registry結果はconfirm比較だけ
- `registry_overlay`: `ownership: production`のgroupだけregistry、shadow/legacy groupと未登録skillはlegacy

mode変更だけではrollback完了とみなさない。
`registry_overlay`で生成済みの03-8 / 05-8成果物にはregistry canonicalが残るため、rollbackではoverlay成果物をcurrent inputとして再利用しない。

rollback手順:

1. 新しい共通execution contextを`legacy` modeで生成する。
2. overlay fingerprintの成果物を無効扱いにする。削除ではなく、current selectionから外す。
3. 03-8 / 05-8から06-30までの影響chainをlegacy modeで再実行する。
4. 各stepのconfirmとfingerprintを確認する。
5. rollback前legacy baselineと件数、canonical値、pair差分を比較する。

rollback完了条件:

- 全対象stepが同じlegacy fingerprint
- 03-8 / 05-8 input/output件数・message_id・schema一致
- overlay fingerprintを持つ成果物がcurrent selectionへ残っていない
- Power BI等のoverlay限定差分が残っておらず、legacy baselineとcanonical/category/primaryが完全一致
- baselineに対する意図しないpair差分0
- confirm error 0

registry導入中はlegacy dictionary、`SKILL_CANONICAL_MAP`、`SKILL_ALIAS_MAP`を削除・破壊変更しない。
部分stepだけlegacyへ戻してcanonical表記を混在させてはならない。

出力schemaを変えないため、rollback時にJSONL変換は不要とする。

## 15. 運用

目標状態のskill追加手順:

1. registry 1 entryを追加または既存entryへaliasを追加
2. registry validation
3. canonical/category/alias Focused Test
4. 対象runのproject/resource hitと限定downstream impact確認
5. confirm

通常のskill/alias追加でPython変更を要求してはならない。
Python変更が必要な場合は、新しいmatch semanticsが本当に必要かを設計reviewする。

日次終了時は次だけ軽く確認する。

- 同じcanonicalの別entryを作っていないか
- aliasが既存canonicalと衝突していないか
- product familyと個別productを誤統合していないか
- categoryが既存canonicalと一致しているか
- 個別aliasをPythonへ追加していないか

## 16. Technical Debt方針との整合

本設計は`pipeline-improvement-guide/TECHNICAL_DEBT.md`の次の方針に対応する。

- 表記揺れ・製品名を設定へ分離
- Pythonには判定ロジックを残す
- 同種regex/ifの重複防止
- 日次中の大規模refactor禁止
- Focused Testを維持できる小単位migration

この設計はPipeline全体の一括refactorではない。
対象はskill canonical管理に限定し、5 groupずつ安全に移行する。

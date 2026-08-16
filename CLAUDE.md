@AGENTS.md

# CLAUDE.md

Pipeline共通の不変ルールは上記 `AGENTS.md` が正本。
このファイルには **Claude Code固有の事項のみ** を書く。

---

## 応答ルール

`.claude/rules/10-response-budget.md` に従う（結論先出し、全文貼り付け禁止、代表3件）。

---

## Skill

Skill実体は `.agents/skills/`。`.claude/skills/` はそこへのsymlink。

```
/step-implementation   調査 → 最小実装 → focused test → confirm → 結果確認
/pipeline-review       通常レビュー（strict で厳しめ）
/pipeline-sync-git     選択同期 → diff → add → commit → push
```

通常の作業はSkillを介さず自然文でそのまま依頼してよい。
使い方は `docs/agent-usage-guide.md` を参照。

---

## Permission

| ファイル | 役割 |
|---|---|
| `.claude/settings.json` | Pipeline共通のPermission（allow / ask / deny / additionalDirectories） |
| `.claude/settings.local.json` | 個人・環境依存（model / defaultMode など）のみ |

共通Permissionを `settings.local.json` に書かない（二重管理禁止）。

- **自走してよい**: workspace内の調査・編集、可逆なファイル操作（mkdir / touch / cp / mv）、
  Python実行、focused test、pytest、confirm、正規sync/git処理、Git read-only、通常commit/push
- **人間確認 / 禁止**: rm系、force push、reset --hard、git clean、
  AWS変更操作、production設定変更、秘密情報取得、復旧困難な操作

---

## Subagent

大規模探索・独立した並列作業が必要なときのみ使用する。
通常のstep実装・レビューは本セッションで完結させる。

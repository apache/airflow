 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# Japanese (ja) Translation Agent Skill

**Locale code:** `ja`
**Preferred variant:** Standard Japanese (ja), polite "Desu/Masu" style.

This file contains locale-specific guidelines so AI translation agents produce
new Japanese strings that stay 100% consistent with the existing Airflow translations.

## 1. Core Airflow Terminology

The following terms **must remain in English unchanged** (case-sensitive):

- `Dag` / `Dags` — Airflow concept; never write "DAG" or "ダグ"
- `XCom` / `XComs` — Cross-communication mechanism
- `Asset` / `Assets` — Data dependency (formerly Dataset)
- `Plugin` / `Plugins`
- `Pool` / `Pools`
- `Provider` / `Providers`
- `Run` / `Runs` — When used standalone (e.g., "All Runs")
- `UTC`, `JSON`, `PID`, `ID`, `REST API`
- Log levels: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`

## 2. Standard Translations

| English Term | Japanese Translation | Notes |
| :--- | :--- | :--- |
| Task | タスク | Standard Katakana |
| Task Instance | タスクインスタンス | |
| Task Group | タスクグループ | |
| Dag Run | Dag 実行 | |
| Backfill | 過去分の再実行 | |
| Trigger | トリガー | |
| Scheduler | スケジューラ | |
| Executor | エグゼキュータ | |
| Connection | 接続 | |
| Variable | 変数 | |
| Audit Log | 監査ログ | |
| State | 状態 | |

## 3. Task/Run States

| English State | Japanese Translation |
| :--- | :--- |
| running | 実行中 |
| failed | 失敗した |
| success | 成功 |
| queued | 待機中 |
| scheduled | スケジュール済 |
| skipped | スキップ済 |
| deferred | 延期済 |
| removed | 削除済 |
| upstream_failed | 上流が失敗しました |

## 4. Japanese-Specific Guidelines

### Tone and Register

- Use **formal Japanese** ("Desu/Masu" form).
- Technical software UI tone: neutral and professional.
- Keep strings concise for buttons and tooltips.

### Spacing (The 1/4 Rule)

- Use a **half-width space** between Japanese characters and English/Numerical characters.
  - Correct: `10 個の Dag`
  - Incorrect: `10個のDag`

### Capitalization

- Capitalize proper technical terms: "Dag", "Asset", "XCom".
- Match the casing of existing translations in `ja.json`.

## 5. Action Verbs (UI Elements)

| English | Japanese |
| :--- | :--- |
| Add | 追加 |
| Delete | 削除 |
| Edit | 編集 |
| Save | 保存 |
| Reset | リセット |
| Cancel | キャンセル |
| Confirm | 確認 |
| Search | 検索 |

## 6. Agent Instructions (DO / DON'T)

**DO:**

- Use polite register for all user-facing labels.
- Preserve all i18next placeholders: `{{count}}`, `{{dagId}}`.
- Follow the spacing rules strictly.

**DON'T:**

- Translate Section 1 terms.
- Use slang or informal forms.
- Use "DAG" in all caps.

---
**Version:** 1.0 (March 2026)

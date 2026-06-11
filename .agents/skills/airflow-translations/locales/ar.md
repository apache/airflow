<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Arabic (`ar`) Translation Agent Skill

**Locale code:** `ar`
**Preferred variant:** Modern Standard Arabic (MSA), consistent with existing translations in `airflow-core/src/airflow/ui/public/i18n/locales/ar/`

This file contains locale-specific guidelines so AI translation agents produce
new Arabic strings that stay consistent with the existing Airflow Arabic
locale. When a term already exists in `ar/*.json`, reuse that wording instead
of introducing a new synonym.

## 1. Core Airflow Terminology

### Global Airflow terms (never translate)

These terms are defined as untranslatable across Airflow locales. Do not
translate them regardless of context:

- `Airflow` — product name
- `Dag` / `Dags` — Airflow concept; never write `DAG`
- `XCom` / `XComs` — Airflow cross-communication mechanism
- `REST API`
- `JSON`
- `UTC`
- Log levels: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`

### Translated by convention (Arabic-specific)

The Arabic locale translates most other UI terms into Arabic. These
established translations should be reused:

- `Operator` → `المشغّل` (plural in current UI: `المُشغِّلات`)
- `Task Instance` → `مثيل المهمة`
- `Pool` → `مجموعة الموارد`
- `Provider` → `حُزمة` when a singular form is needed; the current UI mostly uses the plural `حُزم`
- `Scheduler` → `المُجَدْوِل`
- `Triggerer` → `المُطلِق`
- `Executor` → `منفذ`

Do not add glossary entries for terms that are not yet used in the Arabic
locale files. If a new term appears, inspect nearby existing translations
first and keep the guide limited to terms with real usage.

## 2. Standard Translations

| English Term  | Arabic Translation | Notes |
| ------------- | ------------------ | ----- |
| Task          | `مهمة`             |       |
| Task Instance | `مثيل المهمة`      |       |
| Task Group    | `مجموعة المهام`    |       |
| Dag Run       | `تشغيل Dag`        | Keep `Dag` in English |
| Pool          | `مجموعة الموارد`   |       |
| Provider      | `حُزمة`            | Plural in current UI: `حُزم` |
| Operator      | `المشغّل`          | Plural in current UI: `المُشغِّلات` |
| Scheduler     | `المُجَدْوِل`      | Component label |
| Triggerer     | `المُطلِق`         | Use this specifically for triggerer, not generic trigger |
| Executor      | `منفذ`             | Component label |

## 3. Arabic-Specific Guidelines

### Tone and Register

- Use neutral, professional MSA suitable for a technical UI.
- Keep labels concise.
- Prefer the wording already present in `ar/*.json` over more literary or more
  textbook alternatives.

### Action Labels

- Prefer the concise action labels already used in the locale over newly
  invented imperative forms.
- Existing UI examples include `تشغيل`, `حذف`, `حفظ`, and `تنزيل`.
- Do not introduce imperative-only forms such as `شغّل` or `امسح` unless the
  existing locale already uses them for that exact context.

### Mixed Arabic and English Terms

- Keep embedded English Airflow terms in their original casing: `Dag`, `Dags`,
  `XCom`.
- Preserve placeholders exactly as written: `{{count}}`, `{{dagDisplayName}}`,
  `{{hotkey}}`, and so on.
- Existing patterns include `معرف Dag`, `تشغيل Dag`, and `{{count}} Dags`.

### Plural Forms

Arabic in Airflow uses the full six-category i18next plural set, and the UI
tooling already expects all of these suffixes for `ar`:

- `_zero`
- `_one`
- `_two`
- `_few`
- `_many`
- `_other`

Plural guidance should follow the Unicode CLDR Arabic cardinal rules:

- `_zero` for `0`
- `_one` for `1`
- `_two` for `2`
- `_few` for `3..10` (mod 100)
- `_many` for `11..99` (mod 100)
- `_other` for the remaining cases

Keep all required keys even when some forms are textually identical.

For Airflow terms that stay in English, keep the English term rather than
forcing Arabic dual or plural endings. Example: use `2 Dags`, not `Dagان` or
`Dagين`.

Reuse the existing repo patterns:

```json
"dag_zero": "لا يوجد أي Dag",
"dag_one": "Dag",
"dag_two": "2 Dags",
"dag_few": "Dags",
"dag_many": "Dags",
"dag_other": "Dags"
```

```json
"pool_zero": "لا يوجد أي مجموعة",
"pool_one": "مجموعة",
"pool_two": "مجموعتان",
"pool_few": "مجموعات",
"pool_many": "مجموعة",
"pool_other": "مجموعة"
```

```json
"warning_zero": "لا يوجد أي تحذير",
"warning_one": "1 تحذير",
"warning_two": "تحذيران",
"warning_few": "{{count}} تحذيرات",
"warning_many": "{{count}} تحذير",
"warning_other": "{{count}} تحذير"
```

### Numerals

- Use only Western Arabic numerals: `0 1 2 3 4 5 6 7 8 9`
- Do not use Eastern Arabic numerals: `٠ ١ ٢ ٣ ٤ ٥ ٦ ٧ ٨ ٩`

## 4. Examples from Existing Translations

**Established terminology in the current locale:**

```text
allOperators        -> "جميع المُشغِّلات"
taskInstance_one    -> "مثيل المهمة"
scheduler           -> "المُجَدْوِل"
triggerer           -> "المُطلِق"
executor            -> "منفذ"
Providers           -> "حُزم"
```

**Current Dag patterns:**

```text
dagId               -> "معرف Dag"
triggerDag.title    -> "تشغيل Dag"
favoriteDags_zero   -> "لا توجد أي Dags مفضلة"
```

**Current action-label style:**

```text
delete              -> "حذف"
download.download   -> "تنزيل"
modal.save          -> "حفظ"
```

## 5. Agent Instructions (DO / DON'T)

**DO:**

- Match the wording already used in `ar/*.json`
- Keep `Dag` and `XCom` in English
- Use concise MSA suitable for a software UI
- Provide all six Arabic plural suffixes when a key is pluralized
- Use Western Arabic numerals only
- Take examples from the existing locale files instead of inventing them

**DON'T:**

- Write `DAG`
- Invent a large glossary for terms that are not used in the current locale
- Attach Arabic dual or plural suffixes to English Airflow terms like `Dag`
- Replace established UI wording with a textbook alternative without evidence in
  the repo
- Use Eastern Arabic numerals
- Add grammatical-gender notes for every Arabic noun; they add noise and are
  usually unnecessary here
- Invent action or state examples instead of copying real ones from the locale

---

**Version:** 1.0 — derived from the existing Arabic locale files and Unicode
CLDR Arabic plural rules (May 2026)

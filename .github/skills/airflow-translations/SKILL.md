---
name: airflow-translations
description: >
  Add or update translations for the Apache Airflow UI.
  Guides through setting up locales, scaffolding translation files, translating
  with locale-specific guidelines, and validating results. Use when working with
  i18n tasks in airflow-core/src/airflow/ui/public/i18n/locales/.
license: Apache-2.0
---
<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Airflow Translations

## Determining the Task

Translation work falls into one of two categories depending on whether the
target locale already exists. Check if a directory for the locale exists under
`airflow-core/src/airflow/ui/public/i18n/locales/<locale>/`. If it does, skip
ahead to **Updating an Existing Translation**. If not, start with **Adding a Translation** below.

---

## Adding a Translation

When adding a translation, some configuration files need to be updated before translation work can begin.

### Setting up the locale

First, create the locale directory:

```
mkdir -p airflow-core/src/airflow/ui/public/i18n/locales/<locale>/
```

Then update the following configuration files, keeping the existing alphabetical
ordering in each file:

**`airflow-core/src/airflow/ui/src/i18n/config.ts`**: add the locale to the
`supportedLanguages` array:

```ts
{ code: "<locale>", name: "<native name>" },
```

**`dev/breeze/src/airflow_breeze/commands/ui_commands.py`**: add the plural
suffixes for the language to the `PLURAL_SUFFIXES` dict. Check the i18next
plural rules for the language at <https://jsfiddle.net/6bpxsgd4> to determine
which suffixes are needed:

```
"<locale>": ["<suffixes>"],
```

**`.github/boring-cyborg.yml`**: under `labelPRBasedOnFilePath`, add:

```yaml
translation:<locale>:
  - airflow-core/src/airflow/ui/public/i18n/locales/<locale>/*
```

### Scaffolding the translation files

Once the configuration is in place, run the breeze command to copy every
English namespace file into the new locale directory. This populates each key
with a `TODO: translate:` stub:

```bash
breeze ui check-translation-completeness --language <locale> --add-missing
```

The generated files will look like this:

```json
{
  "allRuns": "TODO: translate: All Runs",
  "blockingDeps": {
    "dependency": "TODO: translate: Dependency",
    "reason": "TODO: translate: Reason"
  }
}
```

### Translating

With the scaffolded files in place, read the locale-specific guideline for the
target language (see the table under **Locale-Specific Guidelines** below). If
one exists, it contains the glossary, tone rules, and formatting conventions
that must be followed. If no locale-specific guideline exists yet, follow the
translation rules described later in this document.

Replace every `TODO: translate: <English terminology>` entry, including the prefix,
with the translated string.

After all entries are translated, continue to **Validation** below.

---

## Updating an Existing Translation

When a locale already exists and you need to fill translation gaps, revise
existing translations, or remove stale keys, start by reading the
locale-specific guideline for the language (see the table under
**Locale-Specific Guidelines** below). This establishes the glossary and
formatting rules to follow.

Next, read the locale's existing JSON files under
`airflow-core/src/airflow/ui/public/i18n/locales/<locale>/` to learn the
terminology already in use. Consistency with established translations is
critical. If a term has been translated a certain way, reuse that exact
translation.

Then check the current state of completeness:

```bash
breeze ui check-translation-completeness --language <locale>
```

If there are **missing** keys, scaffold them with `TODO: translate:` stubs:

```bash
breeze ui check-translation-completeness --language <locale> --add-missing
```

If there are **extra** keys (present in the locale but not in English), remove
them:

```bash
breeze ui check-translation-completeness --language <locale> --remove-extra
```

Now translate the `TODO: translate:` entries following the locale-specific
guideline, then continue to **Validation** below.

---

## Validation

After completing translations, run these checks:

Check completeness. The output should show 0 missing, 0 extra, and 0 TODOs:

```bash
breeze ui check-translation-completeness --language <locale>
```

Run pre-commit hooks to fix formatting, licenses, and linting issues:

```bash
prek run --from-ref main --hook-stage pre-commit
```

---

## General Translation Rules

The following rules apply globally. If the locale-specific guideline for a
language states differently, follow the locale-specific guideline.

### Terms Kept in English

The terms below should remain in English by default. Locale-specific guidelines
may override individual entries where an established local convention exists:

| Term                     | Reason                                       |
| ------------------------ | -------------------------------------------- |
| `Airflow`                | Product name                                 |
| `Dag` / `Dags`           | Airflow convention; always `Dag`, never `DAG`|
| `XCom` / `XComs`         | Airflow cross-communication mechanism name   |
| `Provider` / `Providers` | Airflow extension package name               |
| `REST API`               | Standard technical term                      |
| `JSON`                   | Standard technical format name               |
| `ID`                     | Universal abbreviation                       |
| `PID`                    | Unix process identifier                      |
| `UTC`                    | Time standard                                |
| `Schema`                 | Database term                                |

### Variables and Placeholders

Translation strings use `{{variable}}` interpolation (i18next format).
Never translate or remove variable names inside `{{…}}`. Placeholders may be
reordered as needed for natural word order, but the exact variable casing must
be preserved (e.g., `{{dagDisplayName}}`).

### Plural Forms

Airflow uses i18next plural suffixes (`_one`, `_other`, and optionally `_zero`,
`_two`, `_few`, `_many`). Provide translations for all plural suffixes that the
language requires — the locale-specific guideline specifies which ones. If no
locale guideline exists, check the i18next plural rules at
<https://jsfiddle.net/6bpxsgd4> and provide at minimum `_one` and `_other`.

### Hotkeys

Hotkey values (e.g., `"hotkey": "e"`) are literal key bindings and should
**not** be translated unless the locale-specific guideline says otherwise.

---

## Translation File Structure

All translation files are JSON files located at:

```
airflow-core/src/airflow/ui/public/i18n/locales/<locale-name>/
```

Each locale directory contains namespace JSON files that mirror the English
locale (`en/`). The English locale is the **default locale** and the primary
source for all translations. The current namespace files are:

<!-- START namespace-files, please keep comment here to allow auto update -->
`admin.json`, `assets.json`, `browse.json`, `common.json`, `components.json`, `dag.json`, `dags.json`, `dashboard.json`, `hitl.json`, `tasks.json`
<!-- END namespace-files, please keep comment here to allow auto update -->

---

## Locale-Specific Guidelines

Before translating, read the locale-specific guideline file for the target
language. These contain glossaries, tone rules, and formatting conventions
tailored to each language. If a locale-specific guideline states differently
from a global rule in this document, follow the locale-specific guideline.

| Locale Code | Language                | Guideline File                  |
| ----------- | ----------------------- | ------------------------------- |
| `ar`        | Arabic                  | [locales/ar.md](locales/ar.md)  |
| `ca`        | Catalan                 | [locales/ca.md](locales/ca.md)  |
| `de`        | German                  | [locales/de.md](locales/de.md)  |
| `el`        | Greek                   | [locales/el.md](locales/el.md)  |
| `es`        | Spanish                 | [locales/es.md](locales/es.md)  |
| `fr`        | French                  | [locales/fr.md](locales/fr.md)  |
| `he`        | Hebrew                  | [locales/he.md](locales/he.md)  |
| `hi`        | Hindi                   | [locales/hi.md](locales/hi.md)  |
| `hu`        | Hungarian               | [locales/hu.md](locales/hu.md)  |
| `it`        | Italian                 | [locales/it.md](locales/it.md)  |
| `ja`        | Japanese                | [locales/ja.md](locales/ja.md)  |
| `ko`        | Korean                  | [locales/ko.md](locales/ko.md)  |
| `nl`        | Dutch                   | [locales/nl.md](locales/nl.md)  |
| `pl`        | Polish                  | [locales/pl.md](locales/pl.md)  |
| `pt`        | Portuguese              | [locales/pt.md](locales/pt.md)  |
| `th`        | Thai                    | [locales/th.md](locales/th.md)  |
| `tr`        | Turkish                 | [locales/tr.md](locales/tr.md)  |
| `zh-CN`     | Simplified Chinese      | [locales/zh-CN.md](locales/zh-CN.md) |
| `zh-TW`     | Traditional Chinese     | [locales/zh-TW.md](locales/zh-TW.md) |

If the target locale file does not yet exist, follow only the global rules in
this document.

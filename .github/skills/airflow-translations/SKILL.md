---
name: airflow-translations
description: >
  Translate, review, and maintain Apache Airflow i18n locale strings in JSON
  translation files. Use when working with internationalization, localization,
  or translation tasks in airflow-core/src/airflow/ui/public/i18n/locales/.
  Covers Airflow terminology conventions and translation guidelines.
license: Apache-2.0
---
<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Airflow Translations

## Locale-Specific Guidelines

Before translating, you **must** read the locale-specific guideline file for
the target language. Locale files are located at `locales/<locale-name>.md`
relative to this skill directory.

Match the translation task to the correct locale file using the table below:

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

If the target locale file does not yet exist, follow only the global rules in this document.
When a locale-specific guideline conflicts with a global rule, the **locale-specific
guideline takes precedence**.

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

## Translation Principles

1. **Concise and clear** — Translations are used in UI elements (buttons,
   labels, tooltips). Keep them short and suitable for constrained UI space.
2. **Consistent** — Always use the same translated term for the same English
   term. Refer to the glossary in your locale file.
3. **Accurate** — Maintain the original meaning and intent.
4. **Neutral tone** — Language should be polite and neutral.
5. **Local conventions** — Respect date formats, number formatting, and
   formal/informal tone as appropriate for the locale.

## Do-Not-Translate Terms

The following terms should remain in English by default. Locale-specific
guidelines may override individual entries where established conventions exist:

| Term                     | Reason                                                        |
| ------------------------ | ------------------------------------------------------------- |
| `Airflow`                | Product name                                                  |
| `Dag` / `Dags`           | Airflow convention; always use `Dag`, never `DAG`             |
| `XCom` / `XComs`         | Airflow cross-communication mechanism name                    |
| `Provider` / `Providers` | Airflow extension package name                                |
| `REST API`               | Standard technical term                                       |
| `JSON`                   | Standard technical format name                                |
| `ID`                     | Universal abbreviation                                        |
| `PID`                    | Unix process identifier                                       |
| `UTC`                    | Time standard                                                 |
| `Schema`                 | Database term (keep unless locale has established convention) |

## Variable and Placeholder Handling

Translation strings use the `{{variable}}` interpolation syntax (i18next
format):

- **Never translate** variable names inside `{{...}}`.
- **Never remove** any `{{variable}}` placeholders.
- **Reorder** placeholders as needed to match natural word order.
- **Preserve** exact variable casing (e.g., `{{dagDisplayName}}`).

## Plural Forms

Airflow uses i18next plural suffixes (`_one`, `_other`, and optionally `_zero`,
`_two`, `_few`, `_many`). Provide translations for **all** plural suffixes
relevant to the language you provide translation for.

## Hotkeys

Hotkey values (e.g., `"hotkey": "e"`) are literal key bindings and should
**not** be translated unless the locale-specific guideline specifies otherwise.

## Translation Workflow

1. **Read** the locale-specific guideline (`locales/<locale-name>.md`).
2. **Identify** missing translations — use `--add-missing` to generate stubs
   prefixed with `TODO: translate`:

   ```bash
   breeze ui check-translation-completeness --language <locale-name> --add-missing
   ```

3. **Translate** the `TODO: translate` entries using this guide and the locale
   glossary.
4. **Remove** extra keys not present in the English source:

   ```bash
   breeze ui check-translation-completeness --language <locale-name> --remove-extra
   ```

5. **Validate** completeness:

   ```bash
   breeze ui check-translation-completeness --language <locale-name>
   ```

## Common Airflow Terms

The following terms appear frequently in the English source files. Each locale
glossary should define consistent translations for them. The **Context** column
disambiguates terms that may have different meanings outside of Airflow:

| English Term         | Context                                  |
| -------------------- | ---------------------------------------- |
| Task                 | Unit of work in a Dag                    |
| Task Instance        | Single run of a Task                     |
| Task Group           | Logical grouping of Tasks                |
| Dag Run              | Single execution of a Dag                |
| Operator             | Type/class that defines a Task           |
| Trigger              | Event or mechanism that starts a run     |
| Trigger Rule         | Condition that determines Task execution |
| Triggerer            | Airflow component that handles triggers  |
| Schedule / Scheduler | Timing configuration / Airflow component |
| Backfill             | Retroactive execution of Dag Runs        |
| Asset                | Data dependency tracked by Airflow       |
| Asset Event          | Notification that an Asset was updated   |
| Connection           | External system credentials              |
| Variable             | Key-value configuration store            |
| Pool                 | Resource constraint mechanism            |
| Plugin               | Extensibility mechanism                  |
| Executor             | Component that runs Tasks                |
| Queue                | Execution queue for Tasks                |
| Audit Log            | Record of system events                  |

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Italian (it)

This document provides locale-specific instructions for translating English Airflow UI strings into Italian. It inherits all global rules from the parent [SKILL.md](../SKILL.md).

## Translation Style

Use wording already established in existing `it` locale files first. If a term has no established translation yet, prefer natural Italian UI phrasing over literal translation.

Favor concise, standard UI language commonly used in Italian software interfaces.

**English source:**

```json
"lastDagRun_one": "Last Dag Run",
"deleteConnection_other": "Delete {{count}} connections"
```

**Correct** — natural Italian UI wording:

```json
"lastDagRun_one": "Ultima esecuzione Dag",
"deleteConnection_other": "Elimina {{count}} connessioni"
```

**Incorrect** — overly literal or awkward:

```json
"lastDagRun_one": "Ultima corsa del Dag",
"deleteConnection_other": "Cancellare {{count}} connessioni"
```

## Plural Forms

Italian distinguishes between singular (`_one`) and plural (`_other`). Always provide grammatically correct singular and plural forms.

**English source:**

```json
"taskCount_one": "{{count}} Task",
"taskCount_other": "{{count}} Tasks"
```

**Correct** — proper singular and plural agreement:

```json
"taskCount_one": "{{count}} attività",
"taskCount_other": "{{count}} attività"
```

Note: Some Italian nouns (like *attività*) have identical singular and plural forms. When the noun changes in plural, reflect the change:

```json
"connectionCount_one": "{{count}} connessione",
"connectionCount_other": "{{count}} connessioni"
```

## Counters and Spacing

Follow standard Italian spacing rules:

* Insert a single space between numbers/placeholders and nouns (`{{count}} connessioni`).
* Do not attach counters directly to numbers (Italian does not use forms like `{{count}}connessioni`).
* Maintain readable spacing around technical terms.

```json
"deleteConnection_other": "Elimina {{count}} connessioni",
"taskCount_one": "{{count}} attività",
"lastDagRun_one": "Ultima esecuzione Dag",
"connectionId": "ID connessione"
```

## Articles and Placeholders

Preserve all `{{variable}}` placeholders exactly.
Do not modify variable names.
Adapt articles and prepositions naturally around placeholders.

Reorder phrases only when necessary for correct Italian grammar.

**English source:**

```json
"confirmation": "Are you sure you want to delete {{resourceName}}? This action cannot be undone.",
"description": "{{count}} {{resourceName}} have been successfully deleted. Keys: {{keys}}"
```

**Correct** — placeholders preserved and grammar adapted:

```json
"confirmation": "Sei sicuro di voler eliminare {{resourceName}}? Questa azione non può essere annullata.",
"description": "{{count}} {{resourceName}} sono stati eliminati con successo. Chiavi: {{keys}}"
```

If grammatical gender is unknown, prefer neutral constructions when possible.

**Incorrect** — translated variable names:

```json
"confirmation": "Sei sicuro di voler eliminare {{nomeRisorsa}}?",
"description": "{{conteggio}} {{nomeRisorsa}} eliminati."
```

## Tone and UI Voice

* Use neutral, professional tone.
* Prefer infinitive forms for buttons and actions (`Elimina`, `Modifica`, `Crea`).
* Keep messages concise.
* Use clear confirmation language for destructive actions:
  `"Sei sicuro di voler eliminare {{resourceName}}? Questa azione non può essere annullata."`
* Avoid overly formal or colloquial phrasing.

## Terminology and Casing

* Keep `Dag` casing exactly as `Dag` (never `DAG`).
* Reuse established Italian technical terminology from existing locale files.
* Prefer commonly used translations:

  * `Scheduler` → `Scheduler` (if not localized elsewhere)
  * `Operator` → `Operatore`
  * `Connection` → `Connessione`
  * `Variable` → `Variabile`
* Keep stable technical tokens in English: `XCom`, `REST API`, `JSON`, `URL`, `ID`, `UTC`.

When unsure, check how the term is translated elsewhere in the `it` locale files and maintain consistency.

## Terminology Reference

The established Italian translations are defined in existing locale files. Before translating, **read the existing it JSON files** to learn the established terminology:

```
airflow-core/src/airflow/ui/public/i18n/locales/it/
```

Use the translations found in these files as the authoritative glossary. When translating a term, check how it has been translated elsewhere in the locale to maintain consistency. If a term has not been translated yet, refer to the English source in `en/` and apply the rules in this document.

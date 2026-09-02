<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Italian (`it`) Translation Agent Skill

**Locale code:** `it`
**Preferred variant:** Standard Italian, consistent with existing translations in `airflow-core/src/airflow/ui/public/i18n/locales/it/`

This file contains locale-specific guidelines so AI translation agents produce
new Italian strings that stay consistent with the existing Airflow Italian
locale. When a term already exists in `it/*.json`, reuse that wording instead
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

### Translated by convention (Italian-specific)

The Italian locale keeps several Airflow component names in English, while
others are translated. These established translations should be reused:

- `Operator` → `Operatore` (plural in current UI: `Operatori`)
- `Scheduler` → `Pianificatrice`
- `Triggerer` → `Triggerer` (component label, kept in English)
- `Executor` → `Executor` (component label, kept in English)
- `Pool` → `Pool` (plural in current UI: `Pools`)

Do not add glossary entries for terms that are not yet used in the Italian
locale files. If a new term appears, inspect nearby existing translations
first and keep the guide limited to terms with real usage.

## 2. Standard Translations

| English Term       | Italian Translation | Notes                                        |
| ------------------ | ------------------- | -------------------------------------------- |
| Task               | `Task`              | Kept in English; plural `Tasks`              |
| Task Instance      | `Istanza di Task`   |                                              |
| Task Group         | `Task Group`        | Kept in English                              |
| Dag Run            | `Run del Dag`       | Keep `Dag` in English; plural `Run del Dag`  |
| Dag ID             | `ID del Dag`        |                                              |
| Pool               | `Pool`              | Kept in English; plural `Pools`              |
| Provider           | `Provider`          | Kept in English                              |
| Operator           | `Operatore`         | Plural in current UI: `Operatori`            |
| Scheduler          | `Pianificatrice`    | Component label in the Health panel          |
| Triggerer          | `Triggerer`         | Component label, kept in English             |
| Executor           | `Executor`          | Component label, kept in English             |
| Health             | `Salute`            | Health panel title                           |
| Healthy            | `Sano`              |                                              |
| Unhealthy          | `Malato`            |                                              |
| History            | `Cronologia`        |                                              |
| Source             | `Fonte`             |                                              |
| Group              | `Gruppo`            |                                              |
| Owner              | `Proprietario`      |                                              |
| Description        | `Descrizione`       |                                              |
| Tags               | `Etichette`         |                                              |
| Schedule           | `Programmazione`    |                                              |
| Parameters         | `Parametri`         |                                              |

## 3. Italian-Specific Guidelines

### Tone and Register

- Use neutral, professional standard Italian suitable for a technical UI.
- Keep labels concise.
- Prefer the wording already present in `it/*.json` over more literary or more
  formal alternatives.

### Action Labels

- Prefer the concise action labels already used in the locale over newly
  invented forms.
- Existing UI examples include:
  - `Elimina` (delete)
  - `Salva` (save)
  - `Chiudi` (close)
  - `Modifica` (edit)
  - `Aggiungi` (add)
  - `Cerca` / `Cercare` (search)
- Do not introduce variants for an action unless the existing locale already
  uses them for that exact context.

### Mixed Italian and English Terms

- Keep embedded English Airflow terms in their original casing: `Dag`, `Dags`,
  `XCom`, `XComs`.
- Component names such as `Executor`, `Triggerer`, `Pool`, `Provider`, `Task`,
  `Run`, `Logs`, `Backfill`, and `Triggered` are consistently kept in English in
  the Italian locale. Do not translate them.
- Preserve placeholders exactly as written: `{{count}}`, `{{dagDisplayName}}`,
  `{{hotkey}}`, `{{dag_display_name}}`, and so on.
- Existing patterns include `ID del Dag`, `Run del Dag`, and `{{count}} Tasks`.

### Plural Forms

Italian in Airflow uses four i18next plural suffixes, and the UI tooling
already expects all of these suffixes for `it`:

- `_zero`
- `_one`
- `_many`
- `_other`

Plural guidance follows the Italian cardinal rules (with `_zero` used for the
count `0` and `_many` behaving like `_other` for every count other than `1`):

- `_zero` for `0`
- `_one` for `1`
- `_many` for any other count
- `_other` for any other count

Keep all required keys even when some forms are textually identical. For
Airflow terms that stay in English, keep the English term and its English
plural rather than forcing Italian endings. Example: use `2 Dags`, not
`2 Daghi`.

Reuse the existing repo patterns:

```json
"dag_zero": "Nessun Dag",
"dag_one": "Dag",
"dag_many": "Dags",
"dag_other": "Dags"
```

```json
"pool_zero": "Nessun pool",
"pool_one": "Pool",
"pool_many": "Pools",
"pool_other": "Pools"
```

```json
"taskCount_zero": "Nessun task",
"taskCount_one": "{{count}} Task",
"taskCount_many": "{{count}} Tasks",
"taskCount_other": "{{count}} Tasks"
```

### Articles and Prepositions

- Use the contractions the existing locale already uses, such as
  `dell'`, `dell'Executor`, `dell'Owner`, and `dell'interfaccia`.
- When an English term keeps its English spelling, do not force an Italian
  definite article onto the term inside the string unless the existing locale
  already does so.

## 4. Examples from Existing Translations

**Established terminology in the current locale:**

```text
allOperators          -> "Tutti gli Operatori"
states.success        -> "Successo"
task.operator         -> "Operatore"
taskInstance.executor -> "Executor"
browse.xcoms          -> "XComs"
```

**Current Dag patterns:**

```text
dagId                 -> "ID del Dag"
dagRun_one            -> "Run del Dag"
filters.allRunTypes   -> "Tutti i Tipi di Run"
```

**Current action-label style:**

```text
deleteActions.button            -> "Elimina"
formActions.save                -> "Salva"
components.close                -> "Chiudi"
connections.edit                -> "Modifica Connessione"
pools.add                       -> "Aggiungi Pool"
```

**Current health-panel labels:**

```text
health.health       -> "Salute"
health.healthy      -> "Sano"
health.unhealthy    -> "Malato"
health.scheduler    -> "Pianificatrice"
health.triggerer    -> "Triggerer"
health.dagProcessor -> "Processore del Dag"
```

## 5. Agent Instructions (DO / DON'T)

**DO:**

- Match the wording already used in `it/*.json`
- Keep `Dag`, `XCom`, and component names such as `Executor`, `Triggerer`,
  `Pool`, `Provider`, and `Task` in English
- Use concise standard Italian suitable for a software UI
- Provide all four Italian plural suffixes when a key is pluralized
- Reuse contractions already present in the locale (e.g. `dell'`, `dell'Owner`)
- Take examples from the existing locale files instead of inventing them

**DON'T:**

- Write `DAG`
- Invent a large glossary for terms that are not used in the current locale
- Attach Italian plural or inflectional endings to English Airflow terms like
  `Dag` or `Task`
- Replace established UI wording with a textbook alternative without evidence in
  the repo
- Translate component names that the locale consistently keeps in English
  (`Executor`, `Triggerer`, `Pool`, `Provider`, `Task`, `Run`, `Logs`, `Backfill`)
- Invent action or state examples instead of copying real ones from the locale

---

**Version:** 1.0 — derived from the existing Italian locale files and i18next
Italian plural rules (August 2026)

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Italian (it) Translation Agent Skill

**Locale code:** `it`
**Preferred variant:** Standard Italian (it), consistent with existing translations in `airflow-core/src/airflow/ui/public/i18n/locales/it/`

This file contains locale-specific guidelines so AI translation agents produce
new Italian strings that stay 100% consistent with the existing translations.

## 1. Core Airflow Terminology

### Global Airflow terms (never translate)

These terms are defined as untranslatable across **all** Airflow locales.
Do not translate them regardless of language:

- `Airflow` - Product name
- `Dag` / `Dags` - Airflow concept; never write "DAG"
- `XCom` / `XComs` - Airflow cross-communication mechanism
- `Asset` / `Assets` - Data dependency tracked by Airflow
- `Provider` / `Providers` - Airflow extension package name
- `Pool` / `Pools` - Resource constraint mechanism
- `Map Index` - Task mapping index
- `PID` - Unix process identifier
- `ID` - Universal abbreviation
- `UTC` - Time standard
- `JSON` - Standard technical format name
- `REST API` - Standard technical term
- `Schema` - Database term
- Log levels: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`

### Kept in English by convention (Italian-specific)

The existing Italian locale files leave these terms untranslated. Keep them in
English to stay consistent with established translations:

- `Task` / `Tasks` - Airflow task concept
- `Run` / `Runs` - Used in "Run del Dag", "Tutti i Run", and similar labels
- `Backfill` / `Backfills` - Airflow retroactive execution concept
- `Trigger` / `Triggerer` - Keep as nouns and component names; translate only
  action phrasing around them when an existing translation does so
- `Heartbeat` - Used in component health labels
- `Plugin` / `Plugins` - Airflow extensibility mechanism
- `Owner` - Existing UI strings use "Owner" in ownership labels
- `Retry` - Used in configuration labels and retry-related UI text

## 2. Standard Translations

The following Airflow-specific terms have established Italian translations
that **must be used consistently**:

| English Term           | Italian Translation        | Notes                                      |
| ---------------------- | -------------------------- | ------------------------------------------ |
| Task Instance          | Istanza di Task            | Plural: "Istanze di Task"                  |
| Task Group             | Gruppo di Task             |                                            |
| Dag Run                | Run del Dag                | Plural remains "Run del Dag" in common keys |
| Asset Event            | Evento Asset / Evento di Asset | Match the nearby existing context      |
| Connection             | Connessione                | Plural: "Connessioni"                      |
| Variable               | Variabile                  | Plural: "Variabili"                        |
| Configuration          | Configurazione             |                                            |
| Documentation          | Documentazione             |                                            |
| Security               | Sicurezza                  |                                            |
| User                   | Utente                     | Plural: "Utenti"                           |
| Role                   | Ruolo                      | Plural: "Ruoli"                            |
| Permission             | Permesso                   | Plural: "Permessi"                         |
| State                  | Stato                      |                                            |
| Queue (noun)           | Coda                       | e.g. "In Coda" for "queued"                |
| Duration               | Durata                     |                                            |
| Timezone               | Fuso Orario                |                                            |
| Owner                  | Proprietario               | Existing Dag details use this form         |
| Tag                    | Etichetta                  | e.g. "Filtra Dag per etichetta"            |
| Add                    | Aggiungi                   |                                            |
| Edit                   | Modifica                   |                                            |
| Save                   | Salva                      |                                            |
| Delete                 | Elimina                    |                                            |
| Cancel                 | Annulla                    |                                            |
| Confirm                | Conferma                   |                                            |
| Filter (noun)          | Filtro                     |                                            |
| Filter (verb)          | Filtra                     |                                            |
| Reset                  | Resetta                    |                                            |
| Logout                 | Esci                       |                                            |

## 3. Task/Run States

| English State       | Italian Translation |
| ------------------- | ------------------- |
| running             | In Esecuzione       |
| failed              | Fallito             |
| success             | Successo            |
| queued              | In Coda             |
| scheduled           | Programmato         |
| skipped             | Saltato             |
| deferred            | In Attesa           |
| removed             | Rimosso             |
| up_for_retry        | Da Riavviare        |
| up_for_reschedule   | Da Reschedulare     |
| upstream_failed     | Upstream Fallito    |
| paused              | In Pausa            |
| active              | Attivo              |

## 4. Italian-Specific Guidelines

### Tone and Register

- Use clear, neutral, professional Italian suitable for a technical web UI.
- Prefer concise labels for buttons, table headers, filters, and tooltips.
- Existing strings often use direct action verbs for buttons, such as
  "Aggiungi", "Elimina", "Modifica", and "Salva"; follow that style.

### Gender Agreement

- `Dag` is treated as **masculine**: "del Dag", "il Dag".
- `Run` is treated as **masculine**: "il Run", "Run del Dag".
- `Task` is commonly treated as **feminine** in phrases such as "la Task" and
  "Task fallite"; preserve nearby context when adding related strings.
- `Istanza` is **feminine**: "Istanza di Task", "Istanze di Task fallite".
- `Connessione` and `Variabile` are **feminine**.
- `Evento` is **masculine**: "Evento Asset", "Evento di Asset".

### Plural Forms

- Italian locale files use i18next suffixes `_zero`, `_one`, `_many`, and
  `_other`. Preserve every suffix present in the English source.
- `_many` and `_other` usually use the same Italian plural form:

  ```json
  "task_one": "Task",
  "task_many": "Tasks",
  "task_other": "Tasks"
  ```

  ```json
  "connection_zero": "Nessuna connessione",
  "connection_one": "Connessione",
  "connection_many": "Connessioni",
  "connection_other": "Connessioni"
  ```

### Capitalization

- Match the surrounding Italian files. Headers, navigation labels, and table
  labels often use title-like capitalization, for example "Data Logica",
  "Run del Dag", and "Fuso Orario".
- Use sentence case for longer descriptions and confirmation messages.
- Always preserve the capitalization of Airflow terms: "Dag", "Asset", "XCom",
  "Pool", "Task", "Run", "Backfill", and "Trigger".

### Technical Loanwords

- Keep established English loanwords where the locale already uses them:
  "Task", "Run", "Backfill", "Pool", "Trigger", "Triggerer", "Owner", "Retry",
  and "Heartbeat".
- Do not replace these with new Italian equivalents unless the existing JSON
  files already use the Italian form in the same context.

## 5. Examples from Existing Translations

**Always keep in English:**

- "Dag" -> "Dag"
- "Task" -> "Task"
- "Run" -> "Run"
- "Asset" -> "Asset"
- "Backfill" -> "Backfill"
- "XCom" -> "XCom"
- "Pool" -> "Pool"
- "Trigger" -> "Trigger"

**Common translation patterns:**

```
task_one              -> "Task"
task_many             -> "Tasks"
dagRun_one            -> "Run del Dag"
dagRun_many           -> "Run del Dag"
taskInstance_one      -> "Istanza di Task"
taskInstance_many     -> "Istanze di Task"
assetEvent_one        -> "Evento Asset"
assetEvent_many       -> "Eventi Asset"
connection_one        -> "Connessione"
connection_many       -> "Connessioni"
variable_one          -> "Variabile"
variable_many         -> "Variabili"
allRuns               -> "Tutti i Run"
running               -> "In Esecuzione"
failed                -> "Fallito"
success               -> "Successo"
queued                -> "In Coda"
scheduled             -> "Programmato"
```

**Action verbs (buttons):**

```
Add      -> "Aggiungi"
Delete   -> "Elimina"
Edit     -> "Modifica"
Save     -> "Salva"
Reset    -> "Resetta"
Cancel   -> "Annulla"
Confirm  -> "Conferma"
Search   -> "Cerca" / "Cercare" depending on existing nearby placeholder style
Filter   -> "Filtra"
Logout   -> "Esci"
```

## 6. Agent Instructions (DO / DON'T)

**DO:**

- Match tone, style, gender agreement, and casing from existing `it/*.json`
  files.
- Preserve all i18next placeholders: `{{count}}`, `{{dagName}}`,
  `{{resourceName}}`, `{{hotkey}}`, etc.
- Provide every plural suffix present in the source key (`_zero`, `_one`,
  `_many`, `_other`).
- Check existing Italian translations before adding new terms.
- Keep UI strings concise and natural.

**DON'T:**

- Translate Airflow-specific terms listed in section 1.
- Use "DAG" - always write "Dag".
- Change hotkey values, file paths, code references, or placeholder names.
- Invent new vocabulary when an equivalent already exists in the Italian JSON
  files.
- Mix forms for the same concept inside one namespace.

---

**Version:** 1.0 - derived from existing `it/*.json` locale files (May 2026)

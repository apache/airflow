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

- `Airflow` — Product name
- `Dag` / `Dags` — Airflow concept; never write "DAG"
- `XCom` / `XComs` — Airflow cross-communication mechanism
- `Asset` / `Assets` — Data dependency tracked by Airflow
- `Provider` / `Providers` — Airflow extension package name
- `Map Index` — Task mapping index
- `PID` — Unix process identifier
- `ID` — Universal abbreviation
- `UTC` — Time standard
- `JSON` — Standard technical format name
- `REST API` — Standard technical term
- `Schema` — Database term
- Log levels: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`

### Kept in English by convention (Italian-specific)

The existing Italian locale files leave these terms untranslated.
Keep them in English to stay consistent with established translations:

- `Backfill` / `Backfills` — Airflow-specific retroactive execution concept
- `Catchup` — Airflow scheduling concept
- `Executor` — Airflow component name (e.g., "Configurazione dell'Executor")
- `Plugin` / `Plugins` — Airflow extensibility mechanism
- `Pool` / `Pools` — Resource constraint mechanism
- `Trigger` / `Triggerer` — As **component names and nouns** keep in English (e.g., "Classe del Trigger", "Triggerer Assegnato"); as a **verb** translate as "Attivare"
- `Upstream` / `Downstream` — Used as-is within Italian sentences (e.g., "Upstream Fallito")
- `Heartbeat` — Used as-is in component health labels (e.g., "Ultimo Heartbeat")
- `Run` / `Runs` — When used standalone (e.g., "Tutti i Run", "Run del Dag")
- `Wrap` — Used as-is for text wrap toggling (e.g., "Abilita il wrap")
- `Bundle` — Airflow bundle concept

## 2. Standard Translations

The following Airflow-specific terms have established Italian translations
that **must be used consistently**:

| English Term          | Italian Translation            | Notes                                          |
| --------------------- | ------------------------------ | ---------------------------------------------- |
| Task                  | Task                           | Kept in English; plural: "Tasks"               |
| Task Instance         | Istanza di Task                | Plural: "Istanze di Task"                      |
| Task Group            | Gruppo di Task                 |                                                |
| Dag Run               | Run del Dag                    | Plural: "Run del Dag"                          |
| Trigger (verb)        | Attivare                       | "Attivato da" for "Triggered by"               |
| Trigger Rule          | Regola di Trigger              |                                                |
| Scheduler             | Pianificatrice                 |                                                |
| Schedule (noun)       | Programmazione                 |                                                |
| Operator              | Operatore                      | Plural: "Operatori"                            |
| Connection            | Connessione                    | Plural: "Connessioni"                          |
| Variable              | Variabile                      | Plural: "Variabili"                            |
| Configuration         | Configurazione                 |                                                |
| Audit Log             | Registro di Controllo (Logs)   |                                                |
| Log                   | Log                            | Kept in English; plural: "Logs"                |
| State                 | Stato                          |                                                |
| Queue (noun)          | Coda                           | e.g., "In Coda" for "queued"                   |
| Duration              | Durata                         |                                                |
| Owner                 | Proprietario                   |                                                |
| Tags                  | Etichette                      |                                                |
| Description           | Descrizione                    |                                                |
| Documentation         | Documentazione                 |                                                |
| Timezone              | Fuso Orario                    |                                                |
| Dark Mode             | Modalità scura                 |                                                |
| Light Mode            | Modalità chiara                |                                                |
| Asset Event           | Evento Asset                   | Keep "Asset" in English                        |
| Dag Processor         | Processore del Dag             |                                                |
| Try Number            | Numero di Tentativo            |                                                |

## 3. Task/Run States

| English State       | Italian Translation   |
| ------------------- | --------------------- |
| running             | In Esecuzione         |
| failed              | Fallito               |
| success             | Successo              |
| queued              | In Coda               |
| scheduled           | Programmato           |
| skipped             | Saltato               |
| deferred            | In Attesa             |
| removed             | Rimosso               |
| restarting          | Riavviato             |
| up_for_retry        | Da Riavviare          |
| up_for_reschedule   | Da Reschedulare       |
| upstream_failed     | Upstream Fallito      |
| no_status / none    | Nessun Stato          |
| planned             | Pianificato           |

## 4. Italian-Specific Guidelines

### Tone and Register

- Use a **neutral, formal Italian** tone suitable for technical software UIs.
- Prefer impersonal constructions (e.g., "Premi {{hotkey}} per...") over explicit "tu" or "Lei".
- Keep UI strings concise — they appear in buttons, labels, and tooltips.

### Gender Agreement

- Italian nouns have grammatical gender; match articles and adjectives accordingly:
  - "Dag" is treated as **masculine**: "il Dag", "un Dag", "del Dag"
  - "Task" is treated as **masculine**: "il Task", "un Task"
  - "Run" is treated as **masculine**: "il Run", "del Run"
  - "Connessione" is **feminine**: "la connessione", "una connessione"
  - "Variabile" is **feminine**: "la variabile"
  - "Istanza" is **feminine**: "un'istanza", "l'istanza"
  - "Esecuzione" is **feminine**: "l'esecuzione"

### Plural Forms

- Italian uses i18next plural suffixes `_one`, `_many`, `_other`, and `_zero`.
  `_many` and `_other` must always use the same translation.
  `_zero` uses "Nessun/Nessuna" + noun:

  ```json
  "task_one": "Task",
  "task_many": "Tasks",
  "task_other": "Tasks",
  "task_zero": "Nessun task"
  ```

  ```json
  "connection_one": "Connessione",
  "connection_many": "Connessioni",
  "connection_other": "Connessioni",
  "connection_zero": "Nessuna connessione"
  ```

### Capitalization

- Use **title case** for UI headers, navigation items, and button labels (e.g., "Tutti i Run", "Registro di Controllo").
- Use **sentence case** for descriptions and longer messages.
- Capitalize proper terms: "Dag", "Asset", "XCom", "Pool", "Plugin", etc.

### Preposition Contractions

- Apply standard Italian preposition + article contractions:
  - "di" + "il" → "del" (e.g., "Run del Dag", "ID del Trigger")
  - "di" + "l'" → "dell'" (e.g., "Configurazione dell'Executor")
  - "a" + "il" → "al"
  - "in" + "il" → "nel"

## 5. Examples from Existing Translations

**Always keep in English:**

- "Dag" → "Dag"
- "Asset" → "Asset"
- "XCom" → "XCom"
- "Pool" → "Pool"
- "Plugin" → "Plugin"
- "Backfill" → "Backfill"
- "Catchup" → "Catchup"
- "Run" → "Run"
- "Task" → "Task"

**Common translation patterns:**

```
task_one              → "Task"
task_many             → "Tasks"
task_zero             → "Nessun task"
dagRun_one            → "Run del Dag"
dagRun_many           → "Run del Dag"
dagRun_zero           → "Nessun run del Dag"
backfill_one          → "Backfill"
backfill_many         → "Backfills"
backfill_zero         → "Nessun backfill"
taskInstance_one      → "Istanza di Task"
taskInstance_many     → "Istanze di Task"
taskInstance_zero     → "Nessuna istanza di task"
allRuns               → "Tutti i Run"
running               → "In Esecuzione"
failed                → "Fallito"
success               → "Successo"
queued                → "In Coda"
scheduled             → "Programmato"
```

**Trigger compound nouns — keep "Trigger"/"Triggerer" in English:**

```
triggerer.class           → "Classe del Trigger"
triggerer.id              → "ID del Trigger"
triggerer.createdAt       → "Creato il"
triggerer.assigned        → "Triggerer Assegnato"
triggerer.latestHeartbeat → "Ultimo Heartbeat del Trigger"
triggerer.title           → "Info del Trigger"
```

**Action verbs (buttons):**

```
Add      → "Aggiungi"
Delete   → "Elimina"
Edit     → "Modifica"
Save     → "Salva"
Reset    → "Resetta"
Cancel   → "Annulla"
Confirm  → "Conferma"
Import   → "Importa"
Search   → "Cercare"
Filter   → "Filtra"
Download → "Scarica"
Expand   → "Espandi"
Collapse → "Collassa"
```

**"Cannot X" / error patterns:**

```
Cannot clear Task Instance → "Impossibile pulire l'Istanza di Task"
Failed to load             → "Impossibile caricare"
No items found             → "Nessun {{modelName}} trovato"
```

**Health/status labels:**

```
Healthy   → "Sano"
Unhealthy → "Malato"
```

## 6. Agent Instructions (DO / DON'T)

**DO:**

- Match tone, style, gender agreement, and casing from existing `it/*.json` files
- Use formal, neutral Italian suitable for all Italian-speaking users
- Preserve all i18next placeholders: `{{count}}`, `{{dagName}}`, `{{hotkey}}`, etc.
- Apply correct Italian preposition contractions (del, dell', nel, al, etc.)
- Provide all needed plural suffixes (`_one`, `_many`, `_other`, `_zero`) for each plural key
- Check existing translations before adding new ones to maintain consistency

**DON'T:**

- Translate Airflow-specific terms listed in section 1
- Use "DAG" — always write "Dag"
- Use colloquial or regional Italian expressions
- Invent new vocabulary when an equivalent already exists in the current translations
- Change hotkey values (e.g., `"hotkey": "e"` must stay `"e"`)
- Translate variable names or placeholders inside `{{...}}`

---

**Version:** 1.0 — derived from existing `it/*.json` locale files (April 2026)

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# German (de) Translation Agent Skill

**Locale code:** `de`
**Preferred variant:** Standard German (de), using formal "Sie" register, consistent with existing translations in `airflow-core/src/airflow/ui/public/i18n/locales/de/`

This file contains locale-specific guidelines so AI translation agents produce
new German strings that stay 100% consistent with the existing translations.

## 1. Core Airflow Terminology

### Global Airflow terms (never translate)

These terms are defined as untranslatable across **all** Airflow locales.
Do not translate them regardless of language:

- `Airflow` — Product name
- `Dag` / `Dags` — Airflow concept; never write "DAG". Use neuter form (not male, female).
- `PID` — Unix process identifier
- `ID` — Universal abbreviation
- `UTC` — Time standard
- `JSON` — Standard technical format name
- `REST API` — Standard technical term
- Log levels: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`

### Translated by convention (German-specific)

The existing German translations translate many Airflow terms into native German.
These established translations **must be used consistently**:

- `Asset` / `Assets` → `Datenset (Asset)` / `Datensets (Assets)` — includes English term in parentheses
- `XCom` / `XComs` → `Task Kommunikation (XComs)` — descriptive with English in parentheses
- `Backfill` → `Auffüllung` / `Auffüllungen`
- `Catchup` → `Nachgeholt`
- `Plugin` / `Plugins` → `Plug-in` / `Plug-ins` — hyphenated German spelling
- `Pool` / `Pools` → `Pool` / `Pools` — kept in English
- `Provider` / `Providers` → `Provider` / `Providers` — kept in English
- `Executor` → `Ausführungsumgebung`
- `Trigger` / `Triggerer` → `Abrufumgebung` / `Abrufumgebungs-Information` (component); translated in context

## 2. Standard Translations

The following Airflow-specific terms have established German translations
that **must be used consistently**:

| English Term          | German Translation                    | Notes                                          |
| --------------------- | ------------------------------------- | ---------------------------------------------- |
| Task                  | Task                                  | Kept in English; plural: "Tasks"               |
| Task Instance         | Task Instanz                          | Plural: "Task Instanzen"                       |
| Task Group            | Task Gruppe                           |                                                |
| Dag Run               | Dag Lauf                              | Plural: "Dag Läufe"                            |
| Trigger (verb)        | Auslösen                              | "Ausgelöst durch" for "Triggered by"           |
| Trigger Rule          | Auslöse-Regel                         |                                                |
| Scheduler             | Zeitplaner                            |                                                |
| Schedule (noun)       | Zeitplan                              |                                                |
| Operator              | Operator                              | Plural: "Operatoren"                           |
| Connection            | Verbindung                            | Plural: "Verbindungen"                         |
| Variable              | Variable                              | Plural: "Variablen"                            |
| Configuration         | Konfiguration                         |                                                |
| Audit Log             | Prüf-Log                              |                                                |
| State                 | Status                                |                                                |
| Queue (noun)          | Warteschlange                         | "Wartend" for "queued"                         |
| Duration              | Laufzeit                              |                                                |
| Owner                 | Eigentümer                            |                                                |
| Tags                  | Markierungen                          |                                                |
| Description           | Beschreibung                          |                                                |
| Documentation         | Dokumentation                         | Short form in nav: "Doku"                      |
| Timezone              | Zeitzone                              |                                                |
| Dark Mode             | Dunkelmodus                           |                                                |
| Light Mode            | Hellmodus                             |                                                |
| Asset Event           | Ereignis zu Datenset (Asset)          | Plural: "Ereignisse zu Datensets (Asset)"      |
| Dag Processor         | Dag Prozessor                         |                                                |
| Heartbeat             | Lebenszeichen                         | e.g., "Letztes Lebenszeichen"                  |
| Upstream / Downstream | Vorgelagert / Nachgelagert            |                                                |

## 3. Task/Run States

| English State       | German Translation            |
| ------------------- | ----------------------------- |
| running             | Laufend                       |
| failed              | Fehlgeschlagen                |
| success             | Erfolgreich                   |
| queued              | Wartend                       |
| scheduled           | Geplant                       |
| skipped             | Übersprungen                  |
| deferred            | Delegiert                     |
| removed             | Entfernt                      |
| restarting          | Im Neustart                   |
| up_for_retry        | Wartet auf neuen Versuch      |
| up_for_reschedule   | Wartet auf Neuplanung         |
| upstream_failed     | Vorgelagerte fehlgeschlagen   |
| no_status / none    | Kein Status                   |
| planned             | Geplant                       |

## 4. German-Specific Guidelines

### Tone and Register

- Use **formal German** ("Sie" form). Do not use "du".
- Use a professional, precise tone suitable for technical software UIs.
- Keep UI strings concise — they appear in buttons, labels, and tooltips.

### Gender and Articles

- German nouns have grammatical gender; match articles and adjectives accordingly:
  - "Dag" is treated as **neuter**: "das Dag", "ein Dag"
  - "Task" is treated as **masculine**: "der Task"
  - "Lauf" (Run) is **masculine**: "der Lauf", plural: "die Läufe"
  - "Verbindung" is **feminine**: "die Verbindung"
  - "Variable" is **feminine**: "die Variable"
  - "Datenset" is **neuter**: "das Datenset"

### Compound Nouns

- German forms compound nouns. The existing translations use spaces between Airflow terms
  for readability: "Dag Lauf" (not "Daglauf"), "Task Instanz" (not "Taskinstanz").
- Follow this established pattern for consistency.

### Plural Forms

- German uses i18next plural suffixes `_one` and `_other` only:

  ```json
  "task_one": "Task",
  "task_other": "Tasks"
  ```

  ```json
  "dagRun_one": "Dag Lauf",
  "dagRun_other": "Dag Läufe"
  ```

### Capitalization

- All German **nouns** are capitalized (standard German orthography).
- Use **title case** for UI headers and navigation items.
- Use **sentence case** for descriptions and longer messages.

### Parenthetical English Terms

- The German locale uses a unique pattern of including the English term in parentheses
  after the German translation for clarity:
  - "Datenset (Asset)" — helps users recognize the Airflow concept
  - "Task Kommunikation (XComs)"
  - "Durch Datenset (Asset) ausgelöst" for "Asset triggered"
- Follow this pattern for any new terms that have well-known English equivalents in Airflow.

## 5. Examples from Existing Translations

**Terms with parenthetical English:**

```
Asset          → "Datenset (Asset)"
Assets         → "Datensets (Assets)"
XCom           → "Task Kommunikation (XComs)"
Asset Event    → "Ereignis zu Datenset (Asset)"
```

**Common translation patterns:**

```
task_one              → "Task"
task_other            → "Tasks"
dagRun_one            → "Dag Lauf"
dagRun_other          → "Dag Läufe"
backfill_one          → "Auffüllung"
backfill_other        → "Auffüllungen"
taskInstance_one      → "Task Instanz"
taskInstance_other    → "Task Instanzen"
allRuns               → "Alle Läufe"
running               → "Laufend"
failed                → "Fehlgeschlagen"
success               → "Erfolgreich"
queued                → "Wartend"
scheduled             → "Geplant"
```

**Triggerer compound nouns — translated to German:**

```
triggerer.class           → "Abruf-Klasse"
triggerer.id              → "Abrufungs ID"
triggerer.createdAt       → "Zeitpunkt der Erstellung"
triggerer.assigned        → "Zugewiesene Abrufumgebung"
triggerer.latestHeartbeat → "Letztes Lebenszeichen"
triggerer.title           → "Abrufumgebungs-Information"
```

**Action verbs (buttons):**

```
Add      → "Hinzufügen"
Delete   → "Löschen"
Edit     → "Bearbeiten"
Save     → "Speichern"
Reset    → "Zurücksetzen"
Cancel   → "Abbrechen"
Confirm  → "Bestätigen"
Import   → "Importieren"
Search   → "Suche"
Filter   → "Filter"
Download → "Herunterladen"
Expand   → "Ausblenden"
Collapse → "Einblenden"
```

**Health/status labels:**

```
Healthy   → "Gesund"
Unhealthy → "Fehlerhaft"
```

## 6. Agent Instructions (DO / DON'T)

**DO:**

- Match tone, style, gender agreement, and casing from existing `de/*.json` files
- Use formal German ("Sie" form) throughout
- Preserve all i18next placeholders: `{{count}}`, `{{dagName}}`, `{{hotkey}}`, etc.
- Capitalize all nouns (standard German orthography)
- Include English terms in parentheses for Airflow-specific concepts where the existing translations do so
- Provide all needed plural suffixes (`_one`, `_other`) for each plural key
- Check existing translations before adding new ones to maintain consistency

**DON'T:**

- Write "DAG" — always write "Dag"
- Use informal "du" — always use "Sie" register
- Use colloquial or regional German expressions
- Invent new vocabulary when an equivalent already exists in the current translations
- Change hotkey values (e.g., `"hotkey": "e"` must stay `"e"`)
- Translate variable names or placeholders inside `{{...}}`
- Omit parenthetical English terms where the pattern has been established

## 7. Rationale for Translation Choices

### Formal register ("Sie")

German distinguishes formal from informal address. Because the user group is
unknown, the formal "Sie" register was chosen throughout.

### Why certain terms are not translated

- **Dag / Dags** — Following the devlist discussion
  ["Airflow should deprecate the term 'DAG' for end users"](https://lists.apache.org/thread/lktrzqkzrpvc1cyctxz7zxfmc0fwtq2j)
  and the global rename to `Dag`, this brand-like term is retained. Translating
  it as "Workflow" would be misleading for experienced Airflow users. `Dag` is
  treated as neuter in German ("der Dag").
- **Log levels (CRITICAL, ERROR, WARNING, INFO, DEBUG)** — These strings also
  appear verbatim in log output, so they must not be translated.
- **Pool / Pools** — Directly understood in German; "Schwimmbad" (swimming pool)
  would be absurd, and "Ressourcen-Pool" is too verbose.
- **Provider / Providers** — Translating this does not improve comprehension;
  the English term is well understood in German.
- **Operator / Operatoren** — Mathematical/technical term that also appears in
  code; alternatives like "Betreiber-Implementierung" are too cumbersome.

### Why specific translations were chosen

- **`Asset` → `Datenset (Asset)`** — New term in Airflow 3, so a meaningful
  German translation is appropriate. The English original is kept in
  parentheses so new users can recognise the Airflow concept. Exception: in
  the navigation bar the shorter form "Datensets" is used without the
  parenthetical to save space.
- **`Asset Event` → `Ereignis zu Datenset (Asset)`** — Logical consequence of
  the Asset translation; avoids the clumsy "Datensatz-Ereignis".
- **`Backfill` → `Auffüllen` / `Auffüllung`** — The technical meaning (filling
  gaps in historical runs) maps well to the German concept of "auffüllen".
- **`Bundle` → `Bündel`** — Direct translation that matches the intended meaning.
- **`Catchup` → `Nachholen`** — Direct translation.
- **`Connection` → `Verbindung`** — Although a technical Airflow construct, the
  direct translation is immediately accessible to new users.
- **`Dag ID`**: No translation. `ID` should be favored to be upper case following German Duden.
- **`Task ID`**: No translation. `ID` should be favored to be upper case following German Duden.
- **`Dag Run` → `Dag Lauf`** — While "Run" appears in code and logs, a German
  equivalent improves the overall UI experience. "Dag" is kept untranslated.
- **`Deferred` → `Delegiert`** — The closest German equivalent: a task is
  handed off ("delegiert") to the Triggerer component.
- **`Docs` → `Doku`** — "Dokumentation" is correct but too wide for the
  navigation bar without a line break; "Doku" is a common German abbreviation.
- **`Map Index` → `Planungs-Index`** — No direct equivalent exists; referring
  to the planning/scheduling aspect is the most accurate option.
- **`Plugins` → `Plug-ins`** — Hyphenated form recommended by Duden.
- **`Scheduled` → `Geplant`** — Used for cyclically scheduled Dag runs.
- **`Tag` → `Markierung`** — Describes the purpose (marking/tagging Dags for
  organisation) without the English loanword.
- **`Task Instance` → `Task Instanz`** — "Task" is kept because it appears in
  code and logs; "Aufgabe" would be the purist choice but less recognisable in
  context.
- **`Trigger` (verb) → `Auslösen`** — Most natural German equivalent. "Triggern"
  exists colloquially but "Auslösen" is more formal and consistent with
  "Auslöse-Regel" (Trigger Rule).
- **`Trigger Rule` → `Auslöse-Regel`** — Consistent with the verb choice above;
  describes the condition that starts a task within a Dag Run.
- **`Try Number` → `Versuch Nummer`**: direct translation is matching.
- **`XCom` → `Task Kommunikation (XCom)`** — Translating the concept improves
  navigation for new users; the original `XCom` is kept in parentheses because
  it appears frequently in code and logs.

---

**Version:** 1.1 — rationale consolidated from
`airflow-core/src/airflow/ui/public/i18n/locales/de/README.md` (April 2026)

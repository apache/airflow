<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Dutch (nl) Translation Agent Skill

**Locale code:** `nl`
**Preferred variant:** Standard Dutch (Netherlands), consistent with existing translations in `airflow-core/src/airflow/ui/public/i18n/locales/nl/`

This file contains locale-specific guidelines so AI translation agents produce
new Dutch strings that stay 100% consistent with the existing translations.

## 1. Core Airflow Terminology

The following terms **must remain in English unchanged** (case-sensitive):

- `Dag` / `Dags` — Airflow concept; never write "DAG"
- `XCom` / `XComs` — Airflow cross-communication mechanism
- `Asset` / `Assets` — Data dependency tracked by Airflow
- `Plugin` / `Plugins` — Airflow extensibility mechanism
- `Pool` / `Pools` — Resource constraint mechanism
- `Provider` / `Providers` — Airflow extension package name
- `Run` / `Runs` — When used standalone (e.g., "Laatste Run")
- `Map Index` — Task mapping index
- `PID` — Unix process identifier
- `ID` — Universal abbreviation
- `UTC` — Time standard
- `JSON` — Standard technical format name
- `REST API` — Standard technical term
- Log levels: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`

## 2. Standard Translations

The following Airflow-specific terms have established Dutch translations
that **must be used consistently**:

| English Term          | Dutch Translation          | Notes                                      |
| --------------------- | -------------------------- | ------------------------------------------ |
| Task                  | Taak                       | Always translate Task to "taak" / "Taak"   |
| Task Instance         | Taak Instance              | Plural: "Taak Instances"                   |
| Task Group            | Taak Groep                 | Plural: "Taak Groepen"                     |
| Dag Run               | Dag Run                    | Plural: "Dag Runs"                         |
| Backfill              | Backfill                   |                                            |
| Trigger (noun)        | Trigger                    | e.g. "Asset Triggered"                     |
| Trigger Rule          | Trigger regel              |                                            |
| Triggerer             | Triggerer                  | Component name                             |
| Scheduler             | Scheduler                  |                                            |
| Schedule (noun)       | Planning                   |                                            |
| Executor              | Executor                   |                                            |
| Connection            | Connectie                  | Plural: "Connecties"                       |
| Variable              | Variabele                  | Plural: "Variabelen"                       |
| Audit Log             | Audit Log                  |                                            |
| Log                   | Log                        | Plural: "Logs"                             |
| State                 | Status                     | e.g. "Totaal {{state}}" -> "Totaal Status" |
| Queue (noun)          | Wachtrij                   |                                            |
| Config / Configuration| Configuratie               |                                            |
| Operator              | Operator                   | Plural: "Operators"                        |
| Asset Event           | Asset Event                | Keep as in English                         |
| Catchup               | Catchup                    | Keep as in English                         |

## 3. Task/Run States

| English State       | Dutch Translation     |
| ------------------- | --------------------- |
| running             | Lopend                |
| failed              | Mislukt               |
| success             | Succesvol             |
| queued              | Wachtend              |
| scheduled           | Gepland               |
| skipped             | Overgeslagen          |
| deferred            | Uitgesteld            |
| removed             | Verwijderd            |
| restarting          | Herstartend           |
| up_for_retry        | Wachtend op een nieuwe poging |
| up_for_reschedule   | Wachtend op herplanning |
| upstream_failed     | Upstream mislukt      |
| no_status / none    | Geen status           |
| planned             | Gepland               |

## 4. Dutch-Specific Guidelines

### Tone and Register

- Use **informal Dutch** ("je/jouw" form). Avoid the formal "u" unless specifically requested.
- Use a professional yet accessible tone.
- Keep UI strings concise — they appear in buttons, labels, and tooltips.

### Gender and Number

- Dutch nouns have grammatical gender, but for technical terms, focus on common usage:
  - "Taak" is generally **de-word**: "de taak"
  - "Run" is **de-word**: "de run"
  - "Process" is **het-word**: "het proces"

### Plural Forms

- Dutch uses i18next plural suffixes `_one` and `_other`.
  Most technical terms add "s" or "en":
  - "Connectie" -> "Connecties"
  - "Variabele" -> "Variabelen"
  - "Taak" -> "Taken"

### Capitalization

- Use **Sentence case** (Zinvallende hoofdletters) for descriptions.
- Use **Title Case** or **Initial Capital** for labels and buttons if the English source does so.
- Always capitalize "Dag", "Taak" (when referring to the object), "Asset", "XCom", etc.

## 5. Examples from Existing Translations

**Common UI Patterns:**

```
Add    → "Toevoegen"
Delete → "Verwijderen" (or "Verwijder" on buttons)
Edit   → "Wijzigen" / "Bewerken"
Save   → "Opslaan"
Cancel → "Annuleer"
Search → "Zoeken"
Filter → "Filter"
```

**Interpolation:**

```json
"delete_confirmation": "Weet je zeker dat je {{resourceName}} wilt verwijderen?"
```

## 6. Agent Instructions (DO / DON'T)

**DO:**

- Maintain the "je" (informal) address style.
- Preserve all i18next placeholders: `{{count}}`, `{{dagId}}`, etc.
- Use "Wachtrij" for technical queues.
- Use "Planning" for schedules.
- Provide both `_one` and `_other` plural forms.

**DON'T:**

- Use "u" (formal).
- Translate "Dag" as "DAG".
- Translate terms listed in Section 1.
- Use inconsistent terms for "State" (always use "Status").

---

**Version:** 1.0 — derived from existing `nl/*.json` locale files (March 2026)

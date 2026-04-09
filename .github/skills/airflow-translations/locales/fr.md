<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# French (fr) Translation Agent Skill

**Locale code:** `fr`
**Preferred variant:** Standard French (fr), consistent with existing translations in `airflow-core/src/airflow/ui/public/i18n/locales/fr/`

This file contains locale-specific guidelines so AI translation agents produce
new French strings that stay 100% consistent with the existing translations.

## 1. Core Airflow Terminology

The following terms **must remain in English unchanged** (case-sensitive):

- `Dag` / `Dags` — Airflow concept; never write "DAG"
- `XCom` / `XComs` — Airflow cross-communication mechanism
- `Asset` / `Assets` — Data dependency tracked by Airflow
- `Plugin` / `Plugins` — Airflow extensibility mechanism
- `Pool` / `Pools` — Resource constraint mechanism
- `Provider` / `Providers` — Airflow extension package name
- `Run` / `Runs` — When used standalone (e.g., "Tous les Runs")
- `Map Index` — Task mapping index
- `PID` — Unix process identifier
- `ID` — Universal abbreviation
- `UTC` — Time standard
- `JSON` — Standard technical format name
- `REST API` — Standard technical term
- Log levels: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`

## 2. Standard Translations

The following Airflow-specific terms have established French translations
that **must be used consistently**:

| English Term          | French Translation         | Notes                                      |
| --------------------- | -------------------------- | ------------------------------------------ |
| Task                  | Tâche / tâche              | Lowercase in compound contexts             |
| Task Instance         | Instance de tâche          | Plural: "Instances de tâche"               |
| Task Group            | Groupe de tâches           |                                            |
| Dag Run               | Exécution de Dag           | Plural: "Exécutions de Dag"                |
| Backfill              | Rattrapage                 | Plural: "Rattrapages"                      |
| Trigger (noun)        | Déclencheur                |                                            |
| Trigger Rule          | Règle de déclenchement     |                                            |
| Triggerer             | Déclencheur                | Component name                             |
| Scheduler             | Planificateur              |                                            |
| Schedule (noun)       | Planification              |                                            |
| Executor              | Exécuteur                  |                                            |
| Connection            | Connexion                  | Plural: "Connexions"                       |
| Variable              | Variable                   |                                            |
| Audit Log             | Journal d'audit            |                                            |
| Log                   | Journal                    | Plural: "Journaux"                         |
| State                 | État                       |                                            |
| Queue (noun)          | File                       | e.g., "En file" for "queued"               |
| Config / Configuration| Configuration              |                                            |
| Operator              | Opérateur                  | Plural: "Opérateurs"                       |
| Asset Event           | Événement d'Asset          | Keep "Asset" in English                    |
| Dag Processor         | Analyseur de Dag           |                                            |
| Heartbeat             | Battement                  |                                            |

## 3. Task/Run States

| English State       | French Translation    |
| ------------------- | --------------------- |
| running             | En cours              |
| failed              | Échoué                |
| success             | Succès                |
| queued              | En file               |
| scheduled           | Planifié              |
| skipped             | Ignoré                |
| deferred            | Différé               |
| removed             | Supprimé              |
| restarting          | Redémarrage           |
| up_for_retry        | À réessayer           |
| up_for_reschedule   | À replanifier         |
| upstream_failed     | Échec en amont        |
| no_status / none    | Aucun statut          |
| planned             | Planifié              |

## 4. French-Specific Guidelines

### Tone and Register

- Use **formal French** ("vous" form). Do not use "tu".
- Use a neutral, professional tone suitable for technical software UIs.
- Keep UI strings concise — they appear in buttons, labels, and tooltips.

### Gender Agreement

- French nouns have grammatical gender; match adjectives and articles accordingly:
  - "Dag" is treated as **masculine**: "le Dag", "un Dag"
  - "Tâche" is **feminine**: "la tâche", "une tâche"
  - "Exécution" is **feminine**: "une exécution", "l'exécution"
  - "Connexion" is **feminine**: "la connexion"
  - "Variable" is **feminine**: "la variable"

### Plural Forms

- French uses i18next plural suffixes `_one` and `_many` / `_other`.
  Use the **same translation** form when singular/plural are grammatically identical.
  Otherwise, add an "s" or use a distinct plural form:

  ```json
  "task_one": "Tâche",
  "task_many": "Tâches",
  "task_other": "Tâches"
  ```

- `_many` and `_other` must always use the same translation.

### Capitalization

- Use **sentence case** for descriptions and longer strings.
- Use **title-like capitalization** for headers, labels, and button text
  (match the style of existing translations).
- Capitalize proper terms: "Dag", "Asset", "XCom", "Pool", "Plugin", etc.

### Elision and Contractions

- Apply standard French elision rules:
  - "de" + vowel → "d'" (e.g., "Journal d'audit", "ID d'exécution")
  - "le" + vowel → "l'" (e.g., "l'exécution", "l'opérateur")

## 5. Examples from Existing Translations

**Always keep in English:**

- "Dag" → "Dag"
- "Asset" → "Asset"
- "XCom" → "XCom"
- "Plugin" → "Plugin"
- "Pool" → "Pool"
- "Provider" → "Provider"

**Common translation patterns:**

```
task_one          → "Tâche"
task_many         → "Tâches"
dagRun_one        → "Exécution de Dag"
dagRun_many       → "Exécutions de Dag"
backfill_one      → "Rattrapage"
backfill_many     → "Rattrapages"
taskInstance_one  → "Instance de tâche"
taskInstance_many → "Instances de tâche"
allRuns           → "Tous les Runs"
running           → "En cours"
failed            → "Échoué"
success           → "Succès"
queued            → "En file"
scheduled         → "Planifié"
```

**Action verbs (buttons):**

```
Add    → "Ajouter"
Delete → "Supprimer"
Edit   → "Modifier"
Save   → "Enregistrer"
Reset  → "Réinitialiser"
Cancel → "Annuler"
Confirm→ "Confirmer"
Import → "Importer"
Export → "Exporter"
Search → "Rechercher"
Filter → "Filtrer"
```

## 6. Agent Instructions (DO / DON'T)

**DO:**

- Match tone, style, gender agreement, and casing from existing `fr/*.json` files
- Use formal French ("vous" form) throughout
- Preserve all i18next placeholders: `{{count}}`, `{{dagName}}`, `{{type}}`, etc.
- Apply correct French elision (d', l', j', etc.)
- Provide all needed plural suffixes (`_one`, `_many`, `_other`) for each key

**DON'T:**

- Translate Airflow-specific terms listed in section 1
- Use "tu" (informal) — always use "vous" register
- Change hotkey values (e.g., `"hotkey": "e"` must stay `"e"`)
- Invent new vocabulary when an equivalent already exists in the current translations
- Use "DAG" — always write "Dag"

---

**Version:** 1.0 — derived from existing `fr/*.json` locale files (February 2026)

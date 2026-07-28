<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Catalan (ca) Translation Agent Skill

**Locale code:** `ca`
**Preferred variant:** Standard Catalan (ca), consistent with existing translations in `airflow-core/src/airflow/ui/public/i18n/locales/ca/`

This file contains locale-specific guidelines so AI translation agents produce
new Catalan strings that stay 100% consistent with the existing translations.

## 1. Core Airflow Terminology

The following terms **must remain in English unchanged** (case-sensitive):

- `Dag` / `Dags` — Airflow concept; never write "DAG"
- `XCom` / `XComs` — Airflow cross-communication mechanism
- `Asset` / `Assets` — Data dependency tracked by Airflow
- `Plugin` / `Plugins` — Airflow extensibility mechanism (translated as "Extensió" in nav/labels — keep English in code references)
- `Pool` / `Pools` — Resource constraint mechanism
- `Provider` / `Providers` — Airflow extension package name
- `Map Index` — Task mapping index
- `PID` — Unix process identifier
- `ID` — Universal abbreviation
- `UTC` — Time standard
- `JSON` — Standard technical format name
- `REST API` — Standard technical term
- Log levels: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`

## 2. Standard Translations

The following Airflow-specific terms have established Catalan translations
that **must be used consistently**:

| English Term          | Catalan Translation            | Notes                                          |
| --------------------- | ------------------------------ | ---------------------------------------------- |
| Task                  | Tasca / tasca                  | Lowercase in compound contexts                 |
| Task Instance         | Instància de tasca             | Plural: "Instàncies de tasca"                  |
| Task Group            | Grup de tasques                |                                                |
| Dag Run               | Execució de Dag                | Plural: "Execucions de Dag"                    |
| Backfill              | Reompliment                    | Plural: "Reompliments"                         |
| Trigger (noun)        | Disparador                     |                                                |
| Trigger Rule          | Regla d'execució               |                                                |
| Triggerer             | Triggerer                      | Component name; keep English in technical refs |
| Scheduler             | Programador                    |                                                |
| Schedule (noun)       | Programació                    |                                                |
| Executor              | Executor                       |                                                |
| Connection            | Connexió                       | Plural: "Connexions"                           |
| Variable              | Variable                       | Plural: "Variables"                            |
| Audit Log             | Registre d'auditoria           |                                                |
| Log                   | Registre                       |                                                |
| State                 | Estat                          |                                                |
| Queue (noun)          | Cua                            | e.g., "En cua" for "queued"                    |
| Config / Configuration| Configuració                   |                                                |
| Operator              | Operador                       | Plural: "Operadors"                            |
| Asset Event           | Esdeveniment d'Asset           | Keep "Asset" in English                        |
| Dag Processor         | Dag Processor                  | Component name; keep English                   |
| Heartbeat             | Batec                          |                                                |
| Plugin                | Extensió                       | In UI nav/labels only                          |

## 3. Task/Run States

| English State       | Catalan Translation      |
| ------------------- | ------------------------ |
| running             | Executant-se             |
| failed              | Fallit                   |
| success             | Exitós                   |
| queued              | En cua                   |
| scheduled           | Programat                |
| skipped             | Saltat                   |
| deferred            | Diferit                  |
| removed             | Eliminat                 |
| restarting          | Reiniciant               |
| up_for_retry        | A reintentar             |
| up_for_reschedule   | A reprogramar            |
| upstream_failed     | Fallit aigües amunt      |
| no_status / none    | Sense estat              |
| planned             | Planificat               |
| open                | Obert                    |

## 4. Catalan-Specific Guidelines

### Tone and Register

- Use a **formal, professional register** suitable for technical software UIs.
- Avoid colloquialisms; prefer neutral and precise language.
- Keep UI strings concise — they appear in buttons, labels, and tooltips.

### Gender Agreement

- Catalan nouns have grammatical gender; match adjectives and articles accordingly:
  - "Dag" is treated as **masculine**: "el Dag", "un Dag"
  - "Tasca" is **feminine**: "la tasca", "una tasca"
  - "Execució" is **feminine**: "una execució", "l'execució"
  - "Connexió" is **feminine**: "la connexió"
  - "Variable" is **feminine**: "la variable"
  - "Instància" is **feminine**: "la instància"

### Plural Forms

- Catalan uses i18next plural suffixes `_one` and `_other`.
  Add `-s` or `-es` according to standard Catalan rules, or use the established
  glossary form:

  ```json
  "task_one": "Tasca",
  "task_other": "Tasques"
  ```

  ```json
  "dagRun_one": "Execució de Dag",
  "dagRun_other": "Execucions de Dag"
  ```

### Capitalization

- Use **sentence case** for descriptions and longer strings.
- Use **title-like capitalization** for headers, labels, and button text
  (match the style of existing translations).
- Capitalize proper terms: "Dag", "Asset", "XCom", "Pool", etc.
- Do **not** capitalize common nouns mid-sentence.

### Elision and Contractions

- Apply standard Catalan elision and contraction rules:
  - `de` + vowel → `d'` (e.g., "Registre d'auditoria", "ID d'execució")
  - `el`/`la` + vowel → `l'` (e.g., "l'execució", "l'operador")
- Prepositions `a` + `el` → `al`; `de` + `el` → `del`

### Diacritics

- Always preserve Catalan diacritics: `à`, `è`, `é`, `ï`, `ò`, `ó`, `ú`, `ü`, `ç`, `·` (interpunct in `l·l`).
- Never drop or substitute accents.

### Word Order

- Standard SVO word order — similar to English.
- Adjectives typically follow nouns (e.g., "interval de dates", "execució activa").

## 5. Examples from Existing Translations

**Always keep in English:**

- "Dag" → "Dag"
- "Asset" → "Asset"
- "XCom" → "XCom"
- "Pool" → "Pool"
- "Provider" → "Provider"

**Common translation patterns:**

```
task_one              → "Tasca"
task_other            → "Tasques"
dagRun_one            → "Execució de Dag"
dagRun_other          → "Execucions de Dag"
backfill_one          → "Reompliment"
backfill_other        → "Reompliments"
taskInstance_one      → "Instància de tasca"
taskInstance_other    → "Instàncies de tasca"
assetEvent_one        → "Esdeveniment d'Asset"
assetEvent_other      → "Esdeveniments d'Asset"
running               → "Executant-se"
failed                → "Fallit"
success               → "Exitós"
queued                → "En cua"
scheduled             → "Programat"
```

**Action verbs (buttons):**

```
Add     → "Afegir"
Delete  → "Eliminar"
Edit    → "Editar"
Save    → "Desar"
Reset   → "Restablir"
Cancel  → "Cancel·lar"
Confirm → "Confirmar"
Import  → "Importar"
Export  → "Exportar"
Search  → "Cercar"
Filter  → "Filtrar"
```

## 6. Agent Instructions (DO / DON'T)

**DO:**

- Match tone, style, gender agreement, and casing from existing `ca/*.json` files
- Use formal Catalan register throughout
- Preserve all i18next placeholders: `{{count}}`, `{{dagName}}`, `{{type}}`, etc.
- Apply correct Catalan elision (`d'`, `l'`, contractions `al`, `del`)
- Preserve all diacritics (à, è, é, ï, ò, ó, ú, ü, ç, ·)
- Provide all needed plural suffixes (`_one`, `_other`) for each key

**DON'T:**

- Translate Airflow-specific terms listed in section 1
- Drop or substitute diacritics (e.g., never write "Execucio" for "Execució")
- Change hotkey values (e.g., `"hotkey": "e"` must stay `"e"`)
- Invent new vocabulary when an equivalent already exists in the current translations
- Use "DAG" — always write "Dag"
- Capitalize common nouns mid-sentence

---

**Version:** 1.0 — derived from existing `ca/*.json` locale files (March 2026)

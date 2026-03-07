<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Hungarian (hu) Translation Agent Skill

**Locale code:** `hu`
**Preferred variant:** Standard Hungarian (hu), consistent with existing translations in `airflow-core/src/airflow/ui/public/i18n/locales/hu/`

This file contains locale-specific guidelines so AI translation agents produce
new Hungarian strings that stay 100% consistent with the existing translations.

## 1. Core Airflow Terminology

The following terms **must remain in English unchanged** (case-sensitive):

- `Dag` / `Dags` — Airflow concept; never write "DAG"
- `XCom` / `XComs` — Airflow cross-communication mechanism
- `Pool` / `Pools` — Resource constraint mechanism
- `Provider` / `Providers` — Airflow extension package name
- `Map Index`
- `PID`
- `ID` — Note: Sometimes used as "Azonosító" in labels
- `UTC`
- `JSON`
- `REST API`
- Log levels: `INFO`, `DEBUG` (Note: `CRITICAL`, `ERROR`, `WARNING` are translated)

## 2. Standard Translations

The following Airflow-specific terms have established Hungarian translations
that **must be used consistently**:

| English Term          | Hungarian Translation             | Notes                                      |
| --------------------- | --------------------------------- | ------------------------------------------ |
| Task                  | Feladat                           |                                            |
| Task Instance         | Feladatpéldány                    | Plural: "Feladatpéldányok"                 |
| Task Group            | Feladatcsoport                    |                                            |
| Dag Run               | Dag futás                         | Plural: "Dag futások"                      |
| Run                   | Futás                             | Plural: "Futások"; used standalone         |
| Backfill              | Visszatöltés / Backfill           | "Visszatöltés" is preferred                |
| Trigger (noun)        | Indító                            |                                            |
| Trigger Rule          | Indítási szabály                  |                                            |
| Triggerer             | Indító                            | Component name                             |
| Scheduler             | Ütemező                           |                                            |
| Schedule (noun)       | Ütemezés                          |                                            |
| Executor              | Végrehajtó                        |                                            |
| Connection            | Kapcsolat                         | Plural: "Kapcsolatok"                      |
| Variable              | Változó                           | Plural: "Változók"                         |
| Audit Log             | Audit napló                       |                                            |
| Log                   | Napló                             | Plural: "Naplók"                           |
| State                 | Állapot                           |                                            |
| Queue (noun)          | Sor                               | e.g., "Sorban áll" for "queued"            |
| Config / Configuration| Beállítások / Konfiguráció        | Use "Beállítások" in Admin menu            |
| Operator              | Operátor                          |                                            |
| Asset                 | Adatkészlet (asset)               | Usually kept as "(asset)" for clarity      |
| Asset Event           | Adatkészlet esemény               |                                            |
| Plugin / Plugins      | Bővítmény / Bővítmények           |                                            |
| Pools                 | Poolok                            |                                            |
| Providers             | Szolgáltatók                      |                                            |
| Upstream              | Felfelé mutató (upstream)         |                                            |
| Downstream            | Lefelé mutató (downstream)         |                                            |
| Active (Dag)          | Aktív                             |                                            |
| Paused (Dag)          | Szüneteltetett                    |                                            |

## 3. Task/Run States and Log Levels

### States

| English State       | Hungarian Translation |
| ------------------- | --------------------- |
| running             | Fut                   |
| failed              | Sikertelen            |
| success             | Sikeres               |
| queued              | Sorban áll            |
| scheduled           | Ütemezett             |
| skipped             | Kihagyva              |
| deferred            | Várakozó              |
| removed             | Eltávolítva           |
| restarting          | Újraindítás           |
| up_for_retry        | Újrapróbálkozásra vár |
| up_for_reschedule   | Újraütemezésre vár    |
| upstream_failed     | Előfeltétel sikertelen |
| no_status / none    | Nincs állapot         |
| planned             | Tervezett             |

### Log Levels

| English Level | Hungarian Translation |
| ------------- | --------------------- |
| CRITICAL      | KRITIKUS              |
| ERROR         | HIBA                  |
| WARNING       | FIGYELMEZTETÉS        |
| INFO          | INFO                  |
| DEBUG         | DEBUG                 |

## 4. Hungarian-Specific Guidelines

### Tone and Register

- Use **formal Hungarian** ("Ön" form / Önözés). Do not use informal "te".
- Use a neutral, professional tone suitable for technical software UIs.
- Keep UI strings concise — they appear in buttons, labels, and tooltips.

### Inflection and Word Order

- Hungarian is an agglutinative language, but inflecting i18next placeholders (`{{count}}`) is difficult. Try to phrase sentences so the variable doesn't need suffixes (e.g., "Összesen: {{count}}" instead of "{{count}}-ból").
- Use a hyphen for inflecting "Dag" if necessary: "Dag-ek" (Plural), "Dag-et" (Accusative).

### Plural Forms

- In Hungarian, nouns stay in **singular form** after numbers and quantity words (e.g., "5 feladat" not "5 feladatok", and "Összes feladat" not "Összes feladatok").
- i18next uses `_one` and `_other`. For Hungarian, ensure the noun following a number stays singular in the `_other` translation if it's following a count.

  ```json
  "task_one": "Feladat",
  "task_other": "Feladat"
  ```

  *(Note: If the word is used alone as a plural (e.g., "Tasks"), use the plural "Feladatok". However, if it follows a quantity word (e.g., "All Tasks"), use the singular "Összes feladat".)*

### Capitalization

- Use **sentence case** for descriptions and longer strings.
- Use the capitalization style of existing translations for headers and buttons.
- Preserve proper terms: "Dag", "XCom", "Pool".

## 5. Examples from Existing Translations

**Always keep in English:**

- "Dag"
- "XCom"
- "Pool"

**Common translation patterns:**

```
task_one          → "Feladat"
task_other        → "Feladatok" (without number)
dagRun_one        → "Dag futás"
dagRun_other      → "Dag futások"
run_one           → "Futás"
run_other         → "Futások" (without number)
backfill_one      → "Visszatöltés"
backfill_other    → "Visszatöltések"
taskInstance_one  → "Feladatpéldány"
taskInstance_other→ "Feladatpéldányok"
plugin_one        → "Bővítmény"
plugin_other      → "Bővítmények"
running           → "Fut"
failed            → "Sikertelen"
success           → "Sikeres"
queued            → "Sorban áll"
scheduled         → "Ütemezett"
```

**Action verbs (buttons):**

```
Add    → "Hozzáadás"
Delete → "Törlés"
Edit   → "Szerkesztés"
Save   → "Mentés"
Reset  → "Alaphelyzetbe állítás"
Cancel → "Mégse"
Confirm→ "Megerősítés"
Import → "Importálás"
Export → "Exportálás"
Search → "Keresés"
Filter → "Szűrő"
```

## 6. Agent Instructions (DO / DON'T)

**DO:**

- Match tone, style, and terminology from existing `hu/*.json` files.
- Use formal Hungarian ("Ön" / "Önözés") throughout.
- Preserve all i18next placeholders: `{{count}}`, `{{dagName}}`, `{{type}}`, etc.
- Follow Hungarian grammar for singulars after numbers.
- Translate log levels: `KRITIKUS`, `HIBA`, `FIGYELMEZTETÉS`.

**DON'T:**

- Translate Airflow-specific terms listed in section 1 (except for log levels).
- Use informal language ("te").
- Change hotkey values (e.g., `"hotkey": "e"` must stay `"e"`).
- Invent new vocabulary when an equivalent already exists in current translations.
- Use "DAG" — always write "Dag".

---

**Version:** 1.0 — derived from existing `hu/*.json` locale files (February 2026)

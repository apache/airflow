<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Hebrew (he) Translation Agent Skill

**Locale code:** `he`
**Preferred variant:** Modern Hebrew (he), consistent with existing translations in `airflow-core/src/airflow/ui/public/i18n/locales/he/`

This file contains locale-specific guidelines so AI translation agents produce
new Hebrew strings that stay fully consistent with the existing translations.

## 1. Core Airflow Terminology

### Global Airflow terms (never translate)

These terms are defined as untranslatable across **all** Airflow locales.
Do not translate them regardless of language:

- `Airflow` — Product name
- `Dag` / `Dags` — Airflow concept; never write "DAG"
- `XCom` / `XComs` — Airflow cross-communication mechanism
- `UTC` — Time standard
- `JSON` — Standard technical format name
- `REST API` — Standard technical term
- `Unix` — Operating system name
- Log levels: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`

### Translated by convention (Hebrew-specific)

The existing Hebrew translations translate most Airflow terms into native Hebrew.
These established translations **must be used consistently**:

- `Asset` / `Assets` → `נכס` / `נכסים`
- `Backfill` → `השלמה למפרע` / `השלמות למפרע`
- `Plugin` / `Plugins` → `תוסף` / `תוספים`
- `Pool` / `Pools` → `מאגר משאבים`
- `Provider` / `Providers` → `חבילות עזר`
- `Trigger` / `Triggerer` → `מפעיל` (component noun)
- `Executor` → `מבצע`
- `Heartbeat` → `עדכון חיים` (e.g., "עדכון חיים אחרון" for "Latest Heartbeat")

## 2. Standard Translations

| English Term          | Hebrew Translation            | Notes                                          |
| --------------------- | ----------------------------- | ---------------------------------------------- |
| Task                  | משימה                         |                                                |
| Task Instance         | מופע משימה                    |                                                |
| Task Group            | קבוצת משימות                  |                                                |
| Dag Run               | הרצת Dag                      |                                                |
| Trigger (verb)        | הפעלה                         | "מופעל על-ידי" for "Triggered by"              |
| Trigger Rule          | כלל הפעלה                     |                                                |
| Scheduler             | מתזמן                         |                                                |
| Schedule (noun)       | תזמון                         |                                                |
| Operator              | אופרטור                       |                                                |
| Connection            | חיבור                         |                                                |
| Variable              | משתנה                         |                                                |
| Configuration         | הגדרות                        |                                                |
| Audit Log             | יומן ביקורת                   |                                                |
| State                 | מצב                           |                                                |
| Queue (noun)          | בתור                           | "תור" for "queued"                            |
| Duration              | משך זמן                       |                                                |
| Owner                 | בעלים                         |                                                |
| Tags                  | תגיות                         |                                                |
| Description           | תיאור                         |                                                |
| Documentation         | תיעוד                         |                                                |
| Timezone              | אזור זמן                      |                                                |
| Dark Mode             | מצב כהה                       |                                                |
| Light Mode            | מצב בהיר                      |                                                |
| Asset Event           | אירוע נכס                     |                                                |
| Dag Processor         | מעבד Dag                      |                                                |
| Try Number            | מספר נסיון                    |                                                |

## 3. Task/Run States

| English State       | Hebrew Translation            |
| ------------------- | ----------------------------- |
| running             | בריצה                         |
| failed              | נכשלו                         |
| success             | הצליחו                        |
| queued              | בתור                          |
| scheduled           | בתזמון                        |
| skipped             | דולגו                         |
| deferred            | בהשהייה                       |
| removed             | הוסרו                         |
| restarting          | בהפעלה מחדש                   |
| up_for_retry        | בהמתנה לניסיון חוזר           |
| up_for_reschedule   | בהמתנה לתזמון מחדש            |
| upstream_failed     | משימות קודמות נכשלו           |
| no_status / none    | ללא סטטוס                     |
| planned             | בתכנון                        |

## 4. Hebrew-Specific Guidelines

### Tone and Register

- Use a **neutral, professional Hebrew** tone suitable for technical software UIs.
- The existing translations use masculine forms for imperatives and general references. Follow this established convention for consistency.
- Keep UI strings concise — they appear in buttons, labels, and tooltips.

### Right-to-Left (RTL) Considerations

- Hebrew is an RTL language. UI layout should flip accordingly.
- When mixing Hebrew and English (e.g., "הרצת Dag"), the LTR English term will naturally appear in the correct reading order within an RTL context.
- Preserve all i18next placeholders exactly as-is: `{{count}}`, `{{dagName}}`, etc.

### Plural Forms

- Hebrew uses i18next plural suffixes `_one`, `_two`, and `_other`. For most Airflow UI strings `_two` will be identical to `_other`, but check existing translations and keep the `_two` key when it is present.
- Note: colloquial Hebrew has a true dual form for things that come in pairs (e.g. one sock = גרב, two socks = גרביים, not "2 גרבים"). This rarely applies to Airflow UI terminology but is worth being aware of.

  ```json
  "task_one": "משימה",
  "task_other": "משימות"
  ```

  ```json
  "dagRun_one": "הרצת Dag",
  "dagRun_other": "הרצת Dags"
  ```

### Capitalization of English terms

- For English terms embedded in Hebrew strings, preserve their original casing (e.g., "Dag", "XCom", "Dags").

## 5. Examples from Existing Translations

**Terms translated to Hebrew:**

```
Asset          → "נכס"
Backfill       → "השלמה למפרע"
Pool           → "מאגר משאבים"
Plugin         → "תוסף"
Provider       → "חבילות עזר"
Executor       → "מבצע"
Trigger        → "מפעיל"
Heartbeat      → "עדכון חיים"
```

**Common translation patterns:**

```
task_one              → "משימה"
task_other            → "משימות"
dagRun_one            → "הרצת Dag"
dagRun_other          → "הרצת Dags"
backfill_one          → "השלמה למפרע"
backfill_other        → "השלמות למפרע"
taskInstance_one      → "מופע משימה"
taskInstance_other    → "מופעי משימות"
running               → "בריצה"
failed                → "נכשלו"
success               → "הצליחו"
queued                → "בתור"
scheduled             → "בתזמון"
```

**Action verbs (buttons):**

```
Add      → "הוסף"
Delete   → "מחק"
Save     → "שמור"
Reset    → "אתחל"
Cancel   → "בטל"
Confirm  → "אשר"
Download → "הורד"
Expand   → "הרחב"
Collapse → "צמצם"
Filter   → "סנן"
```

**Triggerer compound nouns:**

```
triggerer.class           → "סוג מפעיל"
triggerer.id              → "מזהה מפעיל"
triggerer.createdAt       → "זמן יצירת מפעיל"
triggerer.assigned        → "מפעיל מוקצה"
triggerer.latestHeartbeat → "עדכון חיים אחרון"
triggerer.title           → "פרטי מפעיל"
```

## 6. Agent Instructions (DO / DON'T)

**DO:**

- Match tone, style, and terminology from existing `he/*.json` files
- Use professional, neutral Hebrew
- Preserve all i18next placeholders: `{{count}}`, `{{dagName}}`, `{{hotkey}}`, etc.
- Use construct state (סמיכות) for compound nouns as established
- Provide all needed plural suffixes (`_one`, `_other`) for each plural key
- Check existing translations before adding new ones to maintain consistency

**DON'T:**

- Write "DAG" — always write "Dag"
- Use colloquial or slang Hebrew
- Invent new vocabulary when an equivalent already exists in the current translations
- Change hotkey values (e.g., `"hotkey": "e"` must stay `"e"`)
- Translate variable names or placeholders inside `{{...}}`
- Add Hebrew prefixed prepositions to English terms (e.g., don't write "ב-Dag", use "ב-Dag" only if established)

---

**Version:** 1.0 — derived from existing `he/*.json` locale files (April 2026)

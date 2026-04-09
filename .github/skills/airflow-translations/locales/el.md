<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Greek (el) Translation Agent Skill

**Locale code:** `el`
**Preferred variant:** Standard Modern Greek (el), consistent with existing translations in `airflow-core/src/airflow/ui/public/i18n/locales/el/`

This file contains locale-specific guidelines so AI translation agents produce
new Greek strings that stay 100% consistent with the existing translations.

## 1. Core Airflow Terminology

The following terms **must remain in English unchanged** (case-sensitive):

- `Dag` / `Dags` — Airflow concept; never write "DAG"
- `XCom` / `XComs` — Airflow cross-communication mechanism
- `Backfill` / `Backfills` — Historical data fill-in; kept as a recognizable technical term
- `Pool` / `Pools` — Resource constraint mechanism
- `Slot` / `Slots` — Pool slot count
- `Map Index` — Task mapping index
- `PID` — Unix process identifier
- `ID` — Universal abbreviation
- `UTC` — Time standard
- `JSON` — Standard technical format name
- `REST API` — Standard technical term
- `URI` — Uniform Resource Identifier
- `Gantt` — Chart type name
- `Catchup` — Dag scheduling catchup setting
- Log levels: `INFO`, `DEBUG` (Note: `CRITICAL`, `ERROR`, `WARNING` are translated — see § 3)

## 2. Standard Translations

The following Airflow-specific terms have established Greek translations
that **must be used consistently**:

| English Term           | Greek Translation                     | Notes                                               |
| ---------------------- | ------------------------------------- | --------------------------------------------------- |
| Task                   | Εργασία                               | Plural: "Εργασίες"                                  |
| Task Instance          | Εκτέλεση Εργασίας                     | Plural: "Εκτελέσεις Εργασίας"                       |
| Task Group             | Ομάδα Εργασιών                        |                                                     |
| Dag Run                | Εκτέλεση Dag                          | Plural: "Εκτελέσεις Dag"                            |
| Run                    | Εκτέλεση                              | Plural: "Εκτελέσεις"; used standalone               |
| Trigger (noun)         | Ενεργοποίηση                          |                                                     |
| Trigger Rule           | Κανόνας Ενεργοποίησης                 |                                                     |
| Triggerer              | Ενεργοποιητής                         | Component name                                      |
| Scheduler              | Προγραμματιστής                       |                                                     |
| Schedule (noun)        | Πρόγραμμα                             |                                                     |
| Executor               | Εκτελεστής                            |                                                     |
| Connection             | Σύνδεση                               | Plural: "Συνδέσεις"                                 |
| Variable               | Μεταβλητή                             | Plural: "Μεταβλητές"                                |
| Audit Log              | Καταγραφή Ελέγχου                     |                                                     |
| Log                    | Καταγραφή                             |                                                     |
| State                  | Κατάσταση                             |                                                     |
| Queue (noun)           | Ουρά                                  | e.g., "Σε Ουρά" for "queued"                        |
| Config / Configuration | Ρυθμίσεις                             |                                                     |
| Operator               | Τελεστής                              | Plural: "Τελεστές"                                  |
| Asset                  | Οντότητα                              | Plural: "Οντότητες" — translated (Greek-specific)   |
| Asset Event            | Συμβάν Οντότητας                      | Plural: "Συμβάντα Οντοτήτων"                        |
| Plugin                 | Πρόσθετο                              | Plural: "Πρόσθετα"                                  |
| Provider               | Πάροχος                               | Plural: "Πάροχοι"                                   |
| Dag Processor          | Επεξεργαστής Dag                      | Component name                                      |
| Heartbeat              | Παλμός                                |                                                     |
| Map Index              | Δείκτης Χάρτη                         |                                                     |
| Upstream (dependency)  | Ανάντη                                | Used in states: "Αποτυχία Ανάντη"                   |
| Upstream (action)      | Άνοδος                                | Used in clear-task action options                   |
| Downstream (action)    | Κάθοδος                               | Used in clear-task action options                   |

> **Note on `Asset`:** Unlike French, Catalan, and other locales where "Asset" is kept in
> English, Greek translates it as **Οντότητα** ("entity"). Use "Οντότητα" consistently
> across all Greek translations.

## 3. Task/Run States and Log Levels

### States

| English State       | Greek Translation            |
| ------------------- | ---------------------------- |
| running             | Εκτελείται                   |
| failed              | Αποτυχία                     |
| success             | Επιτυχία                     |
| queued              | Σε Ουρά                      |
| scheduled           | Προγραμματισμένο             |
| skipped             | Παραλείφθηκε                 |
| deferred            | Αναβληθέν                    |
| removed             | Αφαιρέθηκε                   |
| restarting          | Επανεκκίνηση                 |
| up_for_retry        | Προς Επανάληψη               |
| up_for_reschedule   | Προς Επαναπρογραμματισμό     |
| upstream_failed     | Αποτυχία Ανάντη              |
| no_status / none    | Χωρίς Κατάσταση              |
| planned             | Προγραμματισμένο             |

### Log Levels

| English Level | Greek Translation  |
| ------------- | ------------------ |
| CRITICAL      | ΚΡΙΣΙΜΟ            |
| ERROR         | ΣΦΑΛΜΑ             |
| WARNING       | ΠΡΟΕΙΔΟΠΟΙΗΣΗ      |
| INFO          | INFO               |
| DEBUG         | DEBUG              |

## 4. Greek-Specific Guidelines

### Tone and Register

- Use **formal Greek** ("εσείς/σας" form). Do not use the informal "εσύ/σου".
- Use a neutral, professional tone suitable for technical software UIs.
- Keep UI strings concise — they appear in buttons, labels, and tooltips.

### Grammatical Gender

Greek nouns have three genders: masculine (αρσενικό), feminine (θηλυκό), and neuter (ουδέτερο).
Match adjectives and articles accordingly:

- "Dag" is treated as **neuter**: "το Dag", "ένα Dag"
- "Εργασία" (Task) is **feminine**: "η εργασία", "μια εργασία"
- "Εκτέλεση" (Run/Execution) is **feminine**: "η εκτέλεση", "μια εκτέλεση"
- "Σύνδεση" (Connection) is **feminine**: "η σύνδεση"
- "Μεταβλητή" (Variable) is **feminine**: "η μεταβλητή"
- "Οντότητα" (Asset) is **feminine**: "η οντότητα"
- "Καταγραφή" (Log) is **feminine**: "η καταγραφή"
- "Κατάσταση" (State) is **feminine**: "η κατάσταση"

### Plural Forms

Greek uses i18next plural suffixes `_one` and `_other`. Use the established
plural forms from the glossary:

```json
"task_one": "Εργασία",
"task_other": "Εργασίες"
```

```json
"dagRun_one": "Εκτέλεση Dag",
"dagRun_other": "Εκτελέσεις Dag"
```

```json
"asset_one": "Οντότητα",
"asset_other": "Οντότητες"
```

### Genitive Case

Greek uses the genitive case to express "of X" relationships. This commonly appears
in compound terms where English uses a noun modifier:

```json
"dagRunId":      "ID Εκτέλεσης Dag"       // "ID of Run of Dag"
"taskGroup":     "Ομάδα Εργασιών"          // "Group of Tasks"
"auditLog":      "Καταγραφή Ελέγχου"       // "Log of Audit"
"assetEvent_one":"Συμβάν Οντότητας"        // "Event of Asset"
"triggerRule":   "Κανόνας Ενεργοποίησης"   // "Rule of Triggering"
```

### Capitalization

- Use **sentence case** for descriptions and longer strings.
- Use **title-like capitalization** for headers, labels, and button text
  (match the style of existing Greek translations).
- Capitalize proper terms: "Dag", "XCom", "Backfill", "Pool", etc.

### Diacritics

Greek uses the **tonos** accent mark (΄). Always preserve accented characters —
missing diacritics change the meaning or make text unreadable. Never write
"Εκτελεση", "Συνδεση", "Εργασια", "Οντοτητα", "Καταγραφη", "Κατασταση" etc.
Always use the correctly accented forms from §2.

### Question Mark

The Greek question mark is the **erotimatiko** (`;`), which looks like a semicolon.
When translating English `?`, use `;` in Greek sentences:

```json
"confirmation": "Είστε σίγουροι ότι θέλετε να διαγράψετε το {{resourceName}}; Αυτή η ενέργεια δεν μπορεί να αναιρεθεί."
```

### Placeholders and Variables

Preserve all `{{variable}}` placeholders exactly — never translate placeholder names.
Reorder phrases for natural Greek word order when needed.

## 5. Terminology Reference

The established Greek translations are defined in the existing locale files.
Before translating, **read the existing el JSON files** to learn the
established terminology:

```
airflow-core/src/airflow/ui/public/i18n/locales/el/
```

Use the translations found in these files as the authoritative glossary. When
translating a term, check how it has been translated elsewhere in the locale to
maintain consistency. If a term has not been translated yet, refer to the English
source in `en/` and apply the rules in this document.

### Action Verbs (Buttons)

```
Add     → "Προσθήκη"
Delete  → "Διαγραφή"
Edit    → "Επεξεργασία"
Save    → "Αποθήκευση"
Reset   → "Επαναφορά"
Cancel  → "Ακύρωση"
Confirm → "Επιβεβαίωση"
Clear   → "Εκκαθάριση"
Search  → "Αναζήτηση"
Copy    → "Αντιγραφή"
```

## 6. Agent Instructions (DO / DON'T)

**DO:**

- Use formal Greek ("εσείς/σας" form) throughout
- Preserve all i18next placeholders: `{{count}}`, `{{dagName}}`, `{{type}}`, etc.
- Apply correct Greek genitive case for "of X" constructions
- Provide `_one` and `_other` suffixes for every plural key
- Translate "Asset" as "Οντότητα" (Greek-specific — not kept in English)
- Translate log levels: "ΚΡΙΣΙΜΟ", "ΣΦΑΛΜΑ", "ΠΡΟΕΙΔΟΠΟΙΗΣΗ" for CRITICAL, ERROR, WARNING
- Preserve Greek diacritics (tonos accent) on all Greek words
- Use `;` (erotimatiko) for question marks in Greek sentences

**DON'T:**

- Write "DAG" — always use "Dag"
- Translate `XCom`, `Backfill`, `Pool`, `Slot`, `ID`, `JSON`, `REST API`, `UTC`, `PID`
- Translate `{{variable}}` placeholder names
- Drop Greek diacritics (e.g., never write "Εκτελεση" for "Εκτέλεση")
- Use the informal "εσύ" form
- Translate "INFO" or "DEBUG" log level labels
- Use English `?` for question marks in Greek sentences

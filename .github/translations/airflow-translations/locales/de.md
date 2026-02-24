<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# German (de) Translation Agent Skill

This file contains German language specific guidelines for German translations in the Airflow UI.

---

## 1. Core Airflow Terminology

The following terms **must remain in English unchanged** (case-sensitive):

- `Dag` / `Dags` — Airflow concept; never write "DAG". The term is treated as neutral in German ("das Dag").
- `Pool` / `Pools` — Untranslated.
- `Provider` / `Providers` — Untranslated.
- `Operator` / `Operatoren` — Technical implementation term; remains "Operator" in singular and "Operatoren" in plural.
- `ID` — Universal abbreviation (capitalized).
- Log levels: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG` — **must remain unchanged**.

### Code & Technical Identifiers (Never Translate)

- Python class names
- Function names
- Variable names
- Configuration keys (e.g., `dag_id`, `task_id`)
- CLI flags
- File paths
- URLs
- Environment variable names

---

## 2. Standard Translations

The following translations **must be used consistently**.

### Core Concepts

| English Term        | German Translation            |
|---------------------|------------------------------|
| Asset               | Datenset (Asset)             |
| Asset Event         | Ereignis zu Datenset (Asset) |
| Backfill            | Auffüllen                    |
| Bundle              | Bündel                       |
| Catchup             | Nachholen                    |
| Connection          | Verbindung                   |
| Dag ID              | Dag ID                       |
| Dag Run             | Dag Lauf                     |
| Dag Run ID          | Dag Lauf ID                  |
| Dag Version         | Dag Version                  |
| Dag Processor       | Dag Prozessor                |
| Task                | Task                         |
| Task ID             | Task ID                      |
| Task Instance       | Task Instanz                 |
| Task Group          | Task Gruppe                  |
| XCom                | Task Kommunikation (XCom)    |

### Admin & Configuration

| English Term            | German Translation                          |
|-------------------------|---------------------------------------------|
| Admin                   | Administrator                               |
| Configuration / Config  | Konfiguration                               |
| Connection Type         | Verbindungstyp                              |
| Variable                | Variable                                    |
| Pool Slots              | Pool Belegung                               |
| Plugin                  | Plug-in                                     |
| Provider                | Provider                                    |
| Secret Manager          | Secret-Manager                              |
| Environment Variable    | Umgebungsvariable                           |
| Docs                    | Doku                                        |

### Execution & Scheduling

| English Term     | German Translation                          |
|------------------|---------------------------------------------|
| Deferred         | Delegiert                                   |
| Executor         | Ausführungsumgebung                         |
| Executor Config  | Konfiguration der Ausführungsumgebung       |
| Map Index        | Planungs-Index                              |
| Operator         | Operator                                    |
| Scheduled        | Geplant                                     |
| Schedule         | Zeitplan                                    |
| Scheduler        | Planer                                      |
| Reschedule       | Neuplanung                                  |
| Trigger (verb)   | Auslösen                                    |
| Triggered        | Ausgelöst                                   |
| Triggered By     | Ausgelöst durch                             |
| Triggering User  | Benutzer der ausgelöst hat                  |
| Trigger Rule     | Auslöse-Regel                               |
| Triggerer        | Triggerer                                   |
| Try Number       | Versuch Nummer                              |
| Max Tries        | Maximale Versuche                           |
| Retries          | Wiederholungen                              |
| Queue            | Warteschlange                               |
| Queued At        | Wartend seit                                |
| Run After        | Gelaufen ab                                 |
| Run Type         | Typ des Laufs                               |
| Upstream         | Vorgänger                                   |
| Downstream       | Nachfolger                                  |

### Data & Time

| English Term         | German Translation              |
|----------------------|---------------------------------|
| Data Interval Start  | Datenintervall Start            |
| Data Interval End    | Datenintervall Ende             |
| Duration             | Laufzeit                        |
| Start Date           | Startdatum                      |
| End Date             | Enddatum                        |
| Logical Date         | Logisches Datum                 |
| Timestamp            | Zeitstempel                     |
| Timezone             | Zeitzone                        |
| Created At           | Erstellt um                     |
| Completed At         | Abgeschlossen um                |
| Last Parsed          | Letztmalig eingelesen           |
| Last Parse Duration  | Letzte Dag Einlesedauer         |
| Last Heartbeat       | Letztes Lebenszeichen           |
| Latest Dag Version   | Letzte Dag Version              |
| Latest Run           | Letzter Lauf                    |
| Next Run             | Nächster Lauf                   |

### UI Elements & Actions

| English Term | German Translation |
|--------------|-------------------|
| Add          | Hinzufügen        |
| Delete       | Löschen           |
| Edit         | Bearbeiten        |
| Save         | Speichern         |
| Reset        | Zurücksetzen      |
| Cancel       | Abbrechen         |
| Confirm      | Bestätigen        |
| Import       | Importieren       |
| Export       | Exportieren       |
| Search       | Suchen            |
| Filter       | Filter / Filtern  |
| Copy         | Kopieren          |
| Download     | Herunterladen     |
| Close        | Schließen         |
| Expand       | Ausklappen        |
| Collapse     | Einklappen        |

---

## 3. Tone and Register

- Use formal Standard German (Hochdeutsch).
- Always use formal address ("Sie"), never informal "du".
- Maintain a neutral, professional tone suitable for technical software interfaces.
- Avoid regional variants.

Buttons and UI actions use infinitive form:

Correct:

- Speichern
- Löschen
- Zurücksetzen

Incorrect:

- Speichere
- Lösche

---

## 4. Consistency Rules (DO / DON'T)

### DO

- Match tone and casing from existing `de/*.json` files.
- Preserve placeholders: `{{count}}`, `{{dagName}}`, `{{type}}`, etc.
- Keep all technical identifiers unchanged.
- Reuse established terminology.
- Follow proper hyphenation rules for terms like Plug-in
- Always use formal address (Sie) for neutrality and professionalism

### DON'T

- Do not write "DAG" — always "Dag".
- Do not invent synonyms if a translation already exists.
- Do not translate log level names.
- Do not change hotkey values.
- Do not switch to informal address ("du").

---

**Version:** 1.0 — aligned with existing `de` locale files (February 2026)

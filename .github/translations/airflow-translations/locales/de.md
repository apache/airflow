 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [German (de) Translation Agent Skill](#german-de-translation-agent-skill)
  - [1. Core Airflow Terminology](#1-core-airflow-terminology)
  - [2. Standard Translations](#2-standard-translations)
  - [3. Tone and Register](#3-tone-and-register)
  - [4. Consistency Rules (DO / DON'T)](#4-consistency-rules-do--dont)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# German (de) Translation Agent Skill

This file contains German language specific guidelines for German translations in the Airflow UI.

---

## 1. Core Airflow Terminology

The following terms **must remain in English unchanged** (case-sensitive):

- `Dag` / `Dags` — Airflow concept; never write "DAG". The term is treated as neutral in German ("das Dag"). The term is intentionally not translated because it is a core concept in Airflow and widely used in documentation and code.
- `Pool` / `Pools` — Untranslated.
- `Provider` / `Providers` — Untranslated.
- `Operator` / `Operatoren` — Technical implementation term; remains "Operator" in singular and "Operatoren" in plural.
- `ID` — Universal abbreviation (capitalized).
- Log levels: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG` — **must remain unchanged**.
- `Triggerer` - Component name in Airflow responsible for handling deferred tasks. The term remains untranslated to stay consistent with the codebase and documentation.

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

### Core Concepts

| English Term  | German Translation           | Rationale for Translation                                                                                                                                                                                                     |
|---------------|------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Asset         | Datenset (Asset)             | Since the term is relatively new in Airflow and no established German terminology exists yet, however "Datenset" reflects the concept well. The original English term is kept in parentheses so users can still recognize it. |
| Asset Event   | Ereignis zu Datenset (Asset) | Logical continuation of the translation for "Asset".                                                                                                                                                                          |
| Backfill      | Auffüllen                    | Reflects the technical meaning that missing or past runs are executed afterwards to fill gaps. Alternatives such as "Nachverarbeitung" would be less precise in meaning.                                                      |
| Bundle        | Bündel                       | Direct translation that fits well with the intended meaning of grouping related elements together.                                                                                                                            |
| Catchup       | Nachholen                    | Direct translation describing the concept of executing previously missed scheduled runs.                                                                                                                                      |
| Connection    | Verbindung                   | A technical term that translates directly into German and remains immediately understandable to users.                                                                                                                        |
| Dag ID        | Dag ID                       | "Dag" intentionally remains untranslated as it is a project-specific term. "ID" is capitalized according to German orthography conventions (Duden).                                                                           |
| Dag Run       | Dag Lauf                     | While "Run" frequently appears in code and logs, translating it improves readability in the UI. The established term "Dag" remains unchanged.                                                                                 |
| Dag Run ID    | Dag Lauf ID                  | Consistent derivation from the translation "Dag Lauf".                                                                                                                                                                        |
| Dag Processor | Dag Prozessor                | Direct translation of the technical concept. Alternatives such as "Verarbeiter" would sound unusual in this context.                                                                                                          |
| Task          | Task                         | The term is well established in the Airflow ecosystem and frequently appears in code and logs. Translating it as "Aufgabe" could make it harder to connect UI terms with code references.                                     |
| Task ID       | Task ID                      | Remains consistent with the decision to keep "Task" untranslated.                                                                                                                                                             |
| Task Instance | Task Instanz                 | "Instanz" is the common German technical term for a concrete execution or occurrence of an object.                                                                                                                            |
| Task Group    | Task Gruppe                  | Partial translation: "Task" remains the technical term while "Group" is translated to improve clarity for users.                                                                                                              |
| XCom          | Task Kommunikation (XCom)    | The concept is described in German to improve clarity for new users. The original term remains in parentheses because it frequently appears in Airflow code and logs.                                                         |

### Admin & Configuration

| English Term            | German Translation                          |
|-------------------------|---------------------------------------------|
| Admin                   | Administrator                               |
| Configuration / Config  | Konfiguration                               |
| Connection Type         | Verbindungstyp                              |
| Variable                | Variable                                    |
| Pool Slots              | Pool Belegung                               |
| Plugin                  | Plug-in                                     |
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
|--------------|--------------------|
| Add          | Hinzufügen         |
| Delete       | Löschen            |
| Edit         | Bearbeiten         |
| Save         | Speichern          |
| Reset        | Zurücksetzen       |
| Cancel       | Abbrechen          |
| Confirm      | Bestätigen         |
| Import       | Importieren        |
| Export       | Exportieren        |
| Search       | Suchen             |
| Filter       | Filter / Filtern   |
| Copy         | Kopieren           |
| Download     | Herunterladen      |
| Close        | Schließen          |
| Expand       | Ausklappen         |
| Collapse     | Einklappen         |

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

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Italian (`it`) translation guidelines

Follow the [global translation rules](../SKILL.md) and the existing strings in
`airflow-core/src/airflow/ui/public/i18n/locales/it/`. Use standard Italian
suitable for a technical interface.

## Terminology

Keep `Airflow`, `Dag`/`Dags`, `Task`/`Tasks`, `Asset`/`Assets`, `Pool`/`Pools`,
`Plugin`/`Plugins`, `Provider`/`Providers`, `Backfill`, `Job`/`Jobs`, `Bundle`,
`Callback`, `XCom`, `Trigger`, `Heartbeat`, `Slot`, `Upstream`, `Downstream`,
`Log`, `JSON`, `ID`, `PID`, `UTC` and `Schema` in English.

Use the wording already in the locale: `Run del Dag`, `Connessione`,
`Variabile`, `Programmazione`, `Proprietario`, `Etichette`, `Pulisci` for
clearing a task instance, and `Cercare` in search placeholders. `Scadenza` is
the word for a deadline. A loop is `Ciclo`; one pass of it is `Iterazione`.

Do not copy an awkward existing string into a new key. `Mostrando`, for
example, is not Italian.

## Plurals

Italian needs `_zero`, `_one`, `_many` and `_other`
(`PLURAL_SUFFIXES["it"]`). A key that omits `_many` falls through to the
English `fallbackLng` rather than to the Italian `_other`.

`_many` resolves only at exact multiples of a million (1.000.000, 2.000.000,
…). 1.500.000 and 1.000.001 resolve to `_other`. `_zero` is the explicit
zero-count message (`Nessun` / `Nessuna` / `Nessuno`).

At those million counts a numeral takes **`di`** before the noun it quantifies
("un milione **di** task"), so a `_many` form that merely repeats `_other`
reads ungrammatically when the string interpolates a count directly in front
of a noun:

```json
"change_one": "{{count, number}} modifica",
"change_many": "{{count, number}} di modifiche",
"change_other": "{{count, number}} modifiche",
"change_zero": "Nessuna modifica"
```

When the count stands in brackets and does not quantify the following noun,
`di` does not apply and `_many` matches `_other`:

```json
"requiredActionCount_many": "Azioni richieste ({{count, number}})"
```

`altri` is still a quantified word, so `teams.more_many` is
`e {{count, number}} di altri`. A bare noun with no numeral in the string is
unaffected:

```json
"task_one": "Task",
"task_many": "Tasks",
"task_other": "Tasks",
"task_zero": "Nessun task"
```

The UI prints the count as digits (`1.000.000`), not as the words "un milione".
Keep `{{count, number}}` anyway: that is the form i18next selects `_many` for.

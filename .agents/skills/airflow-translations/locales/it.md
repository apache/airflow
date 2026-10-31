<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Italian (`it`) translation guidelines

Follow the [global translation rules](../SKILL.md) and the existing strings in
`airflow-core/src/airflow/ui/public/i18n/locales/it/`. The English locale defines
the meaning of each key. Use standard Italian suitable for a technical interface.

## Terminology

Keep `Airflow`, `Dag`/`Dags`, `XCom`/`XComs`, `Provider`, `REST API`, `JSON`,
`ID`, `PID` and `UTC` unchanged. Write `Dag` with this capitalization in prose.

The Italian interface also retains `Task`/`Tasks`, `Asset`/`Assets`,
`Pool`/`Pools`, `Plugin`/`Plugins`, `Backfill`/`Backfills`, `Catchup` and
`Heartbeat`. Keep these terms when translating related keys; do not replace
`Task` with `Compito` or `Asset` with `Risorsa`.

Use these existing translations in the corresponding contexts:

| English | Italian | Existing key |
| --- | --- | --- |
| Latest Task Instance | Ultima Istanza di Task | `components:durationChart.lastTaskInstance_one` |
| Dag Run | Run del Dag | `common:dagRun_one` |
| Operator | Operatore | `common:task.operator` |
| Schedule | Programmazione | `common:dagDetails.schedule` |
| Connection | Connessione | `admin:connections.connection_one` |
| Variable | Variabile | `admin:variables.variable_one` |
| Configuration | Configurazione | `common:admin.Config` |
| Owner | Proprietario | `common:dagDetails.owner` |
| Description | Descrizione | `common:dagDetails.description` |
| Tags | Etichette | `common:dagDetails.tags` |
| Dark Mode | Modalità scura | `common:appearance.darkMode` |
| Light Mode | Modalità chiara | `common:appearance.lightMode` |

Some older strings are inconsistent. Do not generalize an isolated spelling or
an awkward translation into a rule for new strings. For example,
`dashboard:health.scheduler` currently says `Pianificatrice`; ask the Italian translation
owners before standardizing component names or changing existing terminology.

## Grammar and interface text

Use Italian articles and prepositions around technical terms: `del Dag`,
`del Task`, `della connessione` and `dell'operatore`. Match gender and number to
the Italian noun in the sentence, including when a placeholder supplies its value.

The Italian translation owner's feedback in
[the earlier Italian guidelines PR](https://github.com/apache/airflow/pull/62685)
prefers `Ultima esecuzione del Dag` for a new label describing the latest Dag
run, and `ID della connessione` for a connection identifier. Existing short
labels such as `Run del Dag` remain in use; do not copy `Ultimo Run del Dag`
or `ID Connessione` into new prose merely because they occur in older strings.

Use direct action labels, following the existing interface: `Aggiungi`,
`Modifica`, `Elimina`, `Salva` and `Scarica`. Search placeholders currently use
`Cercare`, as in `Cercare Connessioni`.

Use sentence case for new sentences and preserve established labels in their
existing context. Keep accents, including `è`, `ì` and `à`, and use the Italian
apostrophe where needed, as in `L'ID della connessione è obbligatorio`.

## Task and run states

These values come from `common:states`. Keep them consistent across views.

| State key | Italian |
| --- | --- |
| `deferred` | In Attesa |
| `failed` | Fallito |
| `no_status`, `none` | Nessun Stato |
| `planned` | Pianificato |
| `queued` | In Coda |
| `removed` | Rimosso |
| `restarting` | Riavviato |
| `running` | In Esecuzione |
| `scheduled` | Programmato |
| `skipped` | Saltato |
| `success` | Successo |
| `up_for_reschedule` | Da Reschedulare |
| `up_for_retry` | Da Riavviare |
| `upstream_failed` | Upstream Fallito |

## Plurals and interpolation

Airflow configures Italian with `_zero`, `_one`, `_many` and `_other` in
`dev/breeze/src/airflow_breeze/commands/ui_commands.py`. Supply all required
forms for a pluralized key. `_many` is an Italian plural category, not a
replacement for `_other`; use `_zero` for the explicit zero-count message.

For example, `admin.json` contains:

```json
{
  "connection_zero": "Nessuna connessione",
  "connection_one": "Connessione",
  "connection_many": "Connessioni",
  "connection_other": "Connessioni"
}
```

Preserve interpolation names, their case, and formatters such as
`{{count, number}}`. For example, the existing delete messages are
`Elimina 1 connessione` for `admin:connections.delete.deleteConnection_one`
and `Elimina {{count, number}} connessioni` for the `_many` and `_other` forms.
Keep markup, URLs, code identifiers and hotkeys unchanged.

## Before submitting

- Check the English key and nearby Italian strings before translating.
- Reuse the terminology above and preserve interpolation tokens exactly.
- Follow the validation commands in the parent skill.
- Keep terminology corrections separate from unrelated translation updates and
  explain them to the Italian translation owners during review.

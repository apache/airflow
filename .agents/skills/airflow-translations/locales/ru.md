<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Russian (ru)

This document provides locale-specific instructions for translating English
Airflow UI strings into Russian. It inherits all global rules from
the parent [SKILL.md](../SKILL.md).

**Locale code:** `ru`
**Register:** formal `Вы` (capitalised), consistent with the existing
translations in `airflow-core/src/airflow/ui/public/i18n/locales/ru/`.

## Plural Forms

Russian has **four plural forms**. Use the correct form for each suffix:

- `_one`: 1, 21, 31, … (nominative singular)
- `_few`: 2-4, 22-24, … (genitive singular)
- `_many`: 0, 5-20, 25-30, … (genitive plural)
- `_other`: the plural label shown without a number (nominative plural, see
  the note below)

**English source:**

```json
"connection_one": "Connection",
"connection_other": "Connections"
```

**Correct:**

```json
"connection_one": "Соединение",
"connection_few": "Соединения",
"connection_many": "Соединений",
"connection_other": "Соединения"
```

**Incorrect:**

```json
"connection_few": "Соединений",   // genitive plural used for 2-4
"connection_many": "Соединения"   // genitive singular used for 5+
```

Where a numeral governs the noun, `_few` and `_many` take different cases and
must differ:

```json
"connection_few": "Соединения",    // 2-4
"connection_many": "Соединений"    // 5 and up
```

Where the string carries no numeral, one plural form covers every count, and
the suffixes may legitimately be identical:

```json
"firstConfirmMessage_few": "Вы собираетесь удалить следующие соединения:",
"firstConfirmMessage_many": "Вы собираетесь удалить следующие соединения:"
```

So judge a set by whether a number is rendered next to the noun, not by
whether the suffixes differ.

**`_other` is the plural label shown without a number.** The UI renders keys
such as `dagRun_other` or `dag_other` on their own (tab names, titles, table
headers), while i18next never selects `_other` for whole-number counts in
Russian. Use the nominative plural:

```json
"dagRun_other": "Запуски Dag-а",
"requiredAction_other": "Необходимые действия"
```

Many existing entries still repeat the `_many` form (e.g. `dag_other: "Dag-ов"`);
those are worth correcting in a separate change.

## Case Declension

Russian nouns change form by grammatical case. Use the case the context
requires, not the nominative by default:

**Nominative** (subject, standalone labels):

```json
"variable_one": "Переменная"
```

**Accusative** (direct object of a command):

```json
"deleteVariable_one": "Удалить 1 переменную"
```

**Genitive** (after numerals 5+, and for "of"):

```json
"deleteVariable_many": "Удалить {{count, number}} переменных"
```

## Gender Agreement

Adjectives, participles and past-tense verbs must agree with the gender of the
noun they describe:

**Correct:**

```json
"firstConfirmMessage_one": "Вы собираетесь удалить следующее соединение:"
"deleteVariable_one": "Удалить 1 переменную"
```

`соединение` is neuter → `следующее`; `переменная` is feminine → accusative
`переменную`.

**Incorrect:**

```json
"firstConfirmMessage_one": "Вы собираетесь удалить следующий соединение:"
```

## Unchanged Terms

Keep these in English, as the existing locale does:

- `Airflow` — product name
- `Dag` / `Dags` — Airflow concept; never write `DAG`. Decline with a hyphen
  where a case ending is needed: `Запуск Dag-а`
- `XCom` — Airflow cross-communication term
- `Backfill` — kept in English throughout the `ru` locale
- `ID`, `JSON`, `API`, `SDK`, `Unix`, `UTC`, `PID` — technical abbreviations
- Log levels: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`

## Verb Forms and User Address

- Use the **infinitive** for buttons and commands: `"Удалить соединение"`
- Use the **formal, capitalised `Вы`** for questions and confirmations:
  `"Вы уверены, что хотите продолжить?"`
- Never use `ты`.

**Correct:**

```json
"confirm": "Вы уверены, что хотите продолжить?",
"deleteWarning": "Это действие нельзя отменить."
```

**Incorrect:**

```json
"confirm": "Ты уверен, что хочешь продолжить?"
```

## Letter ё

The existing locale writes `ё` where standard orthography calls for it, for
example `ещё`, `всё`, `звёздочка`. Keep it — do not substitute `е`.

Take care with pairs that are different words rather than spelling variants:
`все` ("all", plural) and `всё` ("everything") both occur in the locale and
are each correct in their own context.

## Variable and Placeholder Examples

Preserve all `{{variable}}` placeholders and the `, number` formatter exactly.
Word order may change to read naturally in Russian:

**English source:**

```json
"unpauseOnTrigger": "Unpause {{dagName}} on trigger"
```

**Correct:**

```json
"unpauseOnTrigger": "Снять паузу с {{dagName}} при запуске"
```

**Incorrect:**

```json
"unpauseOnTrigger": "Снять паузу с {{имяDag}} при запуске"  // variable renamed
```

## Terminology Glossary

These translations are established in the existing `ru` locale files and must
be reused:

| English | Russian |
|---|---|
| Task | Задача |
| Task Instance | Экземпляр задачи |
| Task Group | Группа задач |
| Dag Run | Запуск Dag-а |
| Connection | Соединение |
| Variable | Переменная |
| Pool | Пул |
| Asset | Актив |
| Trigger (noun) | Триггер |
| Trigger (verb) | Запустить |
| Scheduler | Планировщик |
| Operator | Оператор |
| Executor | Исполнитель |
| Queue | Очередь |
| Owner | Владелец |
| Duration | Длительность |
| Event | Событие |
| Upstream | Вверх по потоку |
| Downstream | Вниз по потоку |
| Paused | Приостановленные |
| Queued | В очереди |
| Running | Выполняется |
| Success | Успех |
| Failed | Ошибка |
| Skipped | Пропущен |
| Deactivated | Деактивирован |

Notes:

- `Upstream` also appears contextually as `выше по цепочке` (e.g. "Запустить
  Dag выше по цепочке этого актива"). Use `Вверх по потоку` for the standalone
  direction label and `выше по цепочке` inside sentences.
- State names such as `Приостановленные` are used as plural filter labels;
  match the grammatical number of the surrounding UI element.

## Terminology Reference

The established Russian translations are defined in the existing locale files.
Before translating, **read the existing ru JSON files** to learn the
established terminology:

```
airflow-core/src/airflow/ui/public/i18n/locales/ru/
```

Use the translations found in these files as the authoritative glossary. When
translating a term, check how it has been translated elsewhere in the locale
to maintain consistency. If a term has not been translated yet, refer to the
English source in `en/` and apply the rules in this document.

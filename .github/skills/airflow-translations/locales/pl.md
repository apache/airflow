<!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# Polish (pl)

This document provides locale-specific instructions for translating English
Airflow UI strings into Polish. It inherits all global rules from
the parent [SKILL.md](../SKILL.md).

## Plural Forms

Polish has **four plural forms** based on the count. Use the correct form for each suffix:

- `_one`: 1 item (nominative singular)
- `_few`: 2-4 items, 22-24, 32-34, etc. (nominative plural)
- `_many`: 5+ items, 11-21, 25-31, etc. (genitive plural)
- `_other`: fractions and general plural

**English source:**

```json
"connection_one": "Connection",
"connection_few": "Connections",
"connection_many": "Connections",
"connection_other": "Connections"
```

**Correct:**

```json
"connection_one": "Połączenie",
"connection_few": "Połączenia",
"connection_many": "Połączeń",
"connection_other": "Połączenia"
```

**Incorrect:**

```json
"connection_few": "Połączeń",  // wrong case for 2-4
"connection_many": "Połączenia" // wrong case for 5+
```

## Case Declension

Polish nouns change form by grammatical case. Use the appropriate case for context:

**Nominative** (subject):

```json
"title": "Wszystkie połączenia"  // All connections
```

**Genitive** (possession, "of"):

```json
"title": "Lista połączeń"  // List of connections
```

**Accusative** (direct object):

```json
"button": "Dodaj połączenie"  // Add connection
```

## Gender Agreement

Match noun gender (masculine/feminine/neuter) with verbs and adjectives:

**Correct:**

```json
"message": "Połączenie zostało usunięte"  // neuter noun + neuter verb
```

**Incorrect:**

```json
"message": "Połączenie został usunięty"  // neuter noun + masculine verb
```

## Unchanged Terms

Keep these in English:

- `XCom` — Airflow cross-communication term
- `ID` — Technical identifier (always uppercase)
- `Dag` / `Dags` → use `Dag` / `Dagi` (Polish plural adaptation)
- Log levels: `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`


## Verb Forms and User Address

- Use **infinitive** for commands: `"Dodaj połączenie"` (Add connection)
- Use **indicative** for status: `"Połączenie zostało usunięte"` (Connection was deleted)
- Use **informal "ty"** (lowercase) for questions: `"Czy chcesz kontynuować?"` (Do you want to continue?)

**Correct:**

```json
"confirm": "Czy na pewno chcesz kontynuować?"
```

**Incorrect:**

```json
"confirm": "Czy na pewno Pan chce kontynuować?"  // overly formal
```

## Diacritics

Polish uses diacritics that must be preserved:

| Letter | Correct | Incorrect |
|--------|---------|-----------|
| ą | będą | bedą |
| ć | połączenie | polaczenie |
| ę | usunięte | usuniete |
| ł | został | zostal |
| ń | koń | kon |
| ó | główna | glowna |
| ś | więcej | wiecej |
| ź | źródło | zrodlo |
| ż | żaden | zaden |

## Variable and Placeholder Examples

Preserve all `{{variable}}` placeholders. Adjust word order for natural Polish:

**English source:**

```json
"title": "Delete {{count}} connections"
```

**Correct:**

```json
"deleteConnection_one": "Usuń 1 połączenie",
"deleteConnection_few": "Usuń {{count}} połączenia",
"deleteConnection_many": "Usuń {{count}} połączeń"
```

**Incorrect:**

```json
"title": "Usuń {{liczba}} połączeń"  // variable name translated
```

## Terminology Reference

The established Polish translations are defined in the existing locale files.
Before translating, **read the existing pl JSON files** to learn the
established terminology:

```
airflow-core/src/airflow/ui/public/i18n/locales/pl/
```

Use the translations found in these files as the authoritative glossary. When
translating a term, check how it has been translated elsewhere in the locale
to maintain consistency. If a term has not been translated yet, refer to the
English source in `en/` and apply the rules in this document.

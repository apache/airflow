<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# 5. DB-API and dialect differences are normalised behind the hook

Date: 2026-07-20

## Status

Accepted

## Context

PEP-249 standardises very little of what actually differs between databases.
Paramstyle varies (`%s`, `?`, `:name`); `cursor.description` may be `None` or
absent entirely; `rowcount` may be `-1`, `None`, or a real count; autocommit may
be a connection attribute, a method, or unsupported; row values may arrive as
tuples, named tuples, dicts, or driver-specific objects; identifier quoting and
reserved-word sets are per-database. The existing area ADR
`src/airflow/providers/common/sql/doc/adr/0002-return-common-data-structure-from-dbapihook-derived-hooks.md`
records why a common return shape was chosen despite that; this decision is about
*where* the remaining differences are allowed to live.

The whole point of `common.sql` is that `SQLExecuteQueryOperator`, the SQL check
operators, `GenericTransfer`, `SQLSensor`, the `@task.sql` decorator, and the ~29
downstream providers can be written once against one behaviour. If a driver quirk
leaks upward, it does not leak into one place — it leaks into every caller, and
each of them grows its own conditional. Those conditionals then diverge, because
they are maintained by different people on different release cadences.

There is a second, sharper reason. The same database is reachable through several
connection types: natively (`MsSqlHook`, `PostgresHook`), through generic JDBC, or
through generic ODBC. Behaviour implemented inside a native hook is simply
unavailable to the JDBC and ODBC paths — which is exactly the gap the existing
area ADR
`src/airflow/providers/common/sql/doc/adr/0003-introduce-notion-of-dialects-in-dbapihook.md`
was written to close. Dialects exist so that per-database behaviour is keyed on
the *database*, not on the connection type that happens to reach it.

The hook already normalises a substantial list: `placeholder` (validated against
the known paramstyles, falling back with a warning when a connection supplies an
unknown one), `set_autocommit` / `get_autocommit` with a manual-commit fallback
when the driver does not support autocommit, `insert_statement_format` /
`replace_statement_format` / `escape_word_format` / `escape_column_names`,
`reserved_words` sourced per dialect, `descriptions` and `last_description`
captured off the cursor, `get_row_count` standardising PEP-249's `-1` / `None` to
`None`-or-non-negative, and `_make_common_data_structure` as the per-hook hook for
driver row types.

SQLAlchemy is used where it helps, but it is deliberately never load-bearing:
`dialect_name` degrades through `make_url` → the `sqlalchemy_scheme` extra → the
`dialect` extra → `"default"`, and `get_reserved_words` suppresses `ImportError`,
`ModuleNotFoundError`, and `NoSuchModuleError`. A missing dialect package raises
`AirflowOptionalProviderFeatureException` with an install hint rather than a bare
import error.

## Decision

Database, driver, and connection-type differences are normalised inside this
package — in `DbApiHook` or in a `Dialect` — and never pushed onto callers.

- **Per-database behaviour goes in a `Dialect`**, not in a native hook, unless the
  database is genuinely reachable through exactly one connection type. That is
  what makes the behaviour available over JDBC and ODBC as well as natively.
- **Per-driver behaviour goes in the hook** — paramstyle, autocommit, cursor
  description and row-count handling, identifier escaping, and row-type
  conversion via `_make_common_data_structure`.
- **Callers stay dialect-agnostic.** Operators, sensors, transfers, decorators,
  and downstream provider code must not branch on database or driver name to work
  around a quirk. If a caller needs to know, that is the signal the hook or the
  dialect is missing an entry point.
- **Prefer a native dialect implementation over a SQLAlchemy round-trip** where
  the dialect can answer directly. SQLAlchemy stays a convenience and a fallback,
  never a hard requirement of a code path.
- **Degrade, do not fail, when a dialect or optional dependency is absent** —
  fall back to the generic `Dialect`, or raise
  `AirflowOptionalProviderFeatureException` with the package to install.
- **Configuration of a quirk is read by name from the connection extras**
  (`placeholder`, `sqlalchemy_scheme`, `dialect`, `escape_column_names`, …), never
  by spreading `Connection.extra` into a driver call — see the security section of
  `providers/AGENTS.md`.

## Consequences

- One behaviour is implemented once and works across native, JDBC, and ODBC
  connections to the same database, instead of three near-copies that drift.
- Downstream providers get new normalisation for free on the next `common.sql`
  release, which is also why the interface-stability rules in ADR 1 bind so
  tightly: normalisation is only useful if the surface delivering it is stable.
- This package carries per-database knowledge it cannot fully test — dialect
  changes need the dialect's own tests plus coverage on at least one real backend,
  and a reviewer without that database cannot confirm correctness from the diff.
- Adding a dialect is a compatibility commitment from the release it ships in.

A change **violates** this decision when it:

- branches on database or driver name inside an operator, sensor, transfer,
  decorator, or downstream provider to work around a quirk that belongs in the
  hook or a dialect;
- implements per-database behaviour in a native hook when a `Dialect` would make
  it available to the JDBC and ODBC connection types as well;
- makes `sqlalchemy` (or any optional dialect package) a hard requirement of a
  code path, or removes one of the fallbacks in `dialect_name` /
  `get_reserved_words`;
- hard-codes a paramstyle, quoting rule, or statement format at a call site
  instead of going through `placeholder`, `escape_word_format`, or the statement
  format properties;
- returns raw driver row objects, a driver-specific `rowcount`, or a raw
  `cursor.description` to callers instead of the normalised forms;
- constructs a new engine or connection per call where the hook already caches
  one.

## Evidence

- #45640 — "Fix escaping of special characters or reserved words as column names
  in dialects of common sql provider": identifier quoting fixed in the dialect
  layer, so every connection type to that database benefits.
- #54437 — "Implemented native `get_column_names` in `PostgresDialect` to become
  SQLAlchemy independent" and #54832 — "`PostgresDialect` should use index instead
  of name in `get_column_names` and `get_primary_keys`": the native-over-SQLAlchemy
  preference applied in practice.
- #54446 — "Fixed resolving of dialect name when host of `JdbcHook` is an JDBC
  URL": dialect resolution made to work for the generic connection type, which is
  the entire reason dialects exist.
- #52976 — "Add rudimentary support for psycopg3": a driver difference absorbed
  inside the hook rather than surfaced to callers.
- #48938 — "Fix `SADeprecationWarning` when using inspector with SQLAlchemy in
  `DbApiHook`" and #62594 — "Cache `DbApiHook.inspector` to avoid creating N
  engines": SQLAlchemy kept as a contained implementation detail, including the
  per-call engine construction that had to be fixed.
- #52224 — "fix(postgres/hooks): ensure `get_df` uses SQLAlchemy engine to avoid
  pandas warning": a driver/library interaction resolved behind the hook's
  dataframe entry point.

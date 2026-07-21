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

PEP-249 standardises very little of what actually differs between databases:
paramstyle (`%s`, `?`, `:name`), `cursor.description` (may be `None`), `rowcount`
(`-1` / `None` / real), autocommit support, row types, identifier quoting, and
reserved words all vary. Area ADR 0002 records why a common return shape was
chosen; this decision is about *where* the remaining differences may live.

The point of `common.sql` is that its operators, sensors, transfers, `@task.sql`,
and the ~29 downstream providers are written once against one behaviour. A driver
quirk that leaks upward leaks into every caller, and those per-caller conditionals
then diverge across cadences. Sharper still: the same database is reachable
natively (`PostgresHook`), through generic JDBC, or through generic ODBC — so
behaviour inside a native hook is unavailable to JDBC/ODBC. Dialects (area ADR
0003) exist so per-database behaviour is keyed on the *database*, not the
connection type reaching it.

The hook already normalises paramstyle (`placeholder`), autocommit with a
manual-commit fallback, statement/escape formats, per-dialect `reserved_words`,
cursor `descriptions`, `get_row_count`, and `_make_common_data_structure` for
driver row types. SQLAlchemy is used but never load-bearing: `dialect_name`
degrades `make_url` → `sqlalchemy_scheme` → `dialect` → `"default"`,
`get_reserved_words` suppresses import errors, and a missing dialect package
raises `AirflowOptionalProviderFeatureException` with an install hint.

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

- One behaviour works across native, JDBC, and ODBC connections to the same
  database, instead of three near-copies that drift.
- Downstream providers get new normalisation for free on the next release — which
  is why the interface-stability rules in ADR 1 bind so tightly.
- This package carries per-database knowledge it cannot fully test: dialect changes
  need the dialect's own tests plus one real backend, and a reviewer without that
  database cannot confirm correctness from the diff.
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

- #45640 — reserved-word column escaping fixed in the dialect layer, so every
  connection type to that database benefits.
- #54437, #54832 — native `get_column_names` / `get_primary_keys` in
  `PostgresDialect`: the native-over-SQLAlchemy preference in practice.
- #54446 — dialect resolution made to work when a `JdbcHook` host is a JDBC URL;
  the generic connection type is the whole reason dialects exist.
- #52976 — psycopg3 driver difference absorbed inside the hook, not surfaced.
- #48938, #62594 — SQLAlchemy kept a contained detail, including the per-call
  engine construction that had to be fixed by caching the inspector.
- #52224 — driver/library interaction resolved behind the hook's dataframe path.

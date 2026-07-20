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

# 4. DbApiHook is a stable provider-facing interface

Date: 2026-07-20

## Status

Accepted

## Context

`DbApiHook` (`hooks/sql.py`) is not an internal base class. It is the contract
that every SQL-speaking provider in the repo inherits from: amazon, google,
snowflake, databricks, postgres, mysql, oracle, teradata, trino, presto, hive,
druid, drill, impala, pinot, exasol, vertica, ydb, clickhousedb, jdbc, odbc,
mssql, sqlite, elasticsearch, pgvector, slack, informatica, openlineage and
common.ai all import `airflow.providers.common.sql` from their sources — 29
distributions at the time of writing. 26 declare it as a runtime dependency,
three more behind an optional extra, and 104 provider `pyproject.toml` files
reference it in some form.

Those packages are released **independently of this one**, on a wave cadence,
against a range of versions of each other (the parent decision in
`providers/adr/` covers that release model). A deployment routinely runs a newer
`apache-airflow-providers-common-sql` under an older
`apache-airflow-providers-snowflake`, or the reverse. So the public shape of
`DbApiHook` — method names, signatures, return shapes, the properties a subclass
may read, and the override points a subclass is expected to implement — is a
cross-version compatibility boundary, not something this package can reshape when
convenient.

The project already learned this the hard way. `hooks/sql.py` still carries a
`connection` setter whose sole purpose is a backwards-compatibility warning, and
whose docstring names the exact released provider versions
(`apache-airflow-providers-mysql<5.7.1`,
`apache-airflow-providers-elasticsearch<5.5.1`, …) that broke when the property
was introduced. The `_make_common_data_structure` override point has its own prek
hook — `scripts/ci/prek/check_common_sql_dependency.py`, registered in
`providers/.pre-commit-config.yaml` — that fails the build when a provider
overrides it without declaring `common-sql>=1.9.1`, because relying on an
override point that did not exist in an older release is exactly the silent break
this boundary is meant to prevent.

The surface is also *recorded* rather than merely implied. Committed `.pyi` stubs
sit next to each public module (`hooks/sql.pyi`, `hooks/handlers.pyi`,
`operators/generic_transfer.pyi`, `sensors/sql.pyi`, `triggers/sql.pyi`,
`dialects/dialect.pyi`), and `README_API.md` documents the Android-style API-diff
workflow that maintains them: additive changes are auto-applied by the stub hook,
while removals and signature changes fail and require an explicit
`UPDATE_COMMON_SQL_API=1` regeneration. MyPy then checks downstream providers
against those stubs, so a provider using something outside the recorded surface is
flagged.

The `get_pandas_df` → `get_df` migration is the worked example of doing this
correctly. `get_df` and `get_df_by_chunks` were added alongside polars support;
`get_pandas_df` and `get_pandas_df_by_chunks` stayed as thin deprecated
delegators raising `AirflowProviderDeprecationWarning`; and downstream providers
were then migrated one package at a time, each in its own PR, over several
release waves. At no point was a released provider broken.

## Decision

Treat the public surface of `DbApiHook` — and of the handler functions and
operator base classes that travel with it — as a stable, provider-facing API.

- **Never rename, remove, or narrow** a public method, property, or class
  attribute of `DbApiHook`, `BaseSQLOperator`, `SQLExecuteQueryOperator`, or the
  handler functions in `hooks/handlers.py`. That includes tightening a parameter
  type, dropping a keyword name a subclass may pass through, and reordering
  positional parameters.
- **Make changes additive.** New behaviour is a new method, or a new keyword
  argument whose default reproduces the previous call semantics exactly.
- **Do not change the `run` return shape.** The list-vs-bare-result rules encoded
  in `return_single_query_results` are a deliberate backwards-compatibility
  compromise; changing when results are wrapped changes what every downstream
  operator writes to XCom.
- **A name that genuinely must move keeps a deprecated shim** that forwards to the
  replacement under `AirflowProviderDeprecationWarning`, as `get_pandas_df` does.
  Removal happens only on a documented major boundary, after the minimum supported
  dependent version no longer references it — never in the change that introduces
  the replacement.
- **The `.pyi` stubs are the gate, not paperwork.** Regenerate them and read the
  diff. If the diff is additive, the change is safe by construction. If it shows a
  removal or a signature change, the PR must say why the break is acceptable and
  which released providers it affects.
- **A new override point comes with a version floor.** When downstream hooks are
  expected to implement something new, the consuming provider's dependency floor
  is bumped with a `# use next version` marker — the mechanism
  `check_common_sql_dependency.py` already enforces for
  `_make_common_data_structure`.

## Consequences

- Downstream providers can be upgraded ahead of, or behind, `common.sql` without
  `AttributeError` / `TypeError` at the hook boundary.
- This package accumulates deprecated shims for a release or two. That cost is
  accepted in exchange for the version-mix guarantee.
- Interface cleanups are slow: the additive change ships first, downstream
  migration happens per-provider over several waves, and removal waits for a major
  boundary. This is the intended pace, not friction to route around.
- Contributors pay an extra step — regenerating stubs and justifying any
  non-additive diff — on every change that touches the public surface.

A change **violates** this decision when it:

- renames, deletes, or changes the signature of a public `DbApiHook`,
  `BaseSQLOperator`, or handler member without leaving a working, deprecated
  forwarding shim;
- changes what `run` returns for a given combination of `sql`, `return_last`, and
  `split_statements`, or alters `descriptions` / `last_description` population;
- regenerates the `.pyi` stubs with `UPDATE_COMMON_SQL_API=1` to accept a removal
  or signature change, without saying in the PR why the break is acceptable and
  which providers it affects;
- deletes a deprecated shim in the same change that introduces its replacement,
  or outside a documented major boundary;
- adds an override point downstream hooks must implement without bumping the
  consuming providers' `common-sql` dependency floor;
- makes a previously optional argument required, or a previously accepted input
  type unaccepted, on any public entry point.

## Evidence

- #48875 — "feat: integrate `polars` in `get_df`, `get_df_by_chunks`": the
  additive half of the pattern — a new capability added as new methods, leaving
  `get_pandas_df` intact as a deprecated delegator.
- #50017 — "Update Exasol provider dependencies and deprecate `get_pandas_df`
  method", #50123 — "Migrate `PrestoHook` and `TrinoHook` to use `get_df`", #50341
  — "Migrate `BigQueryHook` to use `get_df`": the migration half — downstream
  providers moved one package at a time, after the additive change shipped.
- #61144 — "Implement specialized `get_first` and `get_records` method in
  `OracleHook` to avoid serialization issues with XCom's": a return-shape problem
  solved by overriding in the downstream hook rather than by reshaping the shared
  contract.
- #69230 — "Align hook `run()` annotations with None-able handler results": a
  correction to the recorded `run` surface, made as an annotation change rather
  than a behaviour change.
- #45789 — fixing `DbApiHook.insert_rows` logging the wrong number of inserted
  rows: the kind of small correction that is safe precisely because it does
  not move the interface.

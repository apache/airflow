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

`DbApiHook` (`hooks/sql.py`) is not an internal base class — it is the contract
~29 SQL-speaking providers (amazon, google, snowflake, postgres, mysql, oracle,
jdbc, odbc, …) inherit from. Those packages release **independently of this one**
on a wave cadence, and a deployment routinely runs a newer `common-sql` under an
older `snowflake`, or the reverse (the release model is covered by the parent
decision in `providers/adr/`). So the public shape of `DbApiHook` — names,
signatures, return shapes, readable properties, and override points — is a
cross-version compatibility boundary, not something to reshape when convenient.

The project learned this the hard way: `hooks/sql.py` still carries a `connection`
setter that exists only to warn, naming the released versions
(`mysql<5.7.1`, `elasticsearch<5.5.1`, …) it broke. The surface is also
*recorded*: committed `.pyi` stubs sit beside each public module and
`README_API.md` documents the API-diff workflow — additive changes auto-apply,
removals and signature changes fail and require an explicit
`UPDATE_COMMON_SQL_API=1` regeneration; MyPy then checks downstream providers
against the stubs. New override points carry a version floor enforced by
`scripts/ci/prek/check_common_sql_dependency.py`. The `get_pandas_df` → `get_df`
migration is the worked example done right: new methods added, the old ones kept
as deprecated delegators, downstream migrated one package at a time.

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
- This package accumulates deprecated shims for a release or two, and interface
  cleanups are slow (additive change first, per-provider migration, removal only
  at a major boundary). That is the intended pace, not friction to route around.
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

- #48875 — polars in `get_df` / `get_df_by_chunks`: additive half, `get_pandas_df`
  kept as deprecated delegator.
- #50017, #50123, #50341 — migration half; downstream providers moved to `get_df`
  one package at a time, after the additive change shipped.
- #61144 — Oracle return-shape problem solved by overriding downstream, not
  reshaping the shared contract.
- #69230 — `run()` annotations aligned with None-able handler results; annotation
  change, not behaviour change.
- #45789 — `insert_rows` row-count log fix; safe because it does not move the
  interface.

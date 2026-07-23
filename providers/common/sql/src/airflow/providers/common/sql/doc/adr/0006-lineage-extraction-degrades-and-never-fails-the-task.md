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

# 6. Lineage extraction degrades and never fails the task

Date: 2026-07-20

## Status

Accepted

## Context

Lineage is *observability*, not the work the user asked for. The contract of
`SQLExecuteQueryOperator` is that the SQL executes and returns its result; the
OpenLineage event is a side benefit. An operator that succeeds at the SQL and then
fails the task while describing what it did has broken the contract for a feature
the user may not even have enabled.

Extraction sits in a fragile spot, spanning four independently released parts —
this provider, `openlineage`, `common-compat`, and the `openlineage-python`
client — plus the downstream hook supplying the database-specific pieces. Any of
them may be old or absent in a real deployment. The concrete failure modes:
the OpenLineage provider not installed (`ImportError`); installed but too old for
a helper; the downstream hook not implementing the optional `get_openlineage_*`
methods (`AttributeError`); the client too old for a facet (hence
`_attach_check_facets` is `@require_openlineage_version(client_min_version="1.47.0")`
with its `AirflowOptionalProviderFeatureException` caught); or SQL that is absent
or unparsable. The code reflects this: the operator-side
`get_openlineage_facets_on_*` return `None` or empty `OperatorLineage()` and log
at `debug`; hook-side `send_sql_hook_lineage` wraps its body in a `try`, logs a
`warning` on any exception, and returns; and the base hook's `get_openlineage_*`
methods have permissive defaults so a hook implementing none still yields a
well-formed empty result.

## Decision

Lineage extraction in this package is best-effort. It degrades to less lineage —
never to a failed or slower task.

- **No lineage code path may raise into `execute()`.** Operator-side extraction
  returns `None` or an empty `OperatorLineage()`; hook-side reporting swallows and
  logs. A partial result is always preferable to an exception.
- **Every cross-provider import is guarded.** `openlineage` imports sit inside
  `try` / `except ImportError` at the point of use, not at module top level. This
  package must import and run cleanly with no OpenLineage provider installed.
- **Every optional hook-side method is treated as optional.** Calls to
  `get_openlineage_database_info`, `get_openlineage_database_dialect`,
  `get_openlineage_default_schema`, and
  `get_openlineage_database_specific_lineage` handle `AttributeError` and a `None`
  return, because the implementing hook lives in a separately released package.
- **Version-skew is expressed in code, not assumed away.** A helper or facet that
  needs a minimum version is gated (`require_openlineage_version`, a caught
  `ImportError` with a documented fallback), and the fallback path is the safe
  behaviour.
- **Log at the right level.** An absent integration or an unsupported shape is
  `debug` — it is normal. An unexpected exception during reporting is `warning`
  with the exception class and message, and the traceback at `debug`. Neither is
  an error the user must act on.
- **Extraction must not change execution.** It must not re-run the user's SQL,
  materialise a result set the operator streamed, hold a connection open past the
  operation, or add a per-row cost. `on_complete` extraction runs after the task's
  work is done and must stay cheap.

## Consequences

- Users can install, upgrade, or remove the OpenLineage provider independently
  without breaking their SQL tasks.
- Lineage can be silently incomplete — the accepted trade: a missing dataset is
  recoverable, a failed production task is not. The `warning` in
  `send_sql_hook_lineage` is the diagnostic trail.
- Broad `except Exception` in these paths is deliberate, scoped to lineage
  reporting only, and an exception to the narrow-exception rule — do not copy it
  into execution paths.
- Tests must cover the degraded paths explicitly; the happy path alone will not
  catch a regression that turns a skip into a raise.

A change **violates** this decision when it:

- adds a lineage code path that can propagate an exception into `execute()` or
  into `DbApiHook.run`;
- imports `airflow.providers.openlineage` (or `openlineage.client`) at module top
  level, or anywhere outside a guarded block, in a path that runs without the
  OpenLineage provider installed;
- calls an optional `get_openlineage_*` method on a downstream hook without
  handling `AttributeError` or a `None` return;
- uses a new OpenLineage provider or client feature without a version gate and a
  safe fallback;
- narrows the `except Exception` in `send_sql_hook_lineage`, or turns one of the
  `debug` skips in `get_openlineage_facets_on_start` / `on_complete` into a raise;
- makes extraction re-execute the user's SQL, materialise a streamed result, or
  otherwise add cost to the task's critical path.

## Evidence

- #61535 — introduced `send_sql_hook_lineage`, body wrapped in a `try` that logs a
  warning and returns rather than propagating.
- #58897 — moved OpenLineage methods to `BaseSQLOperator`, defining degradation
  once for every SQL operator instead of per subclass.
- #66849 — standardized SQL check facets, version-gated with the optional-feature
  exception caught.
- #63346 — removed row-length logging in `SQLInsertRowsOperator`; observability
  code that materialised a lazy result crashed the operator (the cost rule).
- #57135, #57075 — routed cross-provider lineage imports through `common.compat`,
  which makes the guarded fallbacks version-tolerant.

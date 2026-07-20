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

Lineage is *observability*, not the work the user asked for. When a user runs
`SQLExecuteQueryOperator`, the contract is that the SQL executes and its result is
returned. Producing an OpenLineage event about that execution is a side benefit.
An operator that succeeds at the SQL and then fails the task while describing what
it did has broken the contract for a feature the user may not even have enabled.

This package is where SQL lineage is produced, and it sits in an unusually fragile
spot. Extraction spans four independently released moving parts: this provider,
`apache-airflow-providers-openlineage`, `apache-airflow-providers-common-compat`,
and the `openlineage-python` client library — plus the downstream provider hook
that supplies the database-specific pieces. Every one of those is version-skewed
against the others in real deployments, and the extraction code has to run
correctly when any of them is old, or simply absent.

The failure modes are concrete. The OpenLineage provider may not be installed at
all (`ImportError` on `airflow.providers.openlineage.extractors`). It may be
installed but older than a helper this code wants
(`should_use_external_connection` did not exist before OpenLineage provider
1.8.0). The downstream hook may not implement the optional hook-side methods
(`AttributeError` on `get_openlineage_database_info`,
`get_openlineage_database_dialect`, `get_openlineage_database_specific_lineage`).
The `openlineage-python` client may be too old for a facet being emitted — which
is why `_attach_check_facets` is wrapped in
`@require_openlineage_version(client_min_version="1.47.0")` and its
`AirflowOptionalProviderFeatureException` is caught. The SQL itself may not be a
parseable string, or may be absent entirely, because a subclass supplies it some
other way. And the SQL parser may simply not understand a particular statement.

The code already reflects this. `get_openlineage_facets_on_start` and
`get_openlineage_facets_on_complete` in `operators/sql.py` return `None` or an
empty `OperatorLineage()` at every one of those junctures, logging at `debug`
rather than raising. On the hook side, `send_sql_hook_lineage`
(`hooks/lineage.py`) wraps its entire body in a `try` and, on any exception, logs
a `warning` with the exception class and message plus a `debug` traceback — and
returns normally. The base hook's `get_openlineage_*` methods are defined with
permissive defaults (`get_openlineage_database_info` returns `None`,
`get_openlineage_database_dialect` returns `"generic"`) so that a hook that
implements none of them still produces a well-formed, empty result.

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

- Users can install, upgrade, or remove the OpenLineage provider independently of
  this one without breaking their SQL tasks.
- Lineage can be silently incomplete. That is the accepted trade: a missing
  dataset in a lineage graph is recoverable, a failed production task is not. The
  `warning` in `send_sql_hook_lineage` is the diagnostic trail for when it
  happens.
- Broad `except Exception` appears in these paths deliberately. It is scoped to
  lineage reporting only and is an exception to the project's usual
  narrow-exception rule — it must not be copied into execution paths.
- Tests must cover the degraded paths explicitly, since the happy path alone will
  not catch a regression that turns a skip into a raise.

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

- #61535 — "feat: Add Hook Level Lineage to SQL hooks": introduced
  `send_sql_hook_lineage`, whose entire body is wrapped in a `try` that logs a
  warning and returns rather than propagating.
- #58897 — "chore: Move OpenLineage methods to `BaseSQLOperator`": consolidated
  the guarded extraction paths onto the shared operator base, so the degradation
  behaviour is defined once for every SQL operator instead of per subclass.
- #66849 — "feat: Add standardized SQL check representation for listeners":
  extended the check-operator observability surface, with the facet attachment
  version-gated and its optional-feature exception caught.
- #63346 — "Removed logging of rows length in `SQLInsertRowsOperator` to avoid
  crash on non materialized rows": the concrete form of the cost rule —
  observability code that materialised a lazy result crashed the operator.
- #57135 and #57075 — "Migrate `common.sql` provider to `common.compat`": routed
  the cross-provider lineage and compatibility imports through the compat layer,
  which is what makes the guarded fallbacks version-tolerant.

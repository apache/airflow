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

# 1. Authorization decisions live in the auth manager, not in route bodies

Date: 2026-07-19

## Status

Accepted

## Context

Every authorization question in Airflow — *may this user read this Dag, edit this
connection, see this pool, act on this team's resources* — must be answered in one
place, by one component, so it can be audited, overridden by a pluggable auth
manager, and kept consistent across the whole API surface.

The auth manager is that component. `BaseAuthManager` exposes the decision methods
(`is_authorized_dag`, `is_authorized_connection`, `is_authorized_pool`,
`is_authorized_variable`, `is_authorized_view`, …) plus the collection-scoped
helpers built on them (`batch_is_authorized_*`, `filter_authorized_*`,
`get_authorized_*`). The API server does **not** re-implement these checks in route
bodies: it *enforces* the manager's decision by wiring the `requires_access_*`
dependencies and permitted-filter factories from `core_api/security.py` onto
routes. The route declares the access it needs; the dependency calls the manager;
the manager decides.

This split has a failure mode that recurs in review. When authorization logic leaks
into a handler body, a UI check, or a hand-rolled query filter, it drifts from the
manager's decision, is invisible to a pluggable manager that wants to override it,
and is easy to get subtly wrong — authorizing the wrong resource, forgetting the
team scope, or authorizing a bulk operation as a whole so one permitted item drags
an unpermitted one along. The list/filter case is the same decision seen from the
other side: an endpoint that returns *all* rows and trims them itself, instead of
asking the manager which rows the user may see, is an authorization hole dressed as
a query.

The manager is also where multi-team scoping lives. `get_authorized_dag_ids` and
its siblings group resources by `team_name` and defer to the manager's per-team
decision; a caller that bypasses these helpers loses the team boundary entirely.

## Decision

Authorization is decided by the auth manager and enforced by the API server's
dependencies — never re-derived in a route body:

- **All authorization logic lives in `is_authorized_*`** (and the
  `batch_/filter_/get_authorized_*` helpers built on them). A route obtains a
  decision by depending on the matching `requires_access_*` / permitted-filter
  dependency, not by writing its own check.
- **Authorize the actual resource, scoped to the right team.** The decision is made
  about the resource the operation acts on and its `team_name` in multi-team mode —
  not a client-controllable value, and not the batch as an undifferentiated whole.
  Bulk / multi-resource endpoints authorize every item.
- **List and filter endpoints return only authorized rows** by going through
  `get_authorized_*` / `filter_authorized_*`, keeping the per-team batching so the
  filter does not degrade to a per-row `is_authorized_*` scan on large tables.
- **A pluggable manager may override the decision** — including a more efficient
  batched `filter_authorized_*` — precisely because the decision is centralized;
  callers must route through the manager so that override takes effect.

## Consequences

- Authorization is auditable in one component and one set of methods, rather than
  scattered across handlers and UI code.
- A pluggable auth manager (FAB, Keycloak, a custom one) can change *how* access is
  decided without every call site changing — the enforcement wiring stays put.
- Multi-team scoping is applied uniformly because it lives in the shared helpers.
- Callers pay the cost of wiring the correct dependency and going through the
  `*_authorized_*` helpers — intentional, because that path *is* the boundary.

A change **violates** this decision when it:

- re-implements an authorization check in a route body, a service function, or the
  UI instead of calling `is_authorized_*` via the `requires_access_*` dependency;
- authorizes a client-controllable value, or the wrong resource, instead of the one
  the operation acts on, or drops the `team_name` scope in multi-team mode;
- authorizes a bulk / multi-resource operation as a whole, letting an authorized
  item carry an unauthorized one;
- returns unfiltered rows from a list endpoint and trims them itself instead of
  going through `get_authorized_*` / `filter_authorized_*`.

A reviewer should reject any endpoint whose authorization is decided anywhere other
than the auth manager, or that is scoped to the wrong resource or team.

## Evidence

- #55298 — "List only connections, pools and variables the user has access to":
  moved list filtering onto the manager's `get_authorized_*` helpers instead of
  returning everything and trimming in the route.
- #55278 — "Fix bulk operation permissions for connection, pool and variable":
  closed a bulk-endpoint hole where the operation was not authorized per item.
- #55682 — "Override `get_authorized_connections`, `get_authorized_pools` and
  `get_authorized_variables` in FAB auth manager": a provider manager overriding the
  centralized decision with a more efficient implementation — only possible because
  the decision is centralized.
- #61861 — "Add support for multi-team in Simple auth manager": threaded the
  `team_name` scope through the manager's authorization decisions.
- #65685 — "Honor AUTH_ROLE_PUBLIC in FastAPI API server": kept the
  public/anonymous access decision in the auth-manager-backed enforcement path
  rather than a bespoke route check.

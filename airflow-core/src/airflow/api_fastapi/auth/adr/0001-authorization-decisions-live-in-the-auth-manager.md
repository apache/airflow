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
connection, act on this team's resources* — is answered in one component so it can
be audited, overridden by a pluggable manager, and kept consistent. `BaseAuthManager`
exposes the decision methods (`is_authorized_dag`, `is_authorized_connection`, …)
plus the collection helpers built on them (`batch_is_authorized_*`,
`filter_authorized_*`, `get_authorized_*`). The API server does not re-implement
these checks in route bodies: it *enforces* the manager's decision by wiring the
`requires_access_*` dependencies and permitted-filter factories from
`core_api/security.py` onto routes. The route declares the access it needs; the
dependency calls the manager; the manager decides.

The recurring failure mode is authorization logic leaking into a handler body, a UI
check, or a hand-rolled query filter — where it drifts from the manager, is invisible
to a pluggable override, and gets the resource, the `team_name` scope, or a bulk
operation subtly wrong. A list endpoint that returns *all* rows and trims them itself
is that same hole dressed as a query. Multi-team scoping lives in the manager too:
`get_authorized_dag_ids` and its siblings group by `team_name`; a caller that
bypasses them loses the team boundary entirely.

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

- Authorization is auditable in one component, not scattered across handlers and UI.
- A pluggable manager (FAB, Keycloak, custom) changes *how* access is decided without
  every call site changing, and multi-team scoping applies uniformly.
- Callers pay the cost of wiring the correct dependency — intentional, because that
  path *is* the boundary.

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

- #55298 — moved list filtering onto `get_authorized_*` instead of trim-in-route.
- #55278 — closed a bulk-endpoint hole where the operation was not authorized per item.
- #55682 — FAB manager overrode the centralized decision, more efficiently; only
  possible because it is centralized.
- #61861 — threaded the `team_name` scope through the manager's decisions.
- #65685 — kept the public/anonymous decision in the auth-manager path, not a bespoke
  route check.

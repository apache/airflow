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

# 3. The API server is the sole authorized mediator of client-database access

Date: 2026-07-19

## Status

Accepted

## Context

Airflow's architecture boundary (see the project security model) places the API
server as the **single mediator between clients and the metadata database**. The
React UI and every external REST client hold no database credentials; they reach
Airflow's data only through `routes/public/` and `routes/ui/`. That makes the route
layer the one place where two things must hold for *every* data-touching request:

- **Authorization** — decided by the auth manager and enforced through FastAPI
  **dependencies**: `GetUserDep` resolves the user, `requires_access_*` gate access to
  a resource, and the permitted-filter factories in `security.py` narrow a query to
  the rows a user may see. Enforcement lives in the dependency wired onto the route,
  not in ad-hoc checks in the handler body — and because the check is declarative it
  is easy to omit or mis-scope.
- **Boundedness** — because these are user-driven queries against shared tables, an
  unbounded `SELECT` or an N+1 that passes on a small fixture can degrade the API
  server (and therefore the UI and every client) at real data volume. List endpoints
  go through the shared `paginated_select` helpers; per-row work is batched, not looped.

The recurring failures are specific: authorizing against an attacker-controllable
query-string value instead of the real resource; a bulk endpoint that authorizes the
batch as a whole so one authorized item smuggles in an unauthorized one; a per-row
team-name lookup that turns a list endpoint into N+1; and a list route that forgets
to paginate.

## Decision

All client access to the metadata database flows through the API server's route
layer, subject to two invariants:

- **Every endpoint carries the correct authorization dependency**, scoped to the
  resource it actually touches. Authorization is decided by the auth manager via the
  `security.py` dependencies (`GetUserDep`, `requires_access_*`, permitted-filter
  factories) — not by trusting a client-supplied identifier. The resource authorized
  must be the one the operation acts on (e.g. a file's Dags), not a query-string value
  the caller controls.
- **Bulk and multi-resource endpoints authorize every item and every team context** —
  no path lets one permitted item carry an unpermitted one, across create/overwrite or
  cross-team operations.
- **DB access from endpoints is bounded** — list queries paginate via the shared
  `paginated_select` helpers (no unbounded scan of a user-driven table), related
  lookups are batched/`joinedload`ed to avoid N+1, and the repo-wide DB rules apply
  (keyword-only `session`, no `session.commit()` inside a function that takes a
  `session`).

## Consequences

- The metadata DB has exactly one authorized front door; clients cannot reach data
  outside their token/permission scope.
- Authorization is auditable in one place — the dependencies on the route.
- The API server stays responsive at deployment scale because access is bounded by
  construction; new endpoints carry the cost of wiring the right dependency and a
  bounded query, which is the boundary.

A change **violates** this decision when, in `core_api` route/service code, it:

- adds or edits a data-touching endpoint without the correct `requires_access_*` /
  permitted-filter dependency, or scopes authorization to the wrong resource;
- authorizes against an attacker-controllable value (a query-string `dag_id`) instead
  of the resource the operation acts on;
- lets a bulk / multi-resource / cross-team path authorize the batch as a whole so an
  unauthorized item rides along with authorized ones;
- issues an unbounded list query against a user-driven table (bypassing
  `paginated_select`) or introduces an N+1 access pattern in a per-row/per-team lookup;
- opens a route that lets a client obtain data outside its permitted-filter scope.

A reviewer should reject any endpoint that touches the database without an enforced,
correctly-scoped authorization dependency and a bounded query.

## Evidence

- #69471 — authorized Dag reparse against the file's Dags, not the query-string `dag_id`.
- #67662 — added missing per-file authorization to the dag-source endpoint.
- #67493 — closed a bulk CREATE+OVERWRITE team-context authz bypass.
- #68573 — enforced destination-team permission on bulk connections/variables/pools.
- #67620 — required existing-connection read access when testing an existing connection.
- #68286 — avoided N+1 team-name queries in bulk Dag run authorization.
- #65604 / #64845 — cursor-based pagination for get-dag-runs and get-task-instances.
- #64963 — kept user-driven query filtering on indexed columns for scale.

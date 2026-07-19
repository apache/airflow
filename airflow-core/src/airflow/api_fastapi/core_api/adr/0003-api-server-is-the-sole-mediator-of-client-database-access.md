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
Airflow's data only by calling the endpoints in `routes/public/` and
`routes/ui/`. This makes the route layer the one place where two things must hold
for *every* client request that touches data:

- **Authorization.** Whether a caller may see or change a resource is decided by
  the auth manager and enforced through FastAPI **dependencies** — `GetUserDep`
  resolves the user, `requires_access_*` dependencies gate access to a resource,
  and the permitted-filter factories in `security.py` narrow a query to the rows a
  user is allowed to see. Enforcement lives in the dependency wired onto the route,
  not in ad-hoc checks inside the handler body. A route added or edited without the
  right dependency is an authorization hole, and because the check is declarative
  it is easy to omit or mis-scope.
- **Boundedness.** Because these are user-driven queries against tables shared by
  the whole deployment, an unbounded `SELECT` or an N+1 access pattern that passes
  in a small-fixture test can degrade the API server — and therefore the UI and
  every client — at real data volume. List endpoints go through the shared
  `paginated_select` helpers, and per-row work is batched, not looped.

The recurring failure modes are specific: authorizing against an
attacker-controllable query-string value instead of the real resource; a bulk /
multi-resource endpoint that authorizes the batch as a whole and lets one
authorized item smuggle in an unauthorized one; a per-row team-name lookup that
turns a list endpoint into N+1; and a list route that forgets to paginate. Each
is a hole in the boundary that only this layer can close.

## Decision

All client access to the metadata database flows through the API server's route
layer, subject to two invariants:

- **Every endpoint carries the correct authorization dependency**, scoped to the
  resource it actually touches. Authorization is decided by the auth manager via
  the `security.py` dependencies (`GetUserDep`, `requires_access_*`, permitted-
  filter factories) — not by trusting a client-supplied identifier. The resource
  authorized must be the one the operation acts on (e.g. a file's Dags), not a
  query-string value the caller controls.
- **Bulk and multi-resource endpoints authorize every item and every team
  context** — no path lets one permitted item carry an unpermitted one, across
  create/overwrite or cross-team operations.
- **DB access from endpoints is bounded** — list queries paginate via the shared
  `paginated_select` helpers (no unbounded scan of a user-driven table), related
  lookups are batched/`joinedload`ed to avoid N+1, and the repo-wide DB rules
  apply (keyword-only `session`, no `session.commit()` inside a function that
  takes a `session`).

## Consequences

- The metadata DB has exactly one authorized front door; clients cannot reach data
  outside the scope their token/permissions allow.
- Authorization is auditable in one place — the dependencies on the route — rather
  than scattered through handler bodies.
- The API server stays responsive at deployment scale because list and per-row
  access is bounded by construction.
- New data-touching endpoints carry the cost of wiring the right dependency and a
  bounded query — intentional, because that cost is the boundary.

A change **violates** this decision when, in `core_api` route/service code, it:

- adds or edits a data-touching endpoint without the correct `requires_access_*` /
  permitted-filter dependency, or scopes authorization to the wrong resource;
- authorizes against an attacker-controllable value (a query-string `dag_id`)
  instead of the resource the operation acts on;
- lets a bulk / multi-resource / cross-team path authorize the batch as a whole so
  an unauthorized item rides along with authorized ones;
- issues an unbounded list query against a user-driven table (bypassing
  `paginated_select`) or introduces an N+1 access pattern in a per-row/per-team
  lookup;
- opens a route that lets a client obtain data outside its permitted-filter scope.

A reviewer should reject any endpoint that touches the database without an
enforced, correctly-scoped authorization dependency and a bounded query.

## Evidence

- #69471 — "Authorize Dag reparse against the file's Dags, not the query-string
  `dag_id`": fixed authorization that trusted a client-supplied identifier instead
  of the real resource.
- #67662 — "Apply per-file authorization to dag-source endpoint": added the
  missing resource-scoped authorization to a data-returning route.
- #67493 — "Fix bulk CREATE+OVERWRITE team-context authz bypass": closed a bulk-
  endpoint hole where an operation smuggled past team-context authorization.
- #68573 — "Check destination team permission when using bulk APIs with
  connections, variables and pools": enforced per-item team authorization on bulk
  writes.
- #67620 — "Require existing-connection read access when testing an existing
  connection": added the access check a data-touching action was missing.
- #68286 — "Avoid N+1 team-name queries in bulk Dag run authorization": kept the
  authorization path bounded by batching the per-row team lookup.
- #65604 / #64845 — cursor-based pagination for the get-dag-runs and
  get-task-instances endpoints: bounded list queries through the shared pagination
  path rather than unbounded scans.
- #64963 — "Update search parameters to better leverage DB indexes": kept
  user-driven query filtering on indexed columns so the API server stays
  responsive at scale.

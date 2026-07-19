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

# 1. This client is the only path from the worker to server state, on a task-scoped token

Date: 2026-07-19

## Status

Accepted

## Context

A running task needs server-held state constantly — the value of a Variable or
Connection, an XCom pushed by an upstream task, its own state transitions, an
asset event to emit, a Dag run to trigger. In Airflow's security model the
worker runs untrusted author code and must **never** hold a metadata-DB session;
every one of those interactions instead travels over the network to the API
server's Execution API. `client.py` is the single object that speaks that
protocol on the worker side. There is no second path: the supervisor owns one
`Client`, hands it the task's credential, and every operation namespace
(`task_instances`, `variables`, `xcoms`, `dag_runs`, …) issues its request
through it.

The credential is a **short-lived JWT scoped to the one task instance**.
`BearerAuth` attaches it as the `Authorization` header; when the server returns a
`Refreshed-API-Token` header the client swaps it in via `_update_auth`, so a
long-running task keeps a fresh but still task-bounded token without ever
widening its scope. The client opens **no** database connection of any kind — its
only egress is HTTP to the configured Execution-API base URL.

The mirror image of this decision lives on the server
(`airflow-core/src/airflow/api_fastapi/execution_api/`): a restricted,
authenticated surface whose token scopes and per-task-instance (`ti:self`)
validation are what make handing a worker a token safe. This client's obligation
is to keep *all* of the worker's server-state access on that one mediated,
token-scoped channel — the recurring pressure being convenience and latency,
where reaching around it for "just one row" looks harmless.

## Decision

All worker access to server-held state goes through this Execution-API client,
authenticated with the task's short-lived, task-scoped token; the client opens no
metadata-DB connection. Concretely:

- Every server-state interaction a worker needs is a method on a `Client`
  operation namespace that issues an HTTP request to the Execution API — never a
  direct `airflow.models` / ORM query and never a raw DB connection.
- The task-scoped JWT lives in `BearerAuth` and is refreshed *in place* from the
  server's `Refreshed-API-Token` header; its scope is never widened and it is
  kept out of logs.
- A denied authorization (`401`/`403`) is honored as a distinct
  `PERMISSION_DENIED` outcome — the client does **not** reach around the denial to
  a broader, unmediated source for the same data.

## Consequences

- The worker's blast radius stays bounded: author code runs, but it is never
  handed the server's DB credentials or an unrestricted query surface, and its
  authority expires with the task.
- Every new server-state need a runtime feature has must be expressed as a client
  method against an Execution-API endpoint, which keeps the isolation seam
  visible and reviewable instead of buried in an incidental query.
- There is a network dependency and an extra round-trip — going over HTTP is more
  work than a local read — and that friction is intentional.

A change **violates** this decision when it:

- adds a direct `airflow.models` / ORM session or a raw DB connection to fetch or
  mutate server state instead of issuing a client call;
- moves the token or the client into user/task code, widens the token's scope
  beyond its own task instance, or makes it long-lived or shared;
- logs the JWT unredacted;
- adds an unmediated fallback (direct secrets backend, direct HTTP to another
  service, filesystem) that fetches data the Execution API just denied or would
  mediate.

## Evidence

- #62343 — "Add async connection testing via workers for security isolation":
  routes connection testing through this client / the Execution-API path
  specifically to preserve isolation rather than reaching for server-side access.
- #66575 — "Refuse secrets-backend fallback on Execution-API authz deny": the
  client surfaces `401`/`403` as `PERMISSION_DENIED` so the secrets backend does
  **not** fall back to a less-restrictive source — the mediated decision is
  honored.
- #48597 — "Issue refreshed Execution API JWT to tasks if their current token is
  expiring": the task token is refreshed in place rather than lengthened or
  widened.
- #60108 — "Two-token mechanism for task execution to prevent token expiration
  while tasks wait in executor queues": keeps tokens valid across queueing
  without broadening their scope.

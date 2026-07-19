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

# 1. Workers reach the metadata DB only through the Execution API, on a task-scoped token

Date: 2026-07-19

## Status

Accepted

## Context

The worker is where **untrusted Dag-author code actually runs** — the operator
body, the `python_callable`, hooks, user-supplied templates. Airflow's security
model treats that code as hostile: a task must not be able to reach the metadata
database directly, because a direct ORM session would hand author code the
server's credentials and an unrestricted query surface over every other
deployment's data.

The runtime in this directory is what enforces that. The task subprocess
(`task_runner.py`) holds **no** database session; every interaction it needs
with metadata — reading a Variable/Connection/XCom, updating task state, sending
an asset event, fetching a rendered field — is sent as a message up to the
**supervisor** (`supervisor.py`), which is the only side that talks to the API
server's Execution API. The supervisor authenticates with a **short-lived JWT
scoped to that one task instance**; the token is refreshed by the supervisor as
it nears expiry and redacted from logs, so even the supervisor's authority is
bounded to the task it is running and time-boxed.

The mirror image of this decision lives on the server: the Execution API is a
restricted, authenticated surface whose token scopes and per-task-instance
(`ti:self`) validation are what make handing a worker a token safe. The worker
runtime's obligation is to keep *all* of its metadata access on that path.

The recurring pressure is convenience and latency: reading one row "directly"
from inside the task looks harmless and saves a round-trip, and secrets backends
tempt a local fallback when an Execution-API authorization is denied. The
decision below is what keeps every metadata touch on the mediated, token-scoped
path instead.

## Decision

The worker task process reaches the metadata database **only** through the
Execution API, mediated by the supervisor, never through a direct ORM session or
raw DB connection. Concretely:

- The task subprocess (`task_runner.py`) opens no `airflow.models` session and no
  raw DB connection; it obtains and mutates metadata by sending a `comms.py`
  message to the supervisor, which issues the corresponding Execution-API call.
- The Execution-API client and the credentials live on the **supervisor** side.
  Each task is authenticated with a **short-lived JWT scoped to its task-instance
  id**, refreshed before expiry and kept out of logs.
- A denied Execution-API authorization is honored — the runtime does **not**
  fall back to a broader, unmediated access path (e.g. a direct secrets-backend
  read) to get the data anyway.

## Consequences

- The worker's blast radius stays bounded: author code runs, but it is never
  handed the server's DB credentials or an unrestricted query surface, and its
  authority expires with the task.
- Every metadata need a new runtime feature has must be expressed as an
  Execution-API call, which keeps the isolation seam visible and reviewable
  instead of buried in an incidental query.
- There is friction and an extra round-trip — going through the supervisor is
  more work than a local read — and that friction is intentional.

A change **violates** this decision when, in worker-runtime code reachable from
the task subprocess, it:

- opens a direct `airflow.models` / ORM session or a raw DB connection to read or
  write metadata from inside the task process, instead of sending a supervisor
  message;
- moves the Execution-API client or its credentials into the task subprocess, or
  widens the task token's scope beyond its own task instance / makes it
  long-lived or shared;
- hands the JWT to user code or logs it unredacted;
- adds an unmediated fallback (secrets backend, direct HTTP, filesystem) that
  fetches metadata the Execution API just denied or would mediate.

A reviewer should reject any change that gives the running task a wider, more
direct route to the metadata database than a supervisor-mediated Execution-API
call on its own task-scoped token.

## Evidence

- #48597 — "Issue refreshed Execution API JWT to tasks if their current token is
  expiring": keeps the task-scoped token short-lived by refreshing it from the
  supervisor rather than widening its lifetime.
- #66575 — "Refuse secrets-backend fallback on Execution-API authz deny": when
  the Execution API denies access, the runtime does **not** fall back to a direct
  secrets-backend read — the mediated decision is honored.
- #62343 — "Add async connection testing via workers for security isolation":
  routes connection testing through the worker/Execution-API path specifically to
  preserve isolation rather than testing connections with server-side access.
- #48614 — "Ensure that jwt token is redacted in executor logs" (and #55499,
  "Once again redact JWT tokens in task logs"): keeps the task token out of logs,
  reinforcing that the credential stays bounded to the supervisor.

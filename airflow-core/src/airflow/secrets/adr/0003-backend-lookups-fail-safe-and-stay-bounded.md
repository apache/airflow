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

# 3. Backend lookups fail safe and stay bounded

Date: 2026-07-19

## Status

Accepted

## Context

Secret resolution walks a chain — configured custom backend, environment variables,
metastore — returning the first hit, on the hot path of task setup and operator
execution where one link (Vault or a cloud store) is a remote service that can be
slow, rate-limited, or down. Three properties keep this safe:

- **Fail-safe fall-through.** A backend that errors on one key catches, logs the
  backend *type* at debug, and moves on; only when all come up empty is "not found"
  raised. The one exception is an **authoritative access-denied**
  (`AirflowSecretsBackendAccessDenied`), which re-raises immediately and must **not**
  fall through to a looser backend, or a deny is silently downgraded.
- **Bounded external access.** Lookups go through `SecretCache` first, and backends
  expose opt-outs (e.g. `connections_prefix=None`). An unconditional, uncached
  per-call round-trip turns every task startup into a burst of remote calls.
- **DB-mediated metastore.** Server-side it opens a session, fetches with
  `limit(1)`, and `expunge`s the object, obeying the repo DB rules (keyword-only
  `session`, no `session.commit()` in a session-taking function). A **worker does
  not reach the metastore with a direct session** — it resolves through the
  Execution API, so untrusted task code gets no privileged metadata-DB connection.

## Decision

Secret lookups must fail safe and stay bounded:

- **A backend error falls through to the next backend**, logging only the backend
  type — never crash the caller because one link failed. **Do not** widen or
  narrow this: an authoritative access-denied re-raises and does not fall through;
  everything else falls through and does not abort the chain.
- **Go through `SecretCache` and honour opt-outs** — do not add an unconditional,
  uncached per-call round-trip to an external store; respect
  `connections_prefix=None`-style controls that let a deployment disable a lookup.
- **The metastore backend stays DB-mediated and disciplined** — bounded queries
  (`limit(1)`), expunge fetched objects (`expunge`, not `expunge_all`), keyword-
  only `session`, and no `session.commit()` inside a session-taking function.
- **Workers reach the metastore through the Execution API, not a direct session.**
  The worker-side search path is the client one; do not give task-side code a
  direct privileged metadata-DB connection to resolve secrets.

## Consequences

- A single misconfigured or unreachable backend degrades to the next source instead
  of failing every task that resolves a secret; an authoritative deny stays
  authoritative.
- External stores are hit through a cache and can be opted out of, sparing task
  startup a remote stampede; the metastore path keeps DB access explicit, bounded,
  and — for workers — routed through the Execution API.

**A violating change looks like:** re-raising a generic backend error so it aborts
the chain (or, conversely, swallowing an access-denied and falling through to a
looser backend); adding an uncached per-lookup call to a remote store; issuing an
unbounded metastore query, committing inside a session-taking function, or using
`expunge_all`; or giving worker-side secret resolution a direct ORM session in
place of the Execution API. Such a change is rejected.

## Evidence

- #56602 — "Fix Connection or Variable access in Server context": kept the
  server/worker resolution paths distinct.
- #67810 — "Fix ... positional session use in airflow-core ...": tightened session
  discipline in this area.
- #63080 — "Replace expunge_all with expunge in MetastoreBackend": detached only the
  fetched object, not the whole session.

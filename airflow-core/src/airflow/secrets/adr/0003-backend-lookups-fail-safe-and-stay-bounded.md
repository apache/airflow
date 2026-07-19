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

Secret resolution walks a chain of backends — configured custom backend, then
environment variables, then the metastore — and returns the first hit. That chain
runs on the hot path of task setup and operator execution, and one of its links,
a Vault or cloud secret store, is a **remote service** that can be slow, rate-
limited, or down. Two properties keep this from becoming a reliability or
correctness problem:

- **Fail-safe fall-through.** A backend that errors while resolving one key must
  not abort the whole lookup. The resolver catches the failure, logs the backend
  *type* at debug, and moves on to the next backend; only when every backend
  comes up empty does it raise "not found". The one deliberate exception is an
  **authoritative access-denied** (`AirflowSecretsBackendAccessDenied`): that must
  re-raise immediately and **not** fall through to a less-restrictive backend, or
  a deny would be silently downgraded to an allow.

- **Bounded external access.** Lookups go through `SecretCache` first, and
  provider backends expose opt-outs (e.g. `connections_prefix=None`) so a
  deployment can skip a store it doesn't use. A change that adds an
  unconditional, uncached, per-call round-trip to an external store turns every
  task startup into a burst of remote calls.

The metastore link has its own version of "bounded": it is the **DB-mediated**
backend. On the server side it opens a session, fetches the row with a `limit(1)`
query, and `expunge`s the ORM object so a detached, safe value is returned. It
must obey the repo DB rules — keyword-only `session`, no `session.commit()` inside
a function that takes a `session`. Crucially, a **worker does not reach the
metastore with a direct session**: it resolves secrets through the Execution API
using the client-side search path, so untrusted task code never gets a privileged
metadata-DB connection just to read a variable.

The recurring pressure is, again, convenience: "if the backend throws, just
re-raise so the user sees it", or "read the row directly here, it's simpler than
the API". Both break the properties above.

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

- A single misconfigured or unreachable backend degrades to the next source
  instead of failing every task that resolves a secret.
- An authoritative deny stays authoritative — it is never silently downgraded by
  the fall-through logic.
- External secret stores are hit through a cache and can be opted out of, keeping
  task startup from stampeding a remote service.
- The metastore path keeps its DB access explicit, bounded, and — for workers —
  routed through the Execution API rather than a direct session.

**A violating change looks like:** re-raising a generic backend error so it aborts
the chain (or, conversely, swallowing an access-denied and falling through to a
looser backend); adding an uncached per-lookup call to a remote store; issuing an
unbounded metastore query, committing inside a session-taking function, or using
`expunge_all`; or giving worker-side secret resolution a direct ORM session in
place of the Execution API. Such a change is rejected.

## Evidence

- #56602 — "Fix Connection or Variable access in Server context": corrected how
  secrets are resolved depending on execution context, keeping the server/worker
  paths distinct rather than letting the wrong one run.
- #67810 — "Fix exceptions of positional session use in airflow-core ... leftover
  non-models modules": tightened session discipline in this area, consistent with
  keeping the metastore path's DB access explicit and correct.
- #63080 — "Replace expunge_all with expunge in MetastoreBackend": bounded the
  session hygiene so only the fetched object is detached, not the whole session.

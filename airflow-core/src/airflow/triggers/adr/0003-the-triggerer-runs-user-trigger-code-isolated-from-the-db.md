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

# 3. The triggerer runs user trigger code isolated from the metadata DB, and delivers events at least once

Date: 2026-07-19

## Status

Accepted

## Context

Trigger `run()` bodies are **user-authored code** — a custom operator's async
counterpart polling a REST API, watching a broker, waiting on an external job —
which Airflow neither controls nor trusts. This puts the triggerer in the Dag
File Processor's position: it *must* execute untrusted author code, so it cannot
avoid the problem the way the scheduler does by never importing user code.

The boundary is drawn as for the DFP: the code runs but does **not** hold a
direct, privileged server DB session. The async trigger runner executes in a
subprocess launched with `_AIRFLOW_PROCESS_CONTEXT=client`, and reaches server
state through the Execution API (via the supervisor and an in-process API server)
rather than an `airflow.models` ORM session. Where a trigger needs server-held
state, the runner **injects an accessor** (e.g. `asset_state_store` /
`AssetStateStoreAccessors` on `BaseEventTrigger`). As with the DFP, these are
software guards that *steer* trigger code onto the client path; the security
model is explicit they do not defend against *intentional* bypass — which is why
a change here must not casually widen the runner's DB reach.

Delivery semantics are the second half. Because triggers are re-run across
restart, redistribution, and HA duplication, a `TriggerEvent` can be delivered
**more than once**. The system is **at-least-once** by design — losing an event
(a task stuck deferred forever) is worse than delivering it twice — so resumption
handlers must be idempotent; code assuming single delivery breaks silently under
normal triggerer churn.

## Decision

The triggerer runs user trigger code isolated from the privileged database,
and its event delivery is at-least-once. Concretely:

- The async trigger runner runs with `_AIRFLOW_PROCESS_CONTEXT=client` and
  reaches server state **only through the Execution API / supervisor**, never
  by opening an `airflow.models` ORM session from trigger-side code.
- Server-held state a trigger needs is reached through a **runner-injected
  accessor**, not a new metadata-DB query inside the trigger.
- `TriggerEvent` delivery is **at-least-once**: handlers that act on an event
  (resuming, ending, or failing the task) must tolerate the same event
  arriving more than once and be idempotent.

## Consequences

- The triggerer's blast radius stays bounded: trigger code runs but is not handed
  the server's DB credentials or an unrestricted query surface, the DFP's
  isolation property.
- New trigger features needing server data express it as an Execution-API call or
  injected accessor, keeping the isolation seam visible.
- Resumption logic must survive redelivery — more work than exactly-once, and
  intentionally so, since at-least-once is what makes deferral robust.

A change **violates** this decision when, in trigger code reachable from the
triggerer, it:

- opens a direct `airflow.models` / ORM session (or raw DB connection) to read
  or write metadata from the trigger, instead of the Execution API;
- removes, weakens, or bypasses the `_AIRFLOW_PROCESS_CONTEXT=client` guard on
  the runner path, or runs trigger code under the server process context;
- fetches server-held state by querying the metadata DB from the trigger
  rather than through an injected accessor / the Execution API;
- assumes `TriggerEvent` delivery is exactly-once — building non-idempotent
  resumption that double-delivery would corrupt.

A reviewer should reject any change that gives trigger code a wider, more
direct route to the metadata database than the Execution API, or that assumes
events fire exactly once.

## Evidence

- #62645 — moved `ExecutorCallback` execution into a supervised subprocess rather
  than inline with server privileges.
- #66608 — moved deadline-callback context onto the Execution API instead of a
  direct trigger-side DB read (reverted in #68909 and reworked — a delicate seam).
- #67839 — AIP-103 injects `AssetStateStoreAccessors` so the trigger reads asset
  state through that surface, not its own DB query.
- #68888 — `shared_stream_cohort_grace_period` tunes redelivery on restart,
  reflecting the at-least-once model.

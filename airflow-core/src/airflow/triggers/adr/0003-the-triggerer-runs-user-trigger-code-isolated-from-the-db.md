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

Trigger `run()` bodies are **user-authored code**. A trigger is a
custom operator's async counterpart — it polls a REST API, watches a broker,
waits on an external job — and Airflow neither controls nor trusts what that
code does. This puts the triggerer in the same position as the Dag File
Processor: it *must* execute untrusted author code to do its job, so it cannot
avoid the problem the way the scheduler does (by never importing user code, see
the scheduler's ADR).

The boundary is therefore drawn the same way it is for the DFP: the code runs,
but it does **not** hold a direct, privileged server database session. The
async trigger runner executes in a subprocess launched with
`_AIRFLOW_PROCESS_CONTEXT=client`, and its interactions with server state go
through the Execution API (the same restricted, authenticated surface workers
use) via the supervisor and an in-process API server, rather than an
`airflow.models` ORM session. Where a trigger needs server-held state, the
runner **injects an accessor** (for example `asset_state_store` /
`AssetStateStoreAccessors` on `BaseEventTrigger`) so the trigger reads through
that accessor instead of opening its own query. As with the DFP, these are
software guards that *steer* trigger code onto the client path; the security
model is explicit that they do not defend against *intentional* bypass — which
is exactly why a change here must not casually widen the runner's DB reach.

The second half of this decision is delivery semantics. Because triggers are
reconstructed and re-run across restart, redistribution, and HA duplication, a
`TriggerEvent` can be delivered to the task **more than once**. The system is
**at-least-once**, not exactly-once, by design — losing an event (a task stuck
deferred forever) is worse than delivering one twice. Handlers that resume a
task must therefore be idempotent; code that assumes a single delivery breaks
silently under perfectly normal triggerer churn.

The recurring pressure is convenience on both fronts: the trigger is already
running server-side code, so "just read this row from the DB here" looks
harmless, and "this event only fires once" looks locally true. The decision
below is what keeps trigger code on the API path and keeps handlers honest
about redelivery.

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

- The triggerer's blast radius stays bounded: user trigger code runs, but it
  is not handed the server's DB credentials or an unrestricted query surface —
  the same isolation property the DFP has.
- New trigger features that need server data must express that need as an
  Execution-API call or an injected accessor, keeping the isolation seam
  visible and reviewable.
- Task-resumption logic must be written to survive redelivery, which is more
  work than assuming exactly-once — and intentionally so, because at-least-once
  is what makes deferral robust across triggerer restarts and HA.

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

- #62645 — "Move `ExecutorCallback` execution into a supervised process":
  pushed callback execution into a supervised subprocess rather than running it
  inline with server privileges, consistent with keeping user-triggered code on
  the isolated, supervised side.
- #66608 — "Fetch deadline callback context via Execution API at runtime":
  moved callback context retrieval onto the Execution API instead of a direct
  DB read from the trigger side (later reverted in #68909 and reworked —
  showing how load-bearing and delicate this seam is).
- #67839 — "AIP-103: Passing `AssetStateStoreAccessors` through to
  `BaseEventTrigger`": injects an accessor into the trigger so it reads
  asset state through that surface rather than opening its own DB query.
- #68888 — "Add `shared_stream_cohort_grace_period` to reduce missed events on
  triggerer restart": tunes redelivery behaviour around triggerer restart,
  reflecting the at-least-once delivery model this decision codifies.

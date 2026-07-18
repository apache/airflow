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

# 3. Session discipline and set-based database access

Date: 2026-07-18

## Status

Accepted

## Context

Model code runs inside the scheduler loop and API request handlers, under real
concurrency and latency budgets. The scheduler serialises much of its work
through a single main loop; a slow or lock-holding query there stalls scheduling
for every Dag, not just the one being processed. Multiple schedulers and the API
hit the same tables concurrently, so lock acquisition order and transaction scope
decide whether the system runs smoothly or deadlocks.

Two failure modes recur:

- **Session misuse** — a function that receives a `session` but opens its own
  new session mid-operation splits one logical unit of work across two
  transactions, and a `session.commit()` inside such a function commits a
  caller's in-progress transaction out from under it, breaking atomicity and
  the caller's rollback expectations.
- **Row-at-a-time access** — a per-row query inside a loop (N+1) turns one
  operation into hundreds of round-trips, and unbounded bulk writes / mismatched
  lock ordering across `dag_run` and `task_instance` produce deadlocks under
  concurrent schedulers.

## Decision

- **Thread the active session through.** `session` is a keyword-only parameter;
  functions use the session they are given and do not open a fresh one mid
  operation. Per the project rule, a function in `airflow-core` that takes a
  `session` **must not** call `session.commit()` — the caller owns the
  transaction boundary.
- **Access the database in sets.** Batch and eager-load to avoid N+1; load the
  TIs/DagRuns for a decision in one query rather than per row.
- **Fix deadlocks with deterministic lock ordering, not retries.** Acquire locks
  in a consistent order (`DagRun` before `task_instance`) and use
  `with_for_update(..., skip_locked=True)` / `SKIP_LOCKED` where the semantics
  allow, so concurrent schedulers step around each other instead of colliding.
- **Scope bulk writes to the owning scheduler.** A scheduler mutates the runs it
  owns; bulk `UPDATE`/`DELETE` is bounded and does not reach across into rows
  another scheduler is driving.

## Consequences

- Transactions stay atomic and callers keep control of commit/rollback, so
  partial writes do not leak out on error.
- The scheduler loop stays responsive: set-based access and bounded, correctly
  ordered writes keep round-trips and lock contention low.

A **violating** change looks like: opening `create_session()` / a new session
inside a function that already received one; adding a `session.commit()` to a
function that takes a `session`; issuing a query per element inside a loop
instead of one set query; ordering locks `task_instance`-before-`DagRun` (or
inconsistently) so two schedulers can deadlock; "fixing" a deadlock by wrapping
the operation in a retry loop instead of correcting lock order/scope; or a bulk
write that is unbounded or reaches beyond the scheduler's own rows.

## Evidence

- #66608 — Fetch deadline callback context via Execution API at runtime: moves
  DB access off a hot path and through the proper boundary.
- #64571 — AIP-76: Hold Dag run until all upstream partitions arrive: adds a
  wait/hold mechanism that must query and lock partition state set-based and
  safely under the scheduler.
- #33172 — Skip trigger timeout check on occasional db deadlocks: the
  retry-style deadlock workaround this ADR steers away from in favour of lock
  ordering.

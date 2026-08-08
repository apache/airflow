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

# 4. Asset event emission and Dag-run queueing must be replay-safe

Date: 2026-07-20

## Status

Accepted

## Context

Registering an asset change is not one action: it records an asset event, updates
the asset-partition/Dag-run queue rows, notifies listeners, and emits metrics — on
the hot path where a finishing task reports outlets through the Execution API,
concurrently across every worker.

Two properties make this fragile. First, contention: many workers update the same
asset rows at once and the queue insert is a natural deadlock site, so this area
has repeatedly needed locking and upsert corrections. Second — the property that
generates rejected PRs — **the unit of retry is the whole call, but not everything
in it is transactional.** Listener hooks and the `asset.updates` metric fire
before the Dag runs are queued and are outside the DB transaction, so a retry
re-runs them. One MySQL-deadlock fix attempt surfaced exactly this: review found
the hooks and metric would re-run on replay, and that the deadlock never surfaces
as a 5xx so the task-side retry never engages. Another attempt released the row
lock before emitting events, changing the sequence's atomicity rather than its
cost. Getting this wrong is not a crash — it is a double-fired listener, an
inflated metric, or a Dag run queued twice, surfacing days later with nothing in
the logs pointing back.

## Decision

Changes to asset-change registration and Dag-run queueing must be correct under
concurrent execution and under replay. Concretely:

- **Non-transactional side effects belong after the commit, or must be
  idempotent.** Listener invocations and metric emission must not be positioned
  where a retried transaction re-runs them, unless re-running them is harmless
  and stated to be so.
- **Queue writes are atomic upserts, not read-then-write.** Inserting or updating
  asset queue rows must use a single atomic statement rather than a check
  followed by an insert, which races under concurrency.
- **A change that alters lock scope states what becomes non-atomic.** Releasing a
  lock earlier to reduce contention changes which steps can interleave; that
  trade must be named, not implied by a performance argument.
- **Deadlock and contention fixes name the backend.** MySQL and PostgreSQL differ
  in lock and upsert behaviour here; a fix must say which backend exhibited the
  problem and what the other one does under the change.
- **Retry behaviour is part of the change.** If the failure mode being fixed does
  not surface as a retryable error to the caller, say so — a fix that assumes the
  client retries when it does not is not a fix.

## Consequences

- Performance work in this path routinely turns into a correctness discussion.
- Some contention stays unaddressed because the obvious remedy (shorter locks)
  would make the sequence non-atomic; the project accepts slower over duplicated.
- Emission is at-least-once at worst, so listeners must tolerate repeats.
- Testing needs concurrent, backend-specific tests, raising the contribution bar.

A change **violates** this decision when it:

- moves listener invocation or metric emission inside a transaction boundary that
  may be retried, without establishing that re-emission is harmless;
- replaces an atomic upsert on the asset queue with a check-then-insert, or adds
  a read-modify-write to the queueing path;
- narrows or releases a lock around asset event emission without stating which
  steps can now interleave;
- fixes a deadlock without naming the affected backend and the behaviour under
  the other supported backends;
- assumes the caller retries a failure that does not reach the caller as a
  retryable status.

A reviewer should ask: if this transaction is retried, what runs twice — and if
two workers reach this line at the same moment, which one wins?

## Evidence

- #69977 — merged: atomic upsert for the asset run queue on MySQL, replacing a racing pattern.
- #69944 — closed unmerged; review found the hooks/metric re-run on replay and the deadlock never reaches the caller as a 5xx.
- #66932 — closed unmerged; relieved contention by changing lock scope, not the write pattern.
- #59183 — merged mutex on `AssetModel` when updating asset-partition Dag-run rows.
- #63848 — merged; the queueing decision, not the event record, was wrong (unexpected Dag runs).
- #48427 — merged: wrap listener invocations in try/except so a failing listener can't down the path.

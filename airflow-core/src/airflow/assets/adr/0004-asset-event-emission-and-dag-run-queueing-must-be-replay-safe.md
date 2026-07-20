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

Registering an asset change is not one action. It records an asset event, updates
the asset-partition/Dag-run queue rows, notifies listeners, and emits metrics —
and it does this on the hot path where a finishing task reports its outlets
through the Execution API, concurrently across every worker in the deployment.

Two properties make this fragile in a way that is easy to miss in review. The
first is contention: the same asset rows are updated by many workers at once, and
the queue insert is a natural deadlock site. This area has repeatedly needed
locking and upsert corrections — a mutex on `AssetModel` when updating the
asset-partition Dag-run rows, and a move to an atomic upsert for the asset run
queue on MySQL specifically to stop deadlocks.

The second property is the one that generates rejected pull requests: **the unit
of retry is the whole call, but not everything in the call is transactional.**
Listener hooks and the `asset.updates` metric fire before the Dag runs are
queued, and they are not part of the database transaction. When the transaction
is retried — and the task-side client does retry the Execution API call on a 5xx
— those side effects run a second time. An attempt to fix a MySQL deadlock in
this path surfaced exactly this: review established that the listener hooks and
the metric would run again on a replay, and separately that the deadlock error
does not surface as a 5xx at all, so it simply marks the task failed instead of
retrying. A different attempt at the same congestion problem proposed releasing
the row lock before emitting asset events, which changes the atomicity of the
sequence rather than its cost.

The consequence of getting this wrong is not a crash. It is a double-fired
listener, an inflated `asset.updates` metric, or a Dag run queued twice — all of
which look like data problems in the user's deployment days later, with nothing
in the logs pointing back to the change that caused them.

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

- Asset-change registration is harder to modify than its size suggests, and
  performance work in this path routinely turns into a correctness discussion.
- Some contention remains unaddressed because the obvious remedy — holding locks
  for less time — would make the sequence non-atomic. The project accepts slower,
  serialised behaviour over duplicated events.
- Listener and metric authors can rely on emission being at-least-once at worst,
  which shifts a burden onto listener implementations to tolerate repeats.
- Testing this properly needs concurrent, backend-specific tests rather than a
  single-session unit test, which raises the bar for contributions here.

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

- #69977 — "Use atomic upsert for asset run queue on MySQL to avoid deadlocks":
  the merged fix, replacing a racing pattern with a single atomic statement.
- #69944 — "Fix MySQL deadlock when queueing asset-triggered Dags": closed
  unmerged; the review established that listener hooks and the `asset.updates`
  metric run before `_queue_dagruns()` and are not transactional, so they would
  run again on a replay, and that the deadlock does not surface as a 5xx so the
  task-side retry never engages.
- #66932 — "Release row lock before asset event emission under high concurrency":
  closed unmerged; an attempt to relieve the same contention by changing lock
  scope rather than the write pattern.
- #59183 — "Add a mutex lock to `AssetModel` when updating the asset-partition Dag
  run rows": a merged correction in the same concurrency seam.
- #63848 — "Fix partitioned asset events incorrectly triggering non-partition-aware
  Dags": a merged fix where the queueing decision, not the event record, was
  wrong — the class of defect that surfaces as unexpected Dag runs.
- #48427 — "Wrap all listener invocations in a try / except": the merged decision
  that a failing listener must not take down the emitting path, the counterpart to
  keeping listener effects outside the transaction.

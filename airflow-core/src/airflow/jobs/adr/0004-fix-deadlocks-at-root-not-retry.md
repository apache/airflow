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

# 4. Deadlocks and races are fixed at the root (lock ordering), not by retry or skip

Date: 2026-07-18

## Status

Accepted

## Context

Database deadlocks in the scheduler and triggerer surface as intermittent,
load-dependent failures: two transactions each hold a lock the other needs, the
database picks a victim and aborts it. The tempting fix is reactive — catch the
deadlock exception and retry, or skip the offending check "just this once." That
makes the symptom rarer but leaves the real defect in place:

- The underlying cause is almost always **inconsistent lock-acquisition order**
  — two code paths lock the same set of rows/tables in different orders, or a
  bulk write locks a broad range that overlaps another transaction's rows. Retry
  does not remove the cycle; under higher load it simply recurs, now with added
  latency and wasted work from the aborted transactions.
- **Skipping** a check to dodge a deadlock (e.g. bypassing a timeout sweep when
  the DB is contended) silently drops correctness — timeouts stop firing, state
  drifts — to buy quieter logs.
- Reactive handlers also **mask** where the collision is, so the ordering bug is
  never diagnosed and keeps generating new failure modes.

The correct fix removes the *possibility* of the cycle. Establish a single,
consistent lock-acquisition order everywhere the contended rows are touched;
acquire candidate rows with `FOR UPDATE SKIP LOCKED` in a deterministic
primary-key order (so two transactions never wait on each other for claimable
work — see ADR 2); and scope bulk writes to the rows a given scheduler owns
(for example filtering by `queued_by_job_id`) so different schedulers cannot
lock-overlap on each other's work. Because these bugs are load-dependent and
non-deterministic, the fix must be demonstrated with a **live reproduction**:
show the deadlock occurring before the change and gone after, not merely argue
the ordering by inspection.

## Decision

Deadlocks and lock races in the scheduler/triggerer are fixed at the root:

1. **Fix lock-acquisition order.** Make every path that touches the contended
   rows acquire locks in one consistent order so no wait-cycle can form.
2. **Avoid the wait entirely where possible** using
   `FOR UPDATE SKIP LOCKED` with a deterministic primary-key `order_by`.
3. **Scope bulk writes to the owning scheduler** (e.g. `queued_by_job_id`) so
   schedulers do not lock-overlap on rows they do not own.
4. **Prove it with a live repro** — a reproduction that fails before the change
   and passes after.

Retry-on-deadlock and skip-the-check are **not** accepted as the fix for a
scheduler/triggerer deadlock.

## Consequences

- Deadlocks are eliminated rather than made rarer; behavior is stable as load
  grows, with no correctness dropped to dodge contention.
- Fixes take more effort — they require understanding the actual lock interleave
  and building a repro — but they are durable.
- Bounded, well-understood retries remain acceptable for genuinely *transient*
  infrastructure faults (connection drops, serialization failures on an
  otherwise correct ordering); they are not a substitute for fixing a real
  ordering bug.

A change **violates** this decision when it:

- wraps a scheduler/triggerer statement in a catch-and-retry loop for
  `DeadlockDetected`/`OperationalError` **without** changing the lock ordering
  that caused it;
- skips or short-circuits a check (timeout sweep, state transition) specifically
  to avoid contention;
- widens a bulk write's row range instead of scoping it to the owning job;
- claims to fix a deadlock but ships no reproduction demonstrating it is gone.

A reviewer should reject a "deadlock fix" whose diff only adds retry/skip
handling and leaves the lock-acquisition order unchanged.

## Evidence

- #33172 — "Skip trigger timeout check on occasional db deadlocks": the
  reject-shaped example — dodging the deadlock by skipping the timeout check
  rather than fixing the ordering; not the accepted approach.
- #27344 — "Add retry to submit_event in trigger to avoid deadlock": adds
  retry-on-deadlock in the triggerer without addressing the underlying lock
  order — the reactive pattern this ADR rules out as the fix.
- #66820 — "clear identity map between `_do_scheduling` phases": a root-cause
  scheduler correctness fix (stale identity-map state across phases) of the kind
  this decision favors over papering the symptom with retries.

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
load-dependent failures. The tempting fix is reactive — catch the deadlock and
retry, or skip the check "just this once" — which makes the symptom rarer but
leaves the defect in place. The cause is almost always **inconsistent
lock-acquisition order**: two paths lock the same rows/tables in different orders,
or a bulk write locks a broad range overlapping another transaction. Retry does
not remove the cycle; under load it recurs, with added latency and wasted work.
**Skipping** a check (e.g. bypassing a timeout sweep under contention) silently
drops correctness. Reactive handlers also **mask** where the collision is, so the
ordering bug is never diagnosed.

The correct fix removes the *possibility* of the cycle: a single consistent
lock-acquisition order everywhere the contended rows are touched; `FOR UPDATE SKIP
LOCKED` in deterministic primary-key order (see ADR 2); and bulk writes scoped to
the rows a scheduler owns (e.g. `queued_by_job_id`) so schedulers cannot
lock-overlap.

This is not a prohibition on database retry as such. `@retry_db_transaction`
(`airflow/utils/retries.py`) wraps roughly a dozen call sites deliberately —
`scheduler_job_runner.py`, `dag_processing/manager.py`, `models/dagrun.py` — and
`../../dag_processing/adr/0004` rests on it as the layer that correctly absorbs a
losing race at an outer commit boundary. The distinction is *where* the retry
sits. A retry wrapped around a whole unit of work that owns its commit is a
transaction-level control: it re-runs the entire operation from a clean state and
the ordering underneath it is already correct. A retry added at the statement that
raised, inside an operation whose lock order is wrong, is a symptom filter — it
re-enters the same cycle and wins only by luck.

## Decision

Deadlocks and lock races in the scheduler/triggerer are fixed at the root:

1. **Fix lock-acquisition order.** Make every path that touches the contended
   rows acquire locks in one consistent order so no wait-cycle can form.
2. **Avoid the wait entirely where possible** using
   `FOR UPDATE SKIP LOCKED` with a deterministic primary-key `order_by`.
3. **Scope bulk writes to the owning scheduler** (e.g. `queued_by_job_id`) so
   schedulers do not lock-overlap on rows they do not own.

Retry-on-deadlock and skip-the-check are **not** accepted as the fix for a
scheduler/triggerer deadlock. `@retry_db_transaction` at a commit boundary that
owns the whole unit of work is accepted, and is the established pattern here.

Because these failures are load-dependent and non-deterministic, the reproduction
requirement argued in `../../dag_processing/adr/0004` applies to deadlock fixes
here too; it is not restated in this ADR.

## Consequences

- Deadlocks are eliminated rather than made rarer; behavior is stable as load
  grows, with no correctness dropped to dodge contention.
- Fixes take more effort (understanding the actual lock interleave, building a
  repro) but are durable. Bounded retries remain acceptable for genuinely
  *transient* infrastructure faults (connection drops, serialization failures on
  an otherwise correct ordering), and `@retry_db_transaction` remains the correct
  wrapper at a commit boundary — neither substitutes for fixing an ordering bug.

A change **violates** this decision when it:

- adds a bare `try`/`except DeadlockDetected` / `except OperationalError` and a
  retry **around the individual statement that raised**, rather than applying
  `@retry_db_transaction` at the boundary that owns the whole transaction;
- skips or short-circuits a check (timeout sweep, state transition) specifically
  to avoid contention;
- widens a bulk write's row range instead of scoping it to the owning job;
- describes itself as a deadlock fix while its PR body states no reproduction of
  the deadlock.

Reviewer prompts — judgement calls this ADR informs but a diff cannot settle:

- Does the diff change the lock-acquisition order that caused the deadlock, or
  only how the failure is handled? A "deadlock fix" that leaves the ordering
  untouched and only adds retry/skip handling is the shape to reject.
- Is the retry at a boundary that can safely re-run everything inside it?

## Evidence

- #33172 — "Skip trigger timeout check on occasional db deadlocks": the reject-shaped example — dodging the deadlock by skipping the timeout check rather than fixing the ordering.
- #27344 — "Add retry to submit_event in trigger to avoid deadlock": retry-on-deadlock in the triggerer without addressing lock order — the reactive pattern this ADR rules out as the fix.

Both citations are from 2022–2023 and both are reject-shaped; this ADR has no
recent merged exemplar of a root-cause ordering fix behind it. The decision
matches the ordering discipline visible in `scheduler_job_runner.py` today and in
ADR 2, but the evidence base is thin — treat a well-argued counter-example as
grounds to revisit rather than as a violation.

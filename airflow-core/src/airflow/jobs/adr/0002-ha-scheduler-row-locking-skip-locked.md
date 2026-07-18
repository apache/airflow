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

# 2. HA multi-scheduler correctness via row-locking with SKIP LOCKED and deterministic ordering

Date: 2026-07-18

## Status

Accepted

## Context

Airflow supports active-active high availability: several `SchedulerJobRunner`
processes run concurrently against one metadata database, with no leader
election and no external coordination. Correctness therefore rests entirely on
how each scheduler claims work from shared tables (Dag runs, task instances,
asset/partition state, pools). The invariant is simple to state and easy to
break: **two schedulers must never claim the same row, and caps (pool slots,
`max_active_tasks`, `max_active_runs`, concurrency limits) must never be
exceeded even under a race.**

The classic wrong pattern is *read-then-write*: `SELECT count(...)` to check a
cap, then `UPDATE`/`INSERT` if there is room. Between the read and the write a
second scheduler reads the same count and both proceed — the cap is blown and
work is double-dispatched. The same class of bug appears when a query selects
candidate rows without `skip_locked`, so two schedulers block on each other (or
both wait and then act on the same set), and when a query lacks a deterministic
`order_by`, so concurrent schedulers walk candidate rows in different orders and
livelock or repeatedly collide on the same hot rows.

The established Airflow answer is pessimistic row locking: a scheduler selects
the rows it intends to act on `with_for_update(skip_locked=True)`, so it locks a
*disjoint* subset that no other scheduler can touch, skipping rows already locked
elsewhere instead of blocking. Pairing that with a stable `order_by` (a
deterministic tiebreaker, typically down to the primary key) makes each
scheduler grab a predictable slice and keeps progress monotonic. Caps are then
enforced *inside* the same locked transaction — the slot accounting and the
claim happen atomically — rather than as a separate unlocked count.

New features keep re-introducing shared claimable state — held Dag runs waiting
on partitions, pre-assigned execution identifiers, cross-scheduler garbage
collection — and each one must adopt the same claiming discipline or it becomes
an HA correctness bug the moment a second scheduler is running.

## Decision

Any scheduler operation that claims shared, mutable work must:

1. Select the rows it will act on with `with_for_update(skip_locked=True)` so
   concurrent schedulers lock **disjoint** row sets and never block on each
   other for claimable work.
2. Apply a **deterministic `order_by`** (ending in a unique tiebreaker such as
   the primary key) so ordering is stable across schedulers.
3. Enforce every cap **atomically within the locked transaction** — compute
   remaining slots and claim in the same `for_update` scope. Never gate a write
   on a separate unlocked `count()`/`SELECT` (`read-then-write`).

This applies to task-instance scheduling, Dag-run creation, pool-slot
accounting, and any new table that multiple schedulers dequeue from.

## Consequences

- Multiple schedulers scale throughput without duplicating work or overrunning
  limits; adding a scheduler is safe by construction.
- New claimable state must ship with its locking/order/atomic-cap design from the
  start — retrofitting it after a race is observed is far harder.
- Queries are slightly more constrained (they must be lockable and ordered), and
  indexes must support the `order_by` used under lock.

A change **violates** this decision when it:

- checks a limit with a bare `count()`/`SELECT` and then writes based on that
  count in a separate step (`read-then-write` with a TOCTOU window);
- selects claimable rows without `skip_locked` (schedulers block or both act on
  the same rows), or acquires the lock but omits a deterministic `order_by`;
- claims work outside a `with_for_update` scope, or splits the count and the
  claim across two transactions;
- introduces a new multi-scheduler-dequeued table without row locking and a
  stable order.

A reviewer should reject any scheduler claim path that lacks
`with_for_update(skip_locked=True)`, a deterministic order, or that enforces a
cap outside the locked transaction.

## Evidence

- #64571 — "AIP-76: Hold Dag run until all upstream partitions arrive": adds
  held-run/partition state that multiple schedulers observe and release; the
  claim/release must respect row locking so a run is not released twice.
- #62343 — "Add async connection testing via workers for security isolation":
  routes claimable async work through the isolated worker path, keeping the
  scheduler's dispatch within its locked, atomic claim discipline.
- #65594 — "Pre-assign Celery task ID at queuing time to prevent duplicate
  execution on scheduler crash": pins the execution identity atomically at
  queue time so a crash-and-retry (or a second scheduler) cannot dispatch the
  same task instance twice.

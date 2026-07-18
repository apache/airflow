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

# 3. Bulk writes in the scheduler loop are batched with LIMIT and committed between batches

Date: 2026-07-18

## Status

Accepted

## Context

The scheduler main loop, and the periodic maintenance callbacks it drives (for
example `call_regular_interval` cleanup tasks), share one database session and
run on the critical path. Their latency directly determines cluster scheduling
throughput. A single unbounded bulk write against a user-driven table is a
cluster-wide hazard for two reasons:

- **Lock duration.** A `DELETE`/`UPDATE` with no `LIMIT` takes row (or, on some
  engines, range/gap/page) locks on every matching row and holds them for the
  entire transaction. On a large table that can be hundreds of thousands of
  rows, locked for seconds — blocking every concurrent writer (other schedulers,
  the API server, workers via the Execution API) touching those rows.
- **Loop stall.** While that statement runs, the scheduler's single session is
  busy; the whole loop stalls and no scheduling happens until it commits.

The volume of these tables is user-driven and unbounded — a deployment with many
Dags and a long retention window can accumulate enormous task-instance,
Dag-run, log, and history rows. Retention/garbage-collection work in particular
is tempting to write as one sweeping statement, which is exactly the anti-pattern.

Airflow's established pattern (canonically in `airflow/utils/db_cleanup.py`) is
to batch: delete/update at most `LIMIT` rows per statement, commit after each
batch so locks are released and other writers make progress, and loop until no
rows remain. The filter columns the batch selects on must be indexed so each
batch is a cheap indexed range rather than a full scan. A related coding
standard reinforces this: in `airflow-core`, a function that takes a `session`
parameter must not call `session.commit()` itself — the commit cadence belongs
to the batching driver, not to a helper buried inside it.

## Decision

Any `DELETE` or `UPDATE` the scheduler loop (or a synchronous interval callback
it runs) issues against a user-driven table must be **batched**:

1. Bound each statement with a `LIMIT` (batch size).
2. **Commit between batches** so locks are released and concurrent writers
   proceed; loop until the working set is exhausted.
3. Ensure the columns used to select/filter the batch are **indexed**.
4. Do not issue a single unbounded bulk write in the loop.
5. A function that receives a `session` parameter must not call
   `session.commit()`; the batching driver owns commit cadence.

Follow the batching pattern in `airflow-core/src/airflow/utils/db_cleanup.py`.

## Consequences

- The scheduler loop stays responsive and concurrent writers keep progressing
  even while large cleanup/retention operations run.
- Cleanup work takes more statements/round-trips, but each is short-lived and
  index-backed; total wall time is traded for bounded lock hold time.
- New retention/GC features must design their batch size, commit cadence, and
  supporting index up front.

A change **violates** this decision when it:

- issues an unbounded `DELETE`/`UPDATE` (no `LIMIT`) against a user-driven table
  in the scheduler loop or an interval callback;
- batches but does not commit between batches (locks accumulate for the whole
  run), or selects the batch on an unindexed column (each batch full-scans);
- calls `session.commit()` inside a helper that takes a `session` parameter;
- runs a large synchronous bulk write inline on the scheduler critical path.

A reviewer should reject any loop-path bulk mutation that lacks a `LIMIT`, lacks
an inter-batch commit, or filters on an unindexed column.

## Evidence

- #66463 — "AIP-103: Adding periodic task state garbage collection and retention
  support": introduces periodic state GC/retention, exactly the unbounded-volume
  cleanup that must be batched with `LIMIT` + per-batch commit on indexed
  columns rather than one sweeping delete.
- #62343 — "Add async connection testing via workers for security isolation":
  keeps heavyweight work off the scheduler's synchronous session so the loop's
  write path stays short and non-blocking.

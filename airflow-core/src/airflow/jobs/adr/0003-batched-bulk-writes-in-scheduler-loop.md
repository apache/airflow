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

The scheduler main loop and the periodic maintenance callbacks it drives (e.g.
`call_regular_interval` cleanup) share one database session on the critical path;
their latency directly determines scheduling throughput. A single unbounded bulk
write against a user-driven table is a cluster-wide hazard on two counts: a
`DELETE`/`UPDATE` with no `LIMIT` locks every matching row (hundreds of thousands,
for seconds) for the whole transaction, blocking every concurrent writer; and
while it runs, the single session is busy, so the whole loop stalls until it
commits. These table volumes are user-driven and unbounded, and retention/GC work
is especially tempting to write as one sweeping statement.

The established pattern (canonically `airflow/utils/db_cleanup.py`) batches:
delete/update at most `LIMIT` rows per statement, commit after each batch to
release locks, loop until none remain, and select on indexed columns so each batch
is a cheap indexed range. A related standard reinforces this: in `airflow-core`, a
function taking a `session` parameter must not call `session.commit()` — commit
cadence belongs to the batching driver.

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
- Cleanup takes more short-lived, index-backed statements — total wall time traded
  for bounded lock hold time — and new retention/GC features must design batch
  size, commit cadence, and supporting index up front.

A change **violates** this decision when it:

- issues an unbounded `DELETE`/`UPDATE` — one whose row set is bounded neither by
  a `LIMIT` nor by an explicit, already-enumerated set of primary keys — against a
  user-driven table in the scheduler loop or an interval callback;
- batches but does not commit between batches (locks accumulate for the whole
  run), or selects the batch on an unindexed column (each batch full-scans);
- calls `session.commit()` inside a helper that takes a `session` parameter;
- runs a large synchronous bulk write inline on the scheduler critical path.

A reviewer should reject any loop-path bulk mutation whose row set is unbounded
(no `LIMIT` and no enumerated key set), that lacks an inter-batch commit, or that
filters on an unindexed column.

## Evidence

- #66463 — "AIP-103: periodic task state garbage collection and retention": exactly the unbounded-volume cleanup that must be batched with `LIMIT` + per-batch commit on indexed columns, not one sweeping delete.
- #62343 — "Add async connection testing via workers": keeps heavyweight work off the scheduler's synchronous session so the write path stays short.

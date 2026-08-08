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

# 4. A timetable serves every consumer of its output, not just the scheduler

Date: 2026-07-20

## Status

Accepted

## Context

`next_dagrun_info()` is the consumer a contributor writes tests for, but not the
only one. The `DagRunInfo` a timetable produces — and its fields — is also
consumed by manual triggering, backfill, `airflow dags clear`, the REST API, and
the UI, each reaching the timetable through a different path that a scheduling-loop
test does not exercise.

The evidence is a long tail of fixes with the same shape: a capability was added,
the scheduler path worked, and a *different* consumer was missed — manual trigger
of a cron-partitioned timetable, `partition_date` population, backfill's
partition-date range mode, wrong-day clears and sub-day windows widened to a whole
day (consumers doing their own interval arithmetic), REST partition-clear parity
with the CLI, and API filtering by timetable type. The same pattern hits the
output *fields*: partition fields on `DagRunInfo` broke older consumers, and an
`EventsTimetable`'s description and isoformat separator were defects in what a
*reader* sees. It matters more here because the timetable classes exist in both
the Task SDK and airflow-core, so a one-sided change schedules correctly yet
displays, clears, or backfills wrongly.

## Decision

A change to a timetable, or to the shape of what a timetable returns, must
account for every consumer of that output. Concretely:

- **Enumerate the consumers.** A change to `DagRunInfo` fields or to interval
  computation states its effect on: scheduled runs, manual triggering, backfill,
  `airflow dags clear`, the REST API, and the UI.
- **New fields on timetable output are backward compatible by default.** Consumers
  that predate the field must keep working; adding a field is a compatibility
  event, not an internal detail (`adr/0002`).
- **Consumers must not re-derive interval arithmetic.** Backfill, clear, and
  trigger paths take the interval the timetable computed rather than recomputing
  day or window boundaries themselves — that recomputation is where the wrong-day
  and widened-window defects came from.
- **CLI and REST API stay in parity.** A capability exposed through one is
  exposed through the other, or the gap is stated deliberately.
- **The SDK and core sides land together.** Where a timetable exists on both
  sides of the distribution split, a change to one without the other is
  incomplete.
- **Serialized output carries its descriptive fields.** Descriptions, summaries,
  and formatted timestamps a reader depends on must survive the round trip.

## Consequences

- Timetable changes are wider than they look; a diff confined to a timetable
  class is usually incomplete. Expect the trigger, backfill, and clear paths in
  the same change or a stated follow-up.
- The cost falls hardest on new subsystems: the partition work generated many
  consumer-side corrections because the capability landed before every consumer
  knew about it.
- CLI/REST parity means some changes wait for the second surface.
- A complete change needs context across several packages, raising the bar for a
  first contribution and a common reason first attempts are superseded.

A change **violates** this decision when it:

- adds or changes a `DagRunInfo` field without establishing that existing
  consumers keep working;
- changes interval computation while leaving backfill, clear, or manual-trigger
  paths deriving their own boundaries;
- adds a timetable capability reachable from the CLI but not the REST API, or the
  reverse, without saying why;
- lands a timetable change on the core side or the SDK side alone where the class
  exists on both;
- drops a descriptive or formatted field from the serialized output that a reader
  displays;
- is tested only through the scheduling path when it also changes what manual
  triggering, backfill, or clear produce.

A reviewer should ask: besides the scheduler, who reads this value — and which of
those paths has a test in this change?

## Evidence

- #68342 — new `DagRunInfo` partition fields broke existing consumers; a
  dedicated backward-compat fix.
- #62441, #68458 — the manual-trigger consumer (`CronPartitionedTimetable`, then
  `partition_date` population), missed and fixed in two passes.
- #67717, #68718 — wrong-day clear and sub-day backfill widening: consumers
  re-deriving interval arithmetic and getting it wrong.
- #67537, #68702 — partition-date range backfill and REST partition-clear,
  added to consumers after the timetable side landed; the second restores parity.
- #58852 — API filtering by timetable type, landed separately; earlier attempts
  #54341, #58850 did not merge.
- #51203, #48732 — `EventsTimetable` description and isoformat separator: defects
  in what a reader of the serialized output sees.
- #60204, #58669 — the core / task-sdk distribution split that makes one-sided
  changes incomplete.

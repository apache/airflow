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

`next_dagrun_info()` is the obvious consumer of a timetable, and it is the one a
contributor writes tests for. It is not the only one. The `DagRunInfo` a
timetable produces — and the fields it carries — is also consumed by manual
triggering, by backfill, by `airflow dags clear`, by the REST API, and by the UI,
each of which reaches the timetable through a different path and none of which is
exercised by a test of the scheduling loop.

The evidence for this is a long tail of fixes that all have the same shape: a
timetable capability was added, the scheduler path worked, and a *different*
consumer was missed. Manually triggering a Dag with a cron-partitioned timetable
failed outright and had to be fixed separately; `partition_date` then had to be
populated on the manual-trigger path as its own change. Backfill needed a
partition-date range mode added after the fact. `airflow dags clear` cleared the
wrong day for non-UTC partitioned timetables, and partitioned backfill widened a
sub-day window to a whole day — both consumers applying their own interval
arithmetic to timetable output. Partition clearing then had to be added to the
REST API explicitly to match what the CLI already did. Filtering Dags by timetable
type in the API arrived as a separate change from the timetables themselves, and
was attempted twice by different contributors before landing.

The same pattern governs the fields on the output object rather than the paths
that read it. Adding partition fields to `DagRunInfo` broke consumers that did not
know about them, requiring a dedicated backward-compatibility fix. An
`EventsTimetable`'s description did not survive serialization, and its timestamps
needed an explicit isoformat separator — both defects in what a *reader* of the
timetable's output sees rather than in the schedule it computes.

This matters more here than in most areas because the consumers are spread across
distributions. The timetable classes exist in both the Task SDK and airflow-core
with a deliberate split, so a change frequently has to land in two places to be
coherent, and a change that lands in one produces a Dag that schedules correctly
and displays, clears, or backfills wrongly.

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

- Timetable changes are wider than they first appear, and a diff confined to a
  timetable class is usually incomplete. Reviewers should expect to see the
  trigger, backfill, and clear paths in the same change or in a stated follow-up.
- The cost falls hardest on new subsystems: the partition work generated many
  small consumer-side corrections precisely because the capability landed before
  every consumer knew about it. Sequencing that work consumer-by-consumer would
  have been slower but produced fewer user-visible defects.
- Requiring parity between the CLI and the REST API means some changes wait for
  the second surface, which delays useful capability.
- Contributors need context spread across several packages to make a complete
  change here, which raises the bar for a first contribution to this area and is
  a common reason first attempts are superseded by another pull request.

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

- #68342 — "Fix backward compatibility for `DagRunInfo` partition fields": a
  dedicated fix for consumers broken by new fields on the timetable's output
  object.
- #62441 — "Fix failing to manually trigger a Dag with `CronPartitionedTimetable`"
  and #68458 — "Populate `partition_date` when manually triggering partitioned
  Dags": the manual-trigger consumer, missed and then fixed in two separate
  passes.
- #67717 — "Fix `airflow dags clear` clearing the wrong day for non-UTC
  partitioned timetables" and #68718 — "Fix partitioned backfill widening a
  sub-day window to the whole day": consumers applying their own interval
  arithmetic to timetable output and getting it wrong.
- #67537 — "Backfill partitioned Dags by partition-date range" and #68702 — "Add
  partition clear support to REST API matching the CLI": capability added to
  consumers after the timetable side had landed, the second explicitly to restore
  CLI/API parity.
- #58852 — "Add API support for filtering Dags by timetable type": the API
  consumer, landed separately from the timetables; earlier independent attempts
  (#54341, #58850) did not merge.
- #51203 — "Persist `EventsTimetable`'s description during serialization" and
  #48732 — "Set explicit separator for isoformat when serializing
  `EventsTimetable`": defects in what a reader of the serialized timetable sees.
- #60204 — "`PartitionMapper`, `IdentityMapper`, `PartitionedAssetTimetable`:
  core / task-sdk separation" and #58669 — "Implement timetables in SDK": the
  distribution split that makes one-sided changes incomplete.

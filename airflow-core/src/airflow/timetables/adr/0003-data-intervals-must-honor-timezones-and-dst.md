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

# 3. Data intervals and cron/delta math must honor timezones and DST

Date: 2026-07-19

## Status

Accepted

## Context

Timetables compute wall-clock schedules for humans but must hand the scheduler
timezone-aware UTC instants. The `DataInterval` and `DagRunInfo` contracts in
`base.py` are explicit that `start`, `end`, and `run_after` **MUST** be aware.
The gap between "the author wrote `30 21 * * 5` in `Europe/Zurich`" and "the
scheduler needs a UTC instant" is where this area's hardest bugs live.

Two failure modes recur:

- **DST transitions.** A schedule in a zone with daylight saving crosses a
  boundary twice a year. On a backward transition an hour of wall-clock time
  occurs twice (the "fold hour"); on a forward transition an hour is skipped.
  The cron engine in `_cron.py` deliberately works around this — it computes in
  the timetable's local timezone via `make_naive` → `croniter` →
  `make_aware` → `convert_to_utc`, and `_covers_every_hour` exists specifically
  to reason about the fold hour so a `@daily`-style schedule does not fire twice
  or vanish around the switch.
- **Interval alignment and inference.** `interval.py` aligns boundaries with
  `_align_to_prev` / `_align_to_next` / `_get_prev` / `_get_next`, and its own
  comments note that `_get_prev(_align_to_next(...))` is *not* a substitute for
  the dedicated helpers. `infer_manual_data_interval()` must place a manually
  triggered `run_after` in the *same* interval a scheduled run would occupy, and
  catchup must reproduce the identical sequence of intervals. When any of these
  is computed on a naive datetime, in UTC instead of the local zone, or with a
  misplaced alignment call, the result is an interval shifted by a period, a
  sub-day window widened to a whole day, or a manual run that clears / covers the
  wrong day.

These bugs are hard to test: they manifest only for a particular timezone, on a
particular schedule shape, on the two days a year the offset changes — so they
escape a happy-path test and reach production.

## Decision

All interval and schedule math must be **timezone- and DST-correct**, and
manual / catchup inference must stay consistent with scheduled runs.

- Every datetime a timetable produces is timezone-aware. Cron and delta math is
  performed in the timetable's own timezone and converted to UTC through the
  established round-trip (`make_naive` / `make_aware` / `convert_to_utc`), never
  on a naive value or by assuming UTC.
- DST transitions are handled explicitly. Changes to `_get_next` / `_get_prev`
  or to the fold-hour handling (`_covers_every_hour`) must reason about both the
  folded (backward) and skipped (forward) hour, and about non-UTC schedules —
  not only the UTC happy path.
- Interval boundaries are preserved. Adding or moving an
  `_align_to_prev` / `_align_to_next` / `_get_prev` / `_get_next` call must not
  shift intervals by a period or widen a narrow window; use the dedicated helper
  for its stated purpose rather than composing a lookalike.
- `infer_manual_data_interval()` and `next_dagrun_info()` must agree: a manual
  run's inferred interval matches the scheduled interval for the same position,
  and catchup replays the identical interval sequence.
- Changes must be covered by tests that parametrize timezone, DST-boundary, and
  catchup / manual variants (using `time_machine` for the clock) and **fail
  without the change**.

## Consequences

- Schedules fire at the wall-clock time the author intended, in their timezone,
  including across DST transitions, and the interval a run covers is stable.
- Manual triggers, backfills, catchup, and `airflow dags clear` all operate on
  the same interval the scheduler would, so they act on the intended day.
- Contributors changing the cron/delta engines must think in the local zone and
  test the boundary cases, not just UTC.

A change **violates** this decision when it:

- computes cron/delta boundaries on a naive datetime, or in UTC instead of the
  timetable's own timezone, or drops the `make_naive`/`make_aware`/
  `convert_to_utc` round-trip;
- alters `_get_next` / `_get_prev` / fold-hour handling without accounting for
  the folded and skipped DST hour (making a schedule double-fire or vanish);
- adds or moves an alignment call so intervals shift by a period or a sub-day
  window widens to a whole day;
- makes `infer_manual_data_interval()` (or catchup) disagree with the scheduled
  interval for the same `run_after`;
- lands schedule math without timezone / DST / catchup-parametrized tests that
  fail when reverted.

## Evidence

- #69143 — "Fix drifting data intervals for monthly/yearly schedules with
  catchup disabled": interval boundaries drifted for monthly/yearly schedules,
  the exact alignment-versus-inference inconsistency this decision guards.
- #67717 — "Fix `airflow dags clear` clearing the wrong day for non-UTC
  partitioned timetables": a non-UTC timezone caused the wrong day to be
  selected, a direct timezone-correctness failure in interval math.
- #62441 — "fix failing to manually trigger a Dag with CronPartitionedTimetable":
  manual-trigger interval inference did not line up with the timetable's cron
  schedule.
- #54644 — "Fix cron expression display for Day-of-Month and Day-of-Week
  conflicts": correctness of cron-expression interpretation surfaced in the
  schedule the timetable presents.

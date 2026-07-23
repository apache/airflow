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
timezone-aware UTC instants — `DataInterval` and `DagRunInfo` in `base.py`
require `start`, `end`, and `run_after` to be aware. The gap between "the author
wrote `30 21 * * 5` in `Europe/Zurich`" and "the scheduler needs a UTC instant"
is where this area's hardest bugs live. Two failure modes recur:

- **DST transitions.** A daylight-saving zone crosses a boundary twice a year:
  backward, an hour occurs twice (the "fold hour"); forward, an hour is skipped.
  The cron engine in `_cron.py` computes in local time via `make_naive` →
  `croniter` → `make_aware` → `convert_to_utc`, and `_covers_every_hour` reasons
  about the fold hour so a `@daily`-style schedule does not double-fire or vanish.
- **Interval alignment and inference.** `interval.py` aligns boundaries with
  `_align_to_prev` / `_align_to_next` / `_get_prev` / `_get_next` (its comments
  note `_get_prev(_align_to_next(...))` is *not* a substitute for the dedicated
  helpers). `infer_manual_data_interval()` must place a manual `run_after` in the
  *same* interval a scheduled run would occupy, and catchup must replay the
  identical sequence. Computed on a naive datetime, in UTC, or with a misplaced
  alignment call, the result is an interval shifted by a period, a sub-day window
  widened to a whole day, or a manual run covering the wrong day.

These bugs manifest only for a particular timezone and schedule shape on the two
days a year the offset changes, so they escape happy-path tests.

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

- Schedules fire at the author's intended wall-clock time, across DST, with a
  stable covered interval.
- Manual triggers, backfills, catchup, and `airflow dags clear` all act on the
  same interval the scheduler would, on the intended day.
- Changing the cron/delta engines means thinking in the local zone and testing
  the boundary cases, not just UTC.

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

- #69143 — interval boundaries drifted for monthly/yearly schedules with catchup
  disabled: the alignment-versus-inference inconsistency this guards.
- #67717 — non-UTC partitioned timetable made `airflow dags clear` pick the wrong
  day: a direct timezone-correctness failure in interval math.
- #62441 — manual-trigger inference did not line up with `CronPartitionedTimetable`.
- #54644 — cron-expression display for Day-of-Month / Day-of-Week conflicts.

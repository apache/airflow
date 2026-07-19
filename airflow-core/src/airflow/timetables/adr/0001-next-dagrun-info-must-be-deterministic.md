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

# 1. next_dagrun_info must be deterministic and side-effect-free

Date: 2026-07-19

## Status

Accepted

## Context

The scheduler decides when to create the next Dag run by calling a timetable's
`next_dagrun_info()` (via `next_dagrun_info_v2` /
`next_run_info_from_dag_model`). It does this from the *serialized* Dag — it
reconstructs the timetable from stored data and never re-imports the author's
module (see the scheduler's own `adr/0001`, "the scheduler never runs user
code"). The scheduler holds a direct, privileged database session and runs a
latency-sensitive, largely single-threaded control loop, so a timetable it
evaluates must not carry the scheduler anywhere near author-controlled code,
blocking I/O, or process-local state.

The contract in `base.py` reflects this: `next_dagrun_info()` receives only
`last_automated_data_interval` (the previous scheduled interval, `None` only on
the Dag's first schedule) and a `TimeRestriction` (`earliest` / `latest` /
`catchup`). Its answer must be a pure function of those inputs and the
timetable's own serialized fields. If the computation instead read the live
clock, queried the database, consulted an author callable, or drew a random
number, two evaluations of the same Dag at the same logical position could
disagree — producing a missed run, a duplicate run, or a run over the wrong data
interval. Because the scheduler may re-evaluate the same position (HA replicas,
retries, a restart mid-loop), any hidden dependency on ambient state is a
scheduling-correctness bug, not a style nit.

This decision is closely related to the serialized-output determinism rule in
`serialization/adr/0002`, but sits one layer up: it constrains the *behaviour*
of the timetable the scheduler runs, not only the *bytes* it is stored as.

## Decision

A timetable's scheduling computation must be **deterministic and
side-effect-free**.

- `next_dagrun_info()` (and `infer_manual_data_interval()` and the
  `*_v2` / `*_from_dag_model` wrappers) must depend only on their arguments and
  the timetable's own serialized fields. No `datetime.now()` / wall-clock read,
  no database / network / filesystem access, no RNG, no mutation of shared
  state, and no branching on a live author-supplied callable.
- Given identical inputs, the method must return an identical `DagRunInfo`
  (or `None`) every time. Re-evaluating the same logical position must not
  advance, skip, or duplicate a run.
- "Now" enters scheduling only through the explicit `restriction` the scheduler
  passes (e.g. `latest` / `catchup`), never by the timetable reading the clock
  itself.
- The determinism must be covered by a test that drives the method directly
  (with `time_machine` for any time-dependent branch) and **fails without the
  change** — reproducing the mis-scheduling when the fix is reverted.

## Consequences

- The scheduler stays trustworthy and fast: evaluating a timetable cannot run
  author code, block the loop, or reach the timetable's answer through ambient
  state.
- New scheduling behaviour must be expressed as declarative, serialized data the
  timetable computes over — not as a callable the scheduler invokes at schedule
  time.
- Re-evaluation is safe under HA, retries, and restarts, because the answer is a
  pure function of position.

A change **violates** this decision when it:

- makes `next_dagrun_info()` / `infer_manual_data_interval()` read
  `datetime.now()` (or any live clock) instead of using the passed
  `restriction`;
- has the scheduling computation query the database, hit the network/filesystem,
  or call an author-supplied callable to decide the next interval;
- introduces nondeterminism (RNG, dict/set iteration order, unstable
  serialized fields) so the same input can yield a different `DagRunInfo`;
- lands time-dependent scheduling logic without a `time_machine`-based test that
  fails when the change is reverted.

## Evidence

- #66132 — "Don't re-emit logical_date when previous data_interval is
  zero-length": fixes `next_dagrun_info()` deriving the wrong next position from
  the previous interval, which duplicated a run's logical date instead of
  advancing deterministically.
- #45175 — "Fix ContinuousTimetable false triggering when last run ends in
  future": the timetable produced a spurious next run because its decision did
  not correctly account for the previous interval versus the restriction,
  triggering when it should not have.

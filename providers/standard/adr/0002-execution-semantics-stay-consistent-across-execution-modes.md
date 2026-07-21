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

# 2. Execution semantics stay consistent across execution modes

Date: 2026-07-20

## Status

Accepted

## Context

An operator or sensor here has several execution paths, and which one runs is
frequently **not** the Dag author's decision. `ExternalTaskSensor`, `TimeSensor`,
`TimeDeltaSensor`, `WaitSensor`, `DateTimeSensor` and `TriggerDagRunOperator` take
a `deferrable` parameter defaulting to `conf.getboolean("operators",
"default_deferrable", fallback=False)` — a **deployment-level config**, so the same
Dag runs the blocking path in one environment and the deferred path in another. On
top of that, `BaseSensorOperator` supplies `mode="reschedule"`. So a sensor can run
as: a blocking poke loop; `mode="reschedule"` (poked once per attempt, worker slot
released, resumed as a fresh process with no in-memory state — why
`task_reschedule_count` is in the virtualenv serializable-context allowlist); or
deferred (`self.defer(...)`, a trigger in `triggers/` waits, `execute_complete`
resumes).

Those are three code paths implementing what the author wrote as *one* behaviour,
and when they drift nothing crashes: `check_existence` honoured in `poke()` but
ignored when deferred, `timeout` bounding the loop but not the trigger, a soft-fail
sensor skipping inline but failing on resume. CI is green on whichever path the
test exercised, and the divergence surfaces as a production incident where
`default_deferrable = True`. This provider's history is largely that drift found
one parameter at a time (#64394, #62556, #53669, #58497, #53455). Consolidating the
`*Async` classes into `deferrable=True` (#50864, #51133) made this ADR necessary:
before, deferred behaviour lived in a *separate class* the author chose; after, one
class carries every mode and parity is an internal invariant.

## Decision

An operator or sensor must behave equivalently in every execution mode it
supports. Concretely:

- **A change to one path is a change to all of them.** Touching `poke()` means
  checking `execute()` (including its `deferrable` branch), `execute_complete()`
  and the corresponding trigger in `triggers/`; touching a trigger means
  checking the inline path.
- **Every user-facing parameter is honoured in every mode**, or the operator
  refuses the combination explicitly with an author-facing error. Silently
  ignoring a parameter on one path is a defect, not a limitation.
- **Terminal semantics are identical across modes** — success, failure,
  `soft_fail` / skip, timeout, and the branch decision must produce the same
  downstream task states whether the task ran inline, was rescheduled, or
  resumed from a trigger.
- **`mode="reschedule"` is a first-class path.** State needed across pokes must
  not live on `self`; anything a resumed attempt depends on comes from the
  context or the metadata the Execution API supplies.
- **Adding a mode means implementing all of it** — a new `deferrable` branch
  ships with its trigger, its `execute_complete`, its parameter handling, and
  tests for each; it is not a follow-up PR.
- **Tests cover each supported mode.** A test of the blocking path is not
  coverage of a change to deferred behaviour, and vice versa.

## Consequences

- Changes to sensors and deferrable operators are larger than they look: a one-line
  parameter addition carries a trigger change and per-mode tests.
- Deployments can flip `default_deferrable` without auditing their Dags — the point
  of that config.
- The `deferrable=True` consolidation stays viable only while the parameter is
  genuinely equivalent across modes.
- Reviewers can reject a sensor PR for incompleteness ("this path is not covered")
  without arguing the merits of the change.

A change **violates** this decision when it:

- modifies `poke()`, `execute()`, `execute_complete()`, or a trigger of an
  operator while leaving its other paths unchanged, without stating in the PR
  body why the other paths are unaffected;
- adds or changes a user-facing parameter that only takes effect in one mode,
  without either implementing it in the others or rejecting the combination with
  a clear error;
- produces different terminal states (skip vs fail, timeout handling, branch
  choice) depending on whether the task ran inline, rescheduled, or deferred;
- stores state on `self` that a `mode="reschedule"` attempt would need after the
  process exits;
- adds a `deferrable` branch without the matching trigger, resume method, and
  per-mode tests;
- tests only the mode the contributor happened to run locally.

## Evidence

- #64394 — `ExternalTaskSensor` `check_existence` honoured in `poke()` and dropped
  when deferred: the exact divergence this prevents.
- #62556 — same operator, `timeout` not applied in deferrable mode, same shape.
- #53669 — `TimeSensor` resume path broken while inline kept working; nothing failed
  visibly until a deployment deferred.
- #58497 — `TriggerDagRunOperator` deferral non-functional on Airflow 3: a whole
  mode dead on one core version.
- #53455 — terminal skip semantics differing by path under Airflow 3 branching.
- #47652 — `TimeDeltaSensorAsync` not handling a null logical date the inline path
  tolerated.
- #50864, #51133 — the `*Async` consolidation onto `deferrable=True` that turned
  mode parity into an internal invariant.
- #55037 — a start-from-trigger change reverted: this area is not safe to change on
  reasoning alone.
- #66584 — a trigger-side optimisation admissible only because inline-path
  equivalence held.

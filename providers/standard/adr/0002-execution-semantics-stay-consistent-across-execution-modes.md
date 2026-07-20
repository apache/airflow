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

An operator or sensor in this provider does not have one execution path. It has
several, and which one runs is frequently **not** the Dag author's decision.

`ExternalTaskSensor`, `TimeSensor`, `TimeDeltaSensor`, `WaitSensor`,
`DateTimeSensor` and `TriggerDagRunOperator` all take a `deferrable` parameter
whose default is `conf.getboolean("operators", "default_deferrable",
fallback=False)`. That is a **deployment-level config**. The same Dag file,
unchanged, runs the blocking path in one environment and the deferred path in
another. On top of that, `BaseSensorOperator` supplies `mode="reschedule"`,
under which the sensor's process exits between pokes and the task is re-queued —
a third path, selected per-task by the author.

Concretely, a sensor in this package can execute as:

- a blocking poke loop — `execute()` calls `poke(context)` repeatedly in the
  worker;
- `mode="reschedule"` — `poke(context)` is called once per attempt, the worker
  slot is released, and the task resumes as a fresh process with no in-memory
  state carried over (this is why `task_reschedule_count` is in the virtualenv
  operators' serializable-context allowlist at all);
- deferred — `execute()` calls `self.defer(trigger=..., method_name=...)`, a
  trigger in `triggers/` does the waiting on the triggerer, and
  `execute_complete(context, event)` resumes on a worker.

Those are three different code paths implementing what the author wrote as *one*
behaviour. The failure mode when they drift is the worst kind: nothing crashes.
`check_existence` is honoured in `poke()` and silently ignored in the deferred
path. `timeout` bounds the blocking loop but not the trigger. A sensor that
should soft-fail skips inline but fails when resumed from a trigger. The Dag
looks identical, CI is green on whichever path the test exercised, and the
divergence shows up as a production incident in the deployment that happens to
have `default_deferrable = True`.

The project's history with this provider is largely a history of that drift
being found one parameter at a time: `check_existence` ignored in deferrable
mode (#64394), `timeout` not applied in deferrable mode (#62556),
`execute_complete` missing on `TimeSensor` when `deferrable=True` (#53669),
`TriggerDagRunOperator` deferral not working at all on Airflow 3 (#58497),
sensors skipping differently under Airflow 3 branching operators (#53455).

The consolidation of the `*Async` sensor classes into a `deferrable=True`
parameter (#50864, #51133) made this ADR necessary rather than optional: before
it, the deferred behaviour lived in a *separate class* the author explicitly
chose. After it, one class carries every mode, and parity between them is an
internal invariant of that class rather than a user-visible choice.

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

- Changes to sensors and deferrable operators are larger than they look: a
  one-line parameter addition carries a trigger change and per-mode tests.
- Deployments can flip `default_deferrable` without auditing their Dags, which
  is the entire point of that config existing.
- The `deferrable=True` consolidation stays viable — folding `*Async` classes
  into a parameter is only safe while the parameter is genuinely equivalent.
- Reviewers can reject a sensor PR for incompleteness without arguing about the
  merits of the change itself: "this path is not covered" is sufficient.

A change **violates** this decision when it:

- modifies `poke()`, `execute()`, `execute_complete()`, or a trigger without
  checking the other paths of the same operator;
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

- #64394 — "`ExternalTaskSensor` `check_existence` ignored in deferrable mode
  (Airflow < 3.0)": a documented parameter honoured on the poke path and dropped
  on the deferred one — the exact divergence this decision exists to prevent.
- #62556 — "Fix `ExternalTaskSensor` to use `timeout` parameter in deferrable
  mode": same operator, second parameter, same failure shape.
- #53669 — "Restore `execute_complete` functionality `TimeSensor` when
  `deferrable=True`": the resume path was broken while the inline path kept
  working, so nothing failed visibly until a deployment deferred.
- #58497 — "`TriggerDagRunOperator` deferral mode not working for Airflow 3": a
  whole mode non-functional on one core version.
- #53455 — "Fix sensor skipping in Airflow 3.x branching operators": terminal
  skip semantics differing by path.
- #47652 — "Handle null logical date in `TimeDeltaSensorAsync`": the deferred
  variant not handling an input the inline path tolerated.
- #50864 and #51133 — the `TimeSensorAsync` / `TimeDeltaSensorAsync`
  consolidation onto `deferrable=True`, which is what turned mode parity from a
  user choice into an internal invariant of a single class.
- #55037 — "Revert 'Fix rendering of template fields with start from trigger'":
  a change in the start-from-trigger seam that had to be reverted, evidence that
  this area is not safe to change on reasoning alone.
- #66584 — "Share one poll loop across sibling event triggers": a change on the
  trigger side of the contract, where the inline-path equivalence is what makes
  the optimisation admissible.

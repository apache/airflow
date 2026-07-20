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

# 4. Standard operators expose core semantics; they do not invent them

Date: 2026-07-20

## Status

Accepted

## Context

This provider holds the operators every Airflow user touches — `PythonOperator`,
`BashOperator`, `TriggerDagRunOperator`, `ExternalTaskSensor`, the branch and
short-circuit operators. That makes it the most reachable place in the codebase
to attach a new idea, and it is where proposals for new *core* semantics
repeatedly arrive dressed as operator features.

The recurring shapes are recognisable: a new Dag-run-level date because
`data_interval` is felt to be the wrong name for "the date we are processing";
Dag-level automatic retries because task-level retries do not cover the case; a
new Execution API endpoint added so an operator can ask a question the existing
routes almost answer; a configuration knob that changes which Dag bundle version a
run resolves against. Each is proposed as a parameter on an operator, and each
would in fact change what a Dag run *is*.

Landing any of them here would be a mistake of the most expensive kind. Operators
in this provider ship on the provider cadence against a range of core versions
(`apache-airflow>=2.11.0`), so a semantic invented here becomes an ecosystem
commitment before core has decided whether it wants it — and it becomes one
without the AIP discussion, the UI, the API surface, or the timetable integration
that a real core concept requires. Airflow's scheduling vocabulary is small on
purpose: `logical_date`, `data_interval_start` / `data_interval_end`,
`run_after`, task retries, timetables. A second, operator-local vocabulary that
means almost the same thing is worse than no vocabulary at all, because Dag
authors then have to know which one their tooling reads.

The counterpart is that operators here *should* expose core semantics fully. When
core grows a concept — `run_after`, Dag-run notes, Dag versioning — surfacing it
through the relevant operator is exactly this provider's job, and those additions
land routinely.

## Decision

**An operator in this provider surfaces a semantic that core already owns. It
does not define a new one.**

- **Reuse the existing concept before proposing a parallel one.** "What date is
  being processed" is `data_interval_start` / `data_interval_end` — a half-open
  interval produced by the timetable, collapsible to a point by setting both ends.
  A new date field on a Dag run is a core/timetable decision, not an operator
  parameter.
- **Scheduling behaviour is core's.** Retry policy, run creation, run-state
  transitions and bundle-version resolution are decided by the scheduler and its
  configuration. An operator may trigger, wait on, or read those things; it does
  not implement a second policy for them.
- **A new Execution API route is a core change with a core review.** Do not add
  one as a supporting commit in an operator PR. Ask first whether an existing
  route's response — including its 404 — already carries the information, and
  whether the answer even survives Dag versioning.
- **Anything that changes what a Dag run means goes through the dev list or an
  AIP first**, before code. That is not a formality here: the discussion is the
  cheap part, and the operator surface is the expensive part.
- **Split the layers when a change needs both** — *unless the architecture has
  already split them for you*. A PR that adds a *new* core capability and an
  operator that uses it is reviewed as two PRs, by two sets of reviewers. But a
  handful of operators here have no execution path of their own: Airflow 3 moved
  `TriggerDagRunOperator`'s trigger-and-wait into the task runner, so the operator
  raises `DagRunTriggerException` and
  `task-sdk/src/airflow/sdk/execution_time/task_runner.py` does the work, and the
  branch operators are thin subclasses of
  `task-sdk/src/airflow/sdk/bases/branch.py`. For those, crash recovery, retry
  reattachment or branch resolution *cannot* be changed on one side alone. The
  paired diff is the only correct shape, and asking for a split there asks for a
  PR that does not exist.

## Consequences

- Airflow keeps one scheduling vocabulary, and Dag authors keep one place to
  learn it. Tooling, the UI, and the API agree on what a run's dates mean.
- Genuinely new concepts still arrive — through the AIP and core-review path that
  can carry them, with the API, UI and serialization work included.
- The cost is a slower and less satisfying answer for the contributor: a
  well-implemented, tested, self-consistent PR is closed on grounds that are
  about layering rather than about the code. Some of these proposals address real
  gaps and are closed anyway. Making the layering explicit here is meant to move
  that conversation before the code, not to pretend the gap is imaginary.
- Reviewers must be able to recognise the pattern early — the tell is a parameter
  whose meaning is a property of the *run*, not of the *task*.

A change **violates** this decision when it:

- adds an operator parameter, or a Dag/Dag-run attribute, that introduces a new
  date, interval, or scheduling concept alongside `logical_date`, the data
  interval, or `run_after`;
- implements retry, backoff, or run-recreation policy inside an operator rather
  than relying on task retries and the scheduler;
- adds or modifies an Execution API route (or a core API route) as part of a
  standard-provider change;
- changes how a run resolves its Dag bundle version, its paused state, or any
  other property the scheduler owns;
- ships a core semantic and its operator surface in a single PR, so that neither
  set of reviewers sees the whole change — *except where core already owns that
  operator's execution path* (e.g. `TriggerDagRunOperator` via the task runner),
  in which case the paired change is the only correct shape.

## Evidence

- #67329 — "Add `target_date`, a user-defined processing date for Dag runs":
  closed as won't-fix. The review was explicit that this is conceptually
  `data_interval`, that a point-in-time is expressed by setting start and end
  around it, and that the interval is produced by the timetable.
- #61336 — "Add Dag-level automatic retry feature": closed; retry policy is not
  an operator-level addition.
- #65856 — "Allow future `logical_date` for manual and operator-triggered Dag
  runs": closed; a change to what a run's `logical_date` may be is a core
  semantic, not a `TriggerDagRunOperator` option.
- #67832 — "implement `DagTaskGroupsExistence` and `DagTasksExistence`
  endpoints": closed. The review questioned both the layer (a new route where an
  existing response's 404 already encodes the answer) and the premise (under Dag
  versioning, task existence is not well-defined until a run exists).
- #61063 — "Add configurable bundle version defaults": closed. The review asked
  for the core capability and the `TriggerDagRunOperator` change to be separated,
  and found the assumed Execution API endpoint did not exist.
- #62861 — "Fix `POST /pools` to return 409 when pool already exists": closed;
  API error semantics belong in core's global handler, not in a provider PR.
- #68936, #68955, #69135, #69839 (`TriggerDagRunOperator` — durable waits,
  reattachment, execution ownership) and #68797 (`BranchOperator` relative paths)
  — five open PRs that each change one operator and its task-runner or base-class
  counterpart in the same diff. No compliant split exists for them: the execution
  path lives in `task-sdk/`, by design. They are why the "split the layers" rule
  carries an exception rather than being absolute.
- #61658 and #61657 — two attempts to change the released default of
  `show_return_value_in_logs`: both closed. Even a defensible default is core-
  facing behaviour once it has shipped (ADR 1).

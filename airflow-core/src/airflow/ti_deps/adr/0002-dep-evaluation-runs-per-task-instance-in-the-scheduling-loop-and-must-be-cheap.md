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

# 2. Dependency evaluation runs per task instance in the scheduling loop and must be cheap

Date: 2026-07-19

## Status

Accepted

## Context

Each candidate task instance is gated by a *set* of dependencies — `RUNNING_DEPS`,
`SCHEDULER_QUEUED_DEPS`, or `REQUEUEABLE_DEPS` in `dependencies_deps.py` — and every
dep is evaluated for that instance inside the scheduler's latency-sensitive,
largely single-threaded main loop. A dep's cost is paid for *every* task instance
considered, on *every* cycle, across the deployment.

That multiplier makes a locally innocent change expensive: a dep issuing one query
per upstream task, per map index, or re-fetching the run's finished task instances
becomes an N+1 storm the moment a Dag has wide fan-in or a large mapped group — the
symptom is a scheduler falling behind, not an error. The existing deps avoid this:
the trigger-rule dep counts upstream states with a single aggregate
(`select(TaskInstance.task_id, func.count(...)).group_by(...)`) rather than looping;
`DepContext.ensure_finished_tis()` fetches finished task instances *once* and shares
the list; and several deps iterate lazily with early return. This mirrors the
scheduler-loop cost discipline the repo `CLAUDE.md` requires.

## Decision

A dependency must be cheap to evaluate and must not add per-row database work to
the scheduling loop. Specifically:

- Counting, filtering, and state aggregation go into a single batched SQL
  statement — no query per upstream task, per map index, or per finished
  task instance.
- Reuse the state the context already carries (`DepContext.finished_tis` via
  `ensure_finished_tis()`) instead of re-fetching it inside a dep.
- Prefer lazy iteration and early return so a cheap failing case short-circuits
  before any expensive computation runs.
- Treat added per-cycle cost as a first-class concern: a change that makes a dep
  materially more expensive needs justification and, ideally, a benchmark.

## Consequences

- The scheduler stays responsive as Dags scale in fan-in and mapped-task width;
  dependency checks do not become the bottleneck that starves scheduling.
- New dependency logic needing upstream/aggregate information expresses it as
  set-based SQL, not a Python loop over related task instances.
- Sharing `finished_tis` and other context state is part of the contract, not a
  casual optimisation — removing it reintroduces the per-dep refetch cost.

A change **violates** this decision when it:

- issues a query per upstream task, per map index, or per finished task instance
  instead of one batched/aggregate statement;
- re-fetches finished task instances (or other run state) inside a dep instead of
  reusing `DepContext.finished_tis`;
- adds unbounded or superlinear per-task-instance work to a dep on the scheduling
  hot path;
- removes an existing early-return / lazy-iteration short-circuit and makes the
  common "not ready" case pay for the expensive path.

## Evidence

- #64558 — "Fix teardown scope evaluation: lazy iteration, early return": short-circuits
  cheaply instead of always doing the full work.
- #67684 — "Fix per-index evaluation of `ONE_FAILED` in mapped task groups": aggregate
  state computed set-wise, not per index.
- #61769 — "Fix `max_active_tis_per_dag` for deferred task instances": a concurrency dep
  kept correct and bounded per instance.
- #61227 — "Multi-team. Verify a task uses a pool it has access to": a scheduling-time
  access check paid per task instance.

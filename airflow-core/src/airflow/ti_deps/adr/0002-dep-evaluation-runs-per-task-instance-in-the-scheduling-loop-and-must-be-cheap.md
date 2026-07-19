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
`SCHEDULER_QUEUED_DEPS`, or `REQUEUEABLE_DEPS` in `dependencies_deps.py` — and
every dep in the set is evaluated for that task instance inside the scheduler's
main loop. The loop is latency-sensitive and largely single-threaded per
scheduler, so the cost of a dep is not paid once: it is paid for *every* task
instance the scheduler considers, on *every* cycle, across the whole deployment.

That multiplier is what makes a locally innocent change expensive. A dep that
issues one query per upstream task, or per map index, or re-fetches the run's
finished task instances, turns into an N+1 storm the moment a Dag has wide fan-in
or a large mapped task group — and the symptom is not an error but a scheduler
that falls behind. The existing deps are written to avoid this: the trigger-rule
dep counts upstream states with a single aggregate,
`select(TaskInstance.task_id, func.count(TaskInstance.task_id)).group_by(...)`,
rather than looping; `DepContext.ensure_finished_tis()` fetches the run's
finished task instances *once* and hands the list to every dep that needs it; and
several deps iterate lazily and return early on the first failing condition so a
cheap "not ready yet" answer never pays for the expensive path.

This mirrors the scheduler-loop cost discipline the repo `CLAUDE.md` requires
(push work into batched SQL, don't add unbounded per-row work to the loop);
dependency evaluation is one of the hottest consumers of that budget.

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

- The scheduler stays responsive as Dags scale in fan-in and in mapped-task
  width; dependency checks do not become the bottleneck that starves scheduling.
- New dependency logic that needs upstream/aggregate information must express it
  as set-based SQL, not as a Python loop over related task instances.
- Sharing `finished_tis` and other context state across deps is part of the
  contract, not an optimisation to be undone casually — removing that sharing
  reintroduces the per-dep refetch cost.

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

- #64558 — "Fix teardown scope evaluation: lazy iteration, early return, better
  tests": made a dep short-circuit cheaply instead of always doing the full work
  — the cost discipline this decision codifies.
- #67684 — "Fix per-index evaluation of `ONE_FAILED` in mapped task groups":
  correctness of aggregate state evaluation across a mapped group, where the
  counts must be computed set-wise rather than per index.
- #61769 — "Fix `max_active_tis_per_dag` for deferred task instances": a
  concurrency dep whose per-task-instance evaluation had to stay correct and
  bounded under the scheduler's per-loop accounting.
- #61227 — "Multi-team. Verify a task uses a pool it has access to when
  scheduling": added a scheduling-time access check as a dep, work paid per task
  instance in the loop.

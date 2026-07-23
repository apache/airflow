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

# 3. A TIDep has a stable pass/fail contract

Date: 2026-07-19

## Status

Accepted

## Context

Every task-instance dependency is a `BaseTIDep` subclass with one narrow contract.
It implements `_get_dep_statuses()` to yield `TIDepStatus(dep_name, passed, reason)`
records; the public `get_dep_statuses()` wraps it and applies two short-circuits
first — if the dep is `IGNORABLE` and the context set `ignore_all_deps`, or
`IS_TASK_DEP` and the context set `ignore_task_deps`, it yields a passing status and
returns. A task instance is runnable in a `DepContext` only when *every* dep in the
set reports `passed=True`; the `reason` strings explain to operators *why* a task is
held.

This uniformity is the point: because every dep speaks the same
pass/fail-with-reason language and honours the same `DepContext` flags, deps compose
into interchangeable sets (`RUNNING_DEPS`, `SCHEDULER_QUEUED_DEPS`,
`REQUEUEABLE_DEPS`) callers evaluate without special-casing any dep. The `IGNORABLE`
/ `IS_TASK_DEP` markers are load-bearing — they decide uniformly when a dep is
honoured versus skipped under backfill, clear, and retry — so a dep that classifies
itself wrongly, or reports through a side channel, breaks callers that never
referenced it. History here is deps evolving *within* the contract, with dead
surface removed.

## Decision

New dependency logic integrates through the `BaseTIDep` contract; it does not
bypass the framework or change met/not-met semantics through a side channel.
Specifically:

- A dependency is a `BaseTIDep` subclass whose `_get_dep_statuses()` yields
  `TIDepStatus` records with an informative `reason`; passing/failing status is
  built via `_passing_status` / `_failing_status`, not fabricated elsewhere.
- A dep sets `IGNORABLE` and `IS_TASK_DEP` to describe itself truthfully, and
  relies on `get_dep_statuses()` to apply the `DepContext` ignore-flag
  short-circuits — it does not re-implement or sidestep them.
- "Runnable" remains "every dep in the set passes." New gating is expressed as a
  dep added to the appropriate set in `dependencies_deps.py`, not as an ad-hoc
  check that silently blocks or releases a task outside the dep framework.
- Changes to an existing dep preserve its passing condition for the cases they do
  not intend to change; a changed count, skip rule, or reason is a deliberate,
  reviewed semantics change.

## Consequences

- Callers keep treating deps as an interchangeable, composable set — no per-dep
  special-casing in the scheduler, API, or tests.
- New scheduling constraints have an obvious home: a new `BaseTIDep` in the right
  set, with tests asserting on `get_dep_statuses()` output.
- Reasons stay meaningful and user-facing, so a held task can always be explained
  from its dep statuses.

A change **violates** this decision when it:

- gates task readiness outside the dep framework — a check elsewhere that blocks
  or releases a task without yielding a `TIDepStatus`;
- yields status without a meaningful `reason`, or fabricates pass/fail outside
  `_get_dep_statuses()` / the `_passing_status` / `_failing_status` helpers;
- mis-sets or ignores `IGNORABLE` / `IS_TASK_DEP`, or re-implements the
  `DepContext` ignore-flag short-circuits instead of relying on
  `get_dep_statuses()`;
- silently changes an existing dep's met/not-met condition for cases the change
  did not intend to touch.

## Evidence

- #57725 — "Remove unused `TIDep.get_failure_reasons()`": trimmed dead surface to keep
  the interface minimal.
- #53959 — "Add `ALL_DONE_MIN_ONE_SUCCESS` trigger rule": new behaviour added *through*
  the existing status contract.
- #67684 — "Fix per-index evaluation of `ONE_FAILED` in mapped task groups": corrected
  semantics, same `TIDepStatus` contract.
- #54774 — "Fix trigger rule error messages showing enum names instead of values": the
  `reason` strings are user-facing contract.

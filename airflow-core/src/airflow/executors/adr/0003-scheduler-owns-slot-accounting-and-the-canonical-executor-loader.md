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

# 3. The scheduler owns slot/pool accounting; executors do not oversubscribe, and lookup goes through the single canonical loader

Date: 2026-07-18

## Status

Accepted

## Context

With multiple executors (and, in multi-team deployments, multiple executors per
team), the question "may this workload be dispatched right now?" has exactly one
correct owner: the scheduler. The scheduler is the only component that sees the
whole picture — pools, `max_active_tasks` limits, per-executor `parallelism`, and
the running/queued counts across every executor and team. If capacity decisions
are made anywhere else, two workloads can each independently conclude there is
room and the deployment oversubscribes: more work is dispatched than there are
slots, pools are violated, and the guarantees users rely on quietly break.

Two structural mistakes lead there:

1. A **new workload type dispatches without slot accounting** — it does not
   consult `slots_available` / open slots, or ignores pool and
   `max_active_tasks` limits — so it hands work to an executor that is already
   full.
2. **Executor lookup is duplicated.** The scheduler resolves the executor for a
   workload through a single canonical path,
   `SchedulerJobRunner._try_to_load_executor()` (which layers team resolution and
   caching over `ExecutorLoader.load_executor()`). A second, parallel loader — or
   ad-hoc `getattr`/dict lookups scattered per call site — drifts from the
   canonical team/name resolution and cache, producing inconsistent routing and
   defeating the accounting that assumes one resolution path.

Related: generic per-executor logic that is copy-pasted into each executor
subclass (rather than living once on `BaseExecutor`) is the same failure mode in
a different place — divergent copies that fall out of sync.

## Decision

- Any **new workload type performs slot/capacity accounting before dispatch**:
  it honours `slots_available` / open-slot computation, pools, and
  `max_active_tasks`, so it never dispatches beyond available capacity.
- **All executor lookup routes through the single canonical
  `_try_to_load_executor()`** on the scheduler (backed by
  `ExecutorLoader.load_executor()`). Do not add a second executor-loading helper
  and do not inline per-call-site resolution.
- **Generic executor behaviour lives on `BaseExecutor`**, inherited by
  subclasses — not duplicated into each executor.

## Consequences

- The scheduler remains the sole authority on capacity; no combination of
  executors/teams can collectively oversubscribe.
- There is exactly one place to reason about, and fix, executor resolution and
  team routing.
- Shared executor logic changes in one place and all executors get it.

**A violating change looks like:** adding a workload/dispatch path that submits
work without checking open slots / pools / `max_active_tasks` (letting total
dispatched work exceed capacity); introducing a second executor-loader helper (or
inline `ExecutorLoader`/`getattr` lookups) that bypasses `_try_to_load_executor()`
and its team/cache resolution; or copy-pasting the same accounting logic into
every executor subclass instead of placing it on `BaseExecutor`. Each is rejected.

## Evidence

- #62343 — Add async connection testing via workers for security isolation (new worker-dispatched workload that must respect slot accounting).
- #61153 — Executor synchronous callback workload (a new workload type routed through the canonical loader and scheduler accounting).

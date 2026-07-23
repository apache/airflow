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

"May this workload be dispatched right now?" has exactly one correct owner: the
scheduler. It is the only component that sees the whole picture — pools,
`max_active_tasks`, per-executor `parallelism`, and running/queued counts across
every executor and team. If capacity is decided anywhere else, two workloads each
conclude there is room and the deployment oversubscribes: pools are violated and
guarantees quietly break.

Two structural mistakes lead there: a new workload type that dispatches *without*
consulting `slots_available` / pool / `max_active_tasks` limits; and *duplicated
executor lookup* — a second loader or ad-hoc `getattr`/dict lookups that drift
from the canonical `SchedulerJobRunner._try_to_load_executor()` (team resolution
plus caching over `ExecutorLoader.load_executor()`). Copy-pasting generic
per-executor logic into each subclass instead of `BaseExecutor` is the same
divergence failure in a different place.

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

- The scheduler stays the sole authority on capacity; no combination of
  executors/teams can collectively oversubscribe.
- There is exactly one place to reason about and fix executor resolution and team
  routing, and shared executor logic changes in one place.

**A violating change looks like:** adding a workload/dispatch path that submits
work without checking open slots / pools / `max_active_tasks` (letting total
dispatched work exceed capacity); introducing a second executor-loader helper (or
inline `ExecutorLoader`/`getattr` lookups) that bypasses `_try_to_load_executor()`
and its team/cache resolution; or copy-pasting the same accounting logic into
every executor subclass instead of placing it on `BaseExecutor`. Each is rejected.

## Evidence

- #62343 — async connection testing via workers: a new worker-dispatched workload that must respect slot accounting.
- #61153 — executor synchronous callback workload: routed through the canonical loader and scheduler accounting.

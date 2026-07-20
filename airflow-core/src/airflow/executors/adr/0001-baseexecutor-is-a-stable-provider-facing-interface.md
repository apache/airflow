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

# 1. BaseExecutor is a stable provider-facing interface

Date: 2026-07-18

## Status

Accepted

## Context

`BaseExecutor` (`airflow-core/src/airflow/executors/base_executor.py`) is not an
internal-only base class. It is the contract that every out-of-tree and provider
executor inherits from: Celery, Kubernetes, ECS, AWS Batch, Lambda, and Edge all
subclass `BaseExecutor` and override its hooks (`execute_async`, `sync`,
`try_adopt_task_instances`, `get_task_log`, the `slots_available` /
`parallelism` accounting surface, and the public attributes such as
`running`, `queued_tasks`, `event_buffer`).

Crucially, these executors live in provider distributions that are versioned and
released **independently** of `airflow-core`. A deployment routinely runs the
latest provider (e.g. a newer `apache-airflow-providers-cncf-kubernetes`) on an
older core, or an older provider on a newer core. That means the public shape of
`BaseExecutor` — field names, method signatures, class-level constants, and the
import paths that resolve them — is a cross-version compatibility boundary, not a
detail we can freely reshape when it is convenient for core.

A change that "unifies" or "cleans up" a `BaseExecutor` field or method by
renaming or removing it looks local to core, but it silently breaks every
provider executor that still references the old name, in exactly the
mixed-version combination we support.

## Decision

Treat the public surface of `BaseExecutor` as a stable, provider-facing API:

- **Never rename or remove** a public attribute, method, or class constant of
  `BaseExecutor`, nor change the signature of a public method in a
  backwards-incompatible way (removing/reordering positional parameters, tightening
  types, dropping keyword names an override may pass through).
- Make changes **additive**. New behaviour is a new method / new optional keyword
  argument with a default that preserves the old call semantics.
- When a public name genuinely must move, keep the old name working via a
  `@property` shim (or a thin delegating method / alias) that forwards to the new
  one, and mark it deprecated instead of deleting it. Renamed import entrypoints
  are deprecated, not broken.
- Removal of a deprecated shim happens only on a documented major boundary, after
  the minimum supported provider version no longer references it — never in the
  same change that introduces the replacement.

Generic executor behaviour belongs on `BaseExecutor` so providers inherit it
rather than redeclaring their own copies of the same surface.

## Consequences

- Provider executors can be upgraded ahead of, or behind, core without
  `AttributeError` / `TypeError` at the executor boundary.
- Core carries some deprecated shims for a release or two; that cost is accepted
  in exchange for the version-mix guarantee.
- Refactors that want to tidy the interface must do so additively and leave the
  old names callable.

**A violating change looks like:** renaming or deleting a public `BaseExecutor`
field or method (e.g. renaming `queued_tasks`, dropping `try_adopt_task_instances`,
or changing `execute_async(...)`'s signature) to "unify" the interface, with no
`@property`/alias shim and no deprecation period — so that the latest provider
executor on an older core (or vice versa) raises at runtime. Such a change is
rejected.

## Evidence

- #62645 — Move ExecutorCallback execution into a supervised process (touches the
  executor callback surface that providers inherit).
- #63482 — "Unify executor workload queues with tier-based scheduling": the
  reject-shaped example, and the clearest one in this area. It renamed fields on
  `BaseExecutor`, which every provider executor inherits. A reviewer's entire
  response was *"Sorry, this is not possible. This breaks compatibility between
  providers and core"*, with the version-mix case spelled out — upgrade core, leave
  the providers, and the renamed fields break. **The PR was closed unmerged.** A
  second reviewer supported the direction and asked what it would take to make it
  back-compatible; the successor, #63491, keeps the old names working via
  `@property` — the shim this ADR prescribes — and is still open at the time of
  writing.

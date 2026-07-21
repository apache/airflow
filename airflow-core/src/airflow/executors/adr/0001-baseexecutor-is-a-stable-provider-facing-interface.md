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

`BaseExecutor` (`airflow-core/src/airflow/executors/base_executor.py`) is the
contract every out-of-tree and provider executor inherits from: Celery, Kubernetes,
ECS, AWS Batch, Lambda, and Edge subclass it and override its hooks (`execute_async`,
`sync`, `try_adopt_task_instances`, `get_task_log`, the `slots_available` /
`parallelism` accounting, and public attributes like `running`, `queued_tasks`,
`event_buffer`).

These executors live in provider distributions versioned and released
**independently** of `airflow-core`, and deployments routinely run a newer provider
on older core or the reverse. So the public shape of `BaseExecutor` — field names,
signatures, constants, import paths — is a cross-version compatibility boundary. A
change that "unifies" or "cleans up" by renaming or removing looks local to core but
silently breaks every provider executor still referencing the old name, in exactly
the mixed-version combination we support.

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

- #62645 — moves ExecutorCallback execution into a supervised process, touching the inherited callback surface.
- #63482 — the reject-shaped example: renamed `BaseExecutor` fields; closed unmerged with *"this breaks compatibility between providers and core"* (upgrade core, keep providers, renamed fields break). Successor #63491 keeps the old names via `@property` — the shim this ADR prescribes.

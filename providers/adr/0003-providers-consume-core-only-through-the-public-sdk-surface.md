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

# 3. Providers consume core only through the public Task SDK surface

Date: 2026-07-20

## Status

Accepted

## Context

Provider code runs on workers. An operator's `execute()`, a hook's client calls,
a sensor's `poke()` — all of it executes in the task-execution process, which by
Airflow's architecture reaches the metadata database *only* through the Execution
API, never directly. A provider that imports `airflow.models` and opens an ORM
session is not merely coupling itself to a core internal; it is punching through
the isolation boundary that keeps worker-side code away from the server's
database credentials.

The coupling problem is real on its own terms too. Because providers ship on
their own cadence against a range of core versions (ADR 2), any core symbol a
provider imports becomes part of that provider's compatibility surface. Public
Task SDK entry points — `airflow.sdk` base classes, `airflow.sdk` context
accessors, `airflow.providers.common.compat.sdk.conf` — are maintained with
cross-version stability in mind. Private modules, ORM models, and airflow-core
internals are not: they get renamed, moved, or restructured in a core release
that was never intended to be a provider-breaking one, and the break surfaces in
a deployment rather than in the core PR's CI.

The project has been closing these seams deliberately rather than by convention
alone. Providers were migrated en masse off `airflow.configuration.conf` and
`airflow.sdk.configuration.conf` onto `airflow.providers.common.compat.sdk.conf`,
and `check_conf_import_in_providers.py` now fails the build on the forbidden
imports — with a narrow carve-out for executor modules, which genuinely run
inside airflow-core rather than on a worker. Shared building blocks that
providers subclass, such as `SkipMixin` and `BranchMixIn`, were moved into the
Task SDK so that providers depend on the SDK surface rather than on core.

## Decision

Providers depend on the public Task SDK surface, and on nothing else in core.

- **Subclass and import from `airflow.sdk`.** Operators derive from the SDK
  `BaseOperator`, sensors from `BaseSensorOperator`, hooks from `BaseHook`;
  runtime context comes from the SDK accessors.
- **Never import airflow-core internals or the ORM from provider runtime code** —
  no `airflow.models`, no direct `Session` / `session.query(...)`, no
  private/underscored core modules. Worker-side code reaches Airflow state
  through the Execution API surface the SDK exposes.
- **Import `conf` from `airflow.providers.common.compat.sdk`**, never from
  `airflow.configuration` or `airflow.sdk.configuration`. Executor modules, which
  run inside airflow-core, may use `airflow.configuration` and are excluded by
  the hook for that reason.
- **A new core capability a provider needs must be exposed on the SDK surface
  first.** If the only way to reach it is a core internal, that is a signal the
  SDK is missing an entry point — raise it, rather than importing around it.
- **Provider imports must resolve without airflow-core installed** where the
  provider is expected to run on a Task-SDK-only worker.

## Consequences

- A provider keeps working across core upgrades, because everything it depends on
  is a surface maintained for exactly that guarantee.
- Core can refactor its internals without a coordinated fix across ~100
  independently released provider packages.
- The worker's database isolation holds: no provider becomes the path by which
  worker-side code acquires a metadata-DB session.
- When the SDK lacks something a provider needs, the cost lands as an SDK feature
  request rather than as a hidden coupling — slower in the moment, and the reason
  the boundary survives.

A change **violates** this decision when it:

- imports `airflow.models`, an ORM `Session`, or any private / underscored
  airflow-core module from provider runtime code;
- opens a direct metadata-database query or write from an operator, hook, sensor,
  or trigger instead of going through the SDK / Execution API surface;
- imports `conf` from `airflow.configuration` or `airflow.sdk.configuration` in
  non-executor provider code, bypassing `common.compat.sdk`;
- subclasses or reaches into a core class that is not part of the public SDK
  surface, when an SDK base class or entry point exists for the purpose;
- adds a top-level import that makes the provider fail to import on a
  Task-SDK-only worker.

## Evidence

- #70068 — "Fix `@task.llm_branch` import failure on Task SDK-only workers":
  precisely the failure this boundary prevents — provider code that could not be
  imported where airflow-core is absent.
- #64564 — "Add prek hook to enforce `conf` import from compat SDK in
  providers": the boundary made mechanically enforceable rather than advisory.
- #59979 — "Source amazon provider to use `airflow.sdk.configuration.conf`" and
  #59986 — "Migrate google provider to use `airflow.sdk.configuration.conf`": the
  large providers moved off direct core configuration access.
- #62749 — "Move `SkipMixin` and `BranchMixIn` to Task SDK": shared building
  blocks relocated onto the SDK surface so provider operators depend on the SDK
  rather than on core.
- #69208 — "Added `get_async_hook` in `common.compat` provider": a capability
  providers need, added on the shared compat surface instead of each provider
  reaching into core for it.

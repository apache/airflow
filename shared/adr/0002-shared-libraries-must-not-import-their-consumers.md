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

# 2. Shared libraries must not import their consumers

Date: 2026-07-19

## Status

Accepted

## Context

Shared libraries sit *below* the distributions that use them (ADR 1). That
layering holds only if dependency arrows point one way: a shared library may
depend on third-party packages and other shared libraries, but must not reach up
into `airflow` core or the Task SDK.

Two things break if it does. **Circular dependencies**: task-sdk must be
installable without airflow-core (workers ship the SDK, not the server), so a
`from airflow.models import …` inside a shared library task-sdk links would drag
core onto the worker. **Independent packageability**: a shared library that
imports its consumer can no longer be built, tested, or versioned on its own —
the property ADR 1 relies on. The pull toward violating this is ordinary reuse
("core already has the thing, importing it avoids duplication"), but the needed
primitive must be expressed without the consumer or moved down. Because nothing
local catches an upward import in the monorepo, the boundary is enforced by the
`check-airflow-imports-in-shared` hook, which fails when `shared/**/src` imports
`airflow` core/SDK (with a small allow-list for unavoidable interface shims).

## Decision

Code under `shared/` must not import `airflow` core or the Task SDK:

- A shared library's `src/` imports only third-party packages and other shared
  libraries — never `airflow.models`, `airflow.<core>` internals, or `airflow.sdk`.
- When a shared library needs a capability that currently lives in a consumer, the
  capability is moved *down* into the shared layer (or expressed via an interface
  the consumer implements), not imported *up*.
- The `check-airflow-imports-in-shared` hook is the enforcement; its allow-list is
  for genuinely unavoidable shims and is not extended to wave through a real
  violation.

## Consequences

- The Task SDK stays installable on a worker without airflow-core, and each shared
  library stays buildable/testable on its own.
- Reusing a core class from a shared library is not a shortcut: the shared code
  either duplicates a small primitive deliberately or the primitive is relocated
  into `shared/`.
- The dependency graph stays acyclic and the layering legible.

A change **violates** this decision when, in a `shared/**/src` file, it:

- adds an `import` of `airflow` core (`airflow.models`, core internals) or the
  Task SDK (`airflow.sdk`) to obtain something the library needs;
- reintroduces a consumer dependency (e.g. a core `Connection`/ORM type) into a
  shared library to "reuse" it, rather than decoupling or moving the primitive down;
- extends the `check-airflow-imports-in-shared` allow-list to land an upward import
  instead of removing it.

A reviewer should reject any shared-library change that makes the library depend on
the distribution that links it.

## Evidence

- #61523 — removes an upward `Connection` coupling so the shared library no longer
  needs a core type.
- #58621 — the `BaseSecretsBackend` relocation that lets both sides link the
  primitive without either importing the other.
- The `check-airflow-imports-in-shared` prek hook
  (`scripts/ci/prek/check_airflow_imports_in_shared.py`) — standing enforcement of
  this boundary.

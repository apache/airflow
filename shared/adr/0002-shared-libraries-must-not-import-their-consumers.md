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

Shared libraries exist *below* the distributions that use them (ADR 1). airflow-core
links them; task-sdk links them; providers link them. That layering only holds if
the dependency arrows point one way: a shared library may depend on third-party
packages and on other shared libraries, but it must not reach back up into
`airflow` core or the Task SDK.

Two things break if it does. First, **circular dependencies**: task-sdk must be
installable without airflow-core (workers ship the SDK, not the server), so a
`from airflow.models import …` inside a shared library that task-sdk links would
drag core onto the worker and defeat the split. Second, **independent
packageability**: a shared library that imports its consumer can no longer be built,
tested, or versioned on its own — the very property ADR 1 relies on.

The pull toward violating this is ordinary reuse: the core ORM or a core util
already has the thing the shared code wants, so importing it "avoids duplication."
But the shared library is the *lower* layer; the primitive it needs must be
expressed without the consumer, or moved down into the shared layer too. Because
nothing local catches an upward import — everything is on the path in the monorepo
— the project enforces the boundary with a prek hook,
`check-airflow-imports-in-shared`, which fails when `shared/**/src` imports
`airflow` core/SDK (with a small, explicit allow-list for a few unavoidable
interface shims).

## Decision

Code under `shared/` must not import `airflow` core or the Task SDK. Concretely:

- A shared library's `src/` imports only third-party packages and other shared
  libraries — never `airflow.models`, `airflow.<core>` internals, or `airflow.sdk`.
- When a shared library needs a capability that currently lives in a consumer, the
  capability is moved *down* into the shared layer (or expressed via an interface
  the consumer implements), not imported *up*.
- The `check-airflow-imports-in-shared` hook is the enforcement; its allow-list is
  for genuinely unavoidable shims and is not extended to wave through a real
  violation.

## Consequences

- The Task SDK stays installable and runnable on a worker without airflow-core,
  and each shared library stays buildable/testable on its own.
- Reusing a core class from a shared library is not available as a shortcut; the
  shared code either duplicates a small primitive deliberately or the primitive is
  relocated into `shared/`.
- The dependency graph stays acyclic and the layering stays legible — the shared
  libraries are always the lower layer.

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

- #61523 — "Remove Connection dependency from shared secrets backend": removes an
  upward coupling so the shared library no longer needs a core type.
- #58621 — "Move BaseSecretsBackend to shared library for client server
  separation": the relocation that lets both sides link the primitive without
  either importing the other.
- The `check-airflow-imports-in-shared` prek hook
  (`scripts/ci/prek/check_airflow_imports_in_shared.py`) is the standing
  enforcement of this boundary across all shared libraries.

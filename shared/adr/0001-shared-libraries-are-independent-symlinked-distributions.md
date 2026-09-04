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

# 1. Shared libraries are independent distributions consumed via symlinks

Date: 2026-07-19

## Status

Accepted

## Context

Airflow is a monorepo of independently released distributions — airflow-core,
task-sdk, providers, airflow-ctl. Some code is genuinely common to several of
them (logging setup, serialization, config parsing, secret masking). Duplicating
it lets copies drift; putting it in one distribution and importing from the
others couples release cycles and risks circular dependencies (task-sdk must not
depend on airflow-core — ADR 2).

The chosen model is small, self-contained *shared library distributions* under
`shared/` — `apache-airflow-shared-<name>`, each with its own `pyproject.toml`,
`src/airflow_shared/<name>/` (implicit namespace package), and `tests/<name>/`.
Consumers do not `pip`-depend on a published package during development; each
library's sources are **symlinked** into the consumer
(`airflow-core/src/airflow/_shared/`, `task-sdk/src/airflow/sdk/_shared/`, …),
with symlinks and dependency wiring maintained by `prek` hooks
(`check-shared-distributions-structure`, `check-shared-distributions-usage`).
Editing a consumer's `_shared/` file edits the real file in `shared/<lib>/`, so
**one edit lands in every consumer at once** — the structure is load-bearing.

## Decision

Common code shared by more than one distribution lives in a shared library under
`shared/`, as an independent distribution linked into its consumers:

- Each shared library keeps the enforced layout: `apache-airflow-shared-<name>`,
  `src/airflow_shared/<name>/` (no top-level `__init__.py`), `tests/<name>/`, and
  the correct build-system / wheel targets.
- A change is made in the library at its source, its **own** `tests/<name>/` cover
  it, and every consumer's dependency wiring is updated together — not one
  consumer in isolation.
- When a primitive is needed by more than one distribution, it is *moved into* a
  shared library and linked, rather than left in one distribution and imported by
  the others.

## Consequences

- Common code has a single source of truth, so copies cannot drift; each consumer
  still links only what it needs.
- The blast radius is every consumer that links the library, so review and testing
  must consider all of them; the library's own test suite proves the change.
- The distribution/symlink structure is part of the contract: breaking the layout,
  or wiring one consumer without the others, breaks the build for everyone.

A change **violates** this decision when it:

- duplicates shared code into a consumer (or leaves it in one distribution for the
  others to import) instead of putting it in a `shared/` library;
- breaks the enforced distribution layout — wrong package path, a stray top-level
  `__init__.py`, missing `tests/<name>/`, wrong build-system/wheel targets;
- edits a shared library but updates only one consumer's dependency wiring, or adds
  behaviour without a test in the library's own `tests/<name>/`;
- adds a third-party dependency to a shared library without accounting for the
  cost to every consumer that links it.

## Evidence

- #58621 — moves `BaseSecretsBackend` into a shared library so server and worker
  sides link it independently.
- #61523 — removes a `Connection` dependency so the shared secrets backend stays
  independently linkable.
- #63932 — removes `DualStatsManager`/`Stats` interfaces, consolidating the
  primitive in the shared library rather than duplicating it.

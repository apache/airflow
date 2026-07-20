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

# 3. Core utilities are high-fan-in primitives and are treated as internal API

Date: 2026-07-20

## Status

Accepted

## Context

Almost nothing in `airflow-core/src/airflow/utils/` is a leaf. `provide_session`,
`with_row_locks`, `nulls_first`, `UtcDateTime`, `ExtendedJSON`,
`ExecutorConfigType`, `retry_db_transaction`, and the `helpers.py` grab-bag are
imported by the scheduler, the API server, the Dag processor, the migration
chain, and — because these modules have always been importable — by providers
and user code as well.

That fan-in changes the economics of a change here in three ways.

**The diff understates the blast radius.** A one-line change to a helper is a
behaviour change at every call site the reviewer cannot see. Argument reordering,
a changed default, a subtly different return type, or a "cleanup" that drops a
seemingly redundant branch (the dialect fallbacks in `with_row_locks` and
`nulls_first` look redundant and are not) propagates instantly and silently.

**These modules load in every process.** Schedulers, workers, triggerers, and
short-lived CLI invocations all import them at startup. Work done at import time
— a config read, provider discovery, a heavyweight third-party import — is paid
by every process, every time. Expensive dependencies are therefore imported
lazily where the pattern already exists (Alembic inside `db_manager`, Kubernetes
models inside `sqlalchemy.py`'s pod helpers), and hot lookups are given
appropriate data structures rather than linear scans.

**Correctness details that would be pedantic elsewhere are load-bearing here.**
A helper used inside the scheduler loop must be cheap and side-effect-free.
Durations must be measured with `time.monotonic()`, never `time.time()`, because
a wall-clock delta goes negative or jumps across an NTP step or a DST boundary —
and a helper that returns a nonsense duration corrupts every metric and timeout
built on it. Signal- and thread-sensitive helpers (`timeout_with_traceback`)
must degrade rather than crash on platforms and threads where the primitive is
unavailable, because they are called from contexts their author never saw.

The project has drawn the corresponding structural conclusion: this directory is
a **closed set**. The `check-no-new-airflow-core-utils-modules` prek hook freezes
the list of top-level modules against
`scripts/ci/prek/known_airflow_core_utils_modules.txt`. New shared code belongs
in `shared/` (reused across distributions), in the feature's own sub-package
alongside the code it serves, or in `task-sdk/`. The directory has been actively
shrinking on that basis — `timezone`, `setup_teardown`, `timeout`, the trigger
rule and weight rule modules, and the entry-point helpers have all moved out,
each leaving a redirect shim registered through `add_deprecated_classes` in
`utils/__init__.py` so external imports keep working with a warning.

## Decision

- **Treat every public name in this directory as internal API with real
  consumers.** A signature, default, return type, or semantic change is the
  *point* of a PR — never an incidental part of a larger one — and updates every
  call site in the same change.
- **Do not add a new top-level module here.** The allowlist is frozen. Place new
  code in `shared/`, in the owning feature's sub-package, or in `task-sdk/`. If
  `utils/` genuinely is the right home, agree that with maintainers before
  writing the code, then regenerate the allowlist in the same PR.
- **Moving or removing a module leaves a deprecation shim** registered via
  `add_deprecated_classes` in `utils/__init__.py`, pointing at the new location.
- **Keep helpers cheap and side-effect-free.** No config reads, provider
  discovery, file I/O, or heavyweight imports at module import time; lazy-import
  expensive dependencies; guard type-only heavy imports with `TYPE_CHECKING`.
  Choose data structures for the access pattern — a membership test on a hot path
  is a set, not a list.
- **Measure durations with `time.monotonic()`.** `time.time()` is for wall-clock
  timestamps only.
- **Degrade, do not crash, on platform and context differences.** A helper that
  depends on signals, threads, or a specific dialect must fall back rather than
  raise when the primitive is unavailable — its callers cannot all be enumerated.
- **A `TypeDecorator` change is a data-format change.** Altering `UtcDateTime`,
  `ExtendedJSON`, or `ExecutorConfigType` must keep reading rows already written
  in the old shape.

## Consequences

- Consumers across airflow-core, providers, and user code can rely on these
  helpers without version-gating every call.
- Refactors here are larger and slower than they look: the diff includes every
  call site, which is the correct cost, not an obstacle to route around.
- Genuinely new utility code lands where it is owned, so this directory keeps
  shrinking toward the primitives that must live in core.
- Import-time frugality is a standing constraint: a convenient top-level import
  of an expensive dependency is not acceptable even when it reads better.
- Legacy import paths accumulate shims. That is the accepted price of moving
  modules out without breaking external code.

A change **violates** this decision when it:

- alters the signature, defaults, return type, or semantics of a widely-used
  helper as a side effect of an unrelated change, or without updating every
  call site;
- adds a new top-level module under `utils/`, or regenerates
  `known_airflow_core_utils_modules.txt` to admit one without linking the
  agreement in the PR body;
- moves or deletes a module without registering a redirect shim in
  `utils/__init__.py`;
- performs config reads, provider discovery, file I/O, or heavyweight imports at
  module import time, or promotes an existing lazy import to the top level;
- uses `time.time()` to measure a duration;
- removes a dialect or platform fallback (`with_row_locks` returning an unlocked
  query, `nulls_first` being a no-op off PostgreSQL, signal handling guarded for
  non-main threads) because it looks redundant;
- changes a `TypeDecorator`'s stored representation without handling rows
  already written in the old shape;
- adds a near-duplicate of an existing helper instead of extending it, creating
  two derivations that will drift.

## Evidence

- #66105 — "CI: Block new modules under `airflow-core/src/airflow/utils/`":
  freezes the module set and makes the "put it in `shared/` or the feature's own
  package" rule mechanical.
- #62927 — "refactor: remove modules that are supposed to be removed in Airflow
  3.2": the ongoing shrinking of this directory.
- #53196 — "Extend `add_deprecated_classes` to support wildcard patterns":
  strengthens the shim mechanism that lets whole modules move out without
  breaking importers.
- #60061 — "Update usages of entry_points helpers and deprecate it from utils":
  a module leaving `utils/` for `shared/` with the redirect shim in place.
- #65655 — "Lazy-load Alembic in db_manager imports": import-time cost of a
  heavyweight dependency paid by every process.
- #66306 — "Convert `RUNTIME_VARYING_CALLS` to frozenset for O(1) membership
  lookup": data structure chosen for the hot-path access pattern.
- #63664 — "Fix `timeout_with_traceback` crashes on Windows and non-main
  threads": a helper that must degrade rather than crash in contexts its author
  did not enumerate.
- #56982 — "Fix remaining MyPy type errors in utils/": keeping the contracts of
  these primitives statically checkable, precisely because their call sites are
  everywhere.

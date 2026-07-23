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

# 3. User code runs on the worker, behind an explicit isolation contract

Date: 2026-07-20

## Status

Accepted

## Context

This provider is where Airflow executes arbitrary user code: `PythonOperator`
calls a callable, `BashOperator` runs a command in a subprocess,
`PythonVirtualenvOperator` builds a venv and runs the callable in it,
`ExternalPythonOperator` runs it under a pre-existing interpreter, and `@task`,
`@task.bash`, `@task.virtualenv`, `@task.external_python` are the front doors. Two
boundaries meet here, both easy to erode from a small diff.

**Parse time vs execution time.** These operators are *constructed* by the Dag File
Processor, in a short-lived parse subprocess that runs for every Dag file every
interval and must not be blocked. So `__init__` runs in the processor,
`execute(context)` on the worker. Resolving an interpreter, building a venv,
importing dependencies, reading the filesystem, or hitting a package index during
construction moves work from the worker (bounded by the task) into the parse loop
(multiplied by every parse of every Dag) — and since these operators appear in
essentially every Dag, that multiplication is the whole deployment.

**The virtualenv operators' process boundary.** `_BasePythonVirtualenvOperator`
hands the child interpreter a *filtered* copy of the context. The
`*_SERIALIZABLE_CONTEXT_KEYS` sets in `operators/python.py` are explicit,
version-gated allowlists (`task_reschedule_count` on Airflow 3+, `partition_key` /
`partition_date` on 3.3+); `execute()` narrows `context` to that intersection,
`op_args` / `op_kwargs` are serialized to disk, and the callable runs through the
generated `utils/python_virtualenv_script.jinja2` bootstrap in an interpreter that
may not even have Airflow (`expect_airflow=False`). That allowlist is a contract:
it keeps the child's requirements independent of Airflow's and keeps live objects
(sessions, proxies, client handles) from crossing a boundary where they are
meaningless. It is invisible in a diff — adding a key that "should obviously be
available" has process-boundary consequences, and removing one breaks callables
that read it. The failure modes are opaque, not loud: a bare
`PicklingError` naming nothing until #63270, an offline venv build until #59046,
bundle imports #57631, plugin init #48035, `expect_airflow=False` #54809. Each is
the isolated side lacking what the in-process side had, with an error that does not
say so. The boundary has been
probed deliberately both ways: `use_airflow_context` removed (#46306), and
connection/variable access plus log forwarding landed, reverted, re-landed (#57213,
revert, #58148) — release-boundary decisions, not incidental refactors.

## Decision

Keep user code on the worker, and keep what crosses into an isolated interpreter
an explicit, version-gated allowlist. Concretely:

- **`__init__` does construction only.** No venv creation, interpreter
  resolution, filesystem access, network access, callable import, or heavy
  third-party import at operator construction time — that code runs in the Dag
  processor for every Dag on every parse.
- **Module import stays cheap** for the same reason: heavy or type-only imports
  go behind `TYPE_CHECKING` or `version_compat`.
- **Context crossing a process boundary is allowlisted, not passed through.**
  Adding a key means adding it to the correct `*_SERIALIZABLE_CONTEXT_KEYS` set
  with the right `version_compat` gate and covering it in the serialization
  tests — never widening to "the whole context".
- **What the isolated interpreter may reach is a deliberate decision.** Changing
  it (connections, variables, log forwarding, Airflow availability) is a
  release-boundary change discussed on its own, not folded into a fix.
- **The isolated path must not assume the parent's environment.** Airflow may be
  absent (`expect_airflow=False`), the index may be private or unreachable, the
  bundle path must be resolved and passed explicitly, and temporary state
  (venvs, `__pycache__`) must be cleaned up.
- **Failures on the isolated side produce an author-facing message** naming what
  went wrong — which kwarg failed to serialize, which requirement failed to
  install — not a re-raised low-level error from the serializer or the builder.

## Consequences

- Dag parsing stays fast and bounded regardless of task cost — a precondition for
  these operators being usable in every Dag.
- The virtualenv operators can run callables under interpreters and dependency sets
  Airflow itself could not, because the crossing surface is small and explicit.
- Exposing new context to venv callables is more work than reading it in-process
  (allowlist entry, version gate, test) — deliberate friction; the allowlist is the
  boundary.
- The isolated path is harder to debug, so error-message quality there is a
  functional requirement, not polish.

A change **violates** this decision when it:

- performs venv construction, interpreter resolution, callable import,
  filesystem I/O, or network access in an operator's `__init__` or at module
  import time, rather than in `execute()` on the worker;
- passes the task context — or an unfiltered subset of it — into an isolated
  interpreter instead of extending the appropriate
  `*_SERIALIZABLE_CONTEXT_KEYS` allowlist;
- adds a context key to an allowlist without the matching `version_compat` gate
  or without a serialization test;
- widens what the isolated interpreter can reach (connections, variables,
  logging, Airflow itself) as part of an unrelated change rather than as its own
  discussed decision;
- assumes the child interpreter shares the parent's Airflow install, package
  index, bundle path, or working directory;
- surfaces an isolated-path failure as an opaque serializer, subprocess, or
  package-manager error instead of a message naming the offending input;
- leaves venv or `__pycache__` state behind on the worker after the task
  completes.

## Evidence

- #50446, #50566 — the allowlist and its tests treated as a first-class contract,
  not an implementation detail.
- #46306 — narrowing what crosses the boundary (`use_airflow_context` removed) as
  its own explicit decision.
- #57213, revert, #58148 — widening the boundary treated with the same weight:
  reverted when it destabilised, re-landed separately.
- #63270 — non-serializable `op_kwargs` error naming the offending key instead of a
  bare `PicklingError`: the error-quality requirement in practice.
- #67157 — same requirement, Azure Key Vault secret-backend isolated-side failure.
- #54809 — `external_python` with `expect_airflow=False`: the child may not have
  Airflow.
- #59046, #52287, #52288 — the isolated build cannot assume the parent's network or
  index configuration.
- #57631, #48035 — the child does not inherit the parent's import environment; the
  bundle path is resolved and passed explicitly.
- #53390 — the isolated side cleans up its venv `__pycache__` on the worker.
- #54065, #57599 — the boundary maintained on the output side, and documented where
  users are likely to violate it themselves.

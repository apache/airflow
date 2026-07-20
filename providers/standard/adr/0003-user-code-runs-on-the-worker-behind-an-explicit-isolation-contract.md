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

This provider is where Airflow actually executes arbitrary user code.
`PythonOperator` calls a user callable; `BashOperator` runs a user command
through a subprocess; `PythonVirtualenvOperator` builds a virtualenv and runs the
callable in it; `ExternalPythonOperator` runs it under a pre-existing
interpreter. The decorators `@task`, `@task.bash`, `@task.virtualenv` and
`@task.external_python` are the front doors to all of them.

Two boundaries meet here, and both are easy to erode from a small diff.

**Parse time versus execution time.** Operators in this package are
*constructed* by the Dag File Processor, in a short-lived parse subprocess that
runs for every Dag file on every parse interval and must not be blocked (see the
`dag_processing` area). `__init__` therefore runs in the processor;
`execute(context)` runs on the worker. Resolving an interpreter, building a
venv, importing the callable's dependencies, reading the filesystem, or
contacting a package index during construction moves work from the worker — where
it is bounded by the task — into the parse loop, where it is multiplied by every
parse of every Dag file that imports the operator. Because the standard
operators appear in essentially every Dag, that multiplication is the whole
deployment.

**The process boundary of the virtualenv operators.** `_BasePythonVirtualenvOperator`
does not hand the child interpreter the task context. It hands it a filtered
copy. `BASE_SERIALIZABLE_CONTEXT_KEYS`, `PENDULUM_SERIALIZABLE_CONTEXT_KEYS` and
`AIRFLOW_SERIALIZABLE_CONTEXT_KEYS` in `operators/python.py` are explicit
allowlists, version-gated (`task_reschedule_count` only on Airflow 3+,
`partition_key` / `partition_date` only on 3.3+); `execute()` narrows `context`
to that intersection before delegating, `op_args` / `op_kwargs` are serialized to
disk, and the callable is executed through the generated
`utils/python_virtualenv_script.jinja2` bootstrap in a separate interpreter that
may not even have Airflow installed (`expect_airflow=False`).

That allowlist is a contract, not an implementation detail. It is what makes the
child interpreter's requirements independent of Airflow's, and it is what keeps
a user callable from receiving live objects — sessions, proxies, client handles —
across a process boundary where they are meaningless or unsafe. It is also
invisible in a diff: adding a context key that "should obviously be available"
is a one-line change with process-boundary consequences, and removing one
silently breaks callables that read it.

The failure modes are consistently opaque rather than loud. A non-serializable
`op_kwargs` value surfaced as a bare `PicklingError` naming nothing until #63270
added per-key identification. A venv build without direct internet access failed
unhelpfully until #59046. Bundle-relative imports broke inside subprocess
operators (#57631); plugin initialisation broke inside the jinja bootstrap
(#48035); `expect_airflow=False` failed on the external-python path (#54809).
Each is the same underlying shape: the isolated side does not have what the
in-process side had, and the error does not say so.

The boundary has also been probed deliberately in both directions — the
`use_airflow_context` feature was removed (#46306), and connection/variable
access plus log forwarding from venv code was landed, reverted, and re-landed
(#57213, then reverted, then #58148). Those are release-boundary decisions about
what crosses the process line, not incidental refactors.

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

- Dag parsing stays fast and bounded regardless of how expensive the tasks
  themselves are, which is a precondition for these operators being usable in
  every Dag.
- The virtualenv operators can run callables under interpreters and dependency
  sets that Airflow itself could not, because the crossing surface is small and
  explicit.
- Making a new piece of context available to venv callables is more work than
  reading it in-process: an allowlist entry, a version gate, and a test. That
  friction is deliberate — the allowlist is the boundary.
- Debugging the isolated path is harder than the in-process path, so
  error-message quality on that side is treated as a functional requirement
  rather than polish.

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

- #50446 ("Preserve all context keys during serialization") and #50566 ("Improve
  testing for context serialization") — the allowlist and its tests treated as a
  first-class contract rather than an implementation detail.
- #46306 — "Removing feature: send context in venv operators (using
  `use_airflow_context`)": narrowing what crosses the boundary, done as its own
  explicit decision.
- #57213 ("Allow virtualenv code to access connections/variables and send logs"),
  its subsequent revert, and #58148 (the re-land) — widening the boundary
  treated with the same weight: reverted when it destabilised, re-landed
  separately.
- #63270 — "improve error message for non-serializable `op_kwargs` in
  `PythonVirtualenvOperator`": identifying the offending key instead of
  re-raising an opaque `PicklingError`, the error-quality requirement in
  practice.
- #67157 — "Fix error messages in `PythonVirtualenvOperator` when Azure Key
  Vault secret backend is configured": same requirement, different isolated-side
  failure.
- #54809 ("Fix `external_python` task failure when `expect_airflow=False`") — the
  child interpreter genuinely may not have Airflow.
- #59046 ("fix uv venv fail without direct internet access"), #52287 ("Honor
  `index_urls` when venv is created with `uv`") and #52288 ("Add support for
  `PackageIndex` connections in `PythonVirtualenvOperator`") — the isolated build
  cannot assume the parent's network or index configuration.
- #57631 ("Fix DAG bundle imports in subprocess operators") and #48035 ("Fix
  python operators errors when initialising plugins in virtualenv jinja script")
  — the child does not inherit the parent's import environment; the bundle path
  is resolved from the task instance and passed across explicitly.
- #53390 ("Add venv pycache clean up for the `PythonVirtualenvOperator`") — the
  isolated side cleans up after itself on the worker.
- #54065 ("Introduce `StdoutCaptureManager` to isolate stdout from `logging`
  logs") and #57599 ("Add caution on using Airflow packages in virtualenv
  operator") — the boundary maintained on the output side, and documented where
  users are likely to violate it themselves.

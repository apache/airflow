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

# 3. task-sdk is an independently versioned distribution that must not import airflow-core ORM

Date: 2026-07-19

## Status

Accepted

## Context

`task-sdk` is a **separate Python distribution** with its own `pyproject.toml`,
version, and release cadence. That separation is the point: workers run the Task
SDK and communicate over the versioned Execution API (ADR 1, ADR 2), and the two
sides **deploy independently** — a worker can run a different
`apache-airflow-task-sdk` release than the server's airflow-core. The Execution
API's backward-compatibility machinery only buys anything if the worker runtime is
genuinely decoupled from airflow-core internals.

The rule that keeps it decoupled: the runtime must not import `airflow.models` /
the airflow-core ORM. The worker's view of server-owned shapes comes from
generated datamodels in `airflow.sdk.api.datamodels._generated` (from the
Execution-API spec), not ORM classes; and heavy server-only dependencies (FastAPI,
Cadwyn) stay off the worker import path. This boundary erodes silently: an
`airflow.models` import compiles fine in the monorepo, so the coupling is
invisible until someone installs task-sdk standalone or an older worker meets a
newer server with a drifted "shared" class. Prek hooks
(`check_core_imports_in_sdk`, cross-distribution boundary checks) fail the build
when the runtime reaches back into core.

## Decision

The Task SDK execution runtime stays an independently installable, independently
versioned distribution that does not depend on airflow-core internals.
Concretely:

- Runtime code in this package does **not** import `airflow.models` or other
  airflow-core ORM modules; server-owned shapes are consumed from
  `airflow.sdk.api.datamodels._generated` (generated from the Execution-API spec).
- Heavy server-only dependencies (FastAPI, Cadwyn) are kept **off** the worker
  import path — deferred or confined to non-worker code — so the SDK stays light
  to install and import on a worker.
- Shared types and enums that both sides need are defined in the
  Execution-API / OpenAPI spec and regenerated, not imported cross-distribution
  as Python (rule of thumb: if another language's SDK would need it, it belongs in
  the spec).
- The `check_core_imports_in_sdk` / cross-distribution import-boundary prek hooks
  are the enforcement; new violations are fixed, not blanket-suppressed.

## Consequences

- A worker can run a task-sdk release independent of the server's airflow-core
  version — the premise the Execution-API compatibility work depends on.
- Reusing an ORM class is not a shortcut; a shared shape is added to the spec and
  regenerated, keeping one versioned source of truth across the Python and Go/Java
  SDKs.
- The SDK's import footprint stays small enough to ship to workers without the
  server stack, at the cost of some datamodel/ORM duplication.

A change **violates** this decision when, in the Task SDK runtime, it:

- adds an `import` of `airflow.models` / airflow-core ORM (or another
  server-internal module) to obtain a shape the generated datamodels already
  provide;
- pulls a heavy server-only dependency (FastAPI, Cadwyn, …) onto the worker import
  path instead of deferring or confining it;
- suppresses the `check_core_imports_in_sdk` / cross-distribution boundary hook
  (blanket ignore markers) to land a cross-distribution import rather than
  removing it;
- introduces a shared enum/type by importing it cross-distribution in Python
  instead of defining it in the Execution-API / OpenAPI spec and regenerating.

A reviewer should reject any change that couples the worker runtime to
airflow-core internals or that makes task-sdk un-installable / un-startable
independently of a matching airflow-core version.

## Evidence

- #65358, #65880 — the tooling enforcing the no-core-import rule.
- #69016 — defer Cadwyn import to keep FastAPI off the worker path.
- #67056 — decouple remote logging config from core.
- #68980 — allow missing `api_auth.jwt_secret` for `InProcessExecutionAPI`,
  keeping the SDK usable without server-only configuration.

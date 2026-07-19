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
version, and release cadence. That separation is the whole point: workers run the
Task SDK and communicate with the API server over the versioned Execution API
(ADR 1, ADR 2), and the two sides **deploy independently** — a worker can run a
different `apache-airflow-task-sdk` release than the API server's airflow-core.
The Execution API's backward-compatibility machinery only buys anything if the
worker runtime is genuinely decoupled from airflow-core's internals.

The concrete rule that keeps it decoupled: the execution runtime must not import
`airflow.models` / the airflow-core ORM. The worker's view of task-instance and
other server-owned shapes comes from generated datamodels in
`airflow.sdk.api.datamodels._generated` (produced from the Execution-API spec),
not from the ORM classes. It also means keeping heavy, server-only dependencies
(FastAPI, Cadwyn) off the worker import path, so installing and starting the SDK
on a worker doesn't drag in the whole server.

This boundary erodes silently. An `airflow.models` import compiles fine in the
monorepo — everything is on the path during development — so the coupling is
invisible until someone tries to install task-sdk standalone, or until an older
worker meets a newer server and the "shared" class has drifted. Because nothing
local catches it, the project added prek hooks (`check_core_imports_in_sdk`,
cross-distribution import-boundary checks) to fail the build when the runtime
reaches back into core. The pressure is ordinary reuse — the ORM already models a
TaskInstance, so importing it "avoids duplication" — and the decision below is
what routes that need through the generated datamodels and the spec instead.

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

- A worker can run a task-sdk release independent of the API server's airflow-core
  version, which is the premise the Execution-API compatibility work depends on.
- Reusing an airflow-core ORM class is not available as a shortcut; a shared shape
  is added to the spec and regenerated, which keeps a single versioned source of
  truth across the Python and Go/Java SDKs.
- The SDK's dependency and import footprint stays small enough to ship to workers
  without the server stack, at the cost of some duplication between the generated
  datamodels and the core ORM.

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

- #65358 — "Support inline ignore marker for check_core_imports_in_sdk hook" and
  #65880 — "Add prek checks for cross-distribution import boundaries": the tooling
  that enforces the no-core-import rule this ADR records.
- #69016 — "Defer Cadwyn import to keep FastAPI off the Task SDK worker path":
  keeps a heavy server-only dependency off the worker import path.
- #67056 — "Decouple remote logging config from core": removes an airflow-core
  coupling from the SDK-side path.
- #68980 — "Allow missing `api_auth.jwt_secret` for `InProcessExecutionAPI`":
  keeps the SDK usable without server-only configuration, consistent with an
  independently deployable distribution.

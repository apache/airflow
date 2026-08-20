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

# 3. Run executable Go bundles through the coordinator protocol

Date: 2026-04-30

## Status

Accepted. The original dual-runtime decision recorded here was superseded by
[ADR 0005](0005-retire-go-edge-worker.md). This document now describes the
coordinator protocol that remains.

References to ZIP bundles in the original decision were superseded by
[ADR 0004](0004-self-contained-executable-bundle.md), which embeds source and
metadata in a footer appended to the executable.

## Context

Airflow launches non-Python task runtimes through the coordinator layer. The
Python supervisor owns task lifecycle and Execution API communication, while a
language runtime runs user code in a subprocess. This lets compiled SDKs reuse
the supervisor's task-state handling, heartbeats, remote logging, XCom backend,
and security model.

The language-agnostic executable-bundle contract requires a runtime to accept:

- `--comm=<host:port>` for length-prefixed msgpack request and response frames.
- `--logs=<host:port>` for structured JSON-line task logs.

The Python-side launcher is
[`ExecutableCoordinator`](../../task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py).
It opens both loopback listeners before starting the bundle, then passes their
addresses on the bundle command line.

## Decision

`bundlev1server.Serve` has two valid invocations:

| Invocation | Behaviour |
|---|---|
| `--airflow-metadata [--format=yaml\|json]` | Print the bundle manifest used by `airflow-go-pack`, then exit. |
| `--comm=<host:port> --logs=<host:port>` | Run one task through the coordinator protocol. |

Bundle execution requires both coordinator addresses. Supplying one or neither
returns an error; `--format` is only valid with `--airflow-metadata`.

### Startup and logging

The runtime dials both supervisor listeners concurrently. It installs a
`slog.Handler` on the logs connection before user code runs, reads the first
comm frame, and requires a `StartupDetails` message. The message contains the
task-instance identity, bundle identity, retry decision, runtime context, and
TaskFlow argument bindings.

The runtime materialises the author's `BundleProvider` into an in-memory
registry, looks up the requested `dag_id` and `task_id`, and runs the registered
Go function. Function parameters are resolved from:

- injected `sdk.TIRunContext`, `context.Context`, `*slog.Logger`, and the
  coordinator-backed `sdk.Client`;
- literal TaskFlow arguments carried in `StartupDetails`; and
- upstream return-value XCom references fetched through the supervisor.

The runtime sends a terminal `SucceedTask`, `RetryTask`, or `TaskState` response
on the original frame id. Panics are recovered and converted to failure or
retry responses. SIGINT and SIGTERM cancel the task context so cooperative Go
tasks can stop before the supervisor escalates termination.

### Airflow service calls

The injected `sdk.Client` communicates only through the comm socket. Variable,
Connection, and XCom calls are encoded as coordinator messages and proxied by
the Python supervisor to the Execution API. The Go bundle does not hold a task
JWT or connect to the Execution API itself.

### Framing

Comm messages are prefixed by a four-byte big-endian payload length. Requests
are msgpack `[id, body]` values and responses are `[id, body, error]`. The
runtime correlates requests by id and serialises socket writes so task code may
issue concurrent client calls safely.

The logs socket carries one UTF-8 JSON object per line. If the socket closes,
the handler falls back to stderr rather than blocking the task.

## Consequences

- Go tasks use the same supervisor-owned lifecycle as other language SDKs.
- A bundle author has one entry point and one execution protocol.
- The Go SDK must keep its generated coordinator models aligned with the
  Task SDK supervisor schema. `TestSupervisorSchemaVersionMatchesSnapshot`
  enforces the schema version and `just generate-models` refreshes the models.
- Go bundles remain self-contained executables built by `airflow-go-pack` and
  discovered from their `AFBNDL01` footer without executing them.

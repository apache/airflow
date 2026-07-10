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

# 3. Dual-mode bundle binary: msgpack-over-IPC coordinator protocol alongside the existing go-plugin/Edge-Worker path

Date: 2026-04-30

## Status

Accepted.

The references in this ADR to a "ZIP bundle" — the bundle-spec phrasing
quoted in Context, and the `airflow-go-pack` output described in
Consequences — are superseded by
[ADR 0004](0004-self-contained-executable-bundle.md), which replaces
the ZIP container with a self-contained executable carrying the source
and manifest in an appended footer. The coordinator-mode protocol
decision in this ADR is unaffected: the binary still honours
`--comm=<addr>` / `--logs=<addr>` exactly as described, regardless of
the container format it ships inside. Read the ZIP mentions below with
the ADR 0004 substitution in mind.

## Context

A Go SDK bundle binary today (the artefact built from
[`go-sdk/example/bundle/main.go`](../example/bundle/main.go) via
`bundlev1server.Serve`) speaks exactly one protocol: HashiCorp
[`go-plugin`](https://github.com/hashicorp/go-plugin) gRPC over a
stdio-negotiated socket, gated by the magic-cookie handshake declared in
[`pkg/bundles/shared/handshake.go`](../pkg/bundles/shared/handshake.go).
The Airflow Go *Edge Worker*
([`cmd/airflow-go-edge-worker`](../cmd/airflow-go-edge-worker/main.go),
[`edge/`](../edge)) is the consumer of that protocol — it execs the
bundle binary as a child process, completes the go-plugin handshake,
opens the `DagBundle` gRPC client, and drives `GetMetadata`/`Execute`
([`bundle/bundlev1/bundlev1server/impl/plugin.go`](../bundle/bundlev1/bundlev1server/impl/plugin.go)).
The bundle binary never listens on a public socket; the protocol is
local-process only.

Meanwhile, the Python side of Airflow has standardised on a different
wire protocol for non-Python language runtimes — the *coordinator
protocol* — pioneered by the Java SDK and described in
[java-sdk ADR 0004](../../java-sdk/adr/0004-dag-parsing.md)
and
[java-sdk ADR 0002](../../java-sdk/adr/0002-workload-execution.md).
Its shape is:

- The runtime is launched with `--comm=<host:port>` and
  `--logs=<host:port>` CLI arguments.
- It connects out (TCP, loopback) to both addresses.
- Frames on the comm channel are length-prefixed msgpack: a 4-byte
  big-endian length followed by the msgpack payload. Requests are
  `[id, body]`; responses are `[id, body, error]`.
- Two workloads share one channel, distinguished by the first inbound
  frame: `DagFileParseRequest` (one-shot, returns
  `DagFileParsingResult` and exits) or `StartupDetails` (multi-round
  task execution: the runtime sends `GetConnection` / `GetVariable` /
  `GetXCom` / `SetXCom` and terminates with `SucceedTask` or
  `TaskState`).
- The logs channel carries structured JSON log records emitted by the
  runtime.

The Python-side launcher is
[`ExecutableCoordinator`](../../task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py),
which already builds command lines of the form
`<binary> --comm=<addr> --logs=<addr>` for both `dag_parsing_runtime_cmd`
and `task_execution_runtime_cmd`. The bundle-spec contract
([`task-sdk/docs/executable-bundle-spec.rst`](../../task-sdk/docs/executable-bundle-spec.rst))
ratifies that any compiled SDK shipping a bundle "MUST honour the
SDK coordinator protocol (`--comm=<addr>` / `--logs=<addr>`
socket-based IPC)". The Java SDK satisfies this contract; the Go SDK
currently does not.

The two protocols target different deployment shapes:

- **go-plugin / Edge Worker.** The Go-native worker is itself a long-running
  process that loads bundles in-process and dispatches tasks to them
  over gRPC. It is the only consumer that speaks go-plugin to a Go
  bundle today, and it owns the full task-runtime stack on the worker
  host (no Python in the data path). This is the path
  [`go-sdk/example/bundle/main.go`](../example/bundle/main.go) was
  written for and the path that
  [`pkg/worker`](../pkg/worker) drives.
- **Coordinator / `ExecutableCoordinator`.** The Python task
  runner forks a child that runs `<binary> --comm=… --logs=…`,
  bridges its socket to the Airflow supervisor's fd 0, and proxies
  Airflow service calls (`GetConnection`, `GetVariable`, ...) through
  to the Execution API. This is how Airflow runs non-Python tasks
  *without* a per-language worker — the same way Java runs today, and
  the same way Rust/C++/Zig will run in the future. It is also the
  only path the executable provider's bundle spec recognises.

Today these two paths require two different binaries, even though the
DAG/task definitions, the registry, the worker plumbing, and the
serialisation surfaces overlap almost entirely. That is the gap this
ADR closes.

The user-written `main()` is one line —
`bundlev1server.Serve(&myBundle{})` — and we want to keep it one line.
Whichever protocol the binary should speak must be decided inside
`Serve` based on how it was invoked, not by branching in user code.

## Decision

Make the SDK bundle binary **dual-mode**. A single
`bundlev1server.Serve(bundle, opts...)` call dispatches to one of two
protocol servers based on its CLI arguments and process environment.
User code does not change.

### Invocation matrix

`Serve` evaluates the triggers below in order via a `decideMode`
switch (`server.go`); the first match wins, and go-plugin is the
default when no flag selects another mode.

| #  | Trigger                                                | Mode            | Behaviour |
|----|--------------------------------------------------------|-----------------|-----------|
| 1  | `--airflow-metadata`                                    | metadata-dump   | The single introspection flag (ADR 0001 / ADR 0002, `server.go`). Prints the bundle's `airflow-metadata.yaml` spec as JSON and exits; this is the flag `airflow-go-pack` execs to populate the manifest. |
| 2  | `--comm=<host:port> --logs=<host:port>`                | **coordinator** | New. Speaks the msgpack-over-IPC coordinator protocol. Both flags are required. |
| 3  | exactly one of `--comm` / `--logs`                     | error           | Partial coordinator selection is a hard error (`ErrCoordinatorFlagsIncomplete`), returned to `main` so the caller exits non-zero with usage rather than silently falling back to go-plugin. |
| 4  | none of the above (default)                            | go-plugin       | Existing behaviour. Falls through to `plugin.Serve`, which performs the `AIRFLOW_BUNDLE_MAGIC_COOKIE` handshake and serves `DagBundle` gRPC to the Edge Worker. Running the binary by hand outside an Edge Worker fails the handshake with a diagnostic. |

The two server modes share the same `bundlev1.BundleProvider`
implementation and the same lazy `RegisterDags` recorder cache that
`impl.server` already maintains (`impl/plugin.go:99-121`). Only the
front door changes.

### Coordinator mode: protocol details

When `Serve` enters coordinator mode it:

1. **Parses and validates the addresses.** Both `--comm` and `--logs`
   are `host:port` strings. `127.0.0.1` is the only host the coordinator
   protocol is designed for, but we do not pin it — the value is whatever
   `_runtime_subprocess_entrypoint` chose on the Python side.

2. **Connects out** to the comm address, then to the logs address. Both
   are TCP. We dial; we do not listen. The launcher already has both
   listeners up before exec'ing the binary
   ([java-sdk ADR 0004, "What the Base Class Handles Automatically"](../../java-sdk/adr/0004-dag-parsing.md#what-the-base-class-handles-automatically)).

3. **Routes structured logs to the logs socket.** A new
   `slog.Handler` writes JSON-line records (one record per line, UTF-8,
   newline-terminated) to the logs connection, replacing the
   `hclog`/stderr handler used in go-plugin mode. `slog.SetDefault` is
   called before any user code runs so `log` arguments injected into
   tasks land on the right channel. On disconnect the handler falls
   back to stderr so the binary never deadlocks on a closed sink.

4. **Reads the first comm frame and dispatches by message type.** The
   first frame's body has a `type` field per the Java SDK's encoding
   ([java-sdk ADR 0002, "Task SDK Protocol Messages"](../../java-sdk/adr/0002-workload-execution.md#task-sdk-protocol-messages)).
   Two values are valid here:

   - `DagFileParseRequest` → DAG-parsing one-shot.
   - `StartupDetails` → task execution.

   Any other type is an error frame back to the supervisor and
   `os.Exit(1)`.

#### DAG-parsing path (`DagFileParseRequest` → `DagFileParsingResult`)

```text
Supervisor                          Bundle binary (Go)
    │                                       │
    ├── [4B len][msgpack: id, ─────────────►│
    │   {type: "DagFileParseRequest",       │
    │    file: "<bundle path>"}]            │
    │                                       │
    │                                       ├── BundleProvider.RegisterDags(reg)
    │                                       │   (cached, same as gRPC path)
    │                                       │
    │                                       ├── serialise(reg) →
    │                                       │   DagFileParsingResult
    │                                       │   in DagSerialization v3 JSON
    │                                       │   (see java-sdk ADR-0004)
    │                                       │
    │◄────────────────[4B len][msgpack: ────┤
    │       id, {type: "DagFileParsingResult",
    │            fileloc: "...",
    │            serialized_dags: [...] }]  │
    │                                       │
    │                                       └── close + exit(0)
```

The serialised DAG payload must match Python's `SerializedDAG.serialize_dag`
output **exactly**, including the `__type` / `__var` wrapping rules,
unwrapping of "non-decorated" fields (`start_date`, `end_date`, `tags`),
and the timetable encoding listed in
[java-sdk ADR 0004, "DagFileParsingResult Format"](../../java-sdk/adr/0004-dag-parsing.md#dagfileparsingresult-format).
The Go SDK gains a `serde` package that performs this encoding from
`bundlev1.Bundle` / `bundlev1.Task`, validated against
`validation/serialization/test_dags.yaml` (the same fixture set the Java
SDK uses), so the Go and Java outputs are byte-identical for shared
inputs.

#### Task-execution path (`StartupDetails` → multi-round → `SucceedTask` / `TaskState`)

```text
Supervisor                          Bundle binary (Go)
    │                                       │
    ├── StartupDetails ────────────────────►│
    │   (ti, dag_rel_path, bundle_info,     │
    │    start_date, ti_context; the        │
    │    ti_context carries stub_args, the  │
    │    positional-argument spec captured  │
    │    from the stub Dag's TaskFlow call) │
    │                                       │
    │                                       ├── lookup task:
    │                                       │     bundle.dags[ti.dag_id]
    │                                       │     .tasks[ti.task_id]
    │                                       │   (returns TaskState{state:"removed"}
    │                                       │    if not found, mirroring Java)
    │                                       │
    │                                       ├── bind stub_args onto the task
    │                                       │   fn's data parameters (literals
    │                                       │   decode directly; xcom refs pull
    │                                       │   below); arity/type mismatch
    │                                       │   fails the task
    │                                       │
    │                                       ├── construct sdk.Client whose
    │                                       │   GetConnection / GetVariable /
    │                                       │   GetXCom / SetXCom calls block on
    │                                       │   request/response over the
    │                                       │   comm socket
    │                                       │
    │◄── GetConnection(conn_id) ────────────┤
    ├── ConnectionResult ──────────────────►│
    │◄── GetVariable(key) ──────────────────┤
    ├── VariableResult ────────────────────►│
    │◄── GetXCom(...) ──────────────────────┤
    ├── XComResult ────────────────────────►│
    │◄── SetXCom(...) ──────────────────────┤
    ├── (empty response) ──────────────────►│
    │                                       │
    │                                       ├── task fn returns:
    │                                       │     err == nil → SucceedTask
    │                                       │     err != nil → TaskState{"failed"}
    │                                       │     (panic recovered → "failed")
    │                                       │
    │◄── SucceedTask / TaskState ───────────┤
    │                                       │
    │                                       └── close + exit(0)
```

Concretely, this reuses
[`pkg/worker.Worker`](../pkg/worker/runner.go) for task lookup and
parameter injection — `extract(ctx, sdk.Client, *slog.Logger)`,
`transform(ctx, sdk.VariableClient, *slog.Logger)`, and `load() error`
in the example bundle work unchanged. The injected `sdk.Client`
implementation is swapped: in go-plugin mode it talks to the Execution
API directly via the URL from viper (`impl/plugin.go:182`), in
coordinator mode it talks to the supervisor over the comm socket.
Both implement the same `sdk.Client` / `sdk.VariableClient` interfaces,
so user task code is identical between the two modes.

The `(panic recovered → "failed")` step in the diagram is
`pkg/worker.Worker.ExecuteTaskWorkload`'s existing `defer recover()` block
(`runner.go:295-311`), which logs the panic and calls
`reportStateFailed`. Because both modes reuse the same `Worker`,
this behaviour is identical in go-plugin mode and coordinator mode;
it is not a coordinator-only invention.

Frame correlation, error envelopes, and request `id` numbering follow
java-sdk ADR 0002 verbatim. Re-implementing rather than reusing those
is a deliberate cost of having a separate Go runtime; the validation
fixtures keep the encoders honest.

### go-plugin mode: unchanged

When neither `--airflow-metadata` nor `--comm`/`--logs` is set, `Serve`
falls through to the existing call site:

```go
plugin.Serve(&plugin.ServeConfig{
    HandshakeConfig: shared.Handshake,
    Plugins:         plugin.PluginSet{"dag-bundle": &impl.BundleGRPCPlugin{...}},
    GRPCServer:      plugin.DefaultGRPCServer,
})
```

The handshake env var (`AIRFLOW_BUNDLE_MAGIC_COOKIE`) gates the path
the same way it does today, so an Edge Worker that execs the binary
gets exactly the same protocol it gets today. The `DagBundle` gRPC
service, the registry cache, and the worker injection in
[`impl/plugin.go:178`](../bundle/bundlev1/bundlev1server/impl/plugin.go)
are untouched. (`--airflow-metadata` itself is extended to emit the full
bundle spec per ADR 0002, but the go-plugin path does not depend on its
output.)

### Code organisation

A new internal package
`go-sdk/bundle/bundlev1/bundlev1server/impl/coord` owns the
coordinator-mode server: frame codec, log-sink handler, dag-parse
handler, task-execution handler, and the `sdk.Client` adapter that
proxies to the comm socket. It depends on a new
`go-sdk/bundle/bundlev1/serde` package for DagSerialization v3
encoding. The frame codec is small enough to keep first-party rather
than pulling a new msgpack dependency at the API surface; we use
[`github.com/vmihailenco/msgpack/v5`](https://github.com/vmihailenco/msgpack)
internally.

`bundlev1server.Serve` becomes:

```go
func Serve(bundle bundlev1.BundleProvider, opts ...ServeOpt) error {
    config.SetupViper("")
    flag.Parse()

    switch decideMode() {
    case modeMetadataDump:
        return dumpBundleMetadata(bundle)        // --airflow-metadata: full spec JSON (ADR 0002)
    case modeCoordinator:
        return coord.Serve(bundle, *commAddr, *logsAddr)   // NEW
    case modePlugin:
        return servePlugin(bundle)               // existing go-plugin default
    case modeCoordinatorUsageError:
        return ErrCoordinatorFlagsIncomplete     // partial --comm/--logs
    }
    return nil
}
```

User code (`main.go`) is the same one line:

```go
func main() { bundlev1server.Serve(&myBundle{}) }
```

## Consequences

### Capability gains

- A single binary built from one `bundlev1server.Serve` entry point now
  runs under both the Go-native Edge Worker (go-plugin) and the
  Python-native task runner via `ExecutableCoordinator`
  (msgpack-over-IPC). Authors do not pick a deployment shape at build
  time.
- The bundle artefact produced by `airflow-go-pack` (ADR 0002, as
  revised by [ADR 0004](0004-self-contained-executable-bundle.md))
  becomes spec-conformant
  ([`task-sdk/docs/executable-bundle-spec.rst`](../../task-sdk/docs/executable-bundle-spec.rst))
  without further changes, because the binary now honours
  `--comm=<addr>`/`--logs=<addr>` as the spec demands.
- Mixed-language pipelines (Python `@task.stub` DAGs delegating to a Go
  task) work without a Go worker on the executor host — the same
  coordinator the Java SDK rides on now carries Go.

### Compatibility

- The go-plugin path is unchanged at the wire and at the source level.
  Existing Edge Worker deployments do not need to be rebuilt or
  reconfigured. The protocol selector keys off CLI flags and the
  go-plugin magic-cookie env var, both of which the Edge Worker
  already sets.
- `--airflow-metadata` remains the only introspection flag; extending it
  to emit the full bundle spec (ADR 0002) is additive and does not affect
  the go-plugin path. Adding a binary with this ADR's changes into an
  older Edge Worker deployment is safe; adding an older binary into an
  `ExecutableCoordinator` deployment fails fast with a clear "unknown
  flag: --comm" stderr message rather than hanging.

### New ongoing costs

- The Go SDK now owns a second wire protocol. Encoder drift between
  Python's `SerializedDAG.serialize_dag` and the Go `serde` package is
  the largest maintenance hazard. We mitigate it by sharing
  `validation/serialization/test_dags.yaml` with the Java SDK and
  running the same `compare.py` step in CI for Go output.
- The Task SDK message catalogue (`GetConnection`, `GetVariable`,
  `GetXCom`, `SetXCom`, `SucceedTask`, `TaskState`,
  `ConnectionResult`, `VariableResult`, `XComResult`, `ErrorResponse`,
  `StartupDetails`, `DagFileParseRequest`, `DagFileParsingResult`) is
  duplicated from the Java SDK's Kotlin definitions. Schema changes on
  the Python side need both SDKs updated together; a single
  `task-sdk/protocol/` JSON-schema source of truth is a reasonable
  follow-up but is out of scope here.
- A new transitive dependency on `vmihailenco/msgpack/v5`. It is
  pure-Go and stable; the cost is acceptable.
- The `sdk.Client` interface gains a second backend (comm-socket).
  Tests that previously injected a fake `sdk.VariableClient` (see
  [`example/bundle/main_test.go`](../example/bundle/main_test.go)) keep
  working unchanged — the swap is below the SDK surface.

### Out of scope

- The logs channel format. We emit JSON-line records to match the Java
  SDK; a richer protocol (severity-aware framing, attachment of trace
  ids) is deferred until the Python supervisor side standardises one.
- OTel context propagation. The `context_carrier` field on
  `TaskInstance` is still TODO in
  [`impl/plugin.go:151`](../bundle/bundlev1/bundlev1server/impl/plugin.go#L151)
  and remains TODO in coordinator mode for now.
- A Go-side equivalent of the Java SDK's `Supervisor.kt` (the
  no-Python-in-the-loop execution path). The Edge Worker already fills
  that role for Go via go-plugin; we do not need a second one.

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

# Apache Airflow Go Task SDK

The Go SDK is a Go implementation of the Airflow Task SDK. It lets you write task functions in Go that
have native access to the Airflow "model" (Variables, Connections, and XCom), instead of writing them in
Python.

It is built on the Task Execution Interface (TEI, a.k.a. the Task API) introduced by AIP-72 in Airflow
3.0.0. AIP-72 standardised how a task runtime talks to Airflow over an HTTP Execution API, which decoupled
the language a task is written in from the Airflow core. The Go SDK is one such runtime; the Java SDK is
another.

> [!WARNING]
> This is an **experimental** feature. The SDK is under active development and its APIs, wire protocols,
> and tooling may change between releases without notice.

## The compiled-language constraint

Python tasks are imported and run in-process. Go is compiled, so the model is different.

A single binary that bundles one or more Dags' task functions is called a **bundle**. You build one with
the SDK's packer, `airflow-go-pack`, which compiles your code and appends a metadata footer (the manifest
of `dag_id`s and `task_id`s, plus the Dag source) to the executable. The result is a **self-contained
executable bundle**: a single runnable file that *is* the bundle, with no separate manifest or archive to
ship alongside it.

## You still need a Python stub Dag (for now)

The Task API does not yet carry Dag *structure* for non-Python languages, so the Dag's shape and task
dependencies are still declared in a small Python file using `@task.stub`:

```python
from airflow.sdk import dag, task


@task.stub(queue="golang")
def extract(): ...


@task.stub(queue="golang")
def transform(): ...


@dag()
def simple_dag():
    extract() >> transform()


simple_dag()
```

`@task.stub` tells the Dag parser the "shape" of the Go tasks (their names and dependencies) without any
Python implementation. The `queue=` value routes the task to the Go runtime. This Python requirement is a
known limitation.


## Authoring a bundle

Implement `bundlev1.BundleProvider`, register your Dags and tasks, and `main` is one line. From
[`example/bundle/main.go`](./example/bundle/main.go):

```go
type myBundle struct{}

var _ v1.BundleProvider = (*myBundle)(nil)

func (m *myBundle) GetBundleVersion() v1.BundleInfo {
    return v1.BundleInfo{Name: bundleName, Version: &bundleVersion}
}

func (m *myBundle) RegisterDags(dagbag v1.Registry) error {
    simpleDag := dagbag.AddDag("simple_dag")
    simpleDag.AddTask(extract)
    simpleDag.AddTask(transform)
    return nil
}

func main() {
    if err := bundlev1server.Serve(&myBundle{}); err != nil {
        log.Fatal(err)
    }
}
```

A task is an ordinary Go function. The runtime inspects its signature and injects arguments by type:
`sdk.TIRunContext`, `*slog.Logger`, and an `sdk.Client` (or a narrower interface such as
`sdk.VariableClient`). An optional `(any, error)` return becomes the task's XCom; an `error` return marks
the task failed.

Any other parameter is a **data parameter**: in declaration order, data parameters receive the
positional arguments of the Python stub Dag's TaskFlow call. A JSON-serializable literal in the Dag
file (`transform("uk", ...)`) decodes straight into the parameter; an upstream task output
(`transform(..., extract())`) is pulled from that task's XCom in the current dag run and decoded into
the parameter's type. The runtime fails the task loudly when the argument count doesn't match the
number of data parameters or a declared type can't bind to the Go type. Data parameters must be
JSON-decodable (no func/chan/unsafe-pointer, no non-empty interfaces) — checked once at registration.
TaskFlow argument binding arrives over the coordinator protocol, so it is coordinator-mode only today;
on the Edge Worker path a task with data parameters fails with the arity error.

```go
func extract(ctx sdk.TIRunContext, client sdk.Client, log *slog.Logger) (any, error) {
    conn, err := client.GetConnection(ctx, "test_http")
    // ... do work, honour ctx cancellation ...
    return map[string]any{"go_version": runtime.Version()}, nil
}

// The stub Dag calls transform("uk", extract()): "uk" binds onto country and
// extract's return-value XCom is pulled into extracted.
func transform(
    ctx sdk.TIRunContext, client sdk.VariableClient, log *slog.Logger,
    country string, extracted map[string]any,
) error {
    val, err := client.GetVariable(ctx, "my_variable")
    if err != nil {
        return err
    }
    log.Info("Obtained variable", "my_variable", val, "country", country)
    return nil
}
```

Asking for the narrowest interface a task needs (e.g. `sdk.VariableClient` instead of `sdk.Client`) makes
unit testing easier and documents which Airflow features the task touches. `RegisterDags` is the single
source of truth for which `dag_id`s and `task_id`s a bundle can run.

### Reading the task runtime context

Declare an `sdk.TIRunContext` parameter on a task to read the identifiers and scheduling timestamps of the
running task instance and its Dag run -- the Go equivalent of the execution context the Python and Java SDKs
expose. It is an interface that embeds `context.Context`, so the same `ctx` drives cancellation and client
calls. The runtime binds it by type, just like the other injected parameters:

```go
func extract(ctx sdk.TIRunContext, log *slog.Logger) (any, error) {
    ti := ctx.TaskInstance()
    log.Info("running",
        "dag_id", ti.DagID,
        "run_id", ti.RunID,
        "task_id", ti.TaskID,
        "try_number", ti.TryNumber,
        "logical_date", ctx.DagRun().LogicalDate,
    )
    return nil, nil
}
```

`ctx.TaskInstance()` returns `DagID`, `RunID`, `TaskID`, `MapIndex` (nil for an unmapped task), and
`TryNumber`; `ctx.DagRun()` returns `DagID`, `RunID`, and the `*time.Time` fields `LogicalDate`,
`DataIntervalStart`, and `DataIntervalEnd` (nil when the run has no such value, e.g. a manual trigger).

## Deployment modes

A bundle can run in two ways. The same bundle binary works in both; you pick one per deployment:

1. **Coordinator** (recommended)
2. **Edge Worker**

For the protocol details behind each, see [How it works](#how-it-works).

### Coordinator (recommended)

A Python task runner executes the Go task directly, with no separate Go worker process to run on the host.
This is the same coordinator mechanism the Java SDK uses.

**Why this is recommended:** the mature Python supervisor handles the Airflow-facing concerns, so this path
inherits its capabilities (remote task logs to S3/GCS, the full range of task states, and alternate XCom
backends) rather than reimplementing them in Go. These are exactly the features the Edge Worker path is
still missing (see [Known limitations](#known-limitations)).

#### Quickstart

- Build and pack your bundle with `airflow-go-pack`. The packer compiles the bundle and appends an
  embedded metadata footer so the coordinator can read its `dag_id`s without executing the binary,
  producing a single runnable file:

  ```bash
  go tool airflow-go-pack ./example/bundle -- -trimpath -tags=prod
  ```

  Use `--output <path>` to write the packed bundle straight into a directory the coordinator scans
  (`executables_root`), and pass extra `go build` flags after `--`.

  For cross-compiling (e.g. deploy to a Linux host from an Apple-silicon (darwin/arm64) machine), pass `--goos`/`--goarch` and the
  packer cross-builds for you:

  ```bash
  go tool airflow-go-pack --goos linux --goarch amd64 \
    --output ~/airflow/executable-bundles/sample-dag-bundle \
    ./example/bundle
  ```

  Alternatively, use `--executable`/`--source`. The packer normally execs the binary to read
  its metadata; a cross-compiled binary cannot run on the host, so generate the metadata on a machine that
  can run it and pass the file with `--airflow-metadata`:

  ```bash
  # on linux/amd64 machine:
  go build -o my-bundle ./example/bundle
  ./my-bundle --airflow-metadata > airflow-metadata.yaml

  # on darwin/arm64 machine:
  go tool airflow-go-pack --executable ./my-bundle --source main.go --airflow-metadata airflow-metadata.yaml
  ```

  > [!NOTE]
  > The packer ships via the Go 1.24 `tool` directive, so there is no global install: add
  > `tool github.com/apache/airflow/go-sdk/cmd/airflow-go-pack` to your bundle module's `go.mod` and run
  > it with `go tool airflow-go-pack`. This pins the packer version per project.

- Register the coordinator and route the queue to it, under `[sdk]` in `airflow.cfg` (or the equivalent
  `AIRFLOW__SDK__*` env vars):

  ```ini
  [sdk]
  coordinators = {"go": {"classpath": "airflow.sdk.coordinators.executable.ExecutableCoordinator", "kwargs": {"executables_root": ["~/airflow/executable-bundles"]}}}
  queue_to_coordinator = {"golang": "go"}
  ```

  `executables_root` is one or more directories the coordinator scans for bundles; `queue_to_coordinator`
  routes stub tasks with `queue="golang"` to this Go coordinator.

  > [!IMPORTANT]
  > The coordinator is part of the Airflow worker, so the `[sdk]` config (and the bundle files in
  > `executables_root`) only need to be present wherever tasks actually execute. With `CeleryExecutor`,
  > setting it on the Celery workers is sufficient. With `LocalExecutor`, tasks run inside the scheduler
  > process, so it must be set where the scheduler can read it. The API server and Dag processor do not
  > need it.

- Deploy the matching Python stub Dag (above) into Airflow. There is no separate Go worker to run: the
  Airflow worker forks the bundle binary once per task instance.

### Edge Worker (go-plugin)

A long-running Go worker process (`airflow-go-edge-worker`) polls Airflow for work and runs your bundle,
with no Python in the data path. This path runs end-to-end today, but is missing the features listed under
[Known limitations](#known-limitations).

#### Quickstart

- See [`example/bundle/main.go`](./example/bundle/main.go) for an example Dag bundle.

- Compile it into a binary:

  ```bash
  go build -o ./bin/sample-dag-bundle ./example/bundle
  ```

  (or see the [`Justfile`](./example/bundle/Justfile) for how to build it and set the bundle version at
  build time.)

- Configure the Go edge worker by editing `$AIRFLOW_HOME/go-sdk.yaml`. The ports below are the defaults
  assuming Airflow runs locally via `airflow standalone`; tweak the ports and secrets to match your setup:

  ```yaml
  edge:
    api_url: "http://0.0.0.0:8080/"

  execution:
    api_url: "http://0.0.0.0:8080/execution/"

  api_auth:
    # This needs to match the value from the same setting in your API server for Edge API to function
    secret_key: "hPDU4Yi/wf5COaWiqeI3g=="

  bundles:
    # Which folder to look in for pre-compiled bundle binaries
    folder: "./bin"

  logging:
    # Where to write task logs to
    base_log_folder: "./logs"
    # Secret key matching airflow API server config, to only allow log requests from there.
    secret_key: "u0ZDb2ccINAbhzNmvYzclw=="
  ```

  You can also set these options via environment variables of `AIRFLOW__${SECTION}__${KEY}`, for example
  `AIRFLOW__API_AUTH__SECRET_KEY`.

- Install the worker:

  ```bash
  go install github.com/apache/airflow/go-sdk/cmd/airflow-go-edge-worker@latest
  ```

- Run it:

  ```bash
  airflow-go-edge-worker run --queues golang
  ```

- Deploy the matching Python stub Dag (above) into Airflow.

## Known limitations

A non-exhaustive list of features the **Edge Worker (go-plugin)** path has yet to implement. These are the
main reason the coordinator-based path is recommended: in that mode the Python supervisor handles these
concerns, so they are not limitations there.

- Putting tasks into states other than success or failed/up-for-retry (deferred,
  failed-without-retries, etc.).
- Remote task logs (i.e. S3/GCS etc.).
- XCom reading/writing through non-default XCom backends.

## How it works

The same bundle binary speaks two different protocols; which one it uses is decided at launch by the CLI
flags it was invoked with. User code (`func main`) is identical either way.

### Coordinator protocol

```
Python supervisor / task runner
        │  finds + validates the bundle, then forks it:
        ▼
  <bundle binary> --comm=127.0.0.1:P1 --logs=127.0.0.1:P2
        │  binary dials BACK over TCP loopback (msgpack-over-IPC)
        ▼
  GetConnection / GetVariable / GetXCom / SetXCom ... → SucceedTask / TaskState
```

- The Python `ExecutableCoordinator` forks the bundle binary with `--comm`/`--logs` addresses it is already
  listening on. The binary dials back (it never listens) and speaks a length-prefixed msgpack-over-IPC wire
  protocol on the comm socket, with structured JSON-line logs on the logs socket.
- The Python runtime is the worker. It proxies every `GetConnection` / `GetVariable` / `GetXCom` /
  `SetXCom` call through to the Execution API. The Go binary just runs the task function.

The Go side of the protocol is implemented in `pkg/execution/`. On the Python side it is the
`ExecutableCoordinator` in `task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py`.

### Edge Worker protocol

```
Airflow scheduler ──Edge Executor API──► airflow-go-edge-worker ──go-plugin/gRPC──► bundle binary
   (ExecuteTaskWorkload)                  (long-running Go process)                  (child process)
```

- `airflow-go-edge-worker` is a long-running Go process. It registers with the scheduler, polls the Edge
  Executor API for `ExecuteTaskWorkload`s, and heartbeats.
- For each workload it execs the bundle binary as a child and connects over HashiCorp
  [`go-plugin`](https://github.com/hashicorp/go-plugin) (gRPC over a handshake-gated socket).
- The Task API itself has no way to deliver an `ExecuteTaskWorkload` to a Go worker, so the Edge Executor
  API fills that gap. Longer term that API will likely need stabilising and versioning.

## Regenerating the coordinator-protocol models

The types in `pkg/execution/genmodels/` are generated from the in-tree supervisor schema snapshot
(`task-sdk/src/airflow/sdk/execution_time/schema/schema.json`); do not edit them by hand. To move the
SDK to a newer schema version:

1. Set `SupervisorSchemaVersion` in [`pkg/execution/messages.go`](./pkg/execution/messages.go) to the
   snapshot's `api_version` date.
2. Run `just generate-models`.

`TestSupervisorSchemaVersionMatchesSnapshot` fails when the constant and the snapshot's `api_version`
drift, so a missed bump is caught by `go test` instead of needing a dedicated prek hook.

## Architectural decisions

The [`adr/`](./adr) directory records the design decisions behind the SDK:

- [ADR 0001](./adr/0001-bundle-packing-options.md): bundle-packing options.
- [ADR 0002](./adr/0002-use-go-tool-directive-for-bundle-packer.md): deliver the bundle packer via the
  Go 1.24 `tool` directive.
- [ADR 0003](./adr/0003-coordinator-protocol-msgpack-ipc.md): dual-mode coordinator protocol, where one
  binary speaks both go-plugin gRPC (Edge Worker) and msgpack-over-IPC (Python coordinator).
- [ADR 0004](./adr/0004-self-contained-executable-bundle.md): the self-contained executable bundle, where
  the executable *is* the bundle.

The normative, language-agnostic on-disk bundle format (the footer layout, manifest fields, and what the
`ExecutableCoordinator` reads) is specified in
[`executable-bundle-spec.rst`](https://github.com/apache/airflow/blob/main/task-sdk/docs/executable-bundle-spec.rst).
`airflow-go-pack` produces bundles conforming to that spec.

## Future Direction

This is more of an "it would be nice to have" than any plan or commitment, and a place to record ideas.

- Defining the whole Dag in the Go SDK, so the Python stub Dag is no longer required and a bundle's
  structure and task dependencies can be declared natively in Go.
- The ability to run Airflow tasks "in" an existing code base, i.e. defining an Airflow task function that
  runs (in a goroutine) inside an existing app.
- Doing the task function reflection ahead of time, rather than for each Execute call.

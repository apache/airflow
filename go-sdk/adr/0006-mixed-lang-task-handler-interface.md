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

# 6. Bundle registration and Mixed Lang task handlers

Date: 2026-09-09

## Status

Proposed.

## Decision

1. **A "bundle" is a value the author builds.** `airflow.Bundle()` returns a `*airflow.BundleRef`;
   `main` reads build, register, serve, with `bundle.Serve()` as its last statement.
   It replaces `BundleProvider` and `Registry`, the callback and the write half of the same bundle.
2. **`bundle.Register(items ...airflow.Registraterable)`** is the single registration verb, taking native Dags and task handlers.
3. **A Go bundle registers task handlers, not Dags**: `airflow.TaskHandler(dagId, taskId, fn)`, the Go body for a task Python declares with `@task.stub`.
4. **Both dag_id and task_id are written out on TaskHandler definition**, because Python owns them; nothing is derived from the Go function name.
5. **Every handler must take an `airflow.Context` first**: a struct embedding `context.Context`, exposing `Logger()`, `Client()`, `TaskInstance()`, and `DagRun()`.
   What Airflow supplies a task arrives as a method on that value rather than as a parameter of its own.
6. **Every remaining parameter is data**, bound positionally, or by field when it is a single struct: `arg:"..."` when tagged, else the folded Go field name.

## Context

Python owns everything but the body of a Mixed Lang task: `@task.stub` declares the task, its arguments, and its place in the graph.
The Go side has no Dag to define, so Dag vocabulary misleads.

Renaming the Go function must not change which task body Airflow matches, so `TaskHandler` names the dag_id and the task_id explicitly instead of inferring them from the Go function name.
Additionally, the TaskHandler shouldn't accept any spec as it should only define the implementation of stub operator, so the `airflow.TaskHandler(dag_id, task_id, fn)` is a much cleaner interface.

Registration is inverted today. An author declares a struct with no state, asserts it implements
`v1.BundleProvider`, fills in `RegisterDags(dagbag v1.Registry) error`, and hands the struct to
`bundlev1server.Serve` — three concepts and an empty type before a single task is declared.

The term naming should be refined to reduce the new terminologies across user interface.
The `Registry` should be `Bundle` and the `AddDag` is mis-used for registering the TaskHandler.

The shipped signature (#70209) injects `sdk.TIRunContext`, `*slog.Logger`, and `sdk.Client` by type.
Calling them still needs the `context.Context` passed in by hand, which is awkward from a Go author's perspective.
Exposing the logger and the client on the context itself removes that, and `airflow.Context` in the Signature section below is that shape.
Both keep taking a context, so a call reads `actx.Logger().InfoContext(actx, ...)` or `actx.Client().GetVariable(actx, ...)` and neither holds one in a field.

## Example

```go
func main() {
    bundle := airflow.Bundle()

    bundle.Register(
        airflow.TaskHandler("py_etl", "transform", transform),
        airflow.TaskHandler("py_etl", "via_struct_arg_tag", ViaStructArgTag),
        airflow.TaskHandler("py_etl", "via_struct", ViaStruct),
    )

    if err := bundle.Serve(); err != nil {
        log.Fatal(err)
    }
}
```

Registration can be spread across packages, either by passing the bundle along or by returning `[]airflow.Registraterable` for the caller: `bundle.Register(taskflowbinding.Handlers()...)`.

Three ways a Go function receives a stub task's data, all live in `go-sdk/example/bundle/`.

**Flat positional**, for `def transform(country: str, extracted: dict)`:

```go
func transform(actx airflow.Context, country string, extracted map[string]any) error {
    actx.Logger().InfoContext(actx, "transforming", "country", country, "try", actx.TaskInstance().TryNumber)

    threshold, err := actx.Client().GetVariable(actx, "etl_threshold")
    if err != nil {
        return err
    }
    return writeRows(actx, extracted, threshold)
}
```

**A single `arg:`-tagged struct**, for `def via_struct_arg_tag(region_code: str, threshold: float)`:

```go
type ViaStructArgTagInput struct {
    Region    string  `arg:"region_code"`
    Threshold float64 `arg:"threshold"`
}

func ViaStructArgTag(actx airflow.Context, input ViaStructArgTagInput) (any, error)
```

**A single untagged struct**, where the field name folds to the Python argument, for
`def via_struct(region_code: str, threshold: float)`:

```go
type ViaStructInput struct {
    RegionCode string  // folds to region_code
    Threshold  float64 // folds to threshold
}

func ViaStruct(actx airflow.Context, input ViaStructInput) (any, error)
```

Folding lowercases a name and strips its underscores, on both sides: `RegionCode` and `region_code`
both fold to `regioncode`.

## Signature

```go
package airflow

func Bundle() *BundleRef

func (b *BundleRef) Register(items ...Registraterable)
func (b *BundleRef) Serve() error

// Registraterable is sealed: its only method is unexported, so the set of things a bundle
// accepts stays closed to the SDK's own types — task handlers today, a Dag authored in Go
// once there is one.
type Registraterable interface{ registraterable() }

func TaskHandler(dagId, taskId string, fn any) Registraterable

// Context is what every handler takes first. It is a context, so it passes straight to the
// logger and the client rather than being stored inside either of them.
type Context struct {
    context.Context
    // unexported fields
}

func (c Context) Logger() *slog.Logger
func (c Context) Client() sdk.Client
func (c Context) TaskInstance() TaskInstance
func (c Context) DagRun() DagRun

// FromContext recovers the SDK surface inside a helper typed as a plain context.Context.
func FromContext(ctx context.Context) (Context, bool)
```

## Consequences

- **Registration closes when `Serve` is called.** Registering afterwards is a programming error and panics.
- Graceful termination needs no unwrapping — `actx.Done()` fires on supervisor shutdown, and
  `http.NewRequestWithContext(actx, ...)` accepts it — while cleanup that must outlive cancellation
  uses `context.WithoutCancel(actx)`.

## Alternatives

- **`airflow.TaskHandler(dagId, fn, airflow.WithTaskId(...))`**, defaulting the task_id to the Go
  function name. Rejected: see the ids in Context above.
- **Package-level accessors over a plain `context.Context`** (`airflow.Logger(ctx)`, `airflow.Client(ctx)`), leaving the handler's first parameter as `context.Context`.
  Rejected: `airflow.Logger(ctx)` reads oddly next to `actx.Logger()`, asking the package for something the context already holds.
  It also keeps the SDK surface in package functions instead of on the value, and a context from anywhere else still compiles, so a missing logger or client only shows up when the task runs.
- **A logger and a client that hold the context themselves**, so a call reads `actx.Logger().Info(...)` with nothing passed. Rejected: that is what puts a request-scoped context in a struct field, which the context package warns against.
  `airflow.Context` carrying one is a different case, since it is a purpose-built context rather than a domain type, which is why the context itself stays a struct.
- **Two registration verbs**, one per registerable kind. Rejected: Having `bundle.registerTaskHandler(airflow.TaskHandler(...))` spell the exact term twice, having a sealed type is a much cleaner interface.

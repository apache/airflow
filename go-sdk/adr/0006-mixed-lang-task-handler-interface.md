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

Proposed. Keeps the argument binding shipped in #70209, replaces the provider-and-registry
registration flow around it, and reshapes the task signature, following the design review on #72043.

## Decision

1. **A bundle is a value the author builds.** `airflow.Bundle()` returns a `*airflow.BundleRef`; `main` reads build, register, serve, with `bundle.Serve()` as its last statement.
2. **`bundle.Register(items ...airflow.Registraterable)`** is the single registration verb, taking native Dags and task handlers.
3. **A Go bundle registers task handlers, not Dags**: `airflow.TaskHandler(dagId, taskId, fn)`, the Go body for a task Python declares with `@task.stub`.
4. **task_id is always written out**; nothing is derived from the Go function name.
5. **Every handler takes an `airflow.Context` first**: a struct embedding `context.Context`, exposing `Logger()`, `Client()`, `TaskInstance()`, and `DagRun()`.
6. **Every remaining parameter is data**, bound positionally, or by field when it is a single struct: `arg:"..."` when tagged, else the folded Go field name.

## Context

Python owns everything but the body of a Mixed Lang task: `@task.stub` declares the task, its arguments, and its place in the graph.
The Go side has no Dag to define, so Dag vocabulary misleads.

Registration is inverted today. An author declares a struct with no state, asserts it implements
`v1.BundleProvider`, fills in `RegisterDags(dagbag v1.Registry) error`, and hands the struct to
`bundlev1server.Serve` — three concepts and an empty type before a single task is declared. The two
names are also one object: `Registry` is `Bundle` plus `AddDag`, the write side of the value that
later answers task lookups at execution time.

The shipped signature then injects `sdk.TIRunContext`, `*slog.Logger`, and clients by type in any position, but the user still need to pass the injected `sdk.TIRunContext` to invoke the client and the logger. One required context carrying the logger and the client on it fixes the issue.

## Example

```go
func main() {
    bundle := airflow.Bundle()

    bundle.Register(
        airflow.TaskHandler("py_etl", "transform", transform),
        airflow.TaskHandler("py_etl", "via_struct_arg_tag", ViaStructArgTag),
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
    actx.Logger().Info("transforming", "country", country, "try", actx.TaskInstance().TryNumber)

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

**A single untagged struct**, where the field name folds to the Python argument: `RegionCode` lowercased with underscores stripped is `regioncode`, which matches `region_code`.

// comment: we should give the code example as well

## Consequences

- `BundleProvider`, the empty provider struct, and the `bundlev1server` package all disappear from an
  author's `main`; `Serve` becomes a method on the value they already hold.
- **Registration closes when `Serve` is called.** Registering afterwards is a programming error and
  panics, like every other registration-time check in these ADRs.
- Every existing call site changes, acceptable only because the SDK's README already warns its APIs
  "may change between releases without notice."
- Requiring `airflow.Context` turns a class of mistake into a compile error: a test calling
  `myTask(context.Background(), ...)` no longer builds, where a package-level `airflow.Logger(ctx)`
  accessor over a plain context would have compiled and then failed at run time on a missing value.
- Graceful termination needs no unwrapping — `actx.Done()` fires on supervisor shutdown, and
  `http.NewRequestWithContext(actx, ...)` accepts it — while cleanup that must outlive cancellation
  uses `context.WithoutCancel(actx)`.
- A helper typed as a plain `context.Context` recovers the methods with
  `airflow.FromContext(ctx) (airflow.Context, bool)`.



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

# 7. Native Dag interface

Date: 2026-08-24

## Status

Proposed. Documents the interface proposed in #67155 and #70158; neither has merged, and nothing in the example below exists on `main` today.

## Why

The Go SDK's shipped `Registry.AddDag(dagId string) Dag` only supports the Mixed Lang interface: it matches a Python-owned stub Dag by `dag_id` and offers `AddTask(fn)` / `AddTaskWithName(taskId, fn)`, with no Dag-level schedule, tags, or dependency wiring at all. [ADR 6](0006-mixed-lang-dag-interface.md) proposes renaming it to `AddMixedLangDag`, freeing the `AddDag` name for this ADR, since Go doesn't allow two differently-signed methods sharing one name on the same interface.

Two open PRs, #67155 and #70158, propose a second mode where the graph itself is authored in Go. Task registration needs to express typed data dependencies between Go functions without falling back to string task-ID lookups. For `Task` and `Inputs` alone, a cycle should have no syntax to write at all, not merely a validator that rejects it. This ADR meets that goal for `Task` and `Inputs`, then has to revisit it once `Then` is added below.

## Example

Proposed in #70158, building on #67155.

```go
dag := registry.AddDag(v1.DagSpec{DagId: "etl", Schedule: "@daily"})

extracted := dag.Task(nativeExtract)
transformed := dag.Task(nativeTransform, v1.Inputs(extracted))
loaded := dag.Task(nativeLoad, v1.Inputs(transformed), v1.TaskSpec{Retries: 2})
loaded.Then(dag.Task(cleanupTemp))
```

```go
func nativeExtract(log *slog.Logger) (NativeResult, error) {
    log.Info("extracting native Dag data")
    return NativeResult{Message: "native Dag data"}, nil
}

func nativeTransform(log *slog.Logger, extracted NativeResult) (NativeResult, error) {
    log.Info("transforming native Dag data", "message", extracted.Message)
    return NativeResult{Message: "transformed " + extracted.Message}, nil
}

func nativeLoad(log *slog.Logger, transformed NativeResult) error {
    log.Info("loading native Dag data", "message", transformed.Message)
    return nil
}

func cleanupTemp(log *slog.Logger) error {
    log.Info("cleaning up temp files")
    return nil
}
```

`Registry.AddDag(spec)` would return a `Dag`, and `Dag.Task(fn, opts...)` would register a task and return a `*TaskRef`. `v1.Inputs(refs...)` would feed an upstream's return value into the next task's data parameters, positionally, after any injectables. For anything that wraps a Go function, wiring the value and declaring the dependency are the same call, matching Python TaskFlow's call-argument graph.

PR #70158's own code also exposes a separate `After(refs...)` for order-only edges. This ADR proposes dropping that option, but keeps the need it named: `TaskRef.Then(others ...*TaskRef) *TaskRef` covers order-only edges instead, for cases with no Go function parameter to bind an ignored `Inputs` value into, like [ADR 8](0008-taskgroup-shortcircuit-branch.md)'s `TriggerDagRunOperator`. A bare `v1.TaskSpec{}` value would itself be a `TaskOption`.

None of this exists in the shipped SDK. `main`'s `Dag` only has `AddTask(fn)` / `AddTaskWithName(taskId, fn)`, built for the Mixed Lang case where a Python stub already owns the graph.

## How

- `Dag.Task(fn, opts...)` would classify `fn`'s parameters with `reflect`, in the same injectable-then-data order as the Mixed Lang interface. It does this through its own `isInjectable` check (`go-sdk/bundle/bundlev1/task.go` in #70158), which duplicates the same four type checks (`sdk.TIRunContext`, `context.Context`, `*slog.Logger`, `sdk.Client`) that `binding.go`'s `classifyParam` already runs for the shipped Mixed Lang path. The two checks are separate implementations, not a shared one.
- `v1.Inputs(refs...)` would pair its arguments positionally against `fn`'s data parameters, checking each `TaskRef`'s recorded output type (`TaskRef.out`, a `reflect.Type`) against the parameter it binds. A mismatched count or type panics at Dag-registration time rather than surfacing at task run time.
- `TaskRef.Then(others ...*TaskRef) *TaskRef` records each of `others` as downstream of the receiver and returns the receiver, not an argument: `choose.Then(a, b)` fans `choose` out to both `a` and `b` in one call, and `choose.Then(a).Then(b)` reaches the same result across two calls. That matches Python's list-broadcast form, `cond >> [task1, task2]`, rather than the sequential chain form `a >> b >> c` (`self.set_downstream(other); return other`, `task-sdk/src/airflow/sdk/definitions/_internal/mixins.py:97-100`): once a single call can fan out to more than one downstream task, there's no single "next" `*TaskRef` left to hand back. Unlike `Inputs`, `Then` never touches the callees' parameters; it exists purely to draw edges.
- `Then` is edge-only and isn't combined with `Inputs` for the same downstream task: a task wired through `Then` takes no parameter for that edge at all. For now, that lands on [ADR 8](0008-taskgroup-shortcircuit-branch.md)'s `ShortCircuitOperator` and `BranchOperator`, whose downstream candidates wire through `Then` rather than an `Inputs`-bound parameter the function only ignored.
- A `*TaskRef` only exists once its producing `dag.Task(...)` call has returned, and Go statements evaluate in order, so `Inputs` can only reference an already-registered task: a cycle has no syntax to be written through `Inputs` alone, since an edge can only point backward from a task to one of its own already-registered ancestors-to-be.
  `Then` doesn't carry that guarantee. It links two `*TaskRef`s that both already exist, in whichever direction the receiver and argument are written, so `b.Then(a)` is exactly as legal as `a.Then(b)`, even after `a` already has an `Inputs`-based edge into `b`. For example, `a := dag.Task(A); b := dag.Task(B, v1.Inputs(a)); b.Then(a)` produces a genuine cycle, a→b→a, entirely in syntax this proposal accepts. Introducing `Then` reopens the cycle question ADR 7's `Inputs`-only design closed; a Dag-registration-time cycle check would be needed once `Then` exists.

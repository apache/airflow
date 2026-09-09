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

Date: 2026-09-07

## Status

Proposed. Supersedes the interfaces in #67155 and #70158, reshaped by the design review on #72043.
Nothing below exists on `main`.

## Decision

1. **One Dag type, constructed then registered.** `airflow.Dag(spec)` returns a `*airflow.DagRef`
   that is complete before `bundle.Register(dag)` takes it — the same verb that registers Mixed Lang
   task handlers ([ADR 6](0006-mixed-lang-task-handler-interface.md)). Naming rule:
   `airflow.X(...)` constructs, `*airflow.XRef` is the handle.
2. **Tasks register through `dag.Task(fn any, opts ...airflow.TaskOption)`**, returning a
   `*airflow.TaskRef`. `airflow.Inputs(...)` and a bare `airflow.TaskSpec{}` both satisfy
   `TaskOption`, which is how one variadic carries both.
3. **`airflow.Inputs(refs...)` is data and an edge at once**, binding positionally after the context.
4. **`Before` and `After` are order-only edges on `airflow.Node`**, which both `*airflow.TaskRef`
   and `*airflow.TaskGroupRef` satisfy, so a task and a whole group are equally an edge endpoint.
   They are the Go pair for `>>` and `<<`, both variadic so a single call fans out.
5. **Trigger rules belong to the task**, as `airflow.TaskSpec{TriggerRule: ...}`, never to an edge.
6. **Everything an author writes comes from one `airflow` package.**
7. **No Go-native deferral**, and none is needed: the constructs that defer are DSL tasks Python
   executes.

## Context

A native Dag is authored entirely in Go — schedule, tasks, and dependencies — and serializes into the
Dag JSON a Python Dag would produce. There is one Dag type, as in Python; the Mixed Lang case
registers task handlers instead ([ADR 6](0006-mixed-lang-task-handler-interface.md)) because it
defines no Dag. Dependencies between Go functions have to be typed rather than looked up by task ID,
and a Dag should read like Go rather than transliterated Python. The proposed interface spread its
surface across `v1`, `sdk`, and `slog`, published a half-built Dag to the registry and mutated it
afterwards, and could declare an edge in only one direction.

## Example

Both forms build the same graph; which one an author writes depends on whether the edge carries a
value.

**Data dependencies — the TaskFlow equivalent.** `airflow.Inputs` passes an upstream's return value
in and declares the edge in one call, as calling one TaskFlow function with another's output does in
Python (`extracted = extract(); transformed = transform(extracted); load(transformed)`).

```go
dag := airflow.Dag(airflow.DagSpec{DagId: "etl", Schedule: "@daily"})

extracted := dag.Task(extract)
transformed := dag.Task(transform, airflow.Inputs(extracted))
dag.Task(load, airflow.Inputs(transformed), airflow.TaskSpec{Retries: 2})

bundle.Register(dag)
```

```go
func extract(actx airflow.Context) (Result, error) {
    return Result{Message: "native Dag data"}, nil
}

func transform(actx airflow.Context, extracted Result) (Result, error) {
    return Result{Message: "transformed " + extracted.Message}, nil
}

func load(actx airflow.Context, transformed Result) error { return nil }
```

**Order-only dependencies — the `>>` and `<<` equivalent.** For tasks that must be ordered but
exchange no data; the functions take no parameter for such an edge.

```go
loaded := dag.Task(load, airflow.Inputs(transformed))
cleaned := dag.Task(cleanup, airflow.TaskSpec{TriggerRule: airflow.AllDone})
staging := dag.TaskGroup("staging") // a group carries edges like a task

loaded.Before(dag.Task(notify), cleaned) // loaded >> [notify, cleanup]
cleaned.After(extracted)                 // cleanup << extracted
staging.Before(loaded)                   // staging >> load
```

## Consequences

- **A cycle check becomes necessary.** `Inputs` alone cannot express one, since a `*TaskRef` exists
  only after its own `dag.Task(...)` returns. `Before`/`After` link two existing refs in either
  direction, so `b := dag.Task(B, airflow.Inputs(a)); b.Before(a)` is a genuine cycle in accepted
  syntax, and registration has to reject it. The check has to see through groups, since a group edge
  stands for edges into and out of every task the group holds.
- **A count or type mismatch panics at registration**, not at run time, because each `*TaskRef`
  carries its recorded output type.
- **A chained `Before`/`After` expression is an `airflow.Node`, not a `*TaskRef`.** Go has no
  covariant returns, so a method declared on the interface returns the interface. Take the ref from
  `dag.Task(...)` when it is needed for `Inputs`; the style above calls both verbs as statements and
  never reads the result.
- **Cancellation works even though deferral does not**: `actx.Done()` fires on supervisor shutdown
  ([ADR 6](0006-mixed-lang-task-handler-interface.md)). Concurrency primitives stay out of scope
  until a Go-native task itself needs to wait.

## Alternatives

- **Fetching upstream values at run time**, where a task reads an upstream result inside its own body
  (`result.Get(&out)`) and the graph falls out of the order the Go code executes. Rejected: Airflow
  materializes the whole graph at Dag-processing time and then invokes a single task instance's
  callable per run, so an edge existing only in execution order cannot be parsed without running the
  program to completion. `Inputs` keeps the typed outputs that style is reached for.
- **Separate `Dag` and `MixedLangDag` types.** Rejected: Python has one Dag class, and the Mixed Lang
  case is not a Dag at all ([ADR 6](0006-mixed-lang-task-handler-interface.md)).

## Appendix: Implementation Notes

- **`airflow.Node` is the Go counterpart of Python's `DAGNode`.** There,
  `TaskGroup(TaskGroupMixin, DAGNode)` (`task-sdk/src/airflow/sdk/definitions/taskgroup.py:96`) and
  every operator inherit `DependencyMixin`
  (`task-sdk/src/airflow/sdk/definitions/_internal/mixins.py:35`), which is where `set_upstream` and
  `set_downstream` — `>>` and `<<` — live. Go needs the same base for the same reason, sealed with an
  unexported method like `Registration` and `TaskOption`.
- `*DagRef` satisfies `airflow.Registration` so that one `Bundle.Register` covers Dags and task
  handlers alike; the bundle lifecycle and the sealed-interface mechanics are in
  [ADR 6](0006-mixed-lang-task-handler-interface.md).
- Go forbids a package-level func and a type sharing the name `Dag`, so one of the two has to differ;
  the constructor keeps the plain noun because Dag authors read it most. `Before` and `After` both
  return their own receiver rather than an argument, since a fan-out call has no single "next" ref to
  hand back; they replace #70158's one-directional `After(refs...)` task option.
- One signature classifier serves both interfaces. #70158's `isInjectable`
  (`go-sdk/bundle/bundlev1/task.go`) repeats what `classifyParam` (`go-sdk/pkg/binding/binding.go`)
  already does for the Mixed Lang path; a single required `airflow.Context` leaves one rule for both —
  first parameter is the context, the rest is data.
- Python's `trigger_rule` is an operator attribute and `>>` carries no rule of its own, which is why
  an edge verb that took one would let two edges into the same task disagree.

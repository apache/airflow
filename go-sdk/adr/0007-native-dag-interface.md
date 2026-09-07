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

Proposed. Supersedes the interfaces proposed in #67155 and #70158, reshaped by the design review
on #72043. Nothing below exists on `main`.

## Why

A native Dag is authored entirely in Go: the author owns the schedule, the tasks, and the
dependencies, and the SDK serializes all of it into the Dag JSON a Python Dag would produce. There is
one Dag type, as in Python — the Mixed Lang case registers task handlers instead
([ADR 6](0006-mixed-lang-task-handler-interface.md)), because it defines no Dag. Dependencies between
Go functions have to be typed rather than looked up by task ID, and a Dag should read like Go rather
than like transliterated Python.

## Example

Both examples build the same graph. Which form an author writes depends on whether the edge carries a
value.

### Data dependencies: the TaskFlow equivalent

`airflow.Inputs` passes an upstream's return value in and declares the edge in one call, the way
calling one TaskFlow function with another's output does in Python.

```go
dag := airflow.Dag(airflow.DagSpec{DagId: "etl", Schedule: "@daily"})

extracted := dag.Task(extract)
transformed := dag.Task(transform, airflow.Inputs(extracted))
dag.Task(load, airflow.Inputs(transformed), airflow.TaskSpec{Retries: 2})

registry.AddDags(dag)
```

```python
# the Python Dag this mirrors
extracted = extract()
transformed = transform(extracted)
load(transformed)
```

```go
func extract(ctx context.Context) (Result, error) {
    return Result{Message: "native Dag data"}, nil
}

func transform(ctx context.Context, extracted Result) (Result, error) {
    return Result{Message: "transformed " + extracted.Message}, nil
}

func load(ctx context.Context, transformed Result) error { return nil }
```

### Order-only dependencies: the `>>` and `<<` equivalent

`Before` and `After` draw an edge and pass nothing, for tasks that must be ordered but exchange no
data; the functions take no parameter for such an edge.

```go
loaded := dag.Task(load, airflow.Inputs(transformed))
notified := dag.Task(notify)
cleaned := dag.Task(cleanup, airflow.TaskSpec{TriggerRule: airflow.AllDone})

loaded.Before(notified, cleaned) // loaded >> [notified, cleaned]
cleaned.After(extracted)         // cleanup << extracted
```

## How

- **One user-facing package.** Everything a Dag author writes comes from `airflow`; the proposed
  interface spread the same surface across `v1`, `sdk`, and `slog`, leaking package boundaries that
  exist for the SDK's benefit, not the author's.
- **Construct, then register.** `airflow.Dag(spec)` returns a `*airflow.DagRef` that is complete
  before `registry.AddDags(dag)` hands it over, where the proposed `registry.AddDag(spec)` published
  a half-built Dag and mutated it afterwards. Naming rule: `airflow.X(...)` constructs, `*airflow.XRef`
  is the handle. Go forbids a package-level func and a type sharing the name `Dag`, so one must
  differ; the constructor keeps the plain noun because authors read it most.
- **`airflow.Inputs(refs...)` is data and an edge.** Values bind positionally after the context, and
  each `*TaskRef` carries its recorded output type, so a count or type mismatch panics at
  registration rather than at run time.
- **`Before` / `After` are edges only** — the Go pair for `>>` and `<<`. Both are variadic, so one
  call fans out (`cond >> [t1, t2]`), and both return the receiver, since a fan-out has no single
  "next" ref. This replaces #70158's one-directional `After(refs...)` task option, and leaves `Then`
  to mean only what it means in [ADR-0008](../../airflow-core/adr/lang-sdk/0008-control-flow-constructs.md).
- **Trigger rules belong to the task**, as `airflow.TaskSpec{TriggerRule: ...}`. In Python
  `trigger_rule` is an operator attribute and `>>` carries no rule; an edge verb that took one would
  let two edges into the same task disagree.
- **A cycle check is needed.** `Inputs` alone cannot express one, since a `*TaskRef` exists only
  after its own `dag.Task(...)` returns. `Before`/`After` link two existing refs in either direction,
  so `b := dag.Task(B, airflow.Inputs(a)); b.Before(a)` is a genuine cycle in accepted syntax.
- **One signature classifier.** #70158's `isInjectable` (`go-sdk/bundle/bundlev1/task.go`) repeats
  what `classifyParam` (`go-sdk/pkg/binding/binding.go`) already does for the Mixed Lang path.
  Context accessors ([ADR 6](0006-mixed-lang-task-handler-interface.md)) leave one rule for both:
  first parameter is the context, the rest is data.
- **No Go-native deferral.** A Go task runs to completion in one call. The deferrable constructs
  authors reach for first are DSL tasks Python executes
  ([ADR-0009](../../airflow-core/adr/lang-sdk/0009-provider-operators-as-generated-dsl.md)), which
  defer as they do in a Python Dag, so SDK-level goroutine and channel primitives stay out of scope
  until a Go-native task itself needs to wait.

## Alternatives

- **Fetching upstream values at run time**, where a task reads an upstream result inside its own body
  (`result.Get(&out)`) and the graph falls out of the order the Go code executes, with no edge verb at
  all. Rejected: Airflow materializes the whole graph at Dag-processing time and then invokes a single
  task instance's callable per run, so an edge that exists only in execution order cannot be parsed
  without running the program to completion. `Inputs` keeps the typed, statically declared outputs
  that style is reached for, without the Dag having to execute to be read.
- **Separate `Dag` and `MixedLangDag` types.** Rejected: Python has one Dag class, and the Mixed Lang
  case is not a Dag at all ([ADR 6](0006-mixed-lang-task-handler-interface.md)).

## Question

- Should `Before`/`After` also be methods on a task group, so a whole group can be ordered against
  another the way Python allows `group1 >> group2`? Groups otherwise share the Dag's methods
  ([ADR-0008](../../airflow-core/adr/lang-sdk/0008-control-flow-constructs.md)), but only tasks can
  carry an edge here.

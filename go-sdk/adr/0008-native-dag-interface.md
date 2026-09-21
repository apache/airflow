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

Date: 2026-09-09

## Status

Proposed.

## Decision

1. **One Dag type, constructed then registered.** `airflow.Dag(dagId, spec)` returns a `*airflow.DagRef` that is complete before `bundle.Register(dag)` takes it — the same verb that registers Mixed Lang task handlers ([ADR 7](0007-mixed-lang-task-handler-interface.md)).
   Naming rule: `airflow.X(...)` constructs, `*airflow.XRef` is the entity; what every Dag must have is a positional parameter for dag_id, and the rest travels in a spec struct.
2. **Tasks register through `dag.Task(fn any, opts ...airflow.TaskOption)`**, returning a `*airflow.TaskRef`. `airflow.Inputs(...)` and a bare `airflow.TaskSpec{}` both implement `TaskOption`.
3. **At most one `airflow.TaskSpec` per task.** A second one is a registration error rather than something to merge, so a task's attributes are only ever written in one place.
4. **task_id is the Go function name by default**, spelled exactly as the function is, and `airflow.TaskSpec{TaskId: ...}` sets it to anything else.
5. **`airflow.Inputs(refs...)` declares the data and the edge in one call** for defining graph with TaskFlow syntax.
6. **`Before` and `After` are order-only edges on `airflow.Node`**, which both `*airflow.TaskRef` and `*airflow.TaskGroupRef` satisfy. They are the Go pair for `>>` and `<<`, and both return their argument set as one `Node`, so `a.Before(b, c).Before(d)` is Python's `a >> [b, c] >> d`.
7. **An edge label wraps the endpoint**: `loaded.Before(airflow.Label(notify, "when empty"))` is Python's `loaded >> Label("when empty") >> notify`. Labelling the endpoint rather than the call lets one fan-out give each edge its own label.
8. **Trigger rules belong to the task**, as `airflow.TaskSpec{TriggerRule: ...}`, never to an edge.
9. **A user-facing enum carries its type in the constant name** — e.g. `airflow.TriggerRuleAllDone`.
10. **Everything an author writes comes from one `airflow` package.**
11. **No Go-native deferral**, and none is needed: the constructs that defer are DSL tasks Python executes.
12. **`DagSpec` and `TaskSpec` are generated from Airflow core's serialization schema** (`airflow-core/src/airflow/serialization/schema.json`) into the `airflow` package itself and committed, the way `models.gen.go` already is for the supervisor schema.
    `TaskSpec` implements `airflow.TaskOption`, so a generated struct travels in the same variadic as `airflow.Inputs`.

## Context

A native Dag is authored entirely in Go — schedule, tasks, and dependencies — and serializes into the Dag JSON a Python Dag would produce.
Dependencies between Go functions have to be typed rather than looked up by task ID, and a Dag should read like Go rather than transliterated Python.

The interfaces sketched in #67155 and #70158 spread their surface across `v1`, `sdk`, and `slog`, published a half-built Dag to the registry and mutated it afterwards, and could declare an edge in only one direction.

## Example

Both forms build the same graph; which one an author writes depends on whether the edge carries a value.

**Data dependencies — the TaskFlow equivalent.** `airflow.Inputs` passes an upstream's return value in and declares the edge in one call, as calling one TaskFlow function with another's output does in Python (`extracted = extract(); transformed = transform(extracted); load(transformed)`).

```go
dag := airflow.Dag("etl", airflow.DagSpec{Schedule: "@daily"})

extracted := dag.Task(extract)
transformed := dag.Task(transform, airflow.Inputs(extracted))
dag.Task(load, airflow.Inputs(transformed), airflow.TaskSpec{Retries: 2})

bundle.Register(dag)
```

The task functions, where `Result` is any type the SDK can serialize to XCom — the Go equivalent of what a TaskFlow function returns:

```go
type Result struct {
    Message string `json:"message"`
}

func extract(actx airflow.Context) (Result, error) {
    return Result{Message: "native Dag data"}, nil
}

func transform(actx airflow.Context, extracted Result) (Result, error) {
    return Result{Message: "transformed " + extracted.Message}, nil
}

func load(actx airflow.Context, transformed Result) error { return nil }
```

The task_ids are the function names by default: `extract`, `transform`, and `load`.

**Order-only dependencies — the `>>` and `<<` equivalent.** For tasks that must be ordered but exchange no data; the functions take no parameter for such an edge.

```go
loaded := dag.Task(load, airflow.Inputs(transformed))
cleaned := dag.Task(cleanup, airflow.TaskSpec{TriggerRule: airflow.TriggerRuleAllDone})
notified := dag.Task(notify)
emptyNotice := dag.Task(notifyEmpty, airflow.TaskSpec{TaskId: "notify_empty"})
staging := dag.TaskGroup("staging")  // a group carries edges like a task
staging.Task(stageRows)              // tasks join a group through the group

staging.Before(loaded)            // staging >> load
loaded.Before(notified, cleaned)  // loaded >> [notify, cleanup]
cleaned.After(extracted)          // cleanup << extracted

loaded.Before(airflow.Label(emptyNotice, "when empty"))  // loaded >> Label("when empty") >> notify_empty
```

## Signature

```go
package airflow

func Dag(dagId string, spec DagSpec) *DagRef

func (d *DagRef) Task(fn any, opts ...TaskOption) *TaskRef
func (d *DagRef) TaskGroup(groupId string, opts ...TaskGroupOption) *TaskGroupRef

func (g *TaskGroupRef) Task(fn any, opts ...TaskOption) *TaskRef
func (g *TaskGroupRef) TaskGroup(groupId string, opts ...TaskGroupOption) *TaskGroupRef

// DagSpec and TaskSpec are generated into this package from
// airflow-core/src/airflow/serialization/schema.json and committed.
type DagSpec struct {
    Schedule  string
    StartDate time.Time
    Catchup   bool
    Tags      []string
    // ...
}

// TaskSpec implements TaskOption, so it travels in the same variadic as Inputs.
type TaskSpec struct {
    TaskId      string // defaults to the Go function name
    Retries     int
    TriggerRule TriggerRule
    // ...
}

// TaskOption is sealed: its only method is unexported, so a task takes SDK-defined options and
// nothing else. TaskSpec and the value Inputs returns both implement it.
type TaskOption interface{ applyTask(*taskConfig) }

func Inputs(refs ...*TaskRef) TaskOption

// Node is what an edge connects. *TaskRef and *TaskGroupRef implement it, sealed the same way.
// It is the Go counterpart of Python's DAGNode / DependencyMixin.
// Before and After return their argument set as one Node, which is what makes a chain work.
type Node interface {
    Before(nodes ...Node) Node
    After(nodes ...Node) Node
    node()
}

// Label carries an edge label into Before or After. The Node it returns stands for node itself.
func Label(node Node, text string) Node

// A user-facing enum is a named string type with its type in each constant name.
type TriggerRule string

const (
    TriggerRuleAllSuccess TriggerRule = "all_success"
    TriggerRuleAllDone    TriggerRule = "all_done"
    TriggerRuleOneFailed  TriggerRule = "one_failed"
    // ... one constant per Python trigger rule
)
```

## Consequences

- **A cycle check becomes necessary.** `b := dag.Task(B, airflow.Inputs(a)); b.Before(a)` is a genuine cycle in accepted
  syntax. Either the build-time or the Dag-processing time should reject this.
- **A count or type mismatch panics at registration**, not at run time, because each `*TaskRef` carries its recorded output type.
- **An edge verb returns what it pointed at, not its receiver.** That is what makes `a.Before(b, c).Before(d)` mean `a >> [b, c] >> d`.
  Returning the receiver would read like a chain and mean a second fan-out from `a`.
- **The specs generate into the `airflow` package, not a `gen` package beside it.** An unexported method belongs to the package that declares it, so a generated type living elsewhere could not implement the sealed `TaskOption`, and a type alias cannot gain methods either.
  Generating in place is what keeps both `airflow.TaskSpec` and the seal.
- **The generated names need a mapping.** The core schema carries no `title` fields, unlike the supervisor schema `models.gen.go` reads, so its `dag` and `operator` definitions would generate as `Dag`, a name the constructor already takes, and `Operator`, which is not the SDK's vocabulary.
  Either the schema gains titles or the generate step keeps the map.
- **The schema is the serialized shape, not the authoring shape.** It requires `fileloc` and `tasks` on a Dag, and `task_type`, `_task_module`, `ui_color`, `ui_fgcolor`, and `template_fields` on an operator, all of which the SDK fills in, and it carries a serialized `timetable` object where an author writes a schedule.
  Generation needs an exclusion list and a hand-written field or two, the same kind of rule [ADR-0009](../../airflow-core/adr/lang-sdk/0009-provider-operators-as-generated-dsl.md) states for provider operators.
- **A data edge is labelled by redeclaring it.** Declaring an edge that already exists is idempotent, so `extracted.Before(airflow.Label(transformed, "rows"))` labels the edge `Inputs` created.
- **Renaming a Go function renames the task.** The id is derived, and history, clears, and the UI all key on task_id, so renaming a function whose task carries no `TaskSpec` id is a Dag change.
  `airflow.TaskSpec{TaskId: ...}` pins an id that has to outlive the function's name, and it is also how a Dag gets snake_case ids, since nothing transforms a Go name.

## Alternatives

- **Fetching upstream values at run time**, where a task reads an upstream result inside its own body
  (`result.Get(&out)`) and the graph falls out of the order the Go code executes. Rejected: Airflow
  materializes the whole graph at Dag-processing time and then invokes a single task instance's
  callable per run, so an edge existing only in execution order cannot be parsed without running the
  program to completion. `Inputs` keeps the typed outputs that style is reached for.
- **Labelling the call**, `loaded.Before(notify).Label("when empty")`. Rejected: one call fans out, so a single label on the call cannot give `a.Before(b, c)` a different label per edge.
- **A positional task_id**, `dag.Task(taskId, fn, opts...)`. Rejected: It's more straightforward and native for Go user to define a Task without defining the task_id explicitly. They could still set the task_id other than the function name in the TaskSpec.
- **Separate `Dag` and `MixedLangDag` types.** Rejected: Python has one Dag class, and the Mixed Lang case is not a Dag at all ([ADR 7](0007-mixed-lang-task-handler-interface.md)).

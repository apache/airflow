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

1. **One Dag type, constructed then registered.** `airflow.Dag(dagID, spec)` returns a `*airflow.DagRef` that is complete before `bundle.Register(dag)` takes it — the same verb that registers Mixed Lang task handlers ([ADR 7](0007-mixed-lang-task-handler-interface.md)).
   Naming rule: `airflow.X(...)` constructs, `*airflow.XRef` is the entity; what every Dag must have is a positional parameter for dag_id, and the rest travels in a spec struct.
2. **Tasks register through `dag.Task(fn any, opts ...airflow.TaskOption)`**, returning a `*airflow.TaskRef`. `airflow.Inputs(...)` and a bare `airflow.TaskSpec{}` both implement `TaskOption`.
3. **At most one `airflow.TaskSpec` per task.** A second one is a registration error rather than something to merge, so a task's attributes are only ever written in one place.
4. **task_id is the Go function name by default**, spelled exactly as the function is, and `airflow.TaskSpec{TaskID: ...}` sets it to anything else.
5. **`airflow.Inputs(refs...)` declares the data and the edge in one call** for defining graph with TaskFlow syntax.
6. **`Before` and `After` are order-only edges on `airflow.Node`**, which both `*airflow.TaskRef` and `*airflow.TaskGroupRef` satisfy. They are the Go pair for `>>` and `<<`, and both return their argument set as one `Node`, so `a.Before(b, c).Before(d)` is Python's `a >> [b, c] >> d`.
7. **An edge label wraps the endpoint**: `loaded.Before(airflow.Label(notify, "when empty"))` is Python's `loaded >> Label("when empty") >> notify`. Labelling the endpoint rather than the call lets one fan-out give each edge its own label.
8. **Trigger rules belong to the task**, as `airflow.TaskSpec{TriggerRule: ...}`, never to an edge.
9. **A user-facing enum carries its type in the constant name** — e.g. `airflow.TriggerRuleAllDone`.
10. **Everything an author writes comes from one `airflow` package.**
11. **No Go-native deferral**, and none is needed: the constructs that defer are DSL tasks Python executes.
12. **`DagSpec`, `TaskSpec` and `TaskGroupSpec` are generated from Airflow core's serialization schema** (`airflow-core/src/airflow/serialization/schema.json`) into the `airflow` package itself and committed, the way `models.gen.go` already is for the supervisor schema.
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
emptyNotice := dag.Task(notifyEmpty, airflow.TaskSpec{TaskID: "notify_empty"})
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

func Dag(dagID string, spec ...DagSpec) *DagRef

func (d *DagRef) Task(fn any, opts ...TaskOption) *TaskRef
func (d *DagRef) TaskGroup(groupID string, spec ...TaskGroupSpec) *TaskGroupRef

func (g *TaskGroupRef) Task(fn any, opts ...TaskOption) *TaskRef
func (g *TaskGroupRef) TaskGroup(groupID string, spec ...TaskGroupSpec) *TaskGroupRef

// DagSpec, TaskSpec and TaskGroupSpec are generated into this package from
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
    TaskID      string // defaults to the Go function name
    Retries     int
    TriggerRule TriggerRule
    // ...
}

// TaskOption is sealed: its only method is unexported, so a task takes SDK-defined options and
// nothing else. TaskSpec and the value Inputs returns both implement it.
type TaskOption interface{ applyTask(*taskConfig) error }

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
- **The generated names need a mapping.** The core schema carries no `title` fields, unlike the supervisor schema `models.gen.go` reads, so its `dag`, `operator` and `task_group` definitions would generate as `Dag`, a name the constructor already takes, `Operator`, which is not the SDK's vocabulary, and `TaskGroup`, the name of the method that adds a group.
  Either the schema gains titles or the generate step keeps the map.
- **The schema is the serialized shape, not the authoring shape.** It requires `fileloc` and `tasks` on a Dag, and `task_type`, `_task_module`, `ui_color`, `ui_fgcolor`, and `template_fields` on an operator, all of which the SDK fills in, and it carries a serialized `timetable` object where an author writes a schedule.
  Generation needs an exclusion list and a hand-written field or two, the same kind of rule [ADR-0009](../../airflow-core/adr/lang-sdk/0009-provider-operators-as-generated-dsl.md) states for provider operators.
- **A state enum generated into `genmodels` is re-exported from `airflow` once a field that an author can reach holds its values.** Today the only re-exported enum is `DagRunState`, the type of the values in `TriggerDagRunSpec.AllowedStates` and `TriggerDagRunSpec.FailedStates`. `DagRunType` is re-exported once a field such as `allowed_run_types` on `DagSpec` holds its values. `TaskInstanceState` is re-exported once `airflow.TaskInstance` gets a state field.
  `airflow/enums.go` lists every user-facing enum with the Python enum that it mirrors.
  `airflow.DagRunState` is a type alias of `genmodels.DagRunState`, not a new type. `airflow.DagRun` is an alias of `sdk.DagRun`, and package `sdk` cannot import `airflow`. So a state field that `sdk.DagRun` gains later has a type declared outside `airflow`. `actx.DagRun().State == airflow.DagRunStateFailed` compiles only if both sides have the same type.
  The cost is that `%T` and package reflect report `airflow.DagRunState` as `genmodels.DagRunState`, and package `airflow` cannot declare a method on that type.
- **A value outside an enum is rejected when the task is added.** Go converts a string literal to an enum type without a cast, so the enum type alone does not stop `TaskSpec{TriggerRule: "all_sucess"}`. `dag.Task` panics with a message that names the field and lists the valid values. Python rejects an unknown trigger rule when the operator is constructed. It rejects an unknown weight rule only when the Dag is serialized.
- **A data edge is labelled by redeclaring it.** Declaring an edge that already exists is idempotent, so `extracted.Before(airflow.Label(transformed, "rows"))` labels the edge `Inputs` created.
- **Renaming a Go function renames the task.** The id is derived, and history, clears, and the UI all key on task_id, so renaming a function whose task carries no `TaskSpec` id is a Dag change.
  `airflow.TaskSpec{TaskID: ...}` pins an id that has to outlive the function's name, and it is also how a Dag gets snake_case ids, since nothing transforms a Go name.
  Renaming a task group renames every task whose task_id the group_id prefixes, for the same reason.
- **A group edge is expanded at registration, in the order the group edges were first declared.** `group.Before(loaded)` stands for an edge from each last task of the group, and `extracted.Before(group)` for an edge to each first task.
  Registration expands the group edges one at a time. Each expansion reads every task, every edge declared between two tasks, and the task edges that earlier group edges expanded into, which is the rule the TypeScript SDK's serializer applies.
  So an author can add the tasks of a group, and the edges between them, after putting the group on an edge. Python expands a group edge when `>>` runs, so the two agree when a Dag declares its group edges after its tasks and the edges between them, every group at an end of a group edge holds a task, and no label sits on an edge whose receiver is inside a task group.
  The cycle check runs on the declared edges first and on the expanded graph after, so a cycle that only group edges close is caught, and the message names those group edges.
- **A group with no task is stepped over.** An edge to it continues along each edge from it, and an edge from it back along each edge to it, whenever those were declared, as the TypeScript SDK does. So `extracted.Before(empty).Before(loaded)` runs `load` after `extract`, as Python's `extract >> empty >> load` does.
  What Python does with an empty group depends on how and when its edges are declared: `load << empty << extract` adds no edge, and an empty group with no task before it falls back to the last tasks of the enclosing group or of the whole Dag, which can make a task depend on itself.
- **An edge cannot connect a group to what it holds.** Python's `group >> node_in_group` orders the last tasks of the group before the first tasks of a node inside it, which fails as a cycle whenever that node holds a task. The edge verb rejects the edge when it is declared.
- **A label stays on the edge it is declared on.** A label on a group edge labels none of the task edges that the group edge stands for, and a labelled edge keeps its ends.
  Python's behavior depends on which end is the receiver and which groups hold the ends. When no group holds `extract`, `extract >> Label("rows") >> group` labels each of those task edges as well. When `a` and `b` are in different groups, `a >> Label("x") >> b` replaces `a` with its group, so every last task of `a`'s group runs before `b`. The Go SDK does neither.
- **`TaskGroup` takes `spec ...TaskGroupSpec`, as `Dag` takes `spec ...DagSpec`.** A group has one kind of option, so it needs no sealed option interface. A mapped task group would come through a method of its own, not through this variadic.

## Alternatives

- **Fetching upstream values at run time**, where a task reads an upstream result inside its own body
  (`result.Get(&out)`) and the graph falls out of the order the Go code executes. Rejected: Airflow
  materializes the whole graph at Dag-processing time and then invokes a single task instance's
  callable per run, so an edge existing only in execution order cannot be parsed without running the
  program to completion. `Inputs` keeps the typed outputs that style is reached for.
- **Labelling the call**, `loaded.Before(notify).Label("when empty")`. Rejected: one call fans out, so a single label on the call cannot give `a.Before(b, c)` a different label per edge.
- **A positional task_id**, `dag.Task(taskID, fn, opts...)`. Rejected: It's more straightforward and native for Go user to define a Task without defining the task_id explicitly. They could still set the task_id other than the function name in the TaskSpec.
- **Separate `Dag` and `MixedLangDag` types.** Rejected: Python has one Dag class, and the Mixed Lang case is not a Dag at all ([ADR 7](0007-mixed-lang-task-handler-interface.md)).

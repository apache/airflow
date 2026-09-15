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

1. **One Dag type, constructed then registered.** `airflow.Dag(dagId, spec)` returns a `*airflow.DagRef` that is complete before `bundle.Register(dag)` takes it — the same verb that registers Mixed Lang task handlers ([ADR 6](0006-mixed-lang-task-handler-interface.md)).
   Naming rule: `airflow.X(...)` constructs, `*airflow.XRef` is the entity; what every Dag must have is a positional parameter for dag_id, and the rest travels in a spec struct.
2. **Tasks register through `dag.Task(fn any, opts ...airflow.TaskOption)`**, returning a `*airflow.TaskRef`. `airflow.Inputs(...)` and a bare `airflow.TaskSpec{}` both implement `TaskOption`.

# comment: this is wrong -- task_id shouldn't default to Go function name, we should explicitly spell the task_id out
3. **task_id defaults to the Go function name**, as `@task` does in Python TaskFlow, and
   `airflow.TaskSpec{TaskId: ...}` overrides it. Go owns both ends of a native Dag, so no contract
   outside the program depends on the identifier — unlike a Mixed Lang handler, where Python fixed
   the id first ([ADR 6](0006-mixed-lang-task-handler-interface.md)).
4. **`airflow.Inputs(refs...)` declares the data and the edge in one call** for defining graph with TaskFlow syntax.
5. **`Before` and `After` are order-only edges on `airflow.Node`**, which both `*airflow.TaskRef`and `*airflow.TaskGroupRef` satisfy. They are the Go pair for `>>` and `<<`, and both return an `*airflow.EdgeRef` standing for the edges they just declared.
6. **An edge label hangs off that `*airflow.EdgeRef`**: `loaded.Before(notify).Label("when empty")` is Python's `loaded >> Label("when empty") >> notify`.
7. **Trigger rules belong to the task**, as `airflow.TaskSpec{TriggerRule: ...}`, never to an edge.
8. **A user-facing enum carries its type in the constant name** — e.g. `airflow.TriggerRuleAllDone`.
9. **Everything an author writes comes from one `airflow` package.**
10. **No Go-native deferral**, and none is needed: the constructs that defer are DSL tasks Python executes.

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

The task_ids are `extract`, `transform`, and `load`, from the function names; `dag.Task(load, airflow.TaskSpec{TaskId: "load_rows"})` names one explicitly.

**Order-only dependencies — the `>>` and `<<` equivalent.** For tasks that must be ordered but exchange no data; the functions take no parameter for such an edge.

```go
loaded := dag.Task(load, airflow.Inputs(transformed))
cleaned := dag.Task(cleanup, airflow.TaskSpec{TriggerRule: airflow.TriggerRuleAllDone})
staging := dag.TaskGroup("staging") // a group carries edges like a task
staging.Task(stageRows)             // tasks join a group through the group

loaded.Before(dag.Task(notify), cleaned)        // loaded >> [notify, cleanup]
cleaned.After(extracted)                        // cleanup << extracted
staging.Before(loaded)                          // staging >> load

emptyNotice := dag.Task(notifyEmpty)
loaded.Before(emptyNotice).Label("when empty")  // loaded >> Label("when empty") >> notifyEmpty
```

## Signature

```go
package airflow

func Dag(dagId string, spec DagSpec) *DagRef

func (d *DagRef) Task(fn any, opts ...TaskOption) *TaskRef
func (d *DagRef) TaskGroup(groupId string, opts ...TaskGroupOption) *TaskGroupRef

func (g *TaskGroupRef) Task(fn any, opts ...TaskOption) *TaskRef
func (g *TaskGroupRef) TaskGroup(groupId string, opts ...TaskGroupOption) *TaskGroupRef

// DagSpec and TaskSpec carry the serialized Dag attributes.
// They will be generated from the Airflow Core serialization schema.json directly to avoid drift.
type DagSpec struct {
    Schedule  string
    StartDate time.Time
    Catchup   bool
    Tags      []string
    // ...
}

type TaskSpec struct {
    TaskId      string // defaults to the Go function name
    Retries     int
    TriggerRule TriggerRule
    // ...
}

// TaskOption is sealed — its only method is unexported — so a task takes SDK-defined options
// and nothing else. TaskSpec and the value Inputs returns both implement it.
type TaskOption interface{ applyTask(*taskConfig) }

func Inputs(refs ...*TaskRef) TaskOption

// Node is what an edge connects. *TaskRef and *TaskGroupRef implement it; it is sealed the same way, and is the Go counterpart of Python's DAGNode / DependencyMixin.
type Node interface {
    Before(nodes ...Node) *EdgeRef
    After(nodes ...Node) *EdgeRef
    node()
}

func (e *EdgeRef) Label(text string) *EdgeRef

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

# comment: we should make is possible to a.before(b,c).before(d) syntax
- **`Before` and `After` return edges, not a node, so they do not chain into a path.** `a >> b >> c`
  is two statements in Go; one call fans out (`a.Before(b, c)`), and the returned `*EdgeRef` exists
  to be labelled. Returning the receiver instead would make `a.Before(b).Before(c)` read like
  Python's chain while meaning a fan-out from `a`.
- **A data edge is labelled by redeclaring it.** `airflow.Inputs` hands back no `*EdgeRef`, and declaring an edge that already exists is idempotent, so `extracted.Before(transformed).Label("rows")` labels the edge `Inputs` created.

## Alternatives

- **Fetching upstream values at run time**, where a task reads an upstream result inside its own body
  (`result.Get(&out)`) and the graph falls out of the order the Go code executes. Rejected: Airflow
  materializes the whole graph at Dag-processing time and then invokes a single task instance's
  callable per run, so an edge existing only in execution order cannot be parsed without running the
  program to completion. `Inputs` keeps the typed outputs that style is reached for.

# comment: Let's make the label syntax as `loaded.Before(airflow.Label(notify, "when empty"))` so that we can even support  a.before(airflow.Label(b,...),airflow.Label(c,...)).before(d)
- **An inline label marker**, `loaded.Before(airflow.Label("when empty"), notify)`, transliterating
  Python's `>> Label(...) >>`. Rejected: it widens the variadic from nodes to a mixed type so the
  compiler stops saying what an edge endpoint is, and the chained form already reads left to right.
- **Separate `Dag` and `MixedLangDag` types.** Rejected: Python has one Dag class, and the Mixed Lang case is not a Dag at all ([ADR 6](0006-mixed-lang-task-handler-interface.md)).

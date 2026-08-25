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

# 8. Common task constructs: TaskGroup, ShortCircuitOperator, BranchOperator, TriggerDagRunOperator

Date: 2026-08-24

## Status

Proposed. Extends the `Dag` interface from [ADR 7](0007-native-dag-interface.md) (itself unmerged, #67155/#70158) with four sibling methods: `TaskGroup`, `ShortCircuitOperator`, `BranchOperator`, and `TriggerDagRunOperator`. No PR proposes any of this yet, and no other Lang SDK has designed these four constructs either: this is a first proposal, not a catch-up with precedent.

`ShortCircuitOperator`, `BranchOperator`, and `TriggerDagRunOperator` take the `Operator` suffix because that's what the class each one wraps is called in Python: `ShortCircuitOperator`, `BranchPythonOperator`, and `TriggerDagRunOperator`. `BranchOperator` drops the `Python` infix, since that part of the name is language-specific and this is the Go SDK. `TaskGroup` gets no suffix because it isn't one: it's a `DAGNode`, not a `BaseOperator` subclass.

## Why

The Native Dag interface ([ADR 7](0007-native-dag-interface.md)) only has `dag.Task(fn, opts...)`. Four more constructs are common enough in Python Dags that a native Go author will ask for them immediately: `TaskGroup` (visual/logical grouping, with no execution of its own), `ShortCircuitOperator` (what the TaskFlow decorator `@task.short_circuit` instantiates under the hood, to skip everything downstream when a condition is false), `BranchOperator` (likewise for `@task.branch`, to run only one of several downstream paths), and `TriggerDagRunOperator` (starts a run of a separate Dag, with no decorator form). Status names the underlying operator classes these map to; here they're introduced by the spelling most Python Dag authors actually write.

## Example

### TaskGroup

```go
dag := registry.AddDag(v1.DagSpec{DagId: "etl", Schedule: "@daily"})

group := dag.TaskGroup("transform")
cleaned := group.Task(cleanRows)
validated := group.Task(validateRows, v1.Inputs(cleaned))

dag.Task(nativeLoad, v1.Inputs(validated))
```

`Dag.TaskGroup(groupId string) Dag` returns a handle with the same `Task` (and `TaskGroup`) methods as `dag` itself, so groups nest the way Python's `TaskGroup` nests as a `DAGNode`. Every task registered through it gets its task_id prefixed with `transform.`, matching `prefix_group_id`.

### ShortCircuitOperator

```go
extracted := dag.Task(nativeExtract)
proceed := dag.ShortCircuitOperator(hasRows, v1.Inputs(extracted))
proceed.Then(dag.Task(loadIfReady))
```

```go
func hasRows(log *slog.Logger, extracted NativeResult) (bool, error) {
    return extracted.Message != "", nil
}

func loadIfReady(log *slog.Logger) error {
    log.Info("loading native Dag data")
    return nil
}
```

`Dag.ShortCircuitOperator(fn, opts...) *TaskRef` requires `fn`'s data return to be `bool`. `proceed.Then(dag.Task(loadIfReady))` is what declares `loadIfReady` as downstream of the short circuit here. The `bool` is a runtime skip signal, not data `loadIfReady` needs, so it wires in through [ADR 7](0007-native-dag-interface.md)'s `Then` rather than `Inputs`, and `loadIfReady` takes no parameter for that edge at all, the same as `TriggerDagRunOperator` below. The runtime skips every task reachable from the returned `*TaskRef` when `fn` returns `false`, matching `ShortCircuitOperator`'s `ignore_downstream_trigger_rules` defaulting to `true`.

### BranchOperator

```go
choose := dag.BranchOperator(pickPath)
choose.Then(dag.Task(handleLong), dag.Task(handleShort))
```

```go
func pickPath(log *slog.Logger) (string, error) {
    if longRun() {
        return "handleLong", nil
    }
    return "handleShort", nil
}

func handleLong(log *slog.Logger) error { /* ... */ return nil }
func handleShort(log *slog.Logger) error { /* ... */ return nil }
```

`Dag.BranchOperator(fn, opts...) *TaskRef` requires `fn`'s data return to be `string`. There is no branch-only wiring call: `choose.Then(dag.Task(handleLong), dag.Task(handleShort))` uses the same `Then` mechanism as everywhere else in this proposal, and every task passed to it becomes a branch candidate. The `string` `pickPath` returns is a runtime routing signal, not data the candidates need, so `handleLong` and `handleShort` take no parameter for that edge at all. The runtime keeps only the candidate whose own task_id equals the string `pickPath` returned (`handleLong`/`handleShort` here, the default task_id derived from each function's Go name), and skips the rest.

This is the Go shape of [`example_branch_python_dop_operator_3.py`](../../airflow-core/src/airflow/example_dags/example_branch_python_dop_operator_3.py)'s `cond >> [empty_task_1, empty_task_2]`: `should_run()` returns the chosen operator's own `task_id`, and `choose.Then(...)`'s variadic fan-out is the direct Go counterpart of that list-broadcast `>>`. Python has no `Case` either.

### TriggerDagRunOperator

```go
extracted := dag.Task(nativeExtract)
trigger := dag.TriggerDagRunOperator(v1.TriggerDagRunSpec{
    DagId: "downstream_etl",
    Conf:  map[string]any{"source": "etl"},
})
extracted.Then(trigger)
```

`Dag.TriggerDagRunOperator(spec TriggerDagRunSpec) *TaskRef` is the fourth sibling method alongside `TaskGroup`, `ShortCircuitOperator`, and `BranchOperator`, even though it wraps no Go function and its return is a leaf `*TaskRef`, not a scope.

It still matches Python in one respect: `TriggerDagRunOperator` takes no callable either, and `conf` is a static `dict` (Jinja-templated, never a Python function). With no `fn` parameter list, `v1.Inputs(...)` has nothing to bind into. This is exactly the case [ADR 7](0007-native-dag-interface.md) added `Then` for: `extracted.Then(trigger)` orders the trigger after `extracted` with no value passed, the same way `cond >> trigger_task` would in Python.

## How

- `ShortCircuitOperator` and `BranchOperator` reuse `dag.Task`'s existing registration path: the same `reflect`-based signature validation and `*TaskRef` return from ADR 7. Each is a `Task` with one more runtime hook, not a new registration mechanism; only the required data-return type changes (`bool`, `string`). `TaskGroup` and `TriggerDagRunOperator` don't. `TaskGroup` takes a plain `groupId string` and hands back a `Dag` scope, with no function or reflection involved. `TriggerDagRunOperator` registers a task built entirely from `TriggerDagRunSpec`, with no Go function to reflect over either.
- Downstream of `ShortCircuitOperator` and `BranchOperator`, tasks wire in through `Then`, not `Inputs`: the returned `bool`/`string` is a runtime control signal, not data the next task needs. [ADR 7](0007-native-dag-interface.md) reserves `Then` for exactly that case, an edge with no value to bind, so `loadIfReady`, `handleLong`, and `handleShort` above take no parameter for it at all.
- The skip mechanism already exists on the wire, unused. Python's `ShortCircuitOperator`/`BranchPythonOperator` both call into `skip()`/`skip_all_except()` (`providers/standard/src/airflow/providers/standard/utils/skipmixin.py`), which raise `DownstreamTasksSkipped` (`task-sdk/src/airflow/sdk/exceptions.py`). The task runner (`task-sdk/src/airflow/sdk/execution_time/task_runner.py:1616-1621`) catches that and sends it as a `SkipDownstreamTasks` Execution API message. That message type is **already generated** in Go (`go-sdk/pkg/execution/genmodels/models.gen.go:1549`, `SkipDownstreamTasks{Tasks []string}`), but nothing in `go-sdk/` constructs or sends one today. `ShortCircuitOperator` and `BranchOperator` would be its first callers.
- The generated-but-unused pattern above repeats for `TriggerDagRunOperator`, even though its registration path is different. On Airflow 3, `TriggerDagRunOperator.execute()` raises `DagRunTriggerException` (`task-sdk/src/airflow/sdk/exceptions.py`). The task runner's `_handle_trigger_dag_run` (`task-sdk/src/airflow/sdk/execution_time/task_runner.py:1942`) catches that and sends it as a `TriggerDagRun` Execution API message, also **already generated** in Go (`go-sdk/pkg/execution/genmodels/models.gen.go:1805`, with fields `Conf`, `DagID`, `LogicalDate`, `Note`, `PartitionKey`, `ResetDagRun`, `RunAfter`, `RunID`), but nothing in `go-sdk/` constructs or sends one today.
  `_handle_trigger_dag_run` only ever populates seven of those eight fields: `dag_id`, `run_id`, `logical_date`, `run_after`, `conf`, `reset_dag_run`, and `note`. `partition_key` is never set. `wait_for_completion`, `deferrable`, `poke_interval`, `allowed_states`, `failed_states`, and `skip_when_already_exists` never reach the `TriggerDagRun` message at all; the task runner reads them straight off the caught exception and handles waiting/skipping itself, after the trigger response comes back.
- `TaskGroup`'s serialized form is already established, just not by the schema. `schema.json` only declares `task_group.children` as an unconstrained dict (`{"$ref": "#/definitions/dict"}`). The actual shape, `children[label] = [kind, value]` with `kind` set to `"operator"` for a leaf task or `"taskgroup"` for a nested group, comes from `serialize_for_task_group()` (`task-sdk/src/airflow/sdk/bases/operator.py` and `.../definitions/taskgroup.py`) and the `DagAttributeTypes` enum (`airflow-core/src/airflow/serialization/enums.py`).
  The native-Dag stack this ADR builds on already has a serializer, but it isn't ready for nesting: `go-sdk/pkg/execution/serde.go` (part of #67155/#70158, not on `main`) has a `serializeTaskGroup` that only emits one flat root group, `children[id] = ["operator", id]` for every registered task, unconditionally. Nesting would need that function extended to walk the actual group tree instead of iterating a flat task list.
- The operator's identity is recorded like any other task. Python serializes `task_type`/`_task_module` for every operator (`airflow-core/src/airflow/serialization/serialized_objects.py`, with the fields declared required in `.../serialization/schema.json`), and `ShortCircuitOperator`/`BranchPythonOperator` are just distinct classes recorded the same way everything else is. What's absent is a serialized *skip* marker: no field says which edges are conditional or what a `BranchOperator`'s candidates are. Those are the task's ordinary downstream edges; which ones run is decided entirely at run time by the task's own execution, never by anything special in the Dag JSON.

## Question

- How should the Go SDK implement deferral, so `TriggerDagRunOperator` can support `deferrable=True` together with `wait_for_completion=True`? Python defers via `TaskDeferred`/`DagStateTrigger`, letting the operator yield control and resume once the triggered Dag run finishes; Go has no equivalent mechanism today, so a task function runs to completion in one call with no way to pause mid-run.
- Should the `v1` API move entirely to package-level functions instead of `Dag` methods (`v1.Dag(...)`, `v1.Task(dag, fn, opts...)`, `v1.TriggerDagRunOperator(dag, spec)`), rather than the method style adopted here?

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

# ADR-0008: Control-Flow Constructs in Lang SDKs

## Status

Proposed. Applies to every Lang SDK authoring native Dags; the Go SDK
([`go-sdk/adr/0007`](../../../go-sdk/adr/0007-native-dag-interface.md)) is the worked example and the
first implementation. Grew out of the design review on #72043, where exposing Python's operator
taxonomy in a Go API was rejected unanimously.

## Why

A native Dag interface starts with "register a task, declare an edge". Four constructs follow
immediately, because Python Dags use them everywhere: grouping, conditional skipping
(`ShortCircuitOperator`, `@task.short_circuit`), branching (`BranchPythonOperator`, `@task.branch`),
and triggering another Dag's run (`TriggerDagRunOperator`).

Every Lang SDK has to decide how to spell them. Copying Python's class names is the tempting default
and the wrong one: `dag.ShortCircuitOperator(...)` asks an author who has never seen Airflow to learn
Python's operator taxonomy to write an `if`. Making the SDK feel native to its own language matters
more than making the SDKs look alike.

## Decision

1. **Name the construct after the host language's control flow, not after Python's operator class.**
   Conditional skipping and branching are `if`/`else` and `switch`/`case` in most languages; the SDK
   should use those words. The `Operator` suffix, and the `Python` infix in `BranchPythonOperator`,
   carry nothing an author of another language needs.
2. **Never select a branch by host-language function or method name.** Candidates are chosen by an
   explicit label the author writes, so renaming a function cannot silently rewire a Dag.
3. **No default case.** `BranchPythonOperator` has none to serialize, and an unmatched label is a
   run-time error, exactly as an unknown task_id is in Python (`skip_all_except` raises on
   `invalid_task_ids`, `providers/standard/src/airflow/providers/standard/utils/skipmixin.py:155-159`).
4. **Guarded tasks take no parameter for the control edge.** A condition's `bool` and a branch's label
   are run-time signals, not data, so they travel as an order-only edge — never as a value bound into
   the downstream function.
5. **Triggering a Dag run is an ordinary task from a helper**, not a method on the Dag: it wraps no
   host-language function, so it is DSL, the hand-written member of the family in
   [ADR-0009](0009-provider-operators-as-generated-dsl.md).
6. **Grouping keeps Python's semantics**: a scope offering the same task and nesting methods as the
   Dag, prefixing each task_id with the group id (`prefix_group_id`).

## Example

The Go SDK spelling of all four:

```go
group := dag.TaskGroup("transform")
cleaned := group.Task(cleanRows)
validated := group.Task(validateRows, airflow.Inputs(cleaned))

gate := dag.If(hasRows, airflow.Inputs(validated)) // fn returns (bool, error)
gate.Then(dag.Task(loadIfReady))                   // .Else(...) optional

pick := dag.Switch(pickPath) // fn returns (string, error)
pick.Case("long", dag.Task(handleLong)).
    Case("short", dag.Task(handleShort))

dag.Task(airflow.TriggerDagRun(airflow.TriggerDagRunSpec{DagId: "downstream_etl"})).After(gate)
```

`If` with `Then` alone serializes as `ShortCircuitOperator`, whose
`ignore_downstream_trigger_rules` defaults to `true`; with `Else` it serializes as a branch operator
choosing between the two sides. Short-circuiting is therefore not a separate construct, just the
one-sided case.

## How this lands in core

- **The skip decision is a run-time message, never serialized structure.** Nothing in the Dag JSON
  says which edges are conditional or what a branch's candidates are — those are ordinary downstream
  edges, and Python records `task_type`/`_task_module` for these operators like any other
  (`airflow-core/src/airflow/serialization/serialized_objects.py`). A label-to-task_id map therefore
  stays inside the SDK, at no cost to Python parity.
- **The wire already carries the skip, unused.** `skip()`/`skip_all_except()`
  (`skipmixin.py`) raise `DownstreamTasksSkipped` (`task-sdk/src/airflow/sdk/exceptions.py`), which
  the task runner (`task-sdk/src/airflow/sdk/execution_time/task_runner.py:1616-1621`) sends as
  `SkipDownstreamTasks`. That message is already generated in Go
  (`go-sdk/pkg/execution/genmodels/models.gen.go:1549`) and never sent; a native condition or branch
  is its first caller in any Lang SDK.
- **Trigger and deferral stay in Python.** A trigger task serializes as `TriggerDagRunOperator` and
  runs on a Python worker, where `_handle_trigger_dag_run`
  (`task-sdk/src/airflow/sdk/execution_time/task_runner.py:1942`) sends the `TriggerDagRun` message
  and handles `wait_for_completion`, `deferrable`, `poke_interval`, `allowed_states`,
  `failed_states`, and `skip_when_already_exists`. Those options behave as they do in a Python Dag,
  so no Lang SDK needs a native deferral mechanism to offer them. Go's generated `TriggerDagRun`
  message (`models.gen.go:1805`) stays unused; the Python runner populates seven of its eight fields,
  never `partition_key`.
- **Group nesting needs the serializers extended.** `schema.json` declares `task_group.children` as
  an unconstrained dict; the real shape, `children[label] = [kind, value]` with `kind` of
  `"operator"` or `"taskgroup"`, comes from `serialize_for_task_group()`
  (`task-sdk/src/airflow/sdk/bases/operator.py`, `.../definitions/taskgroup.py`) and
  `DagAttributeTypes` (`airflow-core/src/airflow/serialization/enums.py`). Go's `serializeTaskGroup`
  (`go-sdk/pkg/execution/serde.go`, in #67155/#70158) emits one flat root group and has to walk the
  tree instead.

## Limitation

- **A branch selects exactly one label.** Python's branch callable may return a list of task_ids, and
  `skip_all_except` handles it; a single-label return cannot express that, and no Lang SDK offers it
  for now. An author needing several paths to run together groups them under one label, or gates each
  with its own condition. This is the one branching behaviour the decision above drops, and it is
  accepted rather than open.

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
first implementation. Grew out of the design review on #72043, where naming these constructs after
Python's operator classes was rejected unanimously.

## Decision

1. **Name each construct after the host language's control flow**, not after the Python operator
   class it serializes to. Conditional skipping and branching are `if`/`else` and `switch`/`case` in
   most languages; the `Operator` suffix and the `Python` infix carry nothing an author of another
   language needs.
2. **Never select a branch by host-language function or method name.** Candidates are chosen by an
   explicit label the author writes, so renaming a function cannot silently rewire a Dag.
3. **No default case.** `BranchPythonOperator` has none to serialize, and an unmatched label is a
   run-time error.
4. **Guarded tasks take no parameter for the control edge.** A condition's `bool` and a branch's
   label are run-time signals, not data.
5. **Triggering a Dag run is an ordinary task from a helper**, not a method on the Dag: it wraps no
   host-language function, so it is DSL, the hand-written member of the family in
   [ADR-0009](0009-provider-operators-as-generated-dsl.md).
6. **Grouping keeps Python's semantics**: a scope offering the same task and nesting methods as the
   Dag, prefixing each task_id with the group id (`prefix_group_id`), and standing as an edge
   endpoint in its own right — a whole group can be ordered against a task or another group, as
   `group1 >> group2` does in Python.

## Context

A native Dag interface starts with "register a task, declare an edge". Four constructs follow
immediately, because Python Dags use them everywhere: grouping, conditional skipping
(`ShortCircuitOperator`, `@task.short_circuit`), branching (`BranchPythonOperator`, `@task.branch`),
and triggering another Dag's run (`TriggerDagRunOperator`). Every Lang SDK has to decide how to spell
them, and copying Python's class names is the tempting default and the wrong one:
`dag.ShortCircuitOperator(...)` asks an author who has never seen Airflow to learn Python's operator
taxonomy in order to write an `if`. Feeling native to its own language matters more than the SDKs
looking alike.

## Example

The Go SDK spelling of all four:

```go
group := dag.TaskGroup("transform")
cleaned := group.Task(cleanRows)
validated := group.Task(validateRows, airflow.Inputs(cleaned))

gate := dag.If(hasRows, airflow.Inputs(validated)) // ShortCircuitOperator: skips all downstream when false
gate.Then(dag.Task(loadIfReady))
gate.Else(dag.Task(loadFallback))                  // with Else: a branch between the two sides

pick := dag.Switch(pickPath) // fn returns (string, error), matched against the labels
pick.Case("long", dag.Task(handleLong)).
    Case("short", dag.Task(handleShort))

dag.Task(airflow.TriggerDagRun(airflow.TriggerDagRunSpec{DagId: "downstream_etl"})).After(gate)
```

Short-circuiting is therefore not a separate construct, just the one-sided `If`.

## Consequences

- **A branch selects exactly one label.** Python's branch callable may return a list of task_ids, and
  `skip_all_except` handles it; a single-label return cannot express that, and no Lang SDK offers it
  for now. An author needing several paths together groups them under one label, or gates each with
  its own condition. This limitation is accepted rather than open.
- **No Lang SDK needs a deferral mechanism** to offer `deferrable` or `wait_for_completion`, because
  the trigger task runs in Python.
- **The label-to-task_id mapping lives inside each SDK**, at no cost to Python parity, since the Dag
  JSON carries no branch-candidate field at all.
- **A group edge needs one base type per SDK** that both a task and a group satisfy, since either can
  sit at the end of an edge. Python already has it: `TaskGroup(TaskGroupMixin, DAGNode)`
  (`task-sdk/src/airflow/sdk/definitions/taskgroup.py:96`) and every operator inherit
  `DependencyMixin` (`.../definitions/_internal/mixins.py:35`), where `set_upstream` and
  `set_downstream` live. The Go shape is `airflow.Node`
  ([`go-sdk/adr/0007`](../../../go-sdk/adr/0007-native-dag-interface.md)).

## Appendix: Implementation Notes

- **An unmatched label matches Python's own failure.** `skip_all_except` raises on `invalid_task_ids`
  (`providers/standard/src/airflow/providers/standard/utils/skipmixin.py:155-159`), exactly as an
  unknown task_id does there.
- **`If` with `Then` alone serializes as `ShortCircuitOperator`**, whose
  `ignore_downstream_trigger_rules` defaults to `true`; with `Else` it serializes as a branch
  operator choosing between the two sides.
- **The skip decision is a run-time message, never serialized structure.** Python records
  `task_type`/`_task_module` for these operators like any other
  (`airflow-core/src/airflow/serialization/serialized_objects.py`), but nothing in the Dag JSON says
  which edges are conditional or what a branch's candidates are.
- **The wire already carries the skip, unused.** `skip()`/`skip_all_except()` raise
  `DownstreamTasksSkipped` (`task-sdk/src/airflow/sdk/exceptions.py`), which the task runner
  (`task-sdk/src/airflow/sdk/execution_time/task_runner.py:1616-1621`) sends as
  `SkipDownstreamTasks`. That message is already generated in Go
  (`go-sdk/pkg/execution/genmodels/models.gen.go:1549`) and never sent; a native condition or branch
  is its first caller in any Lang SDK.
- **Trigger and deferral stay in Python.** A trigger task serializes as `TriggerDagRunOperator` and
  runs on a Python worker, where `_handle_trigger_dag_run`
  (`task-sdk/src/airflow/sdk/execution_time/task_runner.py:1942`) sends the `TriggerDagRun` message
  and handles `wait_for_completion`, `deferrable`, `poke_interval`, and the state options itself.
  Go's generated `TriggerDagRun` (`models.gen.go:1805`) stays unused; the Python runner populates
  seven of its eight fields, never `partition_key`.
- **Group nesting needs the serializers extended.** `schema.json` declares `task_group.children` as
  an unconstrained dict; the real shape, `children[label] = [kind, value]` with `kind` of
  `"operator"` or `"taskgroup"`, comes from `serialize_for_task_group()`
  (`task-sdk/src/airflow/sdk/bases/operator.py`, `.../definitions/taskgroup.py`) and
  `DagAttributeTypes` (`airflow-core/src/airflow/serialization/enums.py`). Go's `serializeTaskGroup`
  (`go-sdk/pkg/execution/serde.go`, in #67155/#70158) emits one flat root group and has to walk the
  tree instead.

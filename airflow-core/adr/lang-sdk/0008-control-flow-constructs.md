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

Proposed.

## Decision

1. **Name each construct after the host language's control flow**, Conditional skipping and branching are `if`/`else` and `switch`/`case` in most languages; the `Operator` suffix and the `Python` infix carry nothing an author of another language needs.
   `If`, with or without `Else`, and `Switch` all serialize as a branch operator.
2. **A branch selects a task, not a string.** A case is the task reference the SDK already handed back, and the value on the wire is that task's task_id, so the compiler checks the candidate exists and no label has to be kept in step with it.
3. **No default case.** `BranchPythonOperator` has none to serialize, and a one-sided `If` whose condition is false follows nothing at all.
   A decider returning a ref that is not one of the declared cases is a run-time error the SDK raises, a narrower check than `skip_all_except`, which only rejects a task_id missing from the whole Dag.
4. **Triggering a Dag run is an ordinary DSL task**, it is pure DSL purpose instead of a new runtime.
5. **Grouping keeps Python's semantics**: a scope offering the same task and nesting methods as the Dag, prefixing each task_id with the group id (`prefix_group_id`),
   and can be ordered against a task or another group, as `group1 >> group2` does in Python.

## Context

A native Dag interface starts with "register a task, declare an edge".
Four constructs follow immediately, because Python Dags use them everywhere:

- grouping (`TaskGroup`)
- conditional skipping (`ShortCircuitOperator`, `@task.short_circuit`)
- branching (`BranchPythonOperator`, `@task.branch`)
- triggering another Dag's run (`TriggerDagRunOperator`)

Every Lang SDK has to decide how to spell them, and copying Python's class names is the tempting default but the existing convention of Python SDK might not be straightforward for other Lang SDKs.
For example `dag.ShortCircuitOperator(...)` asks an author who has never seen Airflow to learn Python's operator taxonomy in order to write an `if`.
Feeling native to its own language matters more than the SDKs looking alike.

## Example

The Go SDK spelling of all four. Every callable takes `airflow.Context` first, and a decider returns the case it picked, which the SDK sends as that task's task_id:

```go
func hasRows(actx airflow.Context, rows Rows) (bool, error)
func pickPath(actx airflow.Context) (*airflow.TaskRef, error)
```

```go
group := dag.TaskGroup("transform")
cleaned := group.Task(cleanRows)
validated := group.Task(validateRows, airflow.Inputs(cleaned))

loadIfReadyRef := dag.Task(loadIfReady)
loadFallbackRef := dag.Task(loadFallback)
handleLongRef := dag.Task(handleLong)
handleShortRef := dag.Task(handleShort)

gate := dag.If(hasRows, airflow.Inputs(validated)) // BranchOperator: skips the side not taken
gate.Then(loadIfReadyRef)
gate.Else(loadFallbackRef)

pick := dag.Switch(pickPath) // task_id pickPath, from the function name
pick.Case(handleLongRef).
    Case(handleShortRef)

dag.Task(airflow.TriggerDagRun(airflow.TriggerDagRunSpec{DagId: "downstream_etl"}), airflow.TaskSpec{TaskId: "trigger_downstream"}).After(gate)
```

A decider has to see the refs it returns, so either they are package-level or it is a closure where the Dag is built.

A one-sided `If` is a branch with one candidate rather than a `ShortCircuitOperator`.
That operator defaults to skipping every task in its downstream closure and ignoring their trigger rules, where a branch skips only the immediate downstream it did not take and keeps a task that several branches converge on running.

## Consequences

- **A branch selects exactly one task.** Python's branch callable may return a list of task_ids, and `skip_all_except` handles it;
  a single-ref return cannot express that, and no Lang SDK offers it for now.
  An author needing several paths together puts them behind one task, or gates each with its own condition.
  This limitation is accepted rather than open.
- **No Lang SDK needs a deferral mechanism for now** to offer `deferrable` or `wait_for_completion`, because the trigger task runs in Python.
- **Nothing extra reaches the Dag JSON**, which carries no branch-candidate field at all. A ref is a task_id by the time the decision is sent, so each SDK stores its cases and nothing else.
- **A group edge needs one base type per SDK** that both a task and a group satisfy, since either can sit at the end of an edge.
  Python already has it: `TaskGroup(TaskGroupMixin, DAGNode)` (`task-sdk/src/airflow/sdk/definitions/taskgroup.py:96`) and every operator inherit `DependencyMixin` (`.../definitions/_internal/mixins.py:35`), where `set_upstream` and `set_downstream` live.
  The Go shape is `airflow.Node` ([`go-sdk/adr/0008`](../../../go-sdk/adr/0008-native-dag-interface.md)).

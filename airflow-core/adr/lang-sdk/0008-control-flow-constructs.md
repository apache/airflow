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
2. **Never select a branch by host-language function or method name.** Candidates are chosen by an explicit label the author writes, so renaming a function cannot silently rewire a Dag.
3. **No default case.** `BranchPythonOperator` has none to serialize, and an unmatched label is a run-time error.
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

The Go SDK spelling of all four:

```go
group := dag.TaskGroup("transform")
cleaned := group.Task("clean_rows", cleanRows)
validated := group.Task("validate_rows", validateRows, airflow.Inputs(cleaned))

gate := dag.If("has_rows", hasRows, airflow.Inputs(validated)) // ShortCircuitOperator: skips all downstream when false
gate.Then(dag.Task("load_if_ready", loadIfReady))
gate.Else(dag.Task("load_fallback", loadFallback))             // with Else: a branch between the two sides

pick := dag.Switch("pick_path", pickPath) // fn returns (string, error), matched against the labels
pick.Case("long", dag.Task("handle_long", handleLong)).
    Case("short", dag.Task("handle_short", handleShort))

dag.Task("trigger_downstream", airflow.TriggerDagRun(airflow.TriggerDagRunSpec{DagId: "downstream_etl"})).After(gate)
```

Short-circuiting is therefore not a separate construct, just the one-sided `If`.

## Consequences

- **A branch selects exactly one label.** Python's branch callable may return a list of task_ids, and `skip_all_except` handles it;
  a single-label return cannot express that, and no Lang SDK offers it for now.
  An author needing several paths together groups them under one label, or gates each with its own condition.
  This limitation is accepted rather than open.
- **No Lang SDK needs a deferral mechanism for now** to offer `deferrable` or `wait_for_completion`, because the trigger task runs in Python.
- **The label-to-task_id mapping lives inside each SDK**, at no cost to Python parity, since the Dag JSON carries no branch-candidate field at all.
- **A group edge needs one base type per SDK** that both a task and a group satisfy, since either can sit at the end of an edge.
  Python already has it: `TaskGroup(TaskGroupMixin, DAGNode)` (`task-sdk/src/airflow/sdk/definitions/taskgroup.py:96`) and every operator inherit `DependencyMixin` (`.../definitions/_internal/mixins.py:35`), where `set_upstream` and `set_downstream` live.
  The Go shape is `airflow.Node` ([`go-sdk/adr/0007`](../../../go-sdk/adr/0007-native-dag-interface.md)).

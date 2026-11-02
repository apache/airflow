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

# ADR-0016: Mapped Tasks Across Lang SDKs

## Status

Proposed.

## Terms

| Term | Means |
|---|---|
| Expansion | Turning one mapped task into N task instances while the Dag run is in flight. |
| Expand input | The serialized description of what a task expands over. One per mapped task. |
| Map length | How many instances an expansion produces. |
| Map index | An instance's position in its expansion, `0..N-1`. `-1` is the placeholder that has not expanded yet. |
| Region | The row that scopes one expansion, so the same mapped task expands independently per enclosing iteration. |
| Producer | The task whose output is expanded over. |
| Consumer | The mapped task itself. |
| Binding | One resolved call argument delivered to a runtime. |
| Reduce | Reading a mapped task's whole output from a downstream task, as a sequence rather than one value. |

## Who runs what

Five processes appear in the paths below. The coordinator is the supervisor of a Lang-SDK task,
so "supervisor" and "coordinator" name the same process.

| Process | Language | Its share of mapping |
|---|---|---|
| Scheduler | Python | Counts the expansion and turns the `-1` placeholder into N instances. |
| API server, Execution API | Python | Picks this index's element and builds the bindings for `ti_run`. |
| Supervisor / coordinator | Python, in the worker process | Measures the producer's map length; forwards every XCom request. |
| `map()` helper | Python, one per consuming task instance | Imports a Python Dag file and applies its `map()` callables. |
| SDK runtime | the SDK's own language | Runs the handler, applies the SDK's own `map()` callables, holds the reduce sequence. |

## Decision

1. **Expansion stays in the scheduler for every language.** Map length, instance creation, regions, `REMOVED` handling and trigger rules are core's, not an SDK's.
   An SDK emits an expand input and consumes one value per map index; it never decides how many instances exist.
2. **The expand input keeps Python's `{__type, __var}` encoding**, so there is no duplicated format for describing a mapped task.
3. **Map length is computed once in the supervisor, instead of being re-implemented in each SDK.** The server tells the producer's supervisor whether the length is needed; the supervisor measures the return value and rejects one that is absent or unsized.
   No SDK computes it, the API server loads no extra Dag, and Python's own copy is deleted so the two cannot drift.
4. **Every Python mapping primitive is in scope, and the server resolves all of them.** A primitive an SDK has not implemented yet is a capability flag, not a core limit.
5. **A runtime always consumes delivered bindings; it never resolves the expand input itself.** The server picks this index's element in both authoring modes, so no SDK re-implements element selection or cross-product arithmetic.
6. **Only `map()` callables run outside the server, applied on top of the element it picked.** Selection and transformation separate cleanly, exactly as [`_MapResult`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/task-sdk/src/airflow/sdk/definitions/xcom_arg.py#L398) indexes first and applies the callables second.
   A Dag definition's callables belong to its own SDK and are applied by that runtime, which reads them from its own bundle. A Python Dag's callables are Python, so the helper applies them and the binding arrives as a plain literal.
   No language reads another's callable.
7. **The map-index label is a runtime call, not a template.** `map_index_template` is Jinja and no other language renders it; an SDK sends `SetRenderedMapIndex` instead.
8. **Reduce is a lazy sequence, never a materialized value.** A downstream consumer reads length and elements on demand, as [`LazyXComSequence`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/task-sdk/src/airflow/sdk/execution_time/lazy_sequence.py#L64) does.

## Context

Mapped tasks are the one Airflow feature whose behaviour is spread over all four layers: the
authoring API builds it, serialization carries it, the scheduler materializes it, and the worker
resolves one element of it. Python implements all four. An SDK that copied that split would
re-implement the scheduler's half in its own language, and the halves would diverge.

The split this ADR draws is the smallest one that still lets an SDK feel native: an SDK owns
*what to expand over* and *what to do with one element*, and core owns *how many* and *which*.
Regions carry that split into task loops at no extra cost — each expansion is scoped to its
enclosing iteration — so an SDK that implements this ADR gets per-iteration mapping for free and
loops need no mapping story of their own.

## What the serialized Dag carries

The expand input holds the items themselves when the author wrote a literal, and only a reference
when the author expanded over another task. Expanding over a literal therefore needs no producer
at all, and the scheduler can create every instance when the Dag run is created.

```jsonc
// .expand(bash_command=[1, 2, {"a": "b"}])     items inline
"expand_input": {"type": "dict-of-lists", "value": {"__type": "dict",
  "__var": {"bash_command": [1, 2, {"__type": "dict", "__var": {"a": "b"}}]}}}

// .expand(arg2=op1.output)                     reference only
"expand_input": {"type": "dict-of-lists", "value": {"__type": "dict",
  "__var": {"arg2": {"__type": "xcomref", "__var": {"task_id": "op1", "key": "return_value"}}}}}
```

`zip`, `concat` and `map` nest inside that `value` the same way, and a `map()` callable is stored
as its own source text, for display only.

## Write path

```
 AUTHOR (SDK)                 DAG PROCESSOR          SCHEDULER                  PRODUCER'S SDK RUNTIME
 ────────────                 ─────────────          ─────────                  ──────────────────────
 task.expand(items)    ──▶ serialize:                                           result = handler()
                             _is_mapped                                         SetXCom(return_value) ─┐
                             partial_kwargs                                                           │
                             expand_input      ──▶ literal items:           SUPERVISOR                │
                                                     N instances at once      length needed? ◀─────────┘
                                                   reference:                   len(value) ──▶ xcom.mapped_length
                                                     one placeholder (-1)
                                                     ↓ deps met
                                                   get_task_map_length ──▶ (region, 0..N-1)
```

[`get_task_map_length`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/airflow-core/src/airflow/serialization/definitions/xcom_arg.py#L150)
reads `xcom.mapped_length` for an unmapped producer and counts rows for a mapped one. `zip` takes
the `min` of its arguments, or the `max` when a fill value is set; `concat` takes the sum; `map`
delegates to the argument it wraps, because it is elementwise and cannot change the count.

## Read path

One path for both authoring modes. The server has already narrowed every binding to this map index
before the runtime starts.

```
 API SERVER   ti_run ──▶ TIRunContext{ ti.map_index, expanded_ti_count, arg_bindings }
                │
 COORDINATOR    ├─ map() callables written in Python? ──▶ helper applies them, binding becomes literal
                │
 SDK RUNTIME    ├─ map() callables written in this SDK? ──▶ runtime applies them
                └─ handler(element)
```

What the server puts in each binding, per authoring form:

| Authoring form | Binding | What the SDK runtime does |
|---|---|---|
| `.expand(x=[a, b, c])` | `literal`, `value` already the element | use it |
| `.expand(x=upstream())` | `xcom`, `task_id`, `map_index=-1`, `element_index=i` | GET the one row, take `value[element_index]` |
| `.expand(x=upstream.expand(...))` | `xcom`, `task_id`, `map_index=i` | GET that row |
| `.expand(a=…, b=…)` | one binding per argument, each already decomposed | use each |
| `.expand_kwargs(…)` | one binding per parameter of this index's mapping | use each |
| `.partial(x=…)` | `literal`, or `xcom` with no index | same value every index |
| `.zip(a, b)` | `tuple`, `parts` — one binding per zipped argument | pull each part, assemble |
| `.concat(a, b)` | `xcom` for whichever argument owns this index | GET that row |
| `.map(f)` | the wrapped form's binding, unchanged | apply the callables after selection |
| Mapped task group | the member's binding, taken through the innermost common mapped ancestor | use it |
| Reduce, in a downstream task | `sequence`, `task_id` | build a lazy handle, fetch nothing yet |

A Dag definition and a Python Dag with stub TaskHandler differ in nothing else.

## Reduce steps

1. The server sends a `sequence` binding naming the producing task. Nothing is fetched.
2. The runtime hands the handler a handle, not a value.
3. `length()` sends `GetXComCount`, which the supervisor turns into `HEAD /execution/xcoms/{dag}/{run}/{task}/{key}`.
4. `at(i)` sends `GetXComSequenceItem` → `GET …/{key}/item/{offset}`.
5. `slice(a, b)` sends `GetXComSequenceSlice` → `GET …/{key}/slice`.
6. The server decides which producer rows are live and orders them iteration first, then map index. The runtime sends no indices of its own beyond the offset it asked for.
7. With a configured `[core] xcom_backend`, the supervisor's backend helper decodes each value on the way back.

## Wire changes

On top of the per-map-index bindings in
[#70570](https://github.com/apache/airflow/pull/70570), which add `map_index` and `element_index`
to an XCom binding:

| Change | Where | Why |
|---|---|---|
| `needs_mapped_length: bool` | [`TIRunContext`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/airflow-core/src/airflow/api_fastapi/execution_api/datamodels/taskinstance.py#L495) | The supervisor cannot see the Dag, so the server states whether this producer's return value needs measuring (decision 3). |
| `kind: "sequence"` with `task_id` | `TaskArgBinding` | Reduce. No existing kind names a whole output rather than one row (decision 8). |
| `kind: "tuple"` with `parts` | `TaskArgBinding` | A `zip` element is several rows in one argument, so one `task_id` cannot express it. |
| `expand_input` declared as a property | `serialization/schema.json` | It exists today only as a `dependencies` entry, so the key validates by accident. |

`concat` needs no new kind: the server knows each argument's length already, so it emits an
ordinary XCom binding for whichever argument owns this index. A Dag definition's `map()` callables
need no field either, because that SDK reads them from its own bundle.

Each change ships behind an Execution API version with a mirrored supervisor wire-schema version.

## Runtime steps

What every SDK implements, in order. Nothing here is language-specific.

1. Read `ti.map_index` and `ti_context.expanded_ti_count` from `StartupDetails`. A `map_index` of `-1` means the task is not mapped.
2. Take the delivered binding for this map index. A binding naming a primitive the SDK does not implement fails the task before the handler runs.
3. Apply the SDK's own `map()` callables, if the Dag definition declared any.
4. Optionally send `SetRenderedMapIndex` to label the instance.
5. Run the handler with that one element.
6. Push the return value. The supervisor adds the map length; the SDK sends none.

## Authoring interface

- `expand` takes the arguments that vary per instance; `partial` takes the ones that do not.
- A mapped task returns the same reference type as any other task, so an edge or a later `expand` treats it identically.
- `zip`, `concat` and `map` are methods on a reference, not free functions, so they chain in declaration order.
- A reduce consumer receives a sequence of the element type, not the element type.

In the TypeScript SDK:

```ts
async function extract(): Promise<string[]>;
async function transform(args: { region: string; item: string }): Promise<number>;
async function report(args: { rows: XComSequence<number> }): Promise<void>;
```

```ts
const items = dag.task("extract", extract);
const perItem = dag.task("transform", transform).partial({ region: "uk" }).expand({ item: items });
dag.task("report", report, { rows: perItem });
```

`transform` runs once per element of `extract`'s output and receives one `item`; `report` runs once
and receives every result, as decision 8's lazy sequence.

## Consequences

- **`map()` over a Python callable costs a Dag-file import per consuming task instance**, in the helper process the coordinator starts. Only tasks that actually consume a `map()` pay it; every other stub TaskHandler keeps the no-parse path bindings were built to give it. That helper executes Dag-file Python on the worker for a task whose body is not Python — the same trust boundary as any Python task on that worker, but a new one for a Python Dag with stub TaskHandler.
- **`map()` callables are serialized as source text for display only.** Core deliberately does not deserialize them ([`SchedulerMapXComArg`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/airflow-core/src/airflow/serialization/definitions/xcom_arg.py#L105-L115)), so an SDK emits its own source the same way and no language has to read another's.

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

## Decision

1. **Expansion stays in the scheduler for every language.** Map length, instance creation, regions, `REMOVED` handling and trigger rules are core's, not an SDK's.
   An SDK emits an expand input and consumes one value per map index; it never decides how many instances exist.
2. **The expand input keeps Python's `{__type, __var}` encoding.** An SDK emits the same form the Dag processor already writes for a Python mapped task, so core parses one format and no SDK invents a second.
3. **Map length is computed once in the supervisor, instead of being re-implemented in each SDK.** The server tells the producer's supervisor whether the length is needed; the supervisor measures the return value and rejects one that is absent or unsized.
   The API server loads no extra Dag, and Python's own copy is deleted so the two cannot drift.
4. **Every Python mapping primitive is in scope, and the server resolves all of them.** A primitive an SDK has not implemented yet is a capability flag, not a core limit.
5. **A runtime always consumes delivered bindings; it never resolves the expand input itself.** The server picks this index's element in both authoring modes, so no SDK re-implements element selection or cross-product arithmetic.
6. **Only `map()` callables run outside the server, applied on top of the element it picked.** Selection and transformation separate: [`_MapResult`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/task-sdk/src/airflow/sdk/definitions/xcom_arg.py#L398) indexes first, then applies the callables.
   A Dag definition's callables belong to its own SDK and are applied by that runtime, which reads them from its own bundle. A Python Dag's callables are Python, so the helper applies them and the binding arrives as a plain literal.
   No language reads another's callable.
7. **The map-index label is a runtime call, not a template.** `map_index_template` is Jinja and no other language renders it; an SDK sends `SetRenderedMapIndex` instead.
8. **Reduce is a lazy sequence, never a materialized value.** A downstream consumer reads length and elements on demand, as [`LazyXComSequence`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/task-sdk/src/airflow/sdk/execution_time/lazy_sequence.py#L64) does.

## Context

Mapped task behaviour spans four layers: the authoring API builds it, serialization carries it,
the scheduler materializes it, and the worker resolves one element. Python implements all four. An
SDK that copied that split would re-implement the scheduler's half in its own language, and the two
would diverge.

An SDK owns *what to expand over* and *what to do with one element*, and core owns *how many* and
*which*. The same split holds inside a task loop: a region scopes one expansion to one iteration,
so a mapped task in a loop expands once per iteration with no extra work in the SDK. Task loops
therefore need no mapping design of their own.

## Who runs what

The coordinator is the supervisor of a Lang-SDK task, so the two names mean one process.

| Process | Language | Its share of mapping |
|---|---|---|
| Scheduler | Python | Counts the expansion and turns the `-1` placeholder into N instances. |
| API server, Execution API | Python | Picks this index's element and builds the bindings for `ti_run`. |
| Supervisor / coordinator | Python, in the worker process | Measures the producer's map length; forwards every XCom request. |
| `map()` helper | Python, one per consuming task instance | Imports a Python Dag file and applies its `map()` callables. |
| SDK runtime | the SDK's own language | Runs the handler, applies the SDK's own `map()` callables, holds the reduce sequence. |

## Authoring interface

- `expand` takes the arguments that vary per instance; `partial` takes the arguments that stay the same at every instance.
- A mapped task returns the same reference type as any other task, so an edge or a later `expand` treats it identically.
- `zip`, `concat` and `map` are methods on a reference, not free functions, so they chain in declaration order.
- A reduce consumer receives a sequence of the element type, not the element type.

The running example, in the TypeScript SDK. Every later section follows this same `extract →
transform → report`.

```ts
async function extract(): Promise<string[]>; // ["uk ", " fr", "de "]
async function transform(args: { region: string; row: string }): Promise<number>;
async function report(args: { totals: XComSequence<number> }): Promise<void>;
```

```ts
const rows = dag.task("extract", extract);
const totals = dag.task("transform", transform).partial({ region: "eu" }).expand({ row: rows });
dag.task("report", report, { totals });
```

`transform` runs once per element of `extract`'s output and receives one `row`; `report` runs once
and receives every result, as decision 8's lazy sequence.

## What the Dag serialization carries

The expand input holds the items themselves for a literal, and only a reference for another task's
output. A literal therefore needs no producer: the scheduler creates every instance with the Dag
run.

```jsonc
// .expand({ row: ["uk ", " fr", "de "] })
"expand_input": {"type": "dict-of-lists", "value": {"__type": "dict", "__var": {
  "row": ["uk ", " fr", "de "]}}}

// .expand({ row: rows })
"expand_input": {"type": "dict-of-lists", "value": {"__type": "dict", "__var": {
  "row": {"__type": "xcomref", "__var": {"task_id": "extract", "key": "return_value"}}}}}

// .expand({ row: rows.map(clean) })
"expand_input": {"type": "dict-of-lists", "value": {"__type": "dict", "__var": {
  "row": {"__type": "xcomref", "__var": {
    "type": "map",
    "arg": {"task_id": "extract", "key": "return_value"},
    "callables": ["(row) => row.trim()"]}}}}}

// .expand({ row: rows.zip(lookup, { fillvalue: "" }) })
"expand_input": {"type": "dict-of-lists", "value": {"__type": "dict", "__var": {
  "row": {"__type": "xcomref", "__var": {
    "type": "zip",
    "args": [{"task_id": "extract", "key": "return_value"},
             {"task_id": "lookup", "key": "return_value"}],
    "fillvalue": ""}}}}}
```

Two details an SDK emitter has to get right:

- The outer `type` is the expand input's, `dict-of-lists` or `list-of-dicts`. The inner `type` is the XCom argument's, one of `map`, `zip` and `concat`; a plain argument omits it.
- Only the outermost argument is wrapped in `xcomref`. A nested one — `map`'s `arg`, `zip`'s and `concat`'s `args` — is the bare dict, because [`serialize_xcom_arg`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/task-sdk/src/airflow/sdk/definitions/xcom_arg.py#L590) recurses without re-wrapping.

A `map()` callable is stored as source text, one string per callable in call order: Python writes
`inspect.getsource(f)`, an SDK writes its own equivalent — `fn.toString()` in TypeScript. Core never
deserializes these strings; they exist so the UI can show the transform. Execution always re-enters
the real Dag, so no binding carries a callable or a reference to one.

### Stub-task arg bindings

Serialization drops `python_callable`, so a stub task's signature is lost, and a stub runtime never
parses the Dag file. `_arg_bindings` carries what the server needs instead: one plain-JSON entry per
parameter, in declaration order, whose `kind` says how far parse time got. Four kinds leave the
server unchanged; `param` is the one it resolves at `ti_run`, because that parameter's value differs
per map index.

| `kind` | Means | Other keys |
|---|---|---|
| `literal` | the value itself | `value`, `from_default` |
| `xcom` | an upstream task's output | `task_id` |
| `sequence` | a mapped upstream's whole output, read as a lazy sequence | `task_id` |
| `unresolved` | the server could not finish it; the coordinator's Python helper does | — |
| `param` | the server derives it per map index | `default`, `group_arg` |

```jsonc
// @task.stub def transform(region: str, row: str, scale: int = 2) -> int: ...
// transform.partial(region="eu").expand(row=rows)
"_arg_bindings": [
  {"kind": "param", "name": "region", "value_schema": {"type": "string"}},
  {"kind": "param", "name": "row", "value_schema": {"type": "string"}},
  {"kind": "param", "name": "scale", "default": 2,
   "value_schema": {"type": "integer", "format": "int64"}}
]
```

Every kind carries `name` and `value_schema`: the parameter's name, and the JSON Schema of its
annotation for a runtime that decodes into machine types. Declaration order is load-bearing — an SDK
that binds by position, as Go does, has nothing else to go on.

A `param` entry is the signature, not the call. The mapped values stay in the fields Python already
writes: `op_kwargs_expand_input`, a decorated task's name for `expand_input`, and
`partial_kwargs.op_kwargs`. At `ti_run` the server resolves the entry from `group_arg` first —
`{group_id, key}`, naming an expand argument of an enclosing mapped task group — then from those
fields by `name`, then from `default`. Only `expand_kwargs()` can leave a parameter unbound until
run time, so only it needs that fallback.

Parse time emits `param` just twice: for a mapped stub's parameters, and for a group member's
argument that names its group. A member's other arguments are ordinary `literal`, `xcom`, `sequence`
or `unresolved` entries, and its unbound parameters take their defaults at parse time, because that
fallback does not vary by index.

Two kinds appear on one side only. `param` never reaches the runtime, the server having resolved it.
`tuple`, the `zip()` element below, is never in the Dag, since only the server knows which rows meet
at this index. Nor is the kind in the Dag always the kind on the wire: an argument naming a task in
the same mapped group is `sequence`, that producer being expanded itself, and the server narrows it
to this index's row.

With no `_arg_bindings` at all, a stub task keeps its older behaviour of ignoring call arguments.

## What the Execution API sends

An XCom binding carries the coordinate of this index's element: `map_index` for a row of a mapped
upstream, `element_index` for one element of an unmapped upstream's output. Three further kinds
complete the set:

```python
class XComSequenceArgBinding(_ArgBindingBase):
    """A producing task's whole output, as a lazy sequence (decision 8)."""

    kind: Literal["sequence"]

    task_id: str
    """Task whose ``return_value`` rows make up the sequence. The server decides which rows
    are live and in what order; the runtime asks only for offsets."""


class UnresolvedArgBinding(_ArgBindingBase):
    """
    An argument the server could not finish, because its ``map()`` callables are Python or its
    nesting fits no single binding.

    The coordinator's Python helper resolves it against the Dag file and replaces it with a
    ``LiteralArgBinding``, so no SDK runtime ever receives this kind.
    """

    kind: Literal["unresolved"]


class TupleArgBinding(_ArgBindingBase):
    """One ``zip`` element: several rows gathered into a single argument."""

    kind: Literal["tuple"]

    parts: list[XComArgBinding | LiteralArgBinding]
    """One part per zipped argument, in the order they were zipped. A ``fillvalue`` standing
    in at this index arrives as a ``LiteralArgBinding`` part."""


TaskArgBinding = TypeAliasType(
    "TaskArgBinding",
    Annotated[
        XComArgBinding | LiteralArgBinding | XComSequenceArgBinding | TupleArgBinding | UnresolvedArgBinding,
        Field(discriminator="kind", title="TaskArgBinding"),
    ],
)
```

and one field on the run context:

```python
class TIRunContext(BaseModel):
    ...
    needs_mapped_length: bool = False
    """True when this task's return value is expanded over downstream, so the supervisor has
    to measure it. Derived from ``not ti.is_mapped and any(task.iter_mapped_dependants())``."""
```

`concat` needs no new kind: the server knows each argument's length already, so it emits an
ordinary `XComArgBinding` for whichever argument owns this index. `serialization/schema.json` also
gains `expand_input` as a declared property, which exists today only as a `dependencies` entry, so
the key validates by accident.

Each change ships behind an Execution API version with a mirrored supervisor wire-schema version.

## Write path

```
 AUTHOR (SDK)                 DAG PROCESSOR          SCHEDULER                  PRODUCER'S SDK RUNTIME
 ────────────                 ─────────────          ─────────                  ──────────────────────
 .expand({ row: rows }) ──▶ serialize:                                          result = extract()
                              _is_mapped                                        SetXCom(return_value) ─┐
                              partial_kwargs                                                           │
                              expand_input     ──▶ literal items:           SUPERVISOR                 │
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

### Acting on `needs_mapped_length`

| Process | What it does |
|---|---|
| API server | Sets the flag at `ti_run`, off the serialized task it already loads for the bindings. |
| Supervisor | On `SetXCom` for `return_value`, computes `len(value)` and passes it as `mapped_length`; an unsized value fails the task. No `return_value` push by task end also fails it, since the consumer would otherwise be `UPSTREAM_FAILED` with no reason. Drops the field before forwarding the context, as it already does for `xcom_keys_to_clear`. |
| SDK runtime | Nothing. It never receives the flag and never sends a length. |

## Read path

One path for both authoring modes: every binding is already narrowed to this map index before the
runtime starts.

```
 API SERVER   ti_run ──▶ TIRunContext{ ti.map_index, expanded_ti_count, arg_bindings }
                │
 COORDINATOR    ├─ map() callables written in Python? ──▶ helper applies them, binding becomes literal
                │
 SDK RUNTIME    ├─ map() callables written in this SDK? ──▶ runtime applies them
                └─ transform({ region: "eu", row: … })
```

What the server puts in each binding, per authoring form.

| Authoring form | Binding | What the SDK runtime does |
|---|---|---|
| `.expand({ row: ["a", "b"] })` | `literal`, `value` already the element | use it |
| `.expand({ row: rows })` | `xcom`, `task_id`, `map_index=-1`, `element_index=i` | GET the one row, take `value[element_index]` |
| `.expand({ row: mappedUpstream })` | `xcom`, `task_id`, `map_index=i` | GET that row |
| `.expand({ a: …, b: … })` | one binding per argument, each already decomposed | use each |
| `.partial({ region: … })` | `literal`, or `xcom` with no index | same value every index |
| `.expandKwargs(…)` | one binding per parameter of this index's mapping | use each |
| `.zip(a, b)` | `tuple`, `parts` — one binding per zipped argument | pull each part, assemble |
| `.concat(a, b)` | `xcom` for whichever argument owns this index | GET that row |
| `.map(f)` | the wrapped form's binding, or `unresolved` | see below |
| Mapped task group | the member's binding, taken through the innermost common mapped ancestor | use it |
| Reduce, in a downstream task | `sequence`, `task_id` | build a lazy handle, fetch nothing yet |

A Dag definition and a Python Dag with stub TaskHandler differ in nothing else.

### Acting on `expanded_ti_count`

| Process | What it does |
|---|---|
| API server | Counts the live instances in this instance's region, so inside a loop it counts one iteration. |
| Supervisor | Nothing; forwards it. |
| SDK runtime | Exposes it to the handler read-only, so a handler can say "index 3 of 10". It is **not** used to resolve arguments. |

Python also uses it to pick which upstream map indexes a mapped-task-group member pulls, but only
because Python resolves locally. Under decision 5 the server does that arithmetic, so no SDK
implements it.

### map()

The server never runs a callable. It picks the element, then marks the binding for whoever can: a
stub task means a Python Dag file and Python callables; any other task means the Dag, and the
callables, are the SDK's.

**Case A — the callable is the SDK's.** The running example, with the rows trimmed on the way in:

```ts
const clean = (row: string) => row.trim();
dag.task("transform", transform).partial({ region: "eu" }).expand({ row: rows.map(clean) });
```

```
   ti_run  ──▶  XComArgBinding{ name: "row", task_id: "extract", element_index: 2 }
                     │                        the map wrapper is stripped for selection:
                     │                        map is elementwise, so it cannot move an index
   SDK RUNTIME       ├─ GET xcoms/…/extract/return_value    ──▶  ["uk ", " fr", "de "]
                     ├─ take value[element_index]           ──▶  "de "
                     ├─ find row's map() closures in its OWN bundle, by the binding's name
                     ├─ apply them in order                 ──▶  clean("de ") == "de"
                     └─ transform({ region: "eu", row: "de" })
```

**Case B — the callable is Python.** The same Dag, authored in Python with the body in TypeScript:

```python
# Dag file. The transform is Python; the task body is not.
def clean(row: str) -> str:
    return row.strip()


@task.stub(queue="typescript")
def transform(region: str, row: str) -> int: ...


transform.partial(region="eu").expand(row=extract().map(clean))
```

```ts
// TaskHandler. It sees a finished value and no map() at all.
export async function transform({ region, row }: { region: string; row: string }): Promise<number>;
```

```
   ti_run  ──▶  UnresolvedArgBinding{ name: "row" }    no value, no coordinate: Python has to run
                     │
   COORDINATOR       ├─ _prepare_ti_context sees an "unresolved" binding and starts the helper
                     │
   map() HELPER      ├─ import the Dag file from the pinned bundle
   (Python)          ├─ find the consumer's expand input for "row"
                     ├─ ExpandInput.resolve() at this map index ──▶ pulls the XCom, applies clean
                     └─ return "de"
                     │
   COORDINATOR       └─ rewrite the binding to LiteralArgBinding{ name: "row", value: "de" }
                     │
   SDK RUNTIME       └─ transform({ region: "eu", row: "de" })   an ordinary literal
```

Either way the handler receives a finished value. The runtime finds its own closures by the
binding's parameter name, so no callable crosses the wire. The helper starts only for a task with
at least one `unresolved` binding, and imports the Dag file once however many there are.

### Reduce

`report` is not mapped, so it reads every `transform` result instead of one.

1. The server sends a `sequence` binding naming `transform`. Nothing is fetched.
2. The runtime hands the handler a handle, not a value.
3. `length()` sends `GetXComCount`, which the supervisor turns into `HEAD /execution/xcoms/{dag}/{run}/{task}/{key}`.
4. `at(i)` sends `GetXComSequenceItem` → `GET …/{key}/item/{offset}`.
5. `slice(a, b)` sends `GetXComSequenceSlice` → `GET …/{key}/slice`.
6. The server decides which producer rows are live and orders them iteration first, then map index. The runtime sends only the offset it wants.
7. With a configured `[core] xcom_backend`, the supervisor's backend helper decodes each value on the way back.

## Runtime steps

What every SDK implements, in order. Nothing here is language-specific.

1. Read `ti.map_index` and `ti_context.expanded_ti_count` from `StartupDetails`. A `map_index` of `-1` means the task is not mapped.
2. Take the delivered binding for this map index. A binding naming a primitive the SDK does not implement fails the task before the handler runs.
3. Apply the SDK's own `map()` callables, if the Dag definition declared any.
4. Optionally send `SetRenderedMapIndex` to label the instance.
5. Run the handler with that one element.
6. Push the return value. The supervisor adds the map length; the SDK sends none.

## Consequences

- **`map()` over a Python callable costs a Dag-file import per consuming task instance.** Only tasks that consume a `map()` pay it; every other stub TaskHandler keeps the no-parse path bindings were built to give it. The helper executes Dag-file Python on the worker for a task whose body is not Python — the same trust boundary as any Python task there, but a new one for a stub TaskHandler.
- **`map()` callables are serialized as source text for display only.** Core deliberately does not deserialize them ([`SchedulerMapXComArg`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/airflow-core/src/airflow/serialization/definitions/xcom_arg.py#L105-L115)), so an SDK emits its own source the same way and no language has to read another's.

## Rejected alternatives

**A second field beside `_arg_bindings` for the entries the server resolves.** Splitting one list by
whether parse time could finish it gives a reader two shapes to learn, duplicates `name` and
`value_schema` across both, and leaves `value` meaning the delivered value in one and the unresolved
call argument in the other. A `param` kind says the same thing in one field, and the server already
has the discriminator it needs in `is_mapped` and `get_closest_mapped_task_group`. The cost is
widening `arg_binding.kind` on a field that already ships, which the serialized-Dag versioning work
covers.

**`{__type, __var}` encoding for those entries.** Nothing in a binding needs it. An XCom reference is
`kind` plus `task_id`, because a stub argument may only name an upstream task's `return_value`, so
there is no key to record. A `map()`, `zip()` or `concat()` argument is `unresolved`, which carries no
payload. Every `value` and `default` is checked JSON-serializable when it is written, so it crosses
the language boundary as it stands. Plain JSON also keeps the stored Dag readable, where the encoded
form wrapped every `value_schema` and every dict-valued `default` in a second object.

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

Amends [ADR-0007](0007-taskflow-across-language-boundary.md): it defines the bindings of a mapped stub task, and supports three forms 0007 rejects,
a `.map()`, `.zip()` or `.concat()` result as an expand argument, a mapped upstream's aggregated output, and a stub task with arguments inside a mapped task group.

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
3. **A Lang-SDK producer's map length is computed once in the supervisor, instead of in each SDK.** The server tells the supervisor whether the length is needed; the supervisor measures the return value and rejects one that is absent or not mappable.
   A Python task keeps measuring in its own runtime, so its `ti_run` still loads no Dag, and both apply the same `is_mappable_value` rule.
4. **Every Python mapping primitive is in scope, and the server resolves all of them.** A primitive an SDK has not implemented yet is a capability flag, not a core limit.
   For a Lang-SDK task, three shapes fail at parse time in both authoring modes: a `map()` that is not the outermost call of an expand argument, a `zip()` inside a `zip()`, and a `map()`, `zip()` or `concat()` result passed whole to a task that is not mapped.
5. **A runtime always consumes delivered bindings; it never resolves the expand input itself.** The server works out which row and element this index reads, and the supervisor reads that value through the XCom backend and hands it to the runtime, in both authoring modes.
   No SDK re-implements element selection or cross-product arithmetic, and the server never reads an XCom value.
6. **Only `map()` callables run outside the server and supervisor, applied on top of the element they picked.** Selection and transformation separate: [`_MapResult`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/task-sdk/src/airflow/sdk/definitions/xcom_arg.py#L398) indexes first, then applies the callables.
   The server sends the same binding in both modes, and the runtime picks who applies the callables: a Dag definition's runtime applies its own, read from its own bundle,
   and a stub TaskHandler sends `ApplyMap`, which the supervisor's Python sidecar answers by running the Python Dag's callables.
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

This ADR changes only the Lang-SDK path: a Dag definition, and a Python Dag with stub TaskHandler.
A Python task keeps resolving its arguments, measuring its map length and storing its XComs in its own runtime, as today.

## Who runs what

The coordinator only starts the SDK runtime subprocess; the supervisor handles every request that runtime sends.

| Process | Language | Its share of mapping |
|---|---|---|
| Scheduler | Python | Counts the expansion and turns the `-1` placeholder into N instances. |
| API server, Execution API | Python | Works out which row and element each binding reads, and builds the bindings for `ti_run`. Never reads an XCom value. |
| Supervisor | Python, in the worker process | Measures a Lang-SDK producer's map length; reads each bound value and narrows it to this index; forwards every XCom request. |
| Python sidecar | Python, one per Lang-SDK task instance, started on the first request that needs it | Runs `[core] xcom_backend`, so a runtime only sends and receives plain values. For a Python Dag, imports the Dag file and applies its `map()` callables on `ApplyMap`. |
| SDK runtime | the SDK's own language | Runs the handler, applies its own `map()` callables or sends `ApplyMap`, holds the reduce sequence. |

## Authoring interface

- `expand` takes the arguments that vary per instance; `partial` takes the arguments that stay the same at every instance.
- A mapped task returns the same reference type as any other task, so an edge or a later `expand` treats it identically.
- `zip`, `concat` and `map` are methods on a reference, not free functions, so they chain in declaration order.
- A reduce consumer receives a sequence of the element type, not the element type.
- Expanding over a list of `T` gives each instance a `T`; expanding over a dict gives each instance a `[key, value]` pair, as a Python task gets `(key, value)`.

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
parameter, in declaration order, whose `kind` says how far parse time got. Three kinds leave the
server unchanged; `param` is the one it resolves at `ti_run`, because that parameter's value differs
per map index.

| `kind` | Means | Other keys |
|---|---|---|
| `literal` | the value itself | `value`, `from_default` |
| `xcom` | an upstream task's output | `task_id` |
| `sequence` | a mapped upstream's whole output, read as a lazy sequence | `task_id` |
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
argument that names its group. A member's other arguments are ordinary `literal`, `xcom` or `sequence`
entries, and its unbound parameters take their defaults at parse time, because that
fallback does not vary by index.

Two kinds appear on one side only. `param` never reaches the runtime, the server having resolved it.
`tuple`, the `zip()` element below, is never in the Dag, since only the server knows which rows meet
at this index. Nor is the kind in the Dag always the kind on the wire: an argument naming a task in
the same mapped group is `sequence`, that producer being expanded itself, and the server narrows it
to this index's row. A runtime never sees `xcom` or `tuple` either: the supervisor reads them and sends a `literal`.

With no `_arg_bindings` at all, a stub task keeps its older behaviour of ignoring call arguments.

## What the Execution API sends

The server sends coordinates, never values: an XCom binding names a row, and for an unmapped upstream the element of it this index reads.
The supervisor reads each one through the XCom backend, narrows it, and passes the runtime a `literal`, so a runtime receives only `literal` and `sequence`.
`XComRef` and `LiteralValue` carry no parameter name, so a `zip` part can be either:

```python
class XComRef(BaseModel):
    """One upstream value, read by the supervisor."""

    kind: Literal["xcom"]

    task_id: str
    """Task whose ``return_value`` is read."""

    map_index: int | None = None
    """The row of a mapped upstream. Unset reads an unmapped upstream's only row."""

    element_index: int | None = None
    """Then this element of the row: a list item, or a dict's ``(key, value)`` pair in stored order."""


class LiteralValue(BaseModel):
    """One inline value."""

    kind: Literal["literal"]

    value: JsonValue | None = None


ArgPart = Annotated[XComRef | LiteralValue, Field(discriminator="kind")]


class _ArgBindingBase(BaseModel):
    name: str
    value_schema: ArgValueSchema | None = None

    apply_map: bool = False
    """Run this argument's ``map()`` callables on the picked value (decision 6)."""

    kwarg: str | None = None
    """``expand_kwargs()`` only: take this key of the value, after any ``map()`` callables."""

    fallback: ArgPart | None = None
    """The value when ``kwarg`` is missing: the ``partial()`` value or the signature default."""


class XComArgBinding(_ArgBindingBase, XComRef):
    """An argument read from an upstream task."""


class LiteralArgBinding(_ArgBindingBase, LiteralValue):
    from_default: bool = False


class TupleArgBinding(_ArgBindingBase):
    """One ``zip`` element: several values gathered into a single argument."""

    kind: Literal["tuple"]

    parts: list[ArgPart]
    """One part per zipped argument, in zip order. A ``fillvalue`` standing in at this index
    is a ``LiteralValue``."""


class XComSequenceArgBinding(_ArgBindingBase):
    """A producing task's whole output, as a lazy sequence (decision 8)."""

    kind: Literal["sequence"]

    task_id: str
    """Task whose ``return_value`` rows make up the sequence. The server decides which rows
    are live and in what order; the runtime asks only for offsets."""


TaskArgBinding = TypeAliasType(
    "TaskArgBinding",
    Annotated[
        XComArgBinding | LiteralArgBinding | XComSequenceArgBinding | TupleArgBinding,
        Field(discriminator="kind", title="TaskArgBinding"),
    ],
)
```

Before the runtime starts, the supervisor turns every binding into what the runtime receives:

| From the server | The supervisor | The runtime receives |
|---|---|---|
| `literal` | passes it on | `literal` |
| `xcom` | GETs the row through the XCom backend, then takes `element_index` when set | `literal` |
| `tuple` | reads every part and puts them in one array, in zip order | `literal` |
| any of these with `kwarg`, no `apply_map` | then takes that key, else reads `fallback`; with neither, fails the task | `literal` |
| any of these with `apply_map` | reads the value and any `fallback`, and leaves the callables and `kwarg` to the runtime | `literal`, with `apply_map` |
| `sequence` | passes it on | `sequence` |

The run context gains one field:

```python
class TIRunContext(BaseModel):
    ...
    needs_mapped_length: bool | None = None
    """Set only for a Lang-SDK task: True when its return value is expanded over downstream, so the
    supervisor has to measure it. Derived from ``not ti.is_mapped and any(task.iter_mapped_dependants())``.
    None for a Python task, which measures its own."""
```

The supervisor wire gains one request, for a stub TaskHandler that needs a Python Dag's `map()` callables run:

```python
class ApplyMap(BaseModel):
    """Run a Python Dag's ``map()`` callables for one argument, in the supervisor's Python sidecar."""

    type: Literal["ApplyMap"] = "ApplyMap"

    name: str
    """The parameter whose expand argument holds the callables."""

    value: JsonValue
    """The element the supervisor picked, before the callables."""


class MapResult(BaseModel):
    type: Literal["MapResult"] = "MapResult"

    value: JsonValue
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
| API server | Sets the flag at `ti_run` for a Lang-SDK task, off the serialized task it already loads for the bindings when `ti.is_stub`. A Python task's `ti_run` is unchanged and leaves it `None`. |
| Supervisor | On `SetXCom` for `return_value`, checks the value with `is_mappable_value` and passes `len(value)` as `mapped_length`, before the XCom backend stores it. A value that is not mappable, such as a string, fails the task. No `return_value` push by task end also fails it, since the consumer would otherwise be `UPSTREAM_FAILED` with no reason. Drops the field before forwarding the context, as it already does for `xcom_keys_to_clear`. |
| SDK runtime | Nothing. It never receives the flag and never sends a length. |
| Python task | Unchanged: its runtime measures in `_push_xcom_if_needed` before the value is stored. |

## Read path

One path for both authoring modes: the supervisor narrows every binding to this map index's value before the runtime starts.

```
 API SERVER   ti_run ──▶ TIRunContext{ ti.map_index, expanded_ti_count, arg_bindings }
                │          each xcom binding a coordinate: task_id, map_index, element_index
                │
 SUPERVISOR     ├─ read each value through the XCom backend, narrow it to this index
                └─ rewrite the binding to a literal, keeping apply_map
                │
 SDK RUNTIME    ├─ apply_map in a Dag definition? ──▶ runtime applies its own callables
                ├─ apply_map in a stub TaskHandler? ──▶ ApplyMap ──▶ Python sidecar applies them
                └─ transform({ region: "eu", row: … })
```

What the server sends for each authoring form, and what the runtime receives after the supervisor.

| Authoring form | Binding from the server | The runtime receives |
|---|---|---|
| `.expand({ row: ["a", "b"] })` | `literal`, `value` already the element | that `literal` |
| `.expand({ row: rows })` | `xcom`, `task_id`, `element_index=i` | `literal`: element `i` of the row |
| `.expand({ row: mappedUpstream })` | `xcom`, `task_id`, `map_index=i` | `literal`: that row |
| `.expand({ a: …, b: … })` | one binding per argument, each with its own coordinate | one `literal` per argument |
| `.partial({ region: … })` | `literal`, or `xcom` with no index | the same value at every index |
| `.expandKwargs(rows)` | per parameter: `xcom` with `element_index=i`, `kwarg` and `fallback` | `literal`: that key of element `i`, else the fallback |
| `.zip(a, b)` | `tuple`, one part per zipped argument | `literal`: the parts as one array |
| `.concat(a, b)` | `xcom` for whichever argument owns this index | `literal`: that element |
| `.map(f)` | the wrapped form's binding, with `apply_map` | see below |
| Mapped task group | the member's binding, taken through the innermost common mapped ancestor | `literal` |
| Reduce, in a downstream task | `sequence`, `task_id` | `sequence`, from which it builds a lazy handle and fetches nothing yet |

An element of a dict is its `(key, value)` pair in the stored value's order, the pair a Python task reading the same XCom gets.
A runtime receives it as a two-item array, such as `["uk", 3]`.
With `.expandKwargs(…)`, a parameter missing from this index's dict takes its `partial()` value or its default, and fails the task with neither, as a missing argument does in Python.

A Dag definition and a Python Dag with stub TaskHandler differ only in who applies `map()` callables.

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

The server never runs a callable, and sends the same binding in both authoring modes.
It strips the `map()` wrapper for selection, since `map()` is elementwise and cannot move an index, and sets `apply_map` on the binding.
The supervisor picks the element as for any other binding and keeps `apply_map` set, and the runtime gets the callables run.
Every SDK follows one order: the callables, then `kwarg`, then `fallback`.

`map()` must be the outermost call of an expand argument, so its callables always apply to the whole picked value.
`rows.zip(lookup).map(f)` is allowed; `rows.map(f).zip(lookup)` and `a.map(f).concat(b)` fail at parse time.
Chained `map()` calls are one wrapper with several callables, applied in order.

**Case A: a Dag definition, so the callable is the SDK's.** The running example, with the rows trimmed on the way in:

```ts
const clean = (row: string) => row.trim();
dag.task("transform", transform).partial({ region: "eu" }).expand({ row: rows.map(clean) });
```

```
   ti_run  ──▶  XComArgBinding{ name: "row", task_id: "extract", element_index: 2, apply_map: true }
                     │
   SUPERVISOR        ├─ GET xcoms/…/extract/return_value    ──▶  ["uk ", " fr", "de "]
                     └─ take element 2, send LiteralArgBinding{ name: "row", value: "de ", apply_map: true }
                     │
   SDK RUNTIME       ├─ find row's map() closures in its OWN Dag definition, by the binding's name
                     ├─ apply them in order                 ──▶  clean("de ") == "de"
                     └─ transform({ region: "eu", row: "de" })
```

**Case B: a Python Dag, so the callable is Python.** The same Dag, authored in Python with the body in TypeScript:

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
   ti_run  ──▶  XComArgBinding{ name: "row", task_id: "extract", element_index: 2, apply_map: true }
                     │                       the same binding as in Case A
   SUPERVISOR        └─ take element 2, send LiteralArgBinding{ name: "row", value: "de ", apply_map: true }
                     │
   SDK RUNTIME       ├─ a stub TaskHandler has no map() of its own: send ApplyMap{ name: "row", value: "de " }
   (TaskHandler)     │
   SUPERVISOR        ├─ hand it to the Python sidecar, starting it on first use
                     │
   PYTHON SIDECAR    ├─ import the Dag file from the pinned bundle, once per task instance
                     ├─ find transform's map() callables for "row"
                     └─ apply them in order                 ──▶  MapResult{ value: "de" }
                     │
   SDK RUNTIME       └─ transform({ region: "eu", row: "de" })
```

Either way the handler receives a finished value, and no callable crosses the wire.
A Dag definition's runtime finds its closures by the binding's parameter name.
The sidecar finds the Python callables the same way, through the task's expand input or its mapped task group's, as the server did to set the flag.
The supervisor answers `ApplyMap` the way it answers an XCom backend request, once the sidecar replies, so its request loop keeps running.
Only a stub TaskHandler with an `apply_map` binding starts the sidecar for this.

With `.expandKwargs(rows.map(toKwargs))` the callables build each instance's dict, and each parameter takes its `kwarg` from it.
`toKwargs` runs once per instance: a runtime reuses the result across parameters, and the sidecar does the same across `ApplyMap` requests.

A `map()` callable may skip its instance, as a Python one does by raising `AirflowSkipException`.
The runtime handles a skip from its own callables, or a skip response to `ApplyMap`, like a skip raised by the handler.
Any other error fails the task.

### Reduce

`report` is not mapped, so it reads every `transform` result instead of one.

1. The server sends a `sequence` binding naming `transform`. Nothing is fetched.
2. The runtime hands the handler a handle, not a value.
3. `length()` sends `GetXComCount`, which the supervisor turns into `HEAD /execution/xcoms/{dag}/{run}/{task}/{key}`.
4. `at(i)` sends `GetXComSequenceItem` → `GET …/{key}/item/{offset}`.
5. `slice(a, b)` sends `GetXComSequenceSlice` → `GET …/{key}/slice`.
6. The server decides which producer rows are live and orders them iteration first, then map index. The runtime sends only the offset it wants.
7. With a configured `[core] xcom_backend`, the supervisor's Python sidecar decodes each value on the way back.

### Python sidecar subprocess for the XCom backend

The supervisor runs `[core] xcom_backend` for a Lang-SDK task, so a runtime only sends and receives plain values.
The backend runs in the supervisor's Python sidecar, one process per task instance, started the way a Python task process is, on the first call the configured backend handles.
With the default backend no sidecar starts for XComs.
The sidecar reads connections and variables through the supervisor's API client, as a Python task does.
The supervisor measures a value before the backend stores it, and every value read back goes through the backend first, so nothing measures or indexes a storage path.
A Python task keeps running the backend in its own process.

## Runtime steps

What every SDK implements, in order. Nothing here is language-specific.

1. Read `ti.map_index` and `ti_context.expanded_ti_count` from `StartupDetails`. A `map_index` of `-1` means the task is not mapped.
2. Take the delivered binding for this map index: a `literal`, or a `sequence` for a reduce. A kind the SDK does not implement fails the task before the handler runs.
3. For a binding with `apply_map`, get its `map()` callables run: a Dag definition applies its own, and a stub TaskHandler sends `ApplyMap`. Then take `kwarg`, or `fallback` when the key is missing. Run the callables once per instance, even when several parameters share them. A skip from them skips the instance.
4. Optionally send `SetRenderedMapIndex` to label the instance.
5. Run the handler with that one element.
6. Push the return value. The supervisor adds the map length; the SDK sends none.

## Consequences

- **`map()` over a Python callable costs a Dag-file import per consuming task instance.** Only tasks that consume a `map()` pay it; every other stub TaskHandler keeps the no-parse path bindings were built to give it. The sidecar executes Dag-file Python on the worker for a task whose body is not Python — the same trust boundary as any Python task there, but a new one for a stub TaskHandler.
- **A worker running a Python `map()` for a stub TaskHandler needs the Dag file's Python dependencies.** The sidecar imports the Dag file, so every provider and library it imports must be installed on the workers serving that queue,
  which a Lang-SDK worker image did not need before. A missing one fails only the instances that consume a `map()`, at run time.
- **`map()` callables are serialized as source text for display only.** Core deliberately does not deserialize them ([`SchedulerMapXComArg`](https://github.com/apache/airflow/blob/27b8c82f54d44830176eb049f2e42916c7849bb0/airflow-core/src/airflow/serialization/definitions/xcom_arg.py#L105-L115)), so an SDK emits its own source the same way and no language has to read another's.
- **Two resolution paths must agree.** A Python task resolves its arguments locally, a Lang-SDK task through the server and supervisor.
  A shared conformance test runs every authoring form through both and compares what each instance receives, as the Lang SDK serialization conformance check does for the Dag format.

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
there is no key to record. A `map()`, `zip()` or `concat()` expand argument is a
`param`, resolved from the expand input Python already writes.
Every `value` and `default` is checked JSON-serializable when it is written, so it crosses
the language boundary as it stands. Plain JSON also keeps the stored Dag readable, where the encoded
form wrapped every `value_schema` and every dict-valued `default` in a second object.

**The server reads the XCom to pick an element.** Under an offloading XCom backend the row holds only a storage path,
and every `ti_run` of a mapped Lang-SDK task would read XCom values. The supervisor already reads through the backend.

**Each runtime picks its own element.** Every SDK would re-implement list indexing, dict pairs, `zip` assembly and the `expand_kwargs` split,
and Go cannot recover a JSON object's key order once it decodes one into a map.

**Recording where each `map()` sits in the argument tree.** A `map()` inside a `zip()` or `concat()` applies to one part only,
so the binding would carry a path into the tree and every runtime would walk it. Keeping `map()` outermost leaves one rule: apply the callables to the picked value.

**Applying Python `map()` callables when the task starts.** The supervisor would need to know whether the task runs a Python Dag or a Dag definition, which only the coordinator decides.
The runtime already knows its own mode, and a request reuses the sidecar the XCom backend runs in.

## Related work, out of scope

- **Moving Python tasks onto the supervisor path.** A Python task could send plain values and let the supervisor measure, store and narrow them,
  as it does for a Lang-SDK task, so one implementation serves every runtime. It changes Python task behaviour, so it is a separate follow-up.
- **The shapes decision 4 rejects for a Lang-SDK task.** A `map()` inside a `zip()` or `concat()`, a `zip()` inside a `zip()`,
  and a `map()`, `zip()` or `concat()` result passed whole to a task that is not mapped. Each needs a binding that nests, or a resolver for the whole argument.

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

# ADR-0007: TaskFlow Across the Language Boundary

## Status

Accepted

> **Note:** The inbound half of this decision — the argument-binding spec — is implemented in
> [apache/airflow#69757](https://github.com/apache/airflow/pull/69757) (Python and server side).
> The Go SDK runtime that consumes the spec lands in
> [#70209](https://github.com/apache/airflow/pull/70209), and per-map-index bindings for mapped
> stubs in [#70570](https://github.com/apache/airflow/pull/70570). Decision G is forward-looking
> and not yet implemented; it is recorded here because the wire contract was designed to admit it.

## Context

TaskFlow is the ergonomic core of authoring in Airflow: call a decorated function, get an
`XComArg` back, hand it to another task, and the dependency edge is implied by the call itself.

Per [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ), a task implemented in another
language is declared as a `@task.stub` operator inside an ordinary Python Dag file
([ADR-0001](0001-java-sdk-airflow-integration.md)), with the body living in a Lang-SDK build
artifact shipped in the same bundle ([ADR-0006](0006-no-lang-sdk-source-display.md)). The
question this ADR answers is what TaskFlow means for such a task.

**The outbound direction already worked.** A Go task function returning `(T, error)` has its
first return value pushed as the task's `return_value` XCom
(`go-sdk/bundle/bundlev1/task.go`, using `api.XComReturnValueKey`), which is exactly what a
Python `@task` does. A Lang-SDK task was therefore already consumable by anything downstream.

**The inbound direction did not.** Stub tasks could only be declared argless: a TaskFlow call's
arguments were silently discarded. A Go task needing an upstream's output had to hand-write a
`GetXCom` call with the upstream's `task_id` hardcoded in Go — restating in the foreign language
the wiring the Dag file had already expressed, and duplicating it at every call site.

The root cause is an asymmetry in how call arguments are recovered at execution time. A Python
worker gets them for free: it re-parses the Dag file and deserializes the operator, which carries
its own `op_args`/`op_kwargs`. A foreign runtime cannot parse Python and never materializes the
operator, so anything it needs must be **materialized on the Airflow side and shipped over the
wire**.

The goal is that the natural TaskFlow call works across the language boundary:

```python
@task.stub(queue="golang")
def transform(country: str, extracted: dict): ...


with DAG(...):
    transform("uk", extract())  # extract() is a normal Python @task
```

```go
// The runtime binds "uk" onto country and pulls extract's XCom into extracted.
func transform(ctx sdk.TIRunContext, log *slog.Logger, country string, extracted map[string]any) error
```

## Decision

TaskFlow across the language boundary is defined in two halves. **Outbound** is unchanged: the
task's return value becomes its `return_value` XCom. **Inbound** becomes a materialized
argument-binding spec, captured when the Dag is serialized, carried in the serialized Dag, and
delivered to the runtime at task startup.

### A. Inbound: an ordered, materialized argument-binding spec

A TaskFlow call that passes at least one argument is captured as an ordered list with one entry
per declared parameter. (A call passing none captures no spec at all, which is what keeps
pre-TaskFlow stub Dags serializing; see decision F.) Every entry carries the parameter's `name`
and a `kind`-discriminated payload:

- `XComArgBinding` (`kind: "xcom"`) — the value comes from an upstream task's `return_value`
  XCom, identified by `task_id`.
- `LiteralArgBinding` (`kind: "literal"`) — the value is an inline JSON literal from the Dag file.

Two properties make this a stable target for a foreign runtime:

- **The wire form is always positional.** Keyword arguments are normalized to declaration order
  through signature binding, so a runtime never has to reason about how the author chose to spell
  the call. Because `name` is present too, a runtime may bind by position or by name, whichever
  suits its language.
- **Defaults are explicit.** A parameter left unpassed is captured with its default value and
  `from_default: true`, so keyword-style consumers can distinguish "the author passed this" from
  "this is the signature's default" and leave the latter unclaimed.

Materializing is what makes the contract work at all: the spec is the *only* thing the runtime
receives, so it must be complete and self-describing rather than a reference the runtime is
expected to resolve on its own.

### B. Materialization belongs in core Dag serialization, behind a generic `is_stub` flag

The spec is built in Airflow core, from `OperatorSerialization._serialize_node`, for a non-mapped
`DecoratedOperator` flagged `is_stub`. The `@task.stub` provider contributes exactly one line —
`is_stub: bool = True` — and no knowledge of the binding format.

The `DecoratedOperator` requirement is not incidental: the builder works from the operator's
`python_callable` signature and its bound `op_args`/`op_kwargs`, which only a decorated operator
has. A foreign-runtime operator that is not TaskFlow-shaped would carry the `is_stub` marker but
no bindings, and giving it arguments would need a different capture path.

An earlier revision built the spec inside the standard provider's stub decorator, and the
Execution API recognized stub tasks by matching the operator's class name. Both were rejected in
review:

1. **The spec is defined against the Execution API's `TaskArgBinding` schema.** Building it in a
   provider puts the producer and the schema in different distributions with different release
   cadences, guaranteeing drift.
2. **Deriving JSON Schema from Python type hints is Execution-API-coupled work.** It has no
   business in a provider that otherwise contributes operators.
3. **Gating on the operator's name does not generalize.** Every future foreign-runtime operator
   would have to duplicate a magic string to be recognized. A serialized boolean flag —
   mirroring how `EmptyOperator` is recognized by `is_empty` rather than by its class name — lets
   any such operator opt in by declaring one attribute, and TaskFlow-shaped ones get argument
   binding along with it.

The flag is propagated onto mapped operators as well, so "is this task stub-backed?" is
answerable regardless of mapping, even though arg-binding materialization itself skips mapped
operators.

### C. Outbound: unchanged, and the reason only `return_value` matters

A Lang-SDK task's return value is its `return_value` XCom, as it already was. This is not merely
status quo — it is why `return_value` is the only XCom key that carries meaning across the
boundary, which is what makes the scope limit in decision H coherent rather than arbitrary.

### D. The type system is JSON Schema, not a bespoke enum

A foreign runtime must decode a JSON payload into a native typed value, so it needs to know what
shape to expect. Each binding therefore carries `value_schema`, a JSON-schema fragment derived
from the stub parameter's annotation.

An earlier revision shipped a small Airflow-specific `data_type` enum. Review pushed for reusing
JSON Schema instead, and that is what was adopted:

- Every target language already has JSON-schema vocabulary and tooling; an Airflow-specific enum
  would have to be re-implemented and kept in sync in every SDK.
- JSON Schema is open-vocabulary, so the fragment can be carried verbatim and extended later
  without a wire-format change. Runtimes ignore keywords they do not understand.
- It composes. Nested objects, arrays, and formats fall out of the same mechanism rather than
  needing enum members.

An absent `value_schema` means "unconstrained" — the annotation was missing, was `Any`, or was
something a schema could not be generated for — and the runtime falls back to a decode-only
check. Absence, not a null value, is the signal.

### E. `value_schema` rides on XCom bindings too

Carrying a schema on a *literal* is uncontroversial. Carrying one on an XCom binding drew the
obvious objection: the type of an upstream's payload is not knowable when the Dag is parsed. A
Python task is free to return a dict on one branch and `False` on another.

That objection is correct about XCom payloads and does not apply, because the schema describes
**the stub's declared parameter** — the contract the foreign runtime binds *into* — not a
prediction about what the upstream will actually push. With that reading, keeping it is
worthwhile:

1. **The server-side spec is the source of truth for decoding.** The alternative is to treat each
   runtime's own annotations as authoritative, which makes decoding behavior a property of the
   SDK rather than of the Dag.
2. **It enables early failure.** Once [AIP-85](https://cwiki.apache.org/confluence/x/_Q7OEg)
   lands, a type mismatch in a mixed-language Dag can be reported at Dag-processing time instead
   of when the foreign runtime fails to decode.
3. **It generalizes to native Lang-SDK Dags**, where the same field lets upstream and downstream
   types be checked against each other (decision G).

### F. Delivery at `ti_run`, not at parse time alone

The spec reaches the runtime as an optional `arg_bindings` field on the `ti_run` response's
`TIRunContext`, read from the serialized Dag through the shared, cached `DBDagBag` and returned
only for tasks flagged `is_stub`. A regular task's response is unchanged.

Delivering it purely as part of the serialized Dag was considered and does not suffice:

- **Per-map-index resolution needs the task instance.** Which slice of an upstream's output a
  given mapped stub receives is only knowable once the TaskInstance is joined in — a parse-time
  artifact cannot express it.
- **`ti_run` is where API version negotiation happens.** That is the only place the field can be
  withheld from clients that predate it.

The field ships behind a new Execution API version, `2026-10-30` (targeting Airflow 3.4), with a
mirrored version in the supervisor wire schema so runtimes pinned to the previous schema are
unaffected. Stub Dags that predate arg bindings keep running against older clients.

`arg_bindings` is `None` for a regular Python task and for a stub whose call passed no arguments.
A Lang SDK can short-circuit its whole binding path on that one check.

### G. The same spec is the substrate for native Lang-SDK Dags

*Forward-looking; not implemented.* When Dags can be authored natively in another language
([ADR-0003](0003-pure-java-dags.md), [ADR-0004](0004-dag-parsing.md)), such a Dag declares its
own tasks, edges, and call arguments. Those arguments are expected to be expressed as this same
binding spec rather than a second, native-only format.

The consequence for runtimes is the point: one binding implementation serves both authoring
modes. It is also why the spec is deliberately not `@task.stub`-shaped — no field in
`TaskArgBinding` refers to stubs, Python, or the decorator, so the same wire form describes a
natively authored call. (Its docstrings still say "stub", reflecting the only producer that
exists today; that is wording to revisit, not a constraint in the format.)

### H. Scope: what does not cross the boundary

The following raise when the Dag is serialized, rather than being silently dropped or deferred to
a runtime failure. Most messages name the working alternative; the ones that have none say so.

These checks run only once a TaskFlow call actually passes an argument, since that is when the
binding contract engages. An argless stub whose *signature* would violate one of them (a
`**kwargs` parameter, say) still serializes, exactly as it did before arg bindings existed.

| Rejected | Why |
| --- | --- |
| An XCom key other than `return_value` | Only the return value is meaningful across the boundary (decision C). Reviewers correctly noted that keys are *not* always `return_value` — a `multiple_outputs` task's output subscripts to per-key XComs — so this is a real, deliberate scope limit, not an assumption. The Java SDK dropped custom-key support before its beta for the same reason. |
| `.map()` / `.zip()` / `.concat()` results | These are lazily evaluated Python transformations with no wire representation. |
| A mapped upstream's aggregated output | A foreign runtime pulls single XCom rows; a combined output across map indices is not one row. |
| An upstream output nested inside a list or dict literal | The runtime binds whole arguments. The fix is to pass the upstream output as its own argument. |
| `*args` / `**kwargs` | A foreign runtime binds against a fixed parameter list. |
| Parameter names that collide with Airflow context keys | Stub signatures declare data parameters only; the runtime injects its own task context natively (e.g. the Go SDK's `sdk.TIRunContext` parameter). |
| Non-JSON-serializable literals, including `NaN` and `Infinity` | The spec travels as JSON. |
| A stub task with arguments inside a mapped task group | Such a task has per-map-index instances but no expand input of its own, so its argument values are unresolvable both at parse time and server-side. |

Deferred rather than rejected: mapped (`.expand()`) stubs capture no spec and keep today's
ignored-argument behavior until
[#70570](https://github.com/apache/airflow/pull/70570); `value_schema` for mapped bindings is
tracked in [#70523](https://github.com/apache/airflow/issues/70523).

### Alternatives Considered

- **Hand-written XCom pulls in the foreign runtime (status quo).** Rejected: duplicates the Dag's
  wiring in a second language, hardcodes upstream `task_id`s far from the Dag file, and gives the
  author no reason to believe the TaskFlow call they wrote does anything.
- **Capture the spec in the standard provider's stub decorator.** Rejected: splits the producer
  from the schema it targets, and puts type-hint-to-JSON-Schema derivation in a provider
  (decision B).
- **Recognise stub tasks by operator class name in the Execution API.** Rejected: forces every
  future foreign-runtime operator to duplicate a magic string (decision B).
- **A bespoke `data_type` enum for argument types.** Rejected: re-implemented per SDK, closed
  vocabulary, does not compose (decision D).
- **Deliver the spec only in the serialized Dag, with no `ti_run` field.** Rejected: cannot
  express per-map-index values and offers no version-negotiation point (decision F).
- **Omit `value_schema` on XCom bindings.** Rejected: makes decoding a property of each SDK
  rather than of the Dag (decision E).

## Consequences

- The TaskFlow call an author already knows how to write works on a stub task, and the dependency
  edge comes with it — `transform("uk", extract())` implies `extract >> transform` — instead of
  being declared twice in two languages.
- Every Lang SDK binds against one contract, and gains the same behavior when native Lang-SDK
  Dag authoring arrives (decision G). The cost is that each new SDK owes a binding
  implementation; a runtime that ignores `arg_bindings` degrades to today's behavior rather than
  breaking.
- Unsupported TaskFlow forms fail when the Dag is serialized, with an actionable message, instead
  of surfacing as a decode error inside a foreign runtime where the author has the least context.
- The flip side: there is now a set of TaskFlow constructs that work in Python and not on stub
  tasks (decision H). Authors moving a task across the boundary may have to restructure a call,
  and the boundary is not visible from the Dag file alone.
- `value_schema` is advisory. Nothing enforces that an upstream's payload matches the downstream
  parameter's schema; the runtime is free to reject on decode. Enforcement at Dag-processing time
  becomes possible after AIP-85 but is not part of this decision.
- The mechanism is not private to `@task.stub`: any TaskFlow-shaped operator that sets `is_stub`
  gets arg-binding materialization. A stub-flagged operator that is not a `DecoratedOperator` is
  recognized as stub-backed but receives no bindings (decision B).
- Older clients are unaffected: the version migration strips `arg_bindings`, and the server skips
  deriving it for them entirely.

## Appendix: Implementation Notes

Mechanics and non-obvious details, recorded so they are not re-derived or re-litigated. These are
consequences of the decisions above, not decisions in their own right.

**Where the pieces live**

| Concern | Location |
| --- | --- |
| Spec builder | `airflow-core/src/airflow/serialization/stub_arg_bindings.py` |
| Wire model (`TaskArgBinding` union) | `airflow-core/src/airflow/api_fastapi/execution_api/datamodels/task_arg_binding.py` |
| `ti_run` derivation | `airflow-core/src/airflow/api_fastapi/execution_api/services/task_instances.py` |
| Execution API version | `airflow-core/src/airflow/api_fastapi/execution_api/versions/v2026_10_30.py` |
| Supervisor schema version | `task-sdk/src/airflow/sdk/execution_time/schema/versions/v2026_10_30.py` |
| Stub marker | `providers/standard/src/airflow/providers/standard/decorators/stub.py` |

**Details worth knowing**

- **`kind` is a bare `Literal` with no default, deliberately.** Giving it a default drops it from
  the OpenAPI schema's `required` list; `datamodel-code-generator` then types the generated
  task-sdk client field as `Literal | None`, which pydantic rejects as a tagged-union
  discriminator, and `import airflow.sdk` fails at class-definition time. A plain default and
  `Field(init=False, default=...)` were both tested against the real generator and both fail this
  way. Every entry is built server-side as a plain dict, so no call site wants the default anyway.
- **Key omission, never `null`, is the wire contract for "unconstrained".** `ti_run` responds with
  `exclude_unset`, so an absent key stays absent.
- **`value_schema` generation.** Pydantic's stock JSON-schema generation, plus OpenAPI's
  `int64`/`double` numeric formats — a typed runtime decoding into a machine type cannot get the
  width from the bare `integer`/`number` type names, and `format` is an annotation per JSON
  Schema, so runtimes that do not recognize them simply skip them. Temporal subclasses such as
  `pendulum.DateTime` are normalized to their stdlib bases on retry, so they schema as
  `date-time` rather than being dropped. Generation is cached per annotation (`TypeAdapter`
  construction is expensive and annotations are static) and degrades to no schema rather than
  failing Dag serialization.
- **The builder is imported lazily**, so Python-only deployments never pay for pydantic's
  JSON-schema machinery just to serialize a Dag.
- **`is_stub` and `_arg_bindings` both bypass the generic `{__type, __var}` encoding** on decode,
  because the spec is plain JSON. `is_stub` additionally fails closed — anything that is not JSON
  `true` means "not a stub" — since a non-Python producer's blob is never schema-validated on that
  path. `_arg_bindings` is restored verbatim and validated later, at `ti_run`, where a malformed
  spec becomes a 500 rather than a silently wrong binding.
- **Version gating uses Cadwyn's `VersionChangeWithSideEffects.is_applied`**, not a date
  comparison, so the server skips the derivation entirely for older clients rather than computing
  a value the migration will strip.
- **No serialization schema version bump.** The change is purely additive,
  `definitions.operator` allows additional properties, and schema validation only runs on the
  write path, so nothing on the read side changes for an existing blob. Reviewers noted that the
  serialization version has been effectively unused since Airflow 2 and may be worth making
  meaningful; that policy question is tracked in
  [#71364](https://github.com/apache/airflow/issues/71364).
- **Known gap:** union annotations (for example `dict | bool`) are not yet handled.

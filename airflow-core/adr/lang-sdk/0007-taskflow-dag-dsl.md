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

# ADR-0007: TaskFlow-Style Dag DSL for Native Java Dags

## Status

Accepted

## Context

The Go SDK adopted a TaskFlow-style authoring API: `Dag.Task(fn, opts...)`
returns a handle, `Inputs(handle)` both wires the dependency edge and feeds the
upstream's return value into the downstream function's data parameter, and
`TaskSpec` / `DagSpec` configuration structs are generated from the Dag
serialization schema (`airflow-core/src/airflow/serialization/schema.json`).

The Java SDK could only implement *task bodies*. Everything else about the Dag
had to come from a Python `@task.stub` file:

- `@Builder.Dag` / `@Builder.Task` carried only identity attributes; there was
  no way to express schedule, retries, queue, or any other Dag/task
  configuration.
- The Java-side model kept only an `id -> task class` map: no dependency edges
  and no configuration, so a Java author could not describe a graph and there
  was nothing to serialize for native Dag support.

Python's TaskFlow shows the shape worth matching: calling tasks like functions
*is* the graph declaration — `load(transform(extract()))`. Java annotations
cannot change call semantics (invoking the real method would execute its body),
so the call syntax has to target compile-time-generated twins.

## Decision

### Two authoring surfaces, one model

**Annotation surface** — task bodies are `@Builder.Task`-annotated methods;
configuration lives in the annotations; the graph is wired by a static
`@Wiring` method that receives a processor-generated *flow twin* class
(`<Class>Ref`). Twin methods mirror the task methods: injectable parameters
(`Client`, `Context`) are dropped, data parameters become `In<T>`-typed
inputs, and the return value becomes a `TaskRef<T>` (which extends
`In<T>`). The call graph is the task graph:

```java
@Builder.Dag(id = "java_etl", schedule = "@daily")
public class EtlPipeline {

  @Builder.Task(id = "extract", retries = 2)
  public ExtractResult extract(Client client) { ... }

  @Builder.Task
  public TransformResult transform(Client client, ExtractResult in) { ... }

  @Builder.Task
  public void load(Context ctx, TransformResult in) { ... }

  @Builder.Task
  public void score(double threshold) { ... }

  @Wiring
  static void depends(EtlPipelineRef f) {
    f.load(f.transform(f.extract()));
    f.score(In.value(0.5));              // inline literal input
  }
}
```

There is deliberately **no id-based wiring mode** and no edge-declaring API
on the handles: the wiring calls, and only they, define the graph. A twin call
registers the task, so a `@Builder.Task` method never invoked in the wiring
method is an error (checked when the generated `build()` runs, at Dag-parse
time).

The wiring method is **optional**: a `@Builder.Dag` class without one registers
every task with no Java-side edges. That is the shape for stub-backed tasks —
the Python Dag file owns the graph and the supervisor delivers each call-site
argument at run time — so those classes carry no `@Wiring`, no configuration
attributes, and no Java-side dependencies at all.

**Interface surface** — plain-class task definitions registered as first-class
`TaskDef` objects, with fluent schema-validated `.config(key, value)` calls
and object-reference edges:

```java
var extract = new TaskDef("extract", Extract.class).config("retries", 2);
var transform = new TaskDef("transform", Transform.class).dependsOn(extract);

var dag = new DagDef("java_etl")
    .config("schedule", "@daily")
    .addTask(extract)
    .addTask(transform);
```

A `TaskDef` belongs to at most one Dag. Both surfaces flow into the same
model: schema-keyed configuration maps, `TaskDef` dependency edges, and
recorded parameter inputs on `DagDef`/`TaskDef`.

### One package, one wildcard import

The whole user-facing surface — the `Builder.Dag` / `Builder.Task`
annotations, `@Wiring`, the flow types (`In`, `TaskRef`), and the runtime
types (`DagDef`, `TaskDef`, `Task`, `Client`, `Context`, `Bundle`, ...) —
lives in `org.apache.airflow.sdk`, so every example starts with a single
`import org.apache.airflow.sdk.*`. There is no separate `dsl` package.

Two top-level types cannot share a fully-qualified name, and `Dag` / `Task`
are the names both the annotations and the runtime types want. Keeping the
annotations **nested** in `Builder` resolves that without renaming anything:
`Builder.Dag` and `Builder.Task` read as the annotations they are (they drive
the `*Builder` codegen), the task-implementation interface keeps the plain
name `Task`, and the Dag model is `DagDef`, symmetric with `TaskDef` — both
are the definition objects of the interface API. `@Wiring` needs no
qualification because nothing else claims that name.

By convention the `@Wiring` method sits at the end of the Dag class, after
the task methods it wires — read the tasks first, then the graph.

### Wiring is type-checked by javac itself

Twin input types make the graph checks ordinary Java type checking rather
than bespoke processor analysis:

- numeric parameters accept any numeric upstream (`In<? extends Number>`,
  widened or narrowed at run time by the shared decoder);
- `Object`, raw `Map`, and raw `List` parameters accept any upstream
  (`In<?>`, decoded loosely at run time);
- everything else accepts covariant matches of the declared type
  (`In<? extends T>`).

Unknown upstreams are unrepresentable (a handle only exists once its task is
registered), cycles are unconstructible in call syntax, and type mismatches
are javac errors at the twin call site. The interface API's `dependsOn` can
still express a cycle, so `Bundle` construction validates acyclicity (and
that every referenced upstream is registered in the same Dag).

### Runtime bindings win; wiring is the fallback

Data parameters resolve by **position**, never by parameter name: Java call
syntax is positional (no kwargs) and Java parameter names are not API — an
IDE rename must not change binding behaviour. This matches the Go SDK's
flat-parameter contract.

When the supervisor delivered `arg_bindings` for the run, the binding at the
parameter's position wins over anything the `@Wiring` method declared: for a
stub task the Python call site *is* the graph the scheduler ordered the run
by, so the Java class must not be able to disagree with it. The wiring-
recorded inputs are the fallback, used when no bindings arrived — which is
exactly the native-Dag case, where no Python call site exists. A `TaskRef`
input then resolves to the upstream's return-value XCom and a literal input
to its value. Keyword arguments still bind by name through a `TaskInput`
bundle; the generated code branches on whether bindings arrived, because
bindings fill the bundle field by field while the wiring fallback decodes the
bundle wholesale from its single wired input.

### Generated from the serialization schema

The `Builder` class — the outer container plus its nested `Dag` and `Task`
annotations, configuration attributes and all — and the `SchemaFields`
validation table are generated at build time from a vendored copy of the Dag
serialization schema (`sdk/schema/dag-schema.json`, kept in sync with
airflow-core by the `sync-java-sdk-dag-schema` prek hook), mirroring the
supervisor-schema → jsonschema2pojo pipeline that already exists. Generating
the whole class rather than merging generated attributes into a hand-written
one keeps a single definition of `Builder`; `id` (and `to` on `Dag`) stay the
leading structural attributes, and generation fails if a schema key ever
camel-cases onto one of them.

Field selection mirrors the Go SDK's `TaskSpec` generator: scalar properties
only, serializer-owned keys skipped (`_`-prefixed, schema-required,
`has_on_*`), a documented exclusion list for Python-only concerns that fails
generation when it goes stale, and a hand-curated Dag-level allowlist matching
Go's `DagSpec`. `schedule` is a virtual key that the future serializer maps to
the schema's `timetable` object.

Temporal attributes are ISO-8601 strings in annotations (validated by the
processor at compile time) and `java.time.Duration` /
`java.time.OffsetDateTime` values in `.config` calls.

### Explicit-only lowering, single-sourced dag id

The processor lowers **only attributes written at the use site** into
`DagDef.config(...)` / `TaskDef.config(...)` calls (via
`AnnotationMirror.getElementValues`). Annotation defaults mirror schema
defaults but are never emitted, so the serializer's omit-if-default semantics
stay intact and the scheduler's own defaults win for everything unset.

Because this written-vs-defaulted distinction only exists at compile time,
there is no reflective `new DagDef(SomeClass.class)` constructor. Instead the
generated builder exposes `DAG_ID` and a `dag()` factory (Dag-level config
only, no tasks) alongside `build()`, so the dag id is never restated in user
code.

### No annotation on data parameters

Data parameters need no annotation at all: anything that is not an injectable
type (`Client`, `Context`) is a data parameter, bound in declaration order.
Naming an upstream in an annotation would duplicate what the wiring — or the
Python call site — already declares. `Client.getXCom` / `Client.setXCom`
remain for imperative access.

## Consequences

- One concept, one name: bindings are *args*/*inputs* across the wire
  contract, the Go SDK, and the Java surface; the Java graph is declared the
  way Python TaskFlow declares it, but type-checked at compile time.
- Dependency edges, configuration, and parameter inputs now exist in the
  Java-side model, which is the prerequisite for emitting
  DagSerialization-v3 JSON for native Java Dags.
- Config typos fail at compile time (annotations) or Dag-parse time
  (`.config`), never silently. Wiring mistakes fail at compile time (type
  mismatch, unknown handle) or Dag-parse time (unregistered task, `dependsOn`
  cycle), never at task run time.
- An annotation Dag class that owns its graph writes a `@Wiring` method —
  the same posture as the Go SDK, trading a few lines for one wiring story
  instead of two. Stub-backed classes omit it and stay registration-only.
- The `@Wiring` method references the generated `<Class>Ref` twin, so IDEs
  show unresolved symbols until the first successful build (the standard
  Java codegen experience, as with Dagger or AutoValue).
- New scalar schema keys show up automatically in the annotations and the
  validation table after a schema sync; removals fail the build until the
  exclusion rules are updated — the surface cannot drift silently.
- Generated public API (annotation attributes) varies with the vendored
  schema version, exactly like the generated supervisor-schema models.

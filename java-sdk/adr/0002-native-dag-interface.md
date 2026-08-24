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

# ADR-0002: Native Java Dag — Interface Design

## Status

Proposed

## Context

A Dag authored with no Python stub file has no `@task.stub` call site to declare its graph, so
Java itself must express the graph, the Dag/task configuration, and the task bodies. This ADR is
scoped to what that Java call site looks like for a user. It shares the injectable
`client`/`context` question raised in [ADR-0001](0001-mixed-lang-dag-interface.md), and shares the
protocol substrate (the argument-binding spec) with
[`airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md`](../../airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md).

## Decision

### Annotation based

```java
@Builder.Dag(id = "java_etl", schedule = "@daily")
public class EtlPipeline {

  @Builder.Task(id = "extract", retries = 2)
  public long extract(Client client) {
    return 42L;
  }

  @Builder.Task(id = "transform")
  public long transform(Client client, long extracted) {
    return extracted * 2;
  }

  @Builder.Task(id = "load")
  public void load(Context ctx, long transformed) {
    // ...
  }

  @Wiring
  static void depends(EtlPipelineRef f) {
    f.load(f.transform(f.extract()));
  }
}
```

`f` is a generated `EtlPipelineRef` twin: calling it registers the task, its data parameters
become `In<T>` inputs, and its return value becomes a `TaskRef<T>`. The call graph is the task
graph, and `javac` type-checks it.

### Interface based

```java
var extract = new TaskDef("extract", Extract.class).config("retries", 2);
var transform = new TaskDef("transform", Transform.class).dependsOn(extract);

var dag = new DagDef("java_etl")
    .config("schedule", "@daily")
    .addTask(extract)
    .addTask(transform);
```

`TaskDef`/`DagDef` carry configuration through fluent `.config(key, value)` calls and dependency
edges through `.dependsOn(...)`, keyed to the Dag serialization schema.

## If `client`/`context` were real getter methods instead of injected parameters

The `<ClassName>Ref` twin above exists for one reason: the real `@Builder.Task` method's signature
takes `Client`/`Context` parameters, so it cannot be called directly inside `@Wiring` without a
second, type-compatible surface. If `client`/`context` are getter methods instead — not method
parameters — a task method's parameter list becomes data-only, and the real method is what
`@Wiring` calls. No twin, no `In<T>`/`TaskRef<T>` indirection:

```java
// Before: @Wiring is static and calls a generated twin, because extract()/
// transform() above take a Client parameter the twin has to drop.
@Wiring
static void depends(EtlPipelineRef f) {
  f.load(f.transform(f.extract()));
}
```

```java
// After: client/context come from getters, so extract()/transform()/load()
// are data-only and directly composable. @Wiring calls the real methods.
@Builder.Dag(id = "java_etl", schedule = "@daily")
public class EtlPipeline extends Task {

  @Builder.Task(id = "extract")
  public long extract() {
    var client = getClient();
    return 42L;
  }

  @Builder.Task(id = "transform")
  public long transform(long extracted) {
    return extracted * 2;
  }

  @Builder.Task(id = "load")
  public void load(long transformed) {
    // ...
  }

  @Wiring
  void depends() {
    load(transform(extract()));
  }
}
```

`extract()`, `transform()`, and `load()` are the same methods `@Builder.Task` marks as task
bodies. Once `Client`/`Context` are getters, their real signatures already line up
(`long -> long -> void`), so this type-checks with no generated class produced purely to make the
types match.

Open tradeoffs this ADR does not resolve:

- What calling `extract()` inside `depends()` actually does — whether Dag parsing runs against a
  distinct instance/mode so the call records an edge instead of executing the real task body — is
  a runtime-dispatch question, not a call-site question. This ADR only proposes the shape.
- A task method becomes an ordinary instance method with a real return type, so it is directly
  unit-testable by subclassing and overriding `getClient()`/`getContext()` — no `In<T>` unwrapping
  needed in a test.
- The interface-based `TaskDef`/`DagDef` surface is unaffected either way: it already wires edges
  through object references (`dependsOn(extract)`), not through compile-time-generated calls.
- If adopted, the annotation surface drops `In<T>`, `TaskRef<T>`, and `<Class>Ref` codegen
  entirely, along with the "IDE shows unresolved symbols until the first build" experience that
  comes with a generated twin.

## Consequences

- Two authoring surfaces (annotation, interface) both flow into the same `DagDef`/`TaskDef` model.
- The annotation surface's ergonomics are still open: keeping `@Wiring` + a twin is the safer,
  already-scoped design; dropping it for getters is a real simplification but needs the
  wiring-time-vs-run-time dispatch question answered before it can be adopted.

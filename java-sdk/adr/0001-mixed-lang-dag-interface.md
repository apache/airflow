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

# ADR-0001: Mixed-Lang Dag — Java Task Interface

## Status

Proposed

## Context

The Python `@task.stub` call site already defines task data flow. Java tasks should consume those
bindings directly instead of repeating upstream ids with `@Builder.XCom`.

This ADR is scoped to the **Java call-site interface only** — the shape of the argument-binding
spec itself, how it is materialized, and how it travels over the wire is the protocol-level
decision recorded in
[`airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md`](../../airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md).
This ADR only answers: given that spec, what does the Java code a user writes look like?

## Decision

We support 4 syntaxes for the TaskFlow binding:

### 1. Annotation based with positional injection

```java
@Builder.Task(id = "score")
public long score(Client client, long rows, double threshold) { ... }
```

### 2. Annotation based with explicit struct

```java
public static class ReportInput implements TaskInput {
  @ArgName("run_label")
  public String runLabel;

  public long transformed;
}

@Builder.Task(id = "report")
public void report(ReportInput input) {
  log.log(INFO, "Report {0} for transformed value {1}", input.runLabel, input.transformed);
  if (!"nightly".equals(input.runLabel)) {
    throw new RuntimeException("expected run label 'nightly' but got " + input.runLabel);
  }
}
```

### 3. Interface based with explicit struct

```java
public static class SummarizeInput implements TaskInput {
    @ArgName("region_code")
    public String region;

    public long transformed;
  }

  // summarize(region_code=..., transformed=...) is called with keyword
  // arguments, which bind to the bundle's fields by name.
  public static class Summarize implements InputTask<SummarizeInput> {
    public void execute(@NotNull Context context, Client client, SummarizeInput input) {
      log.log(
          INFO, "Summarize region {0} for transformed value {1}", input.region, input.transformed);
      if (!"emea".equals(input.region)) {
        throw new RuntimeException("expected region 'emea' but got " + input.region);
      }
    }
  }
```

### 4. Interface based with `TaskArgs` getter

```java
public static class Transform implements InputTask<TaskArgs> {
  public void execute(@NotNull Context context, Client client, TaskArgs args) {
    var extracted = args.require(0, Long.class);
    var threshold = args.get(1, Double.class);     // null when it resolves to nothing
    log.log(INFO, "Got extracted value from the bound argument: {0}", extracted);
    // ...
  }
}
```

### How

- Annotated task data parameters bind by position; injected `Client` and `Context` do not consume
  positions.
- Keyword arguments bind through one `TaskInput` bundle, with `@ArgName` only when Python and Java
  names differ.
- Interface tasks use `InputTask<TaskArgs>` for positional access or `InputTask<MyInput>` for named
  fields.
- Missing reference or boxed values become `null`; primitive inputs fail clearly.
- `@Builder.XCom` is removed, keeping the Python call site as the single source of data-flow
  wiring.

## Open Questions

- Not sure should we just keep the `TaskArgs` as internal purpose or it's fine to keep it as public
  interface?
- Should we replace the injectable `client` and `context` with explicit getter method (e.g.
  `getClient` or `getContext`) to respect the real argument type on static check? Currently, we
  need to introduce a runtime class injected by `@Wiring` to workaround the type mismatch error.
  (See [ADR-0002](0002-native-dag-interface.md) for where this same question resurfaces on the
  native-Dag side.)

## Consequences

- One binding contract serves both the annotation and interface authoring surfaces.
- `@Builder.XCom` is retired; the Python call site is the only source of data-flow wiring.
- The public surface is not final until the two open questions above are resolved.

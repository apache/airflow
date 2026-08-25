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

# 6. Mixed Lang Dag interface

Date: 2026-08-24

## Status

Proposed. Documents the interface already shipped via #70209 and proposes renaming `Registry.AddDag` to `AddMixedLangDag`.

## Why

A Mixed Lang Dag's graph is defined by a Python `@task.stub` call; a Go function supplies the task body. The call site's arguments already say what data the task needs. The Go function should receive exactly that, plus whatever the SDK itself needs to inject, without either side hand-writing XCom lookups or hard-coded upstream task IDs.

## Example

### Registering the Dag

```go
dag := registry.AddMixedLangDag("etl")
dag.AddTask(transform)
dag.AddTaskWithName("via_struct_arg_tag", ViaStructArgTag)
```

This ADR proposes renaming the shipped `Registry.AddDag(dagId string) Dag` to `AddMixedLangDag`. Go doesn't allow two methods named `AddDag` with different parameter types on the same interface, and [ADR 7](0007-native-dag-interface.md) proposes a second `AddDag(spec v1.DagSpec) Dag` for the Native Dag interface. One of the two names has to move, and this ADR moves the shipped one rather than naming the new one `AddNativeDag`.

This is a breaking rename with no deprecation alias: every existing `registry.AddDag(dagId)` call site would need updating. That cost is acceptable only because the Go SDK's README already warns it is "experimental" and its "APIs, wire protocols, and tooling may change between releases without notice." The compatibility-preserving alternative is `AddNativeDag`-style naming for the new call, leaving the shipped `AddDag` untouched.

Three ways a Go function can receive a stub task's data, all live in `go-sdk/example/bundle/`:

### 1. Flat positional parameters

```python
@task.stub(queue="golang")
def transform(country: str, extracted: dict): ...
```

```go
func transform(
    ctx sdk.TIRunContext,
    client sdk.VariableClient,
    log *slog.Logger,
    country string,
    extracted map[string]any,
) error {
    log.InfoContext(ctx, "transforming", "country", country)
    // ...
    return nil
}
```

### 2. Single struct, `arg:` tag

```python
@task.stub(queue="golang")
def via_struct_arg_tag(region_code: str, threshold: float): ...
```

```go
type ViaStructArgTagInput struct {
    Region    string  `arg:"region_code"`
    Threshold float64 `arg:"threshold"`
}

func ViaStructArgTag(ctx sdk.TIRunContext, log *slog.Logger, input ViaStructArgTagInput) (any, error) {
    log.InfoContext(ctx, "bound struct (arg: tag)", "region", input.Region, "threshold", input.Threshold)
    return map[string]any{"region": input.Region, "threshold": input.Threshold}, nil
}
```

### 3. Single struct, no tag: folded name

```python
@task.stub(queue="golang")
def via_struct_no_tags(region_code: str, threshold: float): ...
```

```go
type ViaStructNoTagsInput struct {
    RegionCode string
    Threshold  float64
}

func ViaStructNoTags(ctx sdk.TIRunContext, log *slog.Logger, input ViaStructNoTagsInput) (any, error) {
    log.InfoContext(ctx, "bound struct (folded name)", "region", input.RegionCode)
    return map[string]any{"region": input.RegionCode}, nil
}
```

Go lowercases `RegionCode` and strips underscores to get `regioncode`, which matches Python's `region_code` automatically.

## How

- Injectable parameters (`sdk.TIRunContext`, `context.Context`, `*slog.Logger`, `sdk.Client`, or a narrower client interface such as `sdk.VariableClient`, `sdk.ConnectionClient`, or `sdk.XComClient`) are recognized by type, in any position, and never consume a data-binding slot. This is `classifyParam` in `go-sdk/pkg/binding/binding.go`, which `task.go` reaches through `binding.Analyze`.
- Everything else is a data parameter, bound either:
  - **positionally**, in declaration order, for flat parameters, or
  - **by field**, for a single struct parameter. An `arg:"..."` tag is matched first; an untagged field falls back to its folded Go field name (`strings.ToLower(strings.ReplaceAll(name, "_", ""))`). Both are implemented in `go-sdk/pkg/binding/binding.go`.
- A struct carrying `arg:` tags cannot be mixed with other data parameters; that combination is rejected at registration. An untagged struct has no such guard: it's accepted alongside other data parameters and decoded positionally as a single value, like any other data parameter.

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

# 6. Mixed Lang task handler interface

Date: 2026-09-07

## Status

Proposed. Keeps the binding rules shipped via #70209 and replaces that PR's
`registry.AddDag(dagId).AddTask(fn)` registration, following the design review on #72043.

## Why

Python owns everything but the body of a Mixed Lang task: `@task.stub` declares the task, its
arguments, and its place in the graph. The Go side has no Dag to define, only handlers to register,
so it should borrow neither Dag vocabulary nor the `AddDag` name that
[ADR 7](0007-native-dag-interface.md) needs. The Go function should receive exactly what the stub
call site passes, with no hand-written XCom lookups or upstream task IDs.

## Example

```go
registry.AddTaskHandlers(
    airflow.TaskHandler("etl", "transform", transform),
    airflow.TaskHandler("etl", "via_struct_arg_tag", ViaStructArgTag),
)
```

`airflow.TaskHandler(dagId, taskId string, fn any)` builds one handler and `AddTaskHandlers` takes
any number, mirroring `registry.AddDags` in [ADR 7](0007-native-dag-interface.md) so a package owning
a family of handlers can return a slice for the caller to splat. task_id stays explicit because it
must match the `@task.stub` id; deriving it from the Go function name, as the shipped `AddTask(fn)`
does, silently rebinds the handler on a rename. This is breaking with no alias, acceptable only
because the SDK's README already warns its APIs "may change between releases without notice."

### Reading SDK values

A handler's only context-shaped parameter is a Go context, and the rest is read off it:

- `airflow.Logger(ctx)` — the task logger.
- `airflow.Client(ctx)` — the coordinator client, embedding `VariableClient`, `ConnectionClient`, and
  `XComClient` (`go-sdk/sdk/sdk.go`).
- `airflow.TIRunContext(ctx)` — the run context (`go-sdk/sdk/context.go`), exposing `TaskInstance()`
  and `DagRun()`.

Three ways a Go function can receive a stub task's data, all live in `go-sdk/example/bundle/`:

### 1. Flat positional parameters

```python
@task.stub(queue="golang")
def transform(country: str, extracted: dict): ...
```

```go
func transform(ctx context.Context, country string, extracted map[string]any) error {
    ti := airflow.TIRunContext(ctx).TaskInstance()
    airflow.Logger(ctx).InfoContext(ctx, "transforming",
        "country", country, "task_id", ti.TaskID, "try_number", ti.TryNumber)

    threshold, err := airflow.Client(ctx).GetVariable(ctx, "etl_threshold")
    if err != nil {
        return fmt.Errorf("etl_threshold: %w", err)
    }
    return writeRows(ctx, extracted, threshold)
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

func ViaStructArgTag(ctx context.Context, input ViaStructArgTagInput) (any, error) {
    return map[string]any{"region": input.Region}, nil
}
```

### 3. Single struct, no tag: folded name

```go
type ViaStructNoTagsInput struct {
    RegionCode string  // matches region_code
    Threshold  float64
}
```

Go lowercases `RegionCode` and strips underscores to get `regioncode`, which matches Python's
`region_code` automatically.

## How

- **The Go context comes first, and nothing else in the signature is context-shaped.** The shipped
  interface injects `sdk.TIRunContext`, `*slog.Logger`, and client interfaces by type in any
  position, which lets a handler declare no context at all and advertises the SDK's context type
  where Go authors expect Go's — even though `sdk.TIRunContext` embeds `context.Context`.
- **The accessors assert on the value the runtime already binds**, so no second context exists. Go
  forbids a func and a type sharing a name in one package, so the accessors take the short names and
  the types stay in `go-sdk/sdk`; a helper should take the narrow interface or the plain
  `sdk.TaskInstance`/`sdk.DagRun` struct it needs. The same rule forced `Dag`/`DagRef` in
  [ADR 7](0007-native-dag-interface.md).
- **Every remaining parameter is data**, bound positionally in declaration order, or by field for a
  single struct parameter: `arg:"..."` first, else the folded Go field name
  (`strings.ToLower(strings.ReplaceAll(name, "_", ""))`). Both in `go-sdk/pkg/binding/binding.go`.
- **A struct carrying `arg:` tags cannot be mixed with other data parameters**, and that combination
  is rejected at registration. An untagged struct has no such guard: it is decoded positionally as a
  single value, like any other data parameter.

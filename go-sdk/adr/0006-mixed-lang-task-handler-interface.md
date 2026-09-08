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

A handler takes exactly one context, `airflow.Context`, as its first parameter, and reads what the
SDK offers off it:

- `actx.Logger()` — the task logger.
- `actx.Client()` — the coordinator client, embedding `VariableClient`, `ConnectionClient`, and
  `XComClient` (`go-sdk/sdk/sdk.go`).
- `actx.TaskInstance()` and `actx.DagRun()` — the identifiers and scheduling timestamps of the
  running task instance (`go-sdk/sdk/context.go`).

`airflow.Context` is an interface embedding `context.Context`, so it *is* a Go context: it goes
straight into `http.NewRequestWithContext(actx, ...)`, and `actx.Done()` fires when the supervisor
asks the task to stop.

Three ways a Go function can receive a stub task's data, all live in `go-sdk/example/bundle/`:

### 1. Flat positional parameters

```python
@task.stub(queue="golang")
def transform(country: str, extracted: dict): ...
```

```go
func transform(actx airflow.Context, country string, extracted map[string]any) error {
    ti := actx.TaskInstance()
    actx.Logger().Info("transforming",
        "country", country, "task_id", ti.TaskID, "try_number", ti.TryNumber)

    threshold, err := actx.Client().GetVariable(actx, "etl_threshold")
    if err != nil {
        return fmt.Errorf("etl_threshold: %w", err)
    }
    return writeRows(actx, extracted, threshold)
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

func ViaStructArgTag(actx airflow.Context, input ViaStructArgTagInput) (any, error) {
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

- **`airflow.Context` comes first, and it is the only context in the signature.** The shipped
  interface injects `sdk.TIRunContext`, `*slog.Logger`, and client interfaces by type in any
  position, so a handler can declare no context at all and the logger arrives separately from the
  context it logs against. Requiring one context that carries everything removes both problems, and
  a plain `context.Context` first parameter would not: reaching the logger through a package-level
  `airflow.Logger(ctx)` accessor lets `myTask(context.Background(), ...)` compile and then fail at
  run time on a missing value, where a required `airflow.Context` fails to build.
- **An interface embedding `context.Context`, not a struct wrapping one.** The
  [context package advises against storing a Context in a struct](https://pkg.go.dev/context#hdr-Contexts_and_structs),
  and a struct is copyable with a nil inner context, which panics on `Done()`; an interface cannot
  hand task code a half-initialised value. This is what `sdk.TIRunContext` already is
  (`go-sdk/sdk/context.go`), so the change is the accessors becoming methods, not a new mechanism.
  Tests build one with the existing `sdk.NewTIRunContext` constructor.
- **Cancellation is the embedded context's,** so graceful termination needs no unwrapping:
  `pkg/execution/server.go` traps `SIGINT`/`SIGTERM` into the context the runtime binds, and a task
  that ignores `actx.Done()` is still stopped by the supervisor's follow-up `SIGKILL`. Cleanup that
  must outlive cancellation uses `context.WithoutCancel(actx)`, which keeps the values and drops the
  cancellation.
- **`airflow.FromContext(ctx) (airflow.Context, bool)` recovers the SDK surface** in a helper that
  only accepts a plain `context.Context`. The runtime already stores the client and run context as
  context values (`pkg/execution/task_runner.go`), so this is a typed lookup over what is there.
- **Every remaining parameter is data**, bound positionally in declaration order, or by field for a
  single struct parameter: `arg:"..."` first, else the folded Go field name
  (`strings.ToLower(strings.ReplaceAll(name, "_", ""))`). Both in `go-sdk/pkg/binding/binding.go`.
- **A struct carrying `arg:` tags cannot be mixed with other data parameters**, and that combination
  is rejected at registration. An untagged struct has no such guard: it is decoded positionally as a
  single value, like any other data parameter.

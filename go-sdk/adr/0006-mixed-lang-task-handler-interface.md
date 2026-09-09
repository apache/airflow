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

Proposed. Keeps the argument binding shipped in #70209, replaces its registration API, and reshapes
the task signature after the design review on #72043.

## Decision

1. A Go bundle registers **task handlers, not Dags**:
   `registry.Register(airflow.TaskHandler(dagId, taskId, fn), ...)`. `Register` is the bundle's
   single registration verb and takes native Dags in the same call
   ([ADR 7](0007-native-dag-interface.md)).
2. **task_id is always written out**; nothing is derived from the Go function name.
3. **Every handler takes an `airflow.Context` first**, and nothing else is injected — a struct
   embedding `context.Context`, exposing `Logger()`, `Client()`, `TaskInstance()`, and `DagRun()`.
4. **Every remaining parameter is data**, bound positionally, or by field when it is a single struct:
   `arg:"..."` when tagged, else the folded Go field name.
5. This **breaks** `registry.AddDag(dagId).AddTask(fn)`, with no deprecation alias.

## Context

Python owns everything but the body of a Mixed Lang task: `@task.stub` declares the task, its
arguments, and its place in the graph. The Go side has no Dag to define, so Dag vocabulary misleads,
and it occupies the `AddDag` name that [ADR 7](0007-native-dag-interface.md) needs. The shipped
signature injects `sdk.TIRunContext`, `*slog.Logger`, and clients by type in any position, so a
handler can declare no context at all and its logger arrives separately from the context it logs
against; one required context carrying the rest fixes both.

## Example

```go
registry.Register(
    airflow.TaskHandler("etl", "transform", transform),
    airflow.TaskHandler("etl", "via_struct_arg_tag", ViaStructArgTag),
)
```

One bundle usually has both kinds, and one call lists everything it provides:

```go
registry.Register(
    nativeEtl, // *airflow.DagRef, from ADR 7
    airflow.TaskHandler("py_etl", "transform", transform),
)
```

Three ways a Go function receives a stub task's data, all live in `go-sdk/example/bundle/`.

**Flat positional**, for `def transform(country: str, extracted: dict)`:

```go
func transform(actx airflow.Context, country string, extracted map[string]any) error {
    actx.Logger().Info("transforming", "country", country, "try", actx.TaskInstance().TryNumber)

    threshold, err := actx.Client().GetVariable(actx, "etl_threshold")
    if err != nil {
        return err
    }
    return writeRows(actx, extracted, threshold)
}
```

**A single `arg:`-tagged struct**, for `def via_struct_arg_tag(region_code: str, threshold: float)`:

```go
type ViaStructArgTagInput struct {
    Region    string  `arg:"region_code"`
    Threshold float64 `arg:"threshold"`
}

func ViaStructArgTag(actx airflow.Context, input ViaStructArgTagInput) (any, error)
```

**A single untagged struct**, where the field name folds to the Python argument: `RegionCode`
lowercased with underscores stripped is `regioncode`, which matches `region_code`.

## Consequences

- Every existing `registry.AddDag(dagId)` call site changes, acceptable only because the SDK's README
  already warns its APIs "may change between releases without notice."
- A class of mistake becomes a compile error: `myTask(context.Background(), ...)` no longer builds,
  where a package-level `airflow.Logger(ctx)` accessor over a plain context would have compiled and
  then failed at run time on a missing value.
- Graceful termination needs no unwrapping — `actx.Done()` fires on supervisor shutdown, and
  `http.NewRequestWithContext(actx, ...)` accepts it — while cleanup that must outlive cancellation
  uses `context.WithoutCancel(actx)`.
- A helper typed as a plain `context.Context` recovers the methods with
  `airflow.FromContext(ctx) (airflow.Context, bool)`.

## Appendix: Implementation Notes

- **A struct embedding `context.Context`, not an interface.** The context package's advice against
  [storing a Context in a struct](https://pkg.go.dev/context#hdr-Contexts_and_structs) is about
  domain types — a `DagRun` holding a request-scoped context — not about a purpose-built context
  type; `context.WithValue` itself returns a struct. A struct fits because only the SDK implements
  this type: methods can be added as the context grows without breaking anyone, and godoc has a
  concrete type to document. The shipped `sdk.TIRunContext` (`go-sdk/sdk/context.go`) is an
  interface, justified in its doc comment by that same misreading, so the comment is corrected along
  with the change.
- **The constructor is the supported way to build one.** Every field but the embedded context is
  unexported, so no caller can substitute a logger or client; the constructor takes those parts, and
  what is worth faking in a task test is already an interface (`sdk.Client`, and the logger through
  an `slog.Handler`). Embedding does leave `Context` exported as a field name, so
  `airflow.Context{Context: ctx}` compiles and yields a value whose `Logger()` and `Client()` are
  nil. Closing that off would mean an unexported field plus four delegating methods (`Deadline`,
  `Done`, `Err`, `Value`) in place of promotion, which is not worth it for a value the runtime
  supplies. What a struct gives up either way is a user-authored stand-in for the whole context.
- **Cancellation and the value lookup already exist.** `pkg/execution/server.go` traps
  `SIGINT`/`SIGTERM` into the context the runtime binds, and `pkg/execution/task_runner.go` stores
  the client and run context as values on it — which is what makes `airflow.FromContext` a typed
  lookup rather than new plumbing. A task ignoring `actx.Done()` is still stopped by the supervisor's
  follow-up `SIGKILL`.
- **Binding** lives in `go-sdk/pkg/binding/binding.go`: `classifyParam` for the signature, the field
  fallback as `strings.ToLower(strings.ReplaceAll(name, "_", ""))`. A struct carrying `arg:` tags
  cannot be mixed with other data parameters and is rejected at registration; an untagged struct has
  no such guard and is decoded positionally as one value.
- **One verb, over a sealed interface.** `Registry.Register(items ...airflow.Registration)` accepts
  both kinds because `*airflow.DagRef` and `airflow.TaskHandlerRef` both satisfy `Registration`, an
  exported interface whose only method is unexported. That closes the set to the SDK's own types, and
  a Dag author never writes the name — the same technique as `airflow.TaskOption` in
  [ADR 7](0007-native-dag-interface.md), one level up. Two verbs would have forced a bundle to split
  what it provides across separate calls for no gain in safety, since the interface already rejects
  anything else at compile time.
- **Why variadic**, rather than a one-handler-per-call `Register(dagId, taskId, fn)`, which repeats
  the dag_id once per task, or a stateful `handlers.Add(...)` receiver, which adds a variable and a
  statement per task. A package owning a family of handlers can also return a slice for the caller
  to splat.

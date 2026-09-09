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

# 6. Bundle registration and Mixed Lang stub tasks

Date: 2026-09-09

## Status

Proposed. Keeps the argument binding shipped in #70209, replaces the provider-and-registry
registration flow around it, and reshapes the task signature, following the design review on #72043.

## Decision

1. **A bundle is a value the author builds.** `airflow.Bundle()` returns a `*airflow.BundleRef`;
   `main` reads build, register, serve, with `bundle.Serve()` as its last statement.
2. **`bundle.Register(items ...airflow.Registration)`** is the single registration verb, taking
   native Dags ([ADR 7](0007-native-dag-interface.md)) and stub tasks in any mix.
3. **A Go bundle registers stub tasks, not Dags**: `airflow.StubTask(dagId, taskId, fn)`, named
   for the Python `@task.stub` it implements.
4. **task_id is always written out**; nothing is derived from the Go function name.
5. **Every task function takes an `airflow.Context` first**, and nothing else is injected — a struct
   embedding `context.Context`, exposing `Logger()`, `Client()`, `TaskInstance()`, and `DagRun()`.
6. **Every remaining parameter is data**, bound positionally, or by field when it is a single struct:
   `arg:"..."` when tagged, else the folded Go field name.
7. This **breaks** `BundleProvider`, `Registry`, and `AddDag(dagId).AddTask(fn)`, with no
   deprecation alias.

## Context

Python owns everything but the body of a Mixed Lang task: `@task.stub` declares the task, its
arguments, and its place in the graph. The Go side has no Dag to define, so Dag vocabulary misleads,
and it occupies the `AddDag` name that [ADR 7](0007-native-dag-interface.md) needs.

Registration is inverted today. An author declares a struct with no state, asserts it implements
`v1.BundleProvider`, fills in `RegisterDags(dagbag v1.Registry) error`, and hands the struct to
`bundlev1server.Serve` — three concepts and an empty type before a single task is declared. The two
names are also one object: `Registry` is `Bundle` plus `AddDag`, the write side of the value that
later answers task lookups at execution time.

The shipped signature then injects `sdk.TIRunContext`, `*slog.Logger`, and clients by type in any
position, so a task function can declare no context at all and its logger arrives separately from the
context it logs against. One required context carrying the rest fixes both.

## Example

```go
func main() {
    bundle := airflow.Bundle()

    bundle.Register(
        nativeEtl, // *airflow.DagRef, from ADR 7
        airflow.StubTask("py_etl", "transform", transform),
        airflow.StubTask("py_etl", "via_struct_arg_tag", ViaStructArgTag),
    )

    if err := bundle.Serve(); err != nil {
        log.Fatal(err)
    }
}
```

Registration can be spread across packages, either by passing the bundle along or by returning
`[]airflow.Registration` for the caller to splat: `bundle.Register(taskflowbinding.StubTasks()...)`.

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

- `BundleProvider`, the empty provider struct, and the `bundlev1server` package all disappear from an
  author's `main`; `Serve` becomes a method on the value they already hold.
- **Registration closes when `Serve` is called.** Registering afterwards is a programming error and
  panics, like every other registration-time check in these ADRs.
- Every existing call site changes, acceptable only because the SDK's README already warns its APIs
  "may change between releases without notice."
- Requiring `airflow.Context` turns a class of mistake into a compile error: a test calling
  `myTask(context.Background(), ...)` no longer builds, where a package-level `airflow.Logger(ctx)`
  accessor over a plain context would have compiled and then failed at run time on a missing value.
- Graceful termination needs no unwrapping — `actx.Done()` fires on supervisor shutdown, and
  `http.NewRequestWithContext(actx, ...)` accepts it — while cleanup that must outlive cancellation
  uses `context.WithoutCancel(actx)`.
- A helper typed as a plain `context.Context` recovers the methods with
  `airflow.FromContext(ctx) (airflow.Context, bool)`.

## Appendix: Implementation Notes

- **One verb, over a sealed interface.** `Bundle.Register(items ...airflow.Registration)` accepts
  both kinds because `*airflow.DagRef` and `airflow.StubTaskRef` satisfy `Registration`, an
  exported interface whose only method is unexported. That closes the set to the SDK's own types, and
  an author never writes the name — the same technique as `airflow.TaskOption` in
  [ADR 7](0007-native-dag-interface.md). Two verbs would have split what a bundle provides across
  separate calls for no gain in safety, since the interface already rejects anything else at compile
  time. Variadic, rather than one stub task per call, which repeats the dag_id per task.
- **`airflow.Bundle()` follows the same rule as `Dag` and `Task`**: the constructor takes the noun,
  the handle is `*airflow.BundleRef`. Carving out an exception — `NewBundle()` returning
  `*airflow.Bundle` — would buy a better type name for helper signatures, but the recommended way to
  spread registration is a package returning `[]airflow.Registration` rather than passing the bundle
  around, so the type name rarely appears. Keeping the rule mechanical also leaves room for a
  `BundleSpec` argument later, exactly as `airflow.Dag(spec)` takes one. It takes none today: the
  bundle's name is Airflow-side configuration, arriving as `bundle_name` on the wire, and the
  manifest the executable prints carries only the SDK version and the Dag list
  (`go-sdk/internal/airflowmetadata/airflowmetadata.go`).
- **`Serve` keeps both modes the executable already has**, selected by flags in
  `go-sdk/bundle/bundlev1/bundlev1server/server.go`: `--airflow-metadata` prints the manifest JSON
  ([ADR 2](0002-use-go-tool-directive-for-bundle-packer.md),
  [ADR 4](0004-self-contained-executable-bundle.md)), and `--comm`/`--logs` runs the
  msgpack-over-IPC coordinator path ([ADR 3](0003-coordinator-protocol-msgpack-ipc.md)). It returns
  an error rather than exiting, so `main` reports it.
- **The value-first shape already half exists**: `bundlev1.New() Registry`
  (`go-sdk/bundle/bundlev1/registry.go`) builds a registry outside the provider callback, documented
  as useful for unit-testing a `RegisterDags` implementation. Making it the only shape removes the
  callback rather than adding a mechanism.
- **A struct embedding `context.Context`, not an interface.** The context package's advice against
  [storing a Context in a struct](https://pkg.go.dev/context#hdr-Contexts_and_structs) is about
  domain types — a `DagRun` holding a request-scoped context — not about a purpose-built context
  type; `context.WithValue` itself returns a struct. A struct fits because only the SDK implements
  this type: methods can be added as the context grows without breaking anyone, and godoc has a
  concrete type to document. The shipped `sdk.TIRunContext` (`go-sdk/sdk/context.go`) is an
  interface, justified in its doc comment by that same misreading, so the comment is corrected along
  with the change.
- **The constructor is the supported way to build a context.** Every field but the embedded context
  is unexported, so no caller can substitute a logger or client; what is worth faking in a task test
  is already an interface (`sdk.Client`, and the logger through an `slog.Handler`). Embedding does
  leave `Context` exported as a field name, so `airflow.Context{Context: ctx}` compiles and yields a
  value whose `Logger()` and `Client()` are nil; closing that off would mean an unexported field plus
  four delegating methods in place of promotion.
- **Binding** lives in `go-sdk/pkg/binding/binding.go`: `classifyParam` for the signature, the field
  fallback as `strings.ToLower(strings.ReplaceAll(name, "_", ""))`. A struct carrying `arg:` tags
  cannot be mixed with other data parameters and is rejected at registration; an untagged struct has
  no such guard and is decoded positionally as one value.
- **Cancellation and the value lookup already exist.** `pkg/execution/server.go` traps
  `SIGINT`/`SIGTERM` into the context the runtime binds, and `pkg/execution/task_runner.go` stores
  the client and run context as values on it — which is what makes `airflow.FromContext` a typed
  lookup rather than new plumbing. A task ignoring `actx.Done()` is still stopped by the supervisor's
  follow-up `SIGKILL`.

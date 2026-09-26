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

# ADR-0002: Native TypeScript Dag — Interface Design

## Status

Proposed. Revised after the review on #72047.

## Decision

1. **`dag.task(handler)` returns a factory, and the task id is optional.** With no id the task takes
   the handler's function name (`dag.task(extract)` → task `"extract"`); `dag.task(taskId, handler)`
   sets it explicitly, which an anonymous handler must do. A handler takes one object of named
   arguments, and calling the factory both places the task in the Dag and names each of its inputs
   (`load({ total: transform({ rows: extract(), region: "us" }) })`) — the shape Python TaskFlow uses
   for `load(transformed=transform(...))`. `withArgList(...)` supplies the same inputs in the order
   the handler destructures them, for a call that reads better that way.
2. **The call graph is the task graph.** `tsc` checks every named input against the handler's own
   argument type, and a `TaskRef` exists only once its producing call has returned, so a cycle
   through arguments is unrepresentable rather than rejected by a validator. A listed call is checked
   by value type, and which argument each value supplies is the position it was given in.
3. **Every task is called exactly once.** An uncalled task fails when the Dag is read, so none can be
   silently left out of the graph.
4. **`before` and `after` draw order-only edges** — the TypeScript pair for `>>` and `<<`, both
   variadic so one call fans out.
5. **The Dag file owns Dag-level and task-level configuration.** `new Dag(dagId, spec)` carries the
   schedule and the rest of `DagSpec`; `dag.task(handler, spec)` — or `dag.task(taskId, handler, spec)` —
   carries per-task options such as retries. Python owns both in the mixed-language case
   ([ADR-0001](0001-mixed-lang-dag-interface.md)), which is the difference between the two modes.
6. **A handler is a plain function of its own data**; `getContext()` and `getClient()` supply the rest.
7. **One registration verb**: `bundle.register(dag)`, the same call that takes task handlers, with
   `await bundle.serve()` starting the runtime ([ADR-0001](0001-mixed-lang-dag-interface.md)).

## Context

A Dag authored with no Python stub file has no `@task.stub` call site to declare its graph, so
TypeScript itself must express everything Python would otherwise own: the schedule and the rest of
the Dag-level configuration, each task's own options, the graph, and the task bodies. This ADR covers only what that
call site looks like for a user. `Dag` here is exclusively the native case; the mixed-language case
registers task handlers instead ([ADR-0001](0001-mixed-lang-dag-interface.md)). Both share the
protocol substrate recorded in
[`airflow-core/adr/lang-sdk/0007`](../../airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md).

## Example

```ts
import { Bundle, Dag, getClient } from "apache-airflow-ts-sdk";

const dag = new Dag("ts_etl", { schedule: "@daily", catchup: false, tags: ["etl"] });

const extract = dag.task("extract", async (): Promise<number> => 42);

const transform = dag.task("transform", async ({ extracted }: { extracted: number }) => extracted * 2);

const load = dag.task(
  "load",
  async ({ transformed }: { transformed: number }) => {
    await getClient().setXCom({ key: "loaded", value: transformed });
  },
  { retries: 2 },
);

const extracted = extract();
const transformed = transform({ extracted });
const loaded = load({ transformed });

const bundle = new Bundle();
bundle.register(dag);
await bundle.serve();
```

The schedule on the `Dag` and the retries on `load` are the point of a native Dag: nothing outside
this file declares them. A mixed-language handler cannot carry either, because the Python Dag it
belongs to already does.

One statement per task, with each ref named, is the form to write. Nesting the calls
(`load({ transformed: transform({ extracted: extract() }) })`) is legal and equivalent, but it is
shorthand for a two-task chain, not the general shape: a Dag of twenty tasks reads as twenty flat
statements, never as a twenty-deep expression.

### Omitting the task id

A native task defaults its id to the handler's function name, so a named handler needs none:

```ts
const extract = dag.task(async function extract(): Promise<number> {
  return 42;
});
// task id "extract"
```

The id comes from the handler's *source* name, resolved when the bundle is packed and written into
the registration — not from `handler.name` at runtime, which minification renames (see
Implementation Notes). A handler with no source name — a bare anonymous arrow passed inline,
`dag.task(async () => 42)` — has nothing to resolve and is a compile error until given an explicit
id. This default is for native Dags, where both ends of every name are TypeScript; a mixed-language
handler names the Python-owned task explicitly and does not default from the handler's function
name ([ADR-0001](0001-mixed-lang-dag-interface.md), decision 3).

The `TaskSpec` also carries the task id, so it can be set alongside the other task options:

```ts
const extract = dag.task(async () => 42, { taskId: "extract", retries: 2 });
```

### Order-only edges: `>>` and `<<`

An edge that carries no data has no key to put in the wiring object, so it is drawn directly between
refs:

```ts
const cleaned = cleanup();

cleaned.after(loaded, transformed); // [loaded, transformed] >> cleaned
loaded.before(cleaned); // loaded >> cleaned
```

Both return their own receiver, since a fan-out has no single "next" ref to hand back. Fan-*in* with
data is the wiring object itself — `summarize({ north: extractNorth(), south: extractSouth() })` — so
`[a, b] >> c` has an answer in each direction: named keys when values flow, `after` when only order
does. This matches `Before`/`After` in the Go SDK's native Dag interface, spelled to TypeScript
convention.

### Conditional branching: `if` and `else`

`dag.if(condition)` is TypeScript's spelling of the construct
[`airflow-core/adr/lang-sdk/0008`](../../airflow-core/adr/lang-sdk/0008-control-flow-constructs.md)
names after the host language's control flow:

```ts
const gated = dag.task("has_rows", async ({ rows }: { rows: number }) => rows > 0)({
  rows: extracted,
});

dag.if(gated).then(loadIfReady).else(loadFallback);
```

**The condition is a task reference, not a task id and a function.** `dag.if` takes a `TaskRef` the
Dag already handed back, whose handler's return type the compiler checks is `boolean`. So nothing
depends on the task's id or on its function name, and a condition is declared, named and typed the
same way every other task is. The same reasoning that makes a *case* a reference in that ADR's
decision 2 applies to the condition itself.

**A `then` chain is a thenable, and is guarded rather than avoided.** An object with a callable
`then` is a *thenable*: were the object `dag.if` returns to reach an `await`, the runtime would hand
its `then` a resolve function where a task reference belongs. Two things contain that. `.then(...)`
returns an object carrying only `.else`, so nothing past the first step is awaitable at all; and
`.then` rejects a function argument by naming the cause, so an author who does await it reads "this
builds a branch, drop the await" rather than a type error about references.

**A branch is a real branch to Airflow.** The control edges serialize as ordinary order-only edges
and carry no branch-candidate field, as that ADR's consequences require. The condition task is
serialized with `_can_skip_downstream`, and it writes the `skipmixin_key` XCom alongside the skip, so
clearing a skipped branch re-skips it the way a Python `@task.branch` does rather than running the
side the condition rejected.

A one-sided `if` is a branch with one candidate — it skips `then` and follows nothing — rather than a
`ShortCircuitOperator`, which would also skip the whole downstream closure and ignore trigger rules.
A guarded task takes no argument for the control edge: a condition's boolean is a signal, not data.

### Multi-way branching: `switch` and `case`

`dag.switch(decider)` follows
[`airflow-core/adr/lang-sdk/0008`](../../airflow-core/adr/lang-sdk/0008-control-flow-constructs.md)
decision 2 without divergence: a case **is** the reference the SDK handed back, not a label kept in
step with one. The decider is one too.

```ts
const decider = dag.task("pick_path", async ({ rows }: { rows: number }) =>
  rows > 1000 ? handleLong : handleShort,
);
const picked = decider({ rows: extracted });

dag.switch(picked).case(handleLong).case(handleShort);
```

An earlier draft selected a case by a string label the author writes, on the grounds that a handler's
function name does not survive bundling. That concern does not apply: a `TaskRef` carries the task's
own id, which the SDK fixed when the task was declared and esbuild never touches. Selecting by
reference keeps the compiler checking that a candidate exists, which a label cannot.

The cases chain, as they do in Go. `case` reads the candidate list when the task runs rather than
when it is declared, which is what lets the chain follow the `dag.switch(...)` call; and unlike a
condition's `then`, `case` is not a thenable trap, so nothing has to be guarded here.

**No default case**, per decision 3, and **exactly one case is selected**, the limitation that ADR
records for every Lang SDK. A branch with no case at all decides nothing, and is rejected when the
Dag is read.

## Consequences

- One authoring surface (`dag.task()` plus its factory) covers the graph and each task's arguments,
  and `before`/`after` cover edges that carry nothing.
- Handlers are unit-testable as plain functions of their data, with no SDK fixture to construct.
- `DagSpec` and `TaskSpec` are empty placeholders today — `Record<string, never>`
  (`ts-sdk/src/sdk/dag.ts`), so `new Dag("d", { schedule: "@daily" })` is currently a compile error
  by design. Native declaration is what fills them, generated from the serialized-Dag JSON schema the
  way `src/generated/supervisor.ts` is. This ADR does not choose those fields; it fixes where an
  author writes them.
- `TaskOptions` carries the task's spec and nothing else: the names on the wire are the keys of the
  call itself. With wiring moved to the factory call, `inputs` is no longer an option.
- `TaskHandlerArgs` is removed from the public API, `DagRegistry` becomes `Bundle`, and
  `serveDags(registry)` becomes `bundle.serve()`, which breaks
  0.1.0-beta1 authors; see [ADR-0001](0001-mixed-lang-dag-interface.md) for the shipped call sites
  that change.

## Alternatives

- **Positional handlers**, `async (rows: number, region: string) => ...`, offered first and then
  dropped: a positional parameter list has no names on the wire unless the SDK reads them out of the
  handler's source, and a single object of named arguments is what a TypeScript library takes
  anyway. The handler is object-only, and `withArgList(...)` keeps the positional call site for the
  cases that read better in order.
- **Injected `ctx`/`client` arguments**, mimicking the Python signature. Rejected, per the above and
  because feeling native to TypeScript matters more than matching Python's parameter list.

## Appendix: Implementation Notes

- **`getClient()` and `getContext()` read from an `AsyncLocalStorage`** (`node:async_hooks`) store
  that the runtime wraps around the handler call, at the single dispatch site in
  `ts-sdk/src/coordinator/runtime.ts`. The store propagates across every `await` and every promise
  created inside that scope by construction. What it does not cover is work that outlives the
  handler: a floating promise still calling `getClient()` after the handler resolved runs after the
  task's success has been reported, which is equally true of a closed-over client today.
- **A handler's parameter type is exactly its own data.** An earlier draft merged `ctx`/`client` into
  the same object, which forced every typed handler to declare `TArgs & TaskHandlerArgs` and left the
  top-level argument namespace open to collisions with an author's own parameter names. Getters close
  both.
- **The spec argument already has its slot.** `dag.task(taskId, handler, options)` reads `{ spec = {} }`
  and runs `validateEmptySpec` on it (`ts-sdk/src/sdk/dag.ts`), so task fields land on a path that
  exists rather than a new one.
- **A listed call binds by order, and the names still come from the handler.** The serialized Dag
  names every argument, so `withArgList(...)` is zipped with the keys the handler destructures, read
  off its argument pattern at the call. A pattern that cannot be read that way, such as one with a
  default or a rest element, is refused rather than guessed at, and its task is called by name.
- **A `TaskRef` is inert** — a handle for wiring, not a promise. Nothing in a Dag file executes a task
  body.
- **A defaulted task id is read off the handler itself.** The pack step (`ts-sdk/src/cli/pack.ts`)
  minifies and passes esbuild's `keepNames`, so `handler.name` is the author's in a packed bundle as
  much as in one run from source; an argument name is a property name, which minification leaves
  alone. Rewriting the call at pack time was tried first and dropped: it needed a TypeScript parser
  in the packer to tell a real `.task(` from one inside a string or a comment, and it could not see a
  handler declared in another module. A handler with no name leaves nothing to read, which is why an
  anonymous handler must state its id.
- **`withArgNames` and the name folding behind it** ([ADR-0001](0001-mixed-lang-dag-interface.md))
  exist for the mixed-language case and are never needed here: both ends of every name are
  TypeScript, so `tsc` checks the wiring end to end and there is no foreign name to reconcile.
- **`bundle.register(...)` takes any number of Dags and task handlers**, since each carries its own
  ids; the earlier `registerDag`/`registerTaskHandler` split is recorded as a rejected alternative in
  [ADR-0001](0001-mixed-lang-dag-interface.md).

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

1. **`dag.task(taskId, handler)` returns a factory.** Calling the factory both places the task in the
   Dag and supplies its arguments by name — the shape Python TaskFlow uses for
   `load(transformed=transform(...))`.
2. **The call graph is the task graph.** `tsc` checks every wired key against the handler's own
   parameter type, and a `TaskRef` exists only once its producing call has returned, so a cycle
   through arguments is unrepresentable rather than rejected by a validator.
3. **Every task is called exactly once.** An uncalled task fails when the Dag is read, so none can be
   silently left out of the graph.
4. **`before` and `after` draw order-only edges** — the TypeScript pair for `>>` and `<<`, both
   variadic so one call fans out.
5. **The Dag file owns Dag-level and task-level configuration.** `new Dag(dagId, spec)` carries the
   schedule and the rest of `DagSpec`; `dag.task(taskId, handler, spec)` carries per-task options
   such as retries. Python owns both in the mixed-language case
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

## Consequences

- One authoring surface (`dag.task()` plus its factory) covers the graph and each task's arguments,
  and `before`/`after` cover edges that carry nothing.
- Handlers are unit-testable as plain functions of their data, with no SDK fixture to construct.
- `DagSpec` and `TaskSpec` are empty placeholders today — `Record<string, never>`
  (`ts-sdk/src/sdk/dag.ts`), so `new Dag("d", { schedule: "@daily" })` is currently a compile error
  by design. Native declaration is what fills them, generated from the serialized-Dag JSON schema the
  way `src/generated/supervisor.ts` is. This ADR does not choose those fields; it fixes where an
  author writes them.
- `TaskOptions` collapses into `TaskSpec`. The shipped third argument to `dag.task` is
  `{ inputs, spec }`; with wiring moved to the factory call, `inputs` is no longer an option and the
  third argument is the spec itself.
- `TaskHandlerArgs` is removed from the public API, `DagRegistry` becomes `Bundle`, and
  `serveDags(registry)` becomes `bundle.serve()`, which breaks
  0.1.0-beta1 authors; see [ADR-0001](0001-mixed-lang-dag-interface.md) for the shipped call sites
  that change.

## Alternatives

- **Positional wiring** (`load(transform(extract()))`), which becomes expressible once data no longer
  shares an object with `ctx`/`client`, since a handler can then take its arguments positionally and
  `Parameters<typeof handler>` is a real tuple. Rejected: it removes the key names from every call
  site, and those names are what keeps flat, one-statement-per-task wiring readable at twenty tasks.
  A handler may still take several positional arguments; only the *wiring* stays named.
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
- **The spec argument already has its slot.** `dag.task(taskId, handler, options)` reads
  `{ inputs = {}, spec = {} }` and runs `validateEmptySpec` on the spec today
  (`ts-sdk/src/sdk/dag.ts`), so task fields land on a path that exists rather than a new one.
- **A `TaskRef` is inert** — a handle for wiring, not a promise. Nothing in a Dag file executes a task
  body.
- **`withArgNames` and the name folding behind it** ([ADR-0001](0001-mixed-lang-dag-interface.md))
  exist for the mixed-language case and are never needed here: both ends of every name are
  TypeScript, so `tsc` checks the wiring end to end and there is no foreign name to reconcile.
- **`bundle.register(...)` takes any number of Dags and task handlers**, since each carries its own
  ids; the earlier `registerDag`/`registerTaskHandler` split is recorded as a rejected alternative in
  [ADR-0001](0001-mixed-lang-dag-interface.md).

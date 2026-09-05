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

Proposed

## Context

A Dag authored with no Python stub file has no `@task.stub` call site to declare its graph, so TypeScript itself must express both the graph and the task bodies. This ADR covers only what that TypeScript call site looks like for a user. `Dag` here is exclusively the native case; the mixed-language case is the separate `MixedLangDag` class, covered in [ADR-0001](0001-mixed-lang-dag-interface.md). This ADR shares the injectable `ctx`/`client` question raised there, and shares its protocol substrate (the argument-binding spec) with [`airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md`](../../airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md).

## Decision

`dag.task(taskId, handler)` returns a factory. Calling the factory both places the task in the Dag and supplies its arguments by name, the same shape Python TaskFlow itself uses for `load(transformed=transform(...))`:

```ts
const dag = new Dag("ts_etl");

const extract = dag.task("extract", async ({ client }): Promise<number> => {
  const rows = 42;
  await client.setXCom({ key: "row_count", value: rows });
  return rows;
});

interface TransformArgs {
  extracted: number;
}

const transform = dag.task(
  "transform",
  async ({ extracted }: TransformArgs & TaskHandlerArgs) => extracted * 2,
);

interface LoadArgs {
  transformed: number;
}

const load = dag.task("load", async ({ ctx, client, transformed }: LoadArgs & TaskHandlerArgs) => {
  if (transformed <= 0) {
    throw new Error(`task ${ctx.taskId} received a non-positive value: ${transformed}`);
  }
  await client.setXCom({ key: "loaded", value: transformed });
});

load({ transformed: transform({ extracted: extract() }) });
```

The call graph is the task graph. `tsc` checks every wired key against the handler's own parameter type, and a `TaskRef` only exists once its producing call has returned, so a cycle is unrepresentable rather than merely rejected by a validator. Every task must be called exactly once; an uncalled task fails when the Dag is read, so a task can't be silently left out of the graph.

## If `ctx`/`client` were real getter methods instead of injected arguments

The intersection type above, `TransformArgs & TaskHandlerArgs`, exists for one reason: today's handler signature carries `ctx`/`client` as arguments, so a handler that wants type safety on its own data has to say so explicitly. If `ctx`/`client` came from getter functions instead, a handler's parameter type would be exactly its own data:

```ts
// Before: ctx/client share the handler's one argument object, so
// TransformArgs must be intersected with TaskHandlerArgs.
const transform = dag.task(
  "transform",
  async ({ extracted }: TransformArgs & TaskHandlerArgs) => extracted * 2,
);
```

```ts
// After: ctx/client come from getters, so the handler's parameter type is
// exactly its own data, with no TaskHandlerArgs intersection needed.
import { getClient } from "@apache-airflow/ts-sdk";

const transform = dag.task("transform", async ({ extracted }: TransformArgs) => {
  const client = getClient();
  await client.setXCom({ key: "doubled", value: extracted * 2 });
  return extracted * 2;
});
```

Open question this ADR does not resolve:

- What backs `getClient()`/`getContext()` at run time. A Node `AsyncLocalStorage` scoped to the handler's execution is the likely mechanism, but confirming it survives every `await` inside a handler, and any user-spawned concurrency, is a runtime-dispatch question, not a call-site question. This ADR only proposes the shape.

If that question is answered, the implications are:

- The top-level argument namespace closes automatically: `ctx`/`client` can never collide with a Dag author's own parameter name, because they no longer share an object with one.
- A handler becomes directly unit-testable with a plain data argument. No `TaskHandlerArgs` fixture is needed, and there's no risk of forgetting to intersect it in.
- The wiring surface (`TaskInputs<TArgs>`, the mapped type checked at the call site) is unaffected either way, since it already derives from the handler's own declared parameter type, whichever shape that type takes.

### This also reopens positional wiring

The "after" handler above still takes one object (`{ extracted }: TransformArgs`), out of habit. But the *reason* wiring is named rather than positional today is that a handler takes one destructured object, so `Parameters<typeof handler>` is always a one-element tuple with no per-field position to check against. That reason goes away once data no longer has to share an object with `ctx`/`client`: a handler can drop the object entirely and take its arguments positionally, the way an ordinary TypeScript function does.

```ts
// Before (named wiring, current): the wiring object's keys match each
// handler's destructured object.
const transform = dag.task(
  "transform",
  async ({ extracted }: TransformArgs & TaskHandlerArgs) => extracted * 2,
);
const load = dag.task("load", async ({ transformed }: LoadArgs & TaskHandlerArgs) => {
  /* ... */
});
load({ transformed: transform({ extracted: extract() }) });
```

```ts
// After (positional wiring, hypothetical): each handler takes its data
// positionally, so `Parameters<typeof handler>` is a real tuple (`[number]`
// for both transform and load), and a positional TaskFactory can type-check
// a call against it, the same way today's `Wiring<TParams>` type-checks a
// named one.
const extract = dag.task("extract", async () => 42);
const transform = dag.task("transform", async (extracted: number) => extracted * 2);
const load = dag.task("load", async (transformed: number) => {
  const client = getClient();
  await client.setXCom({ key: "loaded", value: transformed });
});

load(transform(extract()));
```

This is a second, independent decision stacked on top of the getters question above. A handler could still take one object for its data even without `ctx`/`client` in it, purely for self-documenting call sites once there are several arguments. Positional wiring only follows if handlers *also* drop the object, and it trades that self-documentation away at every multiple-argument call site. Neither this ADR nor the getters question settles it. It's recorded because positional wiring was previously ruled out entirely, for a reason that no longer holds once `ctx`/`client` are getters.

## Consequences

- One authoring surface (`dag.task()` + factory) covers both the graph and each task's arguments.
- `DagSpec`/`TaskSpec` are unaffected by this ADR. They are the language-neutral Dag configuration surface, not the argument-binding one.
- Whether `ctx`/`client` stay injected arguments or become getters is still open; adopting getters is a real ergonomics win but depends on the runtime-dispatch question above.
- If getters land, positional data parameters become possible for the first time, and with them, positional wiring (`load(transform(extract()))`). Adopting that is a further, separate, unresolved trade-off against today's named, self-documenting convention.

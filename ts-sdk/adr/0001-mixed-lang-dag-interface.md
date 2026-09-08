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

# ADR-0001: Mixed-Lang Dag — TypeScript Task Handler Interface

## Status

Proposed. Revised after the review on #72047.

## Context

The Python `@task.stub` call site already defines task data flow. TypeScript tasks should consume those bindings directly as named arguments, instead of re-fetching each value with `client.getXCom(...)`.

This ADR covers only the TypeScript call-site interface. The argument-binding spec itself (its shape, how it's materialized, how it travels over the wire) is a separate, protocol-level decision recorded in [`airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md`](../../airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md). Given that spec, this ADR only answers what TypeScript code a user writes.

A mixed-language Dag declares its structure in Python, and TypeScript supplies task bodies and nothing else. So TypeScript registers **task handlers**, not a Dag: it owns no dag_id, no schedule, no task order. `Dag` is exclusively the native case, covered in [ADR-0002](0002-native-dag-interface.md).

## Decision

A handler is a plain function of its own data. The SDK's context and client come from getters, and each handler is registered against the dag_id/task_id pair Python already owns.

```ts
import { DagRegistry, getClient, getContext, serveDags } from "apache-airflow-ts-sdk";

interface TransformArgs {
  regionCode: string;
  threshold: number;
}

async function transform({ regionCode, threshold }: TransformArgs) {
  const client = getClient();
  const rows = await client.getXCom<number>({ key: "return_value", taskId: "extract" });
  if (rows === null) {
    throw new Error(`task ${getContext().taskId} has no upstream row count to transform`);
  }
  return { regionCode, passed: rows >= threshold };
}

const registry = new DagRegistry();
registry.registerTaskHandler("etl", "transform", transform);
await serveDags(registry);
```

### Wire names

By default, arguments are transformed automatically: the SDK normalizes the names on **both** sides — lowercased, separators removed — and matches those. Python's `region_code` binds to a handler's `regionCode`, `Name` binds to `name`, and `s3_uri` binds to `s3Uri`, with nothing declared on either side. The Go SDK normalizes the same way (`strings.ToLower(strings.ReplaceAll(name, "_", ""))`), so one Python signature binds identically in either SDK.

```ts
// Python: def transform(region_code: str, s3_uri: str, threshold: float)
async function transform({ regionCode, s3Uri, threshold }: TransformArgs) {
  // ...
}
```

A user can also specify the binding explicitly, with `withArgNames`: its first argument is the argument mapping, and its second is the handler itself. Normalization only absorbs spelling differences, so this is what to reach for when a handler wants a name the Python side never used — a clearer word, or a TypeScript reserved word like `enum`:

```ts
const report = withArgNames(
  { label: "run_label" },
  async ({ label, transformed }: ReportArgs) => {
    if (label !== "nightly") {
      throw new Error(`expected run label "nightly" but got "${label}"`);
    }
  },
);

registry.registerTaskHandler("etl", "report", report);
```

The map's keys are checked against the handler's own parameter type, so `{ labl: "run_label" }` is a compile error naming the right key. Its values are Python names, which `tsc` cannot see and does not check.

### How

- `registry.registerTaskHandler(dagId, taskId, handler)` is the second registration verb, beside `registry.registerDag(...dags)` for the native case ([ADR-0002](0002-native-dag-interface.md)). It takes one handler at a time because each needs its own dag_id/task_id pair, where a Dag carries its own id and several can be registered in one call. task_id must match the `@task.stub` id exactly and is always written out: deriving it from the handler's function name would silently rebind the handler when the function is renamed.
- A handler registered this way has no factory to call, so wiring a mixed-language task the way a native one is wired (`transform()`) is a compile error rather than a runtime throw. That is the guarantee an earlier draft's separate `MixedLangDag` class existed to provide.
- `getClient()` and `getContext()` read from an `AsyncLocalStorage` store the runtime wraps around the handler call, and throw outside a handler. TypeScript keeps type and value namespaces separate, so `getContext()` coexists with the `TaskContext` type without either being renamed.
- The bound argument object is a `Proxy`. Destructuring a name triggers a lookup that folds that name on demand and matches it against the folded wire names, so binding needs nothing declared anywhere, and an entry in `withArgNames` takes precedence over folding.
- An unmatched name is logged with both the requested name and the names actually delivered. It cannot throw: a destructuring default (`{ runId = "manual" }`) is a legitimate miss, and the runtime cannot tell one from a typo.
- `in` folds like a read. `Object.keys` and rest destructuring (`{ ...rest }`) yield Python's names, since the SDK has no TypeScript-side names to enumerate.
- Two Python names that fold to the same token fail the task at dispatch, naming both.
- An upstream's return value is not delivered as a bound argument. Read it explicitly via `client.getXCom({ key: "return_value", taskId: "..." })`.

## Alternatives

- **Annotating the Python name on the field with a phantom type** (`type Arg<T, N extends string> = T & { readonly __arg?: N }`), the way Java's `@ArgName` annotates a `TaskInput` field. Rejected: an `interface` is erased, so the annotation cannot reach dispatch and needs a runtime companion regardless. It also has two silent failure modes — `Arg<string | undefined, N>` collapses to a required `string`, because `undefined & object` is `never`, and the phantom key appears in `keyof` for an object-valued argument.
- **Reading the expected names from `handler.toString()`** and parsing the destructuring pattern. Property names do survive bundling, but a handler whose parameter is not destructured (`async (a: ReportArgs) => a.runLabel`) exposes no names at all, so the check would disappear silently for ordinary code.

## Consequences

- One binding mechanism serves every mixed-language handler, and the Python call site stays the single source of data-flow wiring.
- Folding matches the Go SDK's rule, so the same Python signature binds the same way in either SDK, and neither one needs a rename declared for ordinary snake_case parameters.
- `TaskHandlerArgs` is removed, `Dag` no longer serves the mixed-language case, and the registry's `register(...dags)` becomes `registerDag(...dags)` so the two verbs name what they take. All are shipped API (`src/index.ts`, and the `new Dag(...)` + `dag.task(...)` pattern in `README.md`, `docs/index.md`, and `example/src/main.ts`), so this breaks 0.1.0-beta1 authors and those call sites change with the implementation. The package's own status line already reads "API may change".
- A handler is directly unit-testable with a plain data argument: no `TaskHandlerArgs` fixture, and no intersection to remember.
- `withArgNames` is needed only for a genuine rename, never for a spelling difference, so most handlers never mention it.

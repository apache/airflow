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

## Decision

1. TypeScript registers **task handlers, not Dags**. `new TaskHandler(dagId, taskId, handler)` binds a
   handler to the Python-owned task it implements; `Dag` is exclusively the native case
   ([ADR-0002](0002-native-dag-interface.md)).
2. **A bundle has one registration verb and serves itself.** `bundle.register(...)` takes Dags and
   task handlers alike, in any mixture, and `await bundle.serve()` starts the runtime over them.
   `Bundle` replaces `DagRegistry`, and the free `serveDags(registry)` function goes with it.
3. **task_id is always written out**; nothing is derived from the handler's function name.
4. **A handler is a plain function of its own data**, destructured by name. `getContext()` and
   `getClient()` supply the rest, so nothing the SDK injects shares a namespace with an author's
   arguments.
5. **Names bind by folding on both sides** — lowercased, separators removed — so Python's
   `region_code` reaches a handler's `regionCode` with nothing declared. `withArgNames` is for a
   genuine rename, never for a spelling difference.
6. **An upstream's return value is not a bound argument.** Read it with
   `client.getXCom({ key: "return_value", taskId })`.

## Context

The Python `@task.stub` call site already defines task data flow. TypeScript tasks should consume
those bindings directly as named arguments instead of re-fetching each value with
`client.getXCom(...)`. A mixed-language Dag declares its structure in Python, and TypeScript supplies
task bodies and nothing else: it owns no dag_id, no schedule, no task order.

This ADR covers only the TypeScript call-site interface. The argument-binding spec itself — its
shape, how it is materialized, how it travels over the wire — is a protocol-level decision recorded
in [`airflow-core/adr/lang-sdk/0007`](../../airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md).

## Example

```ts
import { Bundle, TaskHandler, getClient, getContext } from "apache-airflow-ts-sdk";

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

const bundle = new Bundle();
bundle.register(new TaskHandler("etl", "transform", transform));
await bundle.serve();
```

A bundle usually provides both kinds, and one call lists everything it exposes:

```ts
bundle.register(
  nativeEtl, // a Dag, from ADR-0002
  new TaskHandler("py_etl", "transform", transform),
);
```

### Wire names

Arguments bind by folding both sides, so `region_code` reaches `regionCode`, `Name` reaches `name`,
and `s3_uri` reaches `s3Uri` with nothing declared on either side. The Go SDK folds the same way
(`strings.ToLower(strings.ReplaceAll(name, "_", ""))`), so one Python signature binds identically in
either SDK.

```ts
// Python: def transform(region_code: str, s3_uri: str, threshold: float)
async function transform({ regionCode, s3Uri, threshold }: TransformArgs) {
  // ...
}
```

`withArgNames` states a binding explicitly — its first argument is the mapping, its second the
handler. Folding absorbs spelling differences, so this is for a name the Python side never used: a
clearer word, or a TypeScript reserved word like `enum`.

```ts
const report = withArgNames(
  { label: "run_label" },
  async ({ label, transformed }: ReportArgs) => {
    if (label !== "nightly") {
      throw new Error(`expected run label "nightly" but got "${label}"`);
    }
  },
);

bundle.register(new TaskHandler("etl", "report", report));
```

The map's keys are checked against the handler's own parameter type, so `{ labl: "run_label" }` is a
compile error naming the right key. Its values are Python names, which `tsc` cannot see and does not
check.

## Consequences

- One binding mechanism serves every mixed-language handler, and the Python call site stays the
  single source of data-flow wiring.
- Folding matches the Go SDK's rule, so the same Python signature binds the same way in either SDK,
  and neither needs a rename declared for ordinary snake_case parameters.
- A handler is directly unit-testable as a plain function of its data: no fixture to construct and no
  intersection type to remember.
- This breaks 0.1.0-beta1 authors. `DagRegistry` becomes `Bundle`, and `TaskHandlerArgs` and the
  `TaskHandler` type that takes it (`src/sdk/task.ts`) no longer describe a handler and are removed.
  The shipped call sites change with the implementation — `src/index.ts`, and the `new Dag(...)` +
  `dag.task(...)` pattern in `README.md`, `docs/index.md`, and `example/src/main.ts`. The package's
  status line already reads "API may change".
- `serveDags(registry)` is removed in favour of `bundle.serve()`, since a bundle no longer holds
  only Dags. Its name is also quoted in a user-facing error string
  (`ts-sdk/src/cli/pack.ts:237`), which changes with it.

## Alternatives

- **Annotating the Python name on the field with a phantom type**
  (`type Arg<T, N extends string> = T & { readonly __arg?: N }`), the way Java's `@ArgName` annotates
  a `TaskInput` field. Rejected: an `interface` is erased, so the annotation cannot reach dispatch and
  needs a runtime companion regardless. It also has two silent failure modes —
  `Arg<string | undefined, N>` collapses to a required `string`, because `undefined & object` is
  `never`, and the phantom key appears in `keyof` for an object-valued argument.
- **Reading the expected names from `handler.toString()`** and parsing the destructuring pattern.
  Property names do survive bundling, but a handler whose parameter is not destructured
  (`async (a: ReportArgs) => a.runLabel`) exposes no names at all, so the check would disappear
  silently for ordinary code.
- **Two registration verbs**, `registerDag(...dags)` beside `registerTaskHandler(dagId, taskId, fn)`.
  Rejected once a task handler became a value carrying its own ids: the asymmetry that justified the
  split — a Dag knows its id, a bare handler does not — disappears, and a bundle that provides both
  kinds had to say so in two calls.

## Appendix: Implementation Notes

- **`register` widens rather than splits.** The shipped `DagRegistry.register(...dags: Dag[])`
  (`ts-sdk/src/sdk/registry.ts`) already narrows each argument with `instanceof Dag` and rejects a
  foreign copy by brand. `Bundle.register(...items: Registerable[])`, over
  `type Registerable = Dag | TaskHandler`, follows the same path with one more arm — a discriminated
  union being TypeScript's equivalent of the sealed interface the Go SDK uses for the same purpose.
- **`serve` is a method so the coordinator stays unnamed.** `startCoordinator` is deliberately not
  exported — "Dag authors reach the runtime through `serveDags()`, and never name the coordinator
  itself" (`ts-sdk/src/coordinator/index.ts`) — and a method on the object that already holds the
  Dags and handlers keeps that intent while dropping the free function.
- **A task handler has no factory to call**, so wiring one the way a native task is wired
  (`transform()`) is a compile error rather than a runtime throw. That is the guarantee an earlier
  draft's separate `MixedLangDag` class existed to provide.
- **`getClient()` and `getContext()` read from an `AsyncLocalStorage` store** the runtime wraps around
  the handler call, and throw outside a handler. TypeScript keeps type and value namespaces separate,
  so `getContext()` coexists with the `TaskContext` type without either being renamed.
- **The bound argument object is a `Proxy`.** Destructuring a name triggers a lookup that folds that
  name on demand and matches it against the folded wire names, so binding needs nothing declared
  anywhere, and an entry in `withArgNames` takes precedence over folding.
- **An unmatched name is logged**, with both the requested name and the names actually delivered. It
  cannot throw: a destructuring default (`{ runId = "manual" }`) is a legitimate miss, and the runtime
  cannot tell one from a typo.
- `in` folds like a read. `Object.keys` and rest destructuring (`{ ...rest }`) yield Python's names,
  since the SDK has no TypeScript-side names to enumerate. Two Python names that fold to the same
  token fail the task at dispatch, naming both.

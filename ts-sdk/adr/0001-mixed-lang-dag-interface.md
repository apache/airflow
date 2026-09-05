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

# ADR-0001: Mixed-Lang Dag — TypeScript Task Interface

## Status

Proposed

## Context

The Python `@task.stub` call site already defines task data flow. TypeScript tasks should consume those bindings directly as named arguments, instead of re-fetching each value with `client.getXCom(...)`.

This ADR covers only the TypeScript call-site interface. The argument-binding spec itself (its shape, how it's materialized, how it travels over the wire) is a separate, protocol-level decision recorded in [`airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md`](../../airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md). Given that spec, this ADR only answers what TypeScript code a user writes.

A mixed-language Dag declares its structure in Python and supplies task bodies from TypeScript, using `MixedLangDag`, a class dedicated to this mode. The native case, where TypeScript owns the graph too, is a separate `Dag` class, covered in [ADR-0002](0002-native-dag-interface.md).

## Decision

TypeScript uses one syntax for the TaskFlow binding: named arguments merged onto the handler's single parameter object, alongside the SDK's own `ctx`/`client`.

```ts
const dag = new MixedLangDag("etl");

interface TransformArgs {
  region_code: string;
  threshold: number;
}

async function transform({ ctx, client, region_code, threshold }: TransformArgs & TaskHandlerArgs) {
  const rows = await client.getXCom<number>({ key: "return_value", taskId: "extract" });
  if (rows === null) {
    throw new Error(`task ${ctx.taskId} has no upstream row count to transform`);
  }
  const passed = rows >= threshold;
  await client.setXCom({ key: "region", value: region_code });
  return { region_code, passed };
}

dag.task("transform", transform);
```

Renaming a Python name that isn't idiomatic TypeScript is ordinary destructuring, not a separate mechanism:

```ts
async function report({ run_label: runLabel }: { run_label: string } & TaskHandlerArgs) {
  if (runLabel !== "nightly") {
    throw new Error(`expected run label "nightly" but got "${runLabel}"`);
  }
}

dag.task("report", report);
```

### How

- `MixedLangDag.task()` returns a plain `TaskRef`, not a callable factory. There is nothing to wire, since the Python file already owns task order. Calling it the way a native `Dag`'s factory is called (`transform()`) is a compile error, since `TaskRef` has no call signature, not a runtime throw.
- Wire names match the Python parameter names character for character, with no case- or separator-insensitive fallback. Renaming happens once, at the destructuring site.
- `ctx` and `client` are reserved, permanently: bound arguments are merged flat into the same object alongside them, and a bound name that collides with either fails the task at dispatch.
- An upstream's return value is not delivered as a bound argument. Read it explicitly via `client.getXCom({ key: "return_value", taskId: "..." })`.
- `tsc` cannot check a handler's destructuring pattern against the Python call site. A typo binds `undefined` silently; the runtime logs the bound names at dispatch and includes them in a handler-failure message, so the mismatch is diagnosable from the task log.

## Open Questions

- Should the decoded bindings also be exposed as a public, positional/raw accessor (name/value pairs, no interface required), or should that stay an internal runtime detail?
- Should `ctx` and `client` become explicit getter functions (`getClient()`, `getContext()`) instead of arguments merged into the handler's object? (See [ADR-0002](0002-native-dag-interface.md) for where this same question resurfaces on the native-Dag side.)

## Consequences

- One binding mechanism serves every mixed-language handler; there is no second syntax to keep in sync.
- The Python call site stays the single source of data-flow wiring for mixed-language Dags.
- `MixedLangDag` replaces an earlier `isMixedLanguageDag` spec flag on a single `Dag` class. The authoring class itself states the mode, so a mixed-language task that's wired incorrectly fails to compile under `tsc` instead of throwing at runtime.
- The public surface is not final until the two open questions above are resolved.

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

# ADR-0003: Task Loop — TypeScript Authoring Interface

## Status

Proposed.

## Context

A task loop runs a task group again and again until a gate says stop. What a loop means, and the
eight authoring rules every SDK follows, are in
[`airflow-core/adr/lang-sdk/0017`](../../airflow-core/adr/lang-sdk/0017-task-loops-across-lang-sdks.md).
This ADR records the TypeScript spelling of them.

## Decision

1. **`refine.loop(...)` closes a group.** `dag.taskGroup("refine")` builds and fills it as usual.
2. **The gate is a handler wired in one call**: `loop(until, inputs, spec)`, the shape
   `dag.if(handler, inputs, spec)` already has ([ADR-0002](0002-native-dag-interface.md)). `inputs` is
   the gate's own data, as any task's is.
3. **The terminal task is the body's single leaf**, which the SDK orders before the gate. Wiring the
   leaf into `inputs` is what gets the decision checked against what the body produces.
4. **`getLoop()` supplies the loop context**, beside `getContext()` and `getClient()` (ADR-0002
   decision 6). It is awaited, because reading the previous iteration is a round trip.
5. **`index`, `maxIterations` and `previous` are fields**, and `getPrevious<T>()` is the typed view.
   That is `client.getXCom`'s split between a value and the type the caller names.
6. **A body task returns a `LoopTaskRef<T>`**, accepted where either `TaskRef<T>` or
   `TaskRef<XComSequence<T>>` is wanted, so `tsc` checks whichever reading the consumer declares.
7. **`LoopRef` carries `before` and `after`.** An order-only edge from a body ref to a task outside is
   rejected when the Dag is read.

## Example

Newton's method for the square root of two, stopping on the error or at ten iterations.

```ts
import { Dag, getLoop, type LoopDecision, type XComSequence } from "apache-airflow-ts-sdk";

type Metrics = { estimate: number; error: number };

const dag = new Dag("ts_refine", { schedule: "@daily" });
const refine = dag.taskGroup("refine");

const improve = refine.task(async function improve(): Promise<number> {
  const loop = await getLoop();
  console.log(`iteration ${loop.index} of ${loop.maxIterations}`);
  const estimate = loop.getPrevious<Metrics>()?.estimate ?? 1.0;
  return (estimate + 2.0 / estimate) / 2.0;
});

const evaluate = refine.task(async function evaluate({ estimate }: { estimate: number }): Promise<Metrics> {
  return { estimate, error: Math.abs(estimate * estimate - 2) };
});

async function accurateEnough({ result }: { result: Metrics }): Promise<LoopDecision> {
  return { stop: result.error < 1e-6, reason: `error ${result.error.toExponential(1)}` };
}

const report = dag.task(async function report({ history }: { history: XComSequence<Metrics> }) {
  console.log(`settled after ${await history.length()} iterations`);
});

const estimate = improve();
const evaluated = evaluate({ estimate });

const refinement = refine.loop(accurateEnough, { result: evaluated }, { maxIterations: 10 });

report({ history: evaluated }); // every iteration's metrics, through the gate
refinement.before(cleanup()); // order only: the whole loop, not one iteration
```

`evaluated` is written twice and means two things. Inside the loop it is one iteration's `Metrics`,
which is what the gate takes. Outside it is an `XComSequence<Metrics>`, which is what `report` takes.
Decision 6 is what makes both type-check.

A loop that only counts needs no handler:

```ts
const poll = dag.taskGroup("poll");
poll.task(checkOnce)();
poll.loop({ maxIterations: 3 });
```

## Signature

```ts
/** Options of a loop, the trailing argument of `loop()`. */
interface LoopOptions extends TaskSpec {
  maxIterations: number;
}

/** What `until` returns. A bare `boolean` is `{ stop }`. */
interface LoopDecision {
  stop: boolean;
  reason?: string;
}

/**
 * The gate's handler. `A` is its wired inputs, one key per argument, exactly as a task handler
 * takes its own. Nothing of the SDK's is in `A`; the loop context comes from `getLoop()`.
 */
type UntilCallable<A> = (args: A) => Promise<LoopDecision | boolean>;

interface TaskGroupRef {
  loop(options: LoopOptions): LoopRef;
  loop<A>(until: UntilCallable<A>, inputs: A, options: LoopOptions): LoopRef;
}

/**
 * Read inside a loop body or its gate. Throws anywhere else.
 * Awaited because it reads the previous iteration's result, which it caches per task run.
 */
function getLoop(): Promise<LoopContext>;

interface LoopContext {
  readonly index: number;
  readonly maxIterations: number;

  /** The previous iteration's terminal result as it arrived, or `null` at iteration 1. */
  readonly previous: unknown;

  /** The same value, as the type the caller names. `client.getXCom`'s type parameter. */
  getPrevious<T>(): T | null;
}
```

## Consequences

- **A fixed-count loop needs no runtime support.** `loop({ maxIterations })` emits `has_until: false`,
  which core completes without a worker (ADR-0017 decision 4).
- **The gate is an ordinary task.** `LoopOptions` extends `TaskSpec`, so `queue` and `retries` land on
  it, and its id defaults to the handler's function name under the group's prefix. A fixed-count loop
  has no handler to take a name from, so its gate is `__loop_gate`, as Python's is.
- **The body's single leaf is checked twice**: by the SDK when the Dag is read, for the faster
  message, and by the Dag processor, which decides (ADR-0017 decision 3). A nested loop and a loop
  inside a mapped task group are rejected the same way.
- **`getLoop()` throws outside a loop**, so a handler moved out of a group fails where it was moved.
- **A mapped terminal needs no new call.** `getPrevious<XComSequence<Metrics>>()` names the sequence
  type, and `terminal_is_mapped` tells the runtime which to build.
- **The reading has to match where the consumer sits.** Core serves only the all-iterations read from
  outside the loop, so a consumer outside it that declares one iteration's value is rejected when the
  Dag is read rather than failing partway through the run.
- **`loop()` runs once per group.** A group already on an edge can still become one, since `LoopRef`
  takes its edges over.

## Alternatives

- **The loop context as another key of the gate's argument**, `accurateEnough({ loop, result })`.
  Rejected for the reason [ADR-0002](0002-native-dag-interface.md) rejects it for `ctx` and `client`:
  a handler's argument is its own data, and merging SDK values into it leaves the top-level names open
  to collide with the author's. Python puts `loop` among the context kwargs because every Python
  handler already takes them. No TypeScript handler does.
- **A synchronous `getLoop()`**, with `previous` fetched before every body handler runs. Rejected: a
  handler that never reads it would pay a round trip from iteration 2 on. Python pays nothing for the
  same reason, since `loop.previous` is a lazy property.

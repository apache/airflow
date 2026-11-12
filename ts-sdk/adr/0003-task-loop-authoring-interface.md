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
5. **`index` and `maxIterations` are fields, and `getPrevious<T>()` is the one accessor** for the
   previous iteration's terminal result — one, not two, as ADR-0017 asks of every runtime. It is a
   generic method naming its own return type, as `client.getXCom<T>()` is, not a field with a typed
   view beside it.
6. **A body task returns a `LoopTaskRef<T>`**, a `TaskRef<T>` that records the loop it came from.
   Inside the loop it reads as one iteration's `T`. Outside it, `all()` returns a
   `TaskRef<XComSequence<T>>` and a bare ref is rejected when the Dag is read, since core serves only
   the all-iterations read from outside and the SDK records no argument type it could check instead.
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

const cleanup = dag.task(async function cleanup() {});

const estimate = improve();
const evaluated = evaluate({ estimate });

const refinement = refine.loop(accurateEnough, { result: evaluated }, { maxIterations: 10 });

report({ history: evaluated.all() }); // every iteration's metrics, through the gate
refinement.before(cleanup()); // order only: the whole loop, not one iteration
```

`evaluated` is written twice. Inside the loop it is one iteration's `Metrics`, which is what the
gate takes. Outside it only `evaluated.all()` is allowed, an `XComSequence<Metrics>`, which is what
`report` takes; a bare `evaluated` out there is rejected when the Dag is read.

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
 * The gate's handler. `TArgs` is its own arguments, one key each, exactly as a task handler takes
 * its own. Nothing of the SDK's is in `TArgs`; the loop context comes from `getLoop()`.
 */
type UntilCallable<TArgs extends object> = (
  args: TArgs,
) => LoopDecision | boolean | Promise<LoopDecision | boolean>;

interface TaskGroupRef {
  loop(options: LoopOptions): LoopRef;
  /**
   * `NoInfer` keeps `TArgs` coming from the handler alone, so `inputs` is checked key by key against
   * it rather than widening it. `dag.if` takes its inputs the same way.
   */
  loop<TArgs extends object>(
    until: UntilCallable<TArgs>,
    inputs: TaskInputs<NoInfer<TArgs>>,
    options: LoopOptions,
  ): LoopRef;
}

/** What `loop()` returns: the whole loop as one node, which is what an ordering edge leaves from. */
interface LoopRef extends Node {
  readonly groupId: string;
  before(...downstream: readonly Node[]): LoopRef;
  after(...upstream: readonly Node[]): LoopRef;
}

/**
 * What a loop body task returns. It is a `TaskRef<T>`, so it goes wherever one goes; the loop it
 * records is what lets the SDK tell a body ref from an ordinary one when the Dag is read.
 */
interface LoopTaskRef<T> extends TaskRef<T> {
  readonly loopId: string;

  /** Every iteration's value, the one reading core serves from outside the loop. */
  all(): TaskRef<XComSequence<T>>;
}

/**
 * Read inside a loop body or its gate. Throws anywhere else.
 * Awaited because it reads the previous iteration's result, which it caches per task run.
 */
function getLoop(): Promise<LoopContext>;

interface LoopContext {
  readonly index: number;
  readonly maxIterations: number;

  /**
   * The previous iteration's terminal result, as the type the caller names, or `null` at iteration
   * 1. An `XComSequence<T>` when the terminal task is mapped.
   */
  getPrevious<T = unknown>(): T | null;
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
- **The reading is written, not inferred.** `serializeArgBindings` records a task id and no argument
  type, so the SDK cannot tell which reading a consumer declared. `all()` puts the outside reading in
  the Dag instead, and `tsc` checks it because the accessor names its own return type. A bare body
  ref outside the loop is rejected when the Dag is read rather than failing partway through the run.
- **`loop()` runs once per group.** A group already on an edge can still become one, since `LoopRef`
  takes its edges over.

## Alternatives

- **The loop context as another key of the gate's argument**, `accurateEnough({ loop, result })`.
  Rejected for the reason [ADR-0002](0002-native-dag-interface.md) rejects it for `ctx` and `client`:
  a handler's argument is its own data, and merging SDK values into it leaves the top-level names open
  to collide with the author's. Python puts `loop` among the context kwargs because every Python
  handler already takes them. No TypeScript handler does.
- **A synchronous `getLoop()`**, with the previous result fetched before every body handler runs.
  Rejected: a handler that never reads it would pay a round trip from iteration 2 on. Python pays
  nothing for the same reason, since `loop.previous` is a lazy property.
- **A bare body ref outside the loop, with the consumer's declared parameter type picking the
  reading.** Rejected: nothing can check it. `TaskInputs` accepts any `TaskRef` for any argument, and
  `serializeArgBindings` records no type, so a consumer that declared one iteration's value would
  serialize cleanly and fail partway through the run. The Go SDK keeps the bare ref because
  registration records its handlers' parameter types, and the Java SDK because it generates an
  overload per reading; TypeScript has neither, so it asks for the reading instead.

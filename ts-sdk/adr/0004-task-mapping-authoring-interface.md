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

# ADR-0004: Task Mapping — TypeScript Authoring Interface

## Status

Proposed.

## Context

What a mapped task is, what each process does with it, and the authoring rules every SDK follows are in
[`airflow-core/adr/lang-sdk/0016`](../../airflow-core/adr/lang-sdk/0016-mapped-tasks-across-lang-sdks.md).
This ADR records the TypeScript spelling of them.

A handler already takes one object of named arguments
([ADR-0002](0002-native-dag-interface.md), decision 1), so every argument has a key, and TypeScript
can keep Python's keyword spelling where a positional SDK needs a marker (ADR-0016, *Authoring
interface*).

Two rules from outside this ADR shape it. A Dag definition's own task, which every task of a
TypeScript Dag is, may have its `expand_kwargs()` dicts spread into its handler's arguments, so the
handler's own defaults apply (ADR-0016, *Stub-task arg bindings*). And a TypeScript task pushes what
it returns: `undefined` pushes nothing, and `null` is a value it pushes, as `setXCom(null)` does.

## Decision

1. **`expand`, `partial` and `expandKwargs` are methods of the task factory.**
   `transform.expand({...})`, `transform.partial({...}).expand({...})`,
   `transform.expandKwargs(source)` and `transform.partial({...}).expandKwargs(source)` each count as
   the task's one call (ADR-0002, decision 3). A `partial` that is never expanded leaves the task
   uncalled, so it fails when the Dag is read. A task with no arguments has none of the three.
2. **The keys are the handler's own, and mapping does not change the handler.** `transform` takes
   `{ row: string }` whether it is mapped or not. With `expand`, every required key is supplied by
   `partial` or by `expand`, an optional one may be left to the handler's default, no key is
   supplied by both, and an unknown key is a compile error. The only argument types mapping
   brings are `XComSequence<T>` for a reduce and `[K, V]` for expanding over a dict. An argument type
   with an index signature, such as `Record<string, X>`, has no keys to check, so all three methods
   reject it at compile time.
3. **`expandKwargs` spreads each instance's object into the handler's arguments.** The task writes
   ADR-0016's `kwargs` entry, so a key the object leaves out takes the handler's own default, as it
   does in an unmapped call that leaves the key out. A key that `partial` also sets fails the
   instance, as Python's `mapping already partial` does. `tsc` checks the objects of a literal list
   against the handler's type. Objects an upstream builds are checked only as far as that upstream's
   return type goes: their keys exist only at run time, and the runtime has no parameter list to
   check them against, so a key the handler does not take is passed on with the rest.
4. **A mapped task returns a `MappedTaskRef<T>`, which is not a `TaskRef`.** It carries `before`
   and `after` like any node, and goes into a later `expand` for one instance per upstream instance
   with a non-`null` result. Passed anywhere else, as a plain input or through `partial`, it is
   accepted only where the argument is `XComSequence<T>`, and serializes as a reduce. No call marks
   the reduce, unlike a loop body's `all()` ([ADR-0003](0003-task-loop-authoring-interface.md),
   decision 6): a mapped task has no one-instance reading outside an `expand`, so the type alone
   says which reading is meant. A branch target, in `then`, `else` or `case`, also takes a
   `MappedTaskRef`, since a branch runs or skips a mapped task as a whole, as it does in Python.
5. **`map`, `zip` and `concat` are methods of a reference whose value has elements**: a `TaskRef` to
   an array or a record, or a `MappedTaskRef`. A `concat` result offers `map` and `concat`, and a
   `zip` result only `map`, so this SDK leaves out Python's `a.concat(b).zip(c)` and
   `a.zip(b).concat(c)` for now. Each returns a source that only `expand` and `expandKwargs` take,
   so ADR-0016 decision 4's rejected shapes do not compile:
   - **`map` stays outermost.** A `map` result offers only `map`, and chained calls add callables to
     one wrapper.
   - **No `zip` inside a `zip`.** A `zip` result offers only `map`.
   - **No whole-argument use.** Neither a plain input nor `partial` takes a source.
6. **A `map` callable is a synchronous function of one element**: `(element: T) => U`, with `U` not
   a promise. It runs in the consumer's runtime, found by the argument's name, or for `expandKwargs`
   as the task's one source (ADR-0016, *map()*), so an anonymous arrow function is fine. It skips
   its instance by throwing `SkipError`, a new export, the TypeScript equivalent of
   `AirflowSkipException`. It is serialized as `fn.toString()`.
7. **`zip` fills with `padding`, JavaScript's name for it.** `{ padding: v }` makes the zip as long
   as its longest source, with `v` standing in for the missing items, as `Iterator.zip`'s `padding`
   does; without it, the zip is as long as its shortest. It is one value for every source, as
   Python's `fillvalue` is, and serialized as `fillvalue`. There is no `mode`: giving `padding` asks
   for the longest zip, as giving `fillvalue` does in Python. `padding` is a JSON value, so it
   cannot be `undefined`: that has no JSON form, and serializing it would drop the key and quietly
   give the shortest zip. A padded item reaches the handler as `null` either way, so
   `{ padding: null }` is the way to pad with nothing. A `padding` key holding `undefined` fails
   when the Dag is read.
8. **The instance label is a client call, and `mapIndexTemplate` leaves `TaskSpec`.**
   `getClient().setRenderedMapIndex(label)` sends `SetRenderedMapIndex` (ADR-0016, decision 7). The
   generated `TaskSpec` drops `mapIndexTemplate`, since nothing renders its Jinja, and a Dag that
   still sets it fails when it is read, with an error that names `setRenderedMapIndex`. It keeps
   `maxActiveTisPerDagrun`, an ordinary task setting. Jinja templating needs an equivalent in each
   task language, for this field and every other one Python templates; a later ADR specifies it for
   every SDK.
9. **`getContext()` gains `expandedTiCount`**, read-only beside `mapIndex`, for "instance 3 of 10".
   It is `null` for a task that is not mapped, where `mapIndex` is `-1`: the Execution API leaves
   the count out, and Python's is `None`. It is never used to resolve arguments (ADR-0016, *Acting
   on `expanded_ti_count`*).
10. **A reduce reads an `XComSequence<T>`, defined here.** ADR-0003 already names the type for a
    loop's `all()`. It reads on demand, as Python's `LazyXComSequence` does (ADR-0016, decision 8):
    `length()`, `at(i)` with a negative `i` counting from the end, `slice(start, stop)`, and
    `for await`. Its elements are in map-index order, and an instance whose result is `null`, or
    that pushed no value, is not one of them. It is valid only in the handler call that received it,
    so it cannot be returned, pushed or passed on.
11. **A mapped instance that returns `null` or nothing gives no downstream instance.** It pushes a
    `null` or no value, as any task does, and a later `expand` and a reduce both leave it out, as
    they leave out a Python instance that returned `None`. So a mapped task's element type is its
    return type without `null` and `undefined`: `MappedTaskRef<NonNullable<TReturn>>`. An unmapped
    producer that a later `expand` reads must push a list or a record to map over (ADR-0016,
    decision 3).
12. **A mapped task in a loop body is a `LoopMappedTaskRef<T>`**: a `MappedTaskRef<T>` that records
    its loop. Inside the loop it is a `MappedTaskRef<T>` for one iteration. From outside, only `all()`
    reads it, a `TaskRef<XComSequence<T>>` ordered by iteration, then by map index (ADR-0016,
    *Reduce*). ADR-0003's `LoopTaskRef<T>` stays the type of an unmapped body task. An `expand` over
    a body task from outside its loop fails when the Dag is read, ADR-0017's rule (decision 3)
    checked early. Passed bare as a reduce from outside its loop, it fails when the Dag is read too,
    as a bare `LoopTaskRef` does (ADR-0003, decision 6), since `tsc` accepts it there as a
    `MappedTaskRef`.
13. **`triggerRule: "always"` is rejected on a task that expands over an upstream's output**, when
    the Dag is read, as Python rejects it: `always` runs the task even when that upstream failed and
    left nothing to expand over. The check covers `expandKwargs` over a reference.
14. **Three things are not mapped, each behind its own capability flag** (ADR-0016, decision 4):
    - **Task groups.** `dag.taskGroup(id)` takes no inputs, so there is nothing to expand. A later ADR
      designs groups that take inputs, together with ADR-0016's rules for them: no `expand` inside a
      mapped group, no loop inside one, and the deeper-nesting rule.
    - **Operator tasks.** `triggerDagRun` is an `Operator<void, void>`, whose factory takes no
      arguments to expand.
    - **Deciders.** `if` and `switch`, on a Dag or a task group, take no `expand`.

## Example

```ts
import { Dag, type XComSequence } from "@apache-airflow/ts-sdk";

const dag = new Dag("mapping_example", { schedule: "@daily", catchup: false });

// Shared handlers, each registered under several task ids below.
const measure = async ({ path, bucket }: { path: string; bucket: string }) =>
  path.length + bucket.length;

const sizeIfLarge = async ({ path, min = 5 }: { path: string; min?: number }) =>
  path.length >= min ? path.length : undefined;

const summarizeSizes = async ({ sizes }: { sizes: XComSequence<number> }) => {
  console.log(await sizes.length());
};

const listFiles = dag.task("list_files", async (): Promise<string[]> => ["a.csv", "b.csv"]);
const listOtherFiles = dag.task("list_other_files", async (): Promise<string[]> => ["c.csv"]);
const files = listFiles(); // TaskRef<string[]>
const otherFiles = listOtherFiles();

// Expand over an upstream's list, a literal, or a mapped upstream.
const measureFile = dag.task("measure_file", measure);
// MappedTaskRef<number>
const sizes = measureFile.partial({ bucket: "raw" }).expand({ path: files });

// A cross product: four instances.
const measureGrid = dag.task("measure_grid", measure);
measureGrid.expand({ path: ["a.csv", "b.csv"], bucket: ["raw", "eu"] });

// One audit per measure_file instance.
const audit = dag.task("audit", async ({ size }: { size: number }) => {
  console.log(size);
});
audit.expand({ size: sizes });

// One [key, value] pair per instance.
const tag = dag.task("tag", async ({ entry }: { entry: [string, string] }) => {
  console.log(entry[0], entry[1]);
});
tag.expand({ entry: { env: "prod", team: "data" } });

// A reduce: a mapped task where the argument is a sequence.
const summarize = dag.task("summarize", summarizeSizes);
summarize({ sizes });

const report = dag.task(
  "report",
  async ({ sizes, format }: { sizes: XComSequence<number>; format: string }) => {
    console.log(format, await sizes.length());
  },
);
report.partial({ sizes }).expand({ format: ["csv", "pdf"] });

// expandKwargs: one argument object per instance, spread into the handler's arguments.
const measureKwargs = dag.task("measure_kwargs", measure);
const fileArgs = files.map((path) => ({ path, bucket: "raw" }));
measureKwargs.expandKwargs(fileArgs);

const measureSome = dag.task("measure_some", measure);
measureSome.partial({ bucket: "raw" }).expandKwargs([{ path: "a.csv" }, { path: "b.csv" }]);

// The first instance leaves `min` to the handler's default.
const sizeSome = dag.task("size_some", sizeIfLarge);
sizeSome.expandKwargs([{ path: "a.csv" }, { path: "b.csv", min: 10 }]);

// An instance that returns nothing is not in the reduce.
const sizeLarge = dag.task("size_large", sizeIfLarge);
const large = sizeLarge.expand({ path: files }); // MappedTaskRef<number>
const summarizeLarge = dag.task("summarize_large", summarizeSizes);
summarizeLarge({ sizes: large });

// Sources built with map, zip and concat.
const measureTrimmed = dag.task("measure_trimmed", measure);
const trimmed = files.map((path) => path.trim());
measureTrimmed.partial({ bucket: "raw" }).expand({ path: trimmed });

// Either side may be padded, so both slots can be null.
const pair = dag.task("pair", async ({ p }: { p: [string | null, number | null] }) => {
  console.log(p[0], p[1]);
});
const padded = files.zip(sizes, { padding: null });
pair.expand({ p: padded });

const measureBoth = dag.task("measure_both", measure);
const allFiles = files.concat(otherFiles);
measureBoth.partial({ bucket: "raw" }).expand({ path: allFiles });

// A mapped task as a branch target, and as an ordering node.
const hasFiles = async ({ files: paths }: { files: string[] }) => paths.length > 0;
const gate = dag.if(hasFiles, { files }, { taskId: "has_files" });
gate.then(sizes);

const cleanup = dag.task("cleanup", async () => {});
const cleaned = cleanup();
sizes.before(cleaned);
```

## Signature

```ts
/** A mapped task. It goes on edges and into `expand`; elsewhere only as a reduce. */
interface MappedTaskRef<T = unknown> extends Node {
  readonly taskId: string;
  /** @internal Never set; the element type, one instance's return value. */
  readonly [ELEMENT_TYPE]?: T;
  before(...downstream: readonly Node[]): MappedTaskRef<T>;
  after(...upstream: readonly Node[]): MappedTaskRef<T>;
  map<U>(fn: MapCallable<T, U>): MapSource<U>;
  zip<const S extends readonly Expandable[]>(...others: S): ZipSource<[T, ...ElementsOf<S>]>;
  zip<const S extends readonly Expandable[], F extends JsonValue>(
    ...others: [...S, ZipOptions<F>]
  ): ZipSource<[T | F, ...ElementsOrFill<S, F>]>;
  concat(...others: readonly Expandable<T>[]): ConcatSource<T>;
}

/** A mapped task in a loop body. Inside the loop a `MappedTaskRef<T>`; from outside, only `all()`. */
interface LoopMappedTaskRef<T = unknown> extends MappedTaskRef<T> {
  readonly loopId: string;
  /** Every instance of every iteration, ordered by iteration, then map index. */
  all(): TaskRef<XComSequence<T>>;
}

/**
 * `TaskRef` gains the same three, through a `this` parameter that admits only a reference to an
 * array or a record, so `extract().map(...)` on a `TaskRef<number>` is a compile error.
 */
interface TaskRef<TReturn> {
  map<U>(this: TaskRef<HasElements>, fn: MapCallable<ElementOf<TReturn>, U>): MapSource<U>;
  zip<const S extends readonly Expandable[]>(
    this: TaskRef<HasElements>,
    ...others: S
  ): ZipSource<[ElementOf<TReturn>, ...ElementsOf<S>]>;
  zip<const S extends readonly Expandable[], F extends JsonValue>(
    this: TaskRef<HasElements>,
    ...others: [...S, ZipOptions<F>]
  ): ZipSource<[ElementOf<TReturn> | F, ...ElementsOrFill<S, F>]>;
  concat(
    this: TaskRef<HasElements>,
    ...others: readonly Expandable<ElementOf<TReturn>>[]
  ): ConcatSource<ElementOf<TReturn>>;
}

/** An array's element, or a record's `[key, value]` pair (ADR-0016, Authoring interface). */
type ElementOf<R> = R extends readonly (infer E)[]
  ? E
  : R extends Readonly<Record<infer K extends string, infer V>>
    ? [K, V]
    : never;

/** Marks a mapped task's element type, as the SDK's `RETURN_TYPE` marks a `TaskRef`'s value. */
declare const ELEMENT_TYPE: unique symbol;

/** A value with elements: an array or a record. */
type HasElements = readonly unknown[] | Readonly<Record<string, unknown>>;

/** Each `zip` source's element type, in order. */
type ElementsOf<S extends readonly unknown[]> = {
  [I in keyof S]: S[I] extends { readonly [RETURN_TYPE]?: infer R }
    ? ElementOf<R>
    : S[I] extends { readonly [ELEMENT_TYPE]?: infer E }
      ? E
      : S[I] extends ConcatSource<infer E>
        ? E
        : never;
};

/** Each `zip` source's element type, or the padding type where that source runs out. */
type ElementsOrFill<S extends readonly unknown[], F> = {
  [I in keyof S]: ElementsOf<[S[I]]>[0] | F;
};

/** What `zip` and `concat` take: something with elements of `T`. A `zip` or `map` result is not. */
type Expandable<T = unknown> = TaskRef<readonly T[]> | MappedTaskRef<T> | ConcatSource<T>;

type MapCallable<T, U> = (element: T) => U extends PromiseLike<unknown> ? never : U;

interface MapSource<T> { map<U>(fn: MapCallable<T, U>): MapSource<U>; }
interface ZipSource<T> { map<U>(fn: MapCallable<T, U>): MapSource<U>; }
interface ConcatSource<T> {
  map<U>(fn: MapCallable<T, U>): MapSource<U>;
  concat(...others: readonly Expandable<T>[]): ConcatSource<T>;
}

/** `padding` is a JSON value: `undefined` has no JSON form, so pad with `null` instead. */
interface ZipOptions<F extends JsonValue> { padding: F; }

/** What one expanded argument of type `T` can be given. */
type ExpandSource<T> =
  | readonly JsonCompatible<T>[]
  | (T extends readonly [infer K extends string, infer V]
      ? Readonly<Record<K, JsonCompatible<V>>> | TaskRef<Readonly<Record<K, V>>>
      : never)
  | TaskRef<readonly T[]>
  | MappedTaskRef<T>
  | MapSource<T>
  | ZipSource<T>
  | ConcatSource<T>;

type ExpandInputs<TArgs> = { [K in keyof TArgs]: ExpandSource<TArgs[K]> };

/** What `expandKwargs` takes: elements that are each one instance's remaining arguments. */
type KwargsSource<TArgs> =
  | readonly JsonCompatible<TArgs>[]
  | TaskRef<readonly TArgs[]>
  | MappedTaskRef<TArgs>
  | MapSource<TArgs>;

/** A plain input also takes a mapped task, as a reduce, where the argument is a sequence. */
type TaskInputs<TArgs> = {
  [K in keyof TArgs]:
    | TaskRef
    | JsonCompatible<TArgs[K]>
    | (TArgs[K] extends XComSequence<infer E> ? MappedTaskRef<E> : never);
};

/** `never` for an argument type with no known keys, which turns every method below off. */
type Keyed<TArgs> = string extends keyof TArgs ? never : TArgs;

/** A task with no arguments keeps its plain call; there is nothing to expand. */
type TaskFactory<TArgs extends object | void = void, TReturn = unknown> = [TArgs] extends [void]
  ? () => TaskRef<TReturn>
  : MappableTaskFactory<TArgs & object, TReturn>;

interface MappableTaskFactory<TArgs extends object, TReturn> {
  (inputs: TaskInputs<TArgs>): TaskRef<TReturn>;
  expand(inputs: ExpandInputs<Keyed<TArgs>>): MappedTaskRef<NonNullable<TReturn>>;
  expandKwargs(source: KwargsSource<Keyed<TArgs>>): MappedTaskRef<NonNullable<TReturn>>;
  partial<const P extends Partial<TaskInputs<Keyed<TArgs>>>>(
    inputs: P & Record<Exclude<keyof P, keyof TArgs>, never>,
  ): PartialTask<Omit<TArgs, keyof P>, TReturn>;
}

/** What `partial` returns. Only expanding it calls the task. */
interface PartialTask<TRest, TReturn> {
  /** `never` once `partial` has taken every argument, so `expand({})` does not compile. */
  expand(
    inputs: keyof TRest extends never ? never : ExpandInputs<TRest>,
  ): MappedTaskRef<NonNullable<TReturn>>;
  expandKwargs(source: KwargsSource<TRest>): MappedTaskRef<NonNullable<TReturn>>;
}

/** A mapped task's whole output, read on demand. Valid only in the handler call that received it. */
interface XComSequence<T> extends AsyncIterable<T> {
  length(): Promise<number>;
  /** A negative `i` counts from the end. Throws `RangeError` outside the sequence. */
  at(i: number): Promise<T>;
  slice(start?: number, stop?: number): Promise<T[]>;
}

/** Branch targets take a mapped task too. */
interface Condition {
  then(target: TaskRef | MappedTaskRef | Condition | Branch): ConditionElse;
}
interface ConditionElse {
  else(target: TaskRef | MappedTaskRef | Condition | Branch): void;
}
interface Branch {
  case(target: TaskRef | MappedTaskRef): Branch;
}

/** Thrown from a handler or a `map` callable to skip this instance. */
class SkipError extends Error {}

interface TaskContext {
  /** How many instances this expansion has; `null` for a task that is not mapped. */
  readonly expandedTiCount: number | null;
}

interface TaskClient {
  /** Label this instance in the UI, as `map_index_template` does for a Python task. */
  setRenderedMapIndex(label: string): Promise<void>;
}
```

## Implementation notes

What a mapped task writes, matching what `serialize_mapped_operator` writes for a decorated Python
task, so core reads one shape:

- `_is_mapped: true`, with the task's settings under `partial_kwargs` instead of at the top level.
- The expand input under `op_kwargs_expand_input`: `dict-of-lists` for `expand`, `list-of-dicts`
  for `expandKwargs` (ADR-0016, *What the Dag serialization carries*).
- The `partial` values under `partial_kwargs.op_kwargs`.
- `_arg_bindings` in call order, `partial` keys first. An `expand` call writes a `param` entry per
  key. An `expandKwargs` call writes `param` for its `partial` keys and one `kwargs` entry last. A
  key the call does not give has no entry, so the handler's default applies to it. No entry carries
  `default`, since a TypeScript Dag declares defaults only in the handler, or `value_schema`, since
  the handler's types are erased by then.
- Each `map` callable as `fn.toString()`.

JavaScript orders an object's integer-like keys first, so `{ "2": a, "1": b }` expands over `"1"`
before `"2"`. The payload is written in that order, and that is the expansion order.

## Consequences

- **Most mistakes fail in `tsc`.** That covers:
  - a key given twice, or a required key never given;
  - a key the handler does not take;
  - expanding over something that has no elements, or whose elements do not fit the argument;
  - a mapped task passed as one value;
  - every shape ADR-0016 decision 4 rejects.

  Five are left to the Dag read:
  - a `partial` never expanded, and an `expand` with no key when every remaining argument is
    optional, as Python raises for both;
  - `triggerRule: "always"` with an upstream to expand over;
  - an `expand` over a loop body task from outside its loop;
  - a `LoopMappedTaskRef` passed as a reduce from outside its loop.
- **`expandKwargs` over an upstream's objects is checked at run time only for a key `partial` also
  sets.** A key the handler requires but the object leaves out reaches it as `undefined`, and an
  extra one is passed on. Python raises for both. This is what lets the handler's own defaults apply.
- **`TaskInputs` stays as loose as it is.** A plain `TaskRef` input is still checked only as a
  `TaskRef`, as ADR-0003 found. The types above check what mapping adds without tightening it:
  expand sources and `MappedTaskRef` carry their element type, and neither is a `TaskRef`.
- **No new runtime surface beyond ADR-0016's steps.** The handler is called with one instance's
  values or an `XComSequence`. `SkipError`, `setRenderedMapIndex`, `expandedTiCount` and
  `XComSequence` are the only new exports. `SkipError` is the runtime's first general skip; until
  now only a `triggerDagRun` task ended `skipped`.
- **Removing `mapIndexTemplate` from `TaskSpec` breaks a Dag that sets it.** It no longer
  compiles, and a Dag read rejects it where the types do not reach. Today that Dag serializes a
  template nothing renders, so the break surfaces a setting that never worked.

## Alternatives

- **A per-argument marker, as a positional SDK uses**,
  `measureFile.expand({ path: expand(files), bucket: "raw" })`.
  Rejected: TypeScript can split the arguments by key, which is Python's own spelling, so
  ADR-0016 has it keep that spelling. The marker is the substitute for an SDK that cannot.
- **`MappedTaskRef<T>` as a `TaskRef<T>`, with `all()` for the reduce**, as a loop body's ref is.
  Rejected: a loop body's ref has a one-iteration reading inside the loop, which is why the outside
  reading has to be written. A mapped task has no one-instance reading anywhere but `expand`. As a
  `TaskRef` it would also pass `TaskInputs`' loose check for any argument, so passing one as a
  scalar would compile. The same holds for a mapped body task as a `LoopTaskRef<T>`.
- **An array for a reduce**, `{ sizes: number[] }`. Rejected by ADR-0016 decision 8: a reduce is a
  lazy sequence, so its argument is `XComSequence<T>`, as a loop's `all()` already gives.
- **`zip` and `concat` as free functions**, `zip(files, sizes)`. Rejected by ADR-0016, *Authoring
  interface*: methods chain in declaration order.
- **`expandKwargs` only over a literal list**, whose keys `tsc` sees. Rejected: per-instance argument
  objects an upstream builds are what `expandKwargs` is for, and spreading them lets the handler's
  defaults apply without a key list.
- **The author lists `expandKwargs`' keys and defaults in the call.** Rejected: it repeats the
  handler's signature, which a TypeScript task's handler already is (ADR-0016, *Rejected
  alternatives*).
- **Returning `null` pushes nothing, as Python's `None` does.** Rejected: TypeScript tells `null`
  from `undefined`, so `return null` is a value its author chose. Only mapping needs a `null` result
  left out, and it leaves it out without dropping the value.
- **Mapped task groups now.** Rejected for this ADR: a group would need inputs to expand over, which
  is a design of its own.
- **Deprecating `mapIndexTemplate` before removing it.** Rejected: nothing has ever rendered it, so
  a warning period would only keep a setting that does nothing.
- **`fillValue`, Python's name in TypeScript casing.** Rejected: JavaScript names the same stand-in
  `padding` in `Iterator.zip`, and the option name is not on the wire as the author writes it.
- **One `padding` value per source, as `Iterator.zip` takes.** Rejected: Python's `fillvalue`, and
  the wire, take one value for every source, and a per-source one would need a wire change for
  little use.
- **`mode`, as `Iterator.zip` has.** Rejected: `"longest"` without `padding` pads with `undefined`,
  which serializes as `null`, the same as `{ padding: null }`. Python has no `"strict"` zip to map
  with.

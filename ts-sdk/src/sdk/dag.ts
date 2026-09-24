/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// The Dag authoring surface: `new Dag(dagId)`, `dag.task(taskId, handler)`, and
// the factory it returns. Calling a factory declares the task's place in the
// Dag and supplies its arguments, the way calling a TaskFlow function does in
// Python.

import {
  DAG_SCHEMA_FIELDS,
  TASK_SCHEMA_FIELDS,
  type GeneratedDagFields,
  type GeneratedTaskFields,
} from "../generated/dag-schema-fields.js";
import { brand, DUPLICATE_COPY_HINT, hasBrand } from "./brand.js";
import type { JsonValue } from "./client-types.js";
import type { TaskFunction } from "./task.js";

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

/**
 * A copy of `spec` that nothing can change afterwards.
 *
 * Nothing reads a spec until the Dag is packed, long after the author's module
 * has run, so an edit of the object they passed would silently change what
 * ships. A shallow freeze is not enough: `tags` is an array the author still
 * holds a reference to.
 */
function freezeSpec<TSpec extends object>(spec: TSpec, describe: () => string): TSpec {
  return deepFreeze(spec, describe, new WeakSet()) as TSpec;
}

function deepFreeze(value: unknown, describe: () => string, seen: WeakSet<object>): unknown {
  if (typeof value !== "object" || value === null) return value;
  // `startDate` and `endDate` are Dates, and a Date's setters change it in
  // place, so the recorded spec takes a copy. Freezing would not help:
  // Object.freeze does not stop setFullYear.
  if (value instanceof Date) return new Date(value.getTime());
  if (seen.has(value)) {
    throw new Error(`${describe()} refers back to itself, so it cannot be recorded`);
  }
  seen.add(value);
  if (Array.isArray(value)) {
    return Object.freeze(value.map((element) => deepFreeze(element, describe, seen)));
  }
  if (!isPlainRecord(value)) {
    // Passed through, this would be neither copied nor frozen, so the author
    // could still change what ships.
    throw new Error(
      `${describe()} holds a ${kindOf(value)}, which cannot be recorded; a spec field takes a ` +
        "string, number, boolean, Date, or an array of them",
    );
  }
  const copy: Record<string, unknown> = {};
  for (const [key, nested] of Object.entries(value)) {
    copy[key] = deepFreeze(nested, describe, seen);
  }
  return Object.freeze(copy);
}

function kindOf(value: object): string {
  const prototype = Object.getPrototypeOf(value) as { constructor?: { name?: string } } | null;
  return prototype?.constructor?.name ?? "value";
}

const DAG_SPEC_KEYS: ReadonlySet<string> = new Set(Object.keys(DAG_SCHEMA_FIELDS));
const TASK_SPEC_KEYS: ReadonlySet<string> = new Set(Object.keys(TASK_SCHEMA_FIELDS));

/**
 * Dag-level options: the schedule, the tags, how many runs may be active, and
 * the rest of what `DAG(...)` takes in Python.
 *
 * Every field is optional, so `{}` stays valid and a field added later cannot
 * break a call site. An unknown key is rejected, so a misspelled field is an
 * error rather than a Dag that quietly ignores it.
 *
 * Setting a field records it. A Dag declared in TypeScript is not served to
 * Airflow yet, so nothing reads it.
 */
export type DagSpec = GeneratedDagFields;

/**
 * Task-level options: the retries, the pool, the trigger rule, and the rest of
 * what an operator takes in Python.
 *
 * Optional and record-only on the same terms as {@link DagSpec}.
 */
export type TaskSpec = GeneratedTaskFields;

// Carries a reference's return type without carrying a value. Not exported, so
// the property cannot be read or written from outside; it exists only so
// `TaskRef<boolean>` and `TaskRef<number>` are different types to the compiler.
declare const RETURN_TYPE: unique symbol;

/**
 * A reference to the result of one task, returned by calling that task.
 *
 * Identity only: the handler and the value are deliberately not exposed. Pass a
 * reference as an input of a downstream task to make that task depend on it.
 *
 * `TReturn` is the handler's return type, so a construct that needs a
 * particular one can ask for it. A reference of a narrower type is usable
 * wherever a wider one is: a `TaskRef<number>` is a `TaskRef<unknown>`.
 *
 * A reference carries a hidden brand, so a hand-written `{dagId, taskId}`
 * object is not one: an input also takes a plain JSON value, and without the
 * brand such an object could not be told apart from an upstream reference.
 */
export interface TaskRef<TReturn = unknown> {
  /** Identifier of the Dag this task belongs to. */
  readonly dagId: string;
  /** Airflow task ID, including any TaskGroup prefix. */
  readonly taskId: string;
  /** @internal Never set; see {@link RETURN_TYPE}. */
  readonly [RETURN_TYPE]?: TReturn;
}

/** Whether `value` is a TaskRef returned by any copy of this package. */
function isTaskRef(value: unknown): value is TaskRef {
  return hasBrand(value, "TaskRef");
}

/**
 * The first reference reachable inside `value`, or `undefined` when there is
 * none.
 *
 * A reference is an edge, and an edge has to be an input in its own right: one
 * buried in an array or an object is data the serializer would write into the
 * Dag verbatim, leaving the task running without the upstream it was given.
 * TypeScript rejects that for a well-typed argument, so this catches the `any`,
 * the cast and the plain-JavaScript caller.
 */
function findNestedTaskRef(value: unknown, seen: WeakSet<object>): TaskRef | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  if (isTaskRef(value)) return value;
  // A literal that refers back to itself is not JSON either, but it reaches
  // here before anything else has rejected it.
  if (seen.has(value)) return undefined;
  seen.add(value);
  for (const nested of Array.isArray(value) ? value : Object.values(value)) {
    const found = findNestedTaskRef(nested, seen);
    if (found) return found;
  }
  return undefined;
}

/**
 * The part of `T` a literal can express, matched structurally.
 *
 * `Extract<T, JsonValue>` would do for a type literal but collapses an
 * `interface` to `never`, since an interface gets no implicit index signature
 * and so never matches `JsonValue`'s object arm. An object survives only if
 * every property does, which leaves types JSON cannot carry — a `Date`, a
 * method — as `never`, so such an argument can only be given a reference.
 */
type JsonCompatible<T> = T extends JsonValue
  ? T
  : // A function is an object with no keys, so it would otherwise map to `{}`
    // and take every method of a class along with it.
    T extends (...args: never[]) => unknown
    ? never
    : T extends readonly (infer TElement)[]
      ? readonly JsonCompatible<TElement>[]
      : T extends object
        ? T extends { [K in keyof T]: JsonCompatible<T[K]> }
          ? { [K in keyof T]: JsonCompatible<T[K]> }
          : never
        : never;

/**
 * One input of a task: the upstream task that produces the value, or the value.
 *
 * A literal is restricted to the JSON-compatible part of the argument's type,
 * because it has to survive the trip through the serialized Dag. An argument
 * that cannot be expressed as JSON at all, such as a `Date`, can only be given
 * a reference.
 */
export type TaskInput<TValue> = TaskRef<TValue> | JsonCompatible<TValue>;

/** The inputs of a task that declares several arguments, in declaration order. */
export type PositionalInputs<TParams extends readonly unknown[]> = {
  [K in keyof TParams]: TaskInput<TParams[K]>;
};

/** The inputs of a task that declares one object of named arguments, by name. */
export type TaskInputs<TArgs> = {
  [K in keyof TArgs]: TaskRef | JsonCompatible<TArgs[K]>;
};

// Offered only where it means something: `TaskInputs<number>` would map over
// `number`'s own methods and accept `{ toFixed: ... }`.
type NamedInputs<TOnly> = [TOnly] extends [object] ? TaskInputs<TOnly> : never;

/**
 * What `dag.task(...)` returns: call it to declare where the task sits in the Dag.
 *
 * Pass one input per argument the handler declares, in order. An input is
 * either another task's reference, which makes this task wait for that task and
 * receive its result, or a literal value:
 *
 * ```ts
 * const extract = dag.task("extract", async (): Promise<number> => 42);
 * const transform = dag.task("transform", async (rows: number, region: string) => rows);
 * const load = dag.task("load", async (total: number) => {});
 *
 * load(transform(extract(), "us"));
 * ```
 *
 * A handler that declares a single object of named arguments can also be called
 * with that object, which names each input instead of ordering it:
 *
 * ```ts
 * const store = dag.task("store", async ({ total }: { total: number }) => {});
 *
 * store({ total: extract() });
 * ```
 *
 * The compiler checks that every argument is supplied and that each literal
 * matches its argument's type. A reference passed by position is checked against
 * the argument's type as well, which is what tells the two call shapes apart
 * when a handler declares a single argument.
 */
export type TaskFactory<TParams extends readonly unknown[], TReturn = unknown> = [TParams] extends [
  readonly [],
]
  ? () => TaskRef<TReturn>
  : TParams extends readonly [infer TOnly]
    ? (input: TaskInput<TOnly> | NamedInputs<TOnly>) => TaskRef<TReturn>
    : (...inputs: PositionalInputs<TParams>) => TaskRef<TReturn>;

/**
 * The trailing argument of `dag.task()`: the task's own {@link TaskSpec}, plus
 * the names of the handler's positional arguments.
 *
 * Every field is optional, and an unknown key is rejected, so a misspelled
 * field is an error rather than a task that quietly ignores it.
 */
export type TaskOptions = TaskSpec & {
  /**
   * Names for the handler's positional arguments, in declaration order.
   *
   * `airflow-ts-pack` fills this in from the handler's parameter list, so the
   * Dag names each argument as its handler does. Positional inputs bind by
   * order, so a name left out only costs the label: `arg0`, `arg1` and so on
   * stand in for it.
   */
  readonly argBindings?: readonly string[];
};

/** Per-task record a Dag retains: the reference, the handler, and its spec. */
export interface TaskRecord {
  readonly task: TaskRef;
  readonly fn: TaskFunction;
  readonly spec: TaskSpec;
}

/**
 * Internal: what one call to a task factory recorded, by argument name.
 *
 * A {@link TaskRef} is an edge from the upstream task; anything else is a
 * constant argument.
 *
 * Recorded for the serializer that will turn a native Dag into serialized Dag
 * JSON; nothing reads them at execution time. What a running task is called
 * with comes from the supervisor's `arg_bindings`, which are derived from the
 * serialized Dag and resolved per task instance — see decision G of
 * `airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md`. Those
 * bindings therefore win over anything recorded here, and the two never have to
 * be reconciled: a native Dag's wiring is what produced its bindings.
 */
export type RecordedInputs = Readonly<Record<string, TaskRef | JsonValue>>;

// Assigned inside Dag's static block: gives package-internal code access to
// Dag's private state without public accessors on the class.
let taskRecordsOf: (dag: Dag) => ReadonlyMap<string, TaskRecord>;
let inputsOf: (dag: Dag) => ReadonlyMap<string, RecordedInputs>;
let finalizeOf: (dag: Dag) => void;

/** Internal: whether `value` is a Dag built by any copy of this package. */
export function isDag(value: unknown): value is Dag {
  return hasBrand(value, "Dag");
}

/**
 * A Dag declared in TypeScript.
 *
 * Declare the tasks with `dag.task(taskId, handler)`, then call what each
 * returns to lay the Dag out:
 *
 * ```ts
 * const extract = dag.task("extract", async (): Promise<number> => 42);
 * const load = dag.task("load", async ({ rows }: { rows: number }) => {});
 * load({ rows: extract() });
 * ```
 *
 * Every task has to be called exactly once, so none can be left out of the Dag
 * by accident. A TypeScript handler for a task that a *Python* Dag declares is
 * a `TaskHandler` instead, not a task on a `Dag`.
 *
 * Constructing a Dag has no effect beyond the instance itself. Collect the ones
 * a bundle should serve on a `Bundle` and await `bundle.serve()`.
 */
export class Dag {
  /** Identifier of this Dag. */
  readonly dagId: string;
  /** Dag-level options this instance was constructed with, copied and frozen. */
  readonly spec: DagSpec;
  readonly #tasks = new Map<string, TaskRecord>();
  readonly #inputs = new Map<string, RecordedInputs>();
  #finalized = false;

  static {
    taskRecordsOf = (dag) => dag.#tasks;
    inputsOf = (dag) => dag.#inputs;
    finalizeOf = (dag) => dag.#finalize();
  }

  constructor(dagId: string, spec: DagSpec = {}) {
    validateDagSpec(dagId, spec);
    brand(this, "Dag");
    this.dagId = dagId;
    this.spec = freezeSpec(spec, () => `The spec for Dag "${dagId}"`);
  }

  /** Task IDs attached to this Dag, in attachment order. */
  get taskIds(): readonly string[] {
    return [...this.#tasks.keys()];
  }

  /**
   * Declare a task of this Dag, and return the factory that places it.
   *
   * Every argument the handler declares becomes an input of the returned
   * {@link TaskFactory}; `getContext()` and `getClient()` reach the runtime
   * from inside the call, so neither is an argument. The trailing options object
   * carries this task's own {@link TaskSpec}.
   */
  task<TParams extends readonly unknown[] = [], TReturn = unknown>(
    taskId: string,
    handler: (...args: TParams) => TReturn | Promise<TReturn>,
    options: TaskOptions = {},
  ): TaskFactory<TParams, TReturn> {
    if (typeof handler !== "function") {
      throw new Error(`handler for Dag "${this.dagId}" task "${taskId}" must be a function`);
    }
    if (this.#tasks.has(taskId)) {
      throw new Error(`Task "${taskId}" is already registered for Dag "${this.dagId}"`);
    }
    // A task added after the Dag was read could no longer be wired into it, and
    // would sit in the Dag unplaced and unreported.
    if (this.#finalized) {
      throw new Error(
        `Task "${taskId}" cannot be added to Dag "${this.dagId}" after the Dag was read; ` +
          "declare every task while the module is loading",
      );
    }
    const spec = this.#taskSpecOf(taskId, options);
    const argBindings = this.#validateArgBindings(taskId, options.argBindings);
    const task = createTaskRef(this.dagId, taskId);
    this.#tasks.set(taskId, {
      task,
      // The runtime dispatches every handler through one instantiation, as it
      // does a registered TaskHandler; a positional one is wrapped at wiring.
      fn: handler as unknown as TaskFunction,
      spec: freezeSpec(spec, () => `The spec for Dag "${this.dagId}" task "${taskId}"`),
    });
    return ((...inputs: unknown[]) => {
      this.#wire(taskId, inputs, argBindings);
      return task;
    }) as TaskFactory<TParams, TReturn>;
  }

  // TypeScript is bypassable — from plain JavaScript, or an `as TaskSpec` cast
  // — so an unknown key is rejected rather than silently ignored. `argBindings`
  // names the handler's arguments rather than configuring the task, so it is
  // taken out here instead of reaching the spec.
  #taskSpecOf(taskId: string, options: TaskOptions): TaskSpec {
    const value: unknown = options;
    if (!isPlainRecord(value)) {
      throw new Error(`spec for Dag "${this.dagId}" task "${taskId}" must be an object`);
    }
    const spec: Record<string, unknown> = {};
    for (const key of Reflect.ownKeys(value)) {
      if (key === "argBindings") continue;
      if (typeof key !== "string" || !TASK_SPEC_KEYS.has(key)) {
        throw new Error(
          `Unknown option "${String(key)}" in the spec for Dag "${this.dagId}" task "${taskId}"`,
        );
      }
      spec[key] = value[key];
    }
    return spec as TaskSpec;
  }

  // TypeScript is bypassable, and these names become the keys the arguments are
  // recorded under, so an integer-like one would reorder what it labels.
  #validateArgBindings(taskId: string, names: unknown): readonly string[] | undefined {
    if (names === undefined) return undefined;
    const describe = `argBindings for Dag "${this.dagId}" task "${taskId}"`;
    if (!Array.isArray(names)) throw new Error(`${describe} must be an array of names`);
    const seen = new Set<string>();
    for (const name of names as unknown[]) {
      if (typeof name !== "string" || name.length === 0 || /^\d+$/.test(name)) {
        throw new Error(
          `${describe} holds ${JSON.stringify(name)}; each name must be a non-empty ` +
            "string that is not a number",
        );
      }
      if (seen.has(name)) throw new Error(`${describe} names "${name}" twice`);
      seen.add(name);
    }
    return Object.freeze([...(names as string[])]);
  }

  #wire(
    taskId: string,
    inputs: readonly unknown[],
    argBindings: readonly string[] | undefined,
  ): void {
    if (this.#finalized) {
      throw new Error(
        `Task "${taskId}" of Dag "${this.dagId}" was called after the Dag was read; ` +
          "call every task while the module is loading",
      );
    }
    if (this.#inputs.has(taskId)) {
      throw new Error(
        `Task "${taskId}" of Dag "${this.dagId}" was already called; a task holds one place ` +
          "in a Dag, so call it once and reuse the reference",
      );
    }
    const positional = !isNamedCall(inputs);
    const recorded = this.#checkInputs(
      taskId,
      positional ? positionalInputs(inputs, argBindings) : (inputs[0] as Record<string, unknown>),
    );
    this.#inputs.set(taskId, recorded);
    if (positional && inputs.length > 0) {
      const record = this.#tasks.get(taskId)!;
      this.#tasks.set(taskId, { ...record, fn: spreadArgs(record.fn) });
    }
  }

  #checkInputs(taskId: string, inputs: Record<string, unknown>): RecordedInputs {
    // Every own key, not just the enumerable string ones: a symbol key would be
    // copied by the spread below and so has to be checked, not stepped over.
    for (const key of Reflect.ownKeys(inputs)) {
      if (typeof key === "symbol") {
        throw new Error(
          `Input "${String(key)}" of task "${taskId}" is keyed by a symbol; ` +
            "an argument name is a string",
        );
      }
      const value = inputs[key];
      // Anything unbranded is a literal argument, including a look-alike
      // `{dagId, taskId}` object: only a real reference makes an edge.
      if (!isTaskRef(value)) {
        const nested = findNestedTaskRef(value, new WeakSet());
        if (nested) {
          throw new Error(
            `Input "${key}" of task "${taskId}" holds a reference to "${nested.taskId}" inside a ` +
              "literal value, which draws no edge; pass the reference as the input itself, or " +
              "give each upstream its own argument",
          );
        }
        continue;
      }
      if (value.dagId !== this.dagId) {
        throw new Error(
          `Input "${key}" of task "${taskId}" comes from Dag "${value.dagId}", not "${this.dagId}"`,
        );
      }
      const upstream = this.#tasks.get(value.taskId);
      if (!upstream) {
        throw new Error(
          `Input "${key}" of task "${taskId}" refers to unregistered task "${value.taskId}"`,
        );
      }
      // Identity, not just the ID pair: two Dag objects can carry the same
      // dagId, and a second resolved copy of this package brands its own
      // references, so matching IDs do not make a reference this Dag handed out.
      if (upstream.task !== value) {
        throw new Error(
          `Input "${key}" of task "${taskId}" was not returned by this Dag's "${value.taskId}"; ` +
            `it comes from another Dag object with the same ID, or ${DUPLICATE_COPY_HINT}`,
        );
      }
    }
    return Object.freeze({ ...inputs }) as RecordedInputs;
  }

  #finalize(): void {
    if (this.#finalized) return;
    for (const taskId of this.#tasks.keys()) {
      if (!this.#inputs.has(taskId)) {
        throw new Error(
          `Task "${taskId}" of Dag "${this.dagId}" is never called, so it has no place in the ` +
            "Dag; call the factory dag.task(...) returned",
        );
      }
    }
    // Last, so a Dag that failed the check reports that same failure again
    // rather than reporting itself as already read.
    this.#finalized = true;
  }
}

/**
 * Whether a call named its inputs rather than ordering them.
 *
 * One plain object is the named form. A reference is a plain object too, so it
 * is ruled out first: `load(extract())` is one positional input, not a map of
 * argument names.
 */
function isNamedCall(inputs: readonly unknown[]): boolean {
  return inputs.length === 1 && !isTaskRef(inputs[0]) && isPlainRecord(inputs[0]);
}

function positionalInputs(
  inputs: readonly unknown[],
  argBindings: readonly string[] | undefined,
): Record<string, unknown> {
  const byName: Record<string, unknown> = {};
  inputs.forEach((value, index) => {
    byName[argBindings?.[index] ?? `arg${index}`] = value;
  });
  return byName;
}

/**
 * Dispatch a positional handler through the one call shape the runtime uses.
 *
 * A task is called with its bound arguments as a single object, in the order
 * they were recorded, so spreading its values back restores the argument list
 * the handler declared.
 */
function spreadArgs(fn: TaskFunction): TaskFunction {
  const handler = fn as unknown as (...args: unknown[]) => unknown;
  return ((args: Record<string, unknown>) =>
    handler(...Object.values(args ?? {}))) as unknown as TaskFunction;
}

function createTaskRef(dagId: string, taskId: string): TaskRef {
  const task: TaskRef = { dagId, taskId };
  brand(task, "TaskRef");
  return Object.freeze(task);
}

function validateDagSpec(dagId: string, spec: DagSpec): void {
  const value: unknown = spec;
  if (!isPlainRecord(value)) {
    throw new Error(`spec for Dag "${dagId}" must be an object`);
  }
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== "string" || !DAG_SPEC_KEYS.has(key)) {
      throw new Error(`Unknown option "${String(key)}" in the spec for Dag "${dagId}"`);
    }
  }
}

/**
 * Internal: the task records of a Dag, for bundle lookups.
 *
 * Not re-exported from the package root, and the package `"exports"` map
 * blocks deep imports, so this is unreachable from outside the SDK.
 */
export function getDagTaskRecords(dag: Dag): ReadonlyMap<string, TaskRecord> {
  return taskRecordsOf(dag);
}

/** Internal: what each task of a Dag was called with, keyed by task ID.
 *  A task that has not been called is absent. */
export function getDagTaskInputs(dag: Dag): ReadonlyMap<string, RecordedInputs> {
  return inputsOf(dag);
}

/**
 * Internal: check that `dag` is fully laid out, then finalize it against further wiring.
 *
 * Idempotent, and never part of the public surface: a Dag is finished when its
 * module has finished loading, so there is nothing for an author to call.
 */
export function finalizeDag(dag: Dag): void {
  finalizeOf(dag);
}

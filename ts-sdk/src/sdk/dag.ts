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
import { argListValues, isArgList, type ArgList } from "./arg-list.js";
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
// `taskId` is hand-written rather than generated: the schema's task_id is
// serializer-owned, and this is the authoring surface's own way to set it.
const TASK_SPEC_KEYS: ReadonlySet<string> = new Set([...Object.keys(TASK_SCHEMA_FIELDS), "taskId"]);

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
 * The task id is here too, for a handler whose id is not given positionally.
 * Optional and record-only on the same terms as {@link DagSpec}.
 */
export interface TaskSpec extends GeneratedTaskFields {
  /**
   * Airflow task ID, when it should not be the handler's function name.
   *
   * `dag.task(handler)` takes the id from the handler's name. Set this for an
   * id that has to outlive that name, or for an anonymous handler, which has
   * no name to take one from.
   */
  readonly taskId?: string;
}

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

/** The inputs of a task, keyed by the name of the argument each one supplies. */
export type TaskInputs<TArgs> = {
  [K in keyof TArgs]: TaskRef | JsonCompatible<TArgs[K]>;
};

// One value of a listed call. Which argument it supplies is decided by where
// it sits, so every argument's type is allowed in every position. Mapped per
// argument rather than over `TArgs[keyof TArgs]`, so one argument typed
// `unknown` does not swallow what the others accept.
type ListedInput<TArgs> = TaskRef | { [K in keyof TArgs]: JsonCompatible<TArgs[K]> }[keyof TArgs];

/**
 * What `dag.task(...)` returns: call it to declare where the task sits in the Dag.
 *
 * A handler takes one object of named arguments, and the call names each
 * input. An input is either another task's reference, which makes this task
 * wait for that task and receive its result, or a literal value:
 *
 * ```ts
 * const extract = dag.task("extract", async (): Promise<number> => 42);
 * const transform = dag.task(
 *   "transform",
 *   async ({ rows, region }: { rows: number; region: string }) => rows,
 * );
 * const load = dag.task("load", async ({ total }: { total: number }) => {});
 *
 * const extracted = extract();
 * load({ total: transform({ rows: extracted, region: "us" }) });
 * ```
 *
 * {@link withArgList} gives the same inputs in the order the handler
 * destructures its argument, for a call that reads better that way:
 *
 * ```ts
 * transform(withArgList(extracted, "us"));
 * ```
 *
 * A named call is checked argument by argument: every one has to be supplied,
 * and a literal has to match its argument's type. A listed call is checked by
 * value, and which argument each value lands on is the order it is given in.
 */
export type TaskFactory<TArgs extends object | void = void, TReturn = unknown> = [TArgs] extends [
  void,
]
  ? () => TaskRef<TReturn>
  : ((inputs: TaskInputs<TArgs>) => TaskRef<TReturn>) &
      ((inputs: ArgList<readonly ListedInput<TArgs>[]>) => TaskRef<TReturn>);

/**
 * The trailing argument of `dag.task()`: the task's own {@link TaskSpec}.
 *
 * Every field is optional, and an unknown key is rejected, so a misspelled
 * field is an error rather than a task that quietly ignores it.
 */
export type TaskOptions = TaskSpec;

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
   * A handler takes one object of named arguments, and every argument in it
   * becomes an input of the returned {@link TaskFactory}; `getContext()` and
   * `getClient()` reach the runtime from inside the call, so neither is an
   * argument. The trailing options object carries this task's own
   * {@link TaskSpec}.
   *
   * ```ts
   * const extract = dag.task("extract", async () => 42);
   * const transform = dag.task(async function transform() {}); // id "transform"
   * ```
   */
  task<TArgs extends object | void = void, TReturn = unknown>(
    taskId: string,
    handler: (args: TArgs) => TReturn | Promise<TReturn>,
    options?: TaskOptions,
  ): TaskFactory<TArgs, TReturn>;
  /**
   * Declare a task whose id is the handler's function name.
   *
   * `airflow-ts-pack` keeps handler names intact, so minification cannot change
   * a task id. An anonymous handler has no name to take one from, and needs
   * {@link TaskSpec.taskId} to give it one.
   */
  task<TArgs extends object | void = void, TReturn = unknown>(
    handler: (args: TArgs) => TReturn | Promise<TReturn>,
    options?: TaskOptions,
  ): TaskFactory<TArgs, TReturn>;
  task<TArgs extends object | void = void, TReturn = unknown>(
    taskIdOrHandler: string | ((args: TArgs) => TReturn | Promise<TReturn>),
    handlerOrOptions?: ((args: TArgs) => TReturn | Promise<TReturn>) | TaskOptions,
    maybeOptions?: TaskOptions,
  ): TaskFactory<TArgs, TReturn> {
    const idGiven = typeof taskIdOrHandler === "string";
    const handler = (idGiven ? handlerOrOptions : taskIdOrHandler) as (
      args: TArgs,
    ) => TReturn | Promise<TReturn>;
    const given = idGiven ? maybeOptions : (handlerOrOptions as TaskOptions | undefined);
    // Defaulted only when absent: an explicit `null` is a bad spec, not an
    // omitted one, and #taskSpecOf is what reports it.
    const options = given === undefined ? {} : given;
    const specTaskId =
      isPlainRecord(options) && typeof options.taskId === "string" ? options.taskId : undefined;
    // Two ids for one task disagree silently otherwise: the positional one
    // wins and the spec's is dropped without a word.
    if (idGiven && specTaskId !== undefined) {
      throw new Error(
        `Task "${taskIdOrHandler}" of Dag "${this.dagId}" also carries taskId "${specTaskId}" in ` +
          "its spec; give the id once, either positionally or in the spec",
      );
    }
    const defaulted = idGiven ? undefined : (specTaskId ?? readFunctionName(handler));
    const taskId = idGiven ? taskIdOrHandler : defaulted;
    if (taskId === undefined) {
      throw new Error(
        `A task of Dag "${this.dagId}" has no id: its handler has no name to take one from. ` +
          'Pass an id — dag.task("my_task", handler) — or give the handler a name. A bundler ' +
          "that drops function names also lands here; airflow-ts-pack keeps them.",
      );
    }
    // A name Airflow would reject is worth catching where it was taken, not in
    // the server's answer: `fn.bind(...)` names itself "bound extract", and a
    // method can be named anything at all.
    if (defaulted !== undefined && !TASK_ID_CHARACTERS.test(defaulted)) {
      throw new Error(
        `A task of Dag "${this.dagId}" would take the id "${defaulted}" from its handler's name, ` +
          "which Airflow does not accept; give it an id of letters, digits, dashes, dots and " +
          'underscores — dag.task("my_task", handler)',
      );
    }
    if (typeof handler !== "function") {
      throw new Error(`handler for Dag "${this.dagId}" task "${taskId}" must be a function`);
    }
    // TypeScript already says so, but a plain-JavaScript author lands here
    // with the argument list Python would take.
    if (handler.length > 1) {
      throw new Error(
        `Handler for Dag "${this.dagId}" task "${taskId}" declares ${handler.length} parameters; ` +
          "a handler takes one object of named arguments — async ({ rows, region }) => ...",
      );
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
    const task = createTaskRef(this.dagId, taskId);
    this.#tasks.set(taskId, {
      task,
      // The runtime dispatches every handler through one instantiation, as it
      // does a registered TaskHandler.
      fn: handler as unknown as TaskFunction,
      spec: freezeSpec(spec, () => `The spec for Dag "${this.dagId}" task "${taskId}"`),
    });
    return ((inputs?: unknown) => {
      this.#wire(taskId, inputs);
      return task;
    }) as TaskFactory<TArgs, TReturn>;
  }

  // TypeScript is bypassable — from plain JavaScript, or an `as TaskSpec` cast
  // — so an unknown key is rejected rather than silently ignored.
  #taskSpecOf(taskId: string, options: TaskOptions): TaskSpec {
    const value: unknown = options;
    if (!isPlainRecord(value)) {
      throw new Error(`spec for Dag "${this.dagId}" task "${taskId}" must be an object`);
    }
    const spec: Record<string, unknown> = {};
    for (const key of Reflect.ownKeys(value)) {
      if (typeof key !== "string" || !TASK_SPEC_KEYS.has(key)) {
        throw new Error(
          `Unknown option "${String(key)}" in the spec for Dag "${this.dagId}" task "${taskId}"`,
        );
      }
      spec[key] = value[key];
    }
    return spec as TaskSpec;
  }

  #wire(taskId: string, inputs: unknown): void {
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
    this.#inputs.set(taskId, this.#checkInputs(taskId, this.#inputsByName(taskId, inputs)));
  }

  // A call names its inputs, or lists them with withArgList(...) to give them
  // in the order the handler destructures them.
  #inputsByName(taskId: string, inputs: unknown): Record<string, unknown> {
    if (inputs === undefined) return {};
    if (isArgList(inputs)) return this.#byPosition(taskId, argListValues(inputs));
    // A reference is a plain object too, so `load(extracted)` would otherwise
    // read as a map of argument names.
    if (!isPlainRecord(inputs) || isTaskRef(inputs)) {
      throw new Error(
        `Task "${taskId}" of Dag "${this.dagId}" takes one object naming its inputs — ` +
          "myTask({ rows, region }) — or withArgList(...) to give them in order",
      );
    }
    return inputs;
  }

  #byPosition(taskId: string, values: readonly unknown[]): Record<string, unknown> {
    const keys = destructuredKeys(this.#tasks.get(taskId)!.fn);
    if (keys === undefined) {
      throw new Error(
        `Task "${taskId}" of Dag "${this.dagId}" cannot take its inputs in order: its handler's ` +
          "argument is not a plain object pattern — async ({ rows, region }) => ... — so name " +
          "the inputs instead",
      );
    }
    if (keys.length !== values.length) {
      throw new Error(
        `Task "${taskId}" of Dag "${this.dagId}" takes ${keys.length} arguments ` +
          `(${keys.join(", ")}) but was given ${values.length}`,
      );
    }
    return Object.fromEntries(keys.map((key, index) => [key, values[index]]));
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

// What Airflow accepts as a task id, and so what a name taken from a handler
// has to look like.
const TASK_ID_CHARACTERS = /^[\p{L}\p{N}_.-]+$/u;

/**
 * A handler's own function name, or undefined when it has none.
 *
 * Where a defaulted task id comes from. `airflow-ts-pack` bundles with esbuild's
 * `keepNames`, so the name survives minification; a bundler that drops names
 * leaves the empty string, which is why the empty string is not an id.
 */
function readFunctionName(handler: unknown): string | undefined {
  if (typeof handler !== "function") return undefined;
  const { name } = handler as { name?: unknown };
  return typeof name === "string" && name.length > 0 ? name : undefined;
}

// One key of a handler's argument pattern, or a rename of one: the property
// name is what the Dag records, whatever the binding beside it is called.
const DESTRUCTURED_KEY = /^([A-Za-z_$][\w$]*)\s*(?::\s*[A-Za-z_$][\w$]*)?$/;

/**
 * The keys a handler destructures, in source order, or undefined when its
 * argument is not one plain object pattern.
 *
 * The order a listed call binds by: the first value supplies the first key.
 * A pattern that holds anything else, such as a default, a rest element or a
 * nested pattern, has no order that can be read with certainty, so it is
 * refused rather than guessed at. Keys are property names, which survive
 * minification, so a packed bundle reads the same as its source.
 */
function destructuredKeys(handler: unknown): readonly string[] | undefined {
  if (typeof handler !== "function" || handler.length !== 1) return undefined;
  const source = Function.prototype.toString.call(handler);
  const open = source.indexOf("(");
  const arrow = source.indexOf("=>");
  // A "(" past the arrow opens the body, not the parameter list.
  if (open === -1 || (arrow !== -1 && open > arrow)) return undefined;
  const brace = source.indexOf("{", open);
  const close = source.indexOf("}", brace);
  // The parameter has to be the pattern itself, and the pattern has to close
  // before anything nests inside it.
  if (brace === -1 || close === -1 || source.slice(open + 1, brace).trim() !== "") return undefined;
  const parts = source
    .slice(brace + 1, close)
    .split(",")
    .map((part) => part.trim());
  // Prettier writes a trailing comma into a pattern it breaks over lines.
  if (parts.at(-1) === "") parts.pop();
  const keys = parts.map((part) => DESTRUCTURED_KEY.exec(part)?.[1]);
  if (keys.length === 0 || keys.some((key) => key === undefined)) return undefined;
  return Object.freeze(keys as string[]);
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

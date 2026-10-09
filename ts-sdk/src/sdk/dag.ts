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

import type { CoordinatorClient } from "../coordinator/client.js";
import {
  DAG_SCHEMA_FIELDS,
  TASK_SCHEMA_FIELDS,
  type GeneratedDagFields,
  type GeneratedTaskFields,
} from "../generated/dag-schema-fields.js";
import { brand, DUPLICATE_COPY_HINT, hasBrand } from "./brand.js";
import type { JsonValue } from "./client-types.js";
import { getCurrentModuleSource } from "./module-source.js";
import { isOperator, type Operator } from "./operator.js";
import { getClient, type TaskFunction } from "./task.js";

/** Internal: whether `value` is an object literal, not an array or a class instance. */
export function isPlainRecord(value: unknown): value is Record<string, unknown> {
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

const DAG_SPEC_KEYS: ReadonlySet<string> = new Set([...Object.keys(DAG_SCHEMA_FIELDS), "queue"]);
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
 * `queue` is the one hand-written field: Airflow's schema has no Dag-level
 * queue, but every task of a native Dag runs on the same coordinator, so the
 * queue that routes them there belongs on the Dag rather than on each task.
 */
export interface DagSpec extends GeneratedDagFields {
  /**
   * Queue the Dag's tasks run on, unless a task names its own.
   *
   * A native Dag's tasks are executed by the Node coordinator, which the
   * deployment's `queue_to_coordinator` maps a queue to, so this is what
   * routes them there. `queue` on a {@link TaskSpec} wins for that task.
   */
  readonly queue?: string;
}

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
 * `TReturn` is the handler's return type, so a construct that needs a
 * particular one can ask for it. A reference of a narrower type is usable
 * wherever a wider one is: a `TaskRef<number>` is a `TaskRef<unknown>`.
 *
 * A reference carries a hidden brand, so a hand-written `{dagId, taskId}`
 * object is not one: an input also takes a plain JSON value, and without the
 * brand such an object could not be told apart from an upstream reference.
 */
/**
 * What an order-only edge can connect: a task, or a whole task group.
 *
 * The TypeScript counterpart of Python's `DAGNode` and the Go SDK's
 * `airflow.Node`. A group carries edges as a task does, so `before` and
 * `after` take either.
 */
export interface Node {
  /** Identifier of the Dag this node belongs to. */
  readonly dagId: string;
  /**
   * Run this node before each of `downstream`, carrying no value — the
   * TypeScript spelling of Python's `>>`.
   */
  before(...downstream: readonly Node[]): Node;
  /** Run this node after each of `upstream`, carrying no value — Python's `<<`. */
  after(...upstream: readonly Node[]): Node;
}

export interface TaskRef<TReturn = unknown> extends Node {
  /** Identifier of the Dag this task belongs to. */
  readonly dagId: string;
  /** Airflow task ID, including any TaskGroup prefix. */
  readonly taskId: string;
  /** @internal Never set; see {@link RETURN_TYPE}. */
  readonly [RETURN_TYPE]?: TReturn;
  /**
   * Run this task before each of `downstream`, carrying no value — the
   * TypeScript spelling of Python's `>>`.
   *
   * ```ts
   * loaded.before(cleaned, notified); // loaded >> [cleanup, notify]
   * ```
   *
   * Variadic, so one call fans out, and it returns its own receiver rather
   * than its arguments: a fan-out has no single "next" reference to hand back.
   * Declaring an edge that already exists changes nothing.
   */
  before(...downstream: readonly Node[]): TaskRef<TReturn>;
  /**
   * Run this task after each of `upstream`, carrying no value — Python's `<<`.
   *
   * ```ts
   * cleaned.after(loaded, transformed); // [load, transform] >> cleanup
   * ```
   *
   * Fan-*in* that carries data is the wiring object instead
   * (`summarize({ north: extractNorth(), south: extractSouth() })`), so each
   * direction has an answer: named keys when values flow, `after` when only
   * order does.
   */
  after(...upstream: readonly Node[]): TaskRef<TReturn>;
}

/**
 * A scope that declares tasks under a shared id prefix, and carries edges as a
 * whole — Python's `TaskGroup`, spelled to TypeScript convention.
 *
 * Offers the same `task` and `taskGroup` methods as the Dag, so nesting is the
 * same call at every depth. Each id it declares is prefixed with the group id
 * unless {@link TaskGroupOptions.prefixGroupId} turns that off, and the group
 * itself stands at either end of an edge, so a whole group can be ordered
 * against a task or against another group.
 */
export interface TaskGroupRef extends Node {
  /** Identifier of the Dag this group belongs to. */
  readonly dagId: string;
  /** Group ID, including any enclosing group's prefix. */
  readonly groupId: string;
  /** Declare a task in this group; its id carries the group prefix unless it is turned off. */
  task<TArgs extends object | void = void, TReturn = unknown>(
    taskId: string,
    handler: (args: TArgs) => TReturn | Promise<TReturn>,
    options?: TaskOptions,
  ): TaskFactory<TArgs, TReturn>;
  /** Declare a task whose id is the handler's function name, prefixed the same way. */
  task<TArgs extends object | void = void, TReturn = unknown>(
    handler: (args: TArgs) => TReturn | Promise<TReturn>,
    options?: TaskOptions,
  ): TaskFactory<TArgs, TReturn>;
  /** Declare a task an operator runs, such as `triggerDagRun(...)`, prefixed the same way. */
  task<TArgs extends object | void = void, TResult = unknown>(
    taskId: string,
    operator: Operator<TArgs, TResult>,
    options?: TaskOptions,
  ): TaskFactory<TArgs, TResult>;
  task<TArgs extends object | void = void, TResult = unknown>(
    operator: Operator<TArgs, TResult>,
    options: TaskOptions,
  ): TaskFactory<TArgs, TResult>;
  /** Nest a group inside this one. */
  taskGroup(groupId: string, options?: TaskGroupOptions): TaskGroupRef;
  before(...downstream: readonly Node[]): TaskGroupRef;
  after(...upstream: readonly Node[]): TaskGroupRef;
}

/** Options of a task group, the trailing argument of `taskGroup()`. */
export interface TaskGroupOptions {
  /**
   * Whether the ids declared in the group are prefixed with the group's, as
   * in `staging.load`. On by default, as in Python; turning it off keeps them
   * as written, as `prefix_group_id=False` does, so they have to be unique
   * across the Dag.
   */
  readonly prefixGroupId?: boolean;
}

/**
 * An order-only edge of a Dag, between two node IDs.
 *
 * An endpoint is a task ID or a group ID; the two share one namespace, so a
 * bare ID names exactly one node. {@link TaskGroupRecord} is what tells a
 * consumer which kind an endpoint is, and which tasks a group endpoint stands
 * for.
 *
 * Kept apart from the wiring a factory call records, because an edge that
 * carries no value has no argument name to be recorded under.
 */
export interface OrderEdge {
  readonly upstream: string;
  readonly downstream: string;
}

// A task id cannot hold a NUL, so a joined pair cannot collide with one.
const EDGE_KEY_SEPARATOR = "\u0000";

/** Internal: one group of a Dag, and the tree beneath it. */
export interface TaskGroupRecord {
  /** Group ID, including any enclosing group's prefix. */
  readonly groupId: string;
  /** Enclosing group's ID, absent for a group declared on the Dag itself. */
  readonly parentGroupId?: string;
  /** Whether the IDs declared in this group carry its ID as a prefix. */
  readonly prefixGroupId: boolean;
  /** Task IDs declared directly in this group, in declaration order. */
  readonly taskIds: readonly string[];
  /** Group IDs nested directly in this group, in declaration order. */
  readonly childGroupIds: readonly string[];
}

/** Separates a group ID from what it contains, as Python's `prefix_group_id` does. */
const GROUP_SEPARATOR = ".";

/** The Dag's own view of a group, which it appends to as an author declares. */
interface MutableTaskGroupRecord extends TaskGroupRecord {
  readonly taskIds: string[];
  readonly childGroupIds: string[];
}

/** Whether `value` is a task group returned by any copy of this package. */
function isTaskGroupRef(value: unknown): value is TaskGroupRef {
  return hasBrand(value, "TaskGroupRef");
}

/** The ID an edge endpoint is recorded under: a task ID or a group ID. */
function nodeId(node: Node): string | undefined {
  if (isTaskRef(node)) return node.taskId;
  if (isTaskGroupRef(node)) return node.groupId;
  return undefined;
}

/** Internal: whether `value` is a TaskRef returned by any copy of this package. */
export function isTaskRef(value: unknown): value is TaskRef {
  return hasBrand(value, "TaskRef");
}

const conditionTasks = new WeakMap<object, TaskRef>();

function resolveNode(node: Node): Node {
  return (typeof node === "object" && node !== null && conditionTasks.get(node)) || node;
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
 * The call is checked argument by argument: every one has to be supplied, and
 * a literal has to match its argument's type.
 */
export type TaskFactory<TArgs extends object | void = void, TReturn = unknown> = [TArgs] extends [
  void,
]
  ? () => TaskRef<TReturn>
  : (inputs: TaskInputs<TArgs>) => TaskRef<TReturn>;

/**
 * The trailing argument of `dag.task()`: the task's own {@link TaskSpec}.
 *
 * Every field is optional, and an unknown key is rejected, so a misspelled
 * field is an error rather than a task that quietly ignores it.
 */
export type TaskOptions = TaskSpec;

/**
 * A placed condition: name the task each outcome runs. `else` is optional.
 *
 * ```ts
 * dag.if(hasRows, { rows: validated }).then(loadIfReady).else(loadFallback);
 * ```
 */
export interface Condition extends Node {
  /** Airflow task ID of the deciding task. */
  readonly taskId: string;
  then(taskRef: TaskRef | Condition | Branch): ConditionElse;
  before(...downstream: readonly Node[]): Condition;
  after(...upstream: readonly Node[]): Condition;
}

/** What `.then(...)` returns: the other side, which a one-sided condition omits. */
export interface ConditionElse {
  else(taskRef: TaskRef | Condition | Branch): void;
}

/** The arguments after a decider's handler: its inputs, then its {@link TaskSpec}. */
export type DeciderArgs<TArgs extends object | void> = [TArgs] extends [void]
  ? [inputs?: undefined, spec?: TaskOptions]
  : [inputs: TaskInputs<TArgs>, spec?: TaskOptions];

/**
 * A placed multi-way branch: name each task the decider chooses between.
 *
 * ```ts
 * dag.switch(pickPath, { rows: extracted }).case(handleLong).case(handleShort);
 * ```
 */
export interface Branch extends Node {
  /** Airflow task ID of the deciding task. */
  readonly taskId: string;
  case(taskRef: TaskRef): Branch;
  before(...downstream: readonly Node[]): Branch;
  after(...upstream: readonly Node[]): Branch;
}

/** Per-task record a Dag retains: the reference, what runs it, and its spec. */
export type TaskRecord = {
  readonly task: TaskRef;
  readonly spec: TaskSpec;
  /** Whether this task decides which of its downstream tasks to skip. */
  readonly canSkipDownstream?: boolean;
} & TaskBody;

/** What a task runs: the author's handler, or an operator the runtime runs. */
type TaskBody =
  | { readonly fn: TaskFunction; readonly operator?: never }
  | { readonly fn?: never; readonly operator: Operator<never, unknown> };

interface ConditionRecord {
  whenTrue?: TaskRef;
  whenFalse?: TaskRef;
}

// Mirrors `SkipMixin.skip` in `task-sdk/src/airflow/sdk/bases/skipmixin.py`.
const SKIPMIXIN_XCOM_KEY = "skipmixin_key";
const SKIPMIXIN_SKIPPED = "skipped";

/**
 * Internal: skip `taskIds` of the running task's Dag run, as `SkipMixin.skip` does. The skipped
 * IDs are written first, so a downstream task that is cleared later is skipped again.
 */
export async function skipTasks(
  client: CoordinatorClient,
  taskIds: readonly string[],
): Promise<void> {
  await client.setXCom({ key: SKIPMIXIN_XCOM_KEY, value: { [SKIPMIXIN_SKIPPED]: [...taskIds] } });
  await client.skipDownstreamTasks(taskIds);
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
let orderEdgesOf: (dag: Dag) => readonly OrderEdge[];
let definedInOf: (dag: Dag) => string | undefined;
let groupsOf: (dag: Dag) => ReadonlyMap<string, TaskGroupRecord>;
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
  // Keyed by the two task ids, so declaring an edge twice records it once, and
  // insertion-ordered so the serialized Dag reads as written.
  readonly #orderEdges = new Map<string, OrderEdge>();
  readonly #definedIn: string | undefined;
  readonly #conditions = new Map<string, ConditionRecord>();
  readonly #branches = new Map<string, readonly TaskRef[]>();
  // Keyed by full group ID; a group's own record holds what it declares, so
  // the tree is reconstructed by walking from the roots.
  readonly #groups = new Map<string, MutableTaskGroupRecord>();
  // The one reference each group was handed out as, so an edge endpoint can be
  // checked by identity the way a task's is.
  readonly #groupRefs = new Map<string, TaskGroupRef>();
  #finalized = false;

  static {
    taskRecordsOf = (dag) => dag.#tasks;
    inputsOf = (dag) => dag.#inputs;
    orderEdgesOf = (dag) => [...dag.#orderEdges.values()];
    definedInOf = (dag) => dag.#definedIn;
    groupsOf = (dag) => dag.#groups;
    finalizeOf = (dag) => dag.#finalize();
  }

  constructor(dagId: string, spec: DagSpec = {}) {
    validateDagSpec(dagId, spec);
    brand(this, "Dag");
    this.dagId = dagId;
    this.spec = freezeSpec(spec, () => `The spec for Dag "${dagId}"`);
    // `airflow-ts-pack` tags each author-owned source file with its own path
    // right before its non-import statements run, so this is the file the
    // author wrote `new Dag(...)` in — even though esbuild has since inlined
    // every module into one bundle.
    this.#definedIn = getCurrentModuleSource();
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
  /**
   * Declare a task an operator runs, such as `triggerDagRun(spec)`. Its inputs and result are the
   * operator's own. It has no handler to take an id from, so the id is required.
   */
  task<TArgs extends object | void = void, TResult = unknown>(
    taskId: string,
    operator: Operator<TArgs, TResult>,
    options?: TaskOptions,
  ): TaskFactory<TArgs, TResult>;
  task<TArgs extends object | void = void, TResult = unknown>(
    operator: Operator<TArgs, TResult>,
    options: TaskOptions,
  ): TaskFactory<TArgs, TResult>;
  task<TArgs extends object | void = void, TReturn = unknown>(
    taskIdOrHandler:
      string | ((args: TArgs) => TReturn | Promise<TReturn>) | Operator<never, unknown>,
    handlerOrOptions?:
      ((args: TArgs) => TReturn | Promise<TReturn>) | Operator<never, unknown> | TaskOptions,
    maybeOptions?: TaskOptions,
  ): TaskFactory<TArgs, TReturn> {
    return this.#addTask(undefined, taskIdOrHandler, handlerOrOptions, maybeOptions);
  }

  /**
   * Declare a task whose boolean picks a branch; the side not taken is skipped.
   *
   * ```ts
   * dag.if(hasRows, { rows: validated }).then(loadIfReady).else(loadFallback);
   * dag.if(hasRows, { rows: validated }, { taskId: "has_rows", retries: 2 }).then(loadIfReady);
   * ```
   */
  if<TArgs extends object | void = void>(
    handler: (args: TArgs) => boolean | Promise<boolean>,
    ...args: DeciderArgs<NoInfer<TArgs>>
  ): Condition {
    return this.#placeCondition(this.#placeDecider(handler, args) as TaskRef<boolean>);
  }

  #placeDecider(handler: (args: never) => unknown, args: readonly unknown[]): TaskRef {
    const [inputs, spec] = args as [unknown, TaskOptions | undefined];
    const factory = this.#addTask(undefined, handler, spec) as (...inputs: unknown[]) => TaskRef;
    return inputs === undefined ? factory() : factory(inputs);
  }

  #placeCondition(condition: TaskRef<boolean>): Condition {
    const taskId = condition.taskId;
    const branches: ConditionRecord = {};
    this.#conditions.set(taskId, branches);
    this.#wrapDecider(taskId, async (held: unknown) => {
      if (typeof held !== "boolean") {
        throw new Error(
          `Condition "${taskId}" of Dag "${this.dagId}" returned ${describeValue(held)} ` +
            "rather than a boolean, so there is no branch to take",
        );
      }
      const skipped = held ? branches.whenFalse : branches.whenTrue;
      return { skip: skipped ? [skipped.taskId] : [], result: held };
    });

    const named = new Set<"then" | "else">();
    const name = (side: "then" | "else", target: TaskRef | Condition | Branch): void => {
      // `await` calls `then(resolve, reject)`, so a function here means the condition was awaited.
      if (typeof target === "function") {
        throw new Error(
          `dag.if(...) of Dag "${this.dagId}" was awaited. It builds a branch rather than ` +
            "doing work, so there is nothing to wait for; drop the await",
        );
      }
      if (named.has(side)) {
        throw new Error(
          `Condition "${taskId}" of Dag "${this.dagId}" already has a "${side}" branch; ` +
            "a condition names each side once",
        );
      }
      const taskRef = resolveNode(target) as TaskRef;
      this.#validateOwnNode(taskRef, `the "${side}" branch of "${taskId}"`);
      if (!isTaskRef(taskRef)) {
        throw new Error(
          `The "${side}" branch of Dag "${this.dagId}" condition "${taskId}" has to be a task, ` +
            "not a task group",
        );
      }
      if (side === "else" && taskRef === branches.whenTrue) {
        throw new Error(
          `Both branches of Dag "${this.dagId}" condition "${taskId}" are ` +
            `"${taskRef.taskId}", so the condition decides nothing; drop the else branch`,
        );
      }
      named.add(side);
      if (side === "then") branches.whenTrue = taskRef;
      else branches.whenFalse = taskRef;
      condition.before(taskRef);
    };

    const elseStep: ConditionElse = {
      else: (taskRef) => name("else", taskRef),
    };
    const placed: Condition = {
      dagId: this.dagId,
      taskId,
      then: (taskRef) => {
        name("then", taskRef);
        return elseStep;
      },
      ...this.#forwardEdges(condition, () => placed),
    };
    conditionTasks.set(placed, condition);
    return Object.freeze(placed);
  }

  #wrapDecider(
    taskId: string,
    decide: (returned: unknown) => Promise<{ skip: string[]; result: unknown }>,
  ): void {
    const { task, spec, fn } = this.#tasks.get(taskId)!;
    // A decider is always declared from a handler, never from an operator.
    const inner = fn!;
    const wrapped: TaskFunction = async (args) => {
      const { skip, result } = await decide(await inner(args as never));
      if (skip.length > 0) await skipTasks(getClient() as CoordinatorClient, skip);
      return result;
    };
    this.#tasks.set(taskId, { task, spec, canSkipDownstream: true, fn: wrapped });
  }

  /**
   * Declare a task that returns one of its cases; every other case is skipped.
   *
   * ```ts
   * dag.switch(pickPath, { rows: extracted }).case(handleLong).case(handleShort);
   * ```
   */
  switch<TArgs extends object | void = void>(
    handler: (args: TArgs) => TaskRef | Promise<TaskRef>,
    ...args: DeciderArgs<NoInfer<TArgs>>
  ): Branch {
    return this.#placeBranch(this.#placeDecider(handler, args) as TaskRef<TaskRef>);
  }

  #forwardEdges<T>(ref: TaskRef, self: () => T) {
    return {
      before: (...downstream: readonly Node[]) => {
        ref.before(...downstream);
        return self();
      },
      after: (...upstream: readonly Node[]) => {
        ref.after(...upstream);
        return self();
      },
    };
  }

  #placeBranch(decider: TaskRef<TaskRef>): Branch {
    const taskId = decider.taskId;
    const candidates: TaskRef[] = [];
    this.#branches.set(taskId, candidates);
    this.#wrapDecider(taskId, async (chosen: unknown) => {
      const known = candidates.map((ref) => ref.taskId);
      const picked = isTaskRef(chosen) && chosen.dagId === this.dagId ? chosen.taskId : undefined;
      if (picked === undefined || !known.includes(picked)) {
        throw new Error(
          `Task "${taskId}" of Dag "${this.dagId}" chose ` +
            `${isTaskRef(chosen) ? `"${chosen.taskId}"` : describeValue(chosen)}, ` +
            `which is not one of its cases: ${known.join(", ")}`,
        );
      }
      return { skip: known.filter((id) => id !== picked), result: picked };
    });

    const branch: Branch = {
      dagId: this.dagId,
      taskId,
      case: (taskRef) => {
        this.#validateOwnNode(taskRef, `a case of "${taskId}"`);
        if (!isTaskRef(taskRef)) {
          throw new Error(
            `A case of Dag "${this.dagId}" branch "${taskId}" has to be a task, not a task group`,
          );
        }
        if (candidates.some((candidate) => candidate.taskId === taskRef.taskId)) {
          throw new Error(
            `Dag "${this.dagId}" branch "${taskId}" lists "${taskRef.taskId}" twice; ` +
              "each case names a different task",
          );
        }
        candidates.push(taskRef);
        decider.before(taskRef);
        return branch;
      },
      ...this.#forwardEdges(decider, () => branch),
    };
    conditionTasks.set(branch, decider);
    return Object.freeze(branch);
  }

  /**
   * Declare a task group of this Dag.
   *
   * The group prefixes the id of everything declared in it, unless
   * {@link TaskGroupOptions.prefixGroupId} turns that off, and stands at
   * either end of an order-only edge in its own right.
   *
   * ```ts
   * const staging = dag.taskGroup("staging"); // tasks "staging.<id>"
   * const checks = dag.taskGroup("checks", { prefixGroupId: false }); // ids as written
   * ```
   */
  taskGroup(groupId: string, options?: TaskGroupOptions): TaskGroupRef {
    return this.#addGroup(undefined, groupId, options);
  }

  #addTask<TArgs extends object | void, TReturn>(
    groupId: string | undefined,
    taskIdOrHandler:
      string | ((args: TArgs) => TReturn | Promise<TReturn>) | Operator<never, unknown>,
    handlerOrOptions?:
      ((args: TArgs) => TReturn | Promise<TReturn>) | Operator<never, unknown> | TaskOptions,
    maybeOptions?: TaskOptions,
  ): TaskFactory<TArgs, TReturn> {
    const idGiven = typeof taskIdOrHandler === "string";
    const body = idGiven ? handlerOrOptions : taskIdOrHandler;
    const operator = isOperator(body) ? body : undefined;
    const handler = body as (args: TArgs) => TReturn | Promise<TReturn>;
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
        `Task "${taskIdOrHandler as string}" of Dag "${this.dagId}" also carries taskId ` +
          `"${specTaskId}" in its spec; give the id once, either positionally or in the spec`,
      );
    }
    const defaulted = idGiven ? undefined : (specTaskId ?? readFunctionName(handler));
    const declared = idGiven ? (taskIdOrHandler as string) : defaulted;
    if (declared === undefined && operator?.requiresTaskId) {
      throw new Error(
        `A ${operator.label ?? operator.operatorName} task of Dag "${this.dagId}" has no taskId; ` +
          `name it with ${operator.taskIdExample ?? 'dag.task("my_task", operator)'}`,
      );
    }
    if (declared === undefined) {
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
    if (declared.includes(GROUP_SEPARATOR)) {
      // The separator is what joins a group to what it holds, so a task id
      // carrying one would name a group that does not exist.
      throw new Error(
        `Task ID "${declared}" of Dag "${this.dagId}" cannot contain "${GROUP_SEPARATOR}"; ` +
          "declare a task group with taskGroup(...) and the prefix is added for you",
      );
    }
    const taskId = this.#childId(groupId, declared);
    if (operator === undefined && typeof handler !== "function") {
      throw new Error(`handler for Dag "${this.dagId}" task "${taskId}" must be a function`);
    }
    // TypeScript already says so, but a plain-JavaScript author lands here
    // with the argument list Python would take.
    if (operator === undefined && handler.length > 1) {
      throw new Error(
        `Handler for Dag "${this.dagId}" task "${taskId}" declares ${handler.length} parameters; ` +
          "a handler takes one object of named arguments — async ({ rows, region }) => ...",
      );
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
    if (
      operator?.executionTimeoutAlternative !== undefined &&
      spec.executionTimeout !== undefined
    ) {
      throw new Error(
        `Task "${taskId}" of Dag "${this.dagId}" is a ${operator.label ?? operator.operatorName} ` +
          "task, which does not enforce executionTimeout; use " +
          `${operator.executionTimeoutAlternative} instead`,
      );
    }
    this.#reserveNodeId(taskId, "Task");
    const task = this.#createTaskRef(taskId);
    if (groupId !== undefined) this.#groups.get(groupId)!.taskIds.push(taskId);
    // The runtime dispatches every handler through one instantiation, as it
    // does a registered TaskHandler.
    const runs: TaskBody & { readonly canSkipDownstream?: boolean } = operator
      ? { operator, canSkipDownstream: operator.canSkipDownstream }
      : { fn: handler as unknown as TaskFunction };
    this.#tasks.set(taskId, {
      task,
      ...runs,
      spec: freezeSpec(spec, () => `The spec for Dag "${this.dagId}" task "${taskId}"`),
    });
    return ((...inputs: unknown[]) => {
      // TypeScript already says so, but from plain JavaScript a second
      // argument would be dropped without a word.
      if (inputs.length > 1) {
        throw new Error(
          `Task "${taskId}" of Dag "${this.dagId}" was given ${inputs.length} arguments; ` +
            "it takes one object naming its inputs: myTask({ rows, region })",
        );
      }
      if (operator?.takesNoInputs && inputs[0] !== undefined) {
        throw new Error(
          `Task "${taskId}" of Dag "${this.dagId}" is a ` +
            `${operator.label ?? operator.operatorName} task, which takes no inputs`,
        );
      }
      this.#wire(taskId, inputs[0]);
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

  #addGroup(
    parentGroupId: string | undefined,
    groupId: string,
    options: TaskGroupOptions = {},
  ): TaskGroupRef {
    if (typeof groupId !== "string" || groupId.length === 0) {
      throw new Error(`A task group of Dag "${this.dagId}" must have a non-empty ID`);
    }
    if (groupId.includes(GROUP_SEPARATOR)) {
      // The separator is what joins a group to what it holds, so one inside an
      // ID would make the resulting task id ambiguous.
      throw new Error(
        `Task group ID "${groupId}" of Dag "${this.dagId}" cannot contain ` +
          `"${GROUP_SEPARATOR}"; nest groups with taskGroup(...) instead`,
      );
    }
    // Python's validate_group_key. The length is in code points, as Python's
    // len() counts, not in UTF-16 units.
    const length = [...groupId].length;
    if (length > GROUP_ID_MAX_LENGTH) {
      throw new Error(
        `Task group ID "${groupId}" of Dag "${this.dagId}" has ${length} characters; ` +
          `at most ${GROUP_ID_MAX_LENGTH} are allowed`,
      );
    }
    if (!GROUP_ID_CHARACTERS.test(groupId)) {
      throw new Error(
        `Task group ID "${groupId}" of Dag "${this.dagId}" has to be made of letters, digits, ` +
          "dashes and underscores",
      );
    }
    const prefixGroupId = this.#prefixGroupIdOf(groupId, options);
    if (this.#finalized) {
      throw new Error(
        `Task group "${groupId}" cannot be added to Dag "${this.dagId}" after the Dag was read; ` +
          "declare every group while the module is loading",
      );
    }
    const fullId = this.#childId(parentGroupId, groupId);
    this.#reserveNodeId(fullId, "Task group");
    this.#groups.set(fullId, {
      groupId: fullId,
      ...(parentGroupId !== undefined && { parentGroupId }),
      prefixGroupId,
      taskIds: [],
      childGroupIds: [],
    });
    if (parentGroupId !== undefined) this.#groups.get(parentGroupId)!.childGroupIds.push(fullId);
    const group = this.#createTaskGroupRef(fullId);
    this.#groupRefs.set(fullId, group);
    return group;
  }

  // TypeScript is bypassable, as for #taskSpecOf, so a misspelled or mistyped
  // option is rejected rather than read as the default.
  #prefixGroupIdOf(groupId: string, options: TaskGroupOptions): boolean {
    const value: unknown = options;
    if (!isPlainRecord(value)) {
      throw new Error(`options for Dag "${this.dagId}" task group "${groupId}" must be an object`);
    }
    for (const key of Reflect.ownKeys(value)) {
      if (key !== "prefixGroupId") {
        throw new Error(
          `Unknown option "${String(key)}" for Dag "${this.dagId}" task group "${groupId}"`,
        );
      }
    }
    const { prefixGroupId = true } = value;
    if (typeof prefixGroupId !== "boolean") {
      throw new Error(
        `prefixGroupId for Dag "${this.dagId}" task group "${groupId}" must be a boolean`,
      );
    }
    return prefixGroupId;
  }

  // Python's TaskGroup.child_id: a group adds its ID in front of what it holds
  // only when prefixGroupId is on, and a nested group's ID is built the same way.
  #childId(groupId: string | undefined, id: string): string {
    if (groupId === undefined || !this.#groups.get(groupId)!.prefixGroupId) return id;
    return `${groupId}${GROUP_SEPARATOR}${id}`;
  }

  // Tasks and groups share one namespace, as they do in Python: a serialized
  // Dag addresses both by a bare ID, so `dag.task("x")` and `dag.taskGroup("x")`
  // cannot both exist.
  #reserveNodeId(id: string, kind: "Task" | "Task group"): void {
    if (this.#tasks.has(id) || this.#groups.has(id)) {
      throw new Error(`${kind} "${id}" is already registered for Dag "${this.dagId}"`);
    }
  }

  #createTaskGroupRef(groupId: string): TaskGroupRef {
    const group: TaskGroupRef = {
      dagId: this.dagId,
      groupId,
      task: <TArgs extends object | void, TReturn>(
        taskIdOrHandler:
          string | ((args: TArgs) => TReturn | Promise<TReturn>) | Operator<never, unknown>,
        handlerOrOptions?:
          ((args: TArgs) => TReturn | Promise<TReturn>) | Operator<never, unknown> | TaskOptions,
        maybeOptions?: TaskOptions,
      ) => this.#addTask<TArgs, TReturn>(groupId, taskIdOrHandler, handlerOrOptions, maybeOptions),
      taskGroup: (childId: string, options?: TaskGroupOptions) =>
        this.#addGroup(groupId, childId, options),
      before: (...downstream) => {
        for (const other of downstream) this.#addOrderEdge(group, other, "before");
        return group;
      },
      after: (...upstream) => {
        for (const other of upstream) this.#addOrderEdge(other, group, "after");
        return group;
      },
    } as TaskGroupRef;
    brand(group, "TaskGroupRef");
    return Object.freeze(group);
  }

  #createTaskRef(taskId: string): TaskRef {
    const task: TaskRef = {
      dagId: this.dagId,
      taskId,
      before: (...downstream) => {
        for (const other of downstream) this.#addOrderEdge(task, other, "before");
        return task;
      },
      after: (...upstream) => {
        for (const other of upstream) this.#addOrderEdge(other, task, "after");
        return task;
      },
    };
    brand(task, "TaskRef");
    return Object.freeze(task);
  }

  #addOrderEdge(upstreamNode: Node, downstreamNode: Node, verb: "before" | "after"): void {
    const upstream = resolveNode(upstreamNode);
    const downstream = resolveNode(downstreamNode);
    if (this.#finalized) {
      throw new Error(
        `An edge was drawn on Dag "${this.dagId}" after the Dag was read; ` +
          "declare every edge while the module is loading",
      );
    }
    // The argument is the one that can be foreign: the receiver is a node this
    // Dag handed out, since it is what carries the method.
    const other = verb === "before" ? downstream : upstream;
    this.#validateOwnNode(other, `${verb}()`);
    const upstreamId = nodeId(upstream);
    const downstreamId = nodeId(downstream);
    if (upstreamId === downstreamId) {
      throw new Error(
        `${verb}() cannot draw an edge from node "${upstreamId}" of Dag "${this.dagId}" to ` +
          "itself; an edge orders two different nodes",
      );
    }
    const key = `${upstreamId}${EDGE_KEY_SEPARATOR}${downstreamId}`;
    // Idempotent, so an edge drawn from both ends is one edge.
    if (!this.#orderEdges.has(key)) {
      this.#orderEdges.set(
        key,
        Object.freeze({ upstream: upstreamId!, downstream: downstreamId! }),
      );
    }
  }

  #validateOwnNode(node: Node, label: string): void {
    const id = nodeId(node);
    if (id === undefined) {
      throw new Error(
        `${label} on Dag "${this.dagId}" takes tasks and task groups this Dag handed out, ` +
          "not arbitrary values",
      );
    }
    if (node.dagId !== this.dagId) {
      throw new Error(
        `${label} cannot reach Dag "${node.dagId}" node "${id}" from Dag ` +
          `"${this.dagId}"; an edge joins two nodes of one Dag`,
      );
    }
    // Identity, not the ID: two Dag objects can carry the same dagId, and a
    // second resolved copy of this package brands its own nodes. A group is
    // checked the same way — matching on the ID alone would silently retarget
    // the edge at this Dag's own group of that name.
    if (isTaskRef(node) ? this.#tasks.get(id)?.task !== node : this.#groupRefs.get(id) !== node) {
      throw new Error(
        `${label} was given a reference to "${id}" that this Dag did not hand out; ` +
          `it comes from another Dag object with the same ID, or ${DUPLICATE_COPY_HINT}`,
      );
    }
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

  #inputsByName(taskId: string, inputs: unknown): Record<string, unknown> {
    if (inputs === undefined) return {};
    // A reference is a plain object too, so `load(extracted)` would otherwise
    // read as a map of argument names.
    if (!isPlainRecord(inputs) || isTaskRef(inputs)) {
      throw new Error(
        `Task "${taskId}" of Dag "${this.dagId}" takes one object naming its inputs: ` +
          "myTask({ rows, region })",
      );
    }
    return inputs;
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
    for (const [taskId, branches] of this.#conditions) {
      if (branches.whenTrue === undefined) {
        throw new Error(
          `Condition "${taskId}" of Dag "${this.dagId}" names no branch, so it decides nothing; ` +
            "give it one with dag.if(handler, inputs).then(task)",
        );
      }
    }
    for (const [taskId, cases] of this.#branches) {
      if (cases.length === 0) {
        throw new Error(
          `Branch "${taskId}" of Dag "${this.dagId}" has no cases, so it decides nothing; ` +
            "give it the tasks to choose between with dag.switch(handler, inputs).case(task)",
        );
      }
    }
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

// Python's GROUP_KEY_REGEX and validate_group_key limit: a group ID has no dot,
// since the dot is what joins it to what it holds.
const GROUP_ID_CHARACTERS = /^[\p{L}\p{N}_-]+$/u;
const GROUP_ID_MAX_LENGTH = 200;

/** A value as an error message names it: its type, or the literal when short. */
function describeValue(value: unknown): string {
  if (value === null) return "null";
  if (value === undefined) return "undefined";
  if (typeof value === "object") return Array.isArray(value) ? "an array" : "an object";
  return `the ${typeof value} ${typeof value === "string" ? JSON.stringify(value) : String(value)}`;
}

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

/** Internal: every task group of a Dag, keyed by full group ID. */
export function getDagTaskGroups(dag: Dag): ReadonlyMap<string, TaskGroupRecord> {
  return groupsOf(dag);
}

/** Internal: the order-only edges of a Dag, in the order they were drawn. */
export function getDagOrderEdges(dag: Dag): readonly OrderEdge[] {
  return orderEdgesOf(dag);
}

/** Internal: what each task of a Dag was called with, keyed by task ID.
 *  A task that has not been called is absent. */
export function getDagTaskInputs(dag: Dag): ReadonlyMap<string, RecordedInputs> {
  return inputsOf(dag);
}

/** Internal: the source file the Dag was constructed from, captured from
 *  `airflow-ts-pack`'s module-source tag. `undefined` when the constructor
 *  ran outside a packed bundle (e.g. a unit test that instantiates `Dag`
 *  directly), which the manifest omits rather than records as an empty path. */
export function getDagDefinedIn(dag: Dag): string | undefined {
  return definedInOf(dag);
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

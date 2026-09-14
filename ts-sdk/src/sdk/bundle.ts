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

// The bundle: what a TypeScript bundle process provides, and how it serves it.

import { brand, DUPLICATE_COPY_HINT, hasBrand } from "./brand.js";
import { Dag, getDagTaskRecords, isDag, type TaskRef } from "./dag.js";
import { getTaskHandlerFunction, isTaskHandler, TaskHandler } from "./task-handler.js";
import type { TaskFunction } from "./task.js";

// Assigned inside Bundle's static block, as Dag does for its tasks.
let entriesOf: (bundle: Bundle) => ReadonlyMap<string, BundleEntry>;

/**
 * What {@link Bundle.register} and the {@link Bundle} constructor accept: a
 * {@link TaskHandler} for a task that a Python Dag declares, or a {@link Dag}
 * declared in TypeScript.
 */
export type Registerable = Dag | TaskHandler;

// What a bundle holds per dag_id. The two arms are exclusive by construction:
// a Dag is the native case and owns its own tasks, while task handlers supply
// bodies for a Dag that Python declares, so one dag_id is never both.
type BundleEntry =
  | { readonly kind: "dag"; readonly dag: Dag }
  | { readonly kind: "handlers"; readonly handlers: Map<string, TaskFunction> };

function entryTaskIds(entry: BundleEntry): string[] {
  return entry.kind === "dag" ? [...entry.dag.taskIds] : [...entry.handlers.keys()];
}

/** Internal: whether `value` is a Bundle built by any copy of this package. */
export function isBundle(value: unknown): value is Bundle {
  return hasBrand(value, "Bundle");
}

/** Internal: a Dag this bundle provides for, with its task IDs, as
 *  {@link listBundleDags} reports it. A task-less native Dag is included, so
 *  the bundle manifest keeps it visible. */
export interface RegisteredDag {
  /** Identifier of the registered Dag. */
  readonly dagId: string;
  /** Airflow task IDs, including any TaskGroup prefix. */
  readonly tasks: string[];
}

/**
 * What a bundle process provides to Airflow, and the thing that serves it.
 *
 * A bundle entry point builds one, registers what it provides, and awaits
 * `serve()` at module top level:
 *
 * ```ts
 * const dag = new Dag("my_dag");
 * dag.task("extract", extractFn);
 *
 * const bundle = new Bundle();
 * bundle.register(dag);
 * await bundle.serve();
 * ```
 *
 * Registering holds no sockets and starts nothing, so a test can build a bundle
 * and invoke a handler through {@link getTaskHandler} without any runtime in
 * scope. Only `serve()` connects to Airflow.
 *
 * Lookups delegate live to each Dag's task map, so tasks added to a Dag
 * after registration are visible: the bundle records Dag identity, not
 * a snapshot of its tasks.
 */
export class Bundle {
  // Keyed by dag_id and insertion-ordered, so the bundle manifest lists what
  // this process provides in the order the entry point registered it.
  readonly #entries = new Map<string, BundleEntry>();

  static {
    entriesOf = (bundle) => bundle.#entries;
  }

  /** Registers `items`, on the same terms as {@link register}. */
  constructor(...items: Registerable[]) {
    brand(this, "Bundle");
    this.register(...items);
  }

  /** Register what this bundle provides. Registering an already-registered
   *  `dagId` throws, and a call that throws registers none of its items.
   *
   *  The constructor covers the common case; this is for a bundle that
   *  collects what it provides across several modules. */
  register(...items: Registerable[]): void {
    // Validated against what is already held *and* against this call, in full,
    // before anything is written: a call that throws registers none of its
    // items, so a bundle never half-provides what its author listed once.
    const incomingDags = new Set<string>();
    const incomingTaskHandlers = new Set<string>();
    for (const item of items) {
      // Typed as Registerable, so narrowing it would collapse to never; these
      // guard callers reaching this from plain JavaScript.
      const candidate: unknown = item;
      // Another copy's value cannot be registered, since both kinds read
      // private state keyed to this copy's class, so it is rejected by its cause.
      if (candidate instanceof Dag) {
        this.#checkDag(candidate, incomingDags);
      } else if (candidate instanceof TaskHandler) {
        this.#checkTaskHandler(candidate, incomingDags, incomingTaskHandlers);
      } else if (isDag(candidate)) {
        throw new Error(`Dag "${(candidate as Dag).dagId}" ${DUPLICATE_COPY_HINT}`);
      } else if (isTaskHandler(candidate)) {
        const foreign = candidate as TaskHandler;
        throw new Error(
          `Task handler for Dag "${foreign.dagId}" task "${foreign.taskId}" ${DUPLICATE_COPY_HINT}`,
        );
      } else {
        throw new Error("only Dag and TaskHandler instances can be registered");
      }
    }
    for (const item of items) {
      if (item instanceof Dag) {
        this.#entries.set(item.dagId, { kind: "dag", dag: item });
      } else {
        const existing = this.#entries.get(item.dagId);
        const handlers =
          existing?.kind === "handlers" ? existing.handlers : new Map<string, TaskFunction>();
        handlers.set(item.taskId, getTaskHandlerFunction(item));
        if (existing === undefined) {
          this.#entries.set(item.dagId, { kind: "handlers", handlers });
        }
      }
    }
  }

  #checkDag(dag: Dag, incomingDags: Set<string>): void {
    if (this.#entries.get(dag.dagId)?.kind === "handlers") {
      throw new Error(
        `Dag "${dag.dagId}" already has registered task handlers; a Dag declared in ` +
          "TypeScript owns its own tasks, so one Dag ID cannot have both",
      );
    }
    if (this.#entries.has(dag.dagId) || incomingDags.has(dag.dagId)) {
      throw new Error(`Dag "${dag.dagId}" is already registered`);
    }
    incomingDags.add(dag.dagId);
  }

  #checkTaskHandler(
    handler: TaskHandler,
    incomingDags: Set<string>,
    incomingTaskHandlers: Set<string>,
  ): void {
    // A native Dag attaches its tasks with dag.task(...), so a task handler for
    // the same Dag ID would be a second, disagreeing source for its task list.
    if (this.#entries.get(handler.dagId)?.kind === "dag" || incomingDags.has(handler.dagId)) {
      throw new Error(
        `Dag "${handler.dagId}" is declared in TypeScript; attach its tasks with ` +
          "dag.task(...) rather than registering task handlers for them",
      );
    }
    const entry = this.#entries.get(handler.dagId);
    // Keyed on the pair, not the task ID: one bundle serves several Dags, and
    // the same task_id under two of them is two different handlers.
    const pair = `${handler.dagId}\u0000${handler.taskId}`;
    if (
      entry?.kind === "handlers"
        ? entry.handlers.has(handler.taskId)
        : incomingTaskHandlers.has(pair)
    ) {
      throw new Error(
        `A handler for Dag "${handler.dagId}" task "${handler.taskId}" is already registered`,
      );
    }
    incomingTaskHandlers.add(pair);
  }

  /**
   * Serve this bundle to Airflow. The entry point of a TypeScript Dag bundle.
   *
   * A bundle process serves one supervisor request, so a second call, which
   * would connect a second pair of sockets, is rejected. A call that fails is
   * not a serve, and may be retried. Resolves once the supervisor has been sent
   * the terminal frame for the work this process was started for, and the same
   * call answers the build-time `--airflow-metadata` query `airflow-ts-pack`
   * makes.
   *
   * Everything this bundle provides must be registered before `serve()` is
   * awaited: what is left out is not part of the bundle, and its tasks are
   * marked removed at runtime.
   */
  async serve(): Promise<void> {
    // `const { serve } = bundle` detaches the method, which would otherwise
    // fail deep in the runtime on a missing private field rather than here.
    validateOwnBundle(this, "bundle.serve()");
    // Imported here rather than at module top: the coordinator reads a bundle,
    // so a static import would make the authoring surface and the coordinator
    // mutually dependent for the sake of one call.
    const { serveBundle } = await import("../coordinator/serve.js");
    await serveBundle(this);
  }

  /** Look up a registered handler, the way the runtime dispatches a task.
   *  Returns `undefined` when no handler exists. */
  getTaskHandler(dagId: string, taskId: string): TaskFunction | undefined {
    const entry = this.#entries.get(dagId);
    if (entry === undefined) return undefined;
    return entry.kind === "dag"
      ? getDagTaskRecords(entry.dag).get(taskId)?.fn
      : entry.handlers.get(taskId);
  }
}

/** Internal: reject a `this` that is not a Bundle built by this copy, naming
 *  the cause. */
export function validateOwnBundle(value: unknown, accessor: string): asserts value is Bundle {
  if (value instanceof Bundle) return;
  throw new Error(
    isBundle(value)
      ? `The bundle ${accessor} was called on ${DUPLICATE_COPY_HINT}`
      : `${accessor} must be called on a Bundle; build one with new Bundle(...)`,
  );
}

/** Internal: every task handle this bundle can dispatch, across both kinds. Not
 *  re-exported from the package root: enumerating what the runtime dispatches
 *  is the runtime's job. */
export function listBundleTasks(bundle: Bundle): TaskRef[] {
  return [...entriesOf(bundle)].flatMap(([dagId, entry]) =>
    entryTaskIds(entry).map((taskId) => ({ dagId, taskId })),
  );
}

/** Internal: every Dag this bundle provides for, with its task IDs. A native
 *  Dag with no tasks is included; a Dag known only through task handlers always
 *  has at least one, since a handler is what put it here. */
export function listBundleDags(bundle: Bundle): RegisteredDag[] {
  return [...entriesOf(bundle)].map(([dagId, entry]) => ({
    dagId,
    tasks: entryTaskIds(entry),
  }));
}

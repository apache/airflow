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
import { Dag, finalizeDag, getDagTaskRecords, isDag } from "./dag.js";
import { getTaskHandlerFunction, isTaskHandler, TaskHandler } from "./task-handler.js";
import type { TaskFunction } from "./task.js";

// Assigned inside Bundle's static block, as Dag does for its tasks.
let dagsOf: (bundle: Bundle) => ReadonlyMap<string, Dag>;
let taskHandlersOf: (bundle: Bundle) => ReadonlyMap<string, ReadonlyMap<string, TaskFunction>>;

/**
 * What {@link Bundle.register} and the {@link Bundle} constructor accept: a
 * {@link TaskHandler} for a task that a Python Dag declares, or a {@link Dag}
 * declared in TypeScript.
 */
// `never` for the handler's argument type: a TaskHandler is contravariant in it
// (the handler takes it), so this is the one instantiation every typed handler
// is assignable to, `TaskHandler<TransformArgs>` included.
export type Registerable = Dag | TaskHandler<never, unknown>;

/** Internal: whether `value` is a Bundle built by any copy of this package. */
export function isBundle(value: unknown): value is Bundle {
  return hasBrand(value, "Bundle");
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
  // Dags declared in TypeScript, keyed by dag_id in registration order.
  #dags = new Map<string, Dag>();
  // Handlers for the tasks a Python Dag declares, keyed by dag_id and then by
  // task_id, both in registration order.
  #taskHandlers = new Map<string, Map<string, TaskFunction>>();

  static {
    dagsOf = (bundle) => bundle.#dags;
    taskHandlersOf = (bundle) => bundle.#taskHandlers;
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
    // Staged on copies and committed once every item is accepted, so a call
    // that throws registers none of its items and a bundle never
    // half-provides what its author listed in one call.
    const dags = new Map(this.#dags);
    const taskHandlers = new Map(
      [...this.#taskHandlers].map(([dagId, handlers]) => [dagId, new Map(handlers)] as const),
    );
    for (const item of items) {
      // Typed as Registerable, so narrowing it would collapse to never; these
      // guard callers reaching this from plain JavaScript.
      const candidate: unknown = item;
      // Another copy's value cannot be registered, since both kinds read
      // private state keyed to this copy's class, so it is rejected by its cause.
      if (candidate instanceof Dag) {
        stageDag(dags, taskHandlers, candidate);
      } else if (candidate instanceof TaskHandler) {
        stageTaskHandler(dags, taskHandlers, candidate);
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
    this.#dags = dags;
    this.#taskHandlers = taskHandlers;
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
    const dag = this.#dags.get(dagId);
    return dag === undefined
      ? this.#taskHandlers.get(dagId)?.get(taskId)
      : getDagTaskRecords(dag).get(taskId)?.fn;
  }
}

// A Dag declared in TypeScript owns its own tasks, so a dag_id it holds and a
// dag_id task handlers hold are two disagreeing sources for one task list.
// Each kind is therefore rejected for a dag_id the other one already holds,
// whichever order they were registered in.

function stageDag(
  dags: Map<string, Dag>,
  taskHandlers: ReadonlyMap<string, ReadonlyMap<string, TaskFunction>>,
  dag: Dag,
): void {
  if (taskHandlers.has(dag.dagId)) {
    throw new Error(
      `Dag "${dag.dagId}" already has registered task handlers; a Dag declared in ` +
        "TypeScript owns its own tasks, so one Dag ID cannot have both",
    );
  }
  if (dags.has(dag.dagId)) {
    throw new Error(`Dag "${dag.dagId}" is already registered`);
  }
  dags.set(dag.dagId, dag);
}

function stageTaskHandler(
  dags: ReadonlyMap<string, Dag>,
  taskHandlers: Map<string, Map<string, TaskFunction>>,
  handler: TaskHandler,
): void {
  if (dags.has(handler.dagId)) {
    throw new Error(
      `Dag "${handler.dagId}" is declared in TypeScript; attach its tasks with ` +
        "dag.task(...) rather than registering task handlers for them",
    );
  }
  const forDag = taskHandlers.get(handler.dagId) ?? new Map<string, TaskFunction>();
  if (forDag.has(handler.taskId)) {
    throw new Error(
      `A handler for Dag "${handler.dagId}" task "${handler.taskId}" is already registered`,
    );
  }
  forDag.set(handler.taskId, getTaskHandlerFunction(handler));
  taskHandlers.set(handler.dagId, forDag);
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

/** Internal: finalize every Dag this bundle declared in TypeScript, so no task
 *  can be added or wired afterwards. */
export function finalizeBundleDags(bundle: Bundle): void {
  for (const dag of dagsOf(bundle).values()) {
    finalizeDag(dag);
  }
}

/** Internal: the task IDs this bundle can dispatch, per Dag: the Dags declared
 *  in TypeScript first, then the Dags its task handlers name, each in
 *  registration order. A Dag declared in TypeScript with no tasks is included. */
export function bundleDagTaskIds(bundle: Bundle): Map<string, string[]> {
  const byDag = new Map<string, string[]>();
  for (const [dagId, dag] of dagsOf(bundle)) {
    byDag.set(dagId, [...dag.taskIds]);
  }
  for (const [dagId, handlers] of taskHandlersOf(bundle)) {
    byDag.set(dagId, [...handlers.keys()]);
  }
  return byDag;
}
